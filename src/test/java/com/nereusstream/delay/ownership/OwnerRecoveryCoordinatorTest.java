package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OwnerRecoveryCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void publicConstructorBuildsTheSourceApplyExecutorFromItsExactDependencies() {
        final var publicConstructors = Arrays.stream(OwnerRecoveryCoordinator.class.getDeclaredConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
                .toList();

        assertEquals(1, publicConstructors.size());
        assertFalse(Arrays.asList(publicConstructors.get(0).getParameterTypes())
                .contains(SourceApplyWorkClassExecutor.class));
        assertFalse(Arrays.stream(SourceApplyWorkClassExecutor.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("submitRecovery"))
                .anyMatch(method -> Modifier.isPublic(method.getModifiers())));
    }

    @Test
    void runsOneBoundedTurnAndActivatesOnlyAfterTheCursorIsExhausted() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(
                shardId,
                Bytes.sha256(Bytes.utf8("coordinator-assignment")),
                1,
                new KafkaActivationBarrier(shardId, "cluster", topic, 2));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(
                        assignment, "worker-recovery", Bytes.sha256(Bytes.utf8("coordinator-session")), 100, 100)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("bounded"));
        final KafkaSourcePosition firstPosition = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final KafkaSourcePosition secondPosition = new KafkaSourcePosition(shardId, "cluster", topic, 1, null, 1_001);
        final PreparedCommand first = schedule(shardId, "first");
        final PreparedCommand second = schedule(shardId, "second");
        final CompatibleControlSnapshotV1 snapshot = controlSnapshot(shardId);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            final OwnedDelayShard owned =
                    new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            final var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(
                    owned,
                    authority,
                    assignment,
                    SourceReplaySuccessor.strictKafka(),
                    SourceReplayCursor.of(List.<SourceReplayEntry>of(
                                    new SourceReplayRecord(first, firstPosition, null, null),
                                    new SourceReplayRecord(second, secondPosition, null, null))
                            .iterator()),
                    keyPair.getPublic(),
                    snapshot,
                    () -> 101,
                    new ReplayTurnBudget(1, Long.MAX_VALUE, Long.MAX_VALUE),
                    workClasses);

            workClasses.submit(new WorkClassTask(WorkClass.LEASE_FENCE, "recovery-occupied", 1), () -> {});
            final OwnerRecoveryTurn waitingTurn = coordinator.runTurn();
            assertTrue(waitingTurn.waitingForWorkClass());
            assertEquals(WorkClass.SOURCE_APPLY, waitingTurn.pendingTask().workClass());
            assertTrue(waitingTurn.outcomes().isEmpty());
            assertEquals(ShardLifecycleState.CATCHING_UP, owned.state());

            final OwnerRecoveryTurn firstTurn = coordinator.runTurn();
            assertEquals(1, firstTurn.outcomes().size());
            assertFalse(firstTurn.complete());
            assertFalse(firstTurn.waitingForWorkClass());
            assertTrue(firstTurn.hasMore());
            assertEquals(ShardLifecycleState.CATCHING_UP, owned.state());
            assertEquals(
                    ShardLifecycleState.CATCHING_UP,
                    backend.current(shardId).orElseThrow().state());

            final OwnerRecoveryTurn secondTurn = coordinator.runTurn();
            assertEquals(1, secondTurn.outcomes().size());
            assertTrue(secondTurn.complete());
            assertFalse(secondTurn.hasMore());
            assertEquals(3, secondTurn.turnNumber());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
            assertEquals(
                    ShardLifecycleState.ACTIVE_FOR_COMMANDS,
                    backend.current(shardId).orElseThrow().state());
            assertEquals(secondPosition, owned.lastCatchupPosition());

            final OwnerRecoveryTurn idempotent = coordinator.runTurn();
            assertTrue(idempotent.complete());
            assertTrue(idempotent.outcomes().isEmpty());
            assertEquals(3, idempotent.turnNumber());
        }
    }

    @Test
    void clockFailureBeforeCatchupFencesTheLocalOwner() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(
                shardId,
                Bytes.sha256(Bytes.utf8("clock-assignment")),
                1,
                new KafkaActivationBarrier(shardId, "cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(
                        assignment, "worker-clock", Bytes.sha256(Bytes.utf8("clock-session")), 100, 100)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("clock"));
        final CompatibleControlSnapshotV1 snapshot = controlSnapshot(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            final OwnedDelayShard owned =
                    new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            final var keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(
                    owned,
                    authority,
                    assignment,
                    SourceReplaySuccessor.strictKafka(),
                    SourceReplayCursor.of(List.<SourceReplayEntry>of().iterator()),
                    keyPair.getPublic(),
                    snapshot,
                    () -> {
                        throw new AssertionError("clock unavailable");
                    },
                    ReplayTurnBudget.unbounded(),
                    workClasses);

            assertThrows(AssertionError.class, coordinator::runTurn);
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertEquals(
                    ShardLifecycleState.ACQUIRING,
                    backend.current(shardId).orElseThrow().state());
        }
    }

    private static PreparedCommand schedule(final ShardId shardId, final String suffix) {
        return PreparedCommand.schedule(
                shardId,
                new ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("coordinator-lane-" + suffix)),
                        2_000,
                        5_000,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8(suffix)),
                10_000);
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shardId) {
        return new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 101), 1, bytes(32, 102), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(
                        bytes(32, 103),
                        1,
                        new com.nereusstream.delay.protocol.PublishAdmissionBody.ChargeVector(
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass =
                    switch (workClass) {
                        case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                        case QUERY, CHECKPOINT -> false;
                    };
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            maxQueueRecords,
                            1_000_000,
                            maxQueueRecords,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), new AtomicLong()::get);
    }
}
