package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnerRecoveryCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void runsOneBoundedTurnAndActivatesOnlyAfterTheCursorIsExhausted() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("coordinator-assignment")), 1,
                new KafkaActivationBarrier(shardId, "cluster", topic, 2));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "worker-recovery",
                Bytes.sha256(Bytes.utf8("coordinator-session")), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("bounded"));
        final KafkaSourcePosition firstPosition = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                null, 1_000);
        final KafkaSourcePosition secondPosition = new KafkaSourcePosition(shardId, "cluster", topic, 1,
                null, 1_001);
        final PreparedCommand first = schedule(shardId, "first");
        final PreparedCommand second = schedule(shardId, "second");
        final CompatibleControlSnapshotV1 snapshot = controlSnapshot(shardId);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(owned, authority, assignment,
                    SourceReplaySuccessor.strictKafka(), SourceReplayCursor.of(List.<SourceReplayEntry>of(
                            new SourceReplayRecord(first, firstPosition, null, null),
                            new SourceReplayRecord(second, secondPosition, null, null)).iterator()),
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic(), snapshot,
                    () -> 101, new ReplayTurnBudget(1, Long.MAX_VALUE, Long.MAX_VALUE));

            final OwnerRecoveryTurn firstTurn = coordinator.runTurn();
            assertEquals(1, firstTurn.outcomes().size());
            assertFalse(firstTurn.complete());
            assertTrue(firstTurn.hasMore());
            assertEquals(ShardLifecycleState.CATCHING_UP, owned.state());
            assertEquals(ShardLifecycleState.CATCHING_UP, backend.current(shardId).orElseThrow().state());

            final OwnerRecoveryTurn secondTurn = coordinator.runTurn();
            assertEquals(1, secondTurn.outcomes().size());
            assertTrue(secondTurn.complete());
            assertFalse(secondTurn.hasMore());
            assertEquals(2, secondTurn.turnNumber());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, backend.current(shardId).orElseThrow().state());
            assertEquals(secondPosition, owned.lastCatchupPosition());

            final OwnerRecoveryTurn idempotent = coordinator.runTurn();
            assertTrue(idempotent.complete());
            assertTrue(idempotent.outcomes().isEmpty());
            assertEquals(2, idempotent.turnNumber());
        }
    }

    @Test
    void clockFailureBeforeCatchupFencesTheLocalOwner() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("clock-assignment")), 1,
                new KafkaActivationBarrier(shardId, "cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "worker-clock",
                Bytes.sha256(Bytes.utf8("clock-session")), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("clock"));
        final CompatibleControlSnapshotV1 snapshot = controlSnapshot(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            final OwnerRecoveryCoordinator coordinator = new OwnerRecoveryCoordinator(owned, authority, assignment,
                    SourceReplaySuccessor.strictKafka(), SourceReplayCursor.of(List.<SourceReplayEntry>of().iterator()),
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic(), snapshot,
                    () -> { throw new AssertionError("clock unavailable"); },
                    ReplayTurnBudget.unbounded());

            assertThrows(AssertionError.class, coordinator::runTurn);
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertEquals(ShardLifecycleState.ACQUIRING, backend.current(shardId).orElseThrow().state());
        }
    }

    private static PreparedCommand schedule(final ShardId shardId, final String suffix) {
        return PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("coordinator-lane-" + suffix)),
                        2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8(suffix)), 10_000);
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shardId) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 101), 1, bytes(32, 102), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 103), 1,
                        new io.nereusstream.delay.protocol.PublishAdmissionBody.ChargeVector(
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
