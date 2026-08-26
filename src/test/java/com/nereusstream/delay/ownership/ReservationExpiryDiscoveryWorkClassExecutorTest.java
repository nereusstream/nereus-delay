package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReservationExpiryDiscoveryWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void discoversOnlyInsideBoundedGcTurnWithoutMaterializingReservation() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("success"), 2)) {
            final SchedulerBudget scanBudget = new SchedulerBudget(1, 8_192, 1_000);
            final AtomicInteger ownerClockReads = new AtomicInteger();
            final AtomicLong scanClock = new AtomicLong();
            final ReservationExpiryDiscoveryWorkClassExecutor.Submission submission = fixture.executor()
                    .submit(
                            scanBudget,
                            () -> {
                                ownerClockReads.incrementAndGet();
                                return 101;
                            },
                            scanClock::getAndIncrement);

            assertTrue(submission.discovered().isEmpty());
            assertEquals(0, ownerClockReads.get());
            final int domainBytes = Bytes.utf8("nereus-delay-reservation-expiry-discovery-task\0").length;
            final int shardBytes = 16 + 4;
            final int budgetBytes = 4 + 8 + 8;
            assertEquals(
                    scanBudget.maxBytes() + domainBytes + shardBytes + budgetBytes,
                    submission.task().bytes());

            assertEquals(List.of(submission.task()), fixture.workClasses.runTurn(fixture.turnBudget()));
            assertEquals(1, ownerClockReads.get());
            assertEquals(
                    List.of(Bytes.hex(fixture.reservationId)),
                    submission.discovered().orElseThrow().stream()
                            .map(work -> Bytes.hex(work.reservationId()))
                            .toList());
            assertEquals(1, fixture.shard.quota().reservationMessages());
            assertNotNull(fixture.store.getValue(
                    ColumnFamily.TIMELINE, KeyCodec.reservationExpiry(5_000, fixture.reservationId), 5));
            assertEquals(
                    PayloadReservationStatus.EXPIRED,
                    fixture.shard.getReservation(fixture.reservationId).status());
        }
    }

    @Test
    void queueRejectionReadsNeitherClockNorStore() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("rejected"), 1)) {
            final AtomicInteger ownerClockReads = new AtomicInteger();
            final AtomicInteger scanClockReads = new AtomicInteger();
            fixture.workClasses.submit(new WorkClassTask(WorkClass.GC, "occupied", 1), () -> {});

            assertThrows(IllegalStateException.class, () -> fixture.executor()
                    .submit(
                            new SchedulerBudget(1, 8_192, 1_000),
                            () -> {
                                ownerClockReads.incrementAndGet();
                                return 101;
                            },
                            () -> {
                                scanClockReads.incrementAndGet();
                                return 0;
                            }));
            assertEquals(0, ownerClockReads.get());
            assertEquals(0, scanClockReads.get());
            assertEquals(1, fixture.shard.quota().reservationMessages());
            assertNotNull(fixture.store.getValue(
                    ColumnFamily.TIMELINE, KeyCodec.reservationExpiry(5_000, fixture.reservationId), 5));
        }
    }

    @Test
    void undersizedCandidateEnvelopeFailsClosedAndFencesOwner() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("undersized"), 1)) {
            final ReservationExpiryDiscoveryWorkClassExecutor.Submission submission =
                    fixture.executor().submit(new SchedulerBudget(1, 1, 1_000), () -> 101, () -> 0);

            assertThrows(IllegalStateException.class, () -> fixture.workClasses.runTurn(fixture.turnBudget()));
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
            assertTrue(submission.discovered().isEmpty());
            assertEquals(
                    WorkClassExecutionRegistry.ExecutionState.FAILED,
                    fixture.workClasses.state(submission.task()).orElseThrow());
        }
    }

    @Test
    void expiredOwnerFencesBeforeScanClockIsRead() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("expired-owner"), 1)) {
            final AtomicInteger scanClockReads = new AtomicInteger();
            final ReservationExpiryDiscoveryWorkClassExecutor.Submission submission = fixture.executor()
                    .submit(new SchedulerBudget(1, 8_192, 1_000), () -> 200, () -> {
                        scanClockReads.incrementAndGet();
                        return 0;
                    });

            assertThrows(IllegalStateException.class, () -> fixture.workClasses.runTurn(fixture.turnBudget()));
            assertEquals(0, scanClockReads.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
            assertTrue(submission.discovered().isEmpty());
        }
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
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), () -> 0);
    }

    private static byte[] timeFenceBody(
            final ShardId shard,
            final long closeThrough,
            final int keyVersion,
            final byte[] proofId,
            final byte[] proof) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, closeThrough);
            CanonicalProtobuf.uint32(output, 11, keyVersion);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shardId = new ShardId(RouteIncarnation.random(), 57);
        private final UUID topic = UUID.randomUUID();
        private final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        private final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final DelayShard shard;
        private final OwnedDelayShard owned;
        private final WorkClassExecutionRegistry workClasses;
        private final byte[] reservationId;

        private Fixture(final Path root, final int maxQueueRecords) throws Exception {
            final SourceAssignment assignment = new SourceAssignment(
                    shardId,
                    Bytes.sha256(Bytes.utf8("reservation-expiry-discovery-assignment")),
                    1,
                    new KafkaActivationBarrier(shardId, "reservation-expiry-discovery-cluster", topic, 0));
            final OwnerLease lease = backend.acquire(
                            assignment,
                            "reservation-expiry-discovery-owner",
                            Bytes.sha256(Bytes.utf8("reservation-expiry-discovery-session")),
                            100,
                            100)
                    .orElseThrow();
            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shardId, resources);
            shard = new DelayShard(store, new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4, 3, 100, 10_000));
            final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("reservation-expiry-discovery-lane"));
            final LargeScheduleIntent intent = new LargeScheduleIntent(
                    lane,
                    2_000,
                    5_000,
                    OrderingMode.BEST_EFFORT,
                    8,
                    Bytes.sha256(Bytes.utf8("reservation-expiry-discovery-payload")),
                    4_000,
                    9);
            final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
            reservationId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-reservation-id\0"),
                    prepare.commandId().bytes(),
                    prepare.delayMessageId().bytes(),
                    prepare.commandHash());
            shard.apply(prepare, position(0, 1_000));
            final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(
                    5_000,
                    5_000,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                    Bytes.utf8("reservation-expiry-discovery-clock"),
                    1,
                    1,
                    1,
                    Bytes.sha256(Bytes.utf8("reservation-expiry-discovery-proof")),
                    0,
                    null);
            final byte[] proofId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-time-fence-proof\0"),
                    shardId.routeIncarnation().bytes(),
                    Bytes.u32be(shardId.partition()),
                    Bytes.i64be(5_000),
                    Bytes.u32be(1),
                    Bytes.lp32(proof.canonicalBytes()));
            final SystemMutation fence = SystemMutation.signed(
                    shardId,
                    SystemMutationType.TIME_FENCE,
                    9_000,
                    proofId,
                    timeFenceBody(shardId, 5_000, 1, proofId, proof.canonicalBytes()),
                    AuthorIdentity.fence(Bytes.utf8("reservation-expiry-discovery-fence"), 1)
                            .canonicalBytes(),
                    1,
                    keyPair.getPrivate());
            shard.applySystemMutation(fence, position(1, 1_001), keyPair.getPublic());
            owned = new OwnedDelayShard(shard, lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            workClasses = workClasses(maxQueueRecords);
        }

        private ReservationExpiryDiscoveryWorkClassExecutor executor() {
            return new ReservationExpiryDiscoveryWorkClassExecutor(workClasses, owned, authority);
        }

        private KafkaSourcePosition position(final long offset, final long brokerTime) {
            return new KafkaSourcePosition(
                    shardId, "reservation-expiry-discovery-cluster", topic, offset, null, brokerTime);
        }

        private SchedulerBudget turnBudget() {
            return new SchedulerBudget(1, 1_000_000, 1_000);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
