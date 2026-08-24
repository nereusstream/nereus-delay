package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import com.nereusstream.delay.runtime.PayloadReservation;
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
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReservationExpiryWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void materializesOnlyAfterExecutionTimeOwnerFenceAndReleasesQuota() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("materialized"))) {
            final ReservationExpiryWorkClassExecutor executor = fixture.executor();
            final ReservationExpiryWorkClassExecutor.Submission submission = fixture.submit(executor);

            fixture.workClasses.runTurn(fixture.budget());

            final ReservationExpiryWorkClassExecutor.Outcome outcome =
                    submission.outcome().orElseThrow();
            assertEquals(
                    DelayShard.ReservationExpiryMaterializationResult.Kind.MATERIALIZED,
                    outcome.result().kind());
            assertEquals(
                    PayloadReservationStatus.EXPIRED,
                    outcome.result().reservation().status());
            assertEquals(0, fixture.shard.quota().reservationMessages());
            assertEquals(
                    PayloadReservationStatus.EXPIRED,
                    fixture.shard.getReservation(fixture.reservationId).status());
            assertNull(fixture.store.getValue(
                    ColumnFamily.TIMELINE, KeyCodec.reservationExpiry(5_000, fixture.reservationId), 5));
        }
    }

    @Test
    void queueRejectionDoesNotMaterializeOrReleaseReservation() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("rejected"), 1)) {
            final ReservationExpiryWorkClassExecutor executor = fixture.executor();
            fixture.workClasses.submit(new WorkClassTask(WorkClass.GC, "occupied", 1), () -> {});

            assertThrows(IllegalStateException.class, () -> fixture.submit(executor));
            assertEquals(1, fixture.workClasses.registeredActions());
            assertEquals(1, fixture.shard.quota().reservationMessages());
            assertEquals(
                    PayloadReservationStatus.EXPIRED,
                    fixture.shard.getReservation(fixture.reservationId).status());
        }
    }

    @Test
    void staleCandidateIsReportedWithoutApplyingToTheNewReservationProjection() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("stale"))) {
            final PayloadReservation current = fixture.shard.getReservation(fixture.reservationId);
            final PayloadReservation changed = current.withLifecycle(
                    PayloadReservationStatus.ABANDONED,
                    Math.addExact(current.stateVersion(), 1),
                    current.sourcePosition(),
                    null);
            fixture.store.write(batch -> batch.putValue(
                    ColumnFamily.ID, 2, KeyCodec.idReservation(fixture.reservationId), changed.encode()));

            final ReservationExpiryWorkClassExecutor.Submission submission = fixture.submit(fixture.executor());
            fixture.workClasses.runTurn(fixture.budget());

            final ReservationExpiryWorkClassExecutor.Outcome outcome =
                    submission.outcome().orElseThrow();
            assertEquals(
                    DelayShard.ReservationExpiryMaterializationResult.Kind.STALE,
                    outcome.result().kind());
            assertEquals(
                    PayloadReservationStatus.ABANDONED,
                    fixture.shard.getReservation(fixture.reservationId).status());
            assertEquals(1, fixture.shard.quota().reservationMessages());
        }
    }

    @Test
    void expiredOwnerFencesBeforeMaterialization() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("owner-expired"))) {
            final ReservationExpiryWorkClassExecutor.Submission submission =
                    fixture.executor().submit(fixture.candidate, () -> 200);
            fixture.workClasses.runTurn(fixture.budget());

            final ReservationExpiryWorkClassExecutor.Outcome outcome =
                    submission.outcome().orElseThrow();
            assertTrue(outcome.result() == null);
            assertEquals("shard owner lease is not active", outcome.failure().getMessage());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
            assertEquals(1, fixture.shard.quota().reservationMessages());
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
        private final ShardId shardId = new ShardId(RouteIncarnation.random(), 52);
        private final UUID topic = UUID.randomUUID();
        private final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        private final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        private final OwnerLease lease;
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final DelayShard shard;
        private final OwnedDelayShard owned;
        private final WorkClassExecutionRegistry workClasses;
        private final byte[] reservationId;
        private final DelayShard.ReservationExpiryWork candidate;
        private final KeyPair keyPair;

        private Fixture(final Path root) throws Exception {
            this(root, 1);
        }

        private Fixture(final Path root, final int maxQueueRecords) throws Exception {
            final SourceAssignment assignment = new SourceAssignment(
                    shardId,
                    Bytes.sha256(Bytes.utf8("reservation-expiry-assignment")),
                    1,
                    new KafkaActivationBarrier(shardId, "reservation-expiry-cluster", topic, 0));
            lease = backend.acquire(
                            assignment,
                            "reservation-expiry-owner",
                            Bytes.sha256(Bytes.utf8("reservation-expiry-session")),
                            100,
                            100)
                    .orElseThrow();
            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shardId, resources);
            final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4, 3, 100, 10_000);
            shard = new DelayShard(store, shardConfig);
            final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("reservation-expiry-lane"));
            final LargeScheduleIntent intent = new LargeScheduleIntent(
                    lane,
                    2_000,
                    5_000,
                    OrderingMode.BEST_EFFORT,
                    8,
                    Bytes.sha256(Bytes.utf8("reservation-expiry-payload")),
                    4_000,
                    9);
            final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
            reservationId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                    prepare.commandId().bytes(),
                    prepare.delayMessageId().bytes(),
                    prepare.commandHash());
            shard.apply(prepare, position(0, 1_000));
            keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(
                    5_000,
                    5_000,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                    Bytes.utf8("reservation-expiry-clock"),
                    1,
                    1,
                    1,
                    Bytes.sha256(Bytes.utf8("reservation-expiry-proof")),
                    0,
                    null);
            final byte[] proofId = Bytes.sha256(
                    Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
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
                    AuthorIdentity.fence(Bytes.utf8("reservation-expiry-fence"), 1)
                            .canonicalBytes(),
                    1,
                    keyPair.getPrivate());
            shard.applySystemMutation(fence, position(1, 1_001), keyPair.getPublic());
            candidate = com.nereusstream.delay.runtime.DelayShardTestSupport.discoverReservationExpiry(shard, 5_000, 10)
                    .get(0);
            owned = new OwnedDelayShard(shard, lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            workClasses = workClasses(maxQueueRecords);
        }

        private ReservationExpiryWorkClassExecutor executor() {
            return new ReservationExpiryWorkClassExecutor(workClasses, owned, authority);
        }

        private ReservationExpiryWorkClassExecutor.Submission submit(
                final ReservationExpiryWorkClassExecutor executor) {
            return executor.submit(candidate, () -> 101);
        }

        private KafkaSourcePosition position(final long offset, final long brokerTime) {
            return new KafkaSourcePosition(shardId, "reservation-expiry-cluster", topic, offset, null, brokerTime);
        }

        private SchedulerBudget budget() {
            return new SchedulerBudget(1, 1_000_000, 1_000);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
