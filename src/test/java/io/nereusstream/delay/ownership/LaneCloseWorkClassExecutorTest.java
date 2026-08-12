package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.LaneCloseMaterializationCursor;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LaneCloseWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void materializesOneExactCloseCursorAfterOwnerFence() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("success"))) {
            final LaneCloseWorkClassExecutor.Submission submission = fixture.executor()
                    .submit(fixture.candidate, 1, () -> 101);

            fixture.workClasses.runTurn(fixture.budget());

            final LaneCloseWorkClassExecutor.Outcome outcome = submission.outcome().orElseThrow();
            assertEquals(DelayShard.LaneCloseMaterializationExecutionResult.Kind.MATERIALIZED,
                    outcome.result().kind());
            assertEquals(1, outcome.result().result().materializedMessages());
            assertEquals(MessageStatus.DEAD_LETTER, fixture.shard.getMessage(fixture.message.delayMessageId()).status());
            assertEquals(AdmissionGate.CLOSED, fixture.shard.getLane(fixture.lane).admissionGate());
            assertTrue(fixture.shard.getLaneCloseCursor(fixture.lane).phase()
                    == LaneCloseMaterializationCursor.Phase.RESERVATIONS);
        }
    }

    @Test
    void queueRejectionDoesNotAdvanceCloseCursor() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("rejected"), 1)) {
            fixture.workClasses.submit(new WorkClassTask(WorkClass.GC, "occupied", 1), () -> {
            });

            assertThrows(IllegalStateException.class,
                    () -> fixture.executor().submit(fixture.candidate, 1, () -> 101));
            assertEquals(1, fixture.workClasses.registeredActions());
            assertArrayEquals(fixture.candidate.cursor().canonicalBytes(),
                    fixture.shard.getLaneCloseCursor(fixture.lane).canonicalBytes());
            assertEquals(MessageStatus.SCHEDULED, fixture.shard.getMessage(fixture.message.delayMessageId()).status());
        }
    }

    @Test
    void staleCursorIsReportedWithoutApplyingTheQueuedCandidateAgain() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("stale"))) {
            fixture.shard.materializeClosedLane(fixture.lane, 1);
            final LaneCloseWorkClassExecutor.Submission submission = fixture.executor()
                    .submit(fixture.candidate, 1, () -> 101);

            fixture.workClasses.runTurn(fixture.budget());

            final LaneCloseWorkClassExecutor.Outcome outcome = submission.outcome().orElseThrow();
            assertEquals(DelayShard.LaneCloseMaterializationExecutionResult.Kind.STALE,
                    outcome.result().kind());
            assertEquals(LaneCloseMaterializationCursor.Phase.RESERVATIONS,
                    fixture.shard.getLaneCloseCursor(fixture.lane).phase());
        }
    }

    @Test
    void expiredOwnerFencesBeforeCloseMaterialization() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("expired"))) {
            final LaneCloseWorkClassExecutor.Submission submission = fixture.executor()
                    .submit(fixture.candidate, 1, () -> 200);

            fixture.workClasses.runTurn(fixture.budget());

            final LaneCloseWorkClassExecutor.Outcome outcome = submission.outcome().orElseThrow();
            assertTrue(outcome.result() == null);
            assertEquals("shard owner lease is not active", outcome.failure().getMessage());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
            assertArrayEquals(fixture.candidate.cursor().canonicalBytes(),
                    fixture.shard.getLaneCloseCursor(fixture.lane).canonicalBytes());
        }
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 1_000_000,
                    maxQueueRecords, 1_000_000, 1_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }

    private static byte[] closeBody(final ShardId shard, final ControlRef controlRef,
                                    final DestinationLaneId lane, final byte[] laneIncarnation,
                                    final long expectedControlVersion) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] laneTarget = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, lane.bytes());
            CanonicalProtobuf.bytes(output, 2, laneIncarnation);
            CanonicalProtobuf.int64(output, 3, expectedControlVersion);
        });
        final byte[] reason = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
        final byte[] scope = Bytes.sha256(Bytes.utf8("lane-close-work-ack-scope"));
        final byte[] possibleDuplicate = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("lane-close-work-possible")));
            CanonicalProtobuf.bytes(output, 3, scope);
        });
        final byte[] orderLoss = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 3);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("lane-close-work-order")));
            CanonicalProtobuf.bytes(output, 3, scope);
        });
        final byte[] acknowledgements = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, possibleDuplicate);
            CanonicalProtobuf.bytes(output, 1, orderLoss);
        });
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, laneTarget);
            CanonicalProtobuf.bytes(output, 2, reason);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint32(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, acknowledgements);
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 11, branch));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 11);
            CanonicalProtobuf.uint32(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("lane-close-work-semantic")));
            CanonicalProtobuf.int64(output, 14, expectedControlVersion);
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shardId = new ShardId(RouteIncarnation.random(), 73);
        private final UUID topic = UUID.randomUUID();
        private final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        private final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        private final OwnerLease lease;
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final DelayShard shard;
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-close-work-lane"));
        private final PreparedCommand message;
        private final OwnedDelayShard owned;
        private final WorkClassExecutionRegistry workClasses;
        private final DelayShard.LaneCloseMaterializationWork candidate;

        private Fixture(final Path root) throws Exception {
            this(root, 1);
        }

        private Fixture(final Path root, final int maxQueueRecords) throws Exception {
            final SourceAssignment assignment = new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("lane-close-work-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "lane-close-work-cluster", topic, 0));
            lease = backend.acquire(assignment, "lane-close-work-owner",
                    Bytes.sha256(Bytes.utf8("lane-close-work-session")), 100, 100).orElseThrow();
            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shardId, resources);
            shard = new DelayShard(store, io.nereusstream.delay.runtime.DelayShardConfig.defaults());
            message = PreparedCommand.schedule(shardId,
                    new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("lane-close-work-message")), 9_000);
            shard.apply(message, position(0, 1_000));
            final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final io.nereusstream.delay.runtime.LaneRecord beforeClose = shard.getLane(lane);
            final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("lane-close-work-op")),
                    Bytes.sha256(Bytes.utf8("lane-close-work-request")), 1);
            final SystemMutation close = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000,
                    controlRef.logicalOperationIdentity(11),
                    closeBody(shardId, controlRef, lane, beforeClose.laneIncarnation(),
                            beforeClose.laneControlVersion()),
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("lane-close-work-actor")),
                            Bytes.sha256(Bytes.utf8("lane-close-work-roles")),
                            Bytes.sha256(Bytes.utf8("lane-close-work-scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());
            shard.applySystemMutation(close, position(1, 1_001), keyPair.getPublic());
            candidate = shard.discoverLaneCloseMaterialization(1).get(0);
            owned = new OwnedDelayShard(shard, lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            workClasses = workClasses(maxQueueRecords);
        }

        private LaneCloseWorkClassExecutor executor() {
            return new LaneCloseWorkClassExecutor(workClasses, owned, authority);
        }

        private KafkaSourcePosition position(final long offset, final long brokerTime) {
            return new KafkaSourcePosition(shardId, "lane-close-work-cluster", topic, offset, null, brokerTime);
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
