package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationBodyCodec;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExpiryWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void appendsExactSignedMutationWithoutApplyingExpiryLocally() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("persisted"))) {
            final AtomicReference<SystemMutation> appended = new AtomicReference<>();
            final AtomicInteger appendCalls = new AtomicInteger();
            final KafkaSourcePosition expiryPosition = fixture.position(1, 5_100);
            final ExpiryWorkClassExecutor executor = fixture.executor(mutation -> {
                appended.set(mutation);
                appendCalls.incrementAndGet();
                return ShardLogMutationAppender.AppendOutcome.persisted(expiryPosition);
            });

            final ExpiryWorkClassExecutor.Submission submission = fixture.submit(executor);
            assertTrue(submission.result().isEmpty());
            fixture.workClasses.runTurn(fixture.budget());

            final ExpiryWorkClassExecutor.ExpiryHandoffResult result =
                    submission.result().orElseThrow();
            assertEquals(ExpiryWorkClassExecutor.ResultKind.ENQUEUED, result.kind());
            assertEquals(expiryPosition, result.sourcePosition());
            assertEquals(1, appendCalls.get());
            assertEquals(submission.mutation(), appended.get());
            assertEquals(
                    SystemMutationType.EXPIRE_GENERATION, submission.mutation().type());
            assertTrue(submission.mutation().verifySignature(fixture.keyPair.getPublic()));
            assertEquals(
                    fixture.candidate.messageId(),
                    new com.nereusstream.delay.protocol.DelayMessageId(
                            SystemMutationBodyCodec.fields(
                                            SystemMutationType.EXPIRE_GENERATION,
                                            submission.mutation().canonicalBody())
                                    .stream()
                                    .filter(field -> field.number() == 10)
                                    .findFirst()
                                    .orElseThrow()
                                    .rawValue()));
            assertEquals(
                    MessageStatus.SCHEDULED,
                    fixture.shard.getMessage(fixture.candidate.messageId()).status());
            assertEquals(0, fixture.workClasses.registeredActions());
        }
    }

    @Test
    void definitiveNonPersistenceDoesNotChangeShardOrFenceOwner() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("not-persisted"))) {
            final ExpiryWorkClassExecutor executor =
                    fixture.executor(ignored -> ShardLogMutationAppender.AppendOutcome.definitelyNotPersisted());
            final ExpiryWorkClassExecutor.Submission submission = fixture.submit(executor);

            fixture.workClasses.runTurn(fixture.budget());

            assertEquals(
                    ExpiryWorkClassExecutor.ResultKind.DEFINITIVELY_NOT_ENQUEUED,
                    submission.result().orElseThrow().kind());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, fixture.owned.state());
            assertEquals(
                    MessageStatus.SCHEDULED,
                    fixture.shard.getMessage(fixture.candidate.messageId()).status());
        }
    }

    @Test
    void unknownAppendRetainsExactMutationAndDoesNotPretendItWasRejected() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("unknown"))) {
            final ExpiryWorkClassExecutor executor =
                    fixture.executor(ignored -> ShardLogMutationAppender.AppendOutcome.unknown());
            final ExpiryWorkClassExecutor.Submission submission = fixture.submit(executor);

            fixture.workClasses.runTurn(fixture.budget());

            final ExpiryWorkClassExecutor.ExpiryHandoffResult result =
                    submission.result().orElseThrow();
            assertEquals(ExpiryWorkClassExecutor.ResultKind.UNKNOWN, result.kind());
            assertEquals(submission.mutation(), result.mutation());
            assertTrue(result.failure() == null);
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, fixture.owned.state());
        }
    }

    @Test
    void queueRejectionDoesNotApplyOrAllocateAExpiryPosition() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("rejected"), 1)) {
            final AtomicInteger appendCalls = new AtomicInteger();
            final ExpiryWorkClassExecutor executor = fixture.executor(ignored -> {
                appendCalls.incrementAndGet();
                return ShardLogMutationAppender.AppendOutcome.unknown();
            });
            fixture.workClasses.submit(new WorkClassTask(WorkClass.EXPIRY, "occupied", 1), () -> {});

            assertThrows(IllegalStateException.class, () -> fixture.submit(executor));
            assertEquals(0, appendCalls.get());
            assertEquals(1, fixture.workClasses.registeredActions());
            assertEquals(
                    MessageStatus.SCHEDULED,
                    fixture.shard.getMessage(fixture.candidate.messageId()).status());
            assertEquals(fixture.position(0, 1_000), fixture.owned.shard().lastAppliedSourcePosition());
        }
    }

    @Test
    void appendFailureFencesOwnerAndReturnsUnknownEvidence() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("failure"))) {
            final RuntimeException failure = new RuntimeException("append failed");
            final ExpiryWorkClassExecutor executor = fixture.executor(ignored -> {
                throw failure;
            });
            final ExpiryWorkClassExecutor.Submission submission = fixture.submit(executor);

            fixture.workClasses.runTurn(fixture.budget());

            final ExpiryWorkClassExecutor.ExpiryHandoffResult result =
                    submission.result().orElseThrow();
            assertEquals(ExpiryWorkClassExecutor.ResultKind.UNKNOWN, result.kind());
            assertEquals(failure, result.failure());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
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

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(
                5_000,
                5_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("expiry-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("expiry-proof")),
                0,
                null);
    }

    private static OwnerIdentity owner(final long epoch) {
        return new OwnerIdentity(
                Bytes.utf8("expiry-deployment"),
                Bytes.utf8("expiry-worker"),
                epoch,
                Bytes.sha256(Bytes.utf8("expiry-owner-fence")));
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shardId = new ShardId(RouteIncarnation.random(), 37);
        private final UUID topic = UUID.randomUUID();
        private final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        private final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        private final OwnerLease lease;
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final DelayShard shard;
        private final OwnedDelayShard owned;
        private final OwnerIdentity owner;
        private final WorkClassExecutionRegistry workClasses;
        private final KeyPair keyPair;
        private final DelayShard.ExpiryWork candidate;

        private Fixture(final Path root) throws Exception {
            this(root, 1);
        }

        private Fixture(final Path root, final int maxQueueRecords) throws Exception {
            final SourceAssignment assignment = new SourceAssignment(
                    shardId,
                    Bytes.sha256(Bytes.utf8("expiry-assignment")),
                    1,
                    new KafkaActivationBarrier(shardId, "expiry-cluster", topic, 0));
            lease = backend.acquire(assignment, "expiry-owner", Bytes.sha256(Bytes.utf8("expiry-session")), 100, 100)
                    .orElseThrow();
            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shardId, resources);
            shard = new DelayShard(store, DelayShardConfig.defaults());
            final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("expiry-lane"));
            final PreparedCommand schedule = PreparedCommand.schedule(
                    shardId,
                    new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("expiry-payload")),
                    9_000);
            shard.apply(schedule, position(0, 1_000));
            candidate = com.nereusstream.delay.runtime.DelayShardTestSupport.discoverExpiry(shard, 5_000, 1)
                    .get(0);
            owner = owner(lease.ownerEpoch());
            owned = new OwnedDelayShard(shard, lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            workClasses = workClasses(maxQueueRecords);
            keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        }

        private ExpiryWorkClassExecutor executor(final ShardLogMutationAppender appender) {
            return new ExpiryWorkClassExecutor(workClasses, owned, authority, appender);
        }

        private ExpiryWorkClassExecutor.Submission submit(final ExpiryWorkClassExecutor executor) {
            return executor.submit(candidate, evidence(), 9_000, owner, 1, keyPair.getPrivate(), () -> 101);
        }

        private KafkaSourcePosition position(final long offset, final long brokerTime) {
            return new KafkaSourcePosition(shardId, "expiry-cluster", topic, offset, null, brokerTime);
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
