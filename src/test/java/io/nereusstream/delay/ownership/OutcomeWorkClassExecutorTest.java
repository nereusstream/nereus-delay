package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutcomeWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsExactPreparedMutationWithoutApplyingItLocally() throws Exception {
        final Fixture fixture = fixture("persisted");
        try (fixture) {
            final SystemMutation mutation = mutation(fixture, 1);
            final KafkaSourcePosition persistedPosition = fixture.position(1);
            final AtomicReference<SystemMutation> appended = new AtomicReference<>();
            final OutcomeWorkClassExecutor executor = new OutcomeWorkClassExecutor(fixture.workClasses,
                    fixture.owned, fixture.authority, value -> {
                        appended.set(value);
                        return ShardLogMutationAppender.AppendOutcome.persisted(persistedPosition);
                    });

            final OutcomeWorkClassExecutor.Submission submission = executor.submit(mutation, () -> 101);
            assertEquals(WorkClass.OUTCOME_AND_CONTROL, submission.task().workClass());
            assertEquals(mutation.encodeFrame().length, submission.task().bytes());
            assertTrue(submission.result().isEmpty());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final OutcomeWorkClassExecutor.OutcomeHandoffResult result = submission.result().orElseThrow();
            assertEquals(OutcomeWorkClassExecutor.ResultKind.PERSISTED, result.kind());
            assertEquals(persistedPosition, result.sourcePosition());
            assertArrayEquals(mutation.encodeFrame(), appended.get().encodeFrame());
            // The mutation is not source-applied by this bridge.
            assertEquals(null, fixture.owned.shard().getSystemMutationResult(mutation.systemMutationId()));
        }
    }

    @Test
    void preservesDefinitiveAndUnknownAppendOutcomes() throws Exception {
        final Fixture fixture = fixture("tri-state");
        try (fixture) {
            final AtomicInteger calls = new AtomicInteger();
            final AtomicReference<ShardLogMutationAppender.AppendOutcome> appendOutcome = new AtomicReference<>(
                    ShardLogMutationAppender.AppendOutcome.definitelyNotPersisted());
            final OutcomeWorkClassExecutor executor = new OutcomeWorkClassExecutor(fixture.workClasses,
                    fixture.owned, fixture.authority, value -> {
                        calls.incrementAndGet();
                        return appendOutcome.get();
                    });

            final SystemMutation notPersisted = mutation(fixture, 2);
            final OutcomeWorkClassExecutor.Submission first = executor.submit(notPersisted, () -> 101);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(OutcomeWorkClassExecutor.ResultKind.DEFINITIVELY_NOT_PERSISTED,
                    first.result().orElseThrow().kind());

            appendOutcome.set(ShardLogMutationAppender.AppendOutcome.unknown());
            final SystemMutation unknown = mutation(fixture, 3);
            final OutcomeWorkClassExecutor.Submission second = executor.submit(unknown, () -> 101);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            final OutcomeWorkClassExecutor.OutcomeHandoffResult secondResult = second.result().orElseThrow();
            assertEquals(OutcomeWorkClassExecutor.ResultKind.UNKNOWN, secondResult.kind());
            assertEquals(unknown, secondResult.mutation());
            assertEquals(2, calls.get());
        }
    }

    @Test
    void queueRejectionAndExpiredOwnerHaveNoFalsePersistenceProof() throws Exception {
        final Fixture fixture = fixture("rejection");
        try (fixture) {
            fixture.workClasses.submit(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "occupied", 1), () -> {
            });
            final AtomicInteger appendCalls = new AtomicInteger();
            final OutcomeWorkClassExecutor executor = new OutcomeWorkClassExecutor(fixture.workClasses,
                    fixture.owned, fixture.authority, value -> {
                        appendCalls.incrementAndGet();
                        return ShardLogMutationAppender.AppendOutcome.unknown();
                    });
            assertThrows(IllegalStateException.class,
                    () -> executor.submit(mutation(fixture, 4), () -> 101));
            assertEquals(0, appendCalls.get());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final OutcomeWorkClassExecutor.Submission expired = executor.submit(mutation(fixture, 5), () -> 250);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(OutcomeWorkClassExecutor.ResultKind.UNKNOWN, expired.result().orElseThrow().kind());
            assertEquals(0, appendCalls.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    private Fixture fixture(final String name) throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 41);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("outcome-assignment-" + name)), 7,
                new KafkaActivationBarrier(shard, "outcome-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "outcome-owner",
                Bytes.sha256(Bytes.utf8("outcome-session-" + name)), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve(name + "-store"));
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shard, resources);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("outcome-deployment"),
                Bytes.utf8("outcome-worker"), lease.ownerEpoch(),
                Bytes.sha256(Bytes.utf8("outcome-fence")));
        final OwnedDelayShard owned = new OwnedDelayShard(
                new DelayShard(store, DelayShardConfig.defaults()), lease, owner);
        owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
        owned.recordCatchup(new KafkaSourcePosition(shard, "outcome-cluster", topic, 0, null, 1_000));
        owned.activateForCommands(authority, 101);
        return new Fixture(shard, topic, lease, owner, authority, owned, workClasses(1), resources, store);
    }

    private static SystemMutation mutation(final Fixture fixture, final int identity) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] logicalIdentity = Bytes.sha256(Bytes.utf8("outcome-logical-" + identity));
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, fixture.shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, fixture.shard.partition());
        });
        final byte[] nested = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, logicalIdentity);
            CanonicalProtobuf.uint32(output, 11, 3);
            CanonicalProtobuf.uint32(output, 12, 4);
            CanonicalProtobuf.uint32(output, 13, 13);
            CanonicalProtobuf.bytes(output, 15, nested);
            CanonicalProtobuf.bytes(output, 16, nested);
            CanonicalProtobuf.bytes(output, 17, nested);
        });
        final AuthorIdentity author = AuthorIdentity.owner(fixture.owner.deploymentId(),
                fixture.owner.workerRunId(), fixture.owner.ownerEpoch(), fixture.owner.leaseFencingDigest());
        return SystemMutation.signed(fixture.shard, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                logicalIdentity, body, author.canonicalBytes(), 1, keyPair.getPrivate());
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 1_000_000,
                    maxQueueRecords, 1_000_000, 1_000, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }

    private record Fixture(ShardId shard, UUID topic, OwnerLease lease, OwnerIdentityV1 owner,
                           OxiaOwnerLeaseStore authority,
                           OwnedDelayShard owned, WorkClassExecutionRegistry workClasses,
                           SharedRocksDbResources resources, ShardStore store) implements AutoCloseable {
        private KafkaSourcePosition position(final long offset) {
            return new KafkaSourcePosition(shard, "outcome-cluster", topic, offset, null, 1_000 + offset);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
