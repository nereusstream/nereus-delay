package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
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

class ControlWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void persistsExactTimeFenceWithoutApplyingItLocally() throws Exception {
        final Fixture fixture = fixture("persisted");
        try (fixture) {
            final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final SystemMutation mutation = timeFence(fixture, keyPair, 2_000);
            final KafkaSourcePosition persistedPosition = fixture.position(1);
            final AtomicReference<SystemMutation> appended = new AtomicReference<>();
            final ControlWorkClassExecutor executor =
                    new ControlWorkClassExecutor(fixture.workClasses, fixture.owned, fixture.authority, value -> {
                        appended.set(value);
                        return ShardLogMutationAppender.AppendOutcome.persisted(persistedPosition);
                    });

            final ControlWorkClassExecutor.Submission submission = executor.submit(mutation, () -> 101);
            assertEquals(WorkClass.OUTCOME_AND_CONTROL, submission.task().workClass());
            assertEquals(mutation.encodeFrame().length, submission.task().bytes());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final ControlWorkClassExecutor.ControlHandoffResult result =
                    submission.result().orElseThrow();
            assertEquals(ControlWorkClassExecutor.ResultKind.PERSISTED, result.kind());
            assertEquals(persistedPosition, result.sourcePosition());
            assertArrayEquals(mutation.encodeFrame(), appended.get().encodeFrame());
            assertNull(fixture.owned.shard().getSystemMutationResult(mutation.systemMutationId()));
        }
    }

    @Test
    void queueRejectionAndExpiredOwnerDoNotCallControlAppender() throws Exception {
        final Fixture fixture = fixture("fencing");
        try (fixture) {
            final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final AtomicInteger calls = new AtomicInteger();
            final ControlWorkClassExecutor executor =
                    new ControlWorkClassExecutor(fixture.workClasses, fixture.owned, fixture.authority, value -> {
                        calls.incrementAndGet();
                        return ShardLogMutationAppender.AppendOutcome.unknown();
                    });
            fixture.workClasses.submit(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "occupied", 1), () -> {});
            final SystemMutation mutation = timeFence(fixture, keyPair, 2_000);
            assertThrows(IllegalStateException.class, () -> executor.submit(mutation, () -> 101));
            assertEquals(0, calls.get());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final ControlWorkClassExecutor.Submission expired = executor.submit(mutation, () -> 250);
            fixture.workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(
                    ControlWorkClassExecutor.ResultKind.UNKNOWN,
                    expired.result().orElseThrow().kind());
            assertEquals(0, calls.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    private Fixture fixture(final String name) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 52);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("control-assignment-" + name)),
                7,
                new KafkaActivationBarrier(shard, "control-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(
                        assignment, "control-owner", Bytes.sha256(Bytes.utf8("control-session-" + name)), 100, 100)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve(name + "-store"));
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shard, resources);
        final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
        owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
        owned.recordCatchup(new KafkaSourcePosition(shard, "control-cluster", topic, 0, null, 1_000));
        owned.activateForCommands(authority, 101);
        return new Fixture(shard, topic, authority, owned, workClasses(), resources, store);
    }

    private static SystemMutation timeFence(final Fixture fixture, final KeyPair keyPair, final long closeThrough) {
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(
                closeThrough + 1,
                closeThrough + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("control-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("control-proof")),
                0,
                null);
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                fixture.shard.routeIncarnation().bytes(),
                Bytes.u32beBits(fixture.shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(1),
                Bytes.lp32(proof.canonicalBytes()));
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, fixture.shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, fixture.shard.partition());
        });
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, closeThrough);
            CanonicalProtobuf.uint32(output, 11, 1);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof.canonicalBytes());
        });
        return SystemMutation.signed(
                fixture.shard,
                SystemMutationType.TIME_FENCE,
                9_000,
                proofId,
                body,
                AuthorIdentity.fence(Bytes.utf8("control-fence"), 1).canonicalBytes(),
                1,
                keyPair.getPrivate());
    }

    private static WorkClassExecutionRegistry workClasses() {
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
                            1,
                            1_000_000,
                            1,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), () -> 0);
    }

    private record Fixture(
            ShardId shard,
            UUID topic,
            OxiaOwnerLeaseStore authority,
            OwnedDelayShard owned,
            WorkClassExecutionRegistry workClasses,
            SharedRocksDbResources resources,
            ShardStore store)
            implements AutoCloseable {
        private KafkaSourcePosition position(final long offset) {
            return new KafkaSourcePosition(shard, "control-cluster", topic, offset, null, 1_000 + offset);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
