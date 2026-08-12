package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
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
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void successfulQueryUsesReadOnlyClassAndRechecksOwnerAfterRead() {
        final Fixture fixture = fixture("success");
        try (fixture) {
            final AtomicInteger reads = new AtomicInteger();
            final QueryWorkClassExecutor.Submission<String> submission = executor(fixture).submit(
                    request(fixture.shard, "command-result"), () -> 101,
                    now -> {
                        reads.incrementAndGet();
                        return "snapshot@" + now;
                    });

            assertEquals(WorkClass.QUERY, submission.task().workClass());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 10_000, 1_000));
            final QueryWorkClassExecutor.QueryResult<String> result = submission.result().orElseThrow();
            assertEquals(QueryWorkClassExecutor.ResultKind.COMPLETED, result.kind());
            assertEquals("snapshot@101", result.value());
            assertEquals(1, reads.get());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, fixture.owned.state());
        }
    }

    @Test
    void ownershipLossDuringReadDiscardsSnapshotAndReturnsTransition() {
        final Fixture fixture = fixture("ownership-loss");
        try (fixture) {
            final AtomicInteger reads = new AtomicInteger();
            final QueryWorkClassExecutor.Submission<String> submission = executor(fixture).submit(
                    request(fixture.shard, "message"), () -> 101,
                    now -> {
                        reads.incrementAndGet();
                        fixture.backend.release(fixture.lease);
                        return "must-not-escape";
                    });

            fixture.workClasses.runTurn(new SchedulerBudget(1, 10_000, 1_000));
            final QueryWorkClassExecutor.QueryResult<String> result = submission.result().orElseThrow();
            assertEquals(QueryWorkClassExecutor.ResultKind.SHARD_TRANSITIONING, result.kind());
            assertEquals(1, reads.get());
            assertEquals(null, result.value());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    @Test
    void expiredOwnerDoesNotInvokeReadOperation() {
        final Fixture fixture = fixture("expired");
        try (fixture) {
            final AtomicInteger reads = new AtomicInteger();
            final QueryWorkClassExecutor.Submission<String> submission = executor(fixture).submit(
                    request(fixture.shard, "expired"), () -> 250,
                    now -> {
                        reads.incrementAndGet();
                        return "unexpected";
                    });

            fixture.workClasses.runTurn(new SchedulerBudget(1, 10_000, 1_000));
            assertEquals(QueryWorkClassExecutor.ResultKind.SHARD_TRANSITIONING,
                    submission.result().orElseThrow().kind());
            assertEquals(0, reads.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    @Test
    void queueRejectionDoesNotInvokeReadOrTouchOwner() {
        final Fixture fixture = fixture("rejected");
        try (fixture) {
            fixture.workClasses.submit(new WorkClassTask(WorkClass.QUERY, "occupied", 1), () -> {
            });
            final AtomicInteger reads = new AtomicInteger();
            assertThrows(IllegalStateException.class, () -> executor(fixture).submit(
                    request(fixture.shard, "rejected"), () -> 101, now -> {
                        reads.incrementAndGet();
                        return "unexpected";
                    }));
            assertEquals(0, reads.get());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, fixture.owned.state());
        }
    }

    private QueryWorkClassExecutor executor(final Fixture fixture) {
        return new QueryWorkClassExecutor(fixture.workClasses, fixture.owned, fixture.authority);
    }

    private static QueryWorkClassExecutor.QueryRequest request(final ShardId shard, final String name) {
        return new QueryWorkClassExecutor.QueryRequest(shard, Bytes.utf8("query-v1/" + name));
    }

    private Fixture fixture(final String name) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve(name));
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shard, resources);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final UUID topic = UUID.nameUUIDFromBytes(Bytes.utf8("query-topic-" + name));
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("query-assignment-" + name)), 7,
                new KafkaActivationBarrier(shard, "query-cluster", topic, 0));
        final OwnerLease lease = backend.acquire(assignment, "query-owner",
                Bytes.sha256(Bytes.utf8("query-session-" + name)), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final OwnedDelayShard owned = new OwnedDelayShard(
                new DelayShard(store, DelayShardConfig.defaults()), lease);
        owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
        owned.recordCatchup(new KafkaSourcePosition(shard, "query-cluster", topic, 0, null, 1_000));
        owned.activateForCommands(authority, 101);
        return new Fixture(shard, lease, backend, authority, owned, workClasses(1), resources, store);
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 10_000,
                    maxQueueRecords, 10_000, 1_000, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 1, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 20_000), new AtomicLong()::get);
    }

    private record Fixture(ShardId shard, OwnerLease lease, InMemoryOwnerLeaseStore backend,
                            OxiaOwnerLeaseStore authority, OwnedDelayShard owned,
                            WorkClassExecutionRegistry workClasses,
                            SharedRocksDbResources resources, ShardStore store) implements AutoCloseable {
        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
