package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseFenceWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void expiredLeaseFencesOwnerAndStopsSourceExactlyOnce() {
        final Fixture fixture = fixture("expired");
        try (fixture) {
            final AtomicInteger stops = new AtomicInteger();
            final LeaseFenceWorkClassExecutor executor = new LeaseFenceWorkClassExecutor(
                    fixture.workClasses, fixture.owned, fixture.authority);
            final LeaseFenceWorkClassExecutor.Submission submission = executor.submit(fixture.lease,
                    () -> 250, stops::incrementAndGet);

            assertEquals(WorkClass.LEASE_FENCE, submission.task().workClass());
            fixture.workClasses.runTurn(new SchedulerBudget(1, 10_000, 1_000));

            final LeaseFenceWorkClassExecutor.FenceResult result = submission.result().orElseThrow();
            assertEquals(LeaseFenceWorkClassExecutor.ResultKind.FENCED, result.kind());
            assertEquals(1, stops.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    @Test
    void replacementOwnerIsObservedAndOldFenceStillClosesOnlyOldLocalGate() {
        final Fixture fixture = fixture("replacement");
        try (fixture) {
            fixture.authority.release(fixture.lease);
            final OwnerLease replacement = fixture.authority.acquire(fixture.shard, "replacement", 250, 100)
                    .orElseThrow();
            final AtomicInteger stops = new AtomicInteger();
            final LeaseFenceWorkClassExecutor.Submission submission =
                    new LeaseFenceWorkClassExecutor(fixture.workClasses, fixture.owned, fixture.authority)
                            .submit(fixture.lease, () -> 250, stops::incrementAndGet);

            fixture.workClasses.runTurn(new SchedulerBudget(1, 10_000, 1_000));
            final LeaseFenceWorkClassExecutor.FenceResult result = submission.result().orElseThrow();
            assertEquals(LeaseFenceWorkClassExecutor.ResultKind.FENCED, result.kind());
            assertEquals(replacement, result.observedLease());
            assertEquals(1, stops.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
        }
    }

    @Test
    void validOwnerDoesNotFenceOrStop() {
        final Fixture fixture = fixture("valid");
        try (fixture) {
            final AtomicInteger stops = new AtomicInteger();
            final LeaseFenceWorkClassExecutor.Submission submission =
                    new LeaseFenceWorkClassExecutor(fixture.workClasses, fixture.owned, fixture.authority)
                            .submit(fixture.lease, () -> 150, stops::incrementAndGet);

            fixture.workClasses.runTurn(new SchedulerBudget(1, 10_000, 1_000));
            assertEquals(LeaseFenceWorkClassExecutor.ResultKind.OWNER_STILL_VALID,
                    submission.result().orElseThrow().kind());
            assertEquals(0, stops.get());
            assertEquals(ShardLifecycleState.RESTORING, fixture.owned.state());
        }
    }

    @Test
    void queueRejectionHasNoFenceOrStopSideEffect() {
        final Fixture fixture = fixture("rejected");
        try (fixture) {
            fixture.workClasses.submit(new WorkClassTask(WorkClass.LEASE_FENCE, "occupied", 1), () -> {
            });
            final AtomicInteger stops = new AtomicInteger();
            final LeaseFenceWorkClassExecutor executor = new LeaseFenceWorkClassExecutor(
                    fixture.workClasses, fixture.owned, fixture.authority);
            assertThrows(IllegalStateException.class,
                    () -> executor.submit(fixture.lease, () -> 250, stops::incrementAndGet));
            assertEquals(0, stops.get());
            assertEquals(ShardLifecycleState.RESTORING, fixture.owned.state());
            assertEquals(Optional.empty(), fixture.workClasses.state(
                    new WorkClassTask(WorkClass.LEASE_FENCE, "lease-fence/does-not-exist", 1)));
        }
    }

    private Fixture fixture(final String name) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve(name));
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shard, resources);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(shard, "owner", 100, 100).orElseThrow();
        final OwnedDelayShard owned = new OwnedDelayShard(
                new DelayShard(store, DelayShardConfig.defaults()), lease);
        return new Fixture(shard, lease, new OxiaOwnerLeaseStore(backend), owned,
                workClasses(1), resources, store);
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

    private record Fixture(ShardId shard, OwnerLease lease, OxiaOwnerLeaseStore authority,
                            OwnedDelayShard owned, WorkClassExecutionRegistry workClasses,
                            SharedRocksDbResources resources, ShardStore store) implements AutoCloseable {
        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
