package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetWorkerShardFleetRuntimeTest {
    @TempDir
    Path tempDir;

    @Test
    void sourceAndGcRotateIndependentlyAndFailureDoesNotPinAnotherShard() {
        final var registry = registry();
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);

            assertEquals(List.of(first.shard, second.shard), fleet.shardIds());
            assertEquals(first.shard, fleet.runNextSourceTurn(budget, () -> 101).shardId());
            assertEquals(
                    second.shard, fleet.runNextSourceTurn(budget, () -> 101).shardId());
            assertEquals(first.shard, fleet.runNextMaintenanceTurn(budget).shardId());
            assertEquals(first.shard, fleet.runNextSourceTurn(budget, () -> 101).shardId());
            assertEquals(second.shard, fleet.runNextMaintenanceTurn(budget).shardId());
            first.failNextMaintenance = true;
            assertEquals(
                    first.shard,
                    assertThrows(
                                    TargetWorkerShardFleetRuntime.MaintenanceDispatchFailure.class,
                                    () -> fleet.runNextMaintenanceTurn(budget))
                            .selectedShardId());
            assertEquals(second.shard, fleet.runNextMaintenanceTurn(budget).shardId());
            assertEquals(2, first.maintenanceTurns.get());
            assertEquals(2, second.maintenanceTurns.get());

            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TargetWorkerShardFleetRuntime(registry, resources, first, first));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TargetWorkerShardFleetRuntime(registry(), resources, first));
        }
    }

    @Test
    void scheduledMaintenanceContinuesAfterShardFailureAndCloseWaitsForActiveTurn() throws Exception {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        final var closer = Executors.newSingleThreadExecutor();
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("scheduled")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            final var observedSecond = new CountDownLatch(1);
            second.enteredMaintenance = observedSecond;
            first.failNextMaintenance = true;
            final var observedFailure = new AtomicReference<Throwable>();
            final var loop = new TargetWorkerMaintenanceLoop(
                    fleet,
                    new SchedulerBudget(1, 1000, 1_000_000),
                    Duration.ofMillis(1),
                    failure -> {
                        observedFailure.set(failure);
                        throw new IllegalStateException("failure observer failed");
                    },
                    executor);
            final var releaseBlocked = new CountDownLatch(1);
            try {
                loop.start();
                assertTrue(observedSecond.await(5, TimeUnit.SECONDS));
                assertNotNull(observedFailure.get());
                assertEquals(
                        "shard maintenance failure",
                        loop.firstFailure().getCause().getMessage());
                assertEquals(
                        "failure observer failed",
                        loop.firstFailure().getSuppressed()[0].getMessage());

                final var enteredBlocked = new CountDownLatch(1);
                first.releaseMaintenance = releaseBlocked;
                first.enteredMaintenance = enteredBlocked;
                assertTrue(enteredBlocked.await(5, TimeUnit.SECONDS));
                final var closeStarted = new CountDownLatch(1);
                final var closing = closer.submit(() -> {
                    closeStarted.countDown();
                    loop.close();
                });
                assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
                releaseBlocked.countDown();
                closing.get(5, TimeUnit.SECONDS);
                final int completed = first.maintenanceTurns.get() + second.maintenanceTurns.get();
                assertThrows(IllegalStateException.class, loop::pollNow);
                assertEquals(completed, first.maintenanceTurns.get() + second.maintenanceTurns.get());
            } finally {
                releaseBlocked.countDown();
                loop.close();
            }
        } finally {
            executor.shutdownNow();
            closer.shutdownNow();
        }
    }

    private static WorkClassExecutionRegistry registry() {
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
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
                            4,
                            1_000_000,
                            4,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), new AtomicLong()::get);
    }

    private static final class StubShard implements TargetWorkerShardFleetRuntime.ShardTurns {
        private final ShardId shard;
        private final WorkClassExecutionRegistry registry;
        private final SharedRocksDbResources resources;
        private final AtomicInteger maintenanceTurns = new AtomicInteger();
        private volatile boolean failNextMaintenance;
        private volatile CountDownLatch enteredMaintenance;
        private volatile CountDownLatch releaseMaintenance;

        private StubShard(
                final ShardId shard,
                final WorkClassExecutionRegistry registry,
                final SharedRocksDbResources resources) {
            this.shard = shard;
            this.registry = registry;
            this.resources = resources;
        }

        @Override
        public ShardId shardId() {
            return shard;
        }

        @Override
        public void requireFleetComposition(
                final WorkClassExecutionRegistry expectedRegistry, final SharedRocksDbResources expectedResources) {
            if (registry != expectedRegistry || resources != expectedResources) {
                throw new IllegalArgumentException("foreign Target Worker graph");
            }
        }

        @Override
        public SourceApplyCoordinator.TurnResult runSourceTurn(
                final SchedulerBudget budget, final LongSupplier ownerClock) {
            return new SourceApplyCoordinator.TurnResult(
                    SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE, null, null, null, null);
        }

        @Override
        public TargetReservationGcRuntime.Turn runMaintenanceTurn(final SchedulerBudget budget) {
            maintenanceTurns.incrementAndGet();
            if (enteredMaintenance != null) {
                enteredMaintenance.countDown();
            }
            if (releaseMaintenance != null) {
                try {
                    releaseMaintenance.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("maintenance turn interrupted", interrupted);
                }
            }
            if (failNextMaintenance) {
                failNextMaintenance = false;
                throw new IllegalStateException("shard maintenance failure");
            }
            return new TargetReservationGcRuntime.Turn(
                    TargetReservationGcRuntime.Lane.CLOSE,
                    new WorkClassTask(WorkClass.GC, "fleet-test/" + shard.partition(), 1),
                    List.of(),
                    Optional.empty(),
                    Optional.empty());
        }
    }
}
