package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.nereusstream.delay.store.CheckpointScheduler;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
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

            second.duringMaintenance =
                    () -> assertThrows(IllegalStateException.class, () -> fleet.withdraw(second.shard));
            assertEquals(first.shard, fleet.runNextMaintenanceTurn(budget).shardId());
            assertEquals(second.shard, fleet.runNextMaintenanceTurn(budget).shardId());

            fleet.withdraw(first.shard);
            assertEquals(List.of(second.shard), fleet.shardIds());
            assertEquals(
                    second.shard, fleet.runNextSourceTurn(budget, () -> 101).shardId());
            assertEquals(second.shard, fleet.runNextMaintenanceTurn(budget).shardId());
            fleet.withdraw(second.shard);
            assertTrue(fleet.runNextMaintenanceTurnIfPresent(budget).isEmpty());
            assertThrows(IllegalStateException.class, () -> fleet.runNextSourceTurn(budget, () -> 101));

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

    @Test
    void hostStopsAnActiveMaintenanceTurnBeforeDrainingEveryShardAndRetriesFailures() throws Exception {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        final var closer = Executors.newSingleThreadExecutor();
        final var releaseMaintenance = new CountDownLatch(1);
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("host-drain")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            first.enteredMaintenance = new CountDownLatch(1);
            first.releaseMaintenance = releaseMaintenance;
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);
            final var loop =
                    new TargetWorkerMaintenanceLoop(fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first, second));
            try {
                loop.start();
                assertTrue(first.enteredMaintenance.await(5, TimeUnit.SECONDS));
                first.failNextDrain = true;
                final var closing = closer.submit(
                        () -> host.drainAll(new TargetOwnerDrainCoordinator.Request(5_000, budget), budget, () -> 101));
                assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, first.drainCalls.get());
                assertEquals(0, second.drainCalls.get());

                releaseMaintenance.countDown();
                final var firstAttempt = closing.get(5, TimeUnit.SECONDS);
                assertFalse(firstAttempt.complete());
                assertEquals(
                        TargetWorkerHostRuntime.Status.FAILED,
                        firstAttempt.shards().getFirst().status());
                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        firstAttempt.shards().getLast().status());
                assertEquals(1, first.drainCalls.get());
                assertEquals(1, second.drainCalls.get());
                assertThrows(IllegalStateException.class, () -> host.runNextSourceTurn(budget, () -> 101));

                final var retry =
                        host.drainAll(new TargetOwnerDrainCoordinator.Request(5_000, budget), budget, () -> 101);
                assertTrue(retry.complete());
                assertEquals(2, first.drainCalls.get());
                assertEquals(1, second.drainCalls.get());
            } finally {
                releaseMaintenance.countDown();
                loop.close();
            }
        } finally {
            executor.shutdownNow();
            closer.shutdownNow();
        }
    }

    @Test
    void hostSettlesExactPendingCheckpointAfterStopBeforeRetryingDrain() {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        try (var resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("host-checkpoint")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            first.pendingCheckpoint = new WorkClassTask(WorkClass.CHECKPOINT, "host-checkpoint/first", 64);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            final var budget = new SchedulerBudget(1, 1_000, 1_000_000);
            final var loop = new TargetWorkerMaintenanceLoop(
                    fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first, second));
            try {
                final var request = new TargetOwnerDrainCoordinator.Request(5_000, budget);
                final var pending = host.drainAll(request, budget, () -> 101);
                assertFalse(pending.complete());
                assertEquals(TargetWorkerHostRuntime.Status.PENDING_CHECKPOINT, pending.shards().getFirst().status());
                assertEquals(first.pendingCheckpoint, pending.shards().getFirst().pendingCheckpointTask());
                assertEquals(TargetWorkerHostRuntime.Status.RELEASED, pending.shards().getLast().status());
                assertEquals(0, first.drainCalls.get());
                assertEquals(1, second.drainCalls.get());
                assertThrows(
                        IllegalArgumentException.class,
                        () -> host.settlePendingCheckpointTurn(
                                new StubShard(first.shard, registry, resources), budget));
                assertTrue(host.settlePendingCheckpointTurn(first, new SchedulerBudget(1, 1, 1_000_000))
                        .isEmpty());
                assertEquals(TargetWorkerHostRuntime.Status.PENDING_CHECKPOINT,
                        host.drainAll(request, budget, () -> 101).shards().getFirst().status());
                assertEquals(0, first.drainCalls.get());
                assertEquals(
                        Path.of("checkpoint-candidate"),
                        host.settlePendingCheckpointTurn(first, budget).orElseThrow().checkpointPath());
                assertTrue(host.drainAll(request, budget, () -> 101).complete());
                assertEquals(1, first.drainCalls.get());
                assertEquals(1, second.drainCalls.get());
                assertThrows(IllegalStateException.class, () -> host.settlePendingCheckpointTurn(first, budget));
            } finally {
                loop.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void scheduledCandidatesRetainClaimsUntilExactTerminalOutcomeAcrossHostStop() {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        try (var resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("scheduled-candidates")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            final var budget = new SchedulerBudget(1, 1_000, 1_000_000);
            final var loop = new TargetWorkerMaintenanceLoop(
                    fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first, second));
            final var schedule = new TargetCheckpointCandidateSchedule(
                    host,
                    new CheckpointScheduler(10, 0, 2),
                    (TargetCheckpointCandidateSchedule.Dispatcher) (shard, claim) -> {
                        final var stub = (StubShard) shard;
                        final var task = new WorkClassTask(
                                WorkClass.CHECKPOINT, "scheduled/" + stub.shard.partition(), 64);
                        stub.pendingCheckpoint = task;
                        return new TargetCheckpointCandidateSchedule.Candidate(
                                task,
                                () -> stub.pendingCheckpoint == null
                                        ? Optional.of(new TargetCheckpointCandidateWorkClassExecutor.Outcome(
                                                Path.of("candidate"), null))
                                        : Optional.empty());
                    });
            try {
                assertEquals(110, schedule.registerShard(first, 100));
                assertEquals(110, schedule.registerShard(second, 100));
                assertEquals(2, schedule.claimDueAndSubmit(110, 2).size());
                assertTrue(schedule.claimDueAndSubmit(110, 2).isEmpty());
                assertThrows(IllegalStateException.class, () -> schedule.unregister(first.shard));
                assertEquals(
                        TargetWorkerHostRuntime.Status.PENDING_CHECKPOINT,
                        host.drainAll(new TargetOwnerDrainCoordinator.Request(5_000, budget), budget, () -> 101)
                                .shards().getFirst().status());
                assertTrue(schedule.settle(first.shard, new SchedulerBudget(1, 1, 1_000_000), 110).isEmpty());
                assertTrue(schedule.pendingTask(first.shard).isPresent());
                assertEquals(
                        Path.of("candidate"), schedule.settle(first.shard, budget, 111)
                                .orElseThrow()
                                .checkpointPath());
                assertTrue(schedule.pendingTask(first.shard).isEmpty());
                assertTrue(schedule.settle(second.shard, budget, 112).isPresent());
                assertTrue(host.drainAll(new TargetOwnerDrainCoordinator.Request(5_000, budget), budget, () -> 101)
                        .complete());
                schedule.unregister(first.shard);
                schedule.unregister(second.shard);
            } finally {
                loop.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failedScheduledAdmissionReschedulesWithoutLeavingAnInFlightClaim() {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        try (var resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("scheduled-candidate-retry")))) {
            final var shard = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, shard);
            final var budget = new SchedulerBudget(1, 1_000, 1_000_000);
            final var loop = new TargetWorkerMaintenanceLoop(
                    fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(shard));
            final var attempts = new AtomicInteger();
            final var schedule = new TargetCheckpointCandidateSchedule(
                    host,
                    new CheckpointScheduler(10, 0, 1),
                    (TargetCheckpointCandidateSchedule.Dispatcher) (selected, claim) -> {
                        if (attempts.getAndIncrement() == 0) {
                            throw new IllegalStateException("request preflight failed");
                        }
                        final var task = new WorkClassTask(WorkClass.CHECKPOINT, "retry", 64);
                        shard.pendingCheckpoint = task;
                        return new TargetCheckpointCandidateSchedule.Candidate(
                                task,
                                () -> shard.pendingCheckpoint == null
                                        ? Optional.of(new TargetCheckpointCandidateWorkClassExecutor.Outcome(
                                                Path.of("candidate"), null))
                                        : Optional.empty());
                    });
            try {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> schedule.registerShard(new StubShard(shard.shard, registry, resources), 100));
                assertEquals(110, schedule.registerShard(shard, 100));
                assertThrows(IllegalStateException.class, () -> schedule.claimDueAndSubmit(110, 1));
                assertTrue(schedule.pendingTask(shard.shard).isEmpty());
                assertTrue(schedule.claimDueAndSubmit(110, 1).isEmpty());
                assertEquals(1, schedule.claimDueAndSubmit(120, 1).size());
                final WorkClassTask exact = schedule.pendingTask(shard.shard).orElseThrow();
                shard.pendingCheckpoint = new WorkClassTask(WorkClass.CHECKPOINT, "retry", 64);
                assertThrows(IllegalStateException.class, () -> schedule.settle(shard.shard, budget, 120));
                shard.pendingCheckpoint = exact;
                assertTrue(schedule.settle(shard.shard, budget, 120).isPresent());
                schedule.unregister(shard.shard);
            } finally {
                loop.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void hostCheckpointAdmissionReservesExactShardWhileAnotherShardDrains() throws Exception {
        final var registry = registry();
        final var scheduler = Executors.newSingleThreadScheduledExecutor();
        final var submitter = Executors.newSingleThreadExecutor();
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        try (var resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("host-checkpoint-admission")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            final var budget = new SchedulerBudget(1, 1_000, 1_000_000);
            final var request = new TargetOwnerDrainCoordinator.Request(5_000, budget);
            final var loop = new TargetWorkerMaintenanceLoop(
                    fleet, budget, Duration.ofSeconds(10), failure -> {}, scheduler);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first, second));
            try {
                final var admitting = submitter.submit(() -> host.withCheckpointAdmission(first, () -> {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("checkpoint admission test timed out");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("checkpoint admission test interrupted", interrupted);
                    }
                    return "admitted";
                }));
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertThrows(IllegalStateException.class, () -> host.drainShard(first, request, budget, () -> 101));
                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        host.drainShard(second, request, budget, () -> 101).status());
                assertThrows(
                        IllegalArgumentException.class,
                        () -> host.withCheckpointAdmission(
                                new StubShard(first.shard, registry, resources), () -> "stale"));
                release.countDown();
                assertEquals("admitted", admitting.get(5, TimeUnit.SECONDS));
                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        host.drainShard(first, request, budget, () -> 101).status());
                final var replacement = new StubShard(first.shard, registry, resources);
                host.admitShard(replacement, replacement);
                assertThrows(IllegalArgumentException.class, () -> host.withCheckpointAdmission(first, () -> "stale"));
                assertEquals("replacement", host.withCheckpointAdmission(replacement, () -> "replacement"));
                assertTrue(host.drainAll(request, budget, () -> 101).complete());
                assertThrows(
                        IllegalStateException.class,
                        () -> host.withCheckpointAdmission(replacement, () -> "late"));
            } finally {
                release.countDown();
                loop.close();
            }
        } finally {
            submitter.shutdownNow();
            scheduler.shutdownNow();
        }
    }

    @Test
    void hostWithdrawsOnlyOneShardAfterItsActiveGcTurnAndKeepsOtherShardScheduled() throws Exception {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        final var closer = Executors.newSingleThreadExecutor();
        final var releaseMaintenance = new CountDownLatch(1);
        final var releaseDrain = new CountDownLatch(1);
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("host-withdraw")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            first.enteredMaintenance = new CountDownLatch(1);
            first.releaseMaintenance = releaseMaintenance;
            first.enteredDrain = new CountDownLatch(1);
            first.releaseDrain = releaseDrain;
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);
            final var loop =
                    new TargetWorkerMaintenanceLoop(fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first, second));
            try {
                loop.start();
                assertTrue(first.enteredMaintenance.await(5, TimeUnit.SECONDS));
                final var withdrawing = closer.submit(() -> host.drainShard(
                        first, new TargetOwnerDrainCoordinator.Request(5_000, budget), budget, () -> 101));
                assertThrows(TimeoutException.class, () -> withdrawing.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, first.drainCalls.get());

                releaseMaintenance.countDown();
                assertTrue(first.enteredDrain.await(5, TimeUnit.SECONDS));
                assertEquals(List.of(second.shard), fleet.shardIds());
                assertEquals(1, first.maintenanceTurns.get());
                assertEquals(1, first.drainCalls.get());
                assertEquals(0, second.drainCalls.get());
                assertEquals(
                        second.shard, host.runNextSourceTurn(budget, () -> 101).shardId());
                loop.pollNow();
                assertEquals(1, first.maintenanceTurns.get());
                assertEquals(1, second.maintenanceTurns.get());

                releaseDrain.countDown();
                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        withdrawing.get(5, TimeUnit.SECONDS).status());

                assertTrue(host.drainAll(new TargetOwnerDrainCoordinator.Request(5_000, budget), budget, () -> 101)
                        .complete());
                assertEquals(1, second.drainCalls.get());
            } finally {
                releaseMaintenance.countDown();
                releaseDrain.countDown();
                loop.close();
            }
        } finally {
            executor.shutdownNow();
            closer.shutdownNow();
        }
    }

    @Test
    void hostAdmitsNewShardAndReplacesOnlyCompletedInstanceWithExactGraph() {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("host-admit")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var replacement = new StubShard(first.shard, registry, resources);
            final var foreign = new StubShard(new ShardId(RouteIncarnation.random(), 3), registry(), resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first);
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);
            final var request = new TargetOwnerDrainCoordinator.Request(5_000, budget);
            final var loop =
                    new TargetWorkerMaintenanceLoop(fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first));
            try {
                assertThrows(IllegalArgumentException.class, () -> host.admitShard(foreign, foreign));
                assertThrows(IllegalArgumentException.class, () -> host.admitShard(first, second));
                host.admitShard(second, second);
                assertEquals(List.of(first.shard, second.shard), fleet.shardIds());
                assertThrows(IllegalStateException.class, () -> host.admitShard(replacement, replacement));

                first.failNextDrain = true;
                assertEquals(
                        TargetWorkerHostRuntime.Status.FAILED,
                        host.drainShard(first, request, budget, () -> 101).status());
                assertEquals(List.of(second.shard), fleet.shardIds());
                assertThrows(IllegalStateException.class, () -> host.admitShard(replacement, replacement));
                assertEquals(
                        second.shard, host.runNextSourceTurn(budget, () -> 101).shardId());
                loop.pollNow();
                assertEquals(1, second.maintenanceTurns.get());

                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        host.drainShard(first, request, budget, () -> 101).status());
                assertThrows(IllegalArgumentException.class, () -> host.admitShard(first, first));
                host.admitShard(replacement, replacement);
                assertEquals(List.of(second.shard, replacement.shard), fleet.shardIds());
                assertThrows(IllegalArgumentException.class, () -> host.drainShard(first, request, budget, () -> 101));
                assertEquals(
                        second.shard, host.runNextSourceTurn(budget, () -> 101).shardId());
                assertEquals(
                        replacement.shard,
                        host.runNextSourceTurn(budget, () -> 101).shardId());
                loop.pollNow();
                loop.pollNow();
                assertEquals(1, replacement.maintenanceTurns.get());

                assertTrue(host.drainAll(request, budget, () -> 101).complete());
                assertEquals(2, first.drainCalls.get());
                assertEquals(1, second.drainCalls.get());
                assertEquals(1, replacement.drainCalls.get());
                assertThrows(IllegalStateException.class, () -> host.admitShard(foreign, foreign));
            } finally {
                loop.close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void admissionWaitsForInFlightDrainAndRejectsReplacementUntilTerminalResult() throws Exception {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        final var drainer = Executors.newSingleThreadExecutor();
        final var releaseDrain = new CountDownLatch(1);
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("admit-drain")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var replacement = new StubShard(first.shard, registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first);
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);
            final var request = new TargetOwnerDrainCoordinator.Request(5_000, budget);
            final var loop =
                    new TargetWorkerMaintenanceLoop(fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first));
            first.enteredDrain = new CountDownLatch(1);
            first.releaseDrain = releaseDrain;
            try {
                final var draining = drainer.submit(() -> host.drainShard(first, request, budget, () -> 101));
                assertTrue(first.enteredDrain.await(5, TimeUnit.SECONDS));
                assertTrue(fleet.shardIds().isEmpty());
                assertThrows(IllegalStateException.class, () -> host.admitShard(replacement, replacement));
                assertThrows(IllegalStateException.class, () -> host.drainShard(first, request, budget, () -> 101));
                releaseDrain.countDown();
                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        draining.get(5, TimeUnit.SECONDS).status());
                host.admitShard(replacement, replacement);
                assertEquals(
                        replacement.shard,
                        host.runNextSourceTurn(budget, () -> 101).shardId());
            } finally {
                releaseDrain.countDown();
                loop.close();
            }
        } finally {
            executor.shutdownNow();
            drainer.shutdownNow();
        }
    }

    @Test
    void wholeHostShutdownWaitsForSingleShardDrainWithoutRepeatingReleasedStore() throws Exception {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        final var drainer = Executors.newSingleThreadExecutor();
        final var closer = Executors.newSingleThreadExecutor();
        final var releaseDrain = new CountDownLatch(1);
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("admit-shutdown")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first, second);
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);
            final var request = new TargetOwnerDrainCoordinator.Request(5_000, budget);
            final var loop =
                    new TargetWorkerMaintenanceLoop(fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first, second));
            first.enteredDrain = new CountDownLatch(1);
            first.releaseDrain = releaseDrain;
            try {
                final var draining = drainer.submit(() -> host.drainShard(first, request, budget, () -> 101));
                assertTrue(first.enteredDrain.await(5, TimeUnit.SECONDS));
                final var closing = closer.submit(() -> host.drainAll(request, budget, () -> 101));
                assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
                assertTrue(host.stopping());
                assertEquals(0, second.drainCalls.get());
                releaseDrain.countDown();
                assertEquals(
                        TargetWorkerHostRuntime.Status.RELEASED,
                        draining.get(5, TimeUnit.SECONDS).status());
                assertTrue(closing.get(5, TimeUnit.SECONDS).complete());
                assertEquals(1, first.drainCalls.get());
                assertEquals(1, second.drainCalls.get());
            } finally {
                releaseDrain.countDown();
                loop.close();
            }
        } finally {
            executor.shutdownNow();
            drainer.shutdownNow();
            closer.shutdownNow();
        }
    }

    @Test
    void admissionWaitsForSelectedMaintenanceTurnBeforePublishingNewMember() throws Exception {
        final var registry = registry();
        final var executor = Executors.newSingleThreadScheduledExecutor();
        final var admitting = Executors.newSingleThreadExecutor();
        final var releaseMaintenance = new CountDownLatch(1);
        try (var resources = new SharedRocksDbResources(ShardStoreConfig.defaults(tempDir.resolve("admit-gc")))) {
            final var first = new StubShard(new ShardId(RouteIncarnation.random(), 1), registry, resources);
            final var second = new StubShard(new ShardId(RouteIncarnation.random(), 2), registry, resources);
            first.enteredMaintenance = new CountDownLatch(1);
            first.releaseMaintenance = releaseMaintenance;
            final var fleet = new TargetWorkerShardFleetRuntime(registry, resources, first);
            final var budget = new SchedulerBudget(1, 1000, 1_000_000);
            final var loop =
                    new TargetWorkerMaintenanceLoop(fleet, budget, Duration.ofSeconds(10), failure -> {}, executor);
            final var host = new TargetWorkerHostRuntime(fleet, loop, List.of(first));
            try {
                loop.start();
                assertTrue(first.enteredMaintenance.await(5, TimeUnit.SECONDS));
                final var admissionStarted = new CountDownLatch(1);
                final var adding = admitting.submit(() -> {
                    admissionStarted.countDown();
                    host.admitShard(second, second);
                });
                assertTrue(admissionStarted.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> adding.get(100, TimeUnit.MILLISECONDS));
                releaseMaintenance.countDown();
                adding.get(5, TimeUnit.SECONDS);
                assertEquals(List.of(first.shard, second.shard), fleet.shardIds());
                assertEquals(
                        first.shard, host.runNextSourceTurn(budget, () -> 101).shardId());
                assertEquals(
                        second.shard, host.runNextSourceTurn(budget, () -> 101).shardId());
            } finally {
                releaseMaintenance.countDown();
                loop.close();
            }
        } finally {
            executor.shutdownNow();
            admitting.shutdownNow();
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

    private static final class StubShard
            implements TargetWorkerShardFleetRuntime.ShardTurns, TargetWorkerHostRuntime.Shard {
        private final ShardId shard;
        private final WorkClassExecutionRegistry registry;
        private final SharedRocksDbResources resources;
        private final AtomicInteger maintenanceTurns = new AtomicInteger();
        private final AtomicInteger drainCalls = new AtomicInteger();
        private volatile boolean failNextMaintenance;
        private volatile boolean failNextDrain;
        private volatile CountDownLatch enteredMaintenance;
        private volatile CountDownLatch releaseMaintenance;
        private volatile CountDownLatch enteredDrain;
        private volatile CountDownLatch releaseDrain;
        private volatile Runnable duringMaintenance;
        private volatile WorkClassTask pendingCheckpoint;

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
            if (duringMaintenance != null) {
                duringMaintenance.run();
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

        @Override
        public Optional<SourceReplayEntry> pendingSourceEntry() {
            return Optional.empty();
        }

        @Override
        public Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
                final SchedulerBudget budget, final LongSupplier ownerClock) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkClassTask> pendingCheckpointTask() {
            return Optional.ofNullable(pendingCheckpoint);
        }

        @Override
        public Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> runCheckpointTurn(
                final SchedulerBudget budget) {
            if (pendingCheckpoint == null || budget.maxBytes() < pendingCheckpoint.bytes()) {
                return Optional.empty();
            }
            pendingCheckpoint = null;
            return Optional.of(new TargetCheckpointCandidateWorkClassExecutor.Outcome(
                    Path.of("checkpoint-candidate"), null));
        }

        @Override
        public TargetOwnerDrainCoordinator.Result drain(
                final TargetOwnerDrainCoordinator.Request request, final LongSupplier clock) {
            drainCalls.incrementAndGet();
            if (enteredDrain != null) {
                enteredDrain.countDown();
            }
            if (releaseDrain != null) {
                try {
                    releaseDrain.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("shard drain interrupted", interrupted);
                }
            }
            if (failNextDrain) {
                failNextDrain = false;
                throw new IllegalStateException("shard drain failed");
            }
            return new TargetOwnerDrainCoordinator.Result(TargetOwnerDrainCoordinator.Status.RELEASED, null);
        }
    }
}
