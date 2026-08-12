package io.nereusstream.delay.scheduler;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkClassExecutionRegistryTest {
    @Test
    void successfulActionsRunThroughAllEightClassesAndAreRemoved() {
        final WorkClassExecutionRegistry registry = registry(8);
        final List<WorkClass> handled = new ArrayList<>();
        for (WorkClass workClass : WorkClass.values()) {
            final WorkClassTask task = task(workClass, "success-" + workClass);
            registry.submit(task, () -> handled.add(workClass));
            assertEquals(Optional.of(WorkClassExecutionRegistry.ExecutionState.QUEUED), registry.state(task));
        }

        assertEquals(8, registry.runTurn(new SchedulerBudget(8, 64, 1_000)).size());
        assertEquals(8, handled.size());
        assertEquals(0, registry.registeredActions());
    }

    @Test
    void ordinaryFailureIsRetainedForExactExplicitRetryWhileLaterActionCompletes() {
        final WorkClassExecutionRegistry registry = registry(2);
        final WorkClassTask first = task(WorkClass.QUERY, "retry-first");
        final WorkClassTask second = task(WorkClass.QUERY, "retry-second");
        final AtomicBoolean failFirstAttempt = new AtomicBoolean(true);
        final AtomicInteger firstCalls = new AtomicInteger();
        final AtomicInteger secondCalls = new AtomicInteger();
        registry.submit(first, () -> {
            firstCalls.incrementAndGet();
            if (failFirstAttempt.getAndSet(false)) {
                throw new IllegalStateException("query handoff failed");
            }
        });
        registry.submit(second, secondCalls::incrementAndGet);

        assertThrows(IllegalStateException.class,
                () -> registry.runTurn(new SchedulerBudget(2, 16, 1_000)));
        assertEquals(Optional.of(WorkClassExecutionRegistry.ExecutionState.FAILED), registry.state(first));
        assertEquals(Optional.empty(), registry.state(second));
        assertEquals(1, firstCalls.get());
        assertEquals(1, secondCalls.get());

        registry.retry(first);
        assertEquals(List.of(first), registry.runTurn(new SchedulerBudget(1, 8, 1_000)));
        assertEquals(2, firstCalls.get());
        assertEquals(Optional.empty(), registry.state(first));
    }

    @Test
    void fatalFailureMarksStartedActionFailedAndLeavesTrailingActionQueued() {
        final WorkClassExecutionRegistry registry = registry(2);
        final WorkClassTask first = task(WorkClass.GC, "fatal-first");
        final WorkClassTask second = task(WorkClass.GC, "fatal-second");
        final AssertionError fatalFailure = new AssertionError("GC action failed fatally");
        final AtomicInteger secondCalls = new AtomicInteger();
        registry.submit(first, () -> {
            throw fatalFailure;
        });
        registry.submit(second, secondCalls::incrementAndGet);

        assertEquals(fatalFailure, assertThrows(AssertionError.class,
                () -> registry.runTurn(new SchedulerBudget(2, 16, 1_000))));
        assertEquals(Optional.of(WorkClassExecutionRegistry.ExecutionState.FAILED), registry.state(first));
        assertEquals(Optional.of(WorkClassExecutionRegistry.ExecutionState.QUEUED), registry.state(second));
        assertEquals(1, registry.pending(WorkClass.GC));

        assertEquals(List.of(second), registry.runTurn(new SchedulerBudget(1, 8, 1_000)));
        assertEquals(1, secondCalls.get());
        assertEquals(Optional.empty(), registry.state(second));
    }

    @Test
    void rejectedAdmissionRollsBackRegistrationAndIdentityDriftFailsClosed() {
        final WorkClassExecutionRegistry registry = registry(1);
        final WorkClassTask admitted = task(WorkClass.CHECKPOINT, "bounded");
        final WorkClassTask rejected = task(WorkClass.CHECKPOINT, "rejected");
        registry.submit(admitted, () -> {
        });

        assertThrows(IllegalStateException.class, () -> registry.submit(rejected, () -> {
        }));
        assertEquals(Optional.empty(), registry.state(rejected));
        assertEquals(1, registry.registeredActions());
        assertThrows(IllegalStateException.class,
                () -> registry.state(new WorkClassTask(WorkClass.CHECKPOINT, "bounded", 7)));
    }

    private static WorkClassExecutionRegistry registry(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, maxQueueRecords * 8L,
                    maxQueueRecords, maxQueueRecords * 8L, 1_000,
                    protectedClass ? 1 : 0, protectedClass ? 8 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        final WorkClassRuntimeConfig config = new WorkClassRuntimeConfig(policies, 100, 100,
                16, 256);
        return new WorkClassExecutionRegistry(config, new AtomicLong()::get);
    }

    private static WorkClassTask task(final WorkClass workClass, final String id) {
        return new WorkClassTask(workClass, id, 8);
    }
}
