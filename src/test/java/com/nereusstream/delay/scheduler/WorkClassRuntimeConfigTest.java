package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class WorkClassRuntimeConfigTest {
    @Test
    void completeReleaseConfigurationConstructsAndRunsAllEightClasses() {
        final WorkClassRuntimeConfig config = new WorkClassRuntimeConfig(policies(), 100, 50, 16, 256);
        final List<WorkClass> handled = new ArrayList<>();
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            handlers.put(workClass, task -> handled.add(task.workClass()));
        }
        final WorkClassDispatcher dispatcher = config.newDispatcher(new AtomicLong()::get, handlers);
        for (WorkClass workClass : WorkClass.values()) {
            dispatcher.offer(new WorkClassTask(workClass, "task-" + workClass, 8));
        }

        final List<WorkClassTask> completed = dispatcher.runTurn(new SchedulerBudget(8, 64, 1_000));

        assertEquals(8, completed.size());
        assertEquals(8, handled.size());
        assertEquals(EnumSet.allOf(WorkClass.class), EnumSet.copyOf(handled));
    }

    @Test
    void rejectsIncompleteOrSemanticallyInvalidClassPolicies() {
        final EnumMap<WorkClass, WorkClassPolicy> missing = policies();
        missing.remove(WorkClass.CHECKPOINT);
        assertThrows(IllegalArgumentException.class, () -> new WorkClassRuntimeConfig(missing, 100, 50, 16, 256));

        final EnumMap<WorkClass, WorkClassPolicy> missingMinimum = policies();
        missingMinimum.put(WorkClass.GC, policy(WorkClass.GC, 0, 0));
        assertThrows(
                IllegalArgumentException.class, () -> new WorkClassRuntimeConfig(missingMinimum, 100, 50, 16, 256));

        final EnumMap<WorkClass, WorkClassPolicy> wrongPreemption = policies();
        wrongPreemption.put(WorkClass.QUERY, new WorkClassPolicy(1, 8, 64, 8, 64, 1_000, 0, 0, true));
        assertThrows(
                IllegalArgumentException.class, () -> new WorkClassRuntimeConfig(wrongPreemption, 100, 50, 16, 256));
    }

    @Test
    void rejectsAggregateMinimumOversubscriptionAndTurnLimitsBeyondTheQueue() {
        assertThrows(IllegalArgumentException.class, () -> new WorkClassRuntimeConfig(policies(), 100, 50, 5, 47));
        assertThrows(IllegalArgumentException.class, () -> new WorkClassPolicy(1, 1, 8, 2, 8, 1_000, 0, 0, false));
        assertThrows(IllegalArgumentException.class, () -> new WorkClassPolicy(1, 2, 8, 2, 9, 1_000, 0, 0, false));
    }

    private static EnumMap<WorkClass, WorkClassPolicy> policies() {
        final EnumMap<WorkClass, WorkClassPolicy> result = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass =
                    switch (workClass) {
                        case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                        case QUERY, CHECKPOINT -> false;
                    };
            result.put(workClass, policy(workClass, protectedClass ? 1 : 0, protectedClass ? 8 : 0));
        }
        return result;
    }

    private static WorkClassPolicy policy(
            final WorkClass workClass, final long minimumRecords, final long minimumBytes) {
        return new WorkClassPolicy(
                1, 8, 64, 8, 64, 1_000, minimumRecords, minimumBytes, workClass == WorkClass.LEASE_FENCE);
    }
}
