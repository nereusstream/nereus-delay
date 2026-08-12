package io.nereusstream.delay.scheduler;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkClassDispatcherTest {
    @Test
    void requiresEveryFrozenClassBeforeAcceptingWork() {
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = handlers(task -> {
        });
        handlers.remove(WorkClass.CHECKPOINT);

        final WorkClassEventLoop loop = loop();
        assertThrows(IllegalArgumentException.class, () -> new WorkClassDispatcher(loop, handlers));
    }

    @Test
    void dispatchesCheckpointThroughTheBoundedTurnAndReleasesResources() {
        final AtomicReference<WorkClassTask> handled = new AtomicReference<>();
        final WorkClassDispatcher dispatcher = new WorkClassDispatcher(loop(), handlers(handled::set));
        final WorkClassTask task = new WorkClassTask(WorkClass.CHECKPOINT, "checkpoint-1", 8);
        dispatcher.offer(task);

        assertEquals(List.of(task), dispatcher.runTurn(new SchedulerBudget(1, 8, 1_000)));
        assertEquals(task, handled.get());
        assertEquals(0, dispatcher.pending(WorkClass.CHECKPOINT));
    }

    @Test
    void handlerFailurePropagatesAndTheNextTaskCanStillRun() {
        final AtomicReference<WorkClassTask> handled = new AtomicReference<>();
        final IllegalStateException failure = new IllegalStateException("handler failed");
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = handlers(task -> {
            if (task.taskId().equals("first")) {
                throw failure;
            }
            handled.set(task);
        });
        final WorkClassEventLoop loop = loop();
        final WorkClassDispatcher dispatcher = new WorkClassDispatcher(loop, handlers);
        dispatcher.offer(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "first", 8));
        dispatcher.offer(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "second", 8));

        assertThrows(IllegalStateException.class,
                () -> dispatcher.runTurn(new SchedulerBudget(1, 8, 1_000)));
        assertEquals(1, dispatcher.pending(WorkClass.OUTCOME_AND_CONTROL));
        assertEquals(List.of(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "second", 8)),
                dispatcher.runTurn(new SchedulerBudget(1, 8, 1_000)));
        assertEquals(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "second", 8), handled.get());
    }

    @Test
    void handlerRuntimeFailureDoesNotDropLaterTasksAlreadySelectedForTheTurn() {
        final List<String> handled = new ArrayList<>();
        final IllegalStateException failure = new IllegalStateException("first handler failed");
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = handlers(task -> {
            handled.add(task.taskId());
            if (task.taskId().equals("first")) {
                throw failure;
            }
        });
        final WorkClassDispatcher dispatcher = new WorkClassDispatcher(loop(2), handlers);
        dispatcher.offer(new WorkClassTask(WorkClass.QUERY, "first", 8));
        dispatcher.offer(new WorkClassTask(WorkClass.QUERY, "second", 8));

        assertEquals(failure, assertThrows(IllegalStateException.class,
                () -> dispatcher.runTurn(new SchedulerBudget(2, 16, 1_000))));
        assertEquals(List.of("first", "second"), handled);
        assertEquals(0, dispatcher.pending(WorkClass.QUERY));
    }

    @Test
    void fatalHandlerFailureRequeuesOnlyTrailingTasksThatWereNeverStarted() {
        final List<String> handled = new ArrayList<>();
        final AssertionError fatalFailure = new AssertionError("first handler failed fatally");
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = handlers(task -> {
            handled.add(task.taskId());
            if (task.taskId().equals("first")) {
                throw fatalFailure;
            }
        });
        final WorkClassDispatcher dispatcher = new WorkClassDispatcher(loop(2), handlers);
        dispatcher.offer(new WorkClassTask(WorkClass.QUERY, "first", 8));
        dispatcher.offer(new WorkClassTask(WorkClass.QUERY, "second", 8));

        assertEquals(fatalFailure, assertThrows(AssertionError.class,
                () -> dispatcher.runTurn(new SchedulerBudget(2, 16, 1_000))));
        assertEquals(List.of("first"), handled);
        assertEquals(1, dispatcher.pending(WorkClass.QUERY));

        assertEquals(List.of(new WorkClassTask(WorkClass.QUERY, "second", 8)),
                dispatcher.runTurn(new SchedulerBudget(1, 8, 1_000)));
        assertEquals(List.of("first", "second"), handled);
        assertEquals(0, dispatcher.pending(WorkClass.QUERY));
    }

    private static WorkClassEventLoop loop() {
        return loop(1);
    }

    private static WorkClassEventLoop loop(final long totalRecords) {
        final AtomicLong now = new AtomicLong();
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            policies.put(workClass, new WorkClassPolicy(1, 8, 64, 8, 32, 1_000,
                    0, 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassEventLoop(
                new WorkClassScheduler(policies, 100, now::get),
                new WorkClassResourcePool(policies, totalRecords, 64, 100, now::get));
    }

    private static EnumMap<WorkClass, Consumer<WorkClassTask>> handlers(
            final Consumer<WorkClassTask> common) {
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            handlers.put(workClass, common);
        }
        return handlers;
    }
}
