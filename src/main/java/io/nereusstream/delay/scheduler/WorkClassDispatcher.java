package io.nereusstream.delay.scheduler;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Production composition boundary for the bounded Worker work-class loop.
 *
 * <p>The dispatcher requires one handler for every frozen V1 work class before
 * it accepts work.  It delegates queue admission, fairness, resource leases
 * and borrowed-hold checks to {@link WorkClassEventLoop}; this class only
 * routes a selected task to the handler for its class.  A handler owns the
 * durable retry/replay identity for the task.  A failed handler is therefore
 * propagated and does not cause an unsafe implicit requeue.</p>
 */
public final class WorkClassDispatcher {
    private final WorkClassEventLoop eventLoop;
    private final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = new EnumMap<>(WorkClass.class);

    /**
     * Creates a dispatcher with an immutable, complete handler registry.
     * Missing or extra class entries are rejected before any work is offered.
     */
    public WorkClassDispatcher(final WorkClassEventLoop eventLoop,
                               final Map<WorkClass, ? extends Consumer<WorkClassTask>> handlers) {
        this.eventLoop = Objects.requireNonNull(eventLoop, "eventLoop");
        Objects.requireNonNull(handlers, "handlers");
        if (!EnumSet.allOf(WorkClass.class).equals(handlers.keySet())) {
            throw new IllegalArgumentException("handlers must cover every V1 work class exactly");
        }
        for (WorkClass workClass : WorkClass.values()) {
            this.handlers.put(workClass, Objects.requireNonNull(handlers.get(workClass),
                    "handler for " + workClass));
        }
    }

    /** Offers one task to the class-specific bounded queue. */
    public void offer(final WorkClassTask task) {
        eventLoop.offer(Objects.requireNonNull(task, "task"));
    }

    /**
     * Runs one bounded turn through the registered class handler and returns
     * the tasks whose handlers completed successfully.
     */
    public List<WorkClassTask> runTurn(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        final java.util.ArrayList<WorkClassTask> completed = new java.util.ArrayList<>();
        eventLoop.runTurn(budget, task -> {
            handlers.get(task.workClass()).accept(task);
            completed.add(task);
        });
        return List.copyOf(completed);
    }

    public int pending(final WorkClass workClass) {
        return eventLoop.pending(Objects.requireNonNull(workClass, "workClass"));
    }

    public long pendingBytes(final WorkClass workClass) {
        return eventLoop.pendingBytes(Objects.requireNonNull(workClass, "workClass"));
    }
}
