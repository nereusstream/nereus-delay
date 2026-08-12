package io.nereusstream.delay.scheduler;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Local composition seam for the V1 Worker event loop.
 *
 * <p>{@link WorkClassScheduler} owns queue fairness and
 * {@link WorkClassResourcePool} owns shared record/byte tokens.  This class
 * binds them at the bounded-turn boundary: tokens are acquired immediately
 * before a task leaves its queue, and the returned {@link Turn} must be closed
 * before another turn can be polled.  A failed acquisition restores the
 * scheduler queue and releases any tokens acquired earlier in that poll.</p>
 *
 * <p>The class does not execute callbacks, perform RocksDB writes, or claim
 * Oxia authority.  Production code must perform those actions while holding a
 * turn and keep the external write/admission authorities around this local
 * seam.</p>
 */
public final class WorkClassEventLoop {
    private final WorkClassScheduler scheduler;
    private final WorkClassResourcePool resources;
    private Turn activeTurn;

    public WorkClassEventLoop(final WorkClassScheduler scheduler,
                              final WorkClassResourcePool resources) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.resources = Objects.requireNonNull(resources, "resources");
    }

    /** Offers bounded work without consuming a resource token while queued. */
    public synchronized void offer(final WorkClassTask task) {
        final WorkClassTask offered = Objects.requireNonNull(task, "task");
        reserveActiveTurnCapacity(offered);
        scheduler.offer(offered);
    }

    /**
     * Removes one bounded turn and returns the exact token leases for it.
     * Callers must close the returned turn after the bounded chunk finishes.
     */
    public synchronized Turn poll(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        if (activeTurn != null && !activeTurn.isClosed()) {
            throw new IllegalStateException("previous WorkClass turn must be closed before polling again");
        }
        final TurnBuilder builder = new TurnBuilder();
        try {
            final List<WorkClassTask> tasks = scheduler.poll(budget, task ->
                    builder.add(resources.acquire(task.workClass(), 1, task.bytes())));
            if (tasks.isEmpty()) {
                builder.release();
                return Turn.empty();
            }
            final List<WorkClassResourcePool.ResourceLease> leases = builder.snapshotLeases();
            final Turn turn = new Turn(this, tasks, leases);
            builder.clearAfterTransfer();
            activeTurn = turn;
            return turn;
        } catch (RuntimeException | Error failure) {
            try {
                builder.release();
            } catch (RuntimeException | Error releaseFailure) {
                if (releaseFailure != failure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * Runs one bounded turn and always closes its resource leases.
     *
     * <p>The callback executes outside the event-loop monitor, so producers
     * may offer later work while this bounded chunk is running.  A callback
     * failure does not leave the selected chunk's shared capacity borrowed:
     * the turn is closed before the original failure is rethrown.  The
     * callback is still responsible for keeping its own RocksDB/Oxia and
     * external I/O operations within the V1 chunk boundary.</p>
     */
    public void runTurn(final SchedulerBudget budget, final Consumer<WorkClassTask> executor) {
        Objects.requireNonNull(executor, "executor");
        final Turn turn = poll(budget);
        if (turn.isEmpty()) {
            return;
        }
        Throwable primaryFailure = null;
        int nextUnstartedIndex = 0;
        boolean executionStopped = false;
        try {
            for (int index = 0; index < turn.tasks().size(); index++) {
                final WorkClassTask task = turn.tasks().get(index);
                turn.requireWithinBorrowedHold();
                // From this point the handler may have made a durable or
                // external side effect, so this exact task is never requeued
                // implicitly even if it throws a fatal Error.
                nextUnstartedIndex = index + 1;
                try {
                    executor.accept(task);
                } catch (RuntimeException handlerFailure) {
                    // Every task in the Turn has already left its bounded
                    // queue. A normal handler failure owns that task's durable
                    // retry identity, but it must not silently discard later
                    // selected tasks. Continue while the resource hold remains
                    // valid, then propagate the first handler failure.
                    primaryFailure = appendFailure(primaryFailure, handlerFailure);
                }
                turn.requireWithinBorrowedHold();
            }
        } catch (RuntimeException | Error failure) {
            executionStopped = true;
            primaryFailure = appendFailure(primaryFailure, failure);
        }
        if (executionStopped && nextUnstartedIndex < turn.tasks().size()) {
            try {
                requeueUnstarted(turn, nextUnstartedIndex);
            } catch (RuntimeException | Error requeueFailure) {
                primaryFailure = appendFailure(primaryFailure, requeueFailure);
            }
        }
        try {
            turn.close();
        } catch (RuntimeException | Error closeFailure) {
            primaryFailure = appendFailure(primaryFailure, closeFailure);
        }
        if (primaryFailure != null) {
            throwUnchecked(primaryFailure);
        }
    }

    private static Throwable appendFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    public synchronized int pending(final WorkClass workClass) {
        return scheduler.pending(workClass);
    }

    public synchronized long pendingBytes(final WorkClass workClass) {
        return scheduler.pendingBytes(workClass);
    }

    private void reserveActiveTurnCapacity(final WorkClassTask offered) {
        final Turn turn = activeTurn;
        if (turn == null || turn.isClosed()) {
            return;
        }
        int reservedRecords = 0;
        long reservedBytes = 0;
        for (WorkClassTask selected : turn.tasks()) {
            if (selected.workClass() == offered.workClass()) {
                reservedRecords = Math.addExact(reservedRecords, 1);
                reservedBytes = Math.addExact(reservedBytes, selected.bytes());
            }
        }
        final WorkClassPolicy policy = scheduler.policy(offered.workClass());
        final int pendingRecords = scheduler.pending(offered.workClass());
        final long pendingBytes = scheduler.pendingBytes(offered.workClass());
        if (reservedRecords > policy.maxQueueRecords()
                || pendingRecords >= policy.maxQueueRecords() - reservedRecords
                || reservedBytes > policy.maxQueueBytes()
                || pendingBytes > policy.maxQueueBytes() - reservedBytes
                || offered.bytes() > policy.maxQueueBytes() - reservedBytes - pendingBytes) {
            throw new IllegalStateException("work-class queue capacity is reserved for the active turn");
        }
    }

    private synchronized void requeueUnstarted(final Turn turn, final int fromIndex) {
        if (activeTurn != turn || turn.isClosed()) {
            throw new IllegalStateException("cannot requeue tasks from an inactive WorkClass turn");
        }
        scheduler.requeueFirst(turn.tasks().subList(fromIndex, turn.tasks().size()));
    }

    private synchronized void finish(final Turn turn) {
        if (activeTurn == turn) {
            activeTurn = null;
        }
    }

    private static final class TurnBuilder {
        private final java.util.ArrayList<WorkClassResourcePool.ResourceLease> leases =
                new java.util.ArrayList<>();

        private void add(final WorkClassResourcePool.ResourceLease lease) {
            leases.add(Objects.requireNonNull(lease, "resource lease"));
        }

        private List<WorkClassResourcePool.ResourceLease> snapshotLeases() {
            return List.copyOf(leases);
        }

        private void clearAfterTransfer() {
            leases.clear();
        }

        private void release() {
            Throwable failure = null;
            for (int index = leases.size() - 1; index >= 0; index--) {
                try {
                    leases.get(index).close();
                } catch (RuntimeException | Error closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else if (closeFailure != failure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            leases.clear();
            if (failure != null) {
                throwUnchecked(failure);
            }
        }
    }

    /** One bounded work turn and the exact resource leases acquired for it. */
    public static final class Turn implements AutoCloseable {
        private final WorkClassEventLoop owner;
        private final List<WorkClassTask> tasks;
        private final List<WorkClassResourcePool.ResourceLease> leases;
        /**
         * Poll reads this while holding the event-loop monitor.  Keep the read
         * lock-free: close holds the Turn monitor and later enters the event
         * loop to clear {@code activeTurn}, so taking the Turn monitor from
         * poll would invert that order and deadlock concurrent close/poll.
         */
        private volatile boolean closed;

        private Turn(final WorkClassEventLoop owner,
                     final List<WorkClassTask> tasks,
                     final List<WorkClassResourcePool.ResourceLease> leases) {
            this.tasks = List.copyOf(tasks);
            this.leases = List.copyOf(leases);
            if (this.tasks.isEmpty()) {
                if (owner != null || !this.leases.isEmpty()) {
                    throw new IllegalArgumentException("empty turn cannot carry an owner or resource leases");
                }
                this.owner = null;
            } else {
                this.owner = Objects.requireNonNull(owner, "owner");
                if (this.leases.size() != this.tasks.size()) {
                    throw new IllegalArgumentException("non-empty turn must carry one lease per task");
                }
            }
        }

        private static Turn empty() {
            return new Turn(null, List.of(), List.of());
        }

        public List<WorkClassTask> tasks() {
            return tasks;
        }

        public boolean isEmpty() {
            return tasks.isEmpty();
        }

        public boolean isClosed() {
            return closed;
        }

        /** Checks every borrowed lease before the bounded chunk continues. */
        public synchronized void requireWithinBorrowedHold() {
            requireOpen();
            for (WorkClassResourcePool.ResourceLease lease : leases) {
                owner.resources.requireWithinBorrowedHold(lease);
            }
        }

        /**
         * Releases all tokens exactly once.  Hold-time/clock violations are
         * reported after every lease has been released so a failed close does
         * not strand shared capacity.
         */
        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            Throwable failure = null;
            if (owner != null) {
                for (WorkClassResourcePool.ResourceLease lease : leases) {
                    try {
                        owner.resources.requireWithinBorrowedHold(lease);
                    } catch (RuntimeException | Error holdFailure) {
                        if (failure == null) {
                            failure = holdFailure;
                        } else if (holdFailure != failure) {
                            failure.addSuppressed(holdFailure);
                        }
                    }
                }
                for (int index = leases.size() - 1; index >= 0; index--) {
                    try {
                        leases.get(index).close();
                    } catch (RuntimeException | Error releaseFailure) {
                        if (failure == null) {
                            failure = releaseFailure;
                        } else if (releaseFailure != failure) {
                            failure.addSuppressed(releaseFailure);
                        }
                    }
                }
                owner.finish(this);
            }
            closed = true;
            if (failure != null) {
                throwUnchecked(failure);
            }
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("WorkClass turn is already closed");
            }
            if (owner == null) {
                throw new IllegalStateException("empty WorkClass turn has no resource hold");
            }
        }
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked work-class cleanup failure", failure);
    }
}
