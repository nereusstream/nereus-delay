package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Caller-driven, bounded maintenance turns; durable Close/expiry indexes remain the replay authority. */
public final class TargetReservationGcRuntime {
    public enum Lane {
        CLOSE,
        EXPIRY
    }

    public record Turn(
            Lane lane,
            WorkClassTask task,
            List<WorkClassTask> completedTasks,
            Optional<TargetReservationClosureWorkClassExecutor.Result> closeResult,
            Optional<TargetReservationExpiryWorkClassExecutor.Result> expiryResult) {
        public Turn {
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(task, "task");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
            Objects.requireNonNull(closeResult, "closeResult");
            Objects.requireNonNull(expiryResult, "expiryResult");
            if ((lane == Lane.CLOSE && expiryResult.isPresent()) || (lane == Lane.EXPIRY && closeResult.isPresent())) {
                throw new IllegalArgumentException("maintenance result belongs to another GC lane");
            }
        }

        public boolean pending() {
            return closeResult.isEmpty() && expiryResult.isEmpty();
        }
    }

    private final WorkClassExecutionRegistry workClasses;
    private final ShardId shard;
    private final long ownerEpoch;
    private final TargetReservationClosureWorkClassExecutor closes;
    private final TargetReservationExpiryWorkClassExecutor expiries;
    private long requestOrdinal;
    private boolean closeNext = true;
    private boolean running;
    private TargetReservationClosureWorkClassExecutor.Submission pendingClose;
    private TargetReservationExpiryWorkClassExecutor.Submission pendingExpiry;

    TargetReservationGcRuntime(
            final WorkClassExecutionRegistry workClasses,
            final ShardId shard,
            final long ownerEpoch,
            final TargetReservationClosureWorkClassExecutor closes,
            final TargetReservationExpiryWorkClassExecutor expiries) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.shard = Objects.requireNonNull(shard, "shard");
        if (ownerEpoch == 0) {
            throw new IllegalArgumentException("reservation GC requires a nonzero Owner epoch");
        }
        this.ownerEpoch = ownerEpoch;
        this.closes = Objects.requireNonNull(closes, "closes");
        this.expiries = Objects.requireNonNull(expiries, "expiries");
    }

    /** At most one GC action is outstanding; an unexecuted queued action survives a small shared turn budget. */
    public synchronized Turn runTurn(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        if (running) {
            throw new IllegalStateException("reservation GC turn cannot recurse");
        }
        if (pendingClose == null && pendingExpiry == null) {
            if (requestOrdinal == -1L) {
                throw new IllegalStateException("reservation GC request ordinal exhausted");
            }
            final long next = requestOrdinal + 1;
            final byte[] requestId = Bytes.concat(Bytes.u64beBits(ownerEpoch), Bytes.u64beBits(next));
            if (closeNext) {
                pendingClose =
                        closes.submit(new TargetReservationClosureWorkClassExecutor.SweepRequest(shard, requestId));
            } else {
                pendingExpiry = expiries.submit(new TargetReservationExpiryWorkClassExecutor.Request(shard, requestId));
            }
            requestOrdinal = next;
        }
        final Lane lane = pendingClose == null ? Lane.EXPIRY : Lane.CLOSE;
        final WorkClassTask task = pendingClose == null ? pendingExpiry.task() : pendingClose.task();
        final List<WorkClassTask> completed;
        running = true;
        try {
            completed = workClasses.runTurn(budget);
        } finally {
            running = false;
        }
        if (lane == Lane.CLOSE) {
            final var result = pendingClose.result();
            if (result.isPresent()) {
                pendingClose = null;
                closeNext = false;
            }
            return new Turn(lane, task, completed, result, Optional.empty());
        }
        final var result = pendingExpiry.result();
        if (result.isPresent()) {
            pendingExpiry = null;
            closeNext = true;
        }
        return new Turn(lane, task, completed, Optional.empty(), result);
    }
}
