package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.List;
import java.util.Objects;

/** Result of one bounded strict owner recovery turn. */
public record OwnerRecoveryTurn(
        List<SourceReplayOutcome> outcomes,
        boolean complete,
        int turnNumber,
        boolean waitingForWorkClass,
        WorkClassTask pendingTask,
        com.nereusstream.delay.store.BoundedReadBudget.Exhaustion readIncompleteReason) {
    public OwnerRecoveryTurn(
            final List<SourceReplayOutcome> outcomes,
            final boolean complete,
            final int turnNumber,
            final boolean waitingForWorkClass,
            final WorkClassTask pendingTask) {
        this(outcomes, complete, turnNumber, waitingForWorkClass, pendingTask, null);
    }

    /** Compatibility constructor for a turn that did not wait for the dispatcher. */
    public OwnerRecoveryTurn(final List<SourceReplayOutcome> outcomes, final boolean complete, final int turnNumber) {
        this(outcomes, complete, turnNumber, false, null);
    }

    public OwnerRecoveryTurn {
        outcomes = List.copyOf(Objects.requireNonNull(outcomes, "outcomes"));
        if (turnNumber < 0 || (complete && turnNumber == 0)) {
            throw new IllegalArgumentException("invalid recovery turn number");
        }
        if (readIncompleteReason != null && (complete || waitingForWorkClass)) {
            throw new IllegalArgumentException("an incomplete read cannot be complete or waiting on a queued task");
        }
        if ((waitingForWorkClass && complete)
                || (waitingForWorkClass && pendingTask == null)
                || (!waitingForWorkClass && pendingTask != null)) {
            throw new IllegalArgumentException("invalid recovery work-class wait state");
        }
    }

    /** Returns whether the caller must schedule another bounded turn. */
    public boolean hasMore() {
        return !complete;
    }
}
