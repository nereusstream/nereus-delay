package io.nereusstream.delay.ownership;

import java.util.List;
import java.util.Objects;

import io.nereusstream.delay.scheduler.WorkClassTask;

/** Result of one bounded strict owner recovery turn. */
public record OwnerRecoveryTurn(List<SourceReplayOutcome> outcomes, boolean complete, int turnNumber,
                               boolean waitingForWorkClass, WorkClassTask pendingTask) {
    /** Compatibility constructor for a turn that did not wait for the dispatcher. */
    public OwnerRecoveryTurn(final List<SourceReplayOutcome> outcomes, final boolean complete,
                             final int turnNumber) {
        this(outcomes, complete, turnNumber, false, null);
    }

    public OwnerRecoveryTurn {
        outcomes = List.copyOf(Objects.requireNonNull(outcomes, "outcomes"));
        if (turnNumber < 0 || (complete && turnNumber == 0)) {
            throw new IllegalArgumentException("invalid recovery turn number");
        }
        if ((waitingForWorkClass && complete) || (waitingForWorkClass && pendingTask == null)
                || (!waitingForWorkClass && pendingTask != null)) {
            throw new IllegalArgumentException("invalid recovery work-class wait state");
        }
    }

    /** Returns whether the caller must schedule another bounded turn. */
    public boolean hasMore() {
        return !complete;
    }
}
