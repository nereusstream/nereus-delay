package io.nereusstream.delay.ownership;

import java.util.Objects;

public enum ShardLifecycleState {
    UNASSIGNED(1),
    ACQUIRING(2),
    RESTORING(3),
    CATCHING_UP(4),
    ACTIVE_FOR_COMMANDS(5),
    DRAINING(6),
    FENCED(7),
    FAILED(8);

    private final int wireValue;

    ShardLifecycleState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    /**
     * Returns whether a lease CAS may move between these lifecycle states.
     * Forward activation states may be skipped because source replay and
     * restore can complete before the authority CAS is observed; a fenced
     * lease can only be recycled through an unassigned/acquiring state.
     */
    public boolean canTransitionTo(final ShardLifecycleState next) {
        Objects.requireNonNull(next, "next");
        if (this == next) {
            return true;
        }
        return switch (this) {
            case UNASSIGNED -> next == ACQUIRING || next == FENCED || next == FAILED;
            case ACQUIRING -> next == RESTORING || next == CATCHING_UP || next == ACTIVE_FOR_COMMANDS
                    || next == FENCED || next == FAILED;
            case RESTORING -> next == CATCHING_UP || next == ACTIVE_FOR_COMMANDS
                    || next == FENCED || next == FAILED;
            case CATCHING_UP -> next == ACTIVE_FOR_COMMANDS || next == FENCED || next == FAILED;
            case ACTIVE_FOR_COMMANDS -> next == DRAINING || next == FENCED || next == FAILED;
            case DRAINING -> next == FENCED || next == FAILED;
            case FENCED -> next == UNASSIGNED || next == ACQUIRING || next == FAILED;
            case FAILED -> false;
        };
    }
}
