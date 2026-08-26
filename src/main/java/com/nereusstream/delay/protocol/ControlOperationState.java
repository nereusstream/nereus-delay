package com.nereusstream.delay.protocol;

/** Closed operation-level state for a Control Operation query. */
public enum ControlOperationState {
    PENDING(1),
    DISPATCHING(2),
    PARTIALLY_EFFECTIVE(3),
    IN_PROGRESS(4),
    SUCCEEDED(5),
    SUCCEEDED_WITH_OUTSTANDING(6),
    REJECTED(7),
    FAILED_BEFORE_EFFECT(8);

    private final int wireValue;

    ControlOperationState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlOperationState fromWire(final int value) {
        for (ControlOperationState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown ControlOperationState: " + value);
    }
}
