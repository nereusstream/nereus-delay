package com.nereusstream.delay.protocol;

/** Closed target-level state for a Control Operation query. */
public enum TargetMarkerState {
    PENDING(1),
    ENQUEUE_UNCERTAIN(2),
    QUEUED(3),
    EFFECTIVE(4),
    MATERIALIZING(5),
    COMPLETED(6),
    REJECTED(7),
    FAILED_BEFORE_EFFECT(8);

    private final int wireValue;

    TargetMarkerState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static TargetMarkerState fromWire(final int value) {
        for (TargetMarkerState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown TargetMarkerState: " + value);
    }
}
