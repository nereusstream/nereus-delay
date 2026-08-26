package com.nereusstream.delay.protocol;

/** Closed circuit state for a destination Lane. */
public enum LaneCircuitState {
    CLOSED(1),
    OPEN(2),
    HALF_OPEN(3);

    private final int wireValue;

    LaneCircuitState(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static LaneCircuitState fromWire(final long value) {
        for (LaneCircuitState state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown LaneCircuitState: " + value);
    }
}
