package com.nereusstream.delay.protocol;

/** Closed V1 circuit state for a destination Lane. */
public enum LaneCircuitStateV1 {
    CLOSED(1),
    OPEN(2),
    HALF_OPEN(3);

    private final int wireValue;

    LaneCircuitStateV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static LaneCircuitStateV1 fromWire(final long value) {
        for (LaneCircuitStateV1 state : values()) {
            if (state.wireValue == value) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown LaneCircuitStateV1: " + value);
    }
}
