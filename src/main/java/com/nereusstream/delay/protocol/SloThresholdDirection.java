package com.nereusstream.delay.protocol;

/** Direction used by the conservative SLO final merge. */
public enum SloThresholdDirection {
    AT_MOST(1),
    AT_LEAST(2);

    private final int wireValue;

    SloThresholdDirection(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloThresholdDirection fromWire(final long value) {
        for (SloThresholdDirection direction : values()) {
            if (direction.wireValue == value) {
                return direction;
            }
        }
        throw new IllegalArgumentException("unknown SloThresholdDirection: " + value);
    }
}
