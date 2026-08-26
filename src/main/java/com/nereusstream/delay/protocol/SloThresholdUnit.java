package com.nereusstream.delay.protocol;

/** Closed units for SLO measurements. */
public enum SloThresholdUnit {
    MILLISECONDS(1),
    BYTES(2),
    ROUNDS(3);

    private final int wireValue;

    SloThresholdUnit(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloThresholdUnit fromWire(final long value) {
        for (SloThresholdUnit unit : values()) {
            if (unit.wireValue == value) {
                return unit;
            }
        }
        throw new IllegalArgumentException("unknown SloThresholdUnit: " + value);
    }
}
