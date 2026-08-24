package com.nereusstream.delay.protocol;

/** Closed units for SLO measurements. */
public enum SloThresholdUnitV1 {
    MILLISECONDS(1),
    BYTES(2),
    ROUNDS(3);

    private final int wireValue;

    SloThresholdUnitV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloThresholdUnitV1 fromWire(final long value) {
        for (SloThresholdUnitV1 unit : values()) {
            if (unit.wireValue == value) {
                return unit;
            }
        }
        throw new IllegalArgumentException("unknown SloThresholdUnitV1: " + value);
    }
}
