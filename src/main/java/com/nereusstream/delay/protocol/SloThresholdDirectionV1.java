package com.nereusstream.delay.protocol;

/** Direction used by the conservative SLO final merge. */
public enum SloThresholdDirectionV1 {
    AT_MOST(1),
    AT_LEAST(2);

    private final int wireValue;

    SloThresholdDirectionV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloThresholdDirectionV1 fromWire(final long value) {
        for (SloThresholdDirectionV1 direction : values()) {
            if (direction.wireValue == value) {
                return direction;
            }
        }
        throw new IllegalArgumentException("unknown SloThresholdDirectionV1: " + value);
    }
}
