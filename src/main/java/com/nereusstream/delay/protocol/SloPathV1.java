package com.nereusstream.delay.protocol;

/** Closed path projection used by SLO sample starts. */
public enum SloPathV1 {
    NOT_APPLICABLE(1),
    ORDINARY_MANAGED(2),
    MANAGED_PULSAR_HANDOFF(3),
    AUTO_FAST_NATIVE(4);

    private final int wireValue;

    SloPathV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloPathV1 fromWire(final long value) {
        for (SloPathV1 path : values()) {
            if (path.wireValue == value) {
                return path;
            }
        }
        throw new IllegalArgumentException("unknown SloPathV1: " + value);
    }
}
