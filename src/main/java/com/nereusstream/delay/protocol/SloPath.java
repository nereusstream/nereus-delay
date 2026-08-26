package com.nereusstream.delay.protocol;

/** Closed path projection used by SLO sample starts. */
public enum SloPath {
    NOT_APPLICABLE(1),
    ORDINARY_MANAGED(2),
    MANAGED_PULSAR_HANDOFF(3),
    AUTO_FAST_NATIVE(4);

    private final int wireValue;

    SloPath(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloPath fromWire(final long value) {
        for (SloPath path : values()) {
            if (path.wireValue == value) {
                return path;
            }
        }
        throw new IllegalArgumentException("unknown SloPath: " + value);
    }
}
