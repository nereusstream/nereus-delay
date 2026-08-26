package com.nereusstream.delay.protocol;

/** timeout policy; a missing Final is always a bad observation. */
public enum SloTimeoutTreatment {
    BAD(1);

    private final int wireValue;

    SloTimeoutTreatment(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloTimeoutTreatment fromWire(final long value) {
        if (value == BAD.wireValue) {
            return BAD;
        }
        throw new IllegalArgumentException("unknown SloTimeoutTreatment: " + value);
    }
}
