package io.nereusstream.delay.protocol;

/** V1 timeout policy; a missing Final is always a bad observation. */
public enum SloTimeoutTreatmentV1 {
    BAD(1);

    private final int wireValue;

    SloTimeoutTreatmentV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SloTimeoutTreatmentV1 fromWire(final long value) {
        if (value == BAD.wireValue) {
            return BAD;
        }
        throw new IllegalArgumentException("unknown SloTimeoutTreatmentV1: " + value);
    }
}
