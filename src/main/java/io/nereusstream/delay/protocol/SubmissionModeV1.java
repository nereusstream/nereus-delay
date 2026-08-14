package io.nereusstream.delay.protocol;

/** Closed pre-I/O branch preference for a V1 schedule submission. */
public enum SubmissionModeV1 {
    MANAGED(1),
    AUTO_FAST(2);

    private final int wireValue;

    SubmissionModeV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SubmissionModeV1 fromWire(final long value) {
        for (SubmissionModeV1 mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown SubmissionModeV1: " + value);
    }
}
