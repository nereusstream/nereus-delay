package com.nereusstream.delay.protocol;

/** Closed pre-I/O branch preference for a schedule submission. */
public enum SubmissionMode {
    MANAGED(1),
    AUTO_FAST(2);

    private final int wireValue;

    SubmissionMode(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SubmissionMode fromWire(final long value) {
        for (SubmissionMode mode : values()) {
            if (mode.wireValue == value) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown SubmissionMode: " + value);
    }
}
