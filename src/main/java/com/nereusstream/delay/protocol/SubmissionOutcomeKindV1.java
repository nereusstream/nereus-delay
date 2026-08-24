package com.nereusstream.delay.protocol;

/** Closed managed/native submission outcome tags. */
public enum SubmissionOutcomeKindV1 {
    MANAGED(1),
    NATIVE_RECEIPT(2),
    NATIVE_DEFINITELY_NOT_QUEUED(3),
    NATIVE_ENQUEUE_UNCERTAIN(4);

    private final int wireValue;

    SubmissionOutcomeKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SubmissionOutcomeKindV1 fromWire(final long value) {
        for (SubmissionOutcomeKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown SubmissionOutcomeKindV1: " + value);
    }
}
