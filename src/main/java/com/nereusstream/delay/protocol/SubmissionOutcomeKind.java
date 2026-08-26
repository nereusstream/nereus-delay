package com.nereusstream.delay.protocol;

/** Closed managed/native submission outcome tags. */
public enum SubmissionOutcomeKind {
    MANAGED(1),
    NATIVE_RECEIPT(2),
    NATIVE_DEFINITELY_NOT_QUEUED(3),
    NATIVE_ENQUEUE_UNCERTAIN(4);

    private final int wireValue;

    SubmissionOutcomeKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static SubmissionOutcomeKind fromWire(final long value) {
        for (SubmissionOutcomeKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown SubmissionOutcomeKind: " + value);
    }
}
