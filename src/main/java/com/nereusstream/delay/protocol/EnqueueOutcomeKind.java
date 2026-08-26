package com.nereusstream.delay.protocol;

/** Closed managed enqueue outcome tags. */
public enum EnqueueOutcomeKind {
    QUEUED(1),
    DEFINITELY_NOT_QUEUED(2),
    ENQUEUE_UNCERTAIN(3);

    private final int wireValue;

    EnqueueOutcomeKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static EnqueueOutcomeKind fromWire(final long value) {
        for (EnqueueOutcomeKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown EnqueueOutcomeKind: " + value);
    }
}
