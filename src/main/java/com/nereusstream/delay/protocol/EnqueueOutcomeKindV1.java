package com.nereusstream.delay.protocol;

/** Closed managed enqueue outcome tags. */
public enum EnqueueOutcomeKindV1 {
    QUEUED(1),
    DEFINITELY_NOT_QUEUED(2),
    ENQUEUE_UNCERTAIN(3);

    private final int wireValue;

    EnqueueOutcomeKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static EnqueueOutcomeKindV1 fromWire(final long value) {
        for (EnqueueOutcomeKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown EnqueueOutcomeKindV1: " + value);
    }
}
