package com.nereusstream.delay.protocol;

/** Closed upload-handle response tags. */
public enum PayloadUploadHandleOutcome {
    ISSUED(1),
    RESERVATION_EXPIRED(2),
    RESERVATION_ABANDONED(3),
    RESERVATION_CLOSED(4),
    NOT_FOUND_OR_NOT_AUTHORIZED(5),
    SHARD_TRANSITIONING(6),
    SHARD_UNAVAILABLE(7),
    INTEGRITY_ERROR(8),
    OBJECT_STORE_UNAVAILABLE_RETRYABLE(9);

    private final int wireValue;

    PayloadUploadHandleOutcome(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadUploadHandleOutcome fromWire(final long value) {
        for (PayloadUploadHandleOutcome outcome : values()) {
            if (outcome.wireValue == value) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("unknown PayloadUploadHandleOutcome: " + value);
    }
}
