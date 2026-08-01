package io.nereusstream.delay.protocol;

/** Closed upload-handle response tags. */
public enum PayloadUploadHandleOutcomeV1 {
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

    PayloadUploadHandleOutcomeV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadUploadHandleOutcomeV1 fromWire(final long value) {
        for (PayloadUploadHandleOutcomeV1 outcome : values()) {
            if (outcome.wireValue == value) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("unknown PayloadUploadHandleOutcomeV1: " + value);
    }
}
