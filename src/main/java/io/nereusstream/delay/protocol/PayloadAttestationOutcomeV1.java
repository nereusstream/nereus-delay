package io.nereusstream.delay.protocol;

/** Closed payload attestation response tags. */
public enum PayloadAttestationOutcomeV1 {
    ATTESTED(1),
    OBJECT_NOT_READY_RETRYABLE(2),
    OBJECT_STORE_UNAVAILABLE_RETRYABLE(3),
    OBJECT_IDENTITY_CONFLICT(4),
    RESERVATION_EXPIRED(5),
    RESERVATION_ABANDONED(6),
    RESERVATION_CLOSED(7),
    NOT_FOUND_OR_NOT_AUTHORIZED(8),
    SHARD_TRANSITIONING(9),
    SHARD_UNAVAILABLE(10),
    INTEGRITY_ERROR(11);

    private final int wireValue;

    PayloadAttestationOutcomeV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static PayloadAttestationOutcomeV1 fromWire(final long value) {
        for (PayloadAttestationOutcomeV1 outcome : values()) {
            if (outcome.wireValue == value) {
                return outcome;
            }
        }
        throw new IllegalArgumentException("unknown PayloadAttestationOutcomeV1: " + value);
    }
}
