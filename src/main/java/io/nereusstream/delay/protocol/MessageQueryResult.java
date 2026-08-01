package io.nereusstream.delay.protocol;

/** Closed V1 Message query result tags. */
public enum MessageQueryResult {
    RESERVED(1),
    ACTIVE(2),
    TERMINAL(3),
    IDENTITY_RETIRED(4),
    UNKNOWN(5),
    INVALID_RECEIPT(6),
    RECEIPT_MISMATCH(7),
    NOT_FOUND_OR_NOT_AUTHORIZED(8),
    SHARD_TRANSITIONING(9),
    SHARD_UNAVAILABLE(10),
    INTEGRITY_ERROR(11);

    private final int wireValue;

    MessageQueryResult(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static MessageQueryResult fromWire(final int value) {
        for (MessageQueryResult result : values()) {
            if (result.wireValue == value) {
                return result;
            }
        }
        throw new IllegalArgumentException("unknown MessageQueryResult: " + value);
    }
}
