package io.nereusstream.delay.protocol;

/** Closed V1 Command query result tags. */
public enum CommandQueryResult {
    PENDING(1),
    APPLIED(2),
    REJECTED(3),
    RESULT_EXPIRED(4),
    RESULT_EVIDENCE_EXPIRED(5),
    UNKNOWN(6),
    INVALID_RECEIPT(7),
    RECEIPT_MISMATCH(8),
    NOT_FOUND_OR_NOT_AUTHORIZED(9),
    SHARD_TRANSITIONING(10),
    SHARD_UNAVAILABLE(11),
    INTEGRITY_ERROR(12);

    private final int wireValue;

    CommandQueryResult(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static CommandQueryResult fromWire(final int value) {
        for (CommandQueryResult result : values()) {
            if (result.wireValue == value) {
                return result;
            }
        }
        throw new IllegalArgumentException("unknown CommandQueryResult: " + value);
    }
}
