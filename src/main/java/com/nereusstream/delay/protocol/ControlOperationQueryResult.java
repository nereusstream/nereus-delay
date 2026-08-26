package com.nereusstream.delay.protocol;

/** Closed top-level result tag for a Control Operation query. */
public enum ControlOperationQueryResult {
    CURRENT(1),
    INVALID_RECEIPT(2),
    NOT_FOUND_OR_NOT_AUTHORIZED(3),
    INTEGRITY_ERROR(4);

    private final int wireValue;

    ControlOperationQueryResult(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlOperationQueryResult fromWire(final int value) {
        for (ControlOperationQueryResult result : values()) {
            if (result.wireValue == value) {
                return result;
            }
        }
        throw new IllegalArgumentException("unknown ControlOperationQueryResult: " + value);
    }
}
