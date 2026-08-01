package io.nereusstream.delay.protocol;

/** Closed top-level result tag for a Control Operation query. */
public enum ControlOperationQueryResultV1 {
    CURRENT(1),
    INVALID_RECEIPT(2),
    NOT_FOUND_OR_NOT_AUTHORIZED(3),
    INTEGRITY_ERROR(4);

    private final int wireValue;

    ControlOperationQueryResultV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static ControlOperationQueryResultV1 fromWire(final int value) {
        for (ControlOperationQueryResultV1 result : values()) {
            if (result.wireValue == value) {
                return result;
            }
        }
        throw new IllegalArgumentException("unknown ControlOperationQueryResultV1: " + value);
    }
}
