package com.nereusstream.delay.gateway;

/** Gateway operation kinds included in idempotency body hashes. */
public enum GatewayOperationKind {
    SCHEDULE(1),
    PREPARE_LARGE_SCHEDULE(2),
    COMMIT_LARGE_SCHEDULE(3),
    CANCEL(4),
    RESCHEDULE(5);

    private final int wireValue;

    GatewayOperationKind(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static GatewayOperationKind fromWire(final long value) {
        for (GatewayOperationKind kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown Gateway operation kind: " + value);
    }
}
