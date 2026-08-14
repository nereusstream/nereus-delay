package io.nereusstream.delay.gateway;

/** Gateway operation kinds included in idempotency body hashes. */
public enum GatewayOperationKindV1 {
    SCHEDULE(1),
    PREPARE_LARGE_SCHEDULE(2),
    COMMIT_LARGE_SCHEDULE(3),
    CANCEL(4),
    RESCHEDULE(5);

    private final int wireValue;

    GatewayOperationKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }

    public static GatewayOperationKindV1 fromWire(final long value) {
        for (GatewayOperationKindV1 kind : values()) {
            if (kind.wireValue == value) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown Gateway operation kind: " + value);
    }
}
