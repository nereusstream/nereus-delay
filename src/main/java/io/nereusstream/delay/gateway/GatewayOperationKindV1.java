package io.nereusstream.delay.gateway;

/** Gateway operation kinds included in idempotency body hashes. */
public enum GatewayOperationKindV1 {
    SCHEDULE(1);

    private final int wireValue;

    GatewayOperationKindV1(final int wireValue) {
        this.wireValue = wireValue;
    }

    public int wireValue() {
        return wireValue;
    }
}
