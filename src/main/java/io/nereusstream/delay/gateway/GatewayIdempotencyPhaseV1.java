package io.nereusstream.delay.gateway;

/** Durable Gateway lifecycle independent from the enqueue outcome. */
public enum GatewayIdempotencyPhaseV1 {
    PREPARED,
    ACTIVE,
    QUIESCENT;

    public static GatewayIdempotencyPhaseV1 fromWire(final long value) {
        if (value < 1 || value > values().length) {
            throw new IllegalArgumentException("unknown Gateway idempotency phase: " + value);
        }
        return values()[(int) value - 1];
    }
}
