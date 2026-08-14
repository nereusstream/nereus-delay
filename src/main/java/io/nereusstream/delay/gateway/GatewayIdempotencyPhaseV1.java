package io.nereusstream.delay.gateway;

/** Durable Gateway lifecycle independent from the enqueue outcome. */
public enum GatewayIdempotencyPhaseV1 {
    PREPARED,
    ACTIVE,
    QUIESCENT
}
