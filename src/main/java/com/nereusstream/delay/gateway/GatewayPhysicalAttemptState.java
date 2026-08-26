package com.nereusstream.delay.gateway;

/** Durable state of one physical Gateway attempt. */
public enum GatewayPhysicalAttemptState {
    STARTED,
    QUEUED,
    DEFINITELY_NOT_QUEUED,
    UNCERTAIN;

    public static GatewayPhysicalAttemptState fromWire(final long value) {
        if (value < 1 || value > values().length) {
            throw new IllegalArgumentException("unknown Gateway physical attempt state: " + value);
        }
        return values()[(int) value - 1];
    }
}
