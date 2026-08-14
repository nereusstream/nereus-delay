package io.nereusstream.delay.gateway;

/** Durable state of one physical Gateway attempt. */
public enum GatewayPhysicalAttemptStateV1 {
    STARTED,
    QUEUED,
    DEFINITELY_NOT_QUEUED,
    UNCERTAIN
}
