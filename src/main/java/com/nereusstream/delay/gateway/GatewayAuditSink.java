package com.nereusstream.delay.gateway;

/** Durable audit sink boundary; production wiring must provide an authenticated implementation. */
@FunctionalInterface
public interface GatewayAuditSink {
    void record(GatewayAuditEvent event);
}
