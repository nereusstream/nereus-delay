package com.nereusstream.delay.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Bounded local audit sink used by deterministic ingress tests. */
public final class InMemoryGatewayAuditSink implements GatewayAuditSink {
    private final int maxEvents;
    private final List<byte[]> events = new ArrayList<>();

    public InMemoryGatewayAuditSink(final int maxEvents) {
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be positive");
        }
        this.maxEvents = maxEvents;
    }

    @Override
    public synchronized void record(final GatewayAuditEvent event) {
        Objects.requireNonNull(event, "event");
        if (events.size() >= maxEvents) {
            throw new IllegalStateException("Gateway audit sink capacity is exhausted");
        }
        events.add(event.canonicalBytes());
    }

    public synchronized List<byte[]> canonicalEvents() {
        return events.stream().map(com.nereusstream.delay.protocol.Bytes::copy).toList();
    }
}
