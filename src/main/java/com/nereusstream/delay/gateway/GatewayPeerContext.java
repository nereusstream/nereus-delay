package com.nereusstream.delay.gateway;

import io.grpc.Attributes;
import io.grpc.Metadata;
import java.util.Objects;

/** Transport-owned peer context consumed by the tenant authority and never persisted in V1 records. */
public record GatewayPeerContext(Metadata headers, Attributes transportAttributes) {
    public GatewayPeerContext {
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(transportAttributes, "transportAttributes");
    }
}
