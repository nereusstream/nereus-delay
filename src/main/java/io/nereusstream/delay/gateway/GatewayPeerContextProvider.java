package io.nereusstream.delay.gateway;

/** Resolves the transport-owned peer context for one gRPC call. */
@FunctionalInterface
public interface GatewayPeerContextProvider {
    GatewayPeerContext current();
}
