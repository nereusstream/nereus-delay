package io.nereusstream.delay.gateway;

import io.nereusstream.delay.semantic.AuthenticatedTenantContext;

/** mTLS/JWT/service-account authority boundary; implementations must fail closed. */
@FunctionalInterface
public interface GatewayTenantAuthority {
    AuthenticatedTenantContext authenticate(GatewayPeerContext peerContext);
}
