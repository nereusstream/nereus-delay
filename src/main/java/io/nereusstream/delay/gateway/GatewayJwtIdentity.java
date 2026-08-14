package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Objects;

/** Verified JWT projection used to construct the non-wire tenant context. */
public record GatewayJwtIdentity(byte[] authenticatedTenantScopeHash, byte[] tenantRoutingScope,
                                 byte[] principalScopeHash) {
    public GatewayJwtIdentity {
        Objects.requireNonNull(authenticatedTenantScopeHash, "authenticatedTenantScopeHash");
        Objects.requireNonNull(tenantRoutingScope, "tenantRoutingScope");
        Objects.requireNonNull(principalScopeHash, "principalScopeHash");
        Bytes.requireLength(authenticatedTenantScopeHash, 32, "authenticatedTenantScopeHash");
        Bytes.requireLength(tenantRoutingScope, 32, "tenantRoutingScope");
        Bytes.requireLength(principalScopeHash, 32, "principalScopeHash");
        authenticatedTenantScopeHash = Bytes.copy(authenticatedTenantScopeHash);
        tenantRoutingScope = Bytes.copy(tenantRoutingScope);
        principalScopeHash = Bytes.copy(principalScopeHash);
    }

    @Override
    public byte[] authenticatedTenantScopeHash() {
        return Bytes.copy(authenticatedTenantScopeHash);
    }

    @Override
    public byte[] tenantRoutingScope() {
        return Bytes.copy(tenantRoutingScope);
    }

    @Override
    public byte[] principalScopeHash() {
        return Bytes.copy(principalScopeHash);
    }

}
