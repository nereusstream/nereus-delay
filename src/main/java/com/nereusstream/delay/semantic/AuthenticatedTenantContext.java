package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import java.util.Arrays;
import java.util.Objects;

/**
 * Trusted tenant authority supplied by an entry composition.  It contains
 * only digests used for authorization and routing; no caller tenant string is
 * copied into NDL1 command bytes.
 */
public final class AuthenticatedTenantContext {
    private final byte[] authenticatedTenantScopeHash;
    private final byte[] tenantRoutingScope;
    private final byte[] principalScopeHash;

    public AuthenticatedTenantContext(
            final byte[] authenticatedTenantScopeHash,
            final byte[] tenantRoutingScope,
            final byte[] principalScopeHash) {
        this.authenticatedTenantScopeHash = nonZero(authenticatedTenantScopeHash, "authenticatedTenantScopeHash");
        this.tenantRoutingScope = nonZero(tenantRoutingScope, "tenantRoutingScope");
        this.principalScopeHash = nonZero(principalScopeHash, "principalScopeHash");
    }

    public byte[] authenticatedTenantScopeHash() {
        return Bytes.copy(authenticatedTenantScopeHash);
    }

    public byte[] tenantRoutingScope() {
        return Bytes.copy(tenantRoutingScope);
    }

    public byte[] principalScopeHash() {
        return Bytes.copy(principalScopeHash);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof AuthenticatedTenantContext that
                && Arrays.equals(authenticatedTenantScopeHash, that.authenticatedTenantScopeHash)
                && Arrays.equals(tenantRoutingScope, that.tenantRoutingScope)
                && Arrays.equals(principalScopeHash, that.principalScopeHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(authenticatedTenantScopeHash),
                Arrays.hashCode(tenantRoutingScope),
                Arrays.hashCode(principalScopeHash));
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
