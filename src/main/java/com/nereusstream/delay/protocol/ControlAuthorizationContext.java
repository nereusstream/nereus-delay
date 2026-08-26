package com.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Authenticator-produced projection consumed by the local Control RBAC gate.
 * The class contains hashes only; it never authenticates mTLS/OAuth material.
 */
public final class ControlAuthorizationContext {
    private static final int HASH_LENGTH = 32;

    private final byte[] actorIdHash;
    private final ControlRoleSet roleSet;
    private final byte[] tenantResourceScopeHash;

    public ControlAuthorizationContext(
            final byte[] actorIdHash, final ControlRoleSet roleSet, final byte[] tenantResourceScopeHash) {
        this.actorIdHash = fixed(actorIdHash, "actorIdHash");
        this.roleSet = Objects.requireNonNull(roleSet, "roleSet");
        this.tenantResourceScopeHash = fixed(tenantResourceScopeHash, "tenantResourceScopeHash");
    }

    public byte[] actorIdHash() {
        return Bytes.copy(actorIdHash);
    }

    public ControlRoleSet roleSet() {
        return roleSet;
    }

    public byte[] tenantResourceScopeHash() {
        return Bytes.copy(tenantResourceScopeHash);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
