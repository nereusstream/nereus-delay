package com.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Authenticator-produced projection consumed by the local Control RBAC gate.
 * The class contains hashes only; it never authenticates mTLS/OAuth material.
 */
public final class ControlAuthorizationContextV1 {
    private static final int HASH_LENGTH = 32;

    private final byte[] actorIdHash;
    private final ControlRoleSetV1 roleSet;
    private final byte[] tenantResourceScopeHash;

    public ControlAuthorizationContextV1(
            final byte[] actorIdHash, final ControlRoleSetV1 roleSet, final byte[] tenantResourceScopeHash) {
        this.actorIdHash = fixed(actorIdHash, "actorIdHash");
        this.roleSet = Objects.requireNonNull(roleSet, "roleSet");
        this.tenantResourceScopeHash = fixed(tenantResourceScopeHash, "tenantResourceScopeHash");
    }

    public byte[] actorIdHash() {
        return Bytes.copy(actorIdHash);
    }

    public ControlRoleSetV1 roleSet() {
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
