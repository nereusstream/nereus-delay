package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Canonical role-set projection used to bind an authenticated Control author. */
public final class ControlRoleSet {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-control-role-set\0");

    private final List<ControlRole> roles;
    private final byte[] digest;

    public ControlRoleSet(final Set<ControlRole> roles) {
        Objects.requireNonNull(roles, "roles");
        if (roles.isEmpty()) {
            throw new IllegalArgumentException("Control role set must not be empty");
        }
        final EnumSet<ControlRole> copy = EnumSet.copyOf(roles);
        final List<ControlRole> sorted = new ArrayList<>(copy);
        sorted.sort(Comparator.comparingInt(ControlRole::wireValue));
        this.roles = List.copyOf(sorted);
        this.digest = Bytes.sha256(DIGEST_DOMAIN, canonicalBytes());
    }

    public static ControlRoleSet of(final ControlRole first, final ControlRole... rest) {
        Objects.requireNonNull(first, "first");
        final EnumSet<ControlRole> roles = EnumSet.of(first);
        if (rest != null) {
            for (ControlRole role : rest) {
                roles.add(Objects.requireNonNull(role, "role"));
            }
        }
        return new ControlRoleSet(roles);
    }

    public List<ControlRole> roles() {
        return roles;
    }

    public boolean contains(final ControlRole role) {
        return roles.contains(Objects.requireNonNull(role, "role"));
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            for (ControlRole role : roles) {
                CanonicalProtobuf.uint32(output, 1, role.wireValue());
            }
        });
    }
}
