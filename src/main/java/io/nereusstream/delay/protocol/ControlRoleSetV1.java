package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Canonical role-set projection used to bind an authenticated Control author. */
public final class ControlRoleSetV1 {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-control-role-set-v1\0");

    private final List<ControlRoleV1> roles;
    private final byte[] digest;

    public ControlRoleSetV1(final Set<ControlRoleV1> roles) {
        Objects.requireNonNull(roles, "roles");
        if (roles.isEmpty()) {
            throw new IllegalArgumentException("Control role set must not be empty");
        }
        final EnumSet<ControlRoleV1> copy = EnumSet.copyOf(roles);
        final List<ControlRoleV1> sorted = new ArrayList<>(copy);
        sorted.sort(Comparator.comparingInt(ControlRoleV1::wireValue));
        this.roles = List.copyOf(sorted);
        this.digest = Bytes.sha256(DIGEST_DOMAIN, canonicalBytes());
    }

    public static ControlRoleSetV1 of(final ControlRoleV1 first, final ControlRoleV1... rest) {
        Objects.requireNonNull(first, "first");
        final EnumSet<ControlRoleV1> roles = EnumSet.of(first);
        if (rest != null) {
            for (ControlRoleV1 role : rest) {
                roles.add(Objects.requireNonNull(role, "role"));
            }
        }
        return new ControlRoleSetV1(roles);
    }

    public List<ControlRoleV1> roles() {
        return roles;
    }

    public boolean contains(final ControlRoleV1 role) {
        return roles.contains(Objects.requireNonNull(role, "role"));
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            for (ControlRoleV1 role : roles) {
                CanonicalProtobuf.uint32(output, 1, role.wireValue());
            }
        });
    }
}
