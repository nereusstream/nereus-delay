package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.util.Objects;

/**
 * Source-applied authority, not a Profile catalog or a digest-existence test.
 * Implementations must return only authenticated, authorized source registrations with their exact closure history.
 * Issuance must prove tenant/Profile authorization, complete blocking groups and permission to use any legal
 * common credential provider under the offered contract. Storage/authentication failures must propagate.
 * The snapshot must be fenced with the same Owner/source/Store view as the eventual binding mutation.
 */
@FunctionalInterface
public interface TargetMembershipAuthority {
    AppliedGrant resolve(byte[] exactGrantRef);

    /** A closure stops subsequent first bindings; it never revokes or rewrites prior accepted obligations. */
    record AppliedGrant(TargetMembershipGrant grant, SourcePosition closedAt) {
        public AppliedGrant {
            Objects.requireNonNull(grant, "grant");
            if (closedAt != null
                    && TargetSourcePosition.requireBounded(closedAt).compareTo(grant.activationSource()) <= 0) {
                throw new IllegalArgumentException("Target membership closure must follow activation");
            }
        }

        public boolean allowsFirstBinding(final SourcePosition source) {
            TargetSourcePosition.requireBounded(source);
            final int activationOrder = source.compareTo(grant.activationSource());
            if (activationOrder == 0
                    && !java.util.Arrays.equals(
                            source.canonicalBytes(), grant.activationSource().canonicalBytes())) {
                throw new IllegalArgumentException("conflicting Target membership activation source identity");
            }
            if (activationOrder <= 0) {
                return false;
            }
            if (closedAt == null) {
                return true;
            }
            final int order = source.compareTo(closedAt);
            if (order == 0 && !java.util.Arrays.equals(source.canonicalBytes(), closedAt.canonicalBytes())) {
                throw new IllegalArgumentException("conflicting Target membership closure source identity");
            }
            return order < 0;
        }
    }
}
