package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import java.util.Objects;
import java.util.Optional;

/** Source-independent current-head authority used by policy readers and issuers. */
public interface HandoffPolicyAuthority {
    Optional<Publication> current(byte[] policyScopeDigest);

    Publication compareAndSet(byte[] policyScopeDigest, long expectedOxiaVersion, HandoffPolicyHead next);

    default Publication requireCurrent(final byte[] policyScopeDigest) {
        return current(policyScopeDigest)
                .orElseThrow(() -> new IllegalStateException("handoff policy head is not published"));
    }

    /** Publishes a complete head through the same CAS boundary used by Oxia. */
    default Publication publish(
            final byte[] policyScopeDigest, final long expectedOxiaVersion, final HandoffPolicyHead next) {
        return compareAndSet(policyScopeDigest, expectedOxiaVersion, next);
    }

    record Publication(long oxiaVersion, HandoffPolicyHead head) {
        public Publication {
            if (oxiaVersion <= 0) {
                throw new IllegalArgumentException("policy head Oxia version must be positive");
            }
            Objects.requireNonNull(head, "head");
        }

        public boolean sameHead(final Publication other) {
            return other != null
                    && oxiaVersion == other.oxiaVersion
                    && Bytes.constantTimeEquals(head.canonicalBytes(), other.head.canonicalBytes());
        }
    }
}
