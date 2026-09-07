package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetNativePolicyHead;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Separate current-head namespace for Target Native snapshots; old Profile-scoped publications cannot be used. */
public interface TargetNativePolicyAuthority {
    Optional<Publication> current(byte[] scopeDigest);

    /** Atomic CAS of the exact prior revision/head. Backend must validate next() in the same transaction. */
    Publication compareAndSet(byte[] scopeDigest, long expectedRevision, TargetNativePolicyHead next);

    default Publication requireCurrent(final byte[] scopeDigest) {
        return current(scopeDigest)
                .orElseThrow(() -> new IllegalStateException("Target Native common policy is unavailable"));
    }

    default Publication publish(
            final TargetNativePolicyTrust.PublisherPermission permission,
            final TargetNativePolicyTrust trust,
            final ControlAuthorizationContext actor,
            final TargetNativePolicyTrust.PublisherScopeProof proof,
            final SourcePosition source,
            final long expectedRevision,
            final TargetNativePolicySnapshot snapshot) {
        final var approved = Objects.requireNonNull(trust, "trust")
                .publisher(permission.scope().digest(), permission.keyGeneration(), source)
                .orElseThrow(() -> new IllegalArgumentException("Target Native publisher permission is not installed"));
        if (!approved.equals(permission)) {
            throw new IllegalArgumentException("Target Native publisher permission differs from source authority");
        }
        TargetNativePolicyTrust.requirePublisher(approved, actor, proof, snapshot, source);
        final byte[] scope = permission.scope().digest();
        final Publication prior = current(scope).orElse(null);
        if (expectedRevision < 0 || (prior == null ? 0 : prior.revision()) != expectedRevision) {
            throw new IllegalStateException("Target Native publication revision conflict");
        }
        final TargetNativePolicyHead next = TargetNativePolicyHead.next(prior == null ? null : prior.head(), snapshot);
        final Publication result = compareAndSet(scope, expectedRevision, next);
        if (!result.head().equals(next) || result.revision() <= expectedRevision) {
            throw new IllegalStateException("Target Native CAS authority returned another publication");
        }
        return result;
    }

    record Publication(long revision, TargetNativePolicyHead head) {
        public Publication {
            if (revision <= 0) {
                throw new IllegalArgumentException("Target Native publication revision must be positive");
            }
            Objects.requireNonNull(head, "head");
        }

        public boolean sameHead(final Publication other) {
            return other != null && revision == other.revision() && head.equals(other.head());
        }

        public void requireScope(final TargetNativePolicyScope scope) {
            head.snapshot().requireScope(scope);
            if (!Arrays.equals(head.snapshot().policyScopeDigest(), scope.digest())) {
                throw new IllegalArgumentException("Target Native publication belongs to another scope");
            }
        }
    }
}
