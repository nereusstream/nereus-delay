package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Historical, source-fenced authority projections. Request objects cannot be installed as trust. */
public interface TargetNativePolicyTrust {
    Optional<PublisherPermission> publisher(byte[] scopeDigest, int keyGeneration, SourcePosition asOf);

    Optional<Activation> activation(byte[] scopeDigest, long generation);

    Optional<MemberApproval> member(byte[] grantDigest, byte[] scopeDigest, SourcePosition asOf);

    /** A Control-approved issuer for exactly one common scope, including its authenticated actor and lease bound. */
    record PublisherPermission(
            TargetNativePolicyScope scope,
            ControlAuthor author,
            int keyGeneration,
            PublicKey key,
            long maximumLeaseMs,
            SourcePosition activeFrom) {
        public PublisherPermission {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(author, "author");
            Objects.requireNonNull(key, "key");
            TargetSourcePosition.requireBounded(activeFrom);
            if (keyGeneration == 0
                    || maximumLeaseMs <= 0
                    || !scope.sourceShard().equals(activeFrom.shardId())
                    || !Arrays.equals(scope.controlResourceScope(), author.tenantResourceScopeHash())) {
                throw new IllegalArgumentException("invalid source-bound Target Native publisher permission");
            }
        }
    }

    /** Activation binds the full signed snapshot, not merely a generation or key. */
    record Activation(TargetNativePolicyScope scope, TargetNativePolicySnapshot snapshot, SourcePosition activeFrom) {
        public Activation {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(snapshot, "snapshot");
            TargetSourcePosition.requireBounded(activeFrom);
            snapshot.requireScope(scope);
            if (!scope.sourceShard().equals(activeFrom.shardId())) {
                throw new IllegalArgumentException("Target Native activation belongs to another Shard");
            }
        }
    }

    /** First-binding approval. Closing it never installs a per-Profile runtime switch on an existing Native prefix. */
    record MemberApproval(
            TargetMembershipGrant grant,
            TargetNativePolicyScope scope,
            SourcePosition activeFrom,
            SourcePosition closedAt) {
        public MemberApproval {
            Objects.requireNonNull(grant, "grant");
            Objects.requireNonNull(scope, "scope");
            TargetSourcePosition.requireBounded(activeFrom);
            requireAtOrBefore(grant.activationSource(), activeFrom);
            if (grant.activationSource().compareTo(activeFrom) == 0) {
                throw new IllegalArgumentException("Native member approval must follow the separately applied grant");
            }
            if (!scope.sourceShard().equals(activeFrom.shardId())
                    || !scope.target().equals(grant.offered().target())
                    || !Arrays.equals(scope.dispatchRef(), grant.offered().digest())
                    || !Arrays.equals(scope.controlRef(), grant.controls().digest())) {
                throw new IllegalArgumentException("Native member approval differs from the complete grant/scope");
            }
            if (closedAt != null) {
                TargetSourcePosition.requireBounded(closedAt);
                if (closedAt.compareTo(activeFrom) <= 0) {
                    throw new IllegalArgumentException("Native member closure must follow activation");
                }
            }
        }

        public boolean allowsFirstBinding(final SourcePosition source) {
            requireAtOrBefore(activeFrom, source);
            if (source.compareTo(activeFrom) == 0) {
                return false;
            }
            if (closedAt == null) {
                return true;
            }
            final int order = source.compareTo(closedAt);
            if (order == 0 && !Arrays.equals(source.canonicalBytes(), closedAt.canonicalBytes())) {
                throw new IllegalArgumentException("Native member closure has conflicting source bytes");
            }
            return order < 0;
        }
    }

    @FunctionalInterface
    interface PublisherScopeProof {
        boolean covers(PublisherPermission permission, ControlAuthorizationContext actor, SourcePosition at);
    }

    /** Authorizes creation/publication. It does not mutate a current head or record source activation. */
    static void requirePublisher(
            final PublisherPermission permission,
            final ControlAuthorizationContext actor,
            final PublisherScopeProof proof,
            final TargetNativePolicySnapshot snapshot,
            final SourcePosition at) {
        Objects.requireNonNull(permission, "permission");
        Objects.requireNonNull(actor, "actor");
        requireAtOrBefore(permission.activeFrom(), at);
        final var expected = permission.author();
        if (!actor.roleSet().contains(ControlRole.TENANT_POLICY_ADMINISTRATOR)
                || !actor.roleSet().contains(ControlRole.PLATFORM_OPERATOR)
                || !Arrays.equals(expected.operationActorIdHash(), actor.actorIdHash())
                || !Arrays.equals(
                        expected.authenticatedRoleSetHash(), actor.roleSet().digest())
                || !Arrays.equals(expected.tenantResourceScopeHash(), actor.tenantResourceScopeHash())
                || !Objects.requireNonNull(proof, "proof").covers(permission, actor, at)) {
            throw new IllegalArgumentException("Target Native publisher actor/roles/scope are not authorized");
        }
        requireSigned(permission, snapshot);
    }

    /** Historical verification at Claim/Admission or its frozen Admission position; no mutable current key lookup. */
    default Activation requireTrusted(
            final TargetNativePolicyScope scope, final TargetNativePolicySnapshot snapshot, final SourcePosition at) {
        TargetSourcePosition.requireBounded(at);
        snapshot.requireScope(scope);
        if (!scope.sourceShard().equals(at.shardId())) {
            throw new IllegalArgumentException("Target Native trust position belongs to another Shard");
        }
        final var active = activation(scope.digest(), snapshot.generation())
                .orElseThrow(() -> new IllegalArgumentException("Target Native generation is not source-activated"));
        if (!scope.equals(active.scope()) || !snapshot.equals(active.snapshot())) {
            throw new IllegalArgumentException("Target Native activation is for another exact scope/snapshot");
        }
        requireAtOrBefore(active.activeFrom(), at);
        final var permission = publisher(scope.digest(), snapshot.issuerKeyGeneration(), active.activeFrom())
                .orElseThrow(
                        () -> new IllegalArgumentException("Target Native issuer was not authorized at activation"));
        requireAtOrBefore(permission.activeFrom(), active.activeFrom());
        if (!scope.equals(permission.scope())) {
            throw new IllegalArgumentException("Target Native publisher permission does not cover this scope");
        }
        requireSigned(permission, snapshot);
        return active;
    }

    private static void requireSigned(final PublisherPermission permission, final TargetNativePolicySnapshot snapshot) {
        snapshot.requireScope(permission.scope());
        if (snapshot.issuerKeyGeneration() != permission.keyGeneration()
                || snapshot.validUntilEpochMs() - snapshot.validFromEpochMs() > permission.maximumLeaseMs()
                || !snapshot.verifySignature(permission.key())) {
            throw new IllegalArgumentException("Target Native signing key, signature or lease bound is not authorized");
        }
    }

    static void requireAtOrBefore(final SourcePosition left, final SourcePosition right) {
        TargetSourcePosition.requireBounded(left);
        TargetSourcePosition.requireBounded(right);
        final int order = left.compareTo(right);
        if (order > 0 || (order == 0 && !Arrays.equals(left.canonicalBytes(), right.canonicalBytes()))) {
            throw new IllegalArgumentException(
                    "Target Native authority source is future or conflicts at the same offset");
        }
    }
}
