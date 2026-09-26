package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.security.PublicKey;
import java.util.Objects;

/** Historical first-application authority for an existing EXPIRE_GENERATION body. Performs no Store write. */
public final class TargetExpireGenerationVerifier {
    private TargetExpireGenerationVerifier() {}

    /**
     * Resolves the accepted Owner signing key and time-source policy at this exact physical source. A null
     * authorization is a proved denial; unavailable history or an incomplete read must throw. The accepted
     * snapshot must remain valid through the later atomic source commit.
     */
    @FunctionalInterface
    public interface Authority {
        Authorization resolve(
                TargetQuotaScope scope, AuthorIdentity owner, SystemMutation mutation, SourcePosition source);
    }

    public record Authorization(
            PublicKey ownerKey, long maximumProofWidthMs, TargetTimeFenceVerifier.EvidenceAuthority evidence) {
        public Authorization {
            Objects.requireNonNull(ownerKey, "ownerKey");
            Objects.requireNonNull(evidence, "evidence");
            if (maximumProofWidthMs < 0) {
                throw new IllegalArgumentException("invalid Target expiry time policy");
            }
        }
    }

    public record Decision(TargetExpireGenerationBody body, StableCode rejection) {
        public Decision {
            if ((body == null) == (rejection == null)) {
                throw new IllegalArgumentException("Target expiry decision requires a body or rejection");
            }
        }
    }

    /** Immutable first-result replay must precede this historical decision. */
    public static Decision decideFirstApplication(
            final TargetQuotaScope scope,
            final SystemMutation mutation,
            final SourcePosition source,
            final Authority authority) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (scope.target() != null || !scope.shard().equals(source.shardId())) {
            throw new IllegalStateException("Target expiry requires the exact Shard root scope");
        }
        if (mutation.type() != SystemMutationType.EXPIRE_GENERATION
                || !mutation.shardId().equals(source.shardId())) {
            return unauthorized();
        }
        final TargetExpireGenerationBody body;
        final AuthorIdentity author;
        try {
            body = TargetExpireGenerationBody.decode(mutation.canonicalBody());
            author = AuthorIdentity.decode(mutation.authorIdentity());
        } catch (IllegalArgumentException malformed) {
            return unauthorized();
        }
        if (!body.shard().equals(source.shardId())
                || body.retryUntil() != mutation.retryUntilEpochMs()
                || author.kind() != AuthorIdentity.Kind.OWNER
                || !Bytes.constantTimeEquals(body.logicalOperationIdentity(), mutation.logicalOperationIdentity())) {
            return unauthorized();
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return new Decision(null, StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        final Authorization authorized = authority.resolve(scope, author, mutation, source);
        if (authorized == null || !mutation.verifySignature(authorized.ownerKey())) {
            return unauthorized();
        }
        final var proof = body.proof();
        if (proof.earliestEpochMs() < body.expireAt()
                || proof.latestEpochMs() - proof.earliestEpochMs() > authorized.maximumProofWidthMs()
                || !authorized.evidence().verifies(scope, author, source, proof)) {
            return unauthorized();
        }
        return new Decision(body, null);
    }

    private static Decision unauthorized() {
        return new Decision(null, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
    }
}
