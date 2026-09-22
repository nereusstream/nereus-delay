package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.PublicKey;
import java.util.Objects;

/** First-application authentication only; neither a durable fence nor permission to mutate a Store. */
public final class TargetTimeFenceVerifier {
    public static final int MAX_WRITER_ID_BYTES = 256;
    private static final int MAX_AUTHOR_BYTES = MAX_WRITER_ID_BYTES + 17;

    private TargetTimeFenceVerifier() {}

    /**
     * Resolves the immutable Route tenant, historical accepted Fence writer/config generation and signing key,
     * activated protocol and time policy at this exact physical source. Null means a proved authorization denial;
     * absence, unavailable history or an incomplete read must throw. An accepting snapshot must remain protected
     * through the later Store commit. Current wall time/current writer membership cannot replace source authority.
     */
    @FunctionalInterface
    public interface Authority {
        Authorization resolve(
                TargetQuotaScope shardScope, AuthorIdentity writer, SystemMutation mutation, SourcePosition source);
    }

    /**
     * Authenticates the full recorded sample, source identity/config, digest/signature and historical source policy.
     * Returning false is a proved denial. Unknown/timeout throws; a canonical interval alone is not trusted evidence.
     * Must use the supplied source and recorded sample, never resample a live clock during replay.
     */
    @FunctionalInterface
    public interface EvidenceAuthority {
        boolean verifies(
                TargetQuotaScope shardScope,
                AuthorIdentity writer,
                SourcePosition source,
                TrustedUtcIntervalEvidence evidence);
    }

    public record Authorization(
            PublicKey writerKey, long safetyMarginMs, long maximumProofWidthMs, EvidenceAuthority evidence) {
        public Authorization {
            Objects.requireNonNull(writerKey, "writerKey");
            Objects.requireNonNull(evidence, "evidence");
            if (safetyMarginMs < 0 || maximumProofWidthMs < 0) {
                throw new IllegalArgumentException("invalid activated Target time fence policy");
            }
        }
    }

    /** Verifier-owned result; source dedupe, watermark overlay, accounting and atomic persistence remain separate. */
    public static final class Decision {
        private final TargetTimeFenceBody body;
        private final StableCode rejection;

        private Decision(final TargetTimeFenceBody body, final StableCode rejection) {
            this.body = body;
            this.rejection = rejection;
        }

        public TargetTimeFenceBody body() {
            return body;
        }

        public StableCode rejection() {
            return rejection;
        }
    }

    /** Invoke only for a first mutation; immutable first-result replay precedes new authorization decisions. */
    public static Decision decideFirstApplication(
            final TargetQuotaScope shardScope,
            final SystemMutation mutation,
            final SourcePosition source,
            final Authority authority) {
        Objects.requireNonNull(shardScope, "shardScope");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (shardScope.target() != null || !shardScope.shard().equals(source.shardId())) {
            throw new IllegalStateException("Target time fence requires the exact Shard root scope");
        }
        if (mutation.type() != SystemMutationType.TIME_FENCE
                || !mutation.shardId().equals(source.shardId())) {
            return unauthorized();
        }
        final TargetTimeFenceBody body;
        final AuthorIdentity author;
        // Only failures from these local, closed codecs are semantic denials. Never catch authority failures here.
        try {
            body = TargetTimeFenceBody.decode(mutation.canonicalBody());
            final byte[] authorBytes = mutation.authorIdentity();
            if (authorBytes.length > MAX_AUTHOR_BYTES) {
                return unauthorized();
            }
            author = AuthorIdentity.decode(authorBytes);
        } catch (IllegalArgumentException malformed) {
            return unauthorized();
        }
        if (!body.shard().equals(source.shardId())
                || body.retryUntil() != mutation.retryUntilEpochMs()
                || body.fenceKeyVersion() != mutation.signingKeyVersion()
                || !Bytes.constantTimeEquals(body.proofId(), mutation.logicalOperationIdentity())
                || author.kind() != AuthorIdentity.Kind.FENCE
                || author.first().length > MAX_WRITER_ID_BYTES) {
            return unauthorized();
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return new Decision(null, StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        final var authorized = authority.resolve(shardScope, author, mutation, source);
        if (authorized == null || !mutation.verifySignature(authorized.writerKey())) {
            return unauthorized();
        }
        final long minimumEarliest;
        try {
            minimumEarliest = Math.addExact(body.closeThrough(), authorized.safetyMarginMs());
        } catch (ArithmeticException overflow) {
            return unauthorized();
        }
        if (body.proof().earliestEpochMs() < minimumEarliest
                || body.proof().latestEpochMs() - body.proof().earliestEpochMs() > authorized.maximumProofWidthMs()
                || !authorized.evidence().verifies(shardScope, author, source, body.proof())) {
            return unauthorized();
        }
        return new Decision(body, null);
    }

    private static Decision unauthorized() {
        return new Decision(null, StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
    }
}
