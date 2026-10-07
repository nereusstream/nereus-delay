package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.PublicKey;
import java.util.Objects;

/** First-application authentication for the shared v1 Publish Outcome on the Target source path. */
public final class TargetPublishOutcomeVerifier {
    private TargetPublishOutcomeVerifier() {}

    /** Historical accepted-writer, activated-tuple, and current-owner snapshot for one source position. */
    @FunctionalInterface
    public interface Authority {
        Authorization resolve(
                TargetQuotaScope shardScope, AuthorIdentity writer, SystemMutation mutation, SourcePosition source);
    }

    /** Verifies the signed observation against source-protected clock policy; it must not resample a clock. */
    @FunctionalInterface
    public interface DecisionTimeAuthority {
        boolean verifies(
                TargetQuotaScope shardScope,
                AuthorIdentity writer,
                SourcePosition source,
                TrustedUtcIntervalEvidence evidence);
    }

    public record Authorization(
            PublicKey writerKey,
            ProtocolTuple activatedTuple,
            OwnerIdentity activeOwner,
            long maximumDecisionWidthMs,
            long maximumBrokerTimestampDivergenceMs,
            long maximumMutationEnqueueAgeMs,
            DecisionTimeAuthority decisionTime) {
        public Authorization {
            Objects.requireNonNull(writerKey, "writerKey");
            Objects.requireNonNull(activatedTuple, "activatedTuple");
            Objects.requireNonNull(activeOwner, "activeOwner");
            Objects.requireNonNull(decisionTime, "decisionTime");
            if (maximumDecisionWidthMs < 0
                    || maximumBrokerTimestampDivergenceMs < 0
                    || maximumMutationEnqueueAgeMs < 0) {
                throw new IllegalArgumentException("invalid Target Outcome timing policy");
            }
        }
    }

    public record Decision(PublishOutcomeBody body, StableCode rejection, Authorization authorization) {
        public Decision {
            if ((body == null) == (rejection == null)) {
                throw new IllegalArgumentException("Target Outcome decision must accept or reject exactly once");
            }
            if ((body == null) == (authorization != null)) {
                throw new IllegalArgumentException("Target Outcome authorization must accompany only acceptance");
            }
        }
    }

    /** Invoke only after immutable first-result lookup; unavailable authority must retain the source entry. */
    public static Decision decideFirstApplication(
            final TargetQuotaScope shardScope,
            final SystemMutation mutation,
            final SourcePosition source,
            final Authority authority) {
        Objects.requireNonNull(shardScope, "shardScope");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (shardScope.target() != null
                || !shardScope.shard().equals(source.shardId())
                || !shardScope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.PUBLISH_OUTCOME) {
            throw new IllegalStateException("Target Outcome requires the exact Shard root scope/source");
        }
        final PublishOutcomeBody body;
        final AuthorIdentity author;
        try {
            body = PublishOutcomeBody.decode(mutation.canonicalBody());
            author = AuthorIdentity.decode(mutation.authorIdentity());
        } catch (IllegalArgumentException malformed) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (author.kind() != AuthorIdentity.Kind.OWNER
                || !Bytes.constantTimeEquals(
                        mutation.logicalOperationIdentity(), body.initialLogicalOperationIdentity())) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return rejected(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        final Authorization authorized = authority.resolve(shardScope, author, mutation, source);
        if (authorized == null
                || !authorized.activeOwner().equals(author.asOwnerIdentity())
                || !ProtocolTuple.currentSystemMutation().equals(authorized.activatedTuple())
                || !mutation.verifySignature(authorized.writerKey())) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final var observedAt = body.observedAt();
        final long enqueueBound;
        try {
            observedAt.requireWidthAtMost(authorized.maximumDecisionWidthMs());
            enqueueBound = Math.addExact(
                    authorized.maximumMutationEnqueueAgeMs(), authorized.maximumBrokerTimestampDivergenceMs());
        } catch (IllegalArgumentException | ArithmeticException invalidPolicyOrTime) {
            return rejected(StableCode.STALE_SYSTEM_MUTATION);
        }
        final long distance = source.brokerPersistenceTimeEpochMs() < observedAt.earliestEpochMs()
                ? observedAt.earliestEpochMs() - source.brokerPersistenceTimeEpochMs()
                : source.brokerPersistenceTimeEpochMs() > observedAt.latestEpochMs()
                        ? source.brokerPersistenceTimeEpochMs() - observedAt.latestEpochMs()
                        : 0;
        if (distance > enqueueBound || !authorized.decisionTime().verifies(shardScope, author, source, observedAt)) {
            return rejected(StableCode.STALE_SYSTEM_MUTATION);
        }
        return new Decision(body, null, authorized);
    }

    private static Decision rejected(final StableCode rejection) {
        return new Decision(null, Objects.requireNonNull(rejection, "rejection"), null);
    }
}
