package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** First-application authentication for the shared v1 Publish Outcome on the Target source path. */
public final class TargetPublishOutcomeVerifier {
    private TargetPublishOutcomeVerifier() {}

    /**
     * Historical accepted-writer and tuple snapshot; activeOwner authorizes only cross-Owner recovery UNKNOWN.
     * External history I/O belongs to the control resolver before Store preparation, not this snapshot lookup.
     */
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
            DecisionTimeAuthority decisionTime,
            RetryContext retryContext) {
        public Authorization(
                PublicKey writerKey,
                ProtocolTuple activatedTuple,
                OwnerIdentity activeOwner,
                long maximumDecisionWidthMs,
                long maximumBrokerTimestampDivergenceMs,
                long maximumMutationEnqueueAgeMs,
                DecisionTimeAuthority decisionTime) {
            this(
                    writerKey,
                    activatedTuple,
                    activeOwner,
                    maximumDecisionWidthMs,
                    maximumBrokerTimestampDivergenceMs,
                    maximumMutationEnqueueAgeMs,
                    decisionTime,
                    null);
        }

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

    /** Source-history images; the Store must bind both to retained APPLIED Admission first results. */
    public record RetryContext(SystemMutation admission, SystemMutation firstAdmission, RetryPolicySemantic policy) {
        public RetryContext {
            requireAdmission(admission);
            requireAdmission(firstAdmission);
            Objects.requireNonNull(policy, "policy");
        }

        private static void requireAdmission(final SystemMutation mutation) {
            Objects.requireNonNull(mutation, "admission");
            if (mutation.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || mutation.canonicalEnvelope().length
                            > 2 * TargetPublishAdmissionBody.MAX_CANONICAL_BYTES + 1024) {
                throw new IllegalArgumentException("Target retry context requires a bounded Target Admission image");
            }
            final var body = TargetPublishAdmissionBody.decode(mutation.canonicalBody());
            if (!body.owner().equals(AuthorIdentity.decode(mutation.authorIdentity()).asOwnerIdentity())
                    || !body.shard().equals(mutation.shardId())
                    || body.retryUntilEpochMs() != mutation.retryUntilEpochMs()
                    || !Arrays.equals(body.publishAttemptId(), mutation.logicalOperationIdentity())) {
                throw new IllegalArgumentException("Target retry Admission image changes its exact owner/identity");
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
