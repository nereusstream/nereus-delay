package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RetryJitter;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.protocol.UncertainPolicy;
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

    /**
     * Pre-resolved authenticated provider/Journal/receipt snapshot for the exact frozen request and evidence.
     * It must check response correlation, sender/sequence and artifact authority, not just VERIFIED status.
     * History/provider I/O must finish outside the Store ReadView; absence/failure retains the source entry.
     */
    @FunctionalInterface
    public interface EvidenceAuthority {
        void requireAuthenticated(
                TargetOrdinaryPublicationBinding publication,
                PublishEvidence evidence,
                SourcePosition admissionSource,
                SourcePosition outcomeSource);
    }

    /** Authenticates the exact service observation/cursor and its protected recovery domain before Store apply. */
    @FunctionalInterface
    public interface ResolutionAuthority {
        void requireAuthenticated(
                TargetOrdinaryPublicationBinding publication, PublishEvidence evidence, EvidenceCursor cursor,
                SourcePosition admissionSource, SourcePosition unknownSource, SourcePosition resolutionSource);
    }

    @FunctionalInterface
    public interface NativeEvidenceAuthority {
        void requireAuthenticated(
                com.nereusstream.delay.protocol.TargetNativePublicationBinding publication,
                PublishEvidence evidence, SourcePosition admissionSource, SourcePosition outcomeSource);
    }

    @FunctionalInterface
    public interface NativeResolutionAuthority {
        void requireAuthenticated(
                com.nereusstream.delay.protocol.TargetNativePublicationBinding publication,
                PublishEvidence evidence, EvidenceCursor cursor, SourcePosition admissionSource,
                SourcePosition unknownSource, SourcePosition resolutionSource);
    }

    public record EvidenceContext(
            SystemMutation admission, EvidenceAuthority authority, ResolutionAuthority resolutionAuthority,
            NativeEvidenceAuthority nativeAuthority, NativeResolutionAuthority nativeResolutionAuthority) {
        public EvidenceContext(SystemMutation admission, EvidenceAuthority authority) {
            this(admission, authority, null, null, null);
        }
        public EvidenceContext(SystemMutation admission, EvidenceAuthority authority,
                ResolutionAuthority resolutionAuthority) {
            this(admission, authority, resolutionAuthority, null, null);
        }
        public static EvidenceContext nativeContext(SystemMutation admission, NativeEvidenceAuthority authority,
                NativeResolutionAuthority resolution) {
            return new EvidenceContext(admission, null, null, authority, resolution);
        }
        public EvidenceContext {
            RetryContext.requireAdmission(admission);
            final var body = TargetPublishAdmissionBody.decode(admission.canonicalBody());
            if (!body.materialized()) {
                throw new IllegalArgumentException("Target physical evidence requires materialized Admission v4/v5");
            }
            if (body.nativePublication() != null) {
                Objects.requireNonNull(nativeAuthority, "nativeEvidenceAuthority");
                if (authority != null || resolutionAuthority != null) {
                    throw new IllegalArgumentException("Native evidence cannot use ordinary authority");
                }
            } else {
                Objects.requireNonNull(authority, "evidenceAuthority");
                if (nativeAuthority != null || nativeResolutionAuthority != null) {
                    throw new IllegalArgumentException("ordinary evidence cannot use Native authority");
                }
            }
        }
    }

    public record Authorization(
            PublicKey writerKey,
            ProtocolTuple activatedTuple,
            OwnerIdentity activeOwner,
            long maximumDecisionWidthMs,
            long maximumBrokerTimestampDivergenceMs,
            long maximumMutationEnqueueAgeMs,
            DecisionTimeAuthority decisionTime,
            RetryContext retryContext,
            EvidenceContext evidenceContext) {
        public Authorization(
                PublicKey writerKey,
                ProtocolTuple activatedTuple,
                OwnerIdentity activeOwner,
                long maximumDecisionWidthMs,
                long maximumBrokerTimestampDivergenceMs,
                long maximumMutationEnqueueAgeMs,
                DecisionTimeAuthority decisionTime,
                RetryContext retryContext) {
            this(writerKey, activatedTuple, activeOwner, maximumDecisionWidthMs,
                    maximumBrokerTimestampDivergenceMs, maximumMutationEnqueueAgeMs,
                    decisionTime, retryContext, null);
        }

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
                    null,
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
                            > 2 * TargetPublishAdmissionBody.MAX_SUPPORTED_BYTES + 1024) {
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
                || mutation.type() != SystemMutationType.PUBLISH_OUTCOME
                        && mutation.type() != SystemMutationType.EVIDENCE_RESOLUTION) {
            throw new IllegalStateException("Target Outcome requires the exact Shard root scope/source");
        }
        final PublishOutcomeBody body;
        final AuthorIdentity author;
        try {
            body = mutation.type() == SystemMutationType.EVIDENCE_RESOLUTION
                    ? PublishOutcomeBody.decodeEvidenceResolution(mutation.canonicalBody())
                    : PublishOutcomeBody.decode(mutation.canonicalBody());
            author = AuthorIdentity.decode(mutation.authorIdentity());
        } catch (IllegalArgumentException malformed) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final boolean resolution = mutation.type() == SystemMutationType.EVIDENCE_RESOLUTION;
        if (author.kind() != (resolution ? AuthorIdentity.Kind.SERVICE : AuthorIdentity.Kind.OWNER)
                || !Bytes.constantTimeEquals(
                        mutation.logicalOperationIdentity(), resolution
                                ? body.evidenceResolutionLogicalOperationIdentity()
                                : body.initialLogicalOperationIdentity())) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return rejected(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        final Authorization authorized = authority.resolve(shardScope, author, mutation, source);
        if (authorized == null
                || !body.requiredTuple().equals(authorized.activatedTuple())
                || !mutation.verifySignature(authorized.writerKey())) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        // Parsing and tuple support alone cannot adopt a proof before its full immutable snapshot/atomic ledger path exists.
        if (body.recoveryProof() != null) {
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

    /** Revalidates the immutable historical retry window/jitter; current runtime quota counts remain separate. */
    public static void requireHistoricalRetryWindow(TargetPublishAdmissionBody current,
            TargetPublishAdmissionBody first,
            RetryPolicySemantic policy, PublishOutcomeBody outcome) {
        if (!current.materialized()) {
            throw new IllegalStateException("historical Outcome requires protected materialized Admission window");
        }
        requireRetryWindow(current.locator(), current.nativePublication() != null
                        ? current.nativePublication().deliverAtEpochMs() : current.publication().deliverAtEpochMs(),
                current.nativePublication() != null
                        ? current.nativePublication().expireAtEpochMs() : current.publication().expireAtEpochMs(),
                current, first, policy, outcome);
    }

    static void requireRetryWindow(TargetMessageLocator locator, long deliverAt, long expireAt,
            TargetPublishAdmissionBody current, TargetPublishAdmissionBody first,
            RetryPolicySemantic policy, PublishOutcomeBody outcome) {
        final var retry = outcome.retryDecision();
        final long firstAt = first.decisionTime().latestEpochMs();
        final long deadline;
        try {
            deadline = Math.min(expireAt, Math.addExact(firstAt, policy.maxRetryDurationMs()));
        } catch (ArithmeticException invalidWindow) {
            throw new IllegalArgumentException("Target retry deadline overflows its admitted window", invalidWindow);
        }
        if (!retry.policy().matches(policy)
                || retry.retryDomain() != RetryJitter.MESSAGE_PUBLISH
                || retry.completedAttemptNo() != Integer.toUnsignedLong(current.attemptNo())
                || retry.firstAttemptAt() != firstAt
                || firstAt >= expireAt
                || retry.retryDeadline() != deadline) {
            throw new IllegalArgumentException("Target RetryDecision differs from its admitted attempt/window/policy");
        }
        if (retry.hasNextRetryAt()) {
            final long next;
            try {
                next = Math.addExact(
                        outcome.observedAt().latestEpochMs(),
                        RetryJitter.delayMs(
                                RetryJitter.MESSAGE_PUBLISH,
                                locator.messageId(),
                                Integer.toUnsignedLong(locator.generation()),
                                retry.completedAttemptNo(),
                                policy.retryBackoffCap(retry.completedAttemptNo())));
            } catch (ArithmeticException invalidRetryTime) {
                throw new IllegalArgumentException("Target retry jitter time overflows", invalidRetryTime);
            }
            final boolean uncertain = outcome.sideEffect() == 3;
            if ((uncertain ? retry.kind() != 2 : retry.kind() != 2 && retry.kind() != 4)
                    || uncertain && locator.orderingMode() != OrderingMode.BEST_EFFORT
                    || uncertain && policy.uncertainPolicy() != UncertainPolicy.BOUNDED_RETRY_POSSIBLE_DUPLICATE
                    || retry.nextRetryAt() != next
                    || next > deadline
                    || Math.max(deliverAt, next) >= expireAt
) {
                throw new IllegalArgumentException("Target retry differs from its policy/jitter/budget");
            }
        }
    }

    private static Decision rejected(final StableCode rejection) {
        return new Decision(null, Objects.requireNonNull(rejection, "rejection"), null);
    }
}
