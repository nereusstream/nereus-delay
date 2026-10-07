package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.PublicKey;
import java.util.Objects;

/** First-application authentication for the independent Target Admission protocol tuple. */
public final class TargetPublishAdmissionVerifier {
    private TargetPublishAdmissionVerifier() {}

    /** Resolves historical Owner/key and exact source-activated Target tuple; the snapshot must survive commit. */
    @FunctionalInterface
    public interface Authority {
        Authorization resolve(
                TargetQuotaScope shardScope, AuthorIdentity writer, SystemMutation mutation, SourcePosition source);
    }

    /** Authenticates the recorded time evidence against source-ordered Route policy; it must not resample a clock. */
    @FunctionalInterface
    public interface DecisionTimeAuthority {
        boolean verifies(
                TargetQuotaScope shardScope,
                AuthorIdentity writer,
                SourcePosition source,
                TrustedUtcIntervalEvidence evidence);
    }

    /** Resolved source-protected capability/credential/materialization snapshot; no I/O inside Store preparation. */
    @FunctionalInterface
    public interface MaterializationAuthority {
        void requireAuthorized(TargetPublishAdmissionBody body, SourcePosition source);
    }

    public record Authorization(
            PublicKey writerKey,
            ProtocolTuple activatedTuple,
            long maximumDecisionWidthMs,
            long maximumBrokerTimestampDivergenceMs,
            long maximumMutationEnqueueAgeMs,
            DecisionTimeAuthority decisionTime,
            MaterializationAuthority materialization) {
        public Authorization(
                PublicKey writerKey,
                ProtocolTuple activatedTuple,
                long maximumDecisionWidthMs,
                long maximumBrokerTimestampDivergenceMs,
                long maximumMutationEnqueueAgeMs,
                DecisionTimeAuthority decisionTime) {
            this(writerKey, activatedTuple, maximumDecisionWidthMs, maximumBrokerTimestampDivergenceMs,
                    maximumMutationEnqueueAgeMs, decisionTime, null);
        }

        public Authorization {
            Objects.requireNonNull(writerKey, "writerKey");
            Objects.requireNonNull(activatedTuple, "activatedTuple");
            Objects.requireNonNull(decisionTime, "decisionTime");
            if (maximumDecisionWidthMs < 0
                    || maximumBrokerTimestampDivergenceMs < 0
                    || maximumMutationEnqueueAgeMs < 0) {
                throw new IllegalArgumentException("invalid Target Admission timing policy");
            }
        }
    }

    public record Decision(
            TargetPublishAdmissionBody body, StableCode rejection, Authorization authorization) {
        public Decision {
            if ((body == null) == (rejection == null)) {
                throw new IllegalArgumentException("Target Admission decision must accept or reject exactly once");
            }
            if ((body == null) == (authorization != null)) {
                throw new IllegalArgumentException("Target Admission authorization must accompany only acceptance");
            }
        }
    }

    /**
     * Invoke only after immutable first-result lookup; unavailable authority must throw and retain the source entry.
     */
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
                || mutation.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION) {
            throw new IllegalStateException("Target Admission requires the exact Shard root scope/source");
        }
        final TargetPublishAdmissionBody body;
        final AuthorIdentity author;
        try {
            body = TargetPublishAdmissionBody.decode(mutation.canonicalBody());
            author = AuthorIdentity.decode(mutation.authorIdentity());
        } catch (IllegalArgumentException malformed) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (!body.shard().equals(source.shardId())
                || body.retryUntilEpochMs() != mutation.retryUntilEpochMs()
                || author.kind() != AuthorIdentity.Kind.OWNER
                || !body.owner().equals(author.asOwnerIdentity())
                || !Bytes.constantTimeEquals(body.publishAttemptId(), mutation.logicalOperationIdentity())) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return rejected(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        final Authorization authorized = authority.resolve(shardScope, author, mutation, source);
        if (authorized == null
                || !(body.publication() == null
                                ? ProtocolTuple.targetPublishAdmission()
                                : ProtocolTuple.targetMaterializedPublishAdmission())
                        .equals(authorized.activatedTuple())
                || !mutation.verifySignature(authorized.writerKey())) {
            return rejected(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final var time = body.decisionTime();
        final long enqueueBound;
        try {
            time.requireWidthAtMost(authorized.maximumDecisionWidthMs());
            enqueueBound = Math.addExact(
                    authorized.maximumMutationEnqueueAgeMs(), authorized.maximumBrokerTimestampDivergenceMs());
        } catch (IllegalArgumentException | ArithmeticException invalidPolicyOrTime) {
            return rejected(StableCode.STALE_SYSTEM_MUTATION);
        }
        final long distance = source.brokerPersistenceTimeEpochMs() < time.earliestEpochMs()
                ? time.earliestEpochMs() - source.brokerPersistenceTimeEpochMs()
                : source.brokerPersistenceTimeEpochMs() > time.latestEpochMs()
                        ? source.brokerPersistenceTimeEpochMs() - time.latestEpochMs()
                        : 0;
        if (distance > enqueueBound || !authorized.decisionTime().verifies(shardScope, author, source, time)) {
            return rejected(StableCode.STALE_SYSTEM_MUTATION);
        }
        if (body.publication() != null) {
            if (authorized.materialization() == null) {
                throw new IllegalStateException("Target materialization authority is unavailable");
            }
            authorized.materialization().requireAuthorized(body, source);
        }
        return new Decision(body, null, authorized);
    }

    private static Decision rejected(final StableCode rejection) {
        return new Decision(null, Objects.requireNonNull(rejection, "rejection"), null);
    }
}
