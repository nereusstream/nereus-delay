package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/** Signs exact Target initial outcomes; context/physical authentication and source ordering remain external. */
public final class TargetPublishOutcomeMutationFactory {
    private final int keyVersion;
    private volatile PrivateKey key;
    private final java.util.function.Consumer<TrustedUtcIntervalEvidence> signingGuard;

    public TargetPublishOutcomeMutationFactory(int keyVersion, PrivateKey key) {
        this(keyVersion, key, at -> {});
    }

    /** The owning Root checks its lifetime and the configured key window on both sides of signing. */
    public TargetPublishOutcomeMutationFactory(
            int keyVersion, PrivateKey key, java.util.function.Consumer<TrustedUtcIntervalEvidence> signingGuard) {
        if (keyVersion == 0) {
            throw new IllegalArgumentException("Target Outcome signing version is unassigned");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key, "key");
        this.signingGuard = Objects.requireNonNull(signingGuard, "original signing owner");
    }

    /** After the owner's I/O users exit, drop this factory's private key reference and prohibit further signing. */
    public void invalidate() {
        key = null;
    }

    private SystemMutation sign(
            ShardId shard,
            SystemMutationType type,
            long retryUntil,
            byte[] logical,
            byte[] body,
            byte[] author,
            TrustedUtcIntervalEvidence at) {
        signingGuard.accept(at);
        final var original = key;
        if (original == null) {
            throw new IllegalStateException("Target Outcome signing owner closed");
        }
        final var signed = SystemMutation.signed(shard, type, retryUntil, logical, body, author, keyVersion, original);
        signingGuard.accept(at);
        return signed;
    }

    public SystemMutation create(
            final TargetPublishAdmissionStore.Applied applied,
            final DestinationPublishResult result,
            final WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var body = Objects.requireNonNull(applied, "applied").body();
        final var physical = Objects.requireNonNull(result, "result");
        final var c = Objects.requireNonNull(context, "context");
        final int effect =
                switch (physical.disposition()) {
                    case PUBLISHED -> 1;
                    case DEFINITIVELY_NOT_PUBLISHED -> 2;
                    case UNKNOWN -> 3;
                };
        if (effect != 3) {
            final var evidence = PublishEvidence.decode(physical.evidence());
            evidence.requireBusinessMutation(body.publishAttemptId(), effect == 1);
            if (applied.kafkaMapping() != null) {
                if (effect == 1) {
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.requirePublished(
                            applied.kafkaMapping(), evidence);
                } else {
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.requireNotPublished(
                            applied.kafkaMapping(), evidence);
                }
            }
            if (!Arrays.equals(c.transfer(), body.outcomeTransfer())) {
                throw new IllegalArgumentException("Target Outcome changes frozen Admission charge transfer");
            }
            if (effect == 1) {
                if (body.nativePublication() == null) {
                    evidence.requireOrdinaryTargetPublishedBinding(body.publication());
                } else {
                    evidence.requireNativeTargetPublishedBinding(body.nativePublication());
                }
                final var target = body.nativePublication() == null
                        ? body.publication().physical()
                        : body.nativePublication().physical();
                if (physical.brokerResource() == null
                        || !physical.brokerResource().equals(target.resource())
                        || Integer.toUnsignedLong(physical.brokerPartition()) != target.physicalPartition()
                        || !Arrays.equals(physical.externalDeliveryIdentity(), body.publishAttemptId())) {
                    throw new IllegalArgumentException("Target Outcome result changes physical request identity");
                }
            } else {
                if (body.nativePublication() == null) {
                    evidence.requireOrdinaryTargetNotPublishedBinding(body.publication());
                } else {
                    evidence.requireNativeTargetNotPublishedBinding(body.nativePublication());
                }
            }
        } else if (physical.evidence() != null && physical.evidence().length != 0) {
            throw new IllegalArgumentException("Target UNKNOWN cannot carry definitive/opaque evidence");
        }
        final var owner = body.owner();
        final byte[] encoded = PublishOutcomeBody.encodeInitial(
                body.shard(),
                c.retryUntilEpochMs(),
                body.publishAttemptId(),
                effect,
                c.disposition(),
                physical.stableCode(),
                effect == 3 ? null : physical.evidence(),
                c.transfer(),
                c.observedAt(),
                c.retryDecision());
        return sign(
                body.shard(),
                SystemMutationType.PUBLISH_OUTCOME,
                c.retryUntilEpochMs(),
                body.publishAttemptId(),
                encoded,
                AuthorIdentity.owner(
                                owner.deploymentId(),
                                owner.workerRunId(),
                                owner.ownerEpoch(),
                                owner.leaseFencingDigest())
                        .canonicalBytes(),
                c.observedAt());
    }

    /** Signs only the initial conservative hold, never a definitive result or new send/retry permission. */
    public SystemMutation createRecoveryUnknown(
            final TargetPublishAdmissionStore.Recovery recovery,
            final OwnerIdentity recoveryOwner,
            final WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var body = Objects.requireNonNull(recovery, "recovery").body();
        final var owner = Objects.requireNonNull(recoveryOwner, "recoveryOwner");
        final var c = Objects.requireNonNull(context, "context");
        final byte[] zeroTransfer = new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
        if (c.disposition() != 4 || !Arrays.equals(c.transfer(), zeroTransfer)) {
            throw new IllegalArgumentException("Target recovery requires OWNER_FENCED and no early release claim");
        }
        final byte[] encoded = PublishOutcomeBody.encodeInitial(
                body.shard(),
                c.retryUntilEpochMs(),
                body.publishAttemptId(),
                3,
                4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                null,
                c.transfer(),
                c.observedAt(),
                c.retryDecision());
        final var outcome = PublishOutcomeBody.decode(encoded);
        if (!outcome.retryDecision().hasFullShape()
                || outcome.retryDecision().kind() != 5
                || outcome.retryDecision().hasNextRetryAt()
                || outcome.retryDecision().cause() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN) {
            throw new IllegalArgumentException("Target recovery requires typed UNCERTAIN_HOLD and its recovery cause");
        }
        return sign(
                body.shard(),
                SystemMutationType.PUBLISH_OUTCOME,
                c.retryUntilEpochMs(),
                body.publishAttemptId(),
                encoded,
                AuthorIdentity.owner(
                                owner.deploymentId(),
                                owner.workerRunId(),
                                owner.ownerEpoch(),
                                owner.leaseFencingDigest())
                        .canonicalBytes(),
                c.observedAt());
    }

    /** SERVICE resolution of actual Source UNKNOWN; independent history/evidence/retention rights are mandatory. */
    public SystemMutation createKafkaResolution(
            com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown unknown,
            PublishEvidence evidence,
            AuthorIdentity service,
            StableCode code,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var actual = Objects.requireNonNull(unknown, "actual UNKNOWN proof");
        if (Objects.requireNonNull(service, "actual SERVICE author").kind() != AuthorIdentity.Kind.SERVICE
                || !Arrays.equals(
                        context.transfer(), actual.mapping().admission().outcomeTransfer())) {
            throw new IllegalArgumentException("K2 resolution changes SERVICE identity or frozen charge transfer");
        }
        final var cursor = com.nereusstream.delay.protocol.TargetKafkaReceiptWire.cursor(actual.mapping(), evidence);
        final int effect = evidence.verificationStatus()
                        == com.nereusstream.delay.protocol.EvidenceVerificationStatus.VERIFIED_PUBLISHED
                ? 1
                : 2;
        final byte[] body = PublishOutcomeBody.encodeEvidenceResolution(
                actual.admission().shardId(),
                context.retryUntilEpochMs(),
                actual.mapping().admission().publishAttemptId(),
                cursor,
                evidence.canonicalBytes(),
                code,
                effect,
                context.disposition(),
                context.transfer(),
                context.observedAt(),
                context.retryDecision());
        return sign(
                actual.admission().shardId(),
                SystemMutationType.EVIDENCE_RESOLUTION,
                context.retryUntilEpochMs(),
                PublishOutcomeBody.decodeEvidenceResolution(body).evidenceResolutionLogicalOperationIdentity(),
                body,
                service.canonicalBytes(),
                context.observedAt());
    }

    /** Root-owned v7 signing accepts an actual authenticated publication, never a caller's recovery evidence DTO. */
    public SystemMutation createKafkaResolutionWithRecoveryProof(
            com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown unknown,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.RecoveryPublication publication,
            AuthorIdentity service,
            StableCode code,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var actual = Objects.requireNonNull(unknown, "actual Source UNKNOWN");
        final var verified = Objects.requireNonNull(publication).verified();
        final var proof = verified.proof();
        final var evidence = proof.evidence();
        if (Objects.requireNonNull(service).kind() != AuthorIdentity.Kind.SERVICE
                || !Arrays.equals(
                        actual.mapping().canonicalBytes(), proof.mapping().canonicalBytes())
                || !Arrays.equals(
                        actual.admission().canonicalEnvelope(),
                        proof.admission().canonicalEnvelope())
                || !Arrays.equals(
                        context.transfer(), actual.mapping().admission().outcomeTransfer())) {
            throw new IllegalArgumentException(
                    "archived resolution changed original UNKNOWN/Admission/transfer/SERVICE scope");
        }
        final var cursor = com.nereusstream.delay.protocol.TargetKafkaReceiptWire.cursor(actual.mapping(), evidence);
        final int effect = evidence.verificationStatus()
                        == com.nereusstream.delay.protocol.EvidenceVerificationStatus.VERIFIED_PUBLISHED
                ? 1
                : 2;
        final byte[] body = PublishOutcomeBody.encodeEvidenceResolutionWithRecoveryProof(
                actual.admission().shardId(),
                context.retryUntilEpochMs(),
                actual.mapping().admission().publishAttemptId(),
                cursor,
                evidence.canonicalBytes(),
                code,
                effect,
                context.disposition(),
                context.transfer(),
                context.observedAt(),
                context.retryDecision(),
                verified.reference());
        return sign(
                actual.admission().shardId(),
                SystemMutationType.EVIDENCE_RESOLUTION,
                context.retryUntilEpochMs(),
                PublishOutcomeBody.decodeEvidenceResolution(body).evidenceResolutionLogicalOperationIdentity(),
                body,
                service.canonicalBytes(),
                context.observedAt());
    }
}
