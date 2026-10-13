package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/** Signs exact Target initial outcomes; context/physical authentication and source ordering remain external. */
public final class TargetPublishOutcomeMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;

    public TargetPublishOutcomeMutationFactory(int keyVersion, PrivateKey key) {
        if (keyVersion == 0) {
            throw new IllegalArgumentException("Target Outcome signing version is unassigned");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key, "key");
    }

    public SystemMutation create(
            final TargetPublishAdmissionStore.Applied applied, final DestinationPublishResult result,
            final WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var body = Objects.requireNonNull(applied, "applied").body();
        final var physical = Objects.requireNonNull(result, "result");
        final var c = Objects.requireNonNull(context, "context");
        final int effect = switch (physical.disposition()) {
            case PUBLISHED -> 1;
            case DEFINITIVELY_NOT_PUBLISHED -> 2;
            case UNKNOWN -> 3;
        };
        if (effect != 3) {
            final var evidence = PublishEvidence.decode(physical.evidence());
            evidence.requireBusinessMutation(body.publishAttemptId(), effect == 1);
            if (applied.kafkaMapping() != null) {
                if (effect == 1) {
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.requirePublished(applied.kafkaMapping(), evidence);
                } else {
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.requireNotPublished(applied.kafkaMapping(), evidence);
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
                        ? body.publication().physical() : body.nativePublication().physical();
                if (physical.brokerResource() == null
                        || !physical.brokerResource().equals(target.resource())
                        || Integer.toUnsignedLong(physical.brokerPartition())
                                != target.physicalPartition()
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
                body.shard(), c.retryUntilEpochMs(), body.publishAttemptId(), effect, c.disposition(),
                physical.stableCode(), effect == 3 ? null : physical.evidence(), c.transfer(),
                c.observedAt(), c.retryDecision());
        return SystemMutation.signed(body.shard(), SystemMutationType.PUBLISH_OUTCOME, c.retryUntilEpochMs(),
                body.publishAttemptId(), encoded,
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(),
                        owner.leaseFencingDigest()).canonicalBytes(), keyVersion, key);
    }

    /** Signs only the initial conservative hold, never a definitive result or new send/retry permission. */
    public SystemMutation createRecoveryUnknown(
            final TargetPublishAdmissionStore.Recovery recovery, final OwnerIdentity recoveryOwner,
            final WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var body = Objects.requireNonNull(recovery, "recovery").body();
        final var owner = Objects.requireNonNull(recoveryOwner, "recoveryOwner");
        final var c = Objects.requireNonNull(context, "context");
        final byte[] zeroTransfer = new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
        if (c.disposition() != 4 || !Arrays.equals(c.transfer(), zeroTransfer)) {
            throw new IllegalArgumentException("Target recovery requires OWNER_FENCED and no early release claim");
        }
        final byte[] encoded = PublishOutcomeBody.encodeInitial(
                body.shard(), c.retryUntilEpochMs(), body.publishAttemptId(), 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null, c.transfer(), c.observedAt(), c.retryDecision());
        final var outcome = PublishOutcomeBody.decode(encoded);
        if (!outcome.retryDecision().hasFullShape() || outcome.retryDecision().kind() != 5
                || outcome.retryDecision().hasNextRetryAt()
                || outcome.retryDecision().cause() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN) {
            throw new IllegalArgumentException("Target recovery requires typed UNCERTAIN_HOLD and its recovery cause");
        }
        return SystemMutation.signed(body.shard(), SystemMutationType.PUBLISH_OUTCOME, c.retryUntilEpochMs(),
                body.publishAttemptId(), encoded,
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(),
                        owner.leaseFencingDigest()).canonicalBytes(), keyVersion, key);
    }

    /** SERVICE resolution of actual Source UNKNOWN; independent history/evidence/retention rights are mandatory. */
    public SystemMutation createKafkaResolution(
            com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown unknown,
            PublishEvidence evidence, AuthorIdentity service, StableCode code,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var actual = Objects.requireNonNull(unknown, "actual UNKNOWN proof");
        if (Objects.requireNonNull(service, "actual SERVICE author").kind() != AuthorIdentity.Kind.SERVICE
                || !Arrays.equals(context.transfer(), actual.mapping().admission().outcomeTransfer())) {
            throw new IllegalArgumentException("K2 resolution changes SERVICE identity or frozen charge transfer");
        }
        final var cursor = com.nereusstream.delay.protocol.TargetKafkaReceiptWire.cursor(actual.mapping(), evidence);
        final int effect = evidence.verificationStatus()
                == com.nereusstream.delay.protocol.EvidenceVerificationStatus.VERIFIED_PUBLISHED ? 1 : 2;
        final byte[] body = PublishOutcomeBody.encodeEvidenceResolution(actual.admission().shardId(),
                context.retryUntilEpochMs(), actual.mapping().admission().publishAttemptId(), cursor,
                evidence.canonicalBytes(), code, effect, context.disposition(), context.transfer(),
                context.observedAt(), context.retryDecision());
        return SystemMutation.signed(actual.admission().shardId(), SystemMutationType.EVIDENCE_RESOLUTION,
                context.retryUntilEpochMs(), actual.mapping().admission().publishAttemptId(), body,
                service.canonicalBytes(), keyVersion, key);
    }
}
