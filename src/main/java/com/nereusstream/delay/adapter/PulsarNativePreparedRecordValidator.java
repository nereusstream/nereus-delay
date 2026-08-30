package com.nereusstream.delay.adapter;

import com.nereusstream.delay.assessment.PhysicalSendActivationGate;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NativePreparedRecordBinding;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.StableCode;
import java.security.PublicKey;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;

/**
 * Shared fail-closed validator for the H5 native prepared-record path.
 * Every check completes before a guarded transport may transfer Producer
 * ownership.
 */
public final class PulsarNativePreparedRecordValidator {
    private final PulsarTargetResource resource;
    private final PublicKey capabilityIssuerKey;
    private final Clock clock;
    private final PinnedPulsarNativeSubmissionAdapter.CredentialFingerprintProvider credentialFingerprintProvider;
    private final PhysicalSendActivationGate activationGate;
    private final FrozenHandoffPolicyGate handoffPolicyGate;

    public PulsarNativePreparedRecordValidator(
            final PulsarTargetResource resource,
            final PublicKey issuerKey,
            final Clock clock,
            final PinnedPulsarNativeSubmissionAdapter.CredentialFingerprintProvider credentialFingerprintProvider,
            final PhysicalSendActivationGate activationGate) {
        this(
                resource,
                issuerKey,
                clock,
                credentialFingerprintProvider,
                activationGate,
                (snapshot, artifacts, trustedNowEpochMs) -> {
                    if (!snapshot.verifySignature(issuerKey)) {
                        throw new IllegalArgumentException("handoff policy signature is invalid");
                    }
                });
    }

    /** Production constructor with an independent frozen Handoff trust authority. */
    public PulsarNativePreparedRecordValidator(
            final PulsarTargetResource resource,
            final PublicKey capabilityIssuerKey,
            final Clock clock,
            final PinnedPulsarNativeSubmissionAdapter.CredentialFingerprintProvider credentialFingerprintProvider,
            final PhysicalSendActivationGate activationGate,
            final FrozenHandoffPolicyGate handoffPolicyGate) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.capabilityIssuerKey = Objects.requireNonNull(capabilityIssuerKey, "capabilityIssuerKey");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.credentialFingerprintProvider = credentialFingerprintProvider;
        this.activationGate = Objects.requireNonNull(activationGate, "activationGate");
        this.handoffPolicyGate = Objects.requireNonNull(handoffPolicyGate, "handoffPolicyGate");
    }

    public ArtifactGenerationSet artifacts() {
        return activationGate.artifacts();
    }

    /** Materializes and validates the exact target record without touching a Producer. */
    public PulsarPreparedRecord materialize(final PreparedSubmission submission) {
        final PreparedSubmission exact = Objects.requireNonNull(submission, "submission");
        if (!exact.isNativeRecordReady()) {
            throw new Rejection(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE);
        }
        final PulsarPreparedRecord record;
        try {
            record = PulsarNativePreparedRecordFactory.create(
                    exact.nativePrepared(), exact.nativeRecordContext(), activationGate.artifacts());
        } catch (RuntimeException mismatch) {
            throw new Rejection(StableCode.PREPARED_SUBMISSION_MISMATCH, mismatch);
        }
        final StableCode rejection = validate(exact.nativePrepared(), record, activationGate.artifacts());
        if (rejection != null) {
            throw new Rejection(rejection);
        }
        return record;
    }

    /** Returns {@code null} only when every pre-ownership check succeeds. */
    public StableCode validate(
            final NativePreparedDelivery prepared,
            final PulsarPreparedRecord record,
            final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        final long nowEpochMs;
        try {
            nowEpochMs = clock.millis();
            activationGate.requirePhysicalSend(artifacts, nowEpochMs);
        } catch (RuntimeException unavailable) {
            return StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE;
        }
        if (!prepared.isCurrentGeneration()
                || prepared.nativeDeliveryPolicy() != NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF
                || prepared.deliveryContract() != DeliveryContract.PULSAR_NATIVE_DELIVERY
                || prepared.handoffPolicySnapshot() == null
                || artifacts.pulsarRecordGeneration() != PulsarPreparedRecord.SCHEMA_GENERATION
                || !Arrays.equals(record.artifactGenerationSetDigest(), artifacts.setDigest())
                || !Arrays.equals(prepared.handoffPolicySnapshot().artifactGenerationSetDigest(), artifacts.setDigest())
                || !record.template().targetResource().equals(BrokerResourceIdentity.pulsar(prepared.target()))
                || record.template().physicalPartition() != Integer.toUnsignedLong(prepared.physicalPartition())
                || record.template().deliveryContract() != DeliveryContract.PULSAR_NATIVE_DELIVERY
                || !Long.valueOf(prepared.deliverAtEpochMs())
                        .equals(record.template().nativeDeliverAtEpochMs())
                || record.sequenceAuthority().kind() != PulsarSequenceAuthority.Kind.PRODUCER_ASSIGNED
                || record.externalIdentity().kind()
                        != com.nereusstream.delay.protocol.ExternalDeliveryIdentity.Kind.NATIVE_DELIVERY
                || !Arrays.equals(record.externalIdentity().nativeDeliveryId(), prepared.nativeDeliveryId())
                || !Arrays.equals(record.preparedIdentityHash(), prepared.submissionHash())
                || !matchesMetadata(prepared, record)
                || !matchesPinnedResource(prepared)) {
            return StableCode.PREPARED_SUBMISSION_MISMATCH;
        }
        try {
            NativePreparedRecordBinding.requireExact(recordContext(record), prepared);
        } catch (RuntimeException mismatch) {
            return StableCode.PREPARED_SUBMISSION_MISMATCH;
        }
        final boolean signatureValid;
        try {
            signatureValid = prepared.capabilitySnapshot().verifySignature(capabilityIssuerKey);
            handoffPolicyGate.require(prepared.handoffPolicySnapshot(), artifacts, nowEpochMs);
        } catch (RuntimeException invalid) {
            return StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE;
        }
        if (!signatureValid) {
            return StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE;
        }
        if (nowEpochMs < 0
                || nowEpochMs >= prepared.capabilityExpiryEpochMs()
                || nowEpochMs < prepared.handoffPolicySnapshot().validFromEpochMs()
                || nowEpochMs >= prepared.handoffPolicySnapshot().validUntilEpochMs()
                || !prepared.handoffPolicySnapshot().allows(HandoffPath.AUTO_FAST)
                || prepared.handoffPolicySnapshot().mode() != HandoffPolicyMode.ENABLED) {
            return StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE;
        }
        if (credentialFingerprintProvider != null) {
            final byte[] resolvedFingerprint;
            try {
                resolvedFingerprint = credentialFingerprintProvider.resolve(prepared);
                Bytes.requireLength(
                        resolvedFingerprint,
                        NativeCapabilitySnapshot.HASH_LENGTH,
                        "resolvedCredentialFingerprintDigest");
            } catch (RuntimeException unavailable) {
                return StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE;
            }
            if (!Bytes.constantTimeEquals(
                    resolvedFingerprint, prepared.capabilitySnapshot().resolvedCredentialFingerprintDigest())) {
                return StableCode.CREDENTIAL_BINDING_DRIFT;
            }
        }
        return null;
    }

    /** Verifies one already frozen Handoff lease immediately before native Producer ownership. */
    @FunctionalInterface
    public interface FrozenHandoffPolicyGate {
        void require(HandoffPolicySnapshot snapshot, ArtifactGenerationSet artifacts, long trustedNowEpochMs);
    }

    private static com.nereusstream.delay.protocol.NativePreparedRecordContext recordContext(
            final PulsarPreparedRecord record) {
        final var reserved = record.template().reservedMetadata();
        return new com.nereusstream.delay.protocol.NativePreparedRecordContext(
                reserved.routeIncarnation(),
                reserved.shardPartition(),
                reserved.messageId(),
                reserved.generation(),
                reserved.publishAttemptId(),
                record.artifactGenerationSetDigest());
    }

    private boolean matchesPinnedResource(final NativePreparedDelivery prepared) {
        final PulsarBrokerResourceIdentity target = prepared.target();
        return resource.authenticatedClusterId().equals(target.authenticatedClusterId())
                && Arrays.equals(resource.resourceIncarnation(), target.resourceIncarnation())
                && resource.physicalTopic().equals(target.physicalTopic())
                && resource.physicalTopicCreationTimestamp() == target.physicalTopicCreationTimestamp()
                && resource.partition() == prepared.physicalPartition();
    }

    private static boolean matchesMetadata(final NativePreparedDelivery prepared, final PulsarPreparedRecord record) {
        final PulsarMetadata metadata = prepared.metadata();
        final PulsarKey expectedKey = metadata.partitionKey() == null
                ? PulsarKey.none()
                : metadata.keyEncoding() == PulsarMetadata.KeyEncoding.UTF8
                        ? PulsarKey.utf8(metadata.partitionKey())
                        : PulsarKey.binary(metadata.partitionKey());
        return record.template().key().equals(expectedKey)
                && Arrays.equals(record.template().orderingKey(), metadata.orderingKey())
                && record.template().callerProperties().equals(metadata.properties())
                && Objects.equals(record.template().eventTimeEpochMs(), prepared.eventTimeEpochMs())
                && record.template().payload().hasInlinePayload()
                && Arrays.equals(record.template().payload().inlinePayload(), prepared.inlinePayload())
                && record.template().reservedMetadata().deliverAtEpochMs() == prepared.deliverAtEpochMs()
                && Arrays.equals(
                        record.template().reservedMetadata().destinationProfileSemanticHash(),
                        prepared.destination().semanticHash())
                && Arrays.equals(
                        record.template().reservedMetadata().capabilityProfileSemanticHash(),
                        prepared.capability().semanticHash());
    }

    /** Typed local rejection used by zero-I/O composition seams. */
    public static final class Rejection extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final StableCode code;

        public Rejection(final StableCode code) {
            this(code, null);
        }

        public Rejection(final StableCode code, final Throwable cause) {
            super(Objects.requireNonNull(code, "code").name(), cause);
            this.code = code;
        }

        public StableCode code() {
            return code;
        }
    }
}
