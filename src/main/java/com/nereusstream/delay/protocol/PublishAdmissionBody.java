package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Semantic parser for the source-replayable Publish Admission body.
 *
 * <p>The generic System Mutation codec checks the outer field shape. This
 * parser checks the admission equalities that must hold before an attempt
 * can become durable: descriptor/certificate/claim identity projections,
 * canonical descriptor and certificate digests, channel credential binding,
 * and the trusted decision interval.</p>
 */
public final class PublishAdmissionBody {
    private static final int HASH_LENGTH = 32;
    private static final int INCARNATION_LENGTH = 16;
    private static final int MESSAGE_ID_LENGTH = DelayMessageId.LENGTH;

    private final byte[] ownerIdentity;
    private final byte[] storeIncarnation;
    private final byte[] claimId;
    private final byte[] laneId;
    private final byte[] laneIncarnation;
    private final byte[] messageId;
    private final int generation;
    private final byte[] publishAttemptId;
    private final byte[] preparedPublishHash;
    private final byte[] reserveCharge;
    private final byte[] readyCertificateDigest;
    private final Channel channel;
    private final Descriptor descriptor;
    private final DecodedReadyCertificate readyCertificate;
    private final TrustedUtcIntervalEvidence decisionTime;
    private final ClaimPrecondition claimPrecondition;

    private PublishAdmissionBody(
            final byte[] ownerIdentity,
            final byte[] storeIncarnation,
            final byte[] claimId,
            final byte[] laneId,
            final byte[] laneIncarnation,
            final byte[] messageId,
            final int generation,
            final byte[] publishAttemptId,
            final byte[] preparedPublishHash,
            final byte[] reserveCharge,
            final byte[] readyCertificateDigest,
            final Channel channel,
            final Descriptor descriptor,
            final DecodedReadyCertificate readyCertificate,
            final TrustedUtcIntervalEvidence decisionTime,
            final ClaimPrecondition claimPrecondition) {
        this.ownerIdentity = copy(ownerIdentity);
        this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "storeIncarnation");
        this.claimId = fixed(claimId, HASH_LENGTH, "claimId");
        this.laneId = fixed(laneId, HASH_LENGTH, "destinationLaneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "delayMessageId");
        this.generation = generation;
        this.publishAttemptId = fixed(publishAttemptId, HASH_LENGTH, "publishAttemptId");
        this.preparedPublishHash = fixed(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
        this.reserveCharge = copy(reserveCharge);
        this.readyCertificateDigest = fixed(readyCertificateDigest, HASH_LENGTH, "readyCertificateDigest");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.readyCertificate = Objects.requireNonNull(readyCertificate, "readyCertificate");
        this.decisionTime = Objects.requireNonNull(decisionTime, "decisionTime");
        this.claimPrecondition = Objects.requireNonNull(claimPrecondition, "claimPrecondition");
        validateCrossObjectEqualities();
    }

    /** Parses and validates a canonical PUBLISH_ADMISSION body. */
    public static PublishAdmissionBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.PUBLISH_ADMISSION, canonicalBody);
        final byte[] owner = nested(field(fields, 10), 10);
        OwnerIdentity.decode(owner);
        final int generation = QueryCodecSupport.uint32Bits(field(fields, 16), 16);
        final byte[] reserveCharge = nested(field(fields, 19), 19);
        validateChargeVector(reserveCharge);
        final Channel channel = decodeChannel(nested(field(fields, 21), 21));
        final Descriptor descriptor = decodeDescriptor(nested(field(fields, 22), 22));
        final DecodedReadyCertificate certificate = decodeReadyCertificate(nested(field(fields, 23), 23));
        final TrustedUtcIntervalEvidence decision = TrustedUtcIntervalEvidence.decode(nested(field(fields, 24), 24));
        final ClaimPrecondition claim = decodeClaimPrecondition(nested(field(fields, 25), 25));
        final byte[] messageId = bytes(field(fields, 15), 15);
        SystemMutationBodyCodec.requireMessageShard(fields, new DelayMessageId(messageId), "Publish Admission");
        return new PublishAdmissionBody(
                owner,
                bytes(field(fields, 11), 11),
                bytes(field(fields, 12), 12),
                bytes(field(fields, 13), 13),
                bytes(field(fields, 14), 14),
                messageId,
                generation,
                bytes(field(fields, 17), 17),
                bytes(field(fields, 18), 18),
                reserveCharge,
                bytes(field(fields, 20), 20),
                channel,
                descriptor,
                certificate,
                decision,
                claim);
    }

    /**
     * Builds the one canonical PUBLISH_ADMISSION body from typed projections.
     *
     * <p>The result is decoded again before it is returned. This keeps the
     * producer-side construction path behind the same cross-object equality
     * and canonical-byte checks used by source-ordered replay.</p>
     */
    public static byte[] canonicalBytes(
            final ShardId shardId,
            final long retryUntilEpochMs,
            final OwnerIdentity owner,
            final byte[] storeIncarnation,
            final byte[] claimId,
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final DelayMessageId messageId,
            final long generation,
            final byte[] publishAttemptId,
            final PreparedPublishDescriptor descriptor,
            final ChargeVector reserveCharge,
            final ReadyCertificate readyCertificate,
            final TrustedUtcIntervalEvidence decisionTime,
            final byte[] claimPrecondition) {
        final ShardId subject = Objects.requireNonNull(shardId, "shardId");
        final OwnerIdentity typedOwner = Objects.requireNonNull(owner, "owner");
        final PreparedPublishDescriptor typedDescriptor = Objects.requireNonNull(descriptor, "descriptor");
        final ReadyCertificate typedCertificate = Objects.requireNonNull(readyCertificate, "readyCertificate");
        final TrustedUtcIntervalEvidence typedDecision = Objects.requireNonNull(decisionTime, "decisionTime");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubject(subject).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_ADMISSION.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, typedOwner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, fixed(storeIncarnation, INCARNATION_LENGTH, "storeIncarnation"));
            CanonicalProtobuf.bytes(output, 12, fixed(claimId, HASH_LENGTH, "claimId"));
            CanonicalProtobuf.bytes(
                    output, 13, Objects.requireNonNull(laneId, "laneId").bytes());
            CanonicalProtobuf.bytes(output, 14, fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation"));
            CanonicalProtobuf.bytes(
                    output, 15, Objects.requireNonNull(messageId, "messageId").bytes());
            CanonicalProtobuf.uint32(output, 16, checkedUint32(generation, "generation"));
            CanonicalProtobuf.bytes(output, 17, fixed(publishAttemptId, HASH_LENGTH, "publishAttemptId"));
            CanonicalProtobuf.bytes(output, 18, typedDescriptor.preparedPublishHash());
            CanonicalProtobuf.bytes(
                    output,
                    19,
                    Objects.requireNonNull(reserveCharge, "reserveCharge").canonicalBytes());
            CanonicalProtobuf.bytes(output, 20, typedCertificate.certificateDigest());
            CanonicalProtobuf.bytes(output, 21, typedDescriptor.channel().canonicalBytes());
            CanonicalProtobuf.bytes(output, 22, typedDescriptor.canonicalBytes());
            CanonicalProtobuf.bytes(output, 23, typedCertificate.canonicalBytes());
            CanonicalProtobuf.bytes(output, 24, typedDecision.canonicalBytes());
            CanonicalProtobuf.bytes(output, 25, requireNonEmpty(claimPrecondition, "claimPrecondition"));
        });
        decode(encoded);
        return encoded;
    }

    public byte[] ownerIdentity() {
        return copy(ownerIdentity);
    }

    public byte[] storeIncarnation() {
        return copy(storeIncarnation);
    }

    public byte[] claimId() {
        return copy(claimId);
    }

    public byte[] laneId() {
        return copy(laneId);
    }

    public byte[] laneIncarnation() {
        return copy(laneIncarnation);
    }

    public byte[] messageId() {
        return copy(messageId);
    }

    public int generation() {
        return generation;
    }

    public byte[] publishAttemptId() {
        return copy(publishAttemptId);
    }

    public byte[] preparedPublishHash() {
        return copy(preparedPublishHash);
    }

    public byte[] reserveCharge() {
        return copy(reserveCharge);
    }

    /** Decodes the closed 17-dimensional logical charge vector carried by this admission. */
    public ChargeVector chargeVector() {
        final List<CanonicalProtobuf.Reader.Field> fields = read(reserveCharge, "ChargeVector");
        requireExactFields(fields, 17, "ChargeVector");
        final long[] values = new long[17];
        for (int index = 0; index < values.length; index++) {
            values[index] = rawUnsigned(field(fields, index + 1), index + 1);
        }
        return new ChargeVector(
                values[0],
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
                values[6],
                values[7],
                values[8],
                values[9],
                values[10],
                values[11],
                values[12],
                values[13],
                values[14],
                values[15],
                values[16]);
    }

    public byte[] readyCertificateDigest() {
        return copy(readyCertificateDigest);
    }

    public Channel channel() {
        return channel;
    }

    public Descriptor descriptor() {
        return descriptor;
    }

    public DecodedReadyCertificate readyCertificate() {
        return readyCertificate;
    }

    public TrustedUtcIntervalEvidence decisionTime() {
        return decisionTime;
    }

    public ClaimPrecondition claimPrecondition() {
        return claimPrecondition;
    }

    /** Requires the body decision interval to prove the supplied message timing. */
    public void requireTiming(final long actionAt, final long expireAt) {
        if (actionAt < 0
                || expireAt < actionAt
                || decisionTime.earliestEpochMs() < actionAt
                || decisionTime.latestEpochMs() >= expireAt
                || decisionTime.latestEpochMs() >= readyCertificate.validUntilEpochMs()) {
            throw new IllegalArgumentException("Publish Admission decision time is stale");
        }
    }

    /**
     * Requires the source record's authenticated Broker persistence time to
     * remain within the activated decision-evidence and expiry bounds.
     * Arithmetic is checked so an overflowing timestamp cannot bypass the
     * fail-closed timing fence.
     */
    public void requireBrokerTiming(
            final long brokerPersistenceTimeEpochMs,
            final long maxIngressBrokerTimestampDivergenceMs,
            final long maximumAdmissionMutationEnqueueAgeMs) {
        if (brokerPersistenceTimeEpochMs < 0
                || maxIngressBrokerTimestampDivergenceMs < 0
                || maximumAdmissionMutationEnqueueAgeMs < 0) {
            throw new IllegalArgumentException("Broker timing bounds must be non-negative");
        }
        final long safeBrokerDeadline;
        final long allowedDecisionDistance;
        try {
            safeBrokerDeadline = Math.addExact(brokerPersistenceTimeEpochMs, maxIngressBrokerTimestampDivergenceMs);
            allowedDecisionDistance =
                    Math.addExact(maximumAdmissionMutationEnqueueAgeMs, maxIngressBrokerTimestampDivergenceMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Broker timing arithmetic overflow", overflow);
        }
        final long expiryDeadline = Math.min(descriptor.expireAtEpochMs(), readyCertificate.validUntilEpochMs());
        if (safeBrokerDeadline >= expiryDeadline) {
            throw new IllegalArgumentException("Broker persistence time is too close to Admission expiry");
        }
        final long decisionDistance;
        try {
            if (brokerPersistenceTimeEpochMs < decisionTime.earliestEpochMs()) {
                decisionDistance = Math.subtractExact(decisionTime.earliestEpochMs(), brokerPersistenceTimeEpochMs);
            } else if (brokerPersistenceTimeEpochMs > decisionTime.latestEpochMs()) {
                decisionDistance = Math.subtractExact(brokerPersistenceTimeEpochMs, decisionTime.latestEpochMs());
            } else {
                decisionDistance = 0;
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Broker/decision timing arithmetic overflow", overflow);
        }
        if (decisionDistance > allowedDecisionDistance) {
            throw new IllegalArgumentException("Broker persistence time is outside Admission enqueue age");
        }
    }

    /** Requires the ordinary managed timing relationship for a non-catalogued Admission. */
    public void requireOrdinaryManagedTiming() {
        if (descriptor.actionAtEpochMs() != descriptor.deliverAtEpochMs()) {
            throw new IllegalArgumentException("ordinary managed Admission requires actionAt=deliverAt");
        }
    }

    /**
     * Validates the profile-pinned timing relationship for a managed Admission.
     * Ordinary managed delivery uses {@code actionAt=deliverAt}; certified
     * Pulsar handoff uses the one fixed lead from the immutable Destination
     * Profile and the guarded-handoff capability bit.
     */
    public void requireTimingPolicy(
            final DestinationProfileSemantic destinationProfile, final DeliveryCapabilitySemantic capabilityProfile) {
        Objects.requireNonNull(destinationProfile, "destinationProfile");
        Objects.requireNonNull(capabilityProfile, "capabilityProfile");
        final ProfileRef destinationRef = ProfileRef.decode(descriptor.destinationProfile());
        final ProfileRef capabilityRef = ProfileRef.decode(descriptor.capabilityProfile());
        if (!destinationProfile.deliveryCapability().equals(capabilityRef)
                || destinationProfile.adapterKind() != capabilityProfile.adapterKind()
                || destinationProfile.adapterEncodingVersion()
                        != descriptor.value().adapterEncodingVersion()
                || !Arrays.equals(destinationProfile.targetResource().canonicalBytes(), descriptor.targetResource())) {
            throw new IllegalArgumentException("Publish Admission Profile identity mismatch");
        }
        final ChannelResourceIdentity channelIdentity = ChannelResourceIdentity.decode(channel.canonicalBytes());
        if (destinationProfile.adapterKind() != channelIdentity.adapterKind()
                || !Arrays.equals(
                        destinationProfile.targetResource().canonicalBytes(),
                        channelIdentity.targetResource().canonicalBytes())) {
            throw new IllegalArgumentException("Publish Admission Profile/channel identity mismatch");
        }
        final long physicalPartition = channelIdentity.physicalPartition();
        final long targetPartitionCount = Integer.toUnsignedLong(destinationProfile.targetPartitionCount());
        if (physicalPartition >= targetPartitionCount) {
            throw new IllegalArgumentException("Publish Admission physical partition is outside Profile policy");
        }
        final boolean explicitPartition =
                destinationProfile.allowedExplicitPartitions().contains((int) physicalPartition);
        if (destinationProfile.targetPartitionPolicy() == TargetPartitionPolicy.EXPLICIT_ONLY && !explicitPartition) {
            throw new IllegalArgumentException("Publish Admission physical partition is not explicitly allowed");
        }
        if (destinationProfile.targetPartitionPolicy() == TargetPartitionPolicy.HASH_ONLY
                || (destinationProfile.targetPartitionPolicy() == TargetPartitionPolicy.EXPLICIT_OR_HASH
                        && !explicitPartition)) {
            requireHashedPartition(destinationProfile, destinationRef, physicalPartition);
        }
        final PreparedPublishDescriptor typedDescriptor = descriptor.value();
        final long deliverAt = typedDescriptor.deliverAtEpochMs();
        final long actionAt = typedDescriptor.actionAtEpochMs();
        if (typedDescriptor.legacyEncoding() && actionAt != deliverAt) {
            if (destinationProfile.adapterKind() != AdapterKind.PULSAR
                    || destinationProfile.handoffLeadMs() <= 0
                    || !TimingCapability.includes(
                            capabilityProfile.timingCapabilityBits(), TimingCapability.PULSAR_GUARDED_HANDOFF)) {
                throw new IllegalArgumentException("certified Pulsar handoff is not capability-authorized");
            }
            final long expectedActionAt;
            try {
                expectedActionAt = Math.subtractExact(deliverAt, destinationProfile.handoffLeadMs());
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("Pulsar handoff timing underflows deliverAt", overflow);
            }
            if (expectedActionAt < 0 || actionAt != expectedActionAt) {
                throw new IllegalArgumentException("Pulsar handoff actionAt does not match the pinned lead");
            }
            return;
        }
        if (typedDescriptor.deliveryContract() == DeliveryContract.NEREUS_MANAGED_NOT_BEFORE) {
            if (typedDescriptor.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID
                    && typedDescriptor.handoffPolicySnapshot() != null) {
                throw new IllegalArgumentException("FORBID managed Admission cannot carry a handoff snapshot");
            }
            if (actionAt != deliverAt) {
                throw new IllegalArgumentException("ordinary managed timing requires actionAt=deliverAt");
            }
            if (!TimingCapability.includes(
                    capabilityProfile.timingCapabilityBits(), TimingCapability.ORDINARY_MANAGED)) {
                throw new IllegalArgumentException("ordinary managed timing is not capability-authorized");
            }
            return;
        }
        if (destinationProfile.adapterKind() != AdapterKind.PULSAR
                || typedDescriptor.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID
                || typedDescriptor.handoffPolicySnapshot() == null
                || typedDescriptor.handoffPolicySnapshot().mode() != HandoffPolicyMode.ENABLED
                || !typedDescriptor.handoffPolicySnapshot().allows(HandoffPath.MANAGED_HANDOFF)
                || destinationProfile.handoffLeadMs() <= 0
                || !TimingCapability.includes(
                        capabilityProfile.timingCapabilityBits(), TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF)) {
            throw new IllegalArgumentException("certified Pulsar handoff is not capability-authorized");
        }
        final long expectedActionAt;
        try {
            expectedActionAt = Math.subtractExact(
                    deliverAt, typedDescriptor.handoffPolicySnapshot().effectiveLeadMs());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Pulsar handoff timing underflows deliverAt", overflow);
        }
        if (expectedActionAt < 0
                || actionAt != expectedActionAt
                || typedDescriptor.handoffPolicySnapshot().effectiveLeadMs() > destinationProfile.handoffLeadMs()) {
            throw new IllegalArgumentException("Pulsar handoff actionAt does not match the pinned lead");
        }
    }

    private void requireHashedPartition(
            final DestinationProfileSemantic destinationProfile,
            final ProfileRef destinationRef,
            final long physicalPartition) {
        final AdapterMetadata metadata = AdapterMetadata.decode(descriptor.metadata());
        final byte[] routingBytes =
                switch (destinationProfile.targetPartitionHashInput()) {
                    case ORDERING_KEY -> orderingKeyBytes(metadata);
                    case ADAPTER_MESSAGE_KEY ->
                        metadata.kind() == AdapterMetadata.Kind.KAFKA
                                ? nullableBytes(metadata.kafka().key())
                                : nullableBytes(metadata.pulsar().partitionKey());
                    case DELAY_MESSAGE_ID -> descriptor.messageId();
                };
        final long expectedPartition =
                TargetPartitionHash.partition(destinationRef, destinationProfile.targetPartitionCount(), routingBytes);
        if (physicalPartition != expectedPartition) {
            throw new IllegalArgumentException("Publish Admission physical partition hash mismatch");
        }
    }

    private static byte[] nullableBytes(final byte[] value) {
        return value == null ? new byte[0] : value;
    }

    private static byte[] orderingKeyBytes(final AdapterMetadata metadata) {
        if (metadata.kind() != AdapterMetadata.Kind.PULSAR) {
            throw new IllegalArgumentException("Kafka descriptor cannot prove an ordering-key hash");
        }
        return nullableBytes(metadata.pulsar().orderingKey());
    }

    private void validateCrossObjectEqualities() {
        final ChannelResourceIdentity channelIdentity = ChannelResourceIdentity.decode(channel.canonicalBytes());
        if (!Arrays.equals(descriptor.destinationLaneId(), laneId)
                || !Arrays.equals(descriptor.laneIncarnation(), laneIncarnation)
                || !Arrays.equals(descriptor.channel(), channel.canonicalBytes())
                || !Arrays.equals(descriptor.messageId(), messageId)
                || descriptor.generation() != generation
                || !Arrays.equals(descriptor.publishAttemptId(), publishAttemptId)) {
            throw new IllegalArgumentException("Publish Admission descriptor identity mismatch");
        }
        if (!Arrays.equals(
                descriptor.destinationProfile(),
                channelIdentity.credentialUseLease().profile().canonicalBytes())) {
            throw new IllegalArgumentException("Publish Admission descriptor/channel Profile mismatch");
        }
        if (!Arrays.equals(readyCertificate.ownerIdentity(), ownerIdentity)
                || !Arrays.equals(readyCertificate.storeIncarnation(), storeIncarnation)
                || !Arrays.equals(readyCertificate.destinationLaneId(), laneId)
                || !Arrays.equals(readyCertificate.laneIncarnation(), laneIncarnation)
                || !Arrays.equals(readyCertificate.channel(), channel.canonicalBytes())
                || !Arrays.equals(readyCertificate.certificateDigest(), readyCertificateDigest)) {
            throw new IllegalArgumentException("Publish Admission certificate identity mismatch");
        }
        final ClaimPrecondition claim = claimPrecondition;
        if (!Arrays.equals(claim.claimId(), claimId)
                || !Arrays.equals(claim.messageId(), messageId)
                || claim.generation() != generation
                || !Arrays.equals(claim.destinationLaneId(), laneId)
                || !Arrays.equals(claim.laneIncarnation(), laneIncarnation)
                || !Arrays.equals(claim.ownerIdentity(), ownerIdentity)
                || !Arrays.equals(claim.storeIncarnation(), storeIncarnation)) {
            throw new IllegalArgumentException("Publish Admission Claim precondition mismatch");
        }
        final ClaimMaterialization claimMaterialization = claim.materializationValue();
        final byte[] expectedMaterialization = descriptor
                .value()
                .materialization(claimMaterialization.handoffPolicyHeadRef())
                .canonicalBytes();
        if (!Arrays.equals(expectedMaterialization, claim.materialization())) {
            throw new IllegalArgumentException("Publish Admission materialization mismatch");
        }
        if (!Arrays.equals(descriptor.preparedPublishHash(), preparedPublishHash)) {
            throw new IllegalArgumentException("Publish Admission descriptor hash mismatch");
        }
        if (readyCertificate.credentialBindingGeneration() != channel.credentialBindingGeneration()
                || !Arrays.equals(readyCertificate.credentialBindingDigest(), channel.credentialBindingDigest())
                || !Arrays.equals(readyCertificate.credentialFingerprint(), channel.credentialFingerprint())) {
            throw new IllegalArgumentException("Publish Admission certificate/channel credential mismatch");
        }
        if (readyCertificate.validUntilEpochMs()
                > channelIdentity.credentialUseLease().validUntilEpochMs()) {
            throw new IllegalArgumentException("Publish Admission certificate outlives channel credential lease");
        }
        if (descriptor.deliverAtEpochMs() < 0
                || descriptor.expireAtEpochMs() < descriptor.deliverAtEpochMs()
                || descriptor.actionAtEpochMs() < 0
                || descriptor.actionAtEpochMs() > descriptor.deliverAtEpochMs()) {
            throw new IllegalArgumentException("unsupported Publish Admission timing");
        }
    }

    private static Channel decodeChannel(final byte[] encoded) {
        final ChannelResourceIdentity identity = ChannelResourceIdentity.decode(encoded);
        return new Channel(
                encoded,
                identity.destinationLaneId(),
                identity.laneIncarnation(),
                identity.targetResource().canonicalBytes(),
                identity.producerOrTransactionalIdentity(),
                identity.credentialBindingGeneration(),
                identity.credentialBindingDigest(),
                identity.resolvedCredentialVersionFingerprintDigest());
    }

    private static Descriptor decodeDescriptor(final byte[] encoded) {
        final PreparedPublishDescriptor typed = PreparedPublishDescriptor.decode(encoded);
        return new Descriptor(
                encoded,
                typed.preparedPublishHash(),
                typed.destinationLaneId().bytes(),
                typed.laneIncarnation(),
                typed.channel().canonicalBytes(),
                typed.messageId().bytes(),
                rawUint32(typed.generation(), "descriptor generation"),
                typed.publishAttemptId(),
                rawUint32(typed.attemptNo(), "descriptor attemptNo"),
                typed.destinationProfile().canonicalBytes(),
                typed.capabilityProfile().canonicalBytes(),
                typed.targetResource().canonicalBytes(),
                typed.payload().canonicalBytes(),
                typed.businessMetadata().canonicalBytes(),
                typed.deliverAtEpochMs(),
                typed.expireAtEpochMs(),
                typed.actionAtEpochMs());
    }

    /** Validates and decodes a Registry ReadyCertificate outside an Admission body. */
    public static DecodedReadyCertificate decodeReadyCertificate(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readRepeated(encoded, "ReadyCertificate");
        if (fields.size() < 16) {
            throw new IllegalArgumentException("ReadyCertificate fields are incomplete");
        }
        for (int number = 1; number <= 7; number++) {
            if (fields.get(number - 1).number() != number) {
                throw new IllegalArgumentException("ReadyCertificate common field order mismatch");
            }
        }
        int index = 7;
        int evidenceCount = 0;
        while (index < fields.size() && fields.get(index).number() == 8) {
            evidenceCount++;
            index++;
        }
        if (evidenceCount == 0 || index + 8 != fields.size()) {
            throw new IllegalArgumentException("ReadyCertificate evidence cursor field order mismatch");
        }
        for (int number = 9; number <= 16; number++) {
            if (fields.get(index++).number() != number) {
                throw new IllegalArgumentException("ReadyCertificate trailing field order mismatch");
            }
        }
        if (unsigned(field(fields, 1), 1) != 1) {
            throw new IllegalArgumentException("unsupported ReadyCertificate version");
        }
        final byte[] owner = nested(field(fields, 2), 2);
        OwnerIdentity.decode(owner);
        final byte[] channel = nested(field(fields, 6), 6);
        decodeChannel(channel);
        ActivationBarrier.decode(nested(field(fields, 7), 7));
        final List<EvidenceCursor> evidenceCursors = new ArrayList<>(evidenceCount);
        for (int cursorIndex = 0; cursorIndex < evidenceCount; cursorIndex++) {
            evidenceCursors.add(EvidenceCursor.decode(nested(fields.get(7 + cursorIndex), 8)));
        }
        for (int cursorIndex = 1; cursorIndex < evidenceCursors.size(); cursorIndex++) {
            if (evidenceCursors.get(cursorIndex - 1).compareTo(evidenceCursors.get(cursorIndex)) >= 0) {
                throw new IllegalArgumentException("ReadyCertificate evidence cursors must be sorted and unique");
            }
        }
        final long brokerResourceAttestationGeneration = rawUnsigned(field(fields, 9), 9);
        final long configGeneration = rawUnsigned(field(fields, 10), 10);
        final TrustedUtcIntervalEvidence issuedAt = TrustedUtcIntervalEvidence.decode(nested(field(fields, 12), 12));
        final byte[] digest = bytes(field(fields, 16), 16);
        final byte[] expectedDigest =
                Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate\0"), canonicalFields(fields, 15));
        if (!Arrays.equals(digest, expectedDigest)) {
            throw new IllegalArgumentException("ReadyCertificate digest mismatch");
        }
        return new DecodedReadyCertificate(
                encoded,
                owner,
                bytes(field(fields, 3), 3),
                bytes(field(fields, 4), 4),
                bytes(field(fields, 5), 5),
                channel,
                brokerResourceAttestationGeneration,
                configGeneration,
                unsigned(field(fields, 11), 11),
                issuedAt,
                rawUnsigned(field(fields, 13), 13),
                bytes(field(fields, 14), 14),
                bytes(field(fields, 15), 15),
                digest);
    }

    private static ClaimPrecondition decodeClaimPrecondition(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ClaimPrecondition");
        requireExactFieldsRange(fields, 18, 20, "ClaimPrecondition");
        for (int number = 1; number <= 9; number++) {
            field(fields, number);
        }
        for (int number = 12; number <= 20; number++) {
            field(fields, number);
        }
        final boolean hasMaterialization = has(fields, 10);
        if (hasMaterialization != has(fields, 11)) {
            throw new IllegalArgumentException("Claim materialization fields must be paired");
        }
        if (!hasMaterialization) {
            throw new IllegalArgumentException("Publish Admission requires Claim materialization");
        }
        final byte[] materialization = nested(field(fields, 10), 10);
        final ClaimMaterialization materializationValue = ClaimMaterialization.decode(materialization);
        final byte[] materializationDigest = bytes(field(fields, 11), 11);
        final byte[] expectedDigest = materializationValue.materializationDigest();
        if (!Arrays.equals(materializationDigest, expectedDigest)) {
            throw new IllegalArgumentException("Claim materialization digest mismatch");
        }
        final byte[] owner = nested(field(fields, 14), 14);
        OwnerIdentity.decode(owner);
        validateChargeVector(nested(field(fields, 12), 12));
        return new ClaimPrecondition(
                encoded,
                bytes(field(fields, 1), 1),
                bytes(field(fields, 2), 2),
                QueryCodecSupport.uint32Bits(field(fields, 3), 3),
                bytes(field(fields, 5), 5),
                bytes(field(fields, 6), 6),
                owner,
                bytes(field(fields, 15), 15),
                materialization);
    }

    private static void validateBrokerResource(final byte[] encoded) {
        BrokerResourceIdentity.decode(encoded);
    }

    private static void validateChargeVector(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ChargeVector");
        requireExactFields(fields, 17, "ChargeVector");
        for (int number = 1; number <= 17; number++) {
            rawUnsigned(field(fields, number), number);
        }
    }

    /** Exact fields 1-17 of Registry ChargeVector. */
    public record ChargeVector(
            long activeMessages,
            long pendingPayloadBytes,
            long logicalStateBytes,
            long retainedBytes,
            long reservationMessages,
            long reservationPayloadBytes,
            long inflightMessages,
            long inflightBytes,
            long resultRecords,
            long resultBytes,
            long systemMutationRecords,
            long systemMutationBytes,
            long outcomeWalBytes,
            long evidenceRecords,
            long evidenceBytes,
            long laneCount,
            long strongLaneCount) {
        public ChargeVector {
            // Registry ChargeVector fields are complete uint64 bit patterns.
            // The embedded runtime applies a separate signed-capacity guard
            // before it performs local arithmetic (see
            // requireLocalCapacityRange()).
        }

        public long outcomeReserveRecords() {
            requireLocalCapacityRange();
            return Math.addExact(Math.addExact(resultRecords, systemMutationRecords), evidenceRecords);
        }

        public long outcomeReserveBytes() {
            requireLocalCapacityRange();
            return Math.addExact(
                    Math.addExact(resultBytes, systemMutationBytes), Math.addExact(outcomeWalBytes, evidenceBytes));
        }

        /** Rejects values outside the embedded runtime's signed capacity envelope. */
        public void requireLocalCapacityRange() {
            if (activeMessages < 0
                    || pendingPayloadBytes < 0
                    || logicalStateBytes < 0
                    || retainedBytes < 0
                    || reservationMessages < 0
                    || reservationPayloadBytes < 0
                    || inflightMessages < 0
                    || inflightBytes < 0
                    || resultRecords < 0
                    || resultBytes < 0
                    || systemMutationRecords < 0
                    || systemMutationBytes < 0
                    || outcomeWalBytes < 0
                    || evidenceRecords < 0
                    || evidenceBytes < 0
                    || laneCount < 0
                    || strongLaneCount < 0) {
                throw new IllegalArgumentException("ChargeVector exceeds local signed capacity range");
            }
        }

        /** Canonical fields 1-17 projection used by QuotaGrantRef. */
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint64Bits(output, 1, activeMessages);
                CanonicalProtobuf.uint64Bits(output, 2, pendingPayloadBytes);
                CanonicalProtobuf.uint64Bits(output, 3, logicalStateBytes);
                CanonicalProtobuf.uint64Bits(output, 4, retainedBytes);
                CanonicalProtobuf.uint64Bits(output, 5, reservationMessages);
                CanonicalProtobuf.uint64Bits(output, 6, reservationPayloadBytes);
                CanonicalProtobuf.uint64Bits(output, 7, inflightMessages);
                CanonicalProtobuf.uint64Bits(output, 8, inflightBytes);
                CanonicalProtobuf.uint64Bits(output, 9, resultRecords);
                CanonicalProtobuf.uint64Bits(output, 10, resultBytes);
                CanonicalProtobuf.uint64Bits(output, 11, systemMutationRecords);
                CanonicalProtobuf.uint64Bits(output, 12, systemMutationBytes);
                CanonicalProtobuf.uint64Bits(output, 13, outcomeWalBytes);
                CanonicalProtobuf.uint64Bits(output, 14, evidenceRecords);
                CanonicalProtobuf.uint64Bits(output, 15, evidenceBytes);
                CanonicalProtobuf.uint64Bits(output, 16, laneCount);
                CanonicalProtobuf.uint64Bits(output, 17, strongLaneCount);
            });
        }

        /** Strictly decodes canonical fields 1-17 of a ChargeVector. */
        public static ChargeVector decodeCanonical(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ChargeVector");
            requireExactFields(fields, 17, "ChargeVector");
            final long[] values = new long[17];
            for (int index = 0; index < values.length; index++) {
                values[index] = rawUnsigned(field(fields, index + 1), index + 1);
            }
            final ChargeVector result = new ChargeVector(
                    values[0],
                    values[1],
                    values[2],
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                    values[7],
                    values[8],
                    values[9],
                    values[10],
                    values[11],
                    values[12],
                    values[13],
                    values[14],
                    values[15],
                    values[16]);
            if (!Arrays.equals(encoded, result.canonicalBytes())) {
                throw new IllegalArgumentException("non-canonical ChargeVector");
            }
            return result;
        }

        /** Projects the 17 logical charge dimensions into the closed 66-dimensional vector. */
        public CapacityVector toCapacityVector() {
            requireLocalCapacityRange();
            final long[] amounts = new long[CapacityDimension.COUNT];
            final long[] charge = {
                activeMessages,
                pendingPayloadBytes,
                logicalStateBytes,
                retainedBytes,
                reservationMessages,
                reservationPayloadBytes,
                inflightMessages,
                inflightBytes,
                resultRecords,
                resultBytes,
                systemMutationRecords,
                systemMutationBytes,
                outcomeWalBytes,
                evidenceRecords,
                evidenceBytes,
                laneCount,
                strongLaneCount
            };
            System.arraycopy(charge, 0, amounts, 0, charge.length);
            return new CapacityVector(amounts);
        }
    }

    private static byte[] canonicalFields(final List<CanonicalProtobuf.Reader.Field> fields, final int through) {
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : fields) {
                if (field.number() > through) {
                    break;
                }
                if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        if (encoded.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static List<CanonicalProtobuf.Reader.Field> readRepeated(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        if (encoded.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static void requireExactFields(
            final List<CanonicalProtobuf.Reader.Field> fields, final int count, final String name) {
        if (fields.size() != count) {
            throw new IllegalArgumentException(name + " has unexpected field count");
        }
    }

    private static void requireExactFieldsRange(
            final List<CanonicalProtobuf.Reader.Field> fields, final int min, final int max, final String name) {
        if (fields.size() < min || fields.size() > max) {
            throw new IllegalArgumentException(name + " has unexpected field count");
        }
    }

    private static void requireFieldCountRange(
            final List<CanonicalProtobuf.Reader.Field> fields, final int min, final int max, final String name) {
        requireExactFieldsRange(fields, min, max, name);
        for (int number = 1; number <= 10; number++) {
            field(fields, number);
        }
        for (int number = 13; number <= 17; number++) {
            field(fields, number);
        }
    }

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing nested field " + number);
    }

    private static boolean has(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        return fields.stream().anyMatch(field -> field.number() == number);
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid nested scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static long rawUnsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid nested scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        return runtimeUint32(value, "nested uint32");
    }

    /** Narrows a Registry uint32 only at the legacy signed runtime boundary. */
    private static int runtimeUint32(final long value, final String name) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds runtime range");
        }
        return (int) value;
    }

    private static int rawUint32(final long value, final String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " is outside uint32 range");
        }
        return (int) value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid nested bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        read(value, "nested field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final int number) {
        Bytes.requireLength(value, length, "nested field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final int number) {
        if (value.length == 0) {
            throw new IllegalArgumentException("nested field " + number + " must not be empty");
        }
        return value;
    }

    private static byte[] copy(final byte[] value) {
        return Bytes.copy(Objects.requireNonNull(value, "value"));
    }

    private static byte[] requireNonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static int checkedUint32(final long value, final String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " is outside uint32 range");
        }
        return (int) value;
    }

    public static final class Channel {
        private final byte[] canonicalBytes;
        private final byte[] laneId;
        private final byte[] laneIncarnation;
        private final byte[] targetResource;
        private final byte[] producerIdentity;
        private final long credentialBindingGeneration;
        private final byte[] credentialBindingDigest;
        private final byte[] credentialFingerprint;

        private Channel(
                final byte[] canonicalBytes,
                final byte[] laneId,
                final byte[] laneIncarnation,
                final byte[] targetResource,
                final byte[] producerIdentity,
                final long credentialBindingGeneration,
                final byte[] credentialBindingDigest,
                final byte[] credentialFingerprint) {
            this.canonicalBytes = copy(canonicalBytes);
            this.laneId = fixed(laneId, HASH_LENGTH, "channel lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "channel lane incarnation");
            this.targetResource = copy(targetResource);
            this.producerIdentity = copy(producerIdentity);
            this.credentialBindingGeneration = credentialBindingGeneration;
            this.credentialBindingDigest =
                    fixed(credentialBindingDigest, HASH_LENGTH, "channel credential binding digest");
            this.credentialFingerprint = fixed(credentialFingerprint, HASH_LENGTH, "channel credential fingerprint");
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] laneId() {
            return copy(laneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] targetResource() {
            return copy(targetResource);
        }

        public byte[] producerIdentity() {
            return copy(producerIdentity);
        }

        public long credentialBindingGeneration() {
            return credentialBindingGeneration;
        }

        public byte[] credentialBindingDigest() {
            return copy(credentialBindingDigest);
        }

        public byte[] credentialFingerprint() {
            return copy(credentialFingerprint);
        }
    }

    /** Validates and decodes a registry ChannelResourceIdentity outside an Admission body. */
    public static Channel decodeChannelIdentity(final byte[] encoded) {
        return decodeChannel(Objects.requireNonNull(encoded, "encoded"));
    }

    /** Validates a registry BrokerResourceIdentity outside an Admission body. */
    public static void validateBrokerResourceIdentity(final byte[] encoded) {
        validateBrokerResource(Objects.requireNonNull(encoded, "encoded"));
    }

    public static final class Descriptor {
        private final byte[] canonicalBytes;
        private final byte[] preparedPublishHash;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final byte[] channel;
        private final byte[] messageId;
        private final int generation;
        private final byte[] publishAttemptId;
        private final int attemptNo;
        private final byte[] destinationProfile;
        private final byte[] capabilityProfile;
        private final byte[] targetResource;
        private final byte[] payload;
        private final byte[] metadata;
        private final long deliverAtEpochMs;
        private final long expireAtEpochMs;
        private final long actionAtEpochMs;

        private Descriptor(
                final byte[] canonicalBytes,
                final byte[] preparedPublishHash,
                final byte[] destinationLaneId,
                final byte[] laneIncarnation,
                final byte[] channel,
                final byte[] messageId,
                final int generation,
                final byte[] publishAttemptId,
                final int attemptNo,
                final byte[] destinationProfile,
                final byte[] capabilityProfile,
                final byte[] targetResource,
                final byte[] payload,
                final byte[] metadata,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long actionAtEpochMs) {
            this.canonicalBytes = copy(canonicalBytes);
            this.preparedPublishHash = fixed(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "descriptor lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "descriptor lane incarnation");
            this.channel = copy(channel);
            this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "descriptor message id");
            this.generation = generation;
            this.publishAttemptId = fixed(publishAttemptId, HASH_LENGTH, "descriptor attempt id");
            this.attemptNo = attemptNo;
            this.destinationProfile = copy(destinationProfile);
            this.capabilityProfile = copy(capabilityProfile);
            this.targetResource = copy(targetResource);
            this.payload = copy(payload);
            this.metadata = copy(metadata);
            this.deliverAtEpochMs = deliverAtEpochMs;
            this.expireAtEpochMs = expireAtEpochMs;
            this.actionAtEpochMs = actionAtEpochMs;
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] preparedPublishHash() {
            return copy(preparedPublishHash);
        }

        public byte[] destinationLaneId() {
            return copy(destinationLaneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] channel() {
            return copy(channel);
        }

        public byte[] messageId() {
            return copy(messageId);
        }

        public int generation() {
            return generation;
        }

        public byte[] publishAttemptId() {
            return copy(publishAttemptId);
        }

        public int attemptNo() {
            return attemptNo;
        }

        public byte[] destinationProfile() {
            return copy(destinationProfile);
        }

        public byte[] capabilityProfile() {
            return copy(capabilityProfile);
        }

        public byte[] targetResource() {
            return copy(targetResource);
        }

        public byte[] payload() {
            return copy(payload);
        }

        public byte[] metadata() {
            return copy(metadata);
        }

        public long deliverAtEpochMs() {
            return deliverAtEpochMs;
        }

        public long expireAtEpochMs() {
            return expireAtEpochMs;
        }

        public long actionAtEpochMs() {
            return actionAtEpochMs;
        }

        public byte[] materializationBytes() {
            return value().materialization().canonicalBytes();
        }

        /** Returns the typed replay-stable Claim materialization projection. */
        public ClaimMaterialization materialization() {
            return ClaimMaterialization.decode(materializationBytes());
        }

        /** Returns the exact typed Prepared Publish descriptor projection. */
        public PreparedPublishDescriptor value() {
            return PreparedPublishDescriptor.decode(canonicalBytes);
        }
    }

    public static final class DecodedReadyCertificate {
        private final byte[] canonicalBytes;
        private final byte[] ownerIdentity;
        private final byte[] storeIncarnation;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final byte[] channel;
        private final long brokerResourceAttestationGeneration;
        private final long configGeneration;
        private final long validUntilEpochMs;
        private final TrustedUtcIntervalEvidence issuedAt;
        private final long credentialBindingGeneration;
        private final byte[] credentialBindingDigest;
        private final byte[] credentialFingerprint;
        private final byte[] certificateDigest;

        private DecodedReadyCertificate(
                final byte[] canonicalBytes,
                final byte[] ownerIdentity,
                final byte[] storeIncarnation,
                final byte[] destinationLaneId,
                final byte[] laneIncarnation,
                final byte[] channel,
                final long brokerResourceAttestationGeneration,
                final long configGeneration,
                final long validUntilEpochMs,
                final TrustedUtcIntervalEvidence issuedAt,
                final long credentialBindingGeneration,
                final byte[] credentialBindingDigest,
                final byte[] credentialFingerprint,
                final byte[] certificateDigest) {
            this.canonicalBytes = copy(canonicalBytes);
            this.ownerIdentity = copy(ownerIdentity);
            this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "certificate store incarnation");
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "certificate lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "certificate lane incarnation");
            this.channel = copy(channel);
            this.brokerResourceAttestationGeneration = brokerResourceAttestationGeneration;
            this.configGeneration = configGeneration;
            this.validUntilEpochMs = validUntilEpochMs;
            this.issuedAt = issuedAt;
            if (credentialBindingGeneration == 0) {
                throw new IllegalArgumentException("certificate credential generation must be non-zero");
            }
            this.credentialBindingGeneration = credentialBindingGeneration;
            this.credentialBindingDigest =
                    fixed(credentialBindingDigest, HASH_LENGTH, "certificate credential binding digest");
            this.credentialFingerprint =
                    fixed(credentialFingerprint, HASH_LENGTH, "certificate credential fingerprint");
            this.certificateDigest = fixed(certificateDigest, HASH_LENGTH, "certificate digest");
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] ownerIdentity() {
            return copy(ownerIdentity);
        }

        public byte[] storeIncarnation() {
            return copy(storeIncarnation);
        }

        public byte[] destinationLaneId() {
            return copy(destinationLaneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] channel() {
            return copy(channel);
        }

        public long brokerResourceAttestationGeneration() {
            return brokerResourceAttestationGeneration;
        }

        public long configGeneration() {
            return configGeneration;
        }

        public long validUntilEpochMs() {
            return validUntilEpochMs;
        }

        public TrustedUtcIntervalEvidence issuedAt() {
            return issuedAt;
        }

        public long credentialBindingGeneration() {
            return credentialBindingGeneration;
        }

        public byte[] credentialBindingDigest() {
            return copy(credentialBindingDigest);
        }

        public byte[] credentialFingerprint() {
            return copy(credentialFingerprint);
        }

        public byte[] certificateDigest() {
            return copy(certificateDigest);
        }
    }

    public static final class ClaimPrecondition {
        private final byte[] canonicalBytes;
        private final byte[] claimId;
        private final byte[] messageId;
        private final int generation;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final byte[] ownerIdentity;
        private final byte[] storeIncarnation;
        private final byte[] materialization;

        private ClaimPrecondition(
                final byte[] canonicalBytes,
                final byte[] claimId,
                final byte[] messageId,
                final int generation,
                final byte[] destinationLaneId,
                final byte[] laneIncarnation,
                final byte[] ownerIdentity,
                final byte[] storeIncarnation,
                final byte[] materialization) {
            this.canonicalBytes = copy(canonicalBytes);
            this.claimId = fixed(claimId, HASH_LENGTH, "precondition claim id");
            this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "precondition message id");
            this.generation = generation;
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "precondition lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "precondition lane incarnation");
            this.ownerIdentity = copy(ownerIdentity);
            this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "precondition store incarnation");
            this.materialization = copy(materialization);
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] claimId() {
            return copy(claimId);
        }

        public byte[] messageId() {
            return copy(messageId);
        }

        public int generation() {
            return generation;
        }

        public byte[] destinationLaneId() {
            return copy(destinationLaneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] ownerIdentity() {
            return copy(ownerIdentity);
        }

        public byte[] storeIncarnation() {
            return copy(storeIncarnation);
        }

        public byte[] materialization() {
            return copy(materialization);
        }

        /** Returns the validated typed Claim materialization projection. */
        public ClaimMaterialization materializationValue() {
            return ClaimMaterialization.decode(materialization);
        }
    }

    private static void emitBytes(final java.io.ByteArrayOutputStream output, final int number, final byte[] value) {
        CanonicalProtobuf.bytes(output, number, value);
    }

    private static void emitVarint(final java.io.ByteArrayOutputStream output, final int number, final long value) {
        CanonicalProtobuf.int64(output, number, value);
    }

    private static long unsignedValue(final byte[] encoded) {
        if (encoded.length == 0 || encoded.length > 8) {
            throw new IllegalArgumentException("invalid encoded uint64");
        }
        long value = 0;
        for (int index = 0; index < encoded.length; index++) {
            final int current = encoded[index] & 0xff;
            value |= (long) (current & 0x7f) << (index * 7);
        }
        if (value < 0) {
            throw new IllegalArgumentException("encoded uint64 exceeds signed runtime range");
        }
        return value;
    }
}
