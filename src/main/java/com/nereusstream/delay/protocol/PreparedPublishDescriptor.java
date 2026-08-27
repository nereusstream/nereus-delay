package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Typed canonical projection of Registry PreparedPublishDescriptor.
 *
 * <p>The prepared hash is derived from the exact descriptor bytes and is not
 * part of this value. It is appended by the Broker record/admission envelope
 * after the descriptor has been frozen.</p>
 */
public final class PreparedPublishDescriptor {
    public static final int HASH_LENGTH = 32;
    private static final int LANE_INCARNATION_LENGTH = 16;
    private static final String HASH_DOMAIN = "nereus-delay-prepared-publish\0";

    private final AdapterKind adapterKind;
    private final DestinationLaneId destinationLaneId;
    private final byte[] laneIncarnation;
    private final ProfileRef destinationProfile;
    private final ProfileRef capabilityProfile;
    private final BrokerResourceIdentity targetResource;
    private final long physicalPartition;
    private final ChannelResourceIdentity channel;
    private final DelayMessageId messageId;
    private final long generation;
    private final byte[] publishAttemptId;
    private final long attemptNo;
    private final PayloadForPublish payload;
    private final AdapterMetadata businessMetadata;
    private final ReservedPublishMetadata reservedMetadata;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final long actionAtEpochMs;
    private final NativeDeliveryPolicy nativeDeliveryPolicy;
    private final DeliveryContract deliveryContract;
    private final HandoffPolicySnapshot handoffPolicySnapshot;
    private final Long eventTimeEpochMs;
    private final PulsarRecordTemplate pulsarRecordTemplate;
    private final byte[] recordTemplateHash;
    private final byte[] artifactGenerationSetDigest;
    private final boolean legacyEncoding;

    public PreparedPublishDescriptor(
            final AdapterKind adapterKind,
            final DestinationLaneId destinationLaneId,
            final byte[] laneIncarnation,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final BrokerResourceIdentity targetResource,
            final long physicalPartition,
            final ChannelResourceIdentity channel,
            final DelayMessageId messageId,
            final long generation,
            final byte[] publishAttemptId,
            final long attemptNo,
            final PayloadForPublish payload,
            final AdapterMetadata businessMetadata,
            final ReservedPublishMetadata reservedMetadata,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long actionAtEpochMs) {
        this(
                adapterKind,
                destinationLaneId,
                laneIncarnation,
                destinationProfile,
                capabilityProfile,
                targetResource,
                physicalPartition,
                channel,
                messageId,
                generation,
                publishAttemptId,
                attemptNo,
                payload,
                businessMetadata,
                reservedMetadata,
                deliverAtEpochMs,
                expireAtEpochMs,
                actionAtEpochMs,
                NativeDeliveryPolicy.FORBID,
                DeliveryContract.NEREUS_MANAGED_NOT_BEFORE,
                null,
                null,
                null,
                null,
                legacyArtifactGenerationSetDigest(),
                true);
    }

    public PreparedPublishDescriptor(
            final AdapterKind adapterKind,
            final DestinationLaneId destinationLaneId,
            final byte[] laneIncarnation,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final BrokerResourceIdentity targetResource,
            final long physicalPartition,
            final ChannelResourceIdentity channel,
            final DelayMessageId messageId,
            final long generation,
            final byte[] publishAttemptId,
            final long attemptNo,
            final PayloadForPublish payload,
            final AdapterMetadata businessMetadata,
            final ReservedPublishMetadata reservedMetadata,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long actionAtEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final DeliveryContract deliveryContract,
            final HandoffPolicySnapshot handoffPolicySnapshot,
            final Long eventTimeEpochMs,
            final PulsarRecordTemplate pulsarRecordTemplate,
            final byte[] recordTemplateHash,
            final byte[] artifactGenerationSetDigest) {
        this(
                adapterKind,
                destinationLaneId,
                laneIncarnation,
                destinationProfile,
                capabilityProfile,
                targetResource,
                physicalPartition,
                channel,
                messageId,
                generation,
                publishAttemptId,
                attemptNo,
                payload,
                businessMetadata,
                reservedMetadata,
                deliverAtEpochMs,
                expireAtEpochMs,
                actionAtEpochMs,
                nativeDeliveryPolicy,
                deliveryContract,
                handoffPolicySnapshot,
                eventTimeEpochMs,
                pulsarRecordTemplate,
                recordTemplateHash,
                artifactGenerationSetDigest,
                false);
    }

    private PreparedPublishDescriptor(
            final AdapterKind adapterKind,
            final DestinationLaneId destinationLaneId,
            final byte[] laneIncarnation,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final BrokerResourceIdentity targetResource,
            final long physicalPartition,
            final ChannelResourceIdentity channel,
            final DelayMessageId messageId,
            final long generation,
            final byte[] publishAttemptId,
            final long attemptNo,
            final PayloadForPublish payload,
            final AdapterMetadata businessMetadata,
            final ReservedPublishMetadata reservedMetadata,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long actionAtEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final DeliveryContract deliveryContract,
            final HandoffPolicySnapshot handoffPolicySnapshot,
            final Long eventTimeEpochMs,
            final PulsarRecordTemplate pulsarRecordTemplate,
            final byte[] recordTemplateHash,
            final byte[] artifactGenerationSetDigest,
            final boolean legacyEncoding) {
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        this.destinationLaneId = Objects.requireNonNull(destinationLaneId, "destinationLaneId");
        Bytes.requireLength(laneIncarnation, LANE_INCARNATION_LENGTH, "laneIncarnation");
        this.laneIncarnation = Bytes.copy(laneIncarnation);
        this.destinationProfile = requireProfile(destinationProfile, ProfileKind.DESTINATION, "destinationProfile");
        this.capabilityProfile =
                requireProfile(capabilityProfile, ProfileKind.DELIVERY_CAPABILITY, "capabilityProfile");
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        this.physicalPartition = uint32(physicalPartition, "physicalPartition");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.generation = uint32(generation, "generation");
        this.publishAttemptId = fixed(publishAttemptId, "publishAttemptId");
        this.attemptNo = uint32(attemptNo, "attemptNo");
        if (this.attemptNo == 0) {
            throw new IllegalArgumentException("attemptNo must be positive");
        }
        this.payload = Objects.requireNonNull(payload, "payload");
        this.businessMetadata = Objects.requireNonNull(businessMetadata, "businessMetadata");
        this.reservedMetadata = Objects.requireNonNull(reservedMetadata, "reservedMetadata");
        if (businessMetadata.kind() != adapterMetadataKind(adapterKind)) {
            throw new IllegalArgumentException("Prepared Publish metadata branch does not match adapter");
        }
        if (deliverAtEpochMs < 0
                || expireAtEpochMs < deliverAtEpochMs
                || actionAtEpochMs < 0
                || actionAtEpochMs > deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Prepared Publish timing");
        }
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.actionAtEpochMs = actionAtEpochMs;
        this.nativeDeliveryPolicy = Objects.requireNonNull(nativeDeliveryPolicy, "nativeDeliveryPolicy");
        this.deliveryContract = Objects.requireNonNull(deliveryContract, "deliveryContract");
        this.handoffPolicySnapshot = handoffPolicySnapshot;
        if (eventTimeEpochMs != null && eventTimeEpochMs < 0) {
            throw new IllegalArgumentException("eventTime must be non-negative");
        }
        this.eventTimeEpochMs = eventTimeEpochMs;
        this.pulsarRecordTemplate = pulsarRecordTemplate;
        if (recordTemplateHash == null) {
            if (pulsarRecordTemplate != null) {
                throw new IllegalArgumentException("Pulsar record template hash is missing");
            }
            this.recordTemplateHash = null;
        } else {
            this.recordTemplateHash = fixed(recordTemplateHash, "recordTemplateHash");
            if (pulsarRecordTemplate == null
                    || !Arrays.equals(this.recordTemplateHash, pulsarRecordTemplate.recordTemplateHash())) {
                throw new IllegalArgumentException("Pulsar record template hash mismatch");
            }
        }
        this.artifactGenerationSetDigest = fixed(artifactGenerationSetDigest, "artifactGenerationSetDigest");
        this.legacyEncoding = legacyEncoding;
        if (pulsarRecordTemplate != null
                && !Arrays.equals(
                        this.artifactGenerationSetDigest, pulsarRecordTemplate.artifactGenerationSetDigest())) {
            throw new IllegalArgumentException("descriptor artifact generation digest mismatch");
        }
        if (deliveryContract.isNative() && adapterKind != AdapterKind.PULSAR) {
            throw new IllegalArgumentException("native delivery contract requires Pulsar");
        }
        if (deliveryContract == DeliveryContract.NEREUS_MANAGED_NOT_BEFORE) {
            if (!legacyEncoding && (actionAtEpochMs != deliverAtEpochMs || handoffPolicySnapshot != null)) {
                throw new IllegalArgumentException(
                        "ordinary managed contract requires actionAt=deliverAt and no snapshot");
            }
        } else {
            if (nativeDeliveryPolicy == NativeDeliveryPolicy.FORBID
                    || handoffPolicySnapshot == null
                    || handoffPolicySnapshot.mode() != HandoffPolicyMode.ENABLED
                    || !handoffPolicySnapshot.allows(HandoffPath.MANAGED_HANDOFF)) {
                throw new IllegalArgumentException("native contract requires an enabled managed-handoff snapshot");
            }
            final long expectedActionAt;
            try {
                expectedActionAt = Math.subtractExact(deliverAtEpochMs, handoffPolicySnapshot.effectiveLeadMs());
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("native actionAt arithmetic overflow", overflow);
            }
            if (expectedActionAt < 0 || actionAtEpochMs != expectedActionAt) {
                throw new IllegalArgumentException("native actionAt does not match the signed handoff lead");
            }
        }
        if (!legacyEncoding && adapterKind == AdapterKind.PULSAR) {
            if (pulsarRecordTemplate == null || recordTemplateHash == null) {
                throw new IllegalArgumentException("Pulsar descriptor requires an exact record template");
            }
            if (!pulsarRecordTemplate.targetResource().equals(targetResource)
                    || pulsarRecordTemplate.physicalPartition() != physicalPartition
                    || !pulsarRecordTemplate.reservedMetadata().equals(reservedMetadata)
                    || !pulsarRecordTemplate.payload().equals(payload)
                    || pulsarRecordTemplate.deliveryContract() != deliveryContract
                    || !Objects.equals(pulsarRecordTemplate.eventTimeEpochMs(), eventTimeEpochMs)) {
                throw new IllegalArgumentException("Pulsar record template does not match descriptor");
            }
        } else if (!legacyEncoding && (pulsarRecordTemplate != null || recordTemplateHash != null)) {
            throw new IllegalArgumentException("Kafka descriptor cannot carry a Pulsar record template");
        }
        if (handoffPolicySnapshot != null
                && !Arrays.equals(handoffPolicySnapshot.artifactGenerationSetDigest(), artifactGenerationSetDigest)) {
            throw new IllegalArgumentException("handoff snapshot artifact generation digest mismatch");
        }
        validateCrossObjectIdentity();
    }

    public AdapterKind adapterKind() {
        return adapterKind;
    }

    public int descriptorVersion() {
        return legacyEncoding ? 1 : 2;
    }

    public int adapterEncodingVersion() {
        return legacyEncoding ? 1 : 2;
    }

    boolean legacyEncoding() {
        return legacyEncoding;
    }

    public DestinationLaneId destinationLaneId() {
        return destinationLaneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public ProfileRef destinationProfile() {
        return destinationProfile;
    }

    public ProfileRef capabilityProfile() {
        return capabilityProfile;
    }

    public BrokerResourceIdentity targetResource() {
        return targetResource;
    }

    public long physicalPartition() {
        return physicalPartition;
    }

    public ChannelResourceIdentity channel() {
        return channel;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public long generation() {
        return generation;
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public long attemptNo() {
        return attemptNo;
    }

    public PayloadForPublish payload() {
        return payload;
    }

    public AdapterMetadata businessMetadata() {
        return businessMetadata;
    }

    public ReservedPublishMetadata reservedMetadata() {
        return reservedMetadata;
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

    public NativeDeliveryPolicy nativeDeliveryPolicy() {
        return nativeDeliveryPolicy;
    }

    public DeliveryContract deliveryContract() {
        return deliveryContract;
    }

    public HandoffPolicySnapshot handoffPolicySnapshot() {
        return handoffPolicySnapshot;
    }

    public Long eventTimeEpochMs() {
        return eventTimeEpochMs;
    }

    public PulsarRecordTemplate pulsarRecordTemplate() {
        return pulsarRecordTemplate;
    }

    public byte[] recordTemplateHash() {
        return recordTemplateHash == null ? null : Bytes.copy(recordTemplateHash);
    }

    public byte[] artifactGenerationSetDigest() {
        return Bytes.copy(artifactGenerationSetDigest);
    }

    /** Returns the replay-stable materialization projection carried by this descriptor. */
    public ClaimMaterialization materialization() {
        if (legacyEncoding) {
            return new ClaimMaterialization(
                    destinationProfile,
                    capabilityProfile,
                    targetResource,
                    physicalPartition,
                    messageId,
                    generation,
                    payload,
                    businessMetadata,
                    deliverAtEpochMs,
                    expireAtEpochMs,
                    actionAtEpochMs);
        }
        return new ClaimMaterialization(
                destinationProfile,
                capabilityProfile,
                targetResource,
                physicalPartition,
                messageId,
                generation,
                payload,
                businessMetadata,
                deliverAtEpochMs,
                expireAtEpochMs,
                actionAtEpochMs,
                nativeDeliveryPolicy,
                eventTimeEpochMs,
                handoffPolicySnapshot == null ? null : handoffPolicySnapshot.headRef());
    }

    public byte[] canonicalBytes() {
        if (legacyEncoding) {
            return legacyCanonicalBytes();
        }
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 2);
            CanonicalProtobuf.uint32(output, 2, adapterKind.wireValue());
            CanonicalProtobuf.uint32(output, 3, 2);
            CanonicalProtobuf.bytes(output, 4, destinationLaneId.bytes());
            CanonicalProtobuf.bytes(output, 5, laneIncarnation);
            CanonicalProtobuf.bytes(output, 6, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 8, targetResource.canonicalBytes());
            CanonicalProtobuf.uint32(output, 9, physicalPartition);
            CanonicalProtobuf.bytes(output, 10, channel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, generation);
            CanonicalProtobuf.bytes(output, 13, publishAttemptId);
            CanonicalProtobuf.uint32(output, 14, attemptNo);
            CanonicalProtobuf.bytes(output, 15, payload.canonicalBytes());
            CanonicalProtobuf.bytes(output, 16, businessMetadata.canonicalBytes());
            CanonicalProtobuf.bytes(output, 17, reservedMetadata.canonicalBytes());
            CanonicalProtobuf.int64(output, 18, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 19, expireAtEpochMs);
            CanonicalProtobuf.int64(output, 20, actionAtEpochMs);
            CanonicalProtobuf.uint32(output, 21, nativeDeliveryPolicy.wireValue());
            CanonicalProtobuf.uint32(output, 22, deliveryContract.wireValue());
            if (handoffPolicySnapshot != null) {
                CanonicalProtobuf.bytes(output, 23, handoffPolicySnapshot.canonicalBytes());
            }
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.int64(output, 24, eventTimeEpochMs);
            }
            if (pulsarRecordTemplate != null) {
                CanonicalProtobuf.bytes(output, 25, pulsarRecordTemplate.canonicalBytes());
                CanonicalProtobuf.bytes(output, 26, recordTemplateHash);
            }
            CanonicalProtobuf.bytes(output, 27, artifactGenerationSetDigest);
        });
    }

    private byte[] legacyCanonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, adapterKind.wireValue());
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.bytes(output, 4, destinationLaneId.bytes());
            CanonicalProtobuf.bytes(output, 5, laneIncarnation);
            CanonicalProtobuf.bytes(output, 6, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 8, targetResource.canonicalBytes());
            CanonicalProtobuf.uint32(output, 9, physicalPartition);
            CanonicalProtobuf.bytes(output, 10, channel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, generation);
            CanonicalProtobuf.bytes(output, 13, publishAttemptId);
            CanonicalProtobuf.uint32(output, 14, attemptNo);
            CanonicalProtobuf.bytes(output, 15, payload.canonicalBytes());
            CanonicalProtobuf.bytes(output, 16, businessMetadata.canonicalBytes());
            CanonicalProtobuf.bytes(output, 17, reservedMetadata.canonicalBytes());
            CanonicalProtobuf.int64(output, 18, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 19, expireAtEpochMs);
            CanonicalProtobuf.int64(output, 20, actionAtEpochMs);
        });
    }

    public byte[] preparedPublishHash() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), canonicalBytes());
    }

    public static PreparedPublishDescriptor decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PreparedPublishDescriptor");
        if (fields.size() == 20) {
            QueryCodecSupport.requireNumbers(
                    fields,
                    new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
                    "PreparedPublishDescriptor");
            if (QueryCodecSupport.uint(fields.get(0), 1) != 1 || QueryCodecSupport.uint(fields.get(2), 3) != 1) {
                throw new IllegalArgumentException("unsupported PreparedPublishDescriptor version");
            }
            final PreparedPublishDescriptor result = new PreparedPublishDescriptor(
                    AdapterKind.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                    new DestinationLaneId(QueryCodecSupport.fixed(fields.get(3), 4, DestinationLaneId.LENGTH)),
                    QueryCodecSupport.fixed(fields.get(4), 5, LANE_INCARNATION_LENGTH),
                    ProfileRef.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                    ProfileRef.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                    BrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                    uint32(QueryCodecSupport.uint(fields.get(8), 9), "physicalPartition"),
                    ChannelResourceIdentity.decode(QueryCodecSupport.nested(fields.get(9), 10)),
                    new DelayMessageId(QueryCodecSupport.fixed(fields.get(10), 11, DelayMessageId.LENGTH)),
                    uint32(QueryCodecSupport.uint(fields.get(11), 12), "generation"),
                    QueryCodecSupport.fixed(fields.get(12), 13, HASH_LENGTH),
                    uint32(QueryCodecSupport.uint(fields.get(13), 14), "attemptNo"),
                    PayloadForPublish.decode(QueryCodecSupport.nested(fields.get(14), 15)),
                    AdapterMetadata.decode(QueryCodecSupport.nested(fields.get(15), 16)),
                    ReservedPublishMetadata.decode(QueryCodecSupport.nested(fields.get(16), 17)),
                    nonNegative(QueryCodecSupport.uint(fields.get(17), 18), "deliverAtEpochMs"),
                    nonNegative(QueryCodecSupport.uint(fields.get(18), 19), "expireAtEpochMs"),
                    nonNegative(QueryCodecSupport.uint(fields.get(19), 20), "actionAtEpochMs"));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedPublishDescriptor");
            return result;
        }
        if (fields.size() < 23 || fields.size() > 27) {
            throw new IllegalArgumentException("PreparedPublishDescriptor has an unexpected field count");
        }
        for (int index = 0; index < 20; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("PreparedPublishDescriptor field order mismatch");
            }
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != 2 || QueryCodecSupport.uint(fields.get(2), 3) != 2) {
            throw new IllegalArgumentException("unsupported PreparedPublishDescriptor version");
        }
        int index = 20;
        final NativeDeliveryPolicy policy =
                NativeDeliveryPolicy.fromWire(QueryCodecSupport.uint(fields.get(index++), 21));
        final DeliveryContract contract = DeliveryContract.fromWire(QueryCodecSupport.uint(fields.get(index++), 22));
        HandoffPolicySnapshot snapshot = null;
        if (index < fields.size() && fields.get(index).number() == 23) {
            snapshot = HandoffPolicySnapshot.decode(QueryCodecSupport.nested(fields.get(index++), 23));
        }
        Long eventTime = null;
        if (index < fields.size() && fields.get(index).number() == 24) {
            eventTime = QueryCodecSupport.uint(fields.get(index++), 24);
        }
        PulsarRecordTemplate template = null;
        byte[] templateHash = null;
        if (index < fields.size() && fields.get(index).number() == 25) {
            template = PulsarRecordTemplate.decode(QueryCodecSupport.nested(fields.get(index++), 25));
            if (index >= fields.size() || fields.get(index).number() != 26) {
                throw new IllegalArgumentException("PreparedPublishDescriptor record template hash is missing");
            }
            templateHash = QueryCodecSupport.fixed(fields.get(index++), 26, HASH_LENGTH);
        } else if (index < fields.size() && fields.get(index).number() == 26) {
            throw new IllegalArgumentException("PreparedPublishDescriptor record template is missing");
        }
        if (index >= fields.size() || fields.get(index).number() != 27) {
            throw new IllegalArgumentException("PreparedPublishDescriptor artifact generation set is missing");
        }
        final byte[] artifactDigest = QueryCodecSupport.fixed(fields.get(index++), 27, HASH_LENGTH);
        if (index != fields.size()) {
            throw new IllegalArgumentException("PreparedPublishDescriptor field order mismatch");
        }
        final PreparedPublishDescriptor result = new PreparedPublishDescriptor(
                AdapterKind.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                new DestinationLaneId(QueryCodecSupport.fixed(fields.get(3), 4, DestinationLaneId.LENGTH)),
                QueryCodecSupport.fixed(fields.get(4), 5, LANE_INCARNATION_LENGTH),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                BrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                uint32(QueryCodecSupport.uint(fields.get(8), 9), "physicalPartition"),
                ChannelResourceIdentity.decode(QueryCodecSupport.nested(fields.get(9), 10)),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(10), 11, DelayMessageId.LENGTH)),
                uint32(QueryCodecSupport.uint(fields.get(11), 12), "generation"),
                QueryCodecSupport.fixed(fields.get(12), 13, HASH_LENGTH),
                uint32(QueryCodecSupport.uint(fields.get(13), 14), "attemptNo"),
                PayloadForPublish.decode(QueryCodecSupport.nested(fields.get(14), 15)),
                AdapterMetadata.decode(QueryCodecSupport.nested(fields.get(15), 16)),
                ReservedPublishMetadata.decode(QueryCodecSupport.nested(fields.get(16), 17)),
                nonNegative(QueryCodecSupport.uint(fields.get(17), 18), "deliverAtEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(18), 19), "expireAtEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(19), 20), "actionAtEpochMs"),
                policy,
                contract,
                snapshot,
                eventTime,
                template,
                templateHash,
                artifactDigest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedPublishDescriptor");
        return result;
    }

    private void validateCrossObjectIdentity() {
        if (channel.adapterKind() != adapterKind
                || channel.physicalPartition() != physicalPartition
                || !channel.targetResource().equals(targetResource)
                || !Arrays.equals(channel.destinationLaneId(), destinationLaneId.bytes())
                || !Arrays.equals(channel.laneIncarnation(), laneIncarnation)
                || !channel.credentialUseLease().profile().equals(destinationProfile)) {
            throw new IllegalArgumentException("Prepared Publish channel identity mismatch");
        }
        if (!reservedMetadata.messageId().equals(messageId)
                || reservedMetadata.generation() != generation
                || !Arrays.equals(reservedMetadata.publishAttemptId(), publishAttemptId)
                || !Arrays.equals(reservedMetadata.destinationProfileSemanticHash(), destinationProfile.semanticHash())
                || !Arrays.equals(reservedMetadata.capabilityProfileSemanticHash(), capabilityProfile.semanticHash())
                || reservedMetadata.deliverAtEpochMs() != deliverAtEpochMs
                || reservedMetadata.deliveryMode() != DeliveryMode.MANAGED) {
            throw new IllegalArgumentException("Prepared Publish reserved metadata identity mismatch");
        }
        final ShardId messageShard = messageId.routingId().shardId();
        if (!reservedMetadata.routeIncarnation().equals(messageShard.routeIncarnation())
                || reservedMetadata.shardPartition() != messageShard.unsignedPartition()) {
            throw new IllegalArgumentException("Prepared Publish reserved shard identity mismatch");
        }
    }

    private static AdapterMetadata.Kind adapterMetadataKind(final AdapterKind adapterKind) {
        return adapterKind == AdapterKind.KAFKA ? AdapterMetadata.Kind.KAFKA : AdapterMetadata.Kind.PULSAR;
    }

    private static ProfileRef requireProfile(final ProfileRef profile, final ProfileKind expected, final String name) {
        final ProfileRef result = Objects.requireNonNull(profile, name);
        if (result.profileKind() != expected) {
            throw new IllegalArgumentException(name + " must have " + expected + " kind");
        }
        return result;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static long uint32(final long value, final String name) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " is outside uint32 range");
        }
        return value;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    public static byte[] legacyArtifactGenerationSetDigest() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-legacy-artifact-generation-set\0"));
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PreparedPublishDescriptor that)) {
            return false;
        }
        return adapterKind == that.adapterKind
                && destinationLaneId.equals(that.destinationLaneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && destinationProfile.equals(that.destinationProfile)
                && capabilityProfile.equals(that.capabilityProfile)
                && targetResource.equals(that.targetResource)
                && physicalPartition == that.physicalPartition
                && channel.equals(that.channel)
                && messageId.equals(that.messageId)
                && generation == that.generation
                && Arrays.equals(publishAttemptId, that.publishAttemptId)
                && attemptNo == that.attemptNo
                && payload.equals(that.payload)
                && businessMetadata.equals(that.businessMetadata)
                && reservedMetadata.equals(that.reservedMetadata)
                && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs
                && actionAtEpochMs == that.actionAtEpochMs
                && nativeDeliveryPolicy == that.nativeDeliveryPolicy
                && deliveryContract == that.deliveryContract
                && Objects.equals(handoffPolicySnapshot, that.handoffPolicySnapshot)
                && Objects.equals(eventTimeEpochMs, that.eventTimeEpochMs)
                && Objects.equals(pulsarRecordTemplate, that.pulsarRecordTemplate)
                && Arrays.equals(recordTemplateHash, that.recordTemplateHash)
                && Arrays.equals(artifactGenerationSetDigest, that.artifactGenerationSetDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                adapterKind,
                destinationLaneId,
                Arrays.hashCode(laneIncarnation),
                destinationProfile,
                capabilityProfile,
                targetResource,
                physicalPartition,
                channel,
                messageId,
                generation,
                Arrays.hashCode(publishAttemptId),
                attemptNo,
                payload,
                businessMetadata,
                reservedMetadata,
                deliverAtEpochMs,
                expireAtEpochMs,
                actionAtEpochMs,
                nativeDeliveryPolicy,
                deliveryContract,
                handoffPolicySnapshot,
                eventTimeEpochMs,
                pulsarRecordTemplate,
                Arrays.hashCode(recordTemplateHash),
                Arrays.hashCode(artifactGenerationSetDigest));
    }
}
