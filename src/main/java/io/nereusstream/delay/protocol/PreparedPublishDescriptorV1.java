package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Typed canonical projection of Registry PreparedPublishDescriptorV1.
 *
 * <p>The prepared hash is derived from the exact descriptor bytes and is not
 * part of this value.  It is appended by the Broker record/admission envelope
 * after the descriptor has been frozen.</p>
 */
public final class PreparedPublishDescriptorV1 {
    public static final int HASH_LENGTH = 32;
    private static final int LANE_INCARNATION_LENGTH = 16;
    private static final String HASH_DOMAIN = "nereus-delay-prepared-publish-v1\0";

    private final AdapterKindV1 adapterKind;
    private final DestinationLaneId destinationLaneId;
    private final byte[] laneIncarnation;
    private final ProfileRefV1 destinationProfile;
    private final ProfileRefV1 capabilityProfile;
    private final BrokerResourceIdentityV1 targetResource;
    private final long physicalPartition;
    private final ChannelResourceIdentityV1 channel;
    private final DelayMessageId messageId;
    private final long generation;
    private final byte[] publishAttemptId;
    private final long attemptNo;
    private final PayloadForPublishV1 payload;
    private final AdapterMetadataV1 businessMetadata;
    private final ReservedPublishMetadataV1 reservedMetadata;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final long actionAtEpochMs;

    public PreparedPublishDescriptorV1(final AdapterKindV1 adapterKind,
                                       final DestinationLaneId destinationLaneId,
                                       final byte[] laneIncarnation,
                                       final ProfileRefV1 destinationProfile,
                                       final ProfileRefV1 capabilityProfile,
                                       final BrokerResourceIdentityV1 targetResource,
                                       final long physicalPartition,
                                       final ChannelResourceIdentityV1 channel,
                                       final DelayMessageId messageId,
                                       final long generation,
                                       final byte[] publishAttemptId,
                                       final long attemptNo,
                                       final PayloadForPublishV1 payload,
                                       final AdapterMetadataV1 businessMetadata,
                                       final ReservedPublishMetadataV1 reservedMetadata,
                                       final long deliverAtEpochMs,
                                       final long expireAtEpochMs,
                                       final long actionAtEpochMs) {
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        this.destinationLaneId = Objects.requireNonNull(destinationLaneId, "destinationLaneId");
        Bytes.requireLength(laneIncarnation, LANE_INCARNATION_LENGTH, "laneIncarnation");
        this.laneIncarnation = Bytes.copy(laneIncarnation);
        this.destinationProfile = requireProfile(destinationProfile, ProfileKindV1.DESTINATION,
                "destinationProfile");
        this.capabilityProfile = requireProfile(capabilityProfile, ProfileKindV1.DELIVERY_CAPABILITY,
                "capabilityProfile");
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
        if (deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs
                || actionAtEpochMs < 0 || actionAtEpochMs > deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Prepared Publish timing");
        }
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.actionAtEpochMs = actionAtEpochMs;
        validateCrossObjectIdentity();
    }

    public AdapterKindV1 adapterKind() {
        return adapterKind;
    }

    public int descriptorVersion() {
        return 1;
    }

    public int adapterEncodingVersion() {
        return 1;
    }

    public DestinationLaneId destinationLaneId() {
        return destinationLaneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public ProfileRefV1 destinationProfile() {
        return destinationProfile;
    }

    public ProfileRefV1 capabilityProfile() {
        return capabilityProfile;
    }

    public BrokerResourceIdentityV1 targetResource() {
        return targetResource;
    }

    public long physicalPartition() {
        return physicalPartition;
    }

    public ChannelResourceIdentityV1 channel() {
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

    public PayloadForPublishV1 payload() {
        return payload;
    }

    public AdapterMetadataV1 businessMetadata() {
        return businessMetadata;
    }

    public ReservedPublishMetadataV1 reservedMetadata() {
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

    /** Returns the replay-stable materialization projection carried by this descriptor. */
    public ClaimMaterializationV1 materialization() {
        return new ClaimMaterializationV1(destinationProfile, capabilityProfile, targetResource,
                physicalPartition, messageId, generation, payload, businessMetadata,
                deliverAtEpochMs, expireAtEpochMs, actionAtEpochMs);
    }

    public byte[] canonicalBytes() {
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

    public static PreparedPublishDescriptorV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PreparedPublishDescriptorV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, "PreparedPublishDescriptorV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != 1
                || QueryCodecSupport.uint(fields.get(2), 3) != 1) {
            throw new IllegalArgumentException("unsupported PreparedPublishDescriptorV1 version");
        }
        final PreparedPublishDescriptorV1 result = new PreparedPublishDescriptorV1(
                AdapterKindV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                new DestinationLaneId(QueryCodecSupport.fixed(fields.get(3), 4, DestinationLaneId.LENGTH)),
                QueryCodecSupport.fixed(fields.get(4), 5, LANE_INCARNATION_LENGTH),
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                uint32(QueryCodecSupport.uint(fields.get(8), 9), "physicalPartition"),
                ChannelResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(9), 10)),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(10), 11, DelayMessageId.LENGTH)),
                uint32(QueryCodecSupport.uint(fields.get(11), 12), "generation"),
                QueryCodecSupport.fixed(fields.get(12), 13, HASH_LENGTH),
                uint32(QueryCodecSupport.uint(fields.get(13), 14), "attemptNo"),
                PayloadForPublishV1.decode(QueryCodecSupport.nested(fields.get(14), 15)),
                AdapterMetadataV1.decode(QueryCodecSupport.nested(fields.get(15), 16)),
                ReservedPublishMetadataV1.decode(QueryCodecSupport.nested(fields.get(16), 17)),
                nonNegative(QueryCodecSupport.uint(fields.get(17), 18), "deliverAtEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(18), 19), "expireAtEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(19), 20), "actionAtEpochMs"));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedPublishDescriptorV1");
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

    private static AdapterMetadataV1.Kind adapterMetadataKind(final AdapterKindV1 adapterKind) {
        return adapterKind == AdapterKindV1.KAFKA ? AdapterMetadataV1.Kind.KAFKA : AdapterMetadataV1.Kind.PULSAR;
    }

    private static ProfileRefV1 requireProfile(final ProfileRefV1 profile, final ProfileKindV1 expected,
                                               final String name) {
        final ProfileRefV1 result = Objects.requireNonNull(profile, name);
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

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PreparedPublishDescriptorV1 that)) {
            return false;
        }
        return adapterKind == that.adapterKind && destinationLaneId.equals(that.destinationLaneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && destinationProfile.equals(that.destinationProfile)
                && capabilityProfile.equals(that.capabilityProfile)
                && targetResource.equals(that.targetResource) && physicalPartition == that.physicalPartition
                && channel.equals(that.channel) && messageId.equals(that.messageId)
                && generation == that.generation && Arrays.equals(publishAttemptId, that.publishAttemptId)
                && attemptNo == that.attemptNo && payload.equals(that.payload)
                && businessMetadata.equals(that.businessMetadata) && reservedMetadata.equals(that.reservedMetadata)
                && deliverAtEpochMs == that.deliverAtEpochMs && expireAtEpochMs == that.expireAtEpochMs
                && actionAtEpochMs == that.actionAtEpochMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(adapterKind, destinationLaneId, Arrays.hashCode(laneIncarnation), destinationProfile,
                capabilityProfile, targetResource, physicalPartition, channel, messageId, generation,
                Arrays.hashCode(publishAttemptId), attemptNo, payload, businessMetadata, reservedMetadata,
                deliverAtEpochMs, expireAtEpochMs, actionAtEpochMs);
    }
}
