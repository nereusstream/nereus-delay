package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Replay-stable materialization captured by a Claim.
 *
 * <p>This type is deliberately independent of live Broker, Profile, or Object
 * Store lookups. It carries the exact immutable projections needed to rebuild
 * one prepared publish after a crash; the source-ordered Admission still
 * validates all duplicated descriptor fields before Producer ownership.</p>
 */
public final class ClaimMaterialization {
    public static final int HASH_LENGTH = 32;
    private static final String DIGEST_DOMAIN = "nereus-delay-claim-materialization\0";

    private final ProfileRef destinationProfile;
    private final ProfileRef capabilityProfile;
    private final BrokerResourceIdentity targetResource;
    private final long physicalPartition;
    private final DelayMessageId messageId;
    private final long generation;
    private final PayloadForPublish payload;
    private final AdapterMetadata businessMetadata;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final long actionAtEpochMs;

    public ClaimMaterialization(
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final BrokerResourceIdentity targetResource,
            final long physicalPartition,
            final DelayMessageId messageId,
            final long generation,
            final PayloadForPublish payload,
            final AdapterMetadata businessMetadata,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long actionAtEpochMs) {
        this.destinationProfile = requireProfile(destinationProfile, ProfileKind.DESTINATION, "destinationProfile");
        this.capabilityProfile =
                requireProfile(capabilityProfile, ProfileKind.DELIVERY_CAPABILITY, "capabilityProfile");
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        if (physicalPartition < 0 || physicalPartition > 0xffff_ffffL) {
            throw new IllegalArgumentException("physicalPartition is outside uint32 range");
        }
        this.physicalPartition = physicalPartition;
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (generation < 0 || generation > 0xffff_ffffL) {
            throw new IllegalArgumentException("generation is outside uint32 range");
        }
        this.generation = generation;
        this.payload = Objects.requireNonNull(payload, "payload");
        this.businessMetadata = Objects.requireNonNull(businessMetadata, "businessMetadata");
        if ((targetResource.kind() == BrokerResourceIdentity.Kind.KAFKA
                        && businessMetadata.kind() != AdapterMetadata.Kind.KAFKA)
                || (targetResource.kind() == BrokerResourceIdentity.Kind.PULSAR
                        && businessMetadata.kind() != AdapterMetadata.Kind.PULSAR)) {
            throw new IllegalArgumentException("Claim materialization metadata branch does not match target resource");
        }
        if (deliverAtEpochMs < 0
                || expireAtEpochMs < deliverAtEpochMs
                || actionAtEpochMs < 0
                || actionAtEpochMs > deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid Claim materialization timing");
        }
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.actionAtEpochMs = actionAtEpochMs;
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

    public DelayMessageId messageId() {
        return messageId;
    }

    public long generation() {
        return generation;
    }

    public PayloadForPublish payload() {
        return payload;
    }

    public AdapterMetadata businessMetadata() {
        return businessMetadata;
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

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, targetResource.canonicalBytes());
            CanonicalProtobuf.uint32(output, 4, physicalPartition);
            CanonicalProtobuf.bytes(output, 5, messageId.bytes());
            CanonicalProtobuf.uint32(output, 6, generation);
            CanonicalProtobuf.bytes(output, 7, payload.canonicalBytes());
            CanonicalProtobuf.bytes(output, 8, businessMetadata.canonicalBytes());
            CanonicalProtobuf.int64(output, 9, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 10, expireAtEpochMs);
            CanonicalProtobuf.int64(output, 11, actionAtEpochMs);
        });
    }

    /** Returns the digest carried in ClaimPrecondition field 11. */
    public byte[] materializationDigest() {
        return Bytes.sha256(Bytes.utf8(DIGEST_DOMAIN), canonicalBytes());
    }

    /** Decodes and validates canonical ClaimMaterialization bytes. */
    public static ClaimMaterialization decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ClaimMaterialization");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "ClaimMaterialization");
        final ClaimMaterialization result = new ClaimMaterialization(
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                BrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                uint32(fields.get(3), 4),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(4), 5, DelayMessageId.LENGTH)),
                uint32(fields.get(5), 6),
                PayloadForPublish.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                AdapterMetadata.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                nonNegativeInt64(fields.get(8), 9),
                nonNegativeInt64(fields.get(9), 10),
                nonNegativeInt64(fields.get(10), 11));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ClaimMaterialization");
        return result;
    }

    private static long uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = QueryCodecSupport.uint(field, number);
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("ClaimMaterialization uint32 field is outside range: " + number);
        }
        return value;
    }

    private static long nonNegativeInt64(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = QueryCodecSupport.uint(field, number);
        if (value < 0) {
            throw new IllegalArgumentException("ClaimMaterialization int64 field is negative: " + number);
        }
        return value;
    }

    private static ProfileRef requireProfile(final ProfileRef profile, final ProfileKind expected, final String name) {
        final ProfileRef result = Objects.requireNonNull(profile, name);
        if (result.profileKind() != expected) {
            throw new IllegalArgumentException(name + " must have " + expected + " kind");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ClaimMaterialization that
                && physicalPartition == that.physicalPartition
                && generation == that.generation
                && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs
                && actionAtEpochMs == that.actionAtEpochMs
                && destinationProfile.equals(that.destinationProfile)
                && capabilityProfile.equals(that.capabilityProfile)
                && targetResource.equals(that.targetResource)
                && messageId.equals(that.messageId)
                && payload.equals(that.payload)
                && businessMetadata.equals(that.businessMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
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
                Arrays.hashCode(materializationDigest()));
    }
}
