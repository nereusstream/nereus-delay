package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Parser for the immutable fields of the Registry canonical Lane tuple.
 * It proves the exact Profile, Broker-resource and physical-partition
 * projections needed by typed Lane state and Claim materialization. Live
 * Profile semantics, credentials and Broker authority remain outside this
 * local value codec.
 */
public final class CanonicalLaneTuple {
    private static final int TENANT_SCOPE_LENGTH = 32;
    private static final int HASH_LENGTH = 32;
    private static final int MAX_TUPLE_BYTES = 1 << 20;
    private static final int MAX_PROFILE_ID_BYTES = 256;
    private static final int KAFKA_ADAPTER = 1;
    private static final int PULSAR_ADAPTER = 2;
    private static final int KAFKA_RESOURCE = 1;
    private static final int PULSAR_RESOURCE = 2;
    private static final int ORDERED_LANE = 1;
    private static final int UNORDERED_LANE = 2;

    private CanonicalLaneTuple() {}

    /**
     * Returns the immutable projections needed to materialize a Claim.
     *
     * <p>The returned value is parsed directly from the canonical Registry
     * Lane tuple. It does not consult live Profile, credential, or Broker
     * authorities; those remain separate admission gates.</p>
     */
    public static Projection project(final byte[] encoded) {
        return parse(encoded);
    }

    static void requireProfileProjection(
            final byte[] encoded, final ProfileRef destination, final ProfileRef capability) {
        final Projection projection = parse(encoded);
        if (!projection.destinationProfile().equals(destination)) {
            throw new IllegalArgumentException("destination Profile does not project from canonical Lane tuple");
        }
        if (!projection.capabilityProfile().equals(capability)) {
            throw new IllegalArgumentException("capability Profile does not project from canonical Lane tuple");
        }
    }

    static void requireClaimProjection(final byte[] encoded, final ClaimMaterialization materialization) {
        final Projection projection = parse(encoded);
        if (!projection.destinationProfile().equals(materialization.destinationProfile())) {
            throw new IllegalArgumentException("Claim Destination Profile does not project from canonical Lane tuple");
        }
        if (!projection.capabilityProfile().equals(materialization.capabilityProfile())) {
            throw new IllegalArgumentException("Claim Capability Profile does not project from canonical Lane tuple");
        }
        if (!projection.targetResource().equals(materialization.targetResource())) {
            throw new IllegalArgumentException("Claim target resource does not project from canonical Lane tuple");
        }
        if (projection.physicalPartition() != materialization.physicalPartition()) {
            throw new IllegalArgumentException("Claim physical partition does not project from canonical Lane tuple");
        }
    }

    private static Projection parse(final byte[] encoded) {
        final Cursor cursor = new Cursor(encoded);
        final byte[] tenantRouteScopeDigest = cursor.fixed(TENANT_SCOPE_LENGTH, "tenantRoutingScope");
        final int adapter = cursor.u8("adapterKind");
        if (adapter != KAFKA_ADAPTER && adapter != PULSAR_ADAPTER) {
            throw new IllegalArgumentException("unknown Lane tuple adapter kind");
        }
        final byte[] cluster = cursor.lp32(256, "authenticatedTargetClusterId");
        requireCanonicalText(cluster, "authenticatedTargetClusterId");
        final int resourceKind = cursor.u8("brokerResourceKind");
        if ((adapter == KAFKA_ADAPTER && resourceKind != KAFKA_RESOURCE)
                || (adapter == PULSAR_ADAPTER && resourceKind != PULSAR_RESOURCE)) {
            throw new IllegalArgumentException("Lane tuple adapter/resource kind mismatch");
        }
        final byte[] resourceIncarnation;
        final long physicalTopicCreationTimestamp;
        final byte[] nativeTopicUuid;
        if (adapter == KAFKA_ADAPTER) {
            nativeTopicUuid = cursor.fixed(16, "kafkaNativeTopicUuid");
            resourceIncarnation = null;
            physicalTopicCreationTimestamp = 0;
        } else {
            nativeTopicUuid = null;
            resourceIncarnation = cursor.fixed(32, "pulsarResourceIncarnation");
            physicalTopicCreationTimestamp = cursor.u64("pulsarPhysicalTopicCreationTimestamp");
        }
        final byte[] physicalTopic = cursor.lp32(1 << 20, "physicalTopicIdentity");
        if (adapter == KAFKA_ADAPTER) {
            Bytes.requireLength(physicalTopic, 16, "kafkaPhysicalTopicIdentity");
            if (!Arrays.equals(nativeTopicUuid, physicalTopic)) {
                throw new IllegalArgumentException("Kafka Lane tuple topic UUID projections differ");
            }
        } else {
            requireCanonicalText(physicalTopic, "pulsarPhysicalTopicIdentity");
        }
        final long physicalPartition = cursor.u32("physicalPartition");

        final byte[] destinationId = cursor.lp32(MAX_PROFILE_ID_BYTES, "destinationProfileId");
        final long destinationVersion = cursor.u64("destinationProfileVersion");
        final byte[] destinationHash = cursor.fixed(HASH_LENGTH, "destinationProfileSemanticHash");
        final byte[] capabilityId = cursor.lp32(MAX_PROFILE_ID_BYTES, "capabilityProfileId");
        final long capabilityVersion = cursor.u64("capabilityProfileVersion");
        final byte[] capabilityHash = cursor.fixed(HASH_LENGTH, "capabilityProfileSemanticHash");

        final int laneKind = cursor.u8("orderingLaneKind");
        if (laneKind == ORDERED_LANE) {
            cursor.fixed(HASH_LENGTH, "orderingDomainHash");
        } else if (laneKind == UNORDERED_LANE) {
            cursor.u32("unorderedBucket");
        } else {
            throw new IllegalArgumentException("unknown Lane tuple ordering kind");
        }
        cursor.requireEnd();
        final BrokerResourceIdentity target = adapter == KAFKA_ADAPTER
                ? BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                        new String(cluster, StandardCharsets.UTF_8), uuid(nativeTopicUuid)))
                : BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        new String(cluster, StandardCharsets.UTF_8), resourceIncarnation,
                        new String(physicalTopic, StandardCharsets.UTF_8), physicalTopicCreationTimestamp));
        return new Projection(
                tenantRouteScopeDigest,
                target,
                physicalPartition,
                new ProfileRef(destinationId, destinationVersion, destinationHash, ProfileKind.DESTINATION),
                new ProfileRef(capabilityId, capabilityVersion, capabilityHash, ProfileKind.DELIVERY_CAPABILITY));
    }

    private static UUID uuid(final byte[] value) {
        return new UUID(Bytes.readU64be(value, 0), Bytes.readU64be(value, 8));
    }

    public record Projection(
            byte[] tenantRouteScopeDigest,
            BrokerResourceIdentity targetResource,
            long physicalPartition,
            ProfileRef destinationProfile,
            ProfileRef capabilityProfile) {
        public Projection {
            Bytes.requireLength(tenantRouteScopeDigest, TENANT_SCOPE_LENGTH, "tenantRouteScopeDigest");
            tenantRouteScopeDigest = Bytes.copy(tenantRouteScopeDigest);
            Objects.requireNonNull(targetResource, "targetResource");
            Objects.requireNonNull(destinationProfile, "destinationProfile");
            Objects.requireNonNull(capabilityProfile, "capabilityProfile");
        }

        @Override
        public byte[] tenantRouteScopeDigest() {
            return Bytes.copy(tenantRouteScopeDigest);
        }
    }

    private static void requireCanonicalText(final byte[] value, final String name) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (value.length == 0
                || decoded.isBlank()
                || !Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || decoded.indexOf('\0') >= 0
                || !decoded.equals(Normalizer.normalize(decoded, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be non-empty canonical UTF-8");
        }
    }

    private static final class Cursor {
        private final byte[] value;
        private int offset;

        private Cursor(final byte[] value) {
            if (value == null || value.length == 0 || value.length > MAX_TUPLE_BYTES) {
                throw new IllegalArgumentException("canonical Lane tuple is outside the bound");
            }
            this.value = value;
        }

        private byte[] fixed(final int length, final String name) {
            if (length < 0 || offset > value.length - length) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            final byte[] result = Arrays.copyOfRange(value, offset, offset + length);
            offset += length;
            return result;
        }

        private byte[] lp32(final int maximum, final String name) {
            final long length = u32(name + " length");
            if (length > maximum || length > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(name + " exceeds its bound");
            }
            return fixed((int) length, name);
        }

        private int u8(final String name) {
            if (offset >= value.length) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            return value[offset++] & 0xff;
        }

        private long u32(final String name) {
            if (offset > value.length - 4) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            final long result = Bytes.readU32be(value, offset);
            offset += 4;
            return result;
        }

        private long u64(final String name) {
            if (offset > value.length - 8) {
                throw new IllegalArgumentException("canonical Lane tuple is truncated at " + name);
            }
            final long result = Bytes.readU64be(value, offset);
            offset += 8;
            return result;
        }

        private void requireEnd() {
            if (offset != value.length) {
                throw new IllegalArgumentException("canonical Lane tuple has trailing bytes");
            }
        }
    }
}
