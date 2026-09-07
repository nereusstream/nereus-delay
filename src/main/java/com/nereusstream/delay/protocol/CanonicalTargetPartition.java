package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * NDIP-3 physical target tuple. Local decoding proves shape and equality only;
 * authenticated Broker/resource authority is still required at registration.
 */
public record CanonicalTargetPartition(BrokerResourceIdentity resource, long physicalPartition) {
    public static final int VERSION = 1;
    public static final int MAX_CLUSTER_BYTES = 256;
    public static final int MAX_PHYSICAL_TOPIC_BYTES = 1 << 20;
    // Version byte + lp32 + Broker oneof (tag/length) + maximum Pulsar fields + partition.
    public static final int MAX_CANONICAL_BYTES = 1
            + 4
            + 1
            + 3
            + (1 + 2 + MAX_CLUSTER_BYTES)
            + (1 + 1 + 32)
            + (1 + 3 + MAX_PHYSICAL_TOPIC_BYTES)
            + (1 + 10)
            + 4;

    public CanonicalTargetPartition {
        Objects.requireNonNull(resource, "resource");
        if (physicalPartition < 0 || physicalPartition > 0xffff_ffffL) {
            throw new IllegalArgumentException("target partition is outside uint32");
        }
        if (resource.kind() == BrokerResourceIdentity.Kind.KAFKA) {
            requireTextBound(resource.kafka().authenticatedClusterId(), MAX_CLUSTER_BYTES, "target cluster");
            final var uuid = resource.kafka().nativeTopicUuid();
            if (uuid.getMostSignificantBits() == 0 && uuid.getLeastSignificantBits() == 0) {
                throw new IllegalArgumentException("target Kafka topic UUID is unassigned");
            }
        } else {
            final PulsarBrokerResourceIdentity pulsar = resource.pulsar();
            requireTextBound(pulsar.authenticatedClusterId(), MAX_CLUSTER_BYTES, "target cluster");
            requireTextBound(pulsar.physicalTopic(), MAX_PHYSICAL_TOPIC_BYTES, "target physical topic");
            if (Arrays.equals(pulsar.resourceIncarnation(), new byte[32])) {
                throw new IllegalArgumentException("target Pulsar resource token is unassigned");
            }
            if (pulsar.physicalTopicCreationTimestamp() < 0) {
                throw new IllegalArgumentException("target physical creation timestamp is negative");
            }
        }
    }

    public byte[] canonicalBytes() {
        return Bytes.concat(Bytes.u8(VERSION), Bytes.lp32(resource.canonicalBytes()), Bytes.u32be(physicalPartition));
    }

    public TargetPartitionId id() {
        return TargetPartitionId.derive(this);
    }

    public static CanonicalTargetPartition decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < 10 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("target tuple is outside the canonical byte bound");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (Byte.toUnsignedInt(input.get()) != VERSION) {
            throw new IllegalArgumentException("unsupported target tuple version");
        }
        final long resourceLength = Integer.toUnsignedLong(input.getInt());
        if (resourceLength == 0 || resourceLength != input.remaining() - Integer.BYTES) {
            throw new IllegalArgumentException("target tuple resource length is invalid");
        }
        final byte[] resourceBytes = new byte[(int) resourceLength];
        input.get(resourceBytes);
        final CanonicalTargetPartition result = new CanonicalTargetPartition(
                BrokerResourceIdentity.decode(resourceBytes), Integer.toUnsignedLong(input.getInt()));
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("target tuple is not canonical");
        }
        return result;
    }

    /** Extracts physical fields only after the complete old Lane tuple has been validated. */
    public static CanonicalTargetPartition fromLaneTuple(final byte[] canonicalLaneTuple) {
        final CanonicalLaneTuple.Projection lane = CanonicalLaneTuple.project(canonicalLaneTuple);
        return new CanonicalTargetPartition(lane.targetResource(), lane.physicalPartition());
    }

    /** Checks the exact resource/partition supplied by the caller's independently authenticated binding. */
    public void requireResourceProjection(final BrokerResourceIdentity expectedResource, final long expectedPartition) {
        if (!resource.equals(Objects.requireNonNull(expectedResource, "expectedResource"))
                || physicalPartition != expectedPartition) {
            throw new IllegalArgumentException("target tuple differs from the pinned physical resource");
        }
    }

    private static void requireTextBound(final String text, final int maximum, final String name) {
        if (text.getBytes(StandardCharsets.UTF_8).length > maximum) {
            throw new IllegalArgumentException(name + " exceeds its UTF-8 byte bound");
        }
    }
}
