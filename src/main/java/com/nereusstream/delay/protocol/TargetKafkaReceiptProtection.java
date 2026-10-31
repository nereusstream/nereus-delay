package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable CONTROL artifact for one exact K2 receipt slot under an independently protected shared topic. */
public record TargetKafkaReceiptProtection(
        KafkaReceiptSlotResource resource,
        String physicalTopic,
        int slotsPerShard,
        int physicalPartition,
        long evidenceGeneration,
        ProfileRef capability,
        byte[] controlScopeRef,
        byte[] managementEpoch,
        byte[] brokerPolicyImage,
        byte[] managementAuthorityImage) {
    public static final int VERSION = 1;
    public static final int MAX_POLICY_BYTES = 256 * 1024;
    public static final int MAX_MANAGEMENT_BYTES = 16 * 1024;
    public static final int MAX_CANONICAL_BYTES = MAX_POLICY_BYTES + MAX_MANAGEMENT_BYTES + 4096;
    public static final int RETAIN_UNTIL_SOURCE_RETIREMENT_AND_DELETE_CONFIRMED = 1;

    public TargetKafkaReceiptProtection {
        Objects.requireNonNull(resource, "exact Registry receipt resource");
        Objects.requireNonNull(capability, "original immutable capability");
        controlScopeRef = TargetCompatibilityCodec.assigned(controlScopeRef, 32, "control scope");
        managementEpoch = TargetCompatibilityCodec.assigned(managementEpoch, 16, "continuous management epoch");
        brokerPolicyImage = bounded(brokerPolicyImage, MAX_POLICY_BYTES, "actual Broker policy");
        managementAuthorityImage = bounded(managementAuthorityImage, MAX_MANAGEMENT_BYTES, "management authority");
        // Reuse the closed topic/cluster/slot geometry checks without constructing an observation permission.
        new TargetKafkaReceiptBinding(
                physicalTopic,
                resource,
                physicalTopic,
                slotsPerShard,
                physicalPartition,
                0,
                controlScopeRef,
                controlScopeRef);
        if (evidenceGeneration == 0 || capability.profileKind() != ProfileKind.DELIVERY_CAPABILITY) {
            throw new IllegalArgumentException(
                    "receipt protection requires assigned evidence generation and capability");
        }
    }

    @Override
    public byte[] controlScopeRef() {
        return Bytes.copy(controlScopeRef);
    }

    @Override
    public byte[] managementEpoch() {
        return Bytes.copy(managementEpoch);
    }

    @Override
    public byte[] brokerPolicyImage() {
        return Bytes.copy(brokerPolicyImage);
    }

    @Override
    public byte[] managementAuthorityImage() {
        return Bytes.copy(managementAuthorityImage);
    }

    public byte[] exactResourceHash() {
        return ResourceRetireIntentBody.decodeResourceIdentity(
                        ResourceKind.KAFKA_RECEIPT_SLOT, resource.exactResourceCanonicalBytes())
                .identityHash();
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, resource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, Bytes.utf8(physicalTopic));
            CanonicalProtobuf.uint32(out, 4, slotsPerShard);
            CanonicalProtobuf.uint32(out, 5, physicalPartition);
            CanonicalProtobuf.uint64Bits(out, 6, evidenceGeneration);
            CanonicalProtobuf.bytes(out, 7, capability.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, controlScopeRef);
            CanonicalProtobuf.bytes(out, 9, managementEpoch);
            CanonicalProtobuf.bytes(out, 10, brokerPolicyImage);
            CanonicalProtobuf.bytes(out, 11, managementAuthorityImage);
            CanonicalProtobuf.uint32(out, 12, RETAIN_UNTIL_SOURCE_RETIREMENT_AND_DELETE_CONFIRMED);
        });
    }

    public static TargetKafkaReceiptProtection decode(byte[] value) {
        final var f = TargetCompatibilityCodec.read(value, MAX_CANONICAL_BYTES, 12, false, "Target receipt protection");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, "Target receipt protection");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != VERSION
                || QueryCodecSupport.uint(f.getLast(), 12) != RETAIN_UNTIL_SOURCE_RETIREMENT_AND_DELETE_CONFIRMED) {
            throw new IllegalArgumentException("unknown receipt protection version or release rule");
        }
        final byte[] topic = QueryCodecSupport.bytes(f.get(2), 3);
        final String name = new String(topic, java.nio.charset.StandardCharsets.UTF_8);
        if (!Arrays.equals(topic, Bytes.utf8(name))) {
            throw new IllegalArgumentException("invalid receipt topic UTF-8");
        }
        final var image = new TargetKafkaReceiptProtection(
                KafkaReceiptSlotResource.decode(QueryCodecSupport.nested(f.get(1), 2)),
                name,
                QueryCodecSupport.uint32(f.get(3), 4),
                QueryCodecSupport.uint32(f.get(4), 5),
                QueryCodecSupport.uint64Bits(f.get(5), 6),
                ProfileRef.decode(QueryCodecSupport.nested(f.get(6), 7)),
                QueryCodecSupport.fixed(f.get(7), 8, 32),
                QueryCodecSupport.fixed(f.get(8), 9, 16),
                QueryCodecSupport.bytes(f.get(9), 10),
                QueryCodecSupport.bytes(f.get(10), 11));
        QueryCodecSupport.requireCanonical(value, image.canonicalBytes(), "Target receipt protection");
        return image;
    }

    public void requireChannel(TargetChannelIdentity channel) {
        final var c = Objects.requireNonNull(channel, "channel").context();
        if (c.kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || c.evidenceGeneration() != evidenceGeneration
                || !c.sourceShard().routeIncarnation().equals(resource.routeIncarnation())
                || c.sourceShard().partition() != resource.shardPartition()
                || !Arrays.equals(c.controlScopeRef(), controlScopeRef)) {
            throw new IllegalArgumentException(
                    "receipt protection changes channel's exact evidence/source/control scope");
        }
    }

    private static byte[] bounded(byte[] value, int maximum, String label) {
        if (value == null || value.length == 0 || value.length > maximum) {
            throw new IllegalArgumentException(label + " exceeds its finite image bound");
        }
        return Bytes.copy(value);
    }
}
