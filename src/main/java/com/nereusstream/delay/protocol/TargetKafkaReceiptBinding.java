package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Closed Target K2 transport/retention context signed before Source allocates its immutable mapping. */
public record TargetKafkaReceiptBinding(String targetPhysicalTopic, KafkaReceiptSlotResource receiptResource,
        String receiptPhysicalTopic, int slotsPerShard, int receiptPartition, long scanStartOffset,
        byte[] scanObservationDigest, byte[] retentionProtectionDigest) {
    public static final int VERSION = 1;
    public static final int MAX_TOPIC_BYTES = 249;
    public static final int MAX_CANONICAL_BYTES = 2048;

    public TargetKafkaReceiptBinding {
        targetPhysicalTopic = topic(targetPhysicalTopic);
        receiptPhysicalTopic = topic(receiptPhysicalTopic);
        Objects.requireNonNull(receiptResource, "receiptResource");
        scanObservationDigest = TargetCompatibilityCodec.assigned(scanObservationDigest, 32, "scan observation");
        retentionProtectionDigest = TargetCompatibilityCodec.assigned(retentionProtectionDigest, 32, "retention protection");
        final long partition = Math.addExact(Math.multiplyExact(
                Integer.toUnsignedLong(receiptResource.shardPartition()), slotsPerShard),
                Integer.toUnsignedLong(receiptResource.receiptLaneSlot()));
        if (slotsPerShard <= 0 || receiptResource.receiptLaneSlot() < 0
                || receiptResource.receiptLaneSlot() >= slotsPerShard
                || receiptPartition < 0 || partition != receiptPartition || scanStartOffset < 0
                || receiptResource.authenticatedClusterId().getBytes(StandardCharsets.UTF_8).length
                        > CanonicalTargetPartition.MAX_CLUSTER_BYTES
                || receiptResource.receiptTopicUuid().getMostSignificantBits() == 0
                        && receiptResource.receiptTopicUuid().getLeastSignificantBits() == 0) {
            throw new IllegalArgumentException("Target K2 receipt scope/range is invalid");
        }
    }

    /** Shape/equality only. Actual guarded observation, resource and retention authority must be authenticated. */
    public void requirePublication(final TargetOrdinaryPublicationBinding publication) {
        final var p = Objects.requireNonNull(publication, "publication");
        final var context = p.channel().context();
        if (p.physical().resource().kind() != BrokerResourceIdentity.Kind.KAFKA
                || context.kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || !p.physical().resource().kafka().authenticatedClusterId().equals(receiptResource.authenticatedClusterId())
                || !context.sourceShard().routeIncarnation().equals(receiptResource.routeIncarnation())
                || context.sourceShard().partition() != receiptResource.shardPartition()) {
            throw new IllegalArgumentException("Target K2 receipt differs from its exact publication recovery scope");
        }
    }

    @Override public byte[] scanObservationDigest() { return Bytes.copy(scanObservationDigest); }
    @Override public byte[] retentionProtectionDigest() { return Bytes.copy(retentionProtectionDigest); }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, Bytes.utf8(targetPhysicalTopic));
            CanonicalProtobuf.bytes(out, 3, receiptResource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, Bytes.utf8(receiptPhysicalTopic));
            CanonicalProtobuf.uint32(out, 5, slotsPerShard);
            CanonicalProtobuf.uint32(out, 6, receiptPartition);
            CanonicalProtobuf.uint64(out, 7, scanStartOffset);
            CanonicalProtobuf.bytes(out, 8, scanObservationDigest);
            CanonicalProtobuf.bytes(out, 9, retentionProtectionDigest);
        });
    }

    public static TargetKafkaReceiptBinding decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target K2 context");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9}, "Target K2 context");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != VERSION) { throw new IllegalArgumentException("unknown Target K2 context version"); }
        final var result = new TargetKafkaReceiptBinding(utf8(QueryCodecSupport.bytes(f.get(1), 2)),
                KafkaReceiptSlotResource.decode(QueryCodecSupport.nested(f.get(2), 3)),
                utf8(QueryCodecSupport.bytes(f.get(3), 4)), QueryCodecSupport.uint32(f.get(4), 5),
                QueryCodecSupport.uint32(f.get(5), 6), QueryCodecSupport.uint(f.get(6), 7),
                QueryCodecSupport.fixed(f.get(7), 8, 32), QueryCodecSupport.fixed(f.get(8), 9, 32));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target K2 context");
        return result;
    }

    private static String utf8(final byte[] bytes) {
        final String value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8))) { throw new IllegalArgumentException("invalid K2 topic UTF-8"); }
        return value;
    }

    private static String topic(final String value) {
        Objects.requireNonNull(value, "physical topic");
        if (value.isEmpty() || value.length() > MAX_TOPIC_BYTES || value.equals(".") || value.equals("..")
                || !Normalizer.normalize(value, Normalizer.Form.NFC).equals(value)
                || !value.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("invalid exact Kafka physical topic");
        }
        return value;
    }
}
