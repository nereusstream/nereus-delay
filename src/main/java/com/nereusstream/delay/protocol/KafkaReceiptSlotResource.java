package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Canonical typed value for the Registry KafkaReceiptSlotResource branch. */
public final class KafkaReceiptSlotResource {
    private static final int ID_LENGTH = 16;

    private final String authenticatedClusterId;
    private final UUID receiptTopicUuid;
    private final RouteIncarnation routeIncarnation;
    private final int shardPartition;
    private final int receiptLaneSlot;
    private final long slotGeneration;

    public KafkaReceiptSlotResource(
            final String authenticatedClusterId,
            final UUID receiptTopicUuid,
            final RouteIncarnation routeIncarnation,
            final int shardPartition,
            final int receiptLaneSlot,
            final long slotGeneration) {
        this.authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        this.receiptTopicUuid = Objects.requireNonNull(receiptTopicUuid, "receiptTopicUuid");
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.shardPartition = shardPartition;
        this.receiptLaneSlot = receiptLaneSlot;
        if (slotGeneration == 0) {
            throw new IllegalArgumentException("slotGeneration must be non-zero");
        }
        this.slotGeneration = slotGeneration;
    }

    public String authenticatedClusterId() {
        return authenticatedClusterId;
    }

    public UUID receiptTopicUuid() {
        return receiptTopicUuid;
    }

    public RouteIncarnation routeIncarnation() {
        return routeIncarnation;
    }

    public int shardPartition() {
        return shardPartition;
    }

    public int receiptLaneSlot() {
        return receiptLaneSlot;
    }

    public long slotGeneration() {
        return slotGeneration;
    }

    /** Returns the direct branch bytes; ExactResourceIdentity wraps these under field 4. */
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, authenticatedClusterId.getBytes(StandardCharsets.UTF_8));
            CanonicalProtobuf.bytes(output, 2, uuidBytes(receiptTopicUuid));
            CanonicalProtobuf.bytes(output, 3, routeIncarnation.bytes());
            CanonicalProtobuf.uint32Bits(output, 4, shardPartition);
            CanonicalProtobuf.uint32Bits(output, 5, receiptLaneSlot);
            CanonicalProtobuf.uint64Bits(output, 6, slotGeneration);
        });
    }

    /** Returns the full ExactResourceIdentity wrapper for this branch. */
    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, ResourceKind.KAFKA_RECEIPT_SLOT.wireValue(), canonicalBytes()));
    }

    public static KafkaReceiptSlotResource decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "KafkaReceiptSlotResource");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "KafkaReceiptSlotResource");
        final KafkaReceiptSlotResource result = new KafkaReceiptSlotResource(
                utf8(QueryCodecSupport.bytes(fields.get(0), 1)),
                uuid(QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH)),
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(2), 3, ID_LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(3), 4),
                QueryCodecSupport.uint32Bits(fields.get(4), 5),
                QueryCodecSupport.uint64Bits(fields.get(5), 6));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "KafkaReceiptSlotResource");
        return result;
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(ID_LENGTH)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static UUID uuid(final byte[] value) {
        return new UUID(Bytes.readU64be(value, 0), Bytes.readU64be(value, 8));
    }

    private static String utf8(final byte[] value) {
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException("authenticatedClusterId is not valid UTF-8");
        }
        return decoded;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof KafkaReceiptSlotResource that
                && authenticatedClusterId.equals(that.authenticatedClusterId)
                && receiptTopicUuid.equals(that.receiptTopicUuid)
                && routeIncarnation.equals(that.routeIncarnation)
                && shardPartition == that.shardPartition
                && receiptLaneSlot == that.receiptLaneSlot
                && slotGeneration == that.slotGeneration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                authenticatedClusterId,
                receiptTopicUuid,
                routeIncarnation,
                shardPartition,
                receiptLaneSlot,
                slotGeneration);
    }
}
