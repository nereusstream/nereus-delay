package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Reserved Broker-record metadata captured inside a Prepared Publish
 * descriptor. The values are duplicated in the descriptor so a destination
 * adapter cannot silently rewrite Delay identity or timing fields.
 */
public final class ReservedPublishMetadata {
    private static final int HASH_LENGTH = 32;

    private final RouteIncarnation routeIncarnation;
    private final long shardPartition;
    private final DelayMessageId messageId;
    private final long generation;
    private final byte[] publishAttemptId;
    private final byte[] destinationProfileSemanticHash;
    private final byte[] capabilityProfileSemanticHash;
    private final long deliverAtEpochMs;
    private final DeliveryMode deliveryMode;

    public ReservedPublishMetadata(
            final RouteIncarnation routeIncarnation,
            final long shardPartition,
            final DelayMessageId messageId,
            final long generation,
            final byte[] publishAttemptId,
            final byte[] destinationProfileSemanticHash,
            final byte[] capabilityProfileSemanticHash,
            final long deliverAtEpochMs,
            final DeliveryMode deliveryMode) {
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.shardPartition = uint32(shardPartition, "shardPartition");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.generation = uint32(generation, "generation");
        this.publishAttemptId = fixed(publishAttemptId, "publishAttemptId");
        this.destinationProfileSemanticHash = fixed(destinationProfileSemanticHash, "destinationProfileSemanticHash");
        this.capabilityProfileSemanticHash = fixed(capabilityProfileSemanticHash, "capabilityProfileSemanticHash");
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("deliverAtEpochMs must be non-negative");
        }
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.deliveryMode = Objects.requireNonNull(deliveryMode, "deliveryMode");
    }

    public RouteIncarnation routeIncarnation() {
        return routeIncarnation;
    }

    public long shardPartition() {
        return shardPartition;
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

    public byte[] destinationProfileSemanticHash() {
        return Bytes.copy(destinationProfileSemanticHash);
    }

    public byte[] capabilityProfileSemanticHash() {
        return Bytes.copy(capabilityProfileSemanticHash);
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public DeliveryMode deliveryMode() {
        return deliveryMode;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, routeIncarnation.bytes());
            CanonicalProtobuf.uint32(output, 2, shardPartition);
            CanonicalProtobuf.bytes(output, 3, messageId.bytes());
            CanonicalProtobuf.uint32(output, 4, generation);
            CanonicalProtobuf.bytes(output, 5, publishAttemptId);
            CanonicalProtobuf.bytes(output, 6, destinationProfileSemanticHash);
            CanonicalProtobuf.bytes(output, 7, capabilityProfileSemanticHash);
            CanonicalProtobuf.int64(output, 8, deliverAtEpochMs);
            CanonicalProtobuf.uint32(output, 9, deliveryMode.wireValue());
        });
    }

    public static ReservedPublishMetadata decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ReservedPublishMetadata");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "ReservedPublishMetadata");
        final ReservedPublishMetadata result = new ReservedPublishMetadata(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(0), 1, RouteIncarnation.LENGTH)),
                uint32(QueryCodecSupport.uint(fields.get(1), 2), "shardPartition"),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(2), 3, DelayMessageId.LENGTH)),
                uint32(QueryCodecSupport.uint(fields.get(3), 4), "generation"),
                QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH),
                nonNegative(QueryCodecSupport.uint(fields.get(7), 8), "deliverAtEpochMs"),
                DeliveryMode.fromWire(QueryCodecSupport.uint(fields.get(8), 9)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ReservedPublishMetadata");
        return result;
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

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ReservedPublishMetadata that)) {
            return false;
        }
        return shardPartition == that.shardPartition
                && generation == that.generation
                && deliverAtEpochMs == that.deliverAtEpochMs
                && routeIncarnation.equals(that.routeIncarnation)
                && messageId.equals(that.messageId)
                && deliveryMode == that.deliveryMode
                && Arrays.equals(publishAttemptId, that.publishAttemptId)
                && Arrays.equals(destinationProfileSemanticHash, that.destinationProfileSemanticHash)
                && Arrays.equals(capabilityProfileSemanticHash, that.capabilityProfileSemanticHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routeIncarnation,
                shardPartition,
                messageId,
                generation,
                Arrays.hashCode(publishAttemptId),
                Arrays.hashCode(destinationProfileSemanticHash),
                Arrays.hashCode(capabilityProfileSemanticHash),
                deliverAtEpochMs,
                deliveryMode);
    }
}
