package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.RouteIncarnation;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.UUID;

/**
 * Explicit physical identity of one Kafka receipt slot.
 *
 * <p>The receipt topic is a separate pinned resource from the business target.
 * The slot-to-partition calculation is kept here so a journal cannot silently
 * project evidence for a different Shard or slot.</p>
 */
public record KafkaReceiptResource(
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        RouteIncarnation routeIncarnation,
        int shardPartition,
        int receiptLaneSlot,
        long slotGeneration,
        int receiptLaneSlotsPerShard,
        int receiptPartition) {
    public KafkaReceiptResource {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        if (receiptLaneSlotsPerShard <= 0 || receiptLaneSlot < 0 || receiptLaneSlot >= receiptLaneSlotsPerShard) {
            throw new IllegalArgumentException("invalid Kafka receipt slot geometry");
        }
        if (slotGeneration == 0) {
            throw new IllegalArgumentException("slotGeneration must be non-zero");
        }
        long expectedPartition;
        try {
            final long calculatedPartition = Math.addExact(
                    Math.multiplyExact(Integer.toUnsignedLong(shardPartition), receiptLaneSlotsPerShard),
                    receiptLaneSlot);
            expectedPartition = calculatedPartition;
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Kafka receipt partition geometry overflows", overflow);
        }
        if (expectedPartition > 0xffff_ffffL || receiptPartition != (int) expectedPartition) {
            throw new IllegalArgumentException("receiptPartition does not match Shard/slot geometry");
        }
    }

    /** Returns the same slot identity in the Registry's typed resource value. */
    public KafkaReceiptSlotResource protocolResource() {
        return new KafkaReceiptSlotResource(
                authenticatedClusterId,
                nativeTopicUuid,
                routeIncarnation,
                shardPartition,
                receiptLaneSlot,
                slotGeneration);
    }

    /** Returns the full ExactResourceIdentity wrapper for GC/protection bindings. */
    public byte[] exactResourceCanonicalBytes() {
        return protocolResource().exactResourceCanonicalBytes();
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
}
