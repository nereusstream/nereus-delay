package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Source-owned stable transactional domain: renewal cannot reset its sequence or bypass an unresolved mapping. */
public record TargetKafkaReceiptSlot(
        TargetChannelIdentity channel,
        long lastAllocatedSequence,
        byte[] pendingMappingId,
        TargetQuotaMutation mutation,
        byte[] recoveryLineage) {
    public static final int VALUE_TYPE = 46;
    public static final int MAX_CANONICAL_BYTES =
            TargetChannelIdentity.MAX_CANONICAL_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 128;

    public TargetKafkaReceiptSlot {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(mutation, "mutation");
        recoveryLineage = TargetCompatibilityCodec.assigned(recoveryLineage, 16, "lineage");
        pendingMappingId = pendingMappingId == null
                ? null
                : TargetCompatibilityCodec.assigned(pendingMappingId, 32, "pending mapping");
        if (channel.context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || lastAllocatedSequence == 0
                || mutation.isLocalMutation()
                || !channel.context().sourceShard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("invalid Target K2 slot Source scope");
        }
    }

    @Override
    public byte[] pendingMappingId() {
        return pendingMappingId == null ? null : Bytes.copy(pendingMappingId);
    }

    @Override
    public byte[] recoveryLineage() {
        return Bytes.copy(recoveryLineage);
    }

    public byte[] key() {
        return key(channel);
    }

    public static byte[] key(final TargetChannelIdentity channel) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.KAFKA_RECEIPT_SLOT_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetKafkaReceiptMapping.slotId(channel));
    }

    public long nextSequence() {
        if (pendingMappingId != null) {
            throw new IllegalStateException("unresolved Target K2 mapping blocks this stable transaction domain");
        }
        return TargetQuotaMutation.increment(lastAllocatedSequence);
    }

    public void requirePending(final TargetKafkaReceiptMapping mapping) {
        if (pendingMappingId == null
                || !Arrays.equals(pendingMappingId, mapping.mappingId())
                || lastAllocatedSequence != mapping.logicalSequence()
                || !Arrays.equals(key(), key(mapping.admission().channel()))
                || !Arrays.equals(recoveryLineage, mapping.recoveryLineage())) {
            throw new IllegalStateException("Target K2 slot no longer owns this exact mapping");
        }
    }

    public TargetKafkaReceiptSlot settle(final TargetKafkaReceiptMapping mapping, final TargetQuotaMutation result) {
        requirePending(mapping);
        result.requireAfter(mutation);
        return new TargetKafkaReceiptSlot(channel, lastAllocatedSequence, null, result, recoveryLineage);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, channel.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, lastAllocatedSequence);
            if (pendingMappingId != null) {
                CanonicalProtobuf.bytes(out, 4, pendingMappingId);
            }
            CanonicalProtobuf.bytes(out, 5, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, recoveryLineage);
        });
    }

    public static TargetKafkaReceiptSlot decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "Target K2 slot");
        final boolean pending = f.size() == 6;
        QueryCodecSupport.requireNumbers(
                f, pending ? new int[] {1, 2, 3, 4, 5, 6} : new int[] {1, 2, 3, 5, 6}, "Target K2 slot");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown Target K2 slot version");
        }
        final int stamp = pending ? 4 : 3;
        final var result = new TargetKafkaReceiptSlot(
                TargetChannelIdentity.decode(QueryCodecSupport.nested(f.get(1), 2)),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                pending ? QueryCodecSupport.fixed(f.get(3), 4, 32) : null,
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(stamp), 5)),
                QueryCodecSupport.fixed(f.get(stamp + 1), 6, 16));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target K2 slot");
        return result;
    }

    public static TargetKafkaReceiptSlot decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key())
                || !result.channel().context().sourceShard().equals(shard)) {
            throw new IllegalArgumentException("Target K2 slot key/Shard differs");
        }
        return result;
    }
}
