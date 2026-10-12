package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable mapping created in the actual Source Admission batch, independently from a committed Kafka receipt. */
public record TargetKafkaReceiptMapping(TargetPublishAdmissionBody admission, byte[] mutationHash,
        TargetQuotaMutation mutation, long logicalSequence, byte[] recoveryLineage) {
    public static final int VALUE_TYPE = 45;
    public static final int MAX_CANONICAL_BYTES = TargetPublishAdmissionBody.MAX_KAFKA_RECEIPT_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 128;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-kafka-receipt-mapping\0");
    private static final byte[] SLOT_DOMAIN = Bytes.utf8("nereus-delay-target-kafka-receipt-slot\0");

    public TargetKafkaReceiptMapping {
        Objects.requireNonNull(admission, "admission");
        Objects.requireNonNull(mutation, "actual Source stamp");
        mutationHash = TargetCompatibilityCodec.assigned(mutationHash, 32, "signed mutation hash");
        recoveryLineage = TargetCompatibilityCodec.assigned(recoveryLineage, 16, "lineage");
        if (admission.kafkaReceipt() == null || logicalSequence == 0 || mutation.isLocalMutation()
                || !admission.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("Target K2 mapping requires exact v6 Source Admission");
        }
    }

    @Override public byte[] mutationHash() { return Bytes.copy(mutationHash); }
    @Override public byte[] recoveryLineage() { return Bytes.copy(recoveryLineage); }

    public static byte[] slotId(final TargetChannelIdentity channel) {
        if (channel.context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT) {
            throw new IllegalArgumentException("Target K2 slot requires its transactional context");
        }
        return Bytes.sha256(SLOT_DOMAIN, channel.context().producerIdentity());
    }

    public byte[] slotId() { return slotId(admission.channel()); }
    public byte[] mappingId() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] key() { return Bytes.concat(new byte[] {TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG, TargetKeyCodec.KEY_FORMAT}, mappingId()); }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, admission.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, mutationHash);
            CanonicalProtobuf.bytes(out, 4, mutation.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 5, logicalSequence);
            CanonicalProtobuf.bytes(out, 6, recoveryLineage);
        });
    }

    public byte[] canonicalBytes() { return Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 7, mappingId()))); }

    public void requireOriginal(final SystemMutation original, final SourcePosition firstSource) {
        if (original.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !Arrays.equals(original.canonicalBody(), admission.canonicalBytes())
                || !Arrays.equals(original.mutationHash(), mutationHash)
                || !Arrays.equals(Bytes.sha256(original.canonicalEnvelope()), mutation.mutationDigest())
                || !Arrays.equals(firstSource.canonicalBytes(), mutation.source().canonicalBytes())) {
            throw new IllegalStateException("Target K2 mapping differs from original signed Admission/first Source");
        }
    }

    public static TargetKafkaReceiptMapping decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Target K2 mapping");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7}, "Target K2 mapping");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != 1) { throw new IllegalArgumentException("unknown Target K2 mapping version"); }
        final var result = new TargetKafkaReceiptMapping(TargetPublishAdmissionBody.decode(QueryCodecSupport.nested(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(3), 4)),
                QueryCodecSupport.uint64Bits(f.get(4), 5), QueryCodecSupport.fixed(f.get(5), 6, 16));
        if (!Arrays.equals(result.mappingId(), QueryCodecSupport.fixed(f.get(6), 7, 32))) { throw new IllegalArgumentException("Target K2 mapping hash differs"); }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target K2 mapping");
        return result;
    }

    public static TargetKafkaReceiptMapping decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key()) || !result.admission().shard().equals(shard)) {
            throw new IllegalArgumentException("Target K2 mapping key/Shard differs");
        }
        return result;
    }
}
