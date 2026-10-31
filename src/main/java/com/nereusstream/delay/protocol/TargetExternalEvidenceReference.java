package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** One immutable mapping/attempt reference to an external owner. UNKNOWN never removes this reference. */
public record TargetExternalEvidenceReference(
        int kind,
        byte[] artifactDigest,
        byte[] mappingId,
        byte[] attemptId,
        TargetQuotaIdentity referenceOwner,
        TargetQuotaAccounting accounting,
        TargetQuotaMutation mutation,
        byte[] lineage) {
    public static final int VALUE_TYPE = 48;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 192;

    public TargetExternalEvidenceReference {
        artifactDigest = TargetCompatibilityCodec.assigned(artifactDigest, 32, "artifact digest");
        mappingId = TargetCompatibilityCodec.assigned(mappingId, 32, "mapping id");
        attemptId = TargetCompatibilityCodec.assigned(attemptId, 32, "attempt id");
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "lineage");
        Objects.requireNonNull(referenceOwner, "frozen reference owner");
        Objects.requireNonNull(accounting, "frozen accounting");
        Objects.requireNonNull(mutation, "actual Source reference").requireSourceApplied();
        if (kind != 1 && kind != 2
                || referenceOwner.kind() != TargetQuotaIdentity.Kind.TARGET
                || !referenceOwner.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("external evidence reference crosses its Source/frozen Target");
        }
    }

    @Override
    public byte[] artifactDigest() {
        return Bytes.copy(artifactDigest);
    }

    @Override
    public byte[] mappingId() {
        return Bytes.copy(mappingId);
    }

    @Override
    public byte[] attemptId() {
        return Bytes.copy(attemptId);
    }

    @Override
    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    public byte[] key() {
        return key(kind, mappingId);
    }

    public static byte[] key(int kind, byte[] mappingId) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG, TargetKeyCodec.KEY_FORMAT, (byte) kind},
                mappingId);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, kind);
            CanonicalProtobuf.bytes(out, 3, artifactDigest);
            CanonicalProtobuf.bytes(out, 4, mappingId);
            CanonicalProtobuf.bytes(out, 5, attemptId);
            CanonicalProtobuf.bytes(out, 6, referenceOwner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, lineage);
        });
    }

    public static TargetExternalEvidenceReference decode(byte[] value) {
        final var f =
                TargetCompatibilityCodec.read(value, MAX_CANONICAL_BYTES, 9, false, "external evidence reference");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "external evidence reference");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown external reference schema");
        }
        final var result = new TargetExternalEvidenceReference(
                QueryCodecSupport.uint32(f.get(1), 2),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.fixed(f.get(3), 4, 32),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                TargetQuotaIdentity.decode(QueryCodecSupport.nested(f.get(5), 6)),
                TargetQuotaAccounting.decode(QueryCodecSupport.nested(f.get(6), 7)),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(7), 8)),
                QueryCodecSupport.fixed(f.get(8), 9, 16));
        QueryCodecSupport.requireCanonical(value, result.canonicalBytes(), "external evidence reference");
        return result;
    }

    public static TargetExternalEvidenceReference decodeForStore(
            byte[] key, byte[] value, ShardId shard, byte[] lineage) {
        final var result = decode(value);
        final boolean index = result.kind == TargetKafkaReceiptControlArtifact.ANCHOR
                && Arrays.equals(
                        key,
                        com.nereusstream.delay.runtime.TargetExternalEvidenceStore.referenceByAttemptKey(
                                result.attemptId));
        if ((!Arrays.equals(key, result.key()) && !index)
                || !result.referenceOwner.shard().equals(shard)
                || !Arrays.equals(result.lineage, lineage)) {
            throw new IllegalArgumentException("external reference key/Shard/lineage differs");
        }
        return result;
    }
}
