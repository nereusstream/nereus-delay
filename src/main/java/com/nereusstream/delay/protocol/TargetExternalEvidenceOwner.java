package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Unique Source-adopted logical owner of one immutable external K2 catalog object; no early release state. */
public record TargetExternalEvidenceOwner(
        int kind,
        byte[] artifactDigest,
        String exactKey,
        byte[] valueDigest,
        long canonicalValueBytes,
        TargetQuotaIncarnation primaryOwner,
        TargetKeyCodec.Domain originalDomain,
        byte[] originalControlDigest,
        SourcePosition originalControlSource,
        byte[] creatorAttemptId,
        TargetQuotaMutation createdAt) {
    public static final int VALUE_TYPE = 47;
    public static final int MAX_KEY_BYTES = 2048;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaIncarnation.MAX_CANONICAL_BYTES
            + 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + MAX_KEY_BYTES
            + 512;

    public TargetExternalEvidenceOwner {
        artifactDigest = TargetCompatibilityCodec.assigned(artifactDigest, 32, "artifact digest");
        valueDigest = TargetCompatibilityCodec.assigned(valueDigest, 32, "actual object bytes digest");
        originalControlDigest = TargetCompatibilityCodec.assigned(originalControlDigest, 32, "original CONTROL digest");
        creatorAttemptId = creatorAttemptId == null ? new byte[0] : Bytes.copy(creatorAttemptId);
        Objects.requireNonNull(primaryOwner, "full frozen Target incarnation");
        Objects.requireNonNull(originalDomain, "original channel domain");
        Objects.requireNonNull(createdAt, "actual Source adoption stamp").requireSourceApplied();
        TargetSourcePosition.requireBounded(originalControlSource);
        if (kind != TargetKafkaReceiptControlArtifact.PROTECTION && kind != TargetKafkaReceiptControlArtifact.ANCHOR
                || canonicalValueBytes <= 0
                || canonicalValueBytes > TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES
                || primaryOwner.identity().kind() != TargetQuotaIdentity.Kind.TARGET
                || !primaryOwner.identity().shard().equals(createdAt.source().shardId())
                || !primaryOwner.identity().shard().equals(originalControlSource.shardId())
                || (kind == TargetKafkaReceiptControlArtifact.PROTECTION
                        ? creatorAttemptId.length != 0
                        : creatorAttemptId.length != 32)) {
            throw new IllegalArgumentException("external evidence kind/size/frozen Source owner is invalid");
        }
        Objects.requireNonNull(exactKey, "complete external key");
        if (exactKey.isBlank()
                || exactKey.indexOf('\0') >= 0
                || exactKey.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES
                || !Normalizer.isNormalized(exactKey, Normalizer.Form.NFC)
                || !exactKey.equals(new String(exactKey.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("external evidence key is not canonical finite UTF-8");
        }
        primaryOwner.latestMutation().requireAtOrBefore(createdAt);
    }

    @Override
    public byte[] artifactDigest() {
        return Bytes.copy(artifactDigest);
    }

    @Override
    public byte[] valueDigest() {
        return Bytes.copy(valueDigest);
    }

    @Override
    public byte[] originalControlDigest() {
        return Bytes.copy(originalControlDigest);
    }

    @Override
    public byte[] creatorAttemptId() {
        return Bytes.copy(creatorAttemptId);
    }

    public byte[] key() {
        return key(kind, artifactDigest);
    }

    public static byte[] key(int kind, byte[] digest) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.EXTERNAL_EVIDENCE_OWNER_TAG, TargetKeyCodec.KEY_FORMAT, (byte) kind},
                digest);
    }

    public CapacityVector blobCharge() {
        return primaryOwner
                .accounting()
                .externalEvidenceCharge(exactKey.getBytes(StandardCharsets.UTF_8).length, canonicalValueBytes);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, kind);
            CanonicalProtobuf.bytes(out, 3, artifactDigest);
            CanonicalProtobuf.bytes(out, 4, Bytes.utf8(exactKey));
            CanonicalProtobuf.bytes(out, 5, valueDigest);
            CanonicalProtobuf.uint64(out, 6, canonicalValueBytes);
            CanonicalProtobuf.bytes(out, 7, primaryOwner.canonicalBytes());
            CanonicalProtobuf.uint32(out, 8, originalDomain.slot());
            CanonicalProtobuf.uint64Bits(out, 9, originalDomain.generation());
            CanonicalProtobuf.bytes(out, 10, originalControlDigest);
            CanonicalProtobuf.bytes(out, 11, originalControlSource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 12, creatorAttemptId);
            CanonicalProtobuf.bytes(out, 13, createdAt.canonicalBytes());
        });
    }

    public static TargetExternalEvidenceOwner decode(byte[] value) {
        final var f = TargetCompatibilityCodec.read(value, MAX_CANONICAL_BYTES, 13, false, "external evidence owner");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, "external evidence owner");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown external owner schema");
        }
        final var result = new TargetExternalEvidenceOwner(
                QueryCodecSupport.uint32(f.get(1), 2),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                new String(QueryCodecSupport.bytes(f.get(3), 4), StandardCharsets.UTF_8),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                QueryCodecSupport.uint(f.get(5), 6),
                TargetQuotaIncarnation.decode(QueryCodecSupport.nested(f.get(6), 7)),
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(f.get(7), 8), QueryCodecSupport.uint64Bits(f.get(8), 9)),
                QueryCodecSupport.fixed(f.get(9), 10, 32),
                SourcePositionCodec.decode(QueryCodecSupport.nested(f.get(10), 11)),
                QueryCodecSupport.bytes(f.get(11), 12),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(12), 13)));
        QueryCodecSupport.requireCanonical(value, result.canonicalBytes(), "external evidence owner");
        return result;
    }

    public static TargetExternalEvidenceOwner decodeForStore(
            byte[] key, byte[] value, ShardId shard, byte[] tenant, byte[] lineage) {
        final var result = decode(value);
        if (!Arrays.equals(key, result.key())
                || !result.primaryOwner.identity().shard().equals(shard)
                || !Arrays.equals(result.primaryOwner.tenantScope(), tenant)
                || !Arrays.equals(result.primaryOwner.recoveryLineage(), lineage)) {
            throw new IllegalArgumentException("external owner key/Shard/tenant/lineage differs");
        }
        return result;
    }

    public void requireObject(int expectedKind, byte[] digest, String key, byte[] canonicalValue) {
        if (kind != expectedKind
                || !Arrays.equals(artifactDigest, digest)
                || !exactKey.equals(key)
                || canonicalValueBytes != canonicalValue.length
                || !Arrays.equals(valueDigest, Bytes.sha256(canonicalValue))) {
            throw new IllegalStateException("external evidence changed its original exact object");
        }
    }
}
