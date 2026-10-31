package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Durable original C-after prefix entry and its exact bounded mapping closure; never an ordinary Target first. */
public record TargetMigrationPrefixRecord(
        byte[] migrationId,
        byte[] planDigest,
        int recordKind,
        byte[] originalIdentity,
        byte[] originalSemanticHash,
        TargetMigrationRecordReference sourceRecord,
        byte[] rawFrameDigest,
        TargetQuotaMutation prior,
        TargetQuotaMutation at,
        TargetMigrationObjectReference projection) {
    public static final int VALUE_TYPE = 68;
    public static final int MAX_CANONICAL_BYTES = TargetMigrationRecordReference.MAX_CANONICAL_BYTES
            + TargetMigrationObjectReference.MAX_CANONICAL_BYTES
            + 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-prefix-record-v1\0");

    public TargetMigrationPrefixRecord {
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        originalIdentity = TargetCompatibilityCodec.assigned(
                originalIdentity,
                recordKind == ProtocolTuple.CLIENT_COMMAND ? CommandId.LENGTH : 32,
                "original Source record identity");
        originalSemanticHash =
                TargetCompatibilityCodec.assigned(originalSemanticHash, 32, "original command/mutation semantic hash");
        rawFrameDigest = TargetCompatibilityCodec.assigned(rawFrameDigest, 32, "complete original Source frame digest");
        Objects.requireNonNull(sourceRecord);
        Objects.requireNonNull(prior).requireSourceApplied();
        Objects.requireNonNull(at).requireAfter(prior);
        Objects.requireNonNull(projection);
        if (recordKind != ProtocolTuple.CLIENT_COMMAND && recordKind != ProtocolTuple.SYSTEM_MUTATION
                || at.sequence() != TargetQuotaMutation.increment(prior.sequence())
                || recordKind == ProtocolTuple.CLIENT_COMMAND
                        && !new CommandId(originalIdentity)
                                .routingId()
                                .shardId()
                                .equals(at.source().shardId())) {
            throw new IllegalArgumentException("migration prefix changed exact original record/next sequence");
        }
    }

    @Override
    public byte[] migrationId() {
        return Bytes.copy(migrationId);
    }

    @Override
    public byte[] planDigest() {
        return Bytes.copy(planDigest);
    }

    @Override
    public byte[] originalIdentity() {
        return Bytes.copy(originalIdentity);
    }

    @Override
    public byte[] originalSemanticHash() {
        return Bytes.copy(originalSemanticHash);
    }

    @Override
    public byte[] rawFrameDigest() {
        return Bytes.copy(rawFrameDigest);
    }

    public TargetQuotaMutation mutation() {
        return at;
    }

    public void requirePlan(TargetMigrationPlan plan) {
        if (!Arrays.equals(migrationId, plan.migrationId()) || !Arrays.equals(planDigest, plan.digest())) {
            throw new IllegalArgumentException("migration prefix changed complete sealed plan");
        }
        plan.baseline().requireAtOrBefore(prior);
        at.requireAfter(plan.baseline());
    }

    public static byte[] keyFor(byte[] migrationId, long sequence) {
        if (sequence == 0) {
            throw new IllegalArgumentException("unassigned migration prefix sequence");
        }
        return Bytes.concat(
                new byte[] {TargetKeyCodec.MIGRATION_PREFIX_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID"),
                Bytes.u64beBits(sequence));
    }

    public byte[] key() {
        return keyFor(migrationId, at.sequence());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, migrationId);
            CanonicalProtobuf.bytes(out, 3, planDigest);
            CanonicalProtobuf.uint32(out, 4, recordKind);
            CanonicalProtobuf.bytes(out, 5, originalIdentity);
            CanonicalProtobuf.bytes(out, 6, originalSemanticHash);
            CanonicalProtobuf.bytes(out, 7, sourceRecord.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, rawFrameDigest);
            CanonicalProtobuf.bytes(out, 9, prior.canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, at.canonicalBytes());
            CanonicalProtobuf.bytes(out, 11, projection.canonicalBytes());
        });
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 12, digest());
        });
    }

    public static TargetMigrationPrefixRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 12, false, "migration prefix record");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, "migration prefix record");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown migration prefix schema");
        }
        final var value = new TargetMigrationPrefixRecord(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.uint32(f.get(3), 4),
                QueryCodecSupport.bytes(f.get(4), 5),
                QueryCodecSupport.fixed(f.get(5), 6, 32),
                TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                QueryCodecSupport.fixed(f.get(7), 8, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(8), 9)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(9), 10)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(10), 11)));
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(11), 12, 32))) {
            throw new IllegalArgumentException("migration prefix digest changed");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration prefix record");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationPrefixRecord value
                && Arrays.equals(canonicalBytes(), value.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest());
    }
}
