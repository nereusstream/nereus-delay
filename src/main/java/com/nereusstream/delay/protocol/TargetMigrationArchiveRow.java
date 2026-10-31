package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Native72 original logical/physical identity projection; materialization Source never becomes the old logical first. */
public record TargetMigrationArchiveRow(byte[] migrationId, byte[] planDigest, byte[] oldStore, byte[] oldDbIdentity,
        byte[] oldLineage, TargetQuotaIncarnation payer, TargetMigrationArchiveEntry entry, SourceBranch sourceBranch,
        long managementGeneration, byte[] managementDigest, TargetQuotaMutation materializedAt, byte[] sourceFrame) {
    public static final int VALUE_TYPE = 72;
    public static final int SCHEMA_VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = TargetMigrationArchiveEntry.MAX_CANONICAL_BYTES + TargetQuotaIncarnation.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-migration-logical-first-native72-v1\0");
    public enum SourceBranch { COPY_BATCH, LEGACY_PREFIX, TARGET_FIRST, ARCHIVE_REPLAY }
    public TargetMigrationArchiveRow {
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID"); planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        oldStore = TargetCompatibilityCodec.assigned(oldStore, 16, "old Store incarnation"); oldDbIdentity = TargetCompatibilityCodec.assigned(oldDbIdentity, 32, "old DB identity");
        oldLineage = TargetCompatibilityCodec.assigned(oldLineage, 16, "old lineage"); Objects.requireNonNull(payer); Objects.requireNonNull(entry); Objects.requireNonNull(sourceBranch);
        Objects.requireNonNull(materializedAt).requireSourceApplied(); entry.provenanceCut().requireAtOrBefore(materializedAt);
        if (sourceBranch == SourceBranch.COPY_BATCH) {
            if (managementGeneration == 0 || sourceFrame != null) throw new IllegalArgumentException("archive COPY row requires its actual bounded management history");
            managementDigest = TargetCompatibilityCodec.assigned(managementDigest, 32, "actual COPY management input");
            if (!Arrays.equals(managementDigest, materializedAt.mutationDigest())) throw new IllegalArgumentException("archive COPY changed actual Source input");
        } else {
            if (managementGeneration != 0 || managementDigest != null) throw new IllegalArgumentException("archive Source tail cannot masquerade as COPY control");
            sourceFrame = TargetMigrationRetirementInventory.material(sourceFrame, TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES, "actual original archive tail frame");
            TargetMigrationArchiveState.requireFrame(sourceFrame, materializedAt);
        }
    }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] oldStore() { return Bytes.copy(oldStore); }
    @Override public byte[] oldDbIdentity() { return Bytes.copy(oldDbIdentity); }
    @Override public byte[] oldLineage() { return Bytes.copy(oldLineage); }
    @Override public byte[] managementDigest() { return managementDigest == null ? null : Bytes.copy(managementDigest); }
    @Override public byte[] sourceFrame() { return sourceFrame == null ? null : Bytes.copy(sourceFrame); }
    public void requirePlan(TargetMigrationPlan plan) {
        if (!Arrays.equals(migrationId, plan.migrationId()) || !Arrays.equals(planDigest, plan.digest()) || !Arrays.equals(oldStore, plan.oldStore())
                || !Arrays.equals(oldDbIdentity, plan.oldDbIdentity()) || !Arrays.equals(oldLineage, plan.oldLineage())
                || !payer.scope().equals(plan.scope()) || !Arrays.equals(payer.recoveryLineage(), plan.newLineage()))
            throw new IllegalStateException("archive Native72 changed original/new owner Store/Source domains");
    }
    public static byte[] keyFor(byte[] originalKey) {
        if (originalKey == null || originalKey.length < 3 || originalKey.length > TargetMigrationArchiveEntry.MAX_KEY_BYTES
                || originalKey[0] < 1 || originalKey[0] > 5 || originalKey[1] != 1) throw new IllegalArgumentException("exact original namespace/key required");
        return Bytes.concat(new byte[] {TargetKeyCodec.MIGRATION_ARCHIVE_TAG, TargetKeyCodec.KEY_FORMAT}, originalKey);
    }
    public byte[] key() { return keyFor(entry.originalKey()); }
    private byte[] fields() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, migrationId); CanonicalProtobuf.bytes(out, 3, planDigest);
        CanonicalProtobuf.bytes(out, 4, oldStore); CanonicalProtobuf.bytes(out, 5, oldDbIdentity); CanonicalProtobuf.bytes(out, 6, oldLineage);
        CanonicalProtobuf.bytes(out, 7, payer.canonicalBytes()); CanonicalProtobuf.bytes(out, 8, entry.canonicalBytes());
        CanonicalProtobuf.uint32(out, 9, sourceBranch.ordinal() + 1);
        if (sourceBranch == SourceBranch.COPY_BATCH) { CanonicalProtobuf.uint64Bits(out, 10, managementGeneration); CanonicalProtobuf.bytes(out, 11, managementDigest); }
        CanonicalProtobuf.bytes(out, 12, materializedAt.canonicalBytes()); if (sourceFrame != null) CanonicalProtobuf.bytes(out, 13, sourceFrame);
    }); }
    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 14, digest()); }); }
    public static TargetMigrationArchiveRow decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 14, false, "migration Native72 archive row");
        if (f.size() < 12) throw new IllegalArgumentException("archive Native72 fields missing");
        for (int i = 0; i < 9; i++) if (f.get(i).number() != i + 1) throw new IllegalArgumentException("archive Native72 fields changed");
        TargetMigrationRetirementInventory.version(f.getFirst()); final int branch = QueryCodecSupport.uint32(f.get(8), 9);
        if (branch < 1 || branch > 4) throw new IllegalArgumentException("unknown archive Source provenance branch");
        int i = 9; final long generation = branch == 1 ? QueryCodecSupport.uint64Bits(f.get(i++), 10) : 0;
        final byte[] management = branch == 1 ? QueryCodecSupport.fixed(f.get(i++), 11, 32) : null;
        final var at = TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(i++), 12));
        final byte[] frame = branch == 1 ? null : QueryCodecSupport.bytes(f.get(i++), 13);
        if (f.size() != i + 1) throw new IllegalArgumentException("archive Native72 trailing fields");
        final var value = new TargetMigrationArchiveRow(QueryCodecSupport.fixed(f.get(1), 2, 32), QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.fixed(f.get(3), 4, 16), QueryCodecSupport.fixed(f.get(4), 5, 32), QueryCodecSupport.fixed(f.get(5), 6, 16),
                TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(6), 7)), TargetMigrationArchiveEntry.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                SourceBranch.values()[branch - 1], generation, management, at, frame);
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(i), 14, 32))) throw new IllegalArgumentException("archive Native72 digest changed");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration Native72 archive row"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationArchiveRow v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
