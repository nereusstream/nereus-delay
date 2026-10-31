package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** One imported row's explicit baseline and complete old conversion closure; never a fabricated ordinary first/grant. */
public record TargetMigrationOrigin(byte[] planDigest, byte[] migrationId, TargetQuotaMutation baseline,
        TargetMigrationRecordReference oldRecord, ColumnFamily oldFamily, byte[] oldKeyDigest, byte[] oldValueDigest,
        ColumnFamily newFamily, byte[] newKeyDigest, byte[] newValueDigest,
        TargetQuotaIncarnation owner, TargetQuotaUsage contribution, CapacityVector retainedLegacyCharge) {
    public static final int VALUE_TYPE = 66;
    public static final int MAX_CANONICAL_BYTES = TargetMigrationRecordReference.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + TargetQuotaIncarnation.MAX_CANONICAL_BYTES
            + TargetQuotaUsage.MAX_CANONICAL_BYTES + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES + 288;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-origin-v1\0");
    public TargetMigrationOrigin {
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "migration plan");
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        Objects.requireNonNull(baseline).requireSourceApplied(); Objects.requireNonNull(oldRecord);
        Objects.requireNonNull(oldFamily); Objects.requireNonNull(newFamily); Objects.requireNonNull(owner);
        Objects.requireNonNull(contribution); Objects.requireNonNull(retainedLegacyCharge);
        oldKeyDigest = TargetCompatibilityCodec.assigned(oldKeyDigest, 32, "old raw key digest");
        oldValueDigest = TargetCompatibilityCodec.assigned(oldValueDigest, 32, "old raw value digest");
        newKeyDigest = TargetCompatibilityCodec.assigned(newKeyDigest, 32, "new row key digest");
        newValueDigest = TargetCompatibilityCodec.assigned(newValueDigest, 32, "new row value digest");
        if (!owner.identity().shard().equals(baseline.source().shardId())) throw new IllegalArgumentException("migration origin owner/Source mismatch");
    }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] oldKeyDigest() { return Bytes.copy(oldKeyDigest); }
    @Override public byte[] oldValueDigest() { return Bytes.copy(oldValueDigest); }
    @Override public byte[] newKeyDigest() { return Bytes.copy(newKeyDigest); }
    @Override public byte[] newValueDigest() { return Bytes.copy(newValueDigest); }
    public static byte[] keyFor(ColumnFamily family, byte[] newKeyDigest) {
        return Bytes.concat(new byte[] {TargetKeyCodec.MIGRATION_ORIGIN_TAG, TargetKeyCodec.KEY_FORMAT, (byte) familyWire(family)},
                TargetCompatibilityCodec.assigned(newKeyDigest, 32, "imported row key digest"));
    }
    public byte[] key() { return keyFor(newFamily, newKeyDigest); }
    public void requireRow(ColumnFamily family, byte[] key, byte[] nativeValue) {
        if (family != newFamily || !Arrays.equals(Bytes.sha256(key), newKeyDigest) || !Arrays.equals(Bytes.sha256(nativeValue), newValueDigest)) {
            throw new IllegalArgumentException("imported row changed exact family/key/native bytes");
        }
    }
    public void requirePlan(TargetMigrationPlan plan) {
        if (!Arrays.equals(planDigest, plan.digest()) || !Arrays.equals(migrationId, plan.migrationId()) || !baseline.equals(plan.baseline())
                || !Arrays.equals(owner.tenantScope(), plan.scope().tenantScope()) || !Arrays.equals(owner.recoveryLineage(), plan.newLineage())) {
            throw new IllegalArgumentException("import origin differs from complete original plan/baseline/owner");
        }
    }
    public static int familyWire(ColumnFamily family) {
        return switch (Objects.requireNonNull(family)) {
            case TIMELINE -> 1; case ID -> 2; case INFLIGHT -> 3; case DEDUPE -> 4; case TERMINAL -> 5; case GC -> 6; case META -> 7;
        };
    }
    public static ColumnFamily family(int wire) {
        return switch (wire) {
            case 1 -> ColumnFamily.TIMELINE; case 2 -> ColumnFamily.ID; case 3 -> ColumnFamily.INFLIGHT;
            case 4 -> ColumnFamily.DEDUPE; case 5 -> ColumnFamily.TERMINAL; case 6 -> ColumnFamily.GC; case 7 -> ColumnFamily.META;
            default -> throw new IllegalArgumentException("unknown migration column family");
        };
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, planDigest); CanonicalProtobuf.bytes(out, 3, migrationId);
            CanonicalProtobuf.bytes(out, 4, baseline.canonicalBytes()); CanonicalProtobuf.bytes(out, 5, oldRecord.canonicalBytes());
            CanonicalProtobuf.uint32(out, 6, familyWire(oldFamily)); CanonicalProtobuf.bytes(out, 7, oldKeyDigest); CanonicalProtobuf.bytes(out, 8, oldValueDigest);
            CanonicalProtobuf.uint32(out, 9, familyWire(newFamily)); CanonicalProtobuf.bytes(out, 10, newKeyDigest); CanonicalProtobuf.bytes(out, 11, newValueDigest);
            CanonicalProtobuf.bytes(out, 12, owner.canonicalBytes()); CanonicalProtobuf.bytes(out, 13, contribution.canonicalBytes());
            CanonicalProtobuf.bytes(out, 14, retainedLegacyCharge.canonicalBytes());
        });
    }
    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 15, digest()); }); }
    public static TargetMigrationOrigin decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 15, false, "migration origin");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}, "migration origin");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown migration origin schema");
        final var value = new TargetMigrationOrigin(QueryCodecSupport.fixed(f.get(1), 2, 32), QueryCodecSupport.fixed(f.get(2), 3, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)), TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                family(QueryCodecSupport.uint32(f.get(5), 6)), QueryCodecSupport.fixed(f.get(6), 7, 32), QueryCodecSupport.fixed(f.get(7), 8, 32),
                family(QueryCodecSupport.uint32(f.get(8), 9)), QueryCodecSupport.fixed(f.get(9), 10, 32), QueryCodecSupport.fixed(f.get(10), 11, 32),
                TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(11), 12)), TargetQuotaUsage.decode(QueryCodecSupport.bytes(f.get(12), 13)),
                CapacityVector.decode(QueryCodecSupport.bytes(f.get(13), 14)));
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(14), 15, 32))) throw new IllegalArgumentException("migration origin digest changed");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration origin"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationOrigin value && Arrays.equals(canonicalBytes(), value.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
