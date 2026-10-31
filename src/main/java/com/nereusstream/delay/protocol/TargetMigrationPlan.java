package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Sealed pre-A import identity. Candidate is deliberately outside this preimage to avoid a self-reference. */
public record TargetMigrationPlan(TargetQuotaScope scope, byte[] migrationId, long generation,
        byte[] expectedPriorRootDigest, byte[] oldStore, byte[] oldDbIdentity, int oldFormat, byte[] oldLineage,
        TargetQuotaMutation baseline, TargetMigrationObjectReference oldManifest,
        byte[] newStore, byte[] newDbIdentity, int newFormat, byte[] newLineage,
        TargetMigrationObjectReference mapping, TargetMigrationObjectReference custody,
        TargetMigrationObjectReference conservation, TargetMigrationObjectReference closedWriterDomain,
        byte[] managementAuthorityDigest, TargetMigrationObjectReference readerEvidence) {
    public static final int MAX_CANONICAL_BYTES = 6 * (TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 6)
            + TargetQuotaScope.MAX_CANONICAL_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 1024;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-plan-v1\0");

    public TargetMigrationPlan {
        Objects.requireNonNull(scope, "original Shard scope");
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        expectedPriorRootDigest = expectedPriorRootDigest == null ? null
                : TargetCompatibilityCodec.assigned(expectedPriorRootDigest, 32, "expected preceding migration root");
        oldStore = TargetCompatibilityCodec.assigned(oldStore, 16, "old Store incarnation");
        oldDbIdentity = TargetCompatibilityCodec.assigned(oldDbIdentity, 32, "old DB identity");
        oldLineage = TargetCompatibilityCodec.assigned(oldLineage, 16, "old lineage");
        newStore = TargetCompatibilityCodec.assigned(newStore, 16, "new Store incarnation");
        newDbIdentity = TargetCompatibilityCodec.assigned(newDbIdentity, 32, "new DB identity");
        newLineage = TargetCompatibilityCodec.assigned(newLineage, 16, "new lineage");
        managementAuthorityDigest = TargetCompatibilityCodec.assigned(managementAuthorityDigest, 32, "original migration management authority");
        Objects.requireNonNull(baseline, "actual old applied cut C").requireSourceApplied();
        Objects.requireNonNull(oldManifest); Objects.requireNonNull(mapping); Objects.requireNonNull(custody);
        Objects.requireNonNull(conservation); Objects.requireNonNull(closedWriterDomain); Objects.requireNonNull(readerEvidence);
        if (scope.target() != null || generation == 0 || (generation == 1) != (expectedPriorRootDigest == null)
                || oldFormat != 1 || newFormat != 2 || Arrays.equals(oldStore, newStore)
                || Arrays.equals(oldDbIdentity, newDbIdentity) || !scope.shard().equals(baseline.source().shardId())
                || !Arrays.equals(oldLineage, oldManifest.resource().recoveryLineageId())) {
            throw new IllegalArgumentException("migration changed its exact legacy-to-Target Store, predecessor, Source or Manifest identity");
        }
    }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] expectedPriorRootDigest() { return expectedPriorRootDigest == null ? null : Bytes.copy(expectedPriorRootDigest); }
    @Override public byte[] oldStore() { return Bytes.copy(oldStore); }
    @Override public byte[] oldDbIdentity() { return Bytes.copy(oldDbIdentity); }
    @Override public byte[] oldLineage() { return Bytes.copy(oldLineage); }
    @Override public byte[] newStore() { return Bytes.copy(newStore); }
    @Override public byte[] newDbIdentity() { return Bytes.copy(newDbIdentity); }
    @Override public byte[] newLineage() { return Bytes.copy(newLineage); }
    @Override public byte[] managementAuthorityDigest() { return Bytes.copy(managementAuthorityDigest); }
    public byte[] digest() { return Bytes.sha256(DOMAIN, canonicalBytes()); }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, migrationId); CanonicalProtobuf.uint64Bits(out, 4, generation);
            if (expectedPriorRootDigest != null) CanonicalProtobuf.bytes(out, 5, expectedPriorRootDigest);
            CanonicalProtobuf.bytes(out, 6, oldStore); CanonicalProtobuf.bytes(out, 7, oldDbIdentity);
            CanonicalProtobuf.uint32(out, 8, oldFormat); CanonicalProtobuf.bytes(out, 9, oldLineage);
            CanonicalProtobuf.bytes(out, 10, baseline.canonicalBytes()); CanonicalProtobuf.bytes(out, 11, oldManifest.canonicalBytes());
            CanonicalProtobuf.bytes(out, 12, newStore); CanonicalProtobuf.bytes(out, 13, newDbIdentity);
            CanonicalProtobuf.uint32(out, 14, newFormat); CanonicalProtobuf.bytes(out, 15, newLineage);
            CanonicalProtobuf.bytes(out, 16, mapping.canonicalBytes()); CanonicalProtobuf.bytes(out, 17, custody.canonicalBytes());
            CanonicalProtobuf.bytes(out, 18, conservation.canonicalBytes()); CanonicalProtobuf.bytes(out, 19, closedWriterDomain.canonicalBytes());
            CanonicalProtobuf.bytes(out, 20, managementAuthorityDigest); CanonicalProtobuf.bytes(out, 21, readerEvidence.canonicalBytes());
        });
    }
    public static TargetMigrationPlan decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 21, false, "migration plan");
        final boolean prior = f.size() == 21;
        QueryCodecSupport.requireNumbers(f, prior ? new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21}
                : new int[] {1,2,3,4,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21}, "migration plan");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown migration plan schema");
        int i = 4;
        final byte[] preceding = prior ? QueryCodecSupport.fixed(f.get(i++), 5, 32) : null;
        final var value = new TargetMigrationPlan(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.uint64Bits(f.get(3), 4), preceding,
                QueryCodecSupport.fixed(f.get(i++), 6, 16), QueryCodecSupport.fixed(f.get(i++), 7, 32),
                QueryCodecSupport.uint32(f.get(i++), 8), QueryCodecSupport.fixed(f.get(i++), 9, 16),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(i++), 10)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 11)),
                QueryCodecSupport.fixed(f.get(i++), 12, 16), QueryCodecSupport.fixed(f.get(i++), 13, 32),
                QueryCodecSupport.uint32(f.get(i++), 14), QueryCodecSupport.fixed(f.get(i++), 15, 16),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 16)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 17)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 18)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 19)),
                QueryCodecSupport.fixed(f.get(i++), 20, 32), TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i), 21)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration plan"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationPlan value && Arrays.equals(canonicalBytes(), value.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
