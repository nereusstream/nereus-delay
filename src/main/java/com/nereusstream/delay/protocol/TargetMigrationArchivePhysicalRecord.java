package com.nereusstream.delay.protocol;

import java.util.Arrays;

/** Native72 nested schema2 physical occurrence. It references an existing archived first and creates no new logical first. */
public record TargetMigrationArchivePhysicalRecord(int recordKind, byte[] firstOriginalKey, byte[] firstDigest, byte[] outcome, long closedIngressDeadlineThrough) {
    public static final int MAX_CANONICAL_BYTES = TargetMigrationArchiveEntry.MAX_KEY_BYTES
            + com.nereusstream.delay.runtime.TargetResultRecord.MAX_PAYLOAD_BYTES + 128;
    public TargetMigrationArchivePhysicalRecord {
        if (closedIngressDeadlineThrough < -1 || recordKind != ProtocolTuple.CLIENT_COMMAND && recordKind != ProtocolTuple.SYSTEM_MUTATION)
            throw new IllegalArgumentException("archive physical occurrence requires actual Command/System kind");
        if ((firstOriginalKey == null) != (firstDigest == null)) throw new IllegalArgumentException("archive physical first reference must be complete or explicitly absent");
        firstOriginalKey = firstOriginalKey == null ? null : TargetMigrationRetirementInventory.material(firstOriginalKey, TargetMigrationArchiveEntry.MAX_KEY_BYTES, "original archived first key");
        firstDigest = firstDigest == null ? null : TargetCompatibilityCodec.assigned(firstDigest, 32, "exact original Native72 first");
        outcome = TargetMigrationRetirementInventory.material(outcome, com.nereusstream.delay.runtime.TargetResultRecord.MAX_PAYLOAD_BYTES, "actual physical Source outcome");
        if (firstOriginalKey != null && firstOriginalKey[0] != (recordKind == ProtocolTuple.CLIENT_COMMAND ? 1 : 5))
            throw new IllegalArgumentException("archive physical result changed original logical namespace");
    }
    @Override public byte[] firstOriginalKey() { return firstOriginalKey == null ? null : Bytes.copy(firstOriginalKey); }
    @Override public byte[] firstDigest() { return firstDigest == null ? null : Bytes.copy(firstDigest); }
    @Override public byte[] outcome() { return Bytes.copy(outcome); }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 2); CanonicalProtobuf.uint32(out, 2, recordKind);
        if (firstOriginalKey != null) { CanonicalProtobuf.bytes(out, 3, firstOriginalKey); CanonicalProtobuf.bytes(out, 4, firstDigest); } CanonicalProtobuf.bytes(out, 5, outcome); CanonicalProtobuf.uint64Bits(out, 6, closedIngressDeadlineThrough);
    }); }
    public static TargetMigrationArchivePhysicalRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "archive physical occurrence");
        final boolean first = f.size() == 6;
        QueryCodecSupport.requireNumbers(f, first ? new int[] {1,2,3,4,5,6} : new int[] {1,2,5,6}, "archive physical occurrence");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 2) throw new IllegalArgumentException("unknown archive physical schema");
        final var value = new TargetMigrationArchivePhysicalRecord(QueryCodecSupport.uint32(f.get(1), 2), first ? QueryCodecSupport.bytes(f.get(2), 3) : null,
                first ? QueryCodecSupport.fixed(f.get(3), 4, 32) : null, QueryCodecSupport.bytes(f.get(first ? 4 : 2), 5), QueryCodecSupport.uint64Bits(f.get(first ? 5 : 3), 6));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive physical occurrence"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationArchivePhysicalRecord v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
