package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Complete original DEDUPE bytes and their real provenance; compact evidence does not invent a logical first Source. */
public record TargetMigrationArchiveEntry(byte[] originalKey, byte[] rawValue, Origin origin,
        SourcePosition originalFirst, TargetQuotaMutation provenanceCut, TargetMigrationRecordReference originalRecord) {
    public static final int MAX_KEY_BYTES = TargetSourcePosition.MAX_CANONICAL_BYTES + 2;
    public static final int MAX_RAW_BYTES = Math.max(4 * 1024 * 1024,
            com.nereusstream.delay.runtime.TargetResultRecord.MAX_CANONICAL_BYTES + 12);
    public static final int MAX_CANONICAL_BYTES = MAX_KEY_BYTES + MAX_RAW_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetSourcePosition.MAX_CANONICAL_BYTES + TargetMigrationRecordReference.MAX_CANONICAL_BYTES + 64;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-migration-archive-entry-v1\0");
    public enum Origin {
        COMPACT_BASELINE(1), LEGACY_SOURCE_FIRST(2), TARGET_SOURCE_FIRST(3), PHYSICAL_ONLY(4), ARCHIVE_PHYSICAL(5);
        private final int wire; Origin(int wire) { this.wire = wire; }
        public int wireValue() { return wire; }
        static Origin fromWire(int wire) { return switch (wire) {
            case 1 -> COMPACT_BASELINE; case 2 -> LEGACY_SOURCE_FIRST; case 3 -> TARGET_SOURCE_FIRST; case 4 -> PHYSICAL_ONLY; case 5 -> ARCHIVE_PHYSICAL;
            default -> throw new IllegalArgumentException("unknown migration archive provenance"); }; }
    }
    public TargetMigrationArchiveEntry {
        originalKey = TargetMigrationRetirementInventory.material(originalKey, MAX_KEY_BYTES, "complete original dedupe namespace/key");
        rawValue = TargetMigrationRetirementInventory.material(rawValue, MAX_RAW_BYTES, "complete original immutable dedupe value");
        Objects.requireNonNull(origin); Objects.requireNonNull(provenanceCut).requireSourceApplied(); Objects.requireNonNull(originalRecord);
        if (originalKey.length < 3 || originalKey[1] != 1 || originalKey[0] < 1 || originalKey[0] > 5
                || (originalKey[0] == 3) != (origin == Origin.PHYSICAL_ONLY || origin == Origin.ARCHIVE_PHYSICAL)
                || origin == Origin.COMPACT_BASELINE && originalFirst != null
                || origin != Origin.COMPACT_BASELINE && origin != Origin.PHYSICAL_ONLY && origin != Origin.ARCHIVE_PHYSICAL && originalFirst == null)
            throw new IllegalArgumentException("archive changed original namespace or invented compact/physical logical first");
        if (originalFirst != null) {
            originalFirst = TargetSourcePosition.requireBounded(originalFirst);
            if (!originalFirst.sameSourceIdentity(provenanceCut.source()) || originalFirst.compareTo(provenanceCut.source()) > 0)
                throw new IllegalArgumentException("archive original first is after its actual provenance cut");
        }
    }
    @Override public byte[] originalKey() { return Bytes.copy(originalKey); }
    @Override public byte[] rawValue() { return Bytes.copy(rawValue); }
    public int namespace() { return Byte.toUnsignedInt(originalKey[0]); }
    public int rawValueLength() { return rawValue.length; }
    public byte[] digest() { return Bytes.sha256(DOMAIN, canonicalBytes()); }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, originalKey); CanonicalProtobuf.bytes(out, 3, rawValue);
        CanonicalProtobuf.uint32(out, 4, origin.wireValue());
        if (originalFirst != null) CanonicalProtobuf.bytes(out, 5, originalFirst.canonicalBytes());
        CanonicalProtobuf.bytes(out, 6, provenanceCut.canonicalBytes()); CanonicalProtobuf.bytes(out, 7, originalRecord.canonicalBytes());
    }); }
    public static TargetMigrationArchiveEntry decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "migration archive entry");
        final boolean first = f.size() == 7;
        QueryCodecSupport.requireNumbers(f, first ? new int[] {1,2,3,4,5,6,7} : new int[] {1,2,3,4,6,7}, "migration archive entry");
        TargetMigrationRetirementInventory.version(f.getFirst()); int i = 4;
        final var source = first ? TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(i++), 5)) : null;
        final var value = new TargetMigrationArchiveEntry(QueryCodecSupport.bytes(f.get(1), 2), QueryCodecSupport.bytes(f.get(2), 3),
                Origin.fromWire(QueryCodecSupport.uint32(f.get(3), 4)), source, TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(i++), 6)),
                TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(i), 7)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration archive entry"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationArchiveEntry v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
