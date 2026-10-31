package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Five exact original DEDUPE ranges, including physical provenance. Only the private complete scanner proves these data. */
public record TargetMigrationArchiveCoverage(List<Range> ranges) {
    public static final int MAX_CANONICAL_BYTES = 5 * (3 * TargetMigrationArchiveEntry.MAX_KEY_BYTES + 128) + 32;
    public record Range(int namespace, byte[] lowerInclusive, byte[] upperExclusive, byte[] lastKey,
            long rows, long rawBytes, byte[] root, boolean complete) {
        public static final int MAX_CANONICAL_BYTES = 3 * TargetMigrationArchiveEntry.MAX_KEY_BYTES + 128;
        public Range {
            if (namespace < 1 || namespace > 5 || rows < 0 || rawBytes < 0 || (rows == 0) != (lastKey == null))
                throw new IllegalArgumentException("archive range changed exact namespace/count/cursor");
            lowerInclusive = key(lowerInclusive); upperExclusive = key(upperExclusive); lastKey = key(lastKey);
            root = TargetCompatibilityCodec.assigned(root, 32, "actual complete range fold");
            if (lowerInclusive != null && upperExclusive != null && Arrays.compareUnsigned(lowerInclusive, upperExclusive) >= 0)
                throw new IllegalArgumentException("archive range is not ordered");
        }
        private static byte[] key(byte[] value) { return value == null ? null : TargetMigrationRetirementInventory.material(value, TargetMigrationArchiveEntry.MAX_KEY_BYTES, "exact archive range key"); }
        @Override public byte[] lowerInclusive() { return lowerInclusive == null ? null : Bytes.copy(lowerInclusive); }
        @Override public byte[] upperExclusive() { return upperExclusive == null ? null : Bytes.copy(upperExclusive); }
        @Override public byte[] lastKey() { return lastKey == null ? null : Bytes.copy(lastKey); }
        @Override public byte[] root() { return Bytes.copy(root); }
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.uint32(out, 2, namespace);
            if (lowerInclusive != null) CanonicalProtobuf.bytes(out, 3, lowerInclusive);
            if (upperExclusive != null) CanonicalProtobuf.bytes(out, 4, upperExclusive);
            if (lastKey != null) CanonicalProtobuf.bytes(out, 5, lastKey);
            CanonicalProtobuf.uint64(out, 6, rows); CanonicalProtobuf.uint64(out, 7, rawBytes); CanonicalProtobuf.bytes(out, 8, root);
            CanonicalProtobuf.uint32(out, 9, complete ? 1 : 0);
        }); }
        public static Range decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "archive range coverage");
            if (f.size() < 6 || f.getFirst().number() != 1 || f.get(1).number() != 2) throw new IllegalArgumentException("archive range fields missing");
            TargetMigrationRetirementInventory.version(f.getFirst()); int i = 2;
            final byte[] lower = i < f.size() && f.get(i).number() == 3 ? QueryCodecSupport.bytes(f.get(i++), 3) : null;
            final byte[] upper = i < f.size() && f.get(i).number() == 4 ? QueryCodecSupport.bytes(f.get(i++), 4) : null;
            final byte[] last = i < f.size() && f.get(i).number() == 5 ? QueryCodecSupport.bytes(f.get(i++), 5) : null;
            if (f.size() != i + 4) throw new IllegalArgumentException("unknown archive range fields");
            final int complete = QueryCodecSupport.uint32(f.get(i + 3), 9);
            if (complete > 1) throw new IllegalArgumentException("invalid archive range EOF");
            final var value = new Range(QueryCodecSupport.uint32(f.get(1), 2), lower, upper, last,
                    QueryCodecSupport.uint(f.get(i), 6), QueryCodecSupport.uint(f.get(i + 1), 7), QueryCodecSupport.fixed(f.get(i + 2), 8, 32), complete == 1);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive range coverage"); return value;
        }
        @Override public boolean equals(Object other) { return other instanceof Range v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
        @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
    }
    public TargetMigrationArchiveCoverage {
        ranges = List.copyOf(ranges);
        if (ranges.size() != 5) throw new IllegalArgumentException("archive coverage must represent all five original namespaces");
        long count = 0, bytes = 0;
        for (int i = 0; i < 5; i++) {
            if (ranges.get(i).namespace() != i + 1) throw new IllegalArgumentException("archive namespace coverage is duplicate or incomplete");
            count = Math.addExact(count, ranges.get(i).rows()); bytes = Math.addExact(bytes, ranges.get(i).rawBytes());
        }
    }
    public long totalRows() { long count = 0; for (var range : ranges) count = Math.addExact(count, range.rows()); return count; }
    public long totalRawBytes() { long bytes = 0; for (var range : ranges) bytes = Math.addExact(bytes, range.rawBytes()); return bytes; }
    public boolean complete() { return ranges.stream().allMatch(Range::complete); }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1); for (var range : ranges) CanonicalProtobuf.bytes(out, 2, range.canonicalBytes());
    }); }
    public byte[] digest() { return Bytes.sha256(Bytes.utf8("nereus-delay-migration-archive-coverage-v1\0"), canonicalBytes()); }
    public static TargetMigrationArchiveCoverage decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, true, "archive coverage");
        if (f.size() != 6) throw new IllegalArgumentException("archive coverage omitted a namespace");
        TargetMigrationRetirementInventory.version(f.getFirst()); final var ranges = new ArrayList<Range>();
        for (int i = 1; i < f.size(); i++) ranges.add(Range.decode(QueryCodecSupport.bytes(f.get(i), 2)));
        final var value = new TargetMigrationArchiveCoverage(ranges);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive coverage"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationArchiveCoverage v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
