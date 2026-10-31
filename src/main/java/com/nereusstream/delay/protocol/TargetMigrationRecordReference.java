package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Ordinal of a complete raw record in one fixed immutable bundle chunk; a digest is not its replacement. */
public record TargetMigrationRecordReference(TargetMigrationObjectReference object, long ordinal) {
    public static final int MAX_CANONICAL_BYTES = TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 19;

    public TargetMigrationRecordReference {
        Objects.requireNonNull(object, "original raw-record chunk");
        if (ordinal < 0 || ordinal == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid raw record ordinal");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, object.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, ordinal);
        });
    }

    public static TargetMigrationRecordReference decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "migration raw record reference");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "migration raw record reference");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown migration raw record schema");
        }
        final var value = new TargetMigrationRecordReference(
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint(f.get(2), 3));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration raw record reference");
        return value;
    }
}
