package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Serialized full receive inventory, not the original new owner's actual protection/fee capability. */
public record TargetMigrationArchiveReceipt(
        byte[] intentDigest,
        TargetQuotaMutation intentAt,
        TargetQuotaMutation shadowCut,
        TargetQuotaMutation coveredThrough,
        TargetQuotaIdentity newOwner,
        TargetMigrationObjectReference allocation,
        byte[] archiveRoot,
        TargetMigrationArchiveCoverage coverage,
        List<TargetMigrationObjectReference> rawObjects,
        TargetMigrationObjectReference durableReceive,
        byte[] sourceCoverageDigest,
        long tailRows,
        long tailRawBytes) {
    public static final int MAX_RAW_OBJECTS = 128;
    public static final int MAX_CANONICAL_BYTES = 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + (MAX_RAW_OBJECTS + 2) * (TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 6)
            + TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES
            + 192;

    public TargetMigrationArchiveReceipt {
        intentDigest = TargetCompatibilityCodec.assigned(intentDigest, 32, "actual original retirement intent");
        archiveRoot = TargetCompatibilityCodec.assigned(archiveRoot, 32, "complete new archive root");
        sourceCoverageDigest =
                TargetCompatibilityCodec.assigned(sourceCoverageDigest, 32, "complete original Source coverage");
        Objects.requireNonNull(intentAt).requireSourceApplied();
        Objects.requireNonNull(shadowCut).requireSourceApplied();
        Objects.requireNonNull(coveredThrough).requireSourceApplied();
        shadowCut.requireAtOrBefore(coveredThrough);
        Objects.requireNonNull(newOwner);
        Objects.requireNonNull(allocation);
        Objects.requireNonNull(durableReceive);
        Objects.requireNonNull(coverage);
        rawObjects = List.copyOf(rawObjects);
        if (tailRows < 0
                || tailRawBytes < 0
                || !coverage.complete()
                || rawObjects.isEmpty()
                || rawObjects.size() > MAX_RAW_OBJECTS) {
            throw new IllegalArgumentException("complete protected archive receive material required");
        }
        for (int i = 1; i < rawObjects.size(); i++) {
            if (Arrays.compareUnsigned(
                            rawObjects.get(i - 1).canonicalBytes(),
                            rawObjects.get(i).canonicalBytes())
                    >= 0) {
                throw new IllegalArgumentException("received raw archive objects must be uniquely sorted");
            }
        }
    }

    @Override
    public byte[] intentDigest() {
        return Bytes.copy(intentDigest);
    }

    @Override
    public byte[] archiveRoot() {
        return Bytes.copy(archiveRoot);
    }

    @Override
    public byte[] sourceCoverageDigest() {
        return Bytes.copy(sourceCoverageDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, intentDigest);
            CanonicalProtobuf.bytes(out, 3, intentAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, shadowCut.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, coveredThrough.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, newOwner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, allocation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, archiveRoot);
            CanonicalProtobuf.bytes(out, 9, coverage.canonicalBytes());
            for (var object : rawObjects) {
                CanonicalProtobuf.bytes(out, 10, object.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 11, durableReceive.canonicalBytes());
            CanonicalProtobuf.bytes(out, 12, sourceCoverageDigest);
            CanonicalProtobuf.uint64(out, 13, tailRows);
            CanonicalProtobuf.uint64(out, 14, tailRawBytes);
        });
    }

    public static TargetMigrationArchiveReceipt decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, MAX_RAW_OBJECTS + 13, true, "archive receive receipt");
        if (f.size() < 14) {
            throw new IllegalArgumentException("archive receive receipt omitted complete raw material");
        }
        for (int i = 0; i < 9; i++) {
            if (f.get(i).number() != i + 1) {
                throw new IllegalArgumentException("archive receive receipt fields changed");
            }
        }
        TargetMigrationRetirementInventory.version(f.getFirst());
        final var raw = new ArrayList<TargetMigrationObjectReference>();
        for (int i = 9; i < f.size() - 4; i++) {
            raw.add(TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i), 10)));
        }
        final var value = new TargetMigrationArchiveReceipt(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                QueryCodecSupport.fixed(f.get(7), 8, 32),
                TargetMigrationArchiveCoverage.decode(QueryCodecSupport.bytes(f.get(8), 9)),
                raw,
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(f.size() - 4), 11)),
                QueryCodecSupport.fixed(f.get(f.size() - 3), 12, 32),
                QueryCodecSupport.uint(f.get(f.size() - 2), 13),
                QueryCodecSupport.uint(f.getLast(), 14));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive receive receipt");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationArchiveReceipt v && Arrays.equals(canonicalBytes(), v.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
