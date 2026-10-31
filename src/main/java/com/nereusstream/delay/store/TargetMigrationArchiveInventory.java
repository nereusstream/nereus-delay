package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage;
import com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import java.util.Arrays;
import java.util.Objects;

/** One exact immutable custody-inventory link, not an additional Source transfer or retirement fact. */
record TargetMigrationArchiveInventory(
        byte[] intentDigest,
        TargetQuotaMutation intentAt,
        TargetQuotaMutation shadowCut,
        TargetQuotaMutation coveredThrough,
        TargetQuotaIdentity owner,
        TargetMigrationObjectReference allocation,
        byte[] priorRoot,
        TargetMigrationObjectReference priorInventory,
        TargetMigrationObjectReference packet,
        byte[] archiveRoot,
        TargetMigrationArchiveCoverage coverage,
        byte[] sourceCoverageDigest,
        long records,
        int packets,
        long reservedBytes,
        byte[] originalObligation,
        byte[] retention,
        long tailRows,
        long tailRawBytes) {
    static final int MAX_OWNER_MATERIAL_BYTES = 1 << 20;
    static final int MAX_BYTES = 2 * MAX_OWNER_MATERIAL_BYTES
            + TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES
            + 3 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 3 * TargetMigrationObjectReference.MAX_CANONICAL_BYTES
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 1024;

    TargetMigrationArchiveInventory {
        Objects.requireNonNull(intentAt);
        Objects.requireNonNull(shadowCut);
        Objects.requireNonNull(coveredThrough);
        Objects.requireNonNull(owner);
        Objects.requireNonNull(allocation);
        Objects.requireNonNull(packet);
        Objects.requireNonNull(coverage);
        Bytes.requireLength(intentDigest, 32, "actual retirement intent");
        Bytes.requireLength(priorRoot, 32, "prior archive root");
        Bytes.requireLength(archiveRoot, 32, "actual archive root");
        Bytes.requireLength(sourceCoverageDigest, 32, "actual Source coverage");
        if (records < 0
                || packets < 1
                || reservedBytes < 1
                || tailRows < 0
                || tailRawBytes < 0
                || originalObligation == null
                || originalObligation.length == 0
                || originalObligation.length > MAX_OWNER_MATERIAL_BYTES
                || retention == null
                || retention.length == 0
                || retention.length > MAX_OWNER_MATERIAL_BYTES) {
            throw new IllegalArgumentException("complete finite original receiving/fee/retention material required");
        }
        intentDigest = Bytes.copy(intentDigest);
        priorRoot = Bytes.copy(priorRoot);
        archiveRoot = Bytes.copy(archiveRoot);
        sourceCoverageDigest = Bytes.copy(sourceCoverageDigest);
        originalObligation = Bytes.copy(originalObligation);
        retention = Bytes.copy(retention);
    }

    byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, intentDigest);
            CanonicalProtobuf.bytes(out, 3, intentAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, shadowCut.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, coveredThrough.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, allocation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, priorRoot);
            if (priorInventory != null) {
                CanonicalProtobuf.bytes(out, 9, priorInventory.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 10, packet.canonicalBytes());
            CanonicalProtobuf.bytes(out, 11, archiveRoot);
            CanonicalProtobuf.bytes(out, 12, coverage.canonicalBytes());
            CanonicalProtobuf.bytes(out, 13, sourceCoverageDigest);
            CanonicalProtobuf.uint64(out, 14, records);
            CanonicalProtobuf.uint32(out, 15, packets);
            CanonicalProtobuf.uint64(out, 16, reservedBytes);
            CanonicalProtobuf.bytes(out, 17, originalObligation);
            CanonicalProtobuf.bytes(out, 18, retention);
            CanonicalProtobuf.uint64(out, 19, tailRows);
            CanonicalProtobuf.uint64(out, 20, tailRawBytes);
        });
    }

    static TargetMigrationArchiveInventory decode(byte[] encoded) {
        final var f = QueryCodecSupport.read(encoded, "actual archive custody inventory");
        if (encoded.length > MAX_BYTES || f.size() < 19 || f.size() > 20) {
            throw new IllegalArgumentException("archive custody inventory exceeds its exact codec");
        }
        for (int i = 0; i < 8; i++) {
            if (f.get(i).number() != i + 1) {
                throw new IllegalArgumentException("archive inventory fields changed");
            }
        }
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown archive inventory codec");
        }
        int i = 8;
        final var previous = f.get(i).number() == 9
                ? TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 9))
                : null;
        for (int number = 10, index = i; number <= 20; number++, index++) {
            if (f.get(index).number() != number) {
                throw new IllegalArgumentException("archive inventory fields missing");
            }
        }
        final var value = new TargetMigrationArchiveInventory(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                QueryCodecSupport.fixed(f.get(7), 8, 32),
                previous,
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i++), 10)),
                QueryCodecSupport.fixed(f.get(i++), 11, 32),
                TargetMigrationArchiveCoverage.decode(QueryCodecSupport.bytes(f.get(i++), 12)),
                QueryCodecSupport.fixed(f.get(i++), 13, 32),
                QueryCodecSupport.uint(f.get(i++), 14),
                QueryCodecSupport.uint32(f.get(i++), 15),
                QueryCodecSupport.uint(f.get(i++), 16),
                QueryCodecSupport.bytes(f.get(i++), 17),
                QueryCodecSupport.bytes(f.get(i++), 18),
                QueryCodecSupport.uint(f.get(i++), 19),
                QueryCodecSupport.uint(f.get(i), 20));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive custody inventory");
        return value;
    }

    void requireReceipt(TargetMigrationArchiveReceipt receipt) {
        if (!Arrays.equals(intentDigest, receipt.intentDigest())
                || !intentAt.equals(receipt.intentAt())
                || !shadowCut.equals(receipt.shadowCut())
                || !coveredThrough.equals(receipt.coveredThrough())
                || !owner.equals(receipt.newOwner())
                || !allocation.equals(receipt.allocation())
                || !Arrays.equals(archiveRoot, receipt.archiveRoot())
                || !coverage.equals(receipt.coverage())
                || !Arrays.equals(sourceCoverageDigest, receipt.sourceCoverageDigest())
                || tailRows != receipt.tailRows()
                || tailRawBytes != receipt.tailRawBytes()) {
            throw new IllegalStateException("actual receiving inventory differs from the Source TAKEOVER receipt");
        }
    }
}
