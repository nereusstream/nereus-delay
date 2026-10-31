package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import java.util.Arrays;
import java.util.List;

/** Same publication-record projection of Source retirement, including a restart-stable actual CAS anchor. */
record CheckpointCatalogRetirementState(
        byte[] lineage,
        byte[] store,
        byte[] db,
        byte[] intentDigest,
        TargetQuotaMutation intentAt,
        byte[] domainDigest,
        byte[] admissionClosure,
        long barrierVersion,
        byte[] barrierDigest,
        byte[] confirmationDigest,
        TargetQuotaMutation confirmedAt) {
    static final int MAX_BYTES = 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetMigrationCatalogBarrierReceipt.MAX_CLOSURE_BYTES
            + 1024;

    CheckpointCatalogRetirementState {
        lineage = fixed(lineage, 16);
        store = fixed(store, 16);
        db = fixed(db, 32);
        intentDigest = fixed(intentDigest, 32);
        domainDigest = fixed(domainDigest, 32);
        if (intentAt == null
                || admissionClosure == null
                || admissionClosure.length == 0
                || admissionClosure.length > TargetMigrationCatalogBarrierReceipt.MAX_CLOSURE_BYTES
                || barrierVersion < -1
                || (barrierVersion == -1) != (barrierDigest == null)
                || (confirmationDigest == null) != (confirmedAt == null)) {
            throw new IllegalArgumentException("incomplete original Catalog retirement projection");
        }
        admissionClosure = Bytes.copy(admissionClosure);
        intentAt.requireSourceApplied();
        barrierDigest = barrierDigest == null ? null : fixed(barrierDigest, 32);
        confirmationDigest = confirmationDigest == null ? null : fixed(confirmationDigest, 32);
        if (confirmedAt != null) {
            confirmedAt.requireAfter(intentAt);
            if (barrierDigest == null) {
                throw new IllegalArgumentException("unanchored retired Catalog");
            }
        }
    }

    @Override
    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    @Override
    public byte[] store() {
        return Bytes.copy(store);
    }

    @Override
    public byte[] db() {
        return Bytes.copy(db);
    }

    @Override
    public byte[] intentDigest() {
        return Bytes.copy(intentDigest);
    }

    @Override
    public byte[] domainDigest() {
        return Bytes.copy(domainDigest);
    }

    @Override
    public byte[] admissionClosure() {
        return Bytes.copy(admissionClosure);
    }

    @Override
    public byte[] barrierDigest() {
        return barrierDigest == null ? null : Bytes.copy(barrierDigest);
    }

    @Override
    public byte[] confirmationDigest() {
        return confirmationDigest == null ? null : Bytes.copy(confirmationDigest);
    }

    boolean matches(byte[] expectedLineage, byte[] expectedStore) {
        return Arrays.equals(lineage, expectedLineage) || expectedStore != null && Arrays.equals(store, expectedStore);
    }

    boolean anchored() {
        return barrierDigest != null;
    }

    CheckpointCatalogRetirementState anchor(long version, byte[] digest) {
        if (anchored()) {
            return this;
        }
        return new CheckpointCatalogRetirementState(
                lineage,
                store,
                db,
                intentDigest,
                intentAt,
                domainDigest,
                admissionClosure,
                version,
                digest,
                null,
                null);
    }

    CheckpointCatalogRetirementState confirmed(byte[] digest, TargetQuotaMutation at) {
        if (confirmedAt != null && (!Arrays.equals(confirmationDigest, digest) || !confirmedAt.equals(at))) {
            throw new IllegalStateException("Catalog retirement cannot adopt another Source confirmation");
        }
        return new CheckpointCatalogRetirementState(
                lineage,
                store,
                db,
                intentDigest,
                intentAt,
                domainDigest,
                admissionClosure,
                barrierVersion,
                barrierDigest,
                digest,
                at);
    }

    byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, lineage);
            CanonicalProtobuf.bytes(out, 3, store);
            CanonicalProtobuf.bytes(out, 4, db);
            CanonicalProtobuf.bytes(out, 5, intentDigest);
            CanonicalProtobuf.bytes(out, 6, intentAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, domainDigest);
            CanonicalProtobuf.bytes(out, 8, admissionClosure);
            if (anchored()) {
                CanonicalProtobuf.uint64(out, 9, barrierVersion);
                CanonicalProtobuf.bytes(out, 10, barrierDigest);
            }
            if (confirmedAt != null) {
                CanonicalProtobuf.bytes(out, 11, confirmationDigest);
                CanonicalProtobuf.bytes(out, 12, confirmedAt.canonicalBytes());
            }
        });
    }

    static CheckpointCatalogRetirementState decode(byte[] encoded) {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("invalid bounded Catalog retirement state");
        }
        final List<CanonicalProtobuf.Reader.Field> f = QueryCodecSupport.read(encoded, "Catalog retirement state");
        final boolean anchor = f.size() >= 10, confirmed = f.size() == 12;
        QueryCodecSupport.requireNumbers(
                f,
                confirmed
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
                        : anchor ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} : new int[] {1, 2, 3, 4, 5, 6, 7, 8},
                "Catalog retirement state");
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown Catalog retirement state");
        }
        final var state = new CheckpointCatalogRetirementState(
                QueryCodecSupport.fixed(f.get(1), 2, 16),
                QueryCodecSupport.fixed(f.get(2), 3, 16),
                QueryCodecSupport.fixed(f.get(3), 4, 32),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(6), 7, 32),
                QueryCodecSupport.bytes(f.get(7), 8),
                anchor ? QueryCodecSupport.uint(f.get(8), 9) : -1,
                anchor ? QueryCodecSupport.fixed(f.get(9), 10, 32) : null,
                confirmed ? QueryCodecSupport.fixed(f.get(10), 11, 32) : null,
                confirmed ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(11), 12)) : null);
        QueryCodecSupport.requireCanonical(encoded, state.canonicalBytes(), "Catalog retirement state");
        return state;
    }

    private static byte[] fixed(byte[] value, int length) {
        Bytes.requireLength(value, length, "Catalog retirement field");
        return Bytes.copy(value);
    }
}
