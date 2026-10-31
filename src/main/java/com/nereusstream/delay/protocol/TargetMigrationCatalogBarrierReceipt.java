package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Complete same-Source Catalog CAS observation. Serialized data never substitutes for the original owner capability.
 */
public record TargetMigrationCatalogBarrierReceipt(
        byte[] intentDigest,
        TargetQuotaMutation intentAt,
        TargetMigrationRetirementInventory.CatalogDomain domain,
        long barrierVersion,
        byte[] barrierRecordDigest,
        byte[] originalAdmissionClosure) {
    public static final int MAX_CLOSURE_BYTES = 4 * 1024 * 1024;
    public static final int MAX_CANONICAL_BYTES = TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + MAX_CLOSURE_BYTES
            + 128;

    public TargetMigrationCatalogBarrierReceipt {
        intentDigest = TargetCompatibilityCodec.assigned(intentDigest, 32, "original retirement intent");
        Objects.requireNonNull(intentAt).requireSourceApplied();
        Objects.requireNonNull(domain);
        barrierRecordDigest =
                TargetCompatibilityCodec.assigned(barrierRecordDigest, 32, "actual durable Catalog barrier record");
        originalAdmissionClosure = TargetMigrationRetirementInventory.material(
                originalAdmissionClosure, MAX_CLOSURE_BYTES, "original admission closure material");
        if (barrierVersion <= domain.originalVersion()) {
            throw new IllegalArgumentException("Catalog barrier requires a later actual CAS version");
        }
    }

    @Override
    public byte[] intentDigest() {
        return Bytes.copy(intentDigest);
    }

    @Override
    public byte[] barrierRecordDigest() {
        return Bytes.copy(barrierRecordDigest);
    }

    @Override
    public byte[] originalAdmissionClosure() {
        return Bytes.copy(originalAdmissionClosure);
    }

    public void requireIntent(TargetMigrationRetirementIntentRequest intent, TargetQuotaMutation at) {
        if (!Arrays.equals(intentDigest, intent.digest())
                || !intentAt.equals(at)
                || !domain.equals(intent.inventory().catalog())) {
            throw new IllegalArgumentException(
                    "Catalog receipt changed the frozen original retirement domain/Source fact");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, intentDigest);
            CanonicalProtobuf.bytes(out, 3, intentAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, domain.canonicalBytes());
            CanonicalProtobuf.uint64(out, 5, barrierVersion);
            CanonicalProtobuf.bytes(out, 6, barrierRecordDigest);
            CanonicalProtobuf.bytes(out, 7, originalAdmissionClosure);
        });
    }

    public static TargetMigrationCatalogBarrierReceipt decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 7, false, "migration Catalog barrier receipt");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7}, "migration Catalog barrier receipt");
        TargetMigrationRetirementInventory.version(f.getFirst());
        final var value = new TargetMigrationCatalogBarrierReceipt(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetMigrationRetirementInventory.CatalogDomain.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                QueryCodecSupport.uint(f.get(4), 5),
                QueryCodecSupport.fixed(f.get(5), 6, 32),
                QueryCodecSupport.bytes(f.get(6), 7));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration Catalog barrier receipt");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationCatalogBarrierReceipt v
                && Arrays.equals(canonicalBytes(), v.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
