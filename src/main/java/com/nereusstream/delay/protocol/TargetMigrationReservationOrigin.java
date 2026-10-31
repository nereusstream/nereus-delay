package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Original immutable Prepare receipt/source, distinct from the new Store's explicit migration accounting anchor. */
public record TargetMigrationReservationOrigin(
        byte[] planDigest,
        byte[] migrationId,
        TargetMigrationRecordReference originalReservation,
        SourcePosition originalPrepareSource,
        long receiptAnchorStateVersion,
        SourcePosition receiptAnchorSource,
        TargetQuotaMutation importedAt,
        TargetMigrationRecordReference receiptRecord,
        PayloadReservationReceipt originalReceipt,
        long originalStateVersion,
        com.nereusstream.delay.runtime.PayloadReservationStatus originalStatus,
        SourcePosition originalMutationSource) {
    public static final int MAX_RECEIPT_BYTES =
            1024 + 4096 + TargetChannelIdentity.MAX_PROFILE_REF_BYTES + TargetSourcePosition.MAX_CANONICAL_BYTES + 512;
    public static final int MAX_CANONICAL_BYTES = 2 * TargetMigrationRecordReference.MAX_CANONICAL_BYTES
            + 3 * TargetSourcePosition.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + MAX_RECEIPT_BYTES
            + 136;

    public TargetMigrationReservationOrigin {
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        Objects.requireNonNull(originalReservation);
        Objects.requireNonNull(receiptRecord);
        Objects.requireNonNull(originalReceipt);
        originalPrepareSource = TargetSourcePosition.requireBounded(originalPrepareSource);
        receiptAnchorSource = TargetSourcePosition.requireBounded(receiptAnchorSource);
        Objects.requireNonNull(importedAt).requireSourceApplied();
        Objects.requireNonNull(originalStatus);
        originalMutationSource = TargetSourcePosition.requireBounded(originalMutationSource);
        if (receiptAnchorStateVersion <= 0
                || originalPrepareSource.compareTo(receiptAnchorSource) > 0
                || receiptAnchorSource.compareTo(originalMutationSource) > 0
                || originalMutationSource.compareTo(importedAt.source()) > 0
                || originalStateVersion < receiptAnchorStateVersion) {
            throw new IllegalArgumentException(
                    "imported reservation lost original immutable Prepare/receipt/source order");
        }
        if (originalReceipt.frame().length > MAX_RECEIPT_BYTES
                || originalReceipt.objectStoreProfile().canonicalBytes().length
                        > TargetChannelIdentity.MAX_PROFILE_REF_BYTES
                || originalReceipt.stateVersion() != receiptAnchorStateVersion
                || !Arrays.equals(
                        originalReceipt.appliedSourcePosition().canonicalBytes(),
                        receiptAnchorSource.canonicalBytes())) {
            throw new IllegalArgumentException("migration reservation changed complete immutable original receipt");
        }
    }

    @Override
    public byte[] planDigest() {
        return Bytes.copy(planDigest);
    }

    @Override
    public byte[] migrationId() {
        return Bytes.copy(migrationId);
    }

    public void requirePlan(TargetMigrationPlan plan, TargetScheduleBinding binding) {
        if (!Arrays.equals(plan.digest(), planDigest)
                || !Arrays.equals(plan.migrationId(), migrationId)
                || !binding.imported()
                || binding.commandType() != CommandType.PREPARE_LARGE_SCHEDULE
                || !Arrays.equals(binding.migrationPlanDigest(), planDigest)
                || !Arrays.equals(binding.bindingSource().canonicalBytes(), originalPrepareSource.canonicalBytes())
                || !importedAt.equals(
                        binding.schemaVersion() == 2
                                ? TargetMigrationRoot.baselineFor(plan)
                                : binding.migrationSource())) {
            throw new IllegalArgumentException(
                    "reservation origin changed exact original Prepare binding/migration baseline");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, planDigest);
            CanonicalProtobuf.bytes(out, 3, migrationId);
            CanonicalProtobuf.bytes(out, 4, originalReservation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, originalPrepareSource.canonicalBytes());
            CanonicalProtobuf.uint64(out, 6, receiptAnchorStateVersion);
            CanonicalProtobuf.bytes(out, 7, receiptAnchorSource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, importedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, receiptRecord.canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, originalReceipt.frame());
            CanonicalProtobuf.uint64(out, 11, originalStateVersion);
            CanonicalProtobuf.uint32(out, 12, originalStatus.wireValue());
            CanonicalProtobuf.bytes(out, 13, originalMutationSource.canonicalBytes());
        });
    }

    public static TargetMigrationReservationOrigin decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 13, false, "migration reservation origin");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, "migration reservation origin");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown migration reservation origin schema");
        }
        final var value = new TargetMigrationReservationOrigin(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                QueryCodecSupport.uint(f.get(5), 6),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(8), 9)),
                decodeReceipt(QueryCodecSupport.bytes(f.get(9), 10)),
                QueryCodecSupport.uint(f.get(10), 11),
                com.nereusstream.delay.runtime.PayloadReservationStatus.fromWire(
                        QueryCodecSupport.uint32(f.get(11), 12)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(12), 13)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration reservation origin");
        return value;
    }

    private static PayloadReservationReceipt decodeReceipt(byte[] frame) {
        if (frame.length > MAX_RECEIPT_BYTES) {
            throw new IllegalArgumentException("original migration receipt exceeds existing identity bounds");
        }
        return PayloadReservationReceipt.decodeFrame(frame);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationReservationOrigin value
                && Arrays.equals(canonicalBytes(), value.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
