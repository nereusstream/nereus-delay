package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable measurement rules pinned by Target grants and retained charges; no filesystem measurements. */
public final class TargetQuotaAccounting {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 2 + 34 + 2 + 4 * 11 + 34;
    public static final int VALUE_ENVELOPE_BYTES = 12;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-accounting\0");

    /** Each stored record occupies exactly one record-byte class, independent of payload/execution ownership. */
    public enum RecordClass {
        STATE(CapacityDimension.LOGICAL_STATE_BYTES, null),
        RESULT(CapacityDimension.RESULT_BYTES, CapacityDimension.RESULT_RECORDS),
        SYSTEM_MUTATION(CapacityDimension.SYSTEM_MUTATION_BYTES, CapacityDimension.SYSTEM_MUTATION_RECORDS),
        EVIDENCE(CapacityDimension.EVIDENCE_BYTES, CapacityDimension.EVIDENCE_RECORDS);

        private final CapacityDimension bytes;
        private final CapacityDimension records;

        RecordClass(final CapacityDimension bytes, final CapacityDimension records) {
            this.bytes = bytes;
            this.records = records;
        }
    }

    private final byte[] schemaBundleHash;
    private final long recordOverheadBytes;
    private final long kafkaEnvelopeBytes;
    private final long pulsarEnvelopeBytes;
    private final long minimumRecordCostBytes;
    private final byte[] digest;

    public TargetQuotaAccounting(
            final byte[] schemaBundleHash,
            final long recordOverheadBytes,
            final long kafkaEnvelopeBytes,
            final long pulsarEnvelopeBytes,
            final long minimumRecordCostBytes) {
        this.schemaBundleHash = TargetCompatibilityCodec.assigned(schemaBundleHash, 32, "schemaBundleHash");
        if (recordOverheadBytes < 0
                || kafkaEnvelopeBytes < 0
                || pulsarEnvelopeBytes < 0
                || minimumRecordCostBytes <= 0) {
            throw new IllegalArgumentException("invalid Target accounting constants");
        }
        this.recordOverheadBytes = recordOverheadBytes;
        this.kafkaEnvelopeBytes = kafkaEnvelopeBytes;
        this.pulsarEnvelopeBytes = pulsarEnvelopeBytes;
        this.minimumRecordCostBytes = minimumRecordCostBytes;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public byte[] schemaBundleHash() {
        return Bytes.copy(schemaBundleHash);
    }

    public long recordOverheadBytes() {
        return recordOverheadBytes;
    }

    public long kafkaEnvelopeBytes() {
        return kafkaEnvelopeBytes;
    }

    public long pulsarEnvelopeBytes() {
        return pulsarEnvelopeBytes;
    }

    public long minimumRecordCostBytes() {
        return minimumRecordCostBytes;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public long accountedPublishBytes(final AdapterKind adapter, final long payloadBytes, final long metadataBytes) {
        requireLength(payloadBytes);
        requireLength(metadataBytes);
        final long overhead =
                switch (Objects.requireNonNull(adapter, "adapter")) {
                    case KAFKA -> kafkaEnvelopeBytes;
                    case PULSAR -> pulsarEnvelopeBytes;
                };
        return Math.addExact(Math.addExact(payloadBytes, metadataBytes), overhead);
    }

    public long schedulingCost(final AdapterKind adapter, final long payloadBytes, final long metadataBytes) {
        return Math.max(minimumRecordCostBytes, accountedPublishBytes(adapter, payloadBytes, metadataBytes));
    }

    /** Typed payload excludes the 12-byte NV envelope, which is counted exactly once here. */
    public long storedRecordBytes(final long keyBytes, final long typedPayloadBytes) {
        requireLength(keyBytes);
        requireLength(typedPayloadBytes);
        if (keyBytes == 0) {
            throw new IllegalArgumentException("stored accounting record key must be nonempty");
        }
        return Math.addExact(
                Math.addExact(Math.addExact(keyBytes, typedPayloadBytes), VALUE_ENVELOPE_BYTES), recordOverheadBytes);
    }

    public CapacityVector recordCharge(
            final RecordClass recordClass, final long keyBytes, final long typedPayloadBytes) {
        Objects.requireNonNull(recordClass, "recordClass");
        final long[] result = new long[CapacityDimension.COUNT];
        result[recordClass.bytes.wireValue() - 1] = storedRecordBytes(keyBytes, typedPayloadBytes);
        if (recordClass.records != null) {
            result[recordClass.records.wireValue() - 1] = 1;
        }
        return new CapacityVector(result);
    }

    public CapacityVector executionCharge(
            final AdapterKind adapter, final long payloadBytes, final long metadataBytes) {
        final long[] result = new long[CapacityDimension.COUNT];
        result[CapacityDimension.INFLIGHT_MESSAGES.wireValue() - 1] = 1;
        result[CapacityDimension.INFLIGHT_BYTES.wireValue() - 1] =
                accountedPublishBytes(adapter, payloadBytes, metadataBytes);
        return new CapacityVector(result);
    }

    /** WAL callers supply the complete canonical frame; unlike an NV record, it receives no extra envelope. */
    public CapacityVector outcomeWalCharge(final long framedBytes) {
        requireLength(framedBytes);
        if (framedBytes == 0) {
            throw new IllegalArgumentException("Outcome WAL frame must be nonempty");
        }
        final long[] result = new long[CapacityDimension.COUNT];
        result[CapacityDimension.OUTCOME_WAL_BYTES.wireValue() - 1] = Math.addExact(framedBytes, recordOverheadBytes);
        return new CapacityVector(result);
    }

    public static CapacityVector activePayload(final long payloadBytes) {
        return payloadCharge(CapacityDimension.ACTIVE_MESSAGES, CapacityDimension.PENDING_PAYLOAD_BYTES, payloadBytes);
    }

    public static CapacityVector reservedPayload(final long payloadBytes) {
        return payloadCharge(
                CapacityDimension.RESERVATION_MESSAGES, CapacityDimension.RESERVATION_PAYLOAD_BYTES, payloadBytes);
    }

    public static CapacityVector retainedPayload(final long payloadBytes) {
        return payloadCharge(null, CapacityDimension.RETAINED_BYTES, payloadBytes);
    }

    private static CapacityVector payloadCharge(
            final CapacityDimension records, final CapacityDimension bytes, final long amount) {
        requireLength(amount);
        final long[] result = new long[CapacityDimension.COUNT];
        if (records != null) {
            result[records.wireValue() - 1] = 1;
        }
        result[bytes.wireValue() - 1] = amount;
        return new CapacityVector(result);
    }

    private static void requireLength(final long length) {
        if (length < 0) {
            throw new IllegalArgumentException("accounting length exceeds the local capacity range");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, schemaBundleHash);
            CanonicalProtobuf.uint32(out, 3, CanonicalScheduleIntent.QUOTA_ACCOUNTING_VERSION);
            CanonicalProtobuf.uint64(out, 4, recordOverheadBytes);
            CanonicalProtobuf.uint64(out, 5, kafkaEnvelopeBytes);
            CanonicalProtobuf.uint64(out, 6, pulsarEnvelopeBytes);
            CanonicalProtobuf.uint64(out, 7, minimumRecordCostBytes);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 8, digest);
        });
    }

    public static TargetQuotaAccounting decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "TargetQuotaAccounting");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "TargetQuotaAccounting");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION
                || QueryCodecSupport.uint32(fields.get(2), 3) != CanonicalScheduleIntent.QUOTA_ACCOUNTING_VERSION) {
            throw new IllegalArgumentException("unknown Target accounting/input version");
        }
        final var result = new TargetQuotaAccounting(
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                QueryCodecSupport.uint(fields.get(3), 4),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.uint(fields.get(5), 6),
                QueryCodecSupport.uint(fields.get(6), 7));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.get(7), 8, 32))) {
            throw new IllegalArgumentException("Target accounting artifact digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaAccounting");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaAccounting that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
