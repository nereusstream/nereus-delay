package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Semantic parser for the closed {@code DLQ_EXPORT_RESULT_V1} body.
 *
 * <p>This class validates the identity, event, result-state and retry-domain
 * subset that can be checked without a live DLQ adapter.  Evidence branch
 * ownership and provider-specific proof remain adapter/service concerns.</p>
 */
public final class DlqExportResultBody {
    private static final int HASH_LENGTH = 32;

    private final byte[] dlqExportId;
    private final byte[] messageId;
    private final int generation;
    private final long terminalRevision;
    private final byte[] exportEnvelopeHash;
    private final int eventKind;
    private final int sideEffect;
    private final int disposition;
    private final StableCode stableCode;
    private final byte[] evidence;
    private final byte[] transfer;
    private final TrustedUtcIntervalEvidence observedAt;
    private final byte[] retryDecision;
    private final DlqExportStateV1 resultingState;
    private final int physicalAttemptNo;

    private DlqExportResultBody(final byte[] dlqExportId, final byte[] messageId, final int generation,
                                final long terminalRevision, final byte[] exportEnvelopeHash, final int eventKind,
                                final int sideEffect, final int disposition, final StableCode stableCode,
                                final byte[] evidence, final byte[] transfer,
                                final TrustedUtcIntervalEvidence observedAt, final byte[] retryDecision,
                                final DlqExportStateV1 resultingState, final int physicalAttemptNo) {
        this.dlqExportId = fixed(dlqExportId, "dlqExportId");
        this.messageId = fixed(messageId, DelayMessageId.LENGTH, "messageId");
        if (generation < 0 || terminalRevision <= 0) {
            throw new IllegalArgumentException("invalid DLQ export generation/revision");
        }
        this.generation = generation;
        this.terminalRevision = terminalRevision;
        this.exportEnvelopeHash = fixed(exportEnvelopeHash, "exportEnvelopeHash");
        if (eventKind < 1 || eventKind > 2) {
            throw new IllegalArgumentException("invalid DLQ export event kind");
        }
        if (sideEffect < 1 || sideEffect > 3 || disposition < 0 || disposition > 5) {
            throw new IllegalArgumentException("invalid DLQ export side effect/disposition");
        }
        this.eventKind = eventKind;
        this.sideEffect = sideEffect;
        this.disposition = disposition;
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.evidence = copy(evidence);
        this.transfer = copy(transfer);
        this.observedAt = Objects.requireNonNull(observedAt, "observedAt");
        this.retryDecision = copy(retryDecision);
        this.resultingState = Objects.requireNonNull(resultingState, "resultingState");
        if (resultingState == DlqExportStateV1.NOT_CONFIGURED || physicalAttemptNo <= 0) {
            throw new IllegalArgumentException("DLQ export result cannot target NOT_CONFIGURED or attempt zero");
        }
        this.physicalAttemptNo = physicalAttemptNo;
    }

    public static DlqExportResultBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields = SystemMutationBodyCodec.fields(
                SystemMutationType.DLQ_EXPORT_RESULT, canonicalBody);
        final byte[] exportId = fixed(field(fields, 10), 10, HASH_LENGTH);
        final byte[] messageId = fixed(field(fields, 11), 11, DelayMessageId.LENGTH);
        final int generation = boundedInt(unsigned(field(fields, 12), 12), "generation");
        final long terminalRevision = unsigned(field(fields, 13), 13);
        if (terminalRevision <= 0) {
            throw new IllegalArgumentException("terminal revision must be positive");
        }
        final byte[] envelopeHash = fixed(field(fields, 14), 14, HASH_LENGTH);
        final int eventKind = boundedInt(unsigned(field(fields, 15), 15), "eventKind");
        final int sideEffect = boundedInt(unsigned(field(fields, 16), 16), "sideEffect");
        final int disposition = boundedInt(unsigned(field(fields, 17), 17), "disposition");
        final StableCode stableCode = StableCode.fromWire(boundedInt(unsigned(field(fields, 18), 18), "stableCode"));
        final byte[] evidence = optionalNested(fields, 19);
        final PublishEvidenceV1 evidenceValue = evidence.length == 0 ? null : PublishEvidenceV1.decode(evidence);
        final byte[] transfer = nested(field(fields, 20), 20);
        validateChargeVector(transfer);
        final TrustedUtcIntervalEvidence observedAt = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 21), 21));
        final byte[] retryDecision = nested(field(fields, 22), 22);
        final RetryShape retry = validateRetryDecision(retryDecision);
        final DlqExportStateV1 resultingState = DlqExportStateV1.fromWire(unsigned(field(fields, 23), 23));
        final int physicalAttemptNo = boundedInt(unsigned(field(fields, 24), 24), "physicalAttemptNo");
        validateCombination(eventKind, sideEffect, disposition, stableCode, evidence, resultingState, retry);
        final DlqExportResultBody result = new DlqExportResultBody(exportId, messageId, generation,
                terminalRevision, envelopeHash, eventKind, sideEffect, disposition, stableCode, evidence, transfer,
                observedAt, retryDecision, resultingState, physicalAttemptNo);
        if (evidenceValue != null) {
            evidenceValue.requireDlqMutation(exportId, sideEffect == 1);
        }
        return result;
    }

    public byte[] dlqExportId() {
        return copy(dlqExportId);
    }

    public byte[] messageId() {
        return copy(messageId);
    }

    public int generation() {
        return generation;
    }

    public long terminalRevision() {
        return terminalRevision;
    }

    public byte[] exportEnvelopeHash() {
        return copy(exportEnvelopeHash);
    }

    public int eventKind() {
        return eventKind;
    }

    public int sideEffect() {
        return sideEffect;
    }

    public int disposition() {
        return disposition;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public byte[] evidence() {
        return copy(evidence);
    }

    public byte[] transfer() {
        return copy(transfer);
    }

    public TrustedUtcIntervalEvidence observedAt() {
        return observedAt;
    }

    public byte[] retryDecision() {
        return copy(retryDecision);
    }

    public DlqExportStateV1 resultingState() {
        return resultingState;
    }

    public int physicalAttemptNo() {
        return physicalAttemptNo;
    }

    /** Returns the evidence ID from the canonical PublishEvidence branch. */
    public byte[] evidenceId() {
        return evidenceId(evidence);
    }

    /** Returns the Registry logical identity for this result event. */
    public byte[] logicalOperationIdentity() {
        return eventKind == 1
                ? SystemMutation.computeDlqExportAttemptLogicalIdentity(dlqExportId, physicalAttemptNo)
                : SystemMutation.computeDlqExportEvidenceLogicalIdentity(dlqExportId, evidenceId());
    }

    private static void validateCombination(final int eventKind, final int sideEffect, final int disposition,
                                            final StableCode stableCode, final byte[] evidence,
                                            final DlqExportStateV1 resultingState, final RetryShape retry) {
        if (eventKind == 2 && sideEffect == 3) {
            throw new IllegalArgumentException("evidence resolution cannot remain UNKNOWN");
        }
        if (sideEffect == 1) {
            if (disposition != 0 || stableCode != StableCode.OK || evidence.length == 0
                    || resultingState != DlqExportStateV1.PUBLISHED || retry.kind() != 1) {
                throw new IllegalArgumentException("invalid PUBLISHED DLQ export combination");
            }
            return;
        }
        if (sideEffect == 2) {
            if (disposition == 0 || evidence.length == 0
                    || (resultingState != DlqExportStateV1.PENDING
                    && resultingState != DlqExportStateV1.FAILED_PERMANENT)) {
                throw new IllegalArgumentException("invalid NOT_PUBLISHED DLQ export combination");
            }
            if (resultingState == DlqExportStateV1.PENDING && retry.kind() != 2 && retry.kind() != 4) {
                throw new IllegalArgumentException("pending DLQ export requires a scheduled retry decision");
            }
            if (resultingState == DlqExportStateV1.FAILED_PERMANENT && retry.kind() != 1 && retry.kind() != 3) {
                throw new IllegalArgumentException("permanent DLQ export requires NONE or EXHAUSTED retry");
            }
            return;
        }
        if (disposition == 0 || stableCode != StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN || evidence.length != 0
                || (resultingState != DlqExportStateV1.PENDING
                && resultingState != DlqExportStateV1.UNCERTAIN)) {
            throw new IllegalArgumentException("invalid UNKNOWN DLQ export combination");
        }
        if (resultingState == DlqExportStateV1.PENDING && retry.kind() != 2) {
            throw new IllegalArgumentException("unknown DLQ export retry must be scheduled");
        }
        if (resultingState == DlqExportStateV1.UNCERTAIN && retry.kind() != 5) {
            throw new IllegalArgumentException("unknown DLQ export hold must use UNCERTAIN_HOLD");
        }
    }

    private static RetryShape validateRetryDecision(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "RetryDecision");
        if (fields.size() != 8 && fields.size() != 9) {
            throw new IllegalArgumentException("RetryDecision has unexpected field count");
        }
        final int kind = boundedInt(unsigned(field(fields, 1), 1), "retry kind");
        if (kind < 1 || kind > 5) {
            throw new IllegalArgumentException("invalid retry decision kind");
        }
        nested(field(fields, 2), 2);
        unsigned(field(fields, 3), 3);
        unsigned(field(fields, 4), 4);
        unsigned(field(fields, 5), 5);
        final boolean hasNext = fields.stream().anyMatch(value -> value.number() == 6);
        if ((kind == 2 || kind == 4) != hasNext) {
            throw new IllegalArgumentException("retry next-at presence does not match retry kind");
        }
        if (hasNext) {
            unsigned(field(fields, 6), 6);
        }
        if (unsigned(field(fields, 7), 7) != 1) {
            throw new IllegalArgumentException("unsupported retry jitter algorithm");
        }
        StableCode.fromWire(boundedInt(unsigned(field(fields, 8), 8), "retry cause"));
        if (unsigned(field(fields, 9), 9) != 2) {
            throw new IllegalArgumentException("DLQ result must use DLQ_EXPORT retry domain");
        }
        return new RetryShape(kind);
    }

    private static void validateChargeVector(final byte[] encoded) {
        PublishAdmissionBody.ChargeVector.decodeCanonical(encoded);
    }

    private static byte[] evidenceId(final byte[] encoded) {
        return PublishEvidenceV1.decode(encoded).evidenceId();
    }

    private static byte[] optionalNested(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return nested(field, number);
            }
        }
        return new byte[0];
    }

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                        final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing DLQ export body field " + number);
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid DLQ export scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "DLQ export body field " + number);
        return value;
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException("nested DLQ export field " + number + " must not be empty");
        }
        read(value, "nested DLQ export field " + number);
        return value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid DLQ export bytes field " + number);
        }
        return field.rawValue();
    }

    private static int boundedInt(final long value, final String name) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds runtime range");
        }
        return (int) value;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte current : value) {
            if (current != 0) {
                return copy(value);
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return copy(value);
    }

    private static byte[] copy(final byte[] value) {
        return Bytes.copy(Objects.requireNonNull(value, "value"));
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private record RetryShape(int kind) {
    }
}
