package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Semantic parser for the definitive NOT_PUBLISHED Publish Outcome subset. */
public final class PublishOutcomeBody {
    private static final int HASH_LENGTH = 32;

    private final byte[] publishAttemptId;
    private final int sideEffect;
    private final int disposition;
    private final StableCode stableCode;
    private final byte[] evidence;
    private final byte[] transfer;
    private final TrustedUtcIntervalEvidence observedAt;
    private final RetryDecision retryDecision;

    private PublishOutcomeBody(final byte[] publishAttemptId, final int sideEffect, final int disposition,
                               final StableCode stableCode, final byte[] evidence, final byte[] transfer,
                               final TrustedUtcIntervalEvidence observedAt, final RetryDecision retryDecision) {
        this.publishAttemptId = fixed(publishAttemptId, "publishAttemptId");
        this.sideEffect = sideEffect;
        this.disposition = disposition;
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.evidence = copy(evidence);
        this.transfer = copy(transfer);
        this.observedAt = Objects.requireNonNull(observedAt, "observedAt");
        this.retryDecision = Objects.requireNonNull(retryDecision, "retryDecision");
    }

    /**
     * Encodes the canonical body of an initial {@code PUBLISH_OUTCOME_V1}.
     * The body is decoded again before it is returned so callers cannot emit a
     * field-shape or side-effect combination that this local V1 parser would
     * reject.  Signing, Shard Log enqueue and source-ordered apply remain
     * outside this codec.
     */
    public static byte[] encodeInitial(final ShardId shardId, final long retryUntilEpochMs,
                                       final byte[] publishAttemptId, final int sideEffect,
                                       final int disposition, final StableCode stableCode,
                                       final byte[] evidence, final byte[] transfer,
                                       final TrustedUtcIntervalEvidence observedAt,
                                       final byte[] retryDecision) {
        final byte[] encoded = encodeCommon(shardId, retryUntilEpochMs, SystemMutationType.PUBLISH_OUTCOME,
                output -> {
                    CanonicalProtobuf.bytes(output, 10, fixed(publishAttemptId, "publishAttemptId"));
                    CanonicalProtobuf.uint32(output, 11, sideEffect);
                    CanonicalProtobuf.uint32(output, 12, disposition);
                    CanonicalProtobuf.uint32(output, 13, Objects.requireNonNull(stableCode, "stableCode").wireValue());
                    if (evidence != null && evidence.length != 0) {
                        CanonicalProtobuf.bytes(output, 14, evidence);
                    }
                    CanonicalProtobuf.bytes(output, 15, nested(transfer, "transfer"));
                    CanonicalProtobuf.bytes(output, 16, Objects.requireNonNull(observedAt, "observedAt")
                            .canonicalBytes());
                    CanonicalProtobuf.bytes(output, 17, nested(retryDecision, "retryDecision"));
                });
        decode(encoded);
        return encoded;
    }

    /** Encodes the canonical body of a verified {@code EVIDENCE_RESOLUTION_V1}. */
    public static byte[] encodeEvidenceResolution(final ShardId shardId, final long retryUntilEpochMs,
                                                   final byte[] publishAttemptId,
                                                   final EvidenceCursorV1 evidenceCursor,
                                                   final byte[] evidence, final StableCode stableCode,
                                                   final int sideEffect, final int disposition,
                                                   final byte[] transfer,
                                                   final TrustedUtcIntervalEvidence observedAt,
                                                   final byte[] retryDecision) {
        final byte[] encoded = encodeCommon(shardId, retryUntilEpochMs, SystemMutationType.EVIDENCE_RESOLUTION,
                output -> {
                    CanonicalProtobuf.bytes(output, 10, fixed(publishAttemptId, "publishAttemptId"));
                    CanonicalProtobuf.bytes(output, 11, Objects.requireNonNull(evidenceCursor, "evidenceCursor")
                            .canonicalBytes());
                    CanonicalProtobuf.bytes(output, 12, nonEmpty(evidence, "evidence"));
                    CanonicalProtobuf.uint32(output, 13, Objects.requireNonNull(stableCode, "stableCode").wireValue());
                    CanonicalProtobuf.uint32(output, 14, sideEffect);
                    CanonicalProtobuf.uint32(output, 15, disposition);
                    CanonicalProtobuf.bytes(output, 16, nested(transfer, "transfer"));
                    CanonicalProtobuf.bytes(output, 17, Objects.requireNonNull(observedAt, "observedAt")
                            .canonicalBytes());
                    CanonicalProtobuf.bytes(output, 18, nested(retryDecision, "retryDecision"));
                });
        decodeEvidenceResolution(encoded);
        return encoded;
    }

    /** Parses all common fields and strictly validates the definitive PUBLISHED/NOT_PUBLISHED branches. */
    public static PublishOutcomeBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.PUBLISH_OUTCOME, canonicalBody);
        final int sideEffect = intValue(field(fields, 11), 11);
        final int disposition = intValue(field(fields, 12), 12);
        if (sideEffect < 1 || sideEffect > 3 || disposition < 0 || disposition > 4) {
            throw new IllegalArgumentException("invalid publish outcome side effect/disposition");
        }
        final StableCode stableCode = StableCode.fromWire(intValue(field(fields, 13), 13));
        final byte[] attemptId = bytes(field(fields, 10), 10);
        final byte[] evidence = optionalNested(fields, 14);
        final byte[] transfer = nested(field(fields, 15), 15);
        // UNKNOWN is intentionally an evidence-only branch.  Older producers
        // persisted an opaque transfer placeholder here, so do not apply the
        // definitive ChargeVector schema to that branch.
        if (sideEffect != 3) {
            validateChargeVector(transfer);
        }
        final TrustedUtcIntervalEvidence observedAt = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 16), 16));
        final byte[] retryBytes = nested(field(fields, 17), 17);
        final RetryDecision retryDecision = sideEffect == 3
                ? RetryDecision.decodeUnknown(retryBytes) : RetryDecision.decode(retryBytes);
        if (sideEffect == 1 || sideEffect == 2) {
            PublishEvidenceV1.decode(evidence).requireBusinessMutation(attemptId, sideEffect == 1);
            validateDefinitiveCombination(sideEffect, disposition, stableCode, evidence, retryDecision);
        } else if (disposition == 0 || stableCode == StableCode.OK || evidence.length != 0) {
            throw new IllegalArgumentException("invalid UNKNOWN outcome combination");
        }
        return new PublishOutcomeBody(attemptId, sideEffect, disposition, stableCode, evidence,
                transfer, observedAt, retryDecision);
    }

    /**
     * Parses the verified published/not-published subset of an Evidence Resolution. The cursor and evidence
     * messages are required and canonical, but adapter-specific semantic fields remain outside this subset.
     */
    public static PublishOutcomeBody decodeEvidenceResolution(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.EVIDENCE_RESOLUTION, canonicalBody);
        EvidenceCursorV1.decode(nested(field(fields, 11), 11));
        final byte[] attemptId = bytes(field(fields, 10), 10);
        final byte[] evidence = nested(field(fields, 12), 12);
        final StableCode stableCode = StableCode.fromWire(intValue(field(fields, 13), 13));
        final int sideEffect = intValue(field(fields, 14), 14);
        final int disposition = intValue(field(fields, 15), 15);
        final byte[] transfer = nested(field(fields, 16), 16);
        validateChargeVector(transfer);
        final TrustedUtcIntervalEvidence observedAt = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 17), 17));
        final RetryDecision retryDecision = RetryDecision.decode(nested(field(fields, 18), 18));
        if (sideEffect != 1 && sideEffect != 2) {
            throw new IllegalArgumentException("only verified Evidence Resolution outcomes are implemented");
        }
        PublishEvidenceV1.decode(evidence).requireBusinessMutation(attemptId, sideEffect == 1);
        validateDefinitiveCombination(sideEffect, disposition, stableCode, evidence, retryDecision);
        return new PublishOutcomeBody(attemptId, sideEffect, disposition, stableCode, evidence,
                transfer, observedAt, retryDecision);
    }

    public byte[] publishAttemptId() {
        return copy(publishAttemptId);
    }

    /** Returns the registered logical identity for an initial Publish Outcome. */
    public byte[] initialLogicalOperationIdentity() {
        return copy(publishAttemptId);
    }

    /** Returns the registered logical identity for an Evidence Resolution. */
    public byte[] evidenceResolutionLogicalOperationIdentity() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-evidence-resolution-logical-id-v1\0"),
                publishAttemptId, PublishEvidenceV1.decode(evidence).evidenceId());
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

    public RetryDecision retryDecision() {
        return retryDecision;
    }

    private static void validateChargeVector(final byte[] encoded) {
        PublishAdmissionBody.ChargeVector.decodeCanonical(encoded);
    }

    private static void validateDefinitiveCombination(final int sideEffect, final int disposition,
                                                       final StableCode stableCode, final byte[] evidence,
                                                       final RetryDecision retryDecision) {
        if (evidence.length == 0) {
            throw new IllegalArgumentException("definitive publish outcome requires evidence");
        }
        if (sideEffect == 1) {
            if (disposition != 0 || stableCode != StableCode.OK || retryDecision.kind() != 1) {
                throw new IllegalArgumentException("invalid PUBLISHED outcome combination");
            }
        } else if (disposition == 0 || disposition > 3 || stableCode == StableCode.OK) {
            throw new IllegalArgumentException("invalid NOT_PUBLISHED outcome combination");
        } else {
            retryDecision.requireFor(disposition);
        }
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
        throw new IllegalArgumentException("missing Publish Outcome field " + number);
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid Publish Outcome scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Publish Outcome scalar exceeds runtime range");
        }
        return (int) value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Publish Outcome bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException("nested Publish Outcome field must not be empty");
        }
        read(value, "nested Publish Outcome field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return copy(value);
    }

    private static byte[] nested(final byte[] value, final String name) {
        return nonEmpty(value, name);
    }

    private static byte[] encodeCommon(final ShardId shardId, final long retryUntilEpochMs,
                                       final SystemMutationType type,
                                       final CanonicalProtobuf.FieldWriter fields) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(type, "type");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntilEpochMs must be non-negative");
        }
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shardId.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, shardId.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, type.wireValue());
            CanonicalProtobuf.int64(output, 3, retryUntilEpochMs);
            fields.writeTo(output);
        });
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

    public static final class RetryDecision {
        private final byte[] canonicalBytes;
        private final int kind;
        private final long completedAttemptNo;
        private final long firstAttemptAt;
        private final long retryDeadline;
        private final Long nextRetryAt;

        private RetryDecision(final byte[] canonicalBytes, final int kind, final long completedAttemptNo,
                              final long firstAttemptAt, final long retryDeadline, final Long nextRetryAt) {
            this.canonicalBytes = copy(canonicalBytes);
            this.kind = kind;
            this.completedAttemptNo = completedAttemptNo;
            this.firstAttemptAt = firstAttemptAt;
            this.retryDeadline = retryDeadline;
            this.nextRetryAt = nextRetryAt;
        }

        private static RetryDecision decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "RetryDecision");
            if (fields.size() != 8 && fields.size() != 9) {
                throw new IllegalArgumentException("RetryDecision has unexpected field count");
            }
            final int kind = intValue(field(fields, 1), 1);
            if (kind < 1 || kind > 5) {
                throw new IllegalArgumentException("invalid RetryDecision kind");
            }
            final byte[] policy = nested(field(fields, 2), 2);
            validateRetryPolicyRef(policy);
            final long completed = unsigned(field(fields, 3), 3);
            final long first = unsigned(field(fields, 4), 4);
            final long deadline = unsigned(field(fields, 5), 5);
            final boolean hasNext = fields.stream().anyMatch(field -> field.number() == 6);
            final Long next = hasNext ? unsigned(field(fields, 6), 6) : null;
            if ((kind == 2 || kind == 4) != hasNext || next != null && next < first) {
                throw new IllegalArgumentException("RetryDecision next retry presence/timing mismatch");
            }
            if (next != null && next > deadline) {
                throw new IllegalArgumentException("RetryDecision next retry exceeds deadline");
            }
            if (intValue(field(fields, 7), 7) != 1) {
                throw new IllegalArgumentException("unsupported retry jitter algorithm");
            }
            StableCode.fromWire(intValue(field(fields, 8), 8));
            if (intValue(field(fields, 9), 9) != 1) {
                throw new IllegalArgumentException("unsupported retry domain");
            }
            return new RetryDecision(encoded, kind, completed, first, deadline, next);
        }

        private static RetryDecision unchecked(final byte[] encoded) {
            return new RetryDecision(encoded, 1, 0, 0, Long.MAX_VALUE, null);
        }

        private static RetryDecision decodeUnknown(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "RetryDecision");
            // Older UNKNOWN producers used a bounded placeholder.  Preserve
            // that opaque branch, but strictly parse a full RetryDecision when
            // it carries more than the placeholder's single field.
            if (fields.size() == 1 && fields.get(0).number() == 1 && fields.get(0).wireType() == 2) {
                return unchecked(encoded);
            }
            return decode(encoded);
        }

        private static void validateRetryPolicyRef(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "RetryPolicyRef");
            if (fields.size() != 3) {
                throw new IllegalArgumentException("RetryPolicyRef has unexpected field count");
            }
            if (bytes(field(fields, 1), 1).length == 0 || unsigned(field(fields, 2), 2) == 0) {
                throw new IllegalArgumentException("RetryPolicyRef identity is invalid");
            }
            Bytes.requireLength(bytes(field(fields, 3), 3), HASH_LENGTH, "retry policy semantic hash");
        }

        private void requireFor(final int disposition) {
            final int expectedKind = switch (disposition) {
                case 1 -> 2;
                case 2 -> 3;
                case 3 -> 4;
                default -> throw new IllegalArgumentException("invalid NOT_PUBLISHED disposition");
            };
            if (kind != expectedKind) {
                throw new IllegalArgumentException("RetryDecision kind does not match disposition");
            }
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public int kind() {
            return kind;
        }

        public long completedAttemptNo() {
            return completedAttemptNo;
        }

        public long firstAttemptAt() {
            return firstAttemptAt;
        }

        public long retryDeadline() {
            return retryDeadline;
        }

        public long nextRetryAt() {
            if (nextRetryAt == null) {
                throw new IllegalStateException("RetryDecision has no next retry time");
            }
            return nextRetryAt;
        }

        public boolean hasNextRetryAt() {
            return nextRetryAt != null;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof RetryDecision that && kind == that.kind
                    && completedAttemptNo == that.completedAttemptNo && firstAttemptAt == that.firstAttemptAt
                    && retryDeadline == that.retryDeadline && Objects.equals(nextRetryAt, that.nextRetryAt)
                    && Arrays.equals(canonicalBytes, that.canonicalBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, completedAttemptNo, firstAttemptAt, retryDeadline, nextRetryAt,
                    Arrays.hashCode(canonicalBytes));
        }
    }
}
