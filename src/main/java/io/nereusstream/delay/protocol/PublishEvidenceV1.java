package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical evidence envelope shared by Publish Outcome, Evidence Resolution
 * and DLQ Export Result mutations.
 *
 * <p>This codec closes the Registry envelope and branch shape.  Provider
 * ownership, authenticated response proofs and retention are deliberately
 * checked by the adapter/service layer that creates the branch.</p>
 */
public final class PublishEvidenceV1 {
    public static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-publish-evidence-v1\0");

    private final PublishEvidenceKindV1 evidenceKind;
    private final EvidenceVerificationStatusV1 verificationStatus;
    private final byte[] evidenceId;
    private final int branchField;
    private final byte[] branch;

    private PublishEvidenceV1(final PublishEvidenceKindV1 evidenceKind,
                              final EvidenceVerificationStatusV1 verificationStatus,
                              final byte[] evidenceId, final int branchField, final byte[] branch) {
        this.evidenceKind = Objects.requireNonNull(evidenceKind, "evidenceKind");
        this.verificationStatus = Objects.requireNonNull(verificationStatus, "verificationStatus");
        this.evidenceId = nonZero(evidenceId, "evidenceId");
        if (branchField != evidenceKind.wireValue() + 9) {
            throw new IllegalArgumentException("PublishEvidence branch does not match evidence kind");
        }
        this.branchField = branchField;
        this.branch = nested(branch, "PublishEvidence branch");
        validateBranch(evidenceKind, verificationStatus, this.branch);
    }

    /** Creates an envelope and derives its evidence ID from the canonical branch. */
    public static PublishEvidenceV1 create(final PublishEvidenceKindV1 evidenceKind,
                                           final EvidenceVerificationStatusV1 verificationStatus,
                                           final byte[] branch) {
        Objects.requireNonNull(evidenceKind, "evidenceKind");
        Objects.requireNonNull(verificationStatus, "verificationStatus");
        final byte[] canonicalBranch = nested(branch, "PublishEvidence branch");
        validateBranch(evidenceKind, verificationStatus, canonicalBranch);
        final byte[] evidenceId = digest(evidenceKind, verificationStatus, canonicalBranch);
        return new PublishEvidenceV1(evidenceKind, verificationStatus, evidenceId,
                evidenceKind.wireValue() + 9, canonicalBranch);
    }

    public PublishEvidenceKindV1 evidenceKind() {
        return evidenceKind;
    }

    public EvidenceVerificationStatusV1 verificationStatus() {
        return verificationStatus;
    }

    public byte[] evidenceId() {
        return Bytes.copy(evidenceId);
    }

    public int branchField() {
        return branchField;
    }

    public byte[] branch() {
        return Bytes.copy(branch);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, evidenceKind.wireValue());
            CanonicalProtobuf.uint32(output, 2, verificationStatus.wireValue());
            CanonicalProtobuf.bytes(output, 3, evidenceId);
            CanonicalProtobuf.bytes(output, branchField, branch);
        });
    }

    public static PublishEvidenceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PublishEvidenceV1");
        if (fields.size() != 4 || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("PublishEvidenceV1 must contain fields 1,2,3 and one branch");
        }
        final PublishEvidenceKindV1 kind = PublishEvidenceKindV1.fromWire(
                QueryCodecSupport.uint(fields.get(0), 1));
        final EvidenceVerificationStatusV1 status = EvidenceVerificationStatusV1.fromWire(
                QueryCodecSupport.uint(fields.get(1), 2));
        final byte[] evidenceId = QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH);
        final int branchField = fields.get(3).number();
        if (branchField != kind.wireValue() + 9) {
            throw new IllegalArgumentException("PublishEvidence branch does not match evidence kind");
        }
        final byte[] branch = QueryCodecSupport.nested(fields.get(3), branchField);
        final PublishEvidenceV1 result = new PublishEvidenceV1(kind, status, evidenceId, branchField, branch);
        if (!Arrays.equals(evidenceId, digest(kind, status, branch))) {
            throw new IllegalArgumentException("PublishEvidence evidenceId mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublishEvidenceV1");
        return result;
    }

    /** Requires a definitive business outcome owned by the supplied Publish Attempt. */
    public void requireBusinessMutation(final byte[] publishAttemptId, final boolean published) {
        requireOwner(ExternalDeliveryIdentityV1.Kind.PUBLISH_ATTEMPT, publishAttemptId, published);
    }

    /** Requires a definitive DLQ outcome owned by the supplied export identity. */
    public void requireDlqMutation(final byte[] dlqExportId, final boolean published) {
        requireOwner(ExternalDeliveryIdentityV1.Kind.DLQ_EXPORT, dlqExportId, published);
    }

    private void requireOwner(final ExternalDeliveryIdentityV1.Kind ownerKind, final byte[] ownerId,
                              final boolean published) {
        final EvidenceVerificationStatusV1 expected = published
                ? EvidenceVerificationStatusV1.VERIFIED_PUBLISHED
                : EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED;
        if (verificationStatus != expected) {
            throw new IllegalArgumentException("PublishEvidence verification status does not match side effect");
        }
        final ExternalDeliveryIdentityV1 identity = ExternalDeliveryIdentityV1.decode(
                externalIdentityBytes(branchField, branch));
        if (identity.kind() != ownerKind || !Arrays.equals(identity.identity(), ownerId)) {
            throw new IllegalArgumentException("PublishEvidence external identity does not match owner");
        }
    }

    private static byte[] externalIdentityBytes(final int branchField, final byte[] branch) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(branch,
                "PublishEvidence branch");
        final int field = switch (branchField) {
            case 10 -> 6;
            case 11, 12, 15 -> 3;
            case 13 -> 9;
            case 14 -> 5;
            case 16 -> 9;
            case 17 -> 2;
            case 18 -> 2;
            case 19 -> 4;
            default -> throw new IllegalArgumentException("unknown PublishEvidence branch field");
        };
        return QueryCodecSupport.nested(QueryCodecSupport.field(fields, field), field);
    }

    private static void validateBranch(final PublishEvidenceKindV1 kind,
                                       final EvidenceVerificationStatusV1 status,
                                       final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                kind + " evidence branch");
        switch (kind) {
            case KAFKA_PRODUCE_ACK -> validateKafkaAck(fields);
            case KAFKA_TRANSACTIONAL_RECEIPT -> validateKafkaTransactional(fields);
            case KAFKA_RECEIPT_ABSENCE -> validateKafkaAbsence(fields);
            case PULSAR_SEND_ACK -> validatePulsarAck(fields);
            case PULSAR_ATTEMPT_JOURNAL -> validatePulsarJournal(fields);
            case PULSAR_JOURNAL_ABSENCE -> validatePulsarAbsence(fields);
            case BROKER_RESOURCE_GUARD_REJECTION -> validateGuardRejection(fields);
            case OPERATOR_ATTESTATION -> validateOperator(fields, status);
            case ADAPTER_NON_SUBMISSION -> validateAdapterNonSubmission(fields);
            case BROKER_DEFINITIVE_REJECTION -> validateBrokerRejection(fields);
        }
        validateStatusSemantics(kind, status);
    }

    private static void validateKafkaAck(final List<CanonicalProtobuf.Reader.Field> fields) {
        if (fields.size() == 7) {
            requireNumbers(fields, new int[]{1, 2, 3, 5, 6, 7, 8});
        } else if (fields.size() == 8) {
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8});
            uint(fields, 4);
        } else {
            throw new IllegalArgumentException("KafkaProduceAckEvidenceV1 has an unexpected field count");
        }
        BrokerResourceIdentityV1.decode(nested(fields, 1));
        uint(fields, 2);
        uint(fields, 3);
        uint(fields, 5);
        ExternalDeliveryIdentityV1.decode(nested(fields, 6));
        fixed(fields, 7);
        fixed(fields, 8);
    }

    private static void validateKafkaTransactional(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8});
        EvidenceCursorV1.decode(nested(fields, 1));
        uint(fields, 2);
        ExternalDeliveryIdentityV1.decode(nested(fields, 3));
        fixed(fields, 4);
        BrokerResourceIdentityV1.decode(nested(fields, 5));
        uint(fields, 6);
        fixed(fields, 7);
        fixed(fields, 8);
    }

    private static void validateKafkaAbsence(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5});
        EvidenceCursorV1.decode(nested(fields, 1));
        ChannelResourceIdentityV1.decode(nested(fields, 2));
        ExternalDeliveryIdentityV1.decode(nested(fields, 3));
        fixed(fields, 4);
        fixed(fields, 5);
    }

    private static void validatePulsarAck(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
        BrokerResourceIdentityV1.decode(nested(fields, 1));
        uint(fields, 2);
        uint(fields, 3);
        uint(fields, 4);
        uint(fields, 5);
        uint(fields, 6);
        fixed(fields, 7);
        uint(fields, 8);
        ExternalDeliveryIdentityV1.decode(nested(fields, 9));
        fixed(fields, 10);
        fixed(fields, 11);
    }

    private static void validatePulsarJournal(final List<CanonicalProtobuf.Reader.Field> fields) {
        if (fields.size() == 9) {
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        } else if (fields.size() == 10) {
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
            fixed(fields, 10);
        } else {
            throw new IllegalArgumentException("PulsarAttemptJournalEvidenceV1 has an unexpected field count");
        }
        EvidenceCursorV1.decode(nested(fields, 1));
        uint(fields, 2);
        uint(fields, 3);
        uint(fields, 4);
        ExternalDeliveryIdentityV1.decode(nested(fields, 5));
        fixed(fields, 6);
        fixed(fields, 7);
        uint(fields, 8);
        fixed(fields, 9);
    }

    private static void validatePulsarAbsence(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7});
        EvidenceCursorV1.decode(nested(fields, 1));
        ChannelResourceIdentityV1.decode(nested(fields, 2));
        ExternalDeliveryIdentityV1.decode(nested(fields, 3));
        fixed(fields, 4);
        fixed(fields, 5);
        uint(fields, 6);
        fixed(fields, 7);
    }

    private static void validateGuardRejection(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        BrokerResourceIdentityV1.decode(nested(fields, 1));
        uint(fields, 2);
        if (uint(fields, 3) < 1 || uint(fields, 3) > 6) {
            throw new IllegalArgumentException("invalid GuardOperationV1");
        }
        if (uint(fields, 4) == 0) {
            throw new IllegalArgumentException("guard attestation generation must be positive");
        }
        fixed(fields, 5);
        fixed(fields, 6);
        StableCode.fromWire(boundedInt(uint(fields, 7), "guard code"));
        fixed(fields, 8);
        ExternalDeliveryIdentityV1.decode(nested(fields, 9));
        fixed(fields, 10);
    }

    private static void validateOperator(final List<CanonicalProtobuf.Reader.Field> fields,
                                         final EvidenceVerificationStatusV1 status) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
        ProfileRefV1.decode(nested(fields, 1));
        ExternalDeliveryIdentityV1.decode(nested(fields, 2));
        fixed(fields, 3);
        BrokerResourceIdentityV1.decode(nested(fields, 4));
        uint(fields, 5);
        if (EvidenceVerificationStatusV1.fromWire(uint(fields, 6)) != status) {
            throw new IllegalArgumentException("operator evidence status mismatch");
        }
        uint(fields, 7);
        uint(fields, 8);
        fixed(fields, 9);
        uint(fields, 10);
        Bytes.requireLength(QueryCodecSupport.bytes(fields.get(10), 11), 64, "operator signature");
    }

    private static void validateAdapterNonSubmission(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7});
        ChannelResourceIdentityV1.decode(nested(fields, 1));
        ExternalDeliveryIdentityV1.decode(nested(fields, 2));
        fixed(fields, 3);
        if (uint(fields, 4) < 1 || uint(fields, 4) > 2) {
            throw new IllegalArgumentException("invalid AdapterNonSubmissionKindV1");
        }
        fixed(fields, 5);
        if (uint(fields, 6) == 0) {
            throw new IllegalArgumentException("adapter conformance version must be positive");
        }
        StableCode.fromWire(boundedInt(uint(fields, 7), "adapter stable code"));
    }

    private static void validateBrokerRejection(final List<CanonicalProtobuf.Reader.Field> fields) {
        requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        AdapterKindV1.fromWire(uint(fields, 1));
        BrokerResourceIdentityV1.decode(nested(fields, 2));
        uint(fields, 3);
        ExternalDeliveryIdentityV1.decode(nested(fields, 4));
        fixed(fields, 5);
        fixed(fields, 6);
        uint(fields, 7);
        fixed(fields, 8);
        if (uint(fields, 9) == 0) {
            throw new IllegalArgumentException("rejection classifier version must be positive");
        }
    }

    private static void validateStatusSemantics(final PublishEvidenceKindV1 kind,
                                                final EvidenceVerificationStatusV1 status) {
        if (status == EvidenceVerificationStatusV1.UNRESOLVED) {
            return;
        }
        final EvidenceVerificationStatusV1 expected = switch (kind) {
            case KAFKA_PRODUCE_ACK, KAFKA_TRANSACTIONAL_RECEIPT, PULSAR_SEND_ACK, PULSAR_ATTEMPT_JOURNAL ->
                    EvidenceVerificationStatusV1.VERIFIED_PUBLISHED;
            case KAFKA_RECEIPT_ABSENCE, PULSAR_JOURNAL_ABSENCE, BROKER_RESOURCE_GUARD_REJECTION,
                    ADAPTER_NON_SUBMISSION, BROKER_DEFINITIVE_REJECTION ->
                    EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED;
            case OPERATOR_ATTESTATION -> status;
        };
        if (status != expected) {
            throw new IllegalArgumentException("PublishEvidence status does not match branch kind");
        }
    }

    private static byte[] digest(final PublishEvidenceKindV1 kind,
                                 final EvidenceVerificationStatusV1 status, final byte[] branch) {
        return Bytes.sha256(DIGEST_DOMAIN, Bytes.u16be(kind.wireValue()), Bytes.u16be(status.wireValue()), branch);
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] expected) {
        QueryCodecSupport.requireNumbers(fields, expected, "PublishEvidence branch");
    }

    private static byte[] nested(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        return QueryCodecSupport.nested(QueryCodecSupport.field(fields, number), number);
    }

    private static long uint(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        return QueryCodecSupport.uint(QueryCodecSupport.field(fields, number), number);
    }

    private static byte[] fixed(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        return QueryCodecSupport.fixed(QueryCodecSupport.field(fields, number), number, HASH_LENGTH);
    }

    private static int boundedInt(final long value, final String name) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " exceeds runtime range");
        }
        return (int) value;
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte current : value) {
            if (current != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static byte[] nested(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        QueryCodecSupport.read(value, name);
        return Bytes.copy(value);
    }
}
