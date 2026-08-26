package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.EvidenceKind;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Builds the closed Kafka transactional-receipt evidence branch from a
 * physical read_committed observation.
 *
 * <p>The cursor and receipt position are caller-supplied because only the
 * source-locked Kafka client can authenticate Fetch v13, Topic UUID and LSO.
 * This class performs the shared identity binding and refuses to turn a
 * foreign cursor or record digest into business evidence.</p>
 */
public final class KafkaTransactionalPublishEvidence {
    private static final int HASH_LENGTH = 32;

    private KafkaTransactionalPublishEvidence() {}

    /** Creates the typed VERIFIED_PUBLISHED K2 evidence envelope. */
    public static PublishEvidence published(
            final KafkaTransactionalDestinationRequest request,
            final EvidenceCursor receiptCursor,
            final long receiptOffset,
            final byte[] receiptRecordHash) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(receiptCursor, "receiptCursor");
        Bytes.requireLength(receiptRecordHash, HASH_LENGTH, "receiptRecordHash");
        if (receiptOffset == -1L) {
            throw new IllegalArgumentException("receiptOffset cannot be the exhausted uint64 value");
        }
        validateCursor(request, receiptCursor, receiptOffset);
        if (!Arrays.equals(request.canonicalReceiptRecordHash(), receiptRecordHash)) {
            throw new IllegalArgumentException("receipt record digest does not match the exact request");
        }
        final KafkaReceiptJournal.Mapping mapping = request.mapping();
        final BrokerResourceIdentity target = BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                request.target().authenticatedClusterId(), request.target().nativeTopicUuid()));
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, receiptCursor.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, receiptOffset);
            CanonicalProtobuf.bytes(
                    output,
                    3,
                    ExternalDeliveryIdentity.publishAttempt(mapping.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 6, request.target().partition());
            CanonicalProtobuf.bytes(output, 7, mapping.producer().transactionalIdentitySha256());
            CanonicalProtobuf.bytes(output, 8, receiptRecordHash);
        });
        return PublishEvidence.create(
                PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    /**
     * Verifies provider-returned evidence before a source-locked transport
     * promotes an uncertain transaction to PUBLISHED. The provider owns the
     * read_committed proof; this method owns the final request/evidence
     * identity binding.
     */
    public static void requireExactBinding(
            final PublishEvidence evidence,
            final KafkaTransactionalDestinationRequest request,
            final long receiptOffset) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(request, "request");
        if (evidence.evidenceKind() != PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT
                || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalArgumentException("Kafka transaction evidence has the wrong branch");
        }
        evidence.requireBusinessMutation(request.mapping().publishAttemptId(), true);
        if (receiptOffset < 0) {
            throw new IllegalArgumentException("receiptOffset must be non-negative");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = branchFields(evidence.branch());
        final EvidenceCursor cursor = EvidenceCursor.decode(bytes(fields.get(0), 1));
        validateCursor(request, cursor, receiptOffset);
        if (fields.get(1).unsignedValue() != receiptOffset
                || !Arrays.equals(bytes(fields.get(3), 4), request.mapping().preparedPublishHash())
                || !BrokerResourceIdentity.decode(bytes(fields.get(4), 5))
                        .equals(BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                                request.target().authenticatedClusterId(),
                                request.target().nativeTopicUuid())))
                || uint(fields.get(5), 6) != request.target().partition()
                || !Arrays.equals(
                        bytes(fields.get(6), 7), request.mapping().producer().transactionalIdentitySha256())
                || !Arrays.equals(bytes(fields.get(7), 8), request.canonicalReceiptRecordHash())) {
            throw new IllegalArgumentException("Kafka transaction evidence does not match the exact request");
        }
    }

    private static void validateCursor(
            final KafkaTransactionalDestinationRequest request, final EvidenceCursor cursor, final long receiptOffset) {
        final KafkaReceiptJournal.Mapping mapping = request.mapping();
        if (cursor.evidenceKind() != EvidenceKind.KAFKA_RECEIPT_CONTIGUOUS
                || !Arrays.equals(
                        cursor.destinationLaneId(), mapping.producer().laneId().bytes())
                || !Arrays.equals(cursor.laneIncarnation(), mapping.producer().laneIncarnation())
                || !Arrays.equals(
                        cursor.topicUuid(), uuidBytes(request.receiptResource().nativeTopicUuid()))
                || cursor.physicalPartition() != request.receiptResource().receiptPartition()
                || cursor.evidenceGeneration() == 0
                || Long.compareUnsigned(cursor.nextOffsetExclusive(), receiptOffset) <= 0
                || Long.compareUnsigned(cursor.lastObservedLsoExclusive(), receiptOffset) <= 0) {
            throw new IllegalArgumentException("Kafka receipt cursor does not bind the exact transaction channel");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> branchFields(final byte[] branch) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(branch);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() != 8) {
            throw new IllegalArgumentException("Kafka transaction evidence branch has an invalid shape");
        }
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("Kafka transaction evidence branch has an invalid field order");
            }
        }
        return fields;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("Kafka transaction evidence field is not bytes: " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("Kafka transaction evidence field is not uint: " + number);
        }
        final long value = field.unsignedValue();
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Kafka transaction evidence partition is out of range");
        }
        return value;
    }

    private static byte[] uuidBytes(final java.util.UUID uuid) {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
