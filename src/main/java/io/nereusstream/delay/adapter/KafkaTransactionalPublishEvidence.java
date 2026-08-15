package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceKindV1;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Builds the closed V1 Kafka transactional-receipt evidence branch from a
 * physical read_committed observation.
 *
 * <p>The cursor and receipt position are caller-supplied because only the
 * source-locked Kafka client can authenticate Fetch v13, Topic UUID and LSO.
 * This class performs the shared identity binding and refuses to turn a
 * foreign cursor or record digest into business evidence.</p>
 */
public final class KafkaTransactionalPublishEvidence {
    private static final int HASH_LENGTH = 32;

    private KafkaTransactionalPublishEvidence() {
    }

    /** Creates the typed VERIFIED_PUBLISHED K2 evidence envelope. */
    public static PublishEvidenceV1 published(final KafkaTransactionalDestinationRequest request,
                                              final EvidenceCursorV1 receiptCursor,
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
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(request.target().authenticatedClusterId(),
                        request.target().nativeTopicUuid()));
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, receiptCursor.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, receiptOffset);
            CanonicalProtobuf.bytes(output, 3,
                    ExternalDeliveryIdentityV1.publishAttempt(mapping.publishAttemptId()).canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 6, request.target().partition());
            CanonicalProtobuf.bytes(output, 7, mapping.producer().transactionalIdentitySha256());
            CanonicalProtobuf.bytes(output, 8, receiptRecordHash);
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.KAFKA_TRANSACTIONAL_RECEIPT,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch);
    }

    private static void validateCursor(final KafkaTransactionalDestinationRequest request,
                                       final EvidenceCursorV1 cursor, final long receiptOffset) {
        final KafkaReceiptJournal.Mapping mapping = request.mapping();
        if (cursor.evidenceKind() != EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS
                || !Arrays.equals(cursor.destinationLaneId(), mapping.producer().laneId().bytes())
                || !Arrays.equals(cursor.laneIncarnation(), mapping.producer().laneIncarnation())
                || !Arrays.equals(cursor.topicUuid(), uuidBytes(request.receiptResource().nativeTopicUuid()))
                || cursor.physicalPartition() != request.receiptResource().receiptPartition()
                || cursor.evidenceGeneration() == 0
                || Long.compareUnsigned(cursor.nextOffsetExclusive(), receiptOffset) <= 0
                || Long.compareUnsigned(cursor.lastObservedLsoExclusive(), receiptOffset) <= 0) {
            throw new IllegalArgumentException("Kafka receipt cursor does not bind the exact transaction channel");
        }
    }

    private static byte[] uuidBytes(final java.util.UUID uuid) {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
    }
}
