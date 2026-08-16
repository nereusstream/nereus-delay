package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceKindV1;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.DelayMessageId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaTransactionalPublishEvidenceTest {
    @Test
    void typedEvidenceBindsReadCommittedCursorAndExactReceiptRecord() {
        final Fixture fixture = new Fixture();
        final KafkaReceiptJournal.Mapping mapping = fixture.journal.appendNext(fixture.producer,
                new KafkaReceiptJournal.AttemptIdentity(fixture.request.delayMessageId(), fixture.request.generation(),
                        fixture.request.publishAttemptId(), fixture.preparedHash,
                        fixture.source.brokerPersistenceTimeEpochMs(), fixture.source.canonicalBytes()))
                .record().mapping();
        final KafkaTransactionalDestinationRequest transaction = KafkaTransactionalDestinationRequest.create(
                "target-topic", KafkaDestinationRequest.from(fixture.target, fixture.request), "receipt-topic",
                fixture.receipt, mapping);
        final EvidenceCursorV1 cursor = EvidenceCursorV1.kafka(fixture.lane.bytes(), fixture.laneIncarnation,
                uuidBytes(fixture.receipt.nativeTopicUuid()), fixture.receipt.receiptPartition(), 7,
                2_001, 11, 12);

        final PublishEvidenceV1 evidence = KafkaTransactionalPublishEvidence.published(transaction, cursor, 10,
                transaction.canonicalReceiptRecordHash());

        assertEquals(PublishEvidenceKindV1.KAFKA_TRANSACTIONAL_RECEIPT, evidence.evidenceKind());
        assertEquals(EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, evidence.verificationStatus());
        assertEquals(EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS, EvidenceCursorV1.decode(cursor.canonicalBytes())
                .evidenceKind());
        PublishEvidenceV1.decode(evidence.canonicalBytes()).requireBusinessMutation(mapping.publishAttemptId(), true);
        KafkaTransactionalPublishEvidence.requireExactBinding(evidence, transaction, 10);
        assertArrayEquals(transaction.canonicalReceiptRecordHash(),
                KafkaTransactionalDestinationRequest.canonicalReceiptRecordHash(
                        transaction.receiptKey(), transaction.receiptValue()));
    }

    @Test
    void rejectsForeignCursorAndReceiptDigest() {
        final Fixture fixture = new Fixture();
        final KafkaReceiptJournal.Mapping mapping = fixture.journal.appendNext(fixture.producer,
                new KafkaReceiptJournal.AttemptIdentity(fixture.request.delayMessageId(), fixture.request.generation(),
                        fixture.request.publishAttemptId(), fixture.preparedHash,
                        fixture.source.brokerPersistenceTimeEpochMs(), fixture.source.canonicalBytes()))
                .record().mapping();
        final KafkaTransactionalDestinationRequest transaction = KafkaTransactionalDestinationRequest.create(
                "target-topic", KafkaDestinationRequest.from(fixture.target, fixture.request), "receipt-topic",
                fixture.receipt, mapping);
        final EvidenceCursorV1 foreignCursor = EvidenceCursorV1.kafka(
                DestinationLaneId.derive(Bytes.utf8("foreign-lane")).bytes(), fixture.laneIncarnation,
                uuidBytes(fixture.receipt.nativeTopicUuid()), fixture.receipt.receiptPartition(), 7,
                2_001, 11, 12);

        assertThrows(IllegalArgumentException.class, () -> KafkaTransactionalPublishEvidence.published(transaction,
                foreignCursor, 10, transaction.canonicalReceiptRecordHash()));
        assertThrows(IllegalArgumentException.class, () -> KafkaTransactionalPublishEvidence.published(transaction,
                cursor(fixture), 10, Bytes.sha256(Bytes.utf8("foreign-receipt"))));
    }

    @Test
    void providerEvidenceMustRetainTheExactReceiptOffset() {
        final Fixture fixture = new Fixture();
        final KafkaReceiptJournal.Mapping mapping = fixture.journal.appendNext(fixture.producer,
                new KafkaReceiptJournal.AttemptIdentity(fixture.request.delayMessageId(), fixture.request.generation(),
                        fixture.request.publishAttemptId(), fixture.preparedHash,
                        fixture.source.brokerPersistenceTimeEpochMs(), fixture.source.canonicalBytes()))
                .record().mapping();
        final KafkaTransactionalDestinationRequest transaction = KafkaTransactionalDestinationRequest.create(
                "target-topic", KafkaDestinationRequest.from(fixture.target, fixture.request), "receipt-topic",
                fixture.receipt, mapping);
        final PublishEvidenceV1 evidence = KafkaTransactionalPublishEvidence.published(transaction, cursor(fixture), 10,
                transaction.canonicalReceiptRecordHash());

        assertThrows(IllegalArgumentException.class, () -> KafkaTransactionalPublishEvidence.requireExactBinding(
                evidence, transaction, 11));
    }

    private static EvidenceCursorV1 cursor(final Fixture fixture) {
        return EvidenceCursorV1.kafka(fixture.lane.bytes(), fixture.laneIncarnation,
                uuidBytes(fixture.receipt.nativeTopicUuid()), fixture.receipt.receiptPartition(), 7,
                2_001, 11, 12);
    }

    private static byte[] uuidBytes(final UUID uuid) {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }

    private static final class Fixture {
        private final RouteIncarnation route = RouteIncarnation.random();
        private final ShardId shard = new ShardId(route, 0);
        private final UUID targetTopic = UUID.randomUUID();
        private final UUID receiptTopic = UUID.randomUUID();
        private final KafkaTargetResource target = new KafkaTargetResource("cluster", targetTopic, 0);
        private final KafkaReceiptResource receipt = new KafkaReceiptResource("cluster", receiptTopic, route,
                0, 0, 1, 1, 0);
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("evidence-lane"));
        private final byte[] laneIncarnation = bytes(16, 3);
        private final byte[] transactionIdentity = bytes(32, 5);
        private final byte[] preparedHash = bytes(32, 7);
        private final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, receipt);
        private final KafkaReceiptJournal.ProducerKey producer = new KafkaReceiptJournal.ProducerKey(lane,
                laneIncarnation, transactionIdentity, target);
        private final DestinationPublishRequest request = new DestinationPublishRequest(lane, laneIncarnation,
                DelayMessageId.random(shard), 1, bytes(32, 9), 2_000, 2_000, Bytes.utf8("payload"),
                Bytes.utf8("metadata"));
        private final KafkaSourcePosition source = new KafkaSourcePosition(shard, "source-cluster",
                UUID.randomUUID(), 4, 1, 1_000);
    }
}
