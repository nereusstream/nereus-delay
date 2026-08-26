package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceKind;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KafkaReceiptJournalTest {
    @Test
    void mappingMustBeDurableBeforeTargetTransactionAndExactRetryIsIdempotent() {
        final ShardId shard = shard();
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.Mapping mapping =
                KafkaReceiptJournal.Mapping.create(shard, producer, 0, identity(shard, 1));
        final AtomicBoolean senderCalled = new AtomicBoolean();
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, request -> position(10), resource(shard));

        assertThrows(
                KafkaReceiptJournal.JournalException.class,
                () -> journal.sendAfterMapped(mapping, ignored -> {
                    senderCalled.set(true);
                    return CompletableFuture.completedFuture("sent");
                }));
        assertFalse(senderCalled.get());

        final KafkaReceiptJournal.AppendResult first = journal.appendMapped(mapping);
        final KafkaReceiptJournal.AppendResult duplicate = journal.appendMapped(mapping);
        assertFalse(first.idempotent());
        assertTrue(duplicate.idempotent());
        assertEquals(1, journal.records().size());
        assertArrayEquals(first.record().canonicalBytes(), duplicate.record().canonicalBytes());

        assertEquals(
                "sent",
                journal.sendAfterMapped(mapping, ignored -> {
                            senderCalled.set(true);
                            return CompletableFuture.completedFuture("sent");
                        })
                        .toCompletableFuture()
                        .join());
        assertTrue(senderCalled.get());
    }

    @Test
    void attemptIdentityRequiresCanonicalSourcePositionAndMatchingShard() {
        final ShardId shard = shard();
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaReceiptJournal.AttemptIdentity(
                        DelayMessageId.random(shard),
                        0,
                        bytes(32, 1),
                        Bytes.sha256(Bytes.utf8("prepared")),
                        1,
                        Bytes.utf8("not-a-source-position")));
        final ShardId foreign = shard();
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaReceiptJournal.AttemptIdentity(
                        DelayMessageId.random(shard),
                        0,
                        bytes(32, 2),
                        Bytes.sha256(Bytes.utf8("prepared-2")),
                        2,
                        sourcePosition(foreign, 2)));
        final KafkaReceiptJournal.AttemptIdentity identity = identity(foreign, 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> KafkaReceiptJournal.Mapping.create(shard, producer(), 0, identity));
    }

    @Test
    void nullTargetStageFailsClosedAndKeepsTheMappedAttemptUnresolved() {
        final ShardId shard = shard();
        final KafkaReceiptResource resource = resource(shard);
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, request -> position(23), resource);
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.Mapping mapping =
                journal.appendNext(producer, identity(shard, 23)).record().mapping();

        final KafkaReceiptJournal.JournalException failure = assertThrows(
                KafkaReceiptJournal.JournalException.class, () -> journal.sendAfterMapped(mapping, ignored -> null));
        assertEquals(StableCode.INTEGRITY_ERROR, failure.stableCode());
        assertArrayEquals(
                mapping.mappingId(), journal.unresolved(producer).orElseThrow().mappingId());
        assertEquals(1, journal.records().size());
    }

    @Test
    void unresolvedLowerSequenceBlocksUntilRetirement() {
        final ShardId shard = shard();
        final AtomicLong offset = new AtomicLong(1);
        final KafkaReceiptJournal journal =
                new KafkaReceiptJournal(shard, request -> position(offset.getAndIncrement()), resource(shard));
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.AppendResult first = journal.appendNext(producer, identity(shard, 2));

        final KafkaReceiptJournal.JournalException blocked = assertThrows(
                KafkaReceiptJournal.JournalException.class, () -> journal.appendNext(producer, identity(shard, 3)));
        assertEquals(StableCode.INTEGRITY_ERROR, blocked.stableCode());
        assertEquals(0, journal.unresolved(producer).orElseThrow().sequenceId());

        final KafkaReceiptJournal.AppendResult retired =
                journal.retireNotPublished(first.record().mapping().mappingId());
        assertEquals(
                KafkaReceiptJournal.RecordKind.RETIRED_NOT_PUBLISHED,
                retired.record().kind());
        assertTrue(journal.unresolved(producer).isEmpty());
        assertEquals(
                1,
                journal.appendNext(producer, identity(shard, 3))
                        .record()
                        .mapping()
                        .sequenceId());
    }

    @Test
    void retirementUsesDurablePositionAndAdvancesReceiptCursor() {
        final ShardId shard = shard();
        final AtomicLong offset = new AtomicLong(40);
        final AtomicLong appendCalls = new AtomicLong();
        final AtomicReference<KafkaReceiptJournal.RecordKind> lastKind = new AtomicReference<>();
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(
                shard,
                request -> {
                    appendCalls.incrementAndGet();
                    lastKind.set(request.kind());
                    return position(offset.getAndIncrement());
                },
                resource(shard));
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.AppendResult mapped = journal.appendNext(producer, identity(shard, 23));

        final KafkaReceiptJournal.AppendResult retired =
                journal.retireNotPublished(mapped.record().mapping().mappingId());

        assertEquals(2, appendCalls.get());
        assertEquals(KafkaReceiptJournal.RecordKind.RETIRED_NOT_PUBLISHED, lastKind.get());
        assertEquals(41, retired.record().position().offset());
        assertEquals(2, journal.records().size());
        assertEquals(42, journal.evidenceCursor(producer, 1).orElseThrow().nextOffsetExclusive());
    }

    @Test
    void receiptJournalPreservesUnsignedHighBitOffsetsAndOrdering() {
        final ShardId shard = shard();
        final AtomicInteger call = new AtomicInteger();
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(
                shard,
                request -> {
                    final long offset = call.getAndIncrement() == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
                    return position(offset);
                },
                resource(shard));
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.AppendResult mapped = journal.appendNext(producer, identity(shard, 26));
        final KafkaReceiptJournal.AppendResult retired =
                journal.retireNotPublished(mapped.record().mapping().mappingId());

        assertEquals(Long.MAX_VALUE, mapped.record().position().offset());
        assertEquals(Long.MIN_VALUE, retired.record().position().offset());
        assertEquals(
                Long.MIN_VALUE + 1,
                journal.evidenceCursor(producer, 1).orElseThrow().nextOffsetExclusive());
        assertEquals(2, journal.records().size());
    }

    @Test
    void localAppenderFailsClosedBeforeUnsignedOffsetExhaustionCanWrap() {
        final ShardId shard = shard();
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(shard, resource(shard), -1L);

        assertThrows(
                KafkaReceiptJournal.JournalException.class, () -> journal.appendNext(producer(), identity(shard, 28)));
        assertTrue(journal.records().isEmpty());
        // The failed append must not advance the local cursor to zero and
        // accidentally admit a later record after the u64 domain is spent.
        assertThrows(
                KafkaReceiptJournal.JournalException.class, () -> journal.appendNext(producer(), identity(shard, 29)));
        assertTrue(journal.records().isEmpty());
    }

    @Test
    void retirementAppenderFailureKeepsLowerSequenceUnresolved() {
        final ShardId shard = shard();
        final KafkaReceiptJournal journal = new KafkaReceiptJournal(
                shard,
                request -> request.kind() == KafkaReceiptJournal.RecordKind.MAPPED ? position(50) : null,
                resource(shard));
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.AppendResult mapped = journal.appendNext(producer, identity(shard, 24));

        assertThrows(
                KafkaReceiptJournal.JournalException.class,
                () -> journal.retireNotPublished(mapped.record().mapping().mappingId()));
        assertEquals(0, journal.unresolved(producer).orElseThrow().sequenceId());
        assertEquals(1, journal.records().size());
    }

    @Test
    void retirementReplayReconstructsCursorAndIsIdempotent() {
        final ShardId shard = shard();
        final AtomicLong offset = new AtomicLong(60);
        final KafkaReceiptResource resource = resource(shard);
        final KafkaReceiptJournal source =
                new KafkaReceiptJournal(shard, request -> position(offset.getAndIncrement()), resource);
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.AppendResult mapped = source.appendNext(producer, identity(shard, 25));
        final KafkaReceiptJournal.AppendResult retired =
                source.retireNotPublished(mapped.record().mapping().mappingId());

        final KafkaReceiptJournal recovered = new KafkaReceiptJournal(shard, request -> position(90), resource);
        recovered.replay(mapped.record());
        recovered.replay(retired.record());
        recovered.replay(retired.record());

        assertTrue(recovered.unresolved(producer).isEmpty());
        assertEquals(2, recovered.records().size());
        assertEquals(62, recovered.evidenceCursor(producer, 2).orElseThrow().nextOffsetExclusive());
    }

    @Test
    void reconstructedMappedRecordReplayUsesCanonicalPositionBytes() {
        final ShardId shard = shard();
        final KafkaReceiptResource resource = resource(shard);
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal source = new KafkaReceiptJournal(shard, request -> position(70), resource);
        final KafkaReceiptJournal.JournalRecord original =
                source.appendNext(producer, identity(shard, 27)).record();

        final KafkaReceiptJournal.ReceiptPosition reconstructedPosition = new KafkaReceiptJournal.ReceiptPosition(
                original.position().offset(),
                original.position().brokerLogAppendTimeEpochMs(),
                original.position().lastStableOffsetExclusive(),
                original.position().receiptRecordHash());
        final KafkaReceiptJournal.JournalRecord reconstructed = new KafkaReceiptJournal.JournalRecord(
                KafkaReceiptJournal.RecordKind.MAPPED, original.mapping(), reconstructedPosition);
        final KafkaReceiptJournal recovered = new KafkaReceiptJournal(shard, request -> position(90), resource);

        recovered.replay(original);
        recovered.replay(reconstructed);

        assertEquals(1, recovered.records().size());
        assertArrayEquals(original.canonicalBytes(), recovered.records().get(0).canonicalBytes());
    }

    @Test
    void replayProjectsTypedReceiptEvidenceAndBindsReceiptIdentity() {
        final ShardId shard = shard();
        final KafkaReceiptResource resource = resource(shard);
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final AtomicLong offset = new AtomicLong(20);
        final KafkaReceiptJournal source =
                new KafkaReceiptJournal(shard, request -> position(offset.getAndIncrement()), resource);
        final KafkaReceiptJournal.AppendResult appended = source.appendNext(producer, identity(shard, 4));

        final KafkaReceiptJournal recovered = new KafkaReceiptJournal(shard, request -> position(40), resource);
        recovered.replay(appended.record());
        recovered.replay(appended.record());
        assertEquals(1, recovered.records().size());
        assertArrayEquals(
                appended.record().mapping().mappingId(),
                recovered.unresolved(producer).orElseThrow().mappingId());

        final com.nereusstream.delay.protocol.EvidenceCursor cursor =
                recovered.evidenceCursor(producer, 7).orElseThrow();
        assertEquals(EvidenceKind.KAFKA_RECEIPT_CONTIGUOUS, cursor.evidenceKind());
        assertEquals(resource.receiptPartition(), cursor.physicalPartition());
        assertEquals(21, cursor.nextOffsetExclusive());
        assertEquals(21, cursor.lastObservedLsoExclusive());

        final com.nereusstream.delay.protocol.PublishEvidence evidence =
                recovered.publishedEvidence(appended.record().mapping(), 7);
        assertEquals(
                com.nereusstream.delay.protocol.PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT,
                evidence.evidenceKind());
        assertEquals(EvidenceVerificationStatus.VERIFIED_PUBLISHED, evidence.verificationStatus());
        evidence.requireBusinessMutation(appended.record().mapping().publishAttemptId(), true);
        assertArrayEquals(
                evidence.canonicalBytes(),
                com.nereusstream.delay.protocol.PublishEvidence.decode(evidence.canonicalBytes())
                        .canonicalBytes());
    }

    @Test
    void resolverRequiresExactReceiptIdentityAndIndependentAbsencePredicates() {
        final ShardId shard = shard();
        final KafkaReceiptResource resource = resource(shard);
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final AtomicLong offset = new AtomicLong(20);
        final KafkaReceiptJournal journal =
                new KafkaReceiptJournal(shard, request -> position(offset.getAndIncrement()), resource);
        final KafkaReceiptJournal.Mapping mapping =
                journal.appendNext(producer, identity(shard, 6)).record().mapping();
        final KafkaReceiptJournal.ReceiptPosition position =
                journal.records().get(0).position();
        final com.nereusstream.delay.protocol.EvidenceCursor cursor =
                journal.evidenceCursor(producer, 8).orElseThrow();
        final KafkaReceiptJournal.ReceiptMatch exact = new KafkaReceiptJournal.ReceiptMatch(
                position.offset(),
                mapping.publishAttemptId(),
                mapping.preparedPublishHash(),
                position.receiptRecordHash());

        final KafkaReceiptJournal.Resolution published =
                journal.resolve(producer, KafkaReceiptJournal.ReceiptObservation.published(cursor, exact));
        assertEquals(KafkaReceiptJournal.ResolutionKind.PUBLISHED, published.kind());

        final KafkaReceiptJournal.ReceiptMatch drifted = new KafkaReceiptJournal.ReceiptMatch(
                position.offset(),
                mapping.publishAttemptId(),
                Bytes.sha256(Bytes.utf8("different-prepared")),
                position.receiptRecordHash());
        final KafkaReceiptJournal.Resolution mismatch =
                journal.resolve(producer, KafkaReceiptJournal.ReceiptObservation.published(cursor, drifted));
        assertEquals(KafkaReceiptJournal.ResolutionKind.DIVERGENCE, mismatch.kind());
        assertEquals(StableCode.INTEGRITY_ERROR, mismatch.stableCode());

        journal.retireNotPublished(mapping.mappingId());
        final KafkaReceiptJournal.Resolution absent =
                journal.resolve(producer, KafkaReceiptJournal.ReceiptObservation.absent(cursor, true, true));
        assertEquals(KafkaReceiptJournal.ResolutionKind.NOT_PUBLISHED, absent.kind());

        final KafkaReceiptJournal.Resolution missingRetention =
                journal.resolve(producer, KafkaReceiptJournal.ReceiptObservation.absent(cursor, true, false));
        assertEquals(KafkaReceiptJournal.ResolutionKind.DIVERGENCE, missingRetention.kind());
    }

    @Test
    void retiredMappingProjectsStrictReceiptAbsenceEvidence() {
        final ShardId shard = shard();
        final KafkaReceiptResource resource = resource(shard);
        final AtomicLong offset = new AtomicLong(30);
        final KafkaReceiptJournal journal =
                new KafkaReceiptJournal(shard, request -> position(offset.getAndIncrement()), resource);
        final KafkaReceiptJournal.ProducerKey producer = producer();
        final KafkaReceiptJournal.Mapping mapping =
                journal.appendNext(producer, identity(shard, 5)).record().mapping();
        journal.retireNotPublished(mapping.mappingId());

        assertThrows(
                IllegalArgumentException.class,
                () -> journal.notPublishedEvidence(mapping, 9, fencedChannel(producer, resource, 9), new byte[31]));

        final com.nereusstream.delay.protocol.PublishEvidence evidence = journal.notPublishedEvidence(
                mapping, 9, fencedChannel(producer, resource, 9), Bytes.sha256(Bytes.utf8("fence-and-lso")));
        assertEquals(
                com.nereusstream.delay.protocol.PublishEvidenceKind.KAFKA_RECEIPT_ABSENCE, evidence.evidenceKind());
        assertEquals(EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, evidence.verificationStatus());
        evidence.requireBusinessMutation(mapping.publishAttemptId(), false);
        assertArrayEquals(
                evidence.canonicalBytes(),
                com.nereusstream.delay.protocol.PublishEvidence.decode(evidence.canonicalBytes())
                        .canonicalBytes());

        final KafkaReceiptJournal.ProducerKey wrongLane = new KafkaReceiptJournal.ProducerKey(
                new DestinationLaneId(bytes(32, 99)),
                producer.laneIncarnation(),
                producer.transactionalIdentitySha256(),
                producer.target());
        assertThrows(
                KafkaReceiptJournal.JournalException.class,
                () -> journal.notPublishedEvidence(
                        mapping, 9, fencedChannel(wrongLane, resource, 9), Bytes.sha256(Bytes.utf8("fence-and-lso"))));
    }

    private static KafkaReceiptJournal.ReceiptPosition position(final long offset) {
        return new KafkaReceiptJournal.ReceiptPosition(
                offset, 1_000, offset + 1, Bytes.sha256(Bytes.utf8("receipt-" + offset)));
    }

    private static ShardId shard() {
        return new ShardId(RouteIncarnation.random(), 3);
    }

    private static KafkaReceiptResource resource(final ShardId shard) {
        return new KafkaReceiptResource(
                "receipt-cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000004"),
                shard.routeIncarnation(),
                shard.partition(),
                1,
                7,
                4,
                13);
    }

    private static KafkaReceiptJournal.ProducerKey producer() {
        return new KafkaReceiptJournal.ProducerKey(
                new DestinationLaneId(bytes(32, 1)),
                bytes(16, 2),
                Bytes.sha256(Bytes.utf8("stable-transaction")),
                new KafkaTargetResource("receipt-cluster", UUID.fromString("00000000-0000-0000-0000-000000000003"), 4));
    }

    private static ChannelResourceIdentity fencedChannel(
            final KafkaReceiptJournal.ProducerKey producer,
            final KafkaReceiptResource resource,
            final long evidenceGeneration) {
        final BrokerResourceIdentity target = BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                producer.target().authenticatedClusterId(), producer.target().nativeTopicUuid()));
        final BrokerResourceIdentity evidence = BrokerResourceIdentity.kafka(
                new KafkaBrokerResourceIdentity(resource.authenticatedClusterId(), resource.nativeTopicUuid()));
        final byte[] producerIdentity = Bytes.utf8("stable-transaction");
        final byte[] guardDigest = Bytes.sha256(Bytes.utf8("receipt-guard"));
        final byte[] bindingDigest = Bytes.sha256(Bytes.utf8("receipt-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("receipt-fingerprint"));
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKind.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT.wireValue());
            CanonicalProtobuf.bytes(output, 3, producer.laneId().bytes());
            CanonicalProtobuf.bytes(output, 4, producer.laneIncarnation());
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, producer.target().partition());
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producerIdentity);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producerIdentity));
            CanonicalProtobuf.bytes(output, 11, evidence.canonicalBytes());
            CanonicalProtobuf.uint64(output, 12, evidenceGeneration);
            CanonicalProtobuf.bytes(output, 13, guardDigest);
        });
        final ProfileRef profile = new ProfileRef(
                Bytes.utf8("receipt-destination"),
                1,
                Bytes.sha256(Bytes.utf8("receipt-destination-semantic")),
                ProfileKind.DESTINATION);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                1_000,
                1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("receipt-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("receipt-time")),
                0,
                null);
        final CredentialUseLease lease = new CredentialUseLease(
                profile,
                CredentialUseKind.DESTINATION_CHANNEL,
                CredentialUseLease.destinationChannelHolderScope(prefix),
                1,
                bindingDigest,
                fingerprint,
                issuedAt,
                9_000,
                1);
        return new ChannelResourceIdentity(
                AdapterKind.KAFKA,
                ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT,
                producer.laneId().bytes(),
                producer.laneIncarnation(),
                target,
                producer.target().partition(),
                1,
                0,
                producerIdentity,
                Bytes.sha256(producerIdentity),
                evidence,
                evidenceGeneration,
                guardDigest,
                1,
                bindingDigest,
                fingerprint,
                lease);
    }

    private static KafkaReceiptJournal.AttemptIdentity identity(final ShardId shard, final int seed) {
        return new KafkaReceiptJournal.AttemptIdentity(
                DelayMessageId.random(shard),
                0,
                bytes(32, seed),
                Bytes.sha256(Bytes.utf8("prepared-" + seed)),
                900 + seed,
                sourcePosition(shard, seed));
    }

    private static byte[] sourcePosition(final ShardId shard, final int seed) {
        return new KafkaSourcePosition(
                        shard, "test", UUID.nameUUIDFromBytes(Bytes.utf8("source-" + seed)), seed, null, 1_000 + seed)
                .canonicalBytes();
    }

    private static byte[] bytes(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
