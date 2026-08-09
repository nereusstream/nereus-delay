package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PulsarAttemptJournalTest {
    @Test
    void exactMappingMustBeDurableBeforeSendAndReplayIsIdempotent() {
        final ShardId shard = shard();
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.Mapping mapping = PulsarAttemptJournal.Mapping.create(shard, producer, 0,
                identity(shard, 1));
        final AtomicBoolean senderCalled = new AtomicBoolean();
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard, request -> position(10));

        assertThrows(PulsarAttemptJournal.JournalException.class,
                () -> journal.sendAfterMapped(mapping, ignored -> {
                    senderCalled.set(true);
                    return CompletableFuture.completedFuture("sent");
                }));
        assertFalse(senderCalled.get());

        final PulsarAttemptJournal.AppendResult first = journal.appendMapped(mapping);
        final PulsarAttemptJournal.AppendResult duplicate = journal.appendMapped(mapping);
        assertFalse(first.idempotent());
        assertTrue(duplicate.idempotent());
        assertEquals(1, journal.records().size());
        assertArrayEquals(first.record().canonicalBytes(), duplicate.record().canonicalBytes());

        final String result = journal.sendAfterMapped(mapping, ignored -> {
            senderCalled.set(true);
            return CompletableFuture.completedFuture("sent");
        }).toCompletableFuture().join();
        assertEquals("sent", result);
        assertTrue(senderCalled.get());
    }

    @Test
    void lowerUnresolvedSequenceBlocksUntilDurableRetirement() {
        final ShardId shard = shard();
        final AtomicLong entry = new AtomicLong(1);
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard,
                request -> position(entry.getAndIncrement()));
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.AppendResult first = journal.appendNext(producer, identity(shard, 2));

        final PulsarAttemptJournal.JournalException blocked = assertThrows(PulsarAttemptJournal.JournalException.class,
                () -> journal.appendNext(producer, identity(shard, 3)));
        assertEquals(StableCode.INTEGRITY_ERROR, blocked.stableCode());
        assertEquals(0, journal.unresolved(producer).orElseThrow().sequenceId());

        final PulsarAttemptJournal.AppendResult retired = journal.retireNotPublished(first.record().mapping().mappingId());
        assertEquals(PulsarAttemptJournal.RecordKind.RETIRED_NOT_PUBLISHED, retired.record().kind());
        assertTrue(journal.unresolved(producer).isEmpty());

        final PulsarAttemptJournal.AppendResult second = journal.appendNext(producer, identity(shard, 3));
        assertEquals(1, second.record().mapping().sequenceId());
    }

    @Test
    void retransmissionReusesTheExactMappingBeforeInvokingTarget() {
        final ShardId shard = shard();
        final AtomicLong appendCalls = new AtomicLong();
        final AtomicLong targetCalls = new AtomicLong();
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard, request -> {
            appendCalls.incrementAndGet();
            return position(50);
        });
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.AttemptIdentity identity = identity(shard, 20);

        assertEquals("first", journal.sendAfterMapped(producer, identity, ignored -> {
            targetCalls.incrementAndGet();
            return CompletableFuture.completedFuture("first");
        }).toCompletableFuture().join());
        assertEquals("retry", journal.sendAfterMapped(producer, identity, ignored -> {
            targetCalls.incrementAndGet();
            return CompletableFuture.completedFuture("retry");
        }).toCompletableFuture().join());

        assertEquals(1, appendCalls.get(), "an exact retry must not allocate a new Journal sequence");
        assertEquals(2, targetCalls.get());
        assertEquals(1, journal.records().size());
        assertEquals(0, journal.records().get(0).mapping().sequenceId());
    }

    @Test
    void attemptIdentityDriftAndRetiredRetryAreFencedBeforeTargetSend() {
        final ShardId shard = shard();
        final AtomicLong entry = new AtomicLong(60);
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard,
                request -> position(entry.getAndIncrement()));
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.AttemptIdentity identity = identity(shard, 21);
        journal.appendOrReuse(producer, identity);

        final PulsarAttemptJournal.AttemptIdentity drifted = new PulsarAttemptJournal.AttemptIdentity(
                identity.delayMessageId(), identity.generation(), identity.publishAttemptId(),
                Bytes.sha256(Bytes.utf8("different-prepared")), identity.guardedBrokerTimestampEpochMs(),
                identity.sourcePosition());
        final PulsarAttemptJournal.JournalException drift = assertThrows(
                PulsarAttemptJournal.JournalException.class, () -> journal.appendOrReuse(producer, drifted));
        assertEquals(StableCode.INTEGRITY_ERROR, drift.stableCode());

        final byte[] mappingId = journal.records().get(0).mapping().mappingId();
        journal.retireNotPublished(mappingId);
        final PulsarAttemptJournal.JournalException retired = assertThrows(
                PulsarAttemptJournal.JournalException.class, () -> journal.sendAfterMapped(producer, identity,
                        ignored -> CompletableFuture.completedFuture("must-not-send")));
        assertEquals(StableCode.INTEGRITY_ERROR, retired.stableCode());
        assertEquals(2, journal.records().size());
    }

    @Test
    void journalAppendFailureNeverInvokesTargetSender() {
        final ShardId shard = shard();
        final AtomicBoolean targetCalled = new AtomicBoolean();
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard, request -> null);

        final PulsarAttemptJournal.JournalException failure = assertThrows(
                PulsarAttemptJournal.JournalException.class,
                () -> journal.sendAfterMapped(producer(), identity(shard, 22), ignored -> {
                    targetCalled.set(true);
                    return CompletableFuture.completedFuture("must-not-send");
                }));
        assertEquals(StableCode.PULSAR_EVIDENCE_DIVERGENCE, failure.stableCode());
        assertFalse(targetCalled.get());
        assertTrue(journal.records().isEmpty());
    }

    @Test
    void recoveryAndBrokerEvidenceFailClosedOnDivergence() {
        final ShardId shard = shard();
        final AtomicLong entry = new AtomicLong(7);
        final PulsarAttemptJournal source = new PulsarAttemptJournal(shard,
                request -> position(entry.getAndIncrement()));
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.AppendResult appended = source.appendNext(producer, identity(shard, 4));

        final PulsarAttemptJournal.Resolution published = source.resolve(producer,
                new PulsarAttemptJournal.BrokerSequenceEvidence(0, false, false));
        assertEquals(PulsarAttemptJournal.ResolutionKind.PUBLISHED, published.kind());

        final PulsarAttemptJournal.Resolution noProof = source.resolve(producer,
                new PulsarAttemptJournal.BrokerSequenceEvidence(-1, false, false));
        assertEquals(PulsarAttemptJournal.ResolutionKind.DIVERGENCE, noProof.kind());
        assertEquals(StableCode.PULSAR_EVIDENCE_DIVERGENCE, noProof.stableCode());

        final PulsarAttemptJournal.Resolution above = source.resolve(producer,
                new PulsarAttemptJournal.BrokerSequenceEvidence(1, true, true));
        assertEquals(PulsarAttemptJournal.ResolutionKind.DIVERGENCE, above.kind());

        final PulsarAttemptJournal replayed = new PulsarAttemptJournal(shard, request -> position(20));
        for (PulsarAttemptJournal.JournalRecord record : source.records()) {
            replayed.replay(record);
            replayed.replay(record);
        }
        assertEquals(1, replayed.records().size());
        assertArrayEquals(appended.record().mapping().mappingId(),
                replayed.unresolved(producer).orElseThrow().mappingId());

        final PulsarAttemptJournal.Resolution retained = replayed.resolve(producer,
                new PulsarAttemptJournal.BrokerSequenceEvidence(-1, true, true));
        assertEquals(PulsarAttemptJournal.ResolutionKind.NOT_PUBLISHED, retained.kind());
    }

    @Test
    void retirementReplayAndShardIdentityAreFenced() {
        final ShardId shard = shard();
        final PulsarAttemptJournal source = new PulsarAttemptJournal(shard, request -> position(
                request.kind() == PulsarAttemptJournal.RecordKind.MAPPED ? 30 : 31));
        final PulsarAttemptJournal.AppendResult mapped = source.appendNext(producer(), identity(shard, 5));
        final PulsarAttemptJournal.AppendResult retired = source.retireNotPublished(mapped.record().mapping().mappingId());
        final PulsarAttemptJournal recovered = new PulsarAttemptJournal(shard, request -> position(40));
        recovered.replay(mapped.record());
        recovered.replay(retired.record());
        assertTrue(recovered.unresolved(producer()).isEmpty());
        assertEquals(PulsarAttemptJournal.ResolutionKind.NOT_PUBLISHED,
                recovered.resolve(producer(), new PulsarAttemptJournal.BrokerSequenceEvidence(-1, false, false))
                        .kind());

        final ShardId otherShard = new ShardId(RouteIncarnation.random(), shard.partition());
        final PulsarAttemptJournal.JournalException mismatch = assertThrows(
                PulsarAttemptJournal.JournalException.class, () -> recovered.replay(
                        new PulsarAttemptJournal.JournalRecord(
                                PulsarAttemptJournal.RecordKind.MAPPED,
                                PulsarAttemptJournal.Mapping.create(otherShard, producer(), 0,
                                        identity(otherShard, 6)), position(41))));
        assertEquals(StableCode.INTEGRITY_ERROR, mismatch.stableCode());
    }

    @Test
    void replayRejectsAProducerSequenceGapBeforeInstallingState() {
        final ShardId shard = shard();
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard, request -> position(50));
        final PulsarAttemptJournal.Mapping firstGap = PulsarAttemptJournal.Mapping.create(shard, producer, 1,
                identity(shard, 7));

        final PulsarAttemptJournal.JournalException first = assertThrows(
                PulsarAttemptJournal.JournalException.class,
                () -> journal.replay(new PulsarAttemptJournal.JournalRecord(
                        PulsarAttemptJournal.RecordKind.MAPPED, firstGap, position(50))));
        assertEquals(StableCode.INTEGRITY_ERROR, first.stableCode());
        assertTrue(journal.records().isEmpty());

        final PulsarAttemptJournal source = new PulsarAttemptJournal(shard,
                request -> position(request.kind() == PulsarAttemptJournal.RecordKind.MAPPED ? 60 : 61));
        final PulsarAttemptJournal.AppendResult mapped = source.appendNext(producer, identity(shard, 8));
        final PulsarAttemptJournal.AppendResult retired = source.retireNotPublished(mapped.record().mapping().mappingId());
        journal.replay(mapped.record());
        journal.replay(retired.record());
        final PulsarAttemptJournal.Mapping laterGap = PulsarAttemptJournal.Mapping.create(shard, producer, 2,
                identity(shard, 9));
        final PulsarAttemptJournal.JournalException later = assertThrows(
                PulsarAttemptJournal.JournalException.class,
                () -> journal.replay(new PulsarAttemptJournal.JournalRecord(
                        PulsarAttemptJournal.RecordKind.MAPPED, laterGap, position(62))));
        assertEquals(StableCode.INTEGRITY_ERROR, later.stableCode());
        assertEquals(2, journal.records().size());
        assertTrue(journal.unresolved(producer).isEmpty());
    }

    @Test
    void evidenceCursorProjectsTheLatestLaneProducerJournalPosition() {
        final ShardId shard = shard();
        final AtomicLong entry = new AtomicLong(70);
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard,
                request -> new PulsarAttemptJournal.JournalPosition(2, entry.getAndIncrement(), 1, 3,
                        request.kind() == PulsarAttemptJournal.RecordKind.MAPPED ? 1_000 : 1_010));

        assertTrue(journal.evidenceCursor(producer, 1).isEmpty());
        journal.appendNext(producer, identity(shard, 10));
        final io.nereusstream.delay.protocol.EvidenceCursorV1 mapped = journal.evidenceCursor(producer, 1)
                .orElseThrow();
        assertEquals(2, mapped.ledgerId());
        assertEquals(70, mapped.entryId());
        assertEquals(1, mapped.normalizedBatchIndex());
        assertEquals(3, mapped.batchSize());
        assertEquals(1_000, mapped.maxBrokerPersistedAtThroughCursor());
        assertThrows(IllegalArgumentException.class, () -> journal.evidenceCursor(producer, 0));

        journal.retireNotPublished(journal.records().get(0).mapping().mappingId());
        final io.nereusstream.delay.protocol.EvidenceCursorV1 retired = journal.evidenceCursor(producer, 1)
                .orElseThrow();
        assertEquals(71, retired.entryId());
        assertEquals(1_010, retired.maxBrokerPersistedAtThroughCursor());
    }

    @Test
    void publishedEvidenceBindsExactMappingAndJournalRecordHash() {
        final ShardId shard = shard();
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final AtomicLong entry = new AtomicLong(80);
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard, request -> position(entry.getAndIncrement()));
        final PulsarAttemptJournal.Mapping mapping = journal.appendNext(producer, identity(shard, 11))
                .record().mapping();
        final byte[] targetAckEvidence = Bytes.sha256(Bytes.utf8("target-ack-evidence"));

        final io.nereusstream.delay.protocol.PublishEvidenceV1 evidence = journal.publishedEvidence(mapping, 2,
                targetAckEvidence);
        assertEquals(io.nereusstream.delay.protocol.PublishEvidenceKindV1.PULSAR_ATTEMPT_JOURNAL,
                evidence.evidenceKind());
        assertEquals(io.nereusstream.delay.protocol.EvidenceVerificationStatusV1.VERIFIED_PUBLISHED,
                evidence.verificationStatus());
        evidence.requireBusinessMutation(mapping.publishAttemptId(), true);
        assertArrayEquals(evidence.canonicalBytes(),
                io.nereusstream.delay.protocol.PublishEvidenceV1.decode(evidence.canonicalBytes()).canonicalBytes());

        journal.retireNotPublished(mapping.mappingId());
        final PulsarAttemptJournal.JournalException retired = assertThrows(
                PulsarAttemptJournal.JournalException.class,
                () -> journal.publishedEvidence(mapping, 2, targetAckEvidence));
        assertEquals(StableCode.INTEGRITY_ERROR, retired.stableCode());
    }

    private static PulsarAttemptJournal.JournalPosition position(final long entryId) {
        return new PulsarAttemptJournal.JournalPosition(1, entryId, 0, 1, 1_000);
    }

    private static ShardId shard() {
        return new ShardId(RouteIncarnation.random(), 3);
    }

    private static PulsarAttemptJournal.ProducerKey producer() {
        return new PulsarAttemptJournal.ProducerKey(new DestinationLaneId(bytes(32, 1)), bytes(16, 2),
                Bytes.sha256(Bytes.utf8("stable-producer")),
                new PulsarTargetResource("cluster", bytes(32, 3), "persistent://tenant/ns/topic", 4, 0));
    }

    private static PulsarAttemptJournal.AttemptIdentity identity(final ShardId shard, final int seed) {
        return new PulsarAttemptJournal.AttemptIdentity(DelayMessageId.random(shard), 0, bytes(32, seed),
                Bytes.sha256(Bytes.utf8("prepared-" + seed)), 900 + seed, bytes(4, seed + 1));
    }

    private static byte[] bytes(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
