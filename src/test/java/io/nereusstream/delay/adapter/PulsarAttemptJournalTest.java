package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.ChannelKindV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PulsarJournalGenerationResourceV1;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
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
    void localAppenderFailsClosedAfterUnsignedEntryExhaustionWithoutWrapping() {
        final ShardId shard = shard();
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard, -1L);
        final PulsarAttemptJournal.ProducerKey producer = producer();

        final PulsarAttemptJournal.AppendResult finalEntry = journal.appendNext(producer, identity(shard, 31));
        assertEquals(-1L, finalEntry.record().position().entryId());
        assertEquals(1, journal.records().size());

        final PulsarAttemptJournal.JournalException firstExhaustion = assertThrows(
                PulsarAttemptJournal.JournalException.class,
                () -> journal.retireNotPublished(finalEntry.record().mapping().mappingId()));
        assertEquals(StableCode.INTEGRITY_ERROR, firstExhaustion.stableCode());
        assertEquals(1, journal.records().size());

        final PulsarAttemptJournal.JournalException secondExhaustion = assertThrows(
                PulsarAttemptJournal.JournalException.class,
                () -> journal.retireNotPublished(finalEntry.record().mapping().mappingId()));
        assertEquals(StableCode.INTEGRITY_ERROR, secondExhaustion.stableCode());
        assertEquals(1, journal.records().size());
    }

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
        assertEquals(PulsarAttemptJournal.ResolutionKind.DIVERGENCE,
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
    void reconstructedMappedAndRetirementReplayUsesCanonicalPositionBytes() {
        final ShardId shard = shard();
        final PulsarAttemptJournal source = new PulsarAttemptJournal(shard,
                request -> position(request.kind() == PulsarAttemptJournal.RecordKind.MAPPED ? 80 : 81));
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.AppendResult mapped = source.appendNext(producer, identity(shard, 28));
        final PulsarAttemptJournal.AppendResult retired = source.retireNotPublished(mapped.record().mapping().mappingId());

        final PulsarAttemptJournal.JournalPosition mappedPosition = new PulsarAttemptJournal.JournalPosition(
                mapped.record().position().ledgerId(), mapped.record().position().entryId(),
                mapped.record().position().batchIndex(), mapped.record().position().batchSize(),
                mapped.record().position().brokerEntryTimestampEpochMs());
        final PulsarAttemptJournal.JournalPosition retiredPosition = new PulsarAttemptJournal.JournalPosition(
                retired.record().position().ledgerId(), retired.record().position().entryId(),
                retired.record().position().batchIndex(), retired.record().position().batchSize(),
                retired.record().position().brokerEntryTimestampEpochMs());
        final PulsarAttemptJournal recovered = new PulsarAttemptJournal(shard, request -> position(90));

        recovered.replay(new PulsarAttemptJournal.JournalRecord(
                PulsarAttemptJournal.RecordKind.MAPPED, mapped.record().mapping(), mappedPosition));
        recovered.replay(new PulsarAttemptJournal.JournalRecord(
                PulsarAttemptJournal.RecordKind.RETIRED_NOT_PUBLISHED, retired.record().mapping(), retiredPosition));

        assertEquals(2, recovered.records().size());
        assertArrayEquals(retired.record().canonicalBytes(), recovered.records().get(1).canonicalBytes());
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
    void retiredMappingStillRequiresRetentionProofAndFencesLateBrokerPublication() {
        final ShardId shard = shard();
        final AtomicLong entry = new AtomicLong(100);
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard,
                request -> position(entry.getAndIncrement()));
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final PulsarAttemptJournal.Mapping mapping = journal.appendNext(producer, identity(shard, 29))
                .record().mapping();
        journal.retireNotPublished(mapping.mappingId());

        assertEquals(PulsarAttemptJournal.ResolutionKind.DIVERGENCE,
                journal.resolve(producer, new PulsarAttemptJournal.BrokerSequenceEvidence(-1, false, false))
                        .kind());
        assertEquals(PulsarAttemptJournal.ResolutionKind.NOT_PUBLISHED,
                journal.resolve(producer, new PulsarAttemptJournal.BrokerSequenceEvidence(-1, true, true))
                        .kind());
        final PulsarAttemptJournal.Resolution latePublication = journal.resolve(producer,
                new PulsarAttemptJournal.BrokerSequenceEvidence(mapping.sequenceId(), true, true));
        assertEquals(PulsarAttemptJournal.ResolutionKind.DIVERGENCE, latePublication.kind());
        assertEquals(StableCode.PULSAR_EVIDENCE_DIVERGENCE, latePublication.stableCode());
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

    @Test
    void retiredMappingProjectsStrictJournalAbsenceEvidence() {
        final ShardId shard = shard();
        final PulsarAttemptJournal.ProducerKey producer = producer();
        final AtomicLong entry = new AtomicLong(90);
        final PulsarJournalResource journalResource = new PulsarJournalResource("cluster", bytes(32, 4),
                "persistent://nereus/system/attempt-journal", 7, shard.partition());
        final PulsarAttemptJournal journal = new PulsarAttemptJournal(shard,
                request -> position(entry.getAndIncrement()), journalResource);
        final PulsarAttemptJournal.Mapping mapping = journal.appendNext(producer, identity(shard, 12))
                .record().mapping();
        journal.retireNotPublished(mapping.mappingId());
        final ChannelResourceIdentityV1 channel = fencedJournalChannel(producer, 3, journalResource);
        final byte[] retirementBarrier = Bytes.sha256(Bytes.utf8("retirement-barrier"));
        assertEquals(shard.partition(), journal.evidenceCursor(producer, 3).orElseThrow().physicalPartition());

        final io.nereusstream.delay.protocol.PublishEvidenceV1 evidence = journal.notPublishedEvidence(mapping, 3,
                channel, retirementBarrier);
        assertEquals(io.nereusstream.delay.protocol.PublishEvidenceKindV1.PULSAR_JOURNAL_ABSENCE,
                evidence.evidenceKind());
        assertEquals(io.nereusstream.delay.protocol.EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED,
                evidence.verificationStatus());
        evidence.requireBusinessMutation(mapping.publishAttemptId(), false);
        assertArrayEquals(evidence.canonicalBytes(),
                io.nereusstream.delay.protocol.PublishEvidenceV1.decode(evidence.canonicalBytes()).canonicalBytes());

        final ChannelResourceIdentityV1 wrongLane = fencedJournalChannel(
                new PulsarAttemptJournal.ProducerKey(new DestinationLaneId(bytes(32, 99)),
                        producer.laneIncarnation(), producer.stableProducerNameHash(), producer.target()), 3,
                journalResource);
        assertThrows(PulsarAttemptJournal.JournalException.class,
                () -> journal.notPublishedEvidence(mapping, 3, wrongLane, retirementBarrier));
    }

    @Test
    void journalResourceProjectsTypedRegistryGenerationIdentity() {
        final PulsarJournalResource resource = new PulsarJournalResource("cluster", bytes(32, 21),
                "persistent://nereus/system/attempt-journal", Long.MIN_VALUE, 7);

        final PulsarJournalGenerationResourceV1 typed = resource.protocolResource(Long.MIN_VALUE);
        assertEquals(resource.partition(), typed.partition());
        assertEquals(Long.MIN_VALUE, typed.evidenceGeneration());
        assertEquals(typed, PulsarJournalGenerationResourceV1.decode(typed.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.PULSAR_JOURNAL_GENERATION,
                        resource.exactResourceCanonicalBytes(Long.MIN_VALUE));
        assertArrayEquals(resource.exactResourceCanonicalBytes(Long.MIN_VALUE), identity.canonicalBytes());
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

    private static ChannelResourceIdentityV1 fencedJournalChannel(
            final PulsarAttemptJournal.ProducerKey producer, final long evidenceGeneration,
            final PulsarJournalResource journalResource) {
        final PulsarTargetResource target = producer.target();
        final BrokerResourceIdentityV1 targetBroker = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(target.authenticatedClusterId(), target.resourceIncarnation(),
                        target.physicalTopic(), target.physicalTopicCreationTimestamp()));
        final BrokerResourceIdentityV1 evidenceBroker = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(journalResource.authenticatedClusterId(),
                        journalResource.resourceIncarnation(), journalResource.physicalTopic(),
                        journalResource.physicalTopicCreationTimestamp()));
        final byte[] producerIdentity = Bytes.utf8("stable-producer");
        final byte[] guardDigest = Bytes.sha256(Bytes.utf8("journal-guard"));
        final byte[] bindingDigest = Bytes.sha256(Bytes.utf8("journal-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("journal-fingerprint"));
        final byte[] prefix = io.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, 1, AdapterKindV1.PULSAR.wireValue());
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, 2,
                    ChannelKindV1.PULSAR_DEDUP_PRODUCER.wireValue());
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 3, producer.laneId().bytes());
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 4, producer.laneIncarnation());
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 5, targetBroker.canonicalBytes());
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, 6, target.partition());
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint64(output, 7, 1);
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, 8, 0);
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 9, producerIdentity);
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producerIdentity));
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 11, evidenceBroker.canonicalBytes());
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint64(output, 12, evidenceGeneration);
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 13, guardDigest);
        });
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("journal-destination"), 1,
                Bytes.sha256(Bytes.utf8("journal-destination-semantic")), ProfileKindV1.DESTINATION);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(1_000, 1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("journal-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("journal-time")), 0, null);
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(profile,
                CredentialUseKindV1.DESTINATION_CHANNEL,
                CredentialUseLeaseV1.destinationChannelHolderScope(prefix), 1, bindingDigest, fingerprint,
                issuedAt, 9_000, 1);
        return new ChannelResourceIdentityV1(AdapterKindV1.PULSAR, ChannelKindV1.PULSAR_DEDUP_PRODUCER,
                producer.laneId().bytes(), producer.laneIncarnation(), targetBroker, target.partition(), 1, 0,
                producerIdentity, Bytes.sha256(producerIdentity), evidenceBroker, evidenceGeneration, guardDigest, 1,
                bindingDigest, fingerprint, lease);
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
