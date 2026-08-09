package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ChannelKindV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Local protocol seam for the V1 Pulsar Attempt Journal.
 *
 * <p>The real implementation is a Nereus-owned, non-compacted Pulsar topic.
 * This class deliberately models only the ordering and evidence invariants
 * that can be checked without a Broker: a mapping is visible only after the
 * injected appender returns a journal position, a Producer has at most one
 * non-retired mapping, exact replay is idempotent, and insufficient Broker
 * evidence fails closed. It is not a Pulsar transport or a durability claim.</p>
 */
public final class PulsarAttemptJournal {
    private static final int HASH_LENGTH = 32;
    private static final int LANE_INCARNATION_LENGTH = 16;
    private static final byte[] MAPPING_ID_DOMAIN = Bytes.utf8(
            "nereus-delay-pulsar-attempt-journal-mapping-id-v1\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8(
            "nereus-delay-pulsar-attempt-journal-record-v1\0");

    private final ShardId shard;
    private final DurableAppender appender;
    private final PulsarJournalResource journalResource;
    private final Map<String, MappingState> mappings = new HashMap<>();
    private final Map<ProducerKey, ProducerState> producers = new HashMap<>();
    private final List<JournalRecord> records = new ArrayList<>();
    private JournalPosition lastPosition;

    /** Creates a deterministic in-memory appender for local tests only. */
    public PulsarAttemptJournal(final ShardId shard) {
        this(shard, new LocalAppender(), null);
    }

    /** Creates a journal seam with an injected Broker-like durable append. */
    public PulsarAttemptJournal(final ShardId shard, final DurableAppender appender) {
        this(shard, appender, null);
    }

    /** Creates a journal seam with its explicit physical Journal identity. */
    public PulsarAttemptJournal(final ShardId shard, final DurableAppender appender,
                                final PulsarJournalResource journalResource) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.appender = Objects.requireNonNull(appender, "appender");
        if (journalResource != null && journalResource.partition() != shard.partition()) {
            throw new IllegalArgumentException("Attempt Journal partition does not match Shard partition");
        }
        this.journalResource = journalResource;
    }

    /** Allocates and durably appends the next sequence mapping in one local turn. */
    public synchronized AppendResult appendNext(final ProducerKey producer, final AttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        final ProducerState state = producers.get(producer);
        if (state != null && state.unresolvedMappingId != null) {
            throw conflict("an unresolved lower sequence blocks this Producer");
        }
        final long sequenceId;
        try {
            sequenceId = state == null ? 0 : Math.addExact(state.lastSequenceId, 1);
        } catch (ArithmeticException overflow) {
            throw conflict("Pulsar sequence domain exhausted");
        }
        final Mapping mapping = Mapping.create(shard, producer, sequenceId, identity);
        return appendMappedInternal(mapping);
    }

    /**
     * Appends an exact mapping after the caller has fixed its sequence. A
     * repeated exact mapping is idempotent and does not append a second record.
     */
    public synchronized AppendResult appendMapped(final Mapping mapping) {
        Objects.requireNonNull(mapping, "mapping");
        return appendMappedInternal(mapping);
    }

    /**
     * Reuses the exact non-retired mapping for a retransmission of one
     * admitted attempt, or allocates the next sequence when this attempt has
     * not reached the Journal yet.  The attempt ID is not a lookup hint: it
     * is an immutable identity fence.  Reusing it with a different Producer
     * or any different mapping field is an integrity failure.
     */
    public synchronized AppendResult appendOrReuse(final ProducerKey producer,
                                                    final AttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        MappingState matching = null;
        for (MappingState candidate : mappings.values()) {
            if (!Arrays.equals(candidate.mapping.publishAttemptId(), identity.publishAttemptId())) {
                continue;
            }
            if (!candidate.mapping.producer().equals(producer)
                    || !sameAttemptIdentity(candidate.mapping, identity)) {
                throw conflict("publish attempt identity was reused with different Journal mapping bytes");
            }
            matching = candidate;
            break;
        }
        if (matching != null) {
            if (matching.retired) {
                throw conflict("retired publish attempt cannot be sent again");
            }
            return new AppendResult(matching.mappedRecord, true);
        }
        return appendNext(producer, identity);
    }

    /**
     * Durable mapping-before-send entry point for a prepared attempt.  A
     * retransmission reuses the same sequence and mapping record; the target
     * sender is never invoked until the Journal append/replay gate succeeds.
     */
    public <T> CompletionStage<T> sendAfterMapped(final ProducerKey producer,
                                                   final AttemptIdentity identity,
                                                   final TargetSender<T> sender) {
        final AppendResult mapping = appendOrReuse(producer, identity);
        return sendAfterMapped(mapping.record().mapping(), sender);
    }

    /** Appends the durable retirement marker required before a later sequence. */
    public synchronized AppendResult retireNotPublished(final byte[] mappingId) {
        Bytes.requireLength(mappingId, HASH_LENGTH, "mappingId");
        final MappingState state = mappings.get(Bytes.hex(mappingId));
        if (state == null) {
            throw conflict("unknown mapping cannot be retired");
        }
        if (state.retired) {
            return new AppendResult(state.retirementRecord, true);
        }
        final JournalPosition position = append(RecordKind.RETIRED_NOT_PUBLISHED, state.mapping);
        final JournalRecord record = new JournalRecord(RecordKind.RETIRED_NOT_PUBLISHED, state.mapping, position);
        state.retired = true;
        state.retirementRecord = record;
        state.producer.unresolvedMappingId = null;
        records.add(record);
        lastPosition = position;
        return new AppendResult(record, false);
    }

    /** Replays one already-durable Journal record during local recovery. */
    public synchronized void replay(final JournalRecord record) {
        Objects.requireNonNull(record, "record");
        requireShard(record.mapping());
        final String mappingId = Bytes.hex(record.mapping().mappingId());
        final MappingState current = mappings.get(mappingId);
        if (current != null) {
            if (!current.mapping.sameCanonical(record.mapping())) {
                throw conflict("Attempt Journal mapping id/body conflict");
            }
            if (record.kind() == RecordKind.MAPPED) {
                if (current.mappedRecord.position().equals(record.position())) {
                    return;
                }
                throw conflict("Attempt Journal mapped record replay conflict");
            }
            if (current.retired) {
                if (current.retirementRecord.position().equals(record.position())) {
                    return;
                }
                throw conflict("Attempt Journal retirement record replay conflict");
            }
            validatePosition(record.position());
            current.retired = true;
            current.retirementRecord = record;
            current.producer.unresolvedMappingId = null;
            records.add(record);
            lastPosition = record.position();
            return;
        }
        if (record.kind() != RecordKind.MAPPED) {
            throw conflict("retirement record has no mapped predecessor");
        }
        validatePosition(record.position());
        appendState(record.mapping(), record.position());
    }

    /** Returns the current unresolved mapping, if any, for one Producer. */
    public synchronized Optional<Mapping> unresolved(final ProducerKey producer) {
        Objects.requireNonNull(producer, "producer");
        final ProducerState state = producers.get(producer);
        if (state == null || state.unresolvedMappingId == null) {
            return Optional.empty();
        }
        return Optional.of(mappings.get(state.unresolvedMappingId).mapping);
    }

    /** Returns an immutable snapshot in Journal append order. */
    public synchronized List<JournalRecord> records() {
        return List.copyOf(records);
    }

    /**
     * Projects the latest local Journal position for one Lane-scoped Producer
     * into the typed checkpoint evidence cursor.  The cursor is only a local
     * value projection: a real reader must still prove Broker retention,
     * resource identity and contiguous replay before publishing it in a
     * checkpoint manifest.
     */
    public synchronized Optional<EvidenceCursorV1> evidenceCursor(final ProducerKey producer,
                                                                    final long evidenceGeneration) {
        Objects.requireNonNull(producer, "producer");
        if (evidenceGeneration == 0) {
            throw new IllegalArgumentException("evidenceGeneration must be non-zero");
        }
        JournalRecord latest = null;
        long maxBrokerPersistedAt = 0;
        for (JournalRecord record : records) {
            if (!record.mapping().producer().equals(producer)) {
                continue;
            }
            latest = record;
            maxBrokerPersistedAt = Math.max(maxBrokerPersistedAt,
                    record.position().brokerEntryTimestampEpochMs());
        }
        if (latest == null) {
            return Optional.empty();
        }
        final JournalPosition position = latest.position();
        final PulsarTargetResource target = producer.target();
        final String physicalTopic = journalResource == null
                ? target.physicalTopic() : journalResource.physicalTopic();
        final long physicalTopicCreationTimestamp = journalResource == null
                ? target.physicalTopicCreationTimestamp() : journalResource.physicalTopicCreationTimestamp();
        final byte[] resourceIncarnation = journalResource == null
                ? target.resourceIncarnation() : journalResource.resourceIncarnation();
        final int physicalPartition = journalResource == null ? target.partition() : journalResource.partition();
        return Optional.of(EvidenceCursorV1.pulsar(producer.laneId().bytes(), producer.laneIncarnation(),
                resourceIncarnation, physicalPartition, evidenceGeneration, maxBrokerPersistedAt,
                physicalTopic, physicalTopicCreationTimestamp, position.ledgerId(),
                position.entryId(), position.batchIndex(), position.batchSize()));
    }

    /**
     * Builds the canonical local PUBLISHED Journal evidence branch for one
     * still-live mapping. The returned value is suitable for local codec and
     * identity tests only; Broker acknowledgement, guard attestation and
     * retention authority remain outside this class.
     */
    public synchronized PublishEvidenceV1 publishedEvidence(final Mapping mapping, final long evidenceGeneration,
                                                             final byte[] targetAckEvidenceId) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
        if (state == null || !state.mapping.sameCanonical(mapping)) {
            throw conflict("published evidence has no exact Journal mapping");
        }
        if (state.retired) {
            throw conflict("retired Journal mapping cannot produce PUBLISHED evidence");
        }
        final EvidenceCursorV1 cursor = evidenceCursor(mapping.producer(), evidenceGeneration).orElseThrow(() ->
                conflict("published evidence has no Journal cursor"));
        if (targetAckEvidenceId != null) {
            Bytes.requireLength(targetAckEvidenceId, HASH_LENGTH, "targetAckEvidenceId");
        }
        final JournalRecord record = state.mappedRecord;
        final JournalPosition position = record.position();
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, cursor.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, position.ledgerId());
            CanonicalProtobuf.uint64Bits(output, 3, position.entryId());
            CanonicalProtobuf.uint32Bits(output, 4, position.batchIndex());
            CanonicalProtobuf.bytes(output, 5, ExternalDeliveryIdentityV1.publishAttempt(mapping.publishAttemptId())
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 7, mapping.producer().stableProducerNameHash());
            CanonicalProtobuf.uint64Bits(output, 8, mapping.sequenceId());
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(record.canonicalBytes()));
            if (targetAckEvidenceId != null) {
                CanonicalProtobuf.bytes(output, 10, targetAckEvidenceId);
            }
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.PULSAR_ATTEMPT_JOURNAL,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch);
    }

    /**
     * Builds the canonical local VERIFIED_NOT_PUBLISHED Journal-absence
     * branch after an exact retirement record is durable.  The channel and
     * barrier digest are caller-supplied proofs from the fenced adapter; this
     * class validates their identity binding, but cannot authenticate a
     * Pulsar fencing response or prove remote retention.
     */
    public synchronized PublishEvidenceV1 notPublishedEvidence(
            final Mapping mapping, final long evidenceGeneration,
            final ChannelResourceIdentityV1 fencedChannel,
            final byte[] retirementBarrierEvidence) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        Objects.requireNonNull(fencedChannel, "fencedChannel");
        Bytes.requireLength(retirementBarrierEvidence, HASH_LENGTH, "retirementBarrierEvidence");
        if (evidenceGeneration == 0) {
            throw new IllegalArgumentException("evidenceGeneration must be non-zero");
        }
        final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
        if (state == null || !state.mapping.sameCanonical(mapping)) {
            throw conflict("not-published evidence has no exact Journal mapping");
        }
        if (!state.retired || state.retirementRecord == null) {
            throw conflict("not-published evidence requires a durable Journal retirement");
        }
        final EvidenceCursorV1 cursor = evidenceCursor(mapping.producer(), evidenceGeneration).orElseThrow(() ->
                conflict("not-published evidence has no Journal cursor"));
        validateFencedJournalChannel(mapping.producer(), fencedChannel, cursor, evidenceGeneration,
                shard.partition());
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, cursor.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, fencedChannel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, ExternalDeliveryIdentityV1.publishAttempt(mapping.publishAttemptId())
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 5, mapping.producer().stableProducerNameHash());
            CanonicalProtobuf.uint64(output, 6, mapping.sequenceId());
            CanonicalProtobuf.bytes(output, 7, retirementBarrierEvidence);
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.PULSAR_JOURNAL_ABSENCE,
                EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED, branch);
    }

    private static void validateFencedJournalChannel(final ProducerKey producer,
                                                      final ChannelResourceIdentityV1 channel,
                                                      final EvidenceCursorV1 cursor,
                                                      final long evidenceGeneration,
                                                      final int journalPartition) {
        if (channel.adapterKind() != AdapterKindV1.PULSAR
                || channel.channelKind() != ChannelKindV1.PULSAR_DEDUP_PRODUCER) {
            throw conflict("Journal absence requires a fenced Pulsar dedup channel");
        }
        if (!Arrays.equals(channel.destinationLaneId(), producer.laneId().bytes())
                || !Arrays.equals(channel.laneIncarnation(), producer.laneIncarnation())
                || !Arrays.equals(channel.producerOrTransactionalIdentitySha256(),
                producer.stableProducerNameHash())) {
            throw conflict("fenced Journal channel is bound to another Lane or Producer");
        }
        if (channel.physicalPartition() != producer.target().partition()
                || channel.evidenceGeneration() == null
                || channel.evidenceGeneration() != evidenceGeneration) {
            throw conflict("fenced Journal channel partition/generation does not match evidence cursor");
        }
        final BrokerResourceIdentityV1 expectedTarget = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(producer.target().authenticatedClusterId(),
                        producer.target().resourceIncarnation(), producer.target().physicalTopic(),
                        producer.target().physicalTopicCreationTimestamp()));
        if (!expectedTarget.equals(channel.targetResource())) {
            throw conflict("fenced Journal channel target identity differs from Producer target");
        }
        if (cursor.evidenceKind() != io.nereusstream.delay.protocol.EvidenceKindV1.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS
                || cursor.evidenceGeneration() != evidenceGeneration
                || cursor.physicalPartition() != journalPartition) {
            throw conflict("Journal cursor identity does not match the fenced channel");
        }
        final BrokerResourceIdentityV1 evidenceResource = channel.evidenceResource();
        if (evidenceResource == null || evidenceResource.kind() != BrokerResourceIdentityV1.Kind.PULSAR) {
            throw conflict("fenced Journal channel has no Pulsar evidence resource");
        }
        final PulsarBrokerResourceIdentityV1 evidence = evidenceResource.pulsar();
        if (!Arrays.equals(cursor.resourceToken(), evidence.resourceIncarnation())
                || !cursor.physicalTopic().equals(evidence.physicalTopic())
                || cursor.physicalTopicCreationTimestamp() != evidence.physicalTopicCreationTimestamp()) {
            throw conflict("Journal cursor identity differs from the fenced evidence resource");
        }
    }

    /**
     * Resolves the latest mapping for a Producer from a physical last-sequence
     * observation. A lower value is proof only when both retention predicates
     * are true; otherwise the result is the typed divergence branch.
     */
    public synchronized Resolution resolve(final ProducerKey producer, final BrokerSequenceEvidence evidence) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(evidence, "evidence");
        if (evidence.brokerLastSequenceId() < -1) {
            return Resolution.divergence(null, "invalid Broker last sequence");
        }
        final ProducerState state = producers.get(producer);
        if (state == null) {
            return Resolution.empty();
        }
        final MappingState latest = mappings.get(state.lastMappingId);
        if (latest == null) {
            return Resolution.divergence(null, "Producer index lost its latest mapping");
        }
        if (evidence.brokerLastSequenceId() >= 0
                && Long.compareUnsigned(evidence.brokerLastSequenceId(), state.lastSequenceId) > 0) {
            return Resolution.divergence(latest.mapping, "Broker sequence is above the Journal maximum");
        }
        if (latest.retired) {
            return Resolution.notPublished(latest.mapping);
        }
        if (evidence.brokerLastSequenceId() >= 0
                && Long.compareUnsigned(evidence.brokerLastSequenceId(), latest.mapping.sequenceId()) >= 0) {
            return Resolution.published(latest.mapping);
        }
        if (evidence.inactivityHorizonValid() && evidence.producerSnapshotRetained()) {
            return Resolution.notPublished(latest.mapping);
        }
        return Resolution.divergence(latest.mapping, "Broker lower sequence lacks retention proof");
    }

    /** Allows a target SEND only after the exact mapping has become durable. */
    public <T> CompletionStage<T> sendAfterMapped(final Mapping mapping, final TargetSender<T> sender) {
        Objects.requireNonNull(mapping, "mapping");
        Objects.requireNonNull(sender, "sender");
        synchronized (this) {
            requireShard(mapping);
            final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
            if (state == null || !state.mapping.sameCanonical(mapping) || state.retired) {
                throw conflict("target SEND has no exact durable non-retired mapping");
            }
        }
        return sender.send(mapping);
    }

    private AppendResult appendMappedInternal(final Mapping mapping) {
        requireShard(mapping);
        final String mappingId = Bytes.hex(mapping.mappingId());
        final MappingState current = mappings.get(mappingId);
        if (current != null) {
            if (!current.mapping.sameCanonical(mapping)) {
                throw conflict("Attempt Journal mapping id/body conflict");
            }
            return new AppendResult(current.mappedRecord, true);
        }
        final ProducerState state = producers.get(mapping.producer());
        if (state != null && state.unresolvedMappingId != null) {
            throw conflict("an unresolved lower sequence blocks this Producer");
        }
        final long expectedSequence = state == null ? 0 : nextSequence(state.lastSequenceId);
        if (mapping.sequenceId() != expectedSequence) {
            throw conflict("mapping sequence is not the next Producer sequence");
        }
        final JournalPosition position = append(RecordKind.MAPPED, mapping);
        appendState(mapping, position);
        return new AppendResult(new JournalRecord(RecordKind.MAPPED, mapping, position), false);
    }

    private void appendState(final Mapping mapping, final JournalPosition position) {
        final ProducerState state = producers.computeIfAbsent(mapping.producer(), ignored -> new ProducerState());
        if (state.unresolvedMappingId != null) {
            throw conflict("replay would create two unresolved mappings");
        }
        final long expectedSequence = state.lastSequenceId < 0 ? 0 : nextSequence(state.lastSequenceId);
        if (mapping.sequenceId() != expectedSequence) {
            throw conflict("replayed mapping sequence is not the next Producer sequence");
        }
        final JournalRecord record = new JournalRecord(RecordKind.MAPPED, mapping, position);
        final MappingState value = new MappingState(mapping, state, record);
        mappings.put(Bytes.hex(mapping.mappingId()), value);
        state.lastMappingId = Bytes.hex(mapping.mappingId());
        state.lastSequenceId = mapping.sequenceId();
        state.unresolvedMappingId = state.lastMappingId;
        records.add(record);
        lastPosition = position;
    }

    private static boolean sameAttemptIdentity(final Mapping mapping, final AttemptIdentity identity) {
        return mapping.delayMessageId().equals(identity.delayMessageId())
                && mapping.generation() == identity.generation()
                && Arrays.equals(mapping.publishAttemptId(), identity.publishAttemptId())
                && Arrays.equals(mapping.preparedPublishHash(), identity.preparedPublishHash())
                && mapping.guardedBrokerTimestampEpochMs() == identity.guardedBrokerTimestampEpochMs()
                && Arrays.equals(mapping.sourcePosition(), identity.sourcePosition());
    }

    private JournalPosition append(final RecordKind kind, final Mapping mapping) {
        final JournalPosition position;
        try {
            position = appender.append(new AppendRequest(kind, mapping));
        } catch (RuntimeException failure) {
            throw failure;
        }
        if (position == null) {
            throw new JournalException(StableCode.PULSAR_EVIDENCE_DIVERGENCE,
                    "Journal appender returned no durable position");
        }
        validatePosition(position);
        return position;
    }

    private void validatePosition(final JournalPosition position) {
        Objects.requireNonNull(position, "position");
        if (lastPosition != null && position.compareTo(lastPosition) <= 0) {
            throw new JournalException(StableCode.PULSAR_EVIDENCE_DIVERGENCE,
                    "Journal position is not strictly increasing");
        }
    }

    private void requireShard(final Mapping mapping) {
        if (!shard.equals(mapping.shard())) {
            throw conflict("mapping belongs to another Shard");
        }
    }

    private static long nextSequence(final long lastSequenceId) {
        try {
            return Math.addExact(lastSequenceId, 1);
        } catch (ArithmeticException overflow) {
            throw conflict("Pulsar sequence domain exhausted");
        }
    }

    private static JournalException conflict(final String message) {
        return new JournalException(StableCode.INTEGRITY_ERROR, message);
    }

    @FunctionalInterface
    public interface DurableAppender {
        JournalPosition append(AppendRequest request);
    }

    @FunctionalInterface
    public interface TargetSender<T> {
        CompletionStage<T> send(Mapping mapping);
    }

    public enum RecordKind {
        MAPPED(1),
        RETIRED_NOT_PUBLISHED(2);

        private final int wireValue;

        RecordKind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }
    }

    public enum ResolutionKind {
        EMPTY,
        PUBLISHED,
        NOT_PUBLISHED,
        DIVERGENCE
    }

    public record AppendRequest(RecordKind kind, Mapping mapping) {
        public AppendRequest {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mapping, "mapping");
        }
    }

    public record AppendResult(JournalRecord record, boolean idempotent) {
        public AppendResult {
            Objects.requireNonNull(record, "record");
        }
    }

    public record BrokerSequenceEvidence(long brokerLastSequenceId,
                                         boolean producerSnapshotRetained,
                                         boolean inactivityHorizonValid) {
        public BrokerSequenceEvidence {
            if (brokerLastSequenceId < -1) {
                throw new IllegalArgumentException("brokerLastSequenceId must be -1 or non-negative");
            }
        }
    }

    public record Resolution(ResolutionKind kind, Mapping mapping, StableCode stableCode, String detail) {
        public Resolution {
            Objects.requireNonNull(kind, "kind");
            if (kind == ResolutionKind.DIVERGENCE && stableCode != StableCode.PULSAR_EVIDENCE_DIVERGENCE) {
                throw new IllegalArgumentException("divergence must use PULSAR_EVIDENCE_DIVERGENCE");
            }
            if (kind != ResolutionKind.DIVERGENCE && stableCode != null) {
                throw new IllegalArgumentException("non-divergence resolution cannot carry a stable code");
            }
        }

        private static Resolution empty() {
            return new Resolution(ResolutionKind.EMPTY, null, null, null);
        }

        private static Resolution published(final Mapping mapping) {
            return new Resolution(ResolutionKind.PUBLISHED, mapping, null, null);
        }

        private static Resolution notPublished(final Mapping mapping) {
            return new Resolution(ResolutionKind.NOT_PUBLISHED, mapping, null, null);
        }

        private static Resolution divergence(final Mapping mapping, final String detail) {
            return new Resolution(ResolutionKind.DIVERGENCE, mapping, StableCode.PULSAR_EVIDENCE_DIVERGENCE,
                    detail);
        }
    }

    public record JournalPosition(long ledgerId, long entryId, int batchIndex, int batchSize,
                                  long brokerEntryTimestampEpochMs) implements Comparable<JournalPosition> {
        public JournalPosition {
            if (brokerEntryTimestampEpochMs < 0 || batchSize == 0
                    || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
                throw new IllegalArgumentException("invalid Attempt Journal position");
            }
        }

        @Override
        public int compareTo(final JournalPosition other) {
            Objects.requireNonNull(other, "other");
            int comparison = Long.compareUnsigned(ledgerId, other.ledgerId);
            if (comparison == 0) {
                comparison = Long.compareUnsigned(entryId, other.entryId);
            }
            if (comparison == 0) {
                comparison = Integer.compareUnsigned(batchIndex, other.batchIndex);
            }
            return comparison;
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(Bytes.u64beBits(ledgerId), Bytes.u64beBits(entryId), Bytes.u32beBits(batchIndex),
                    Bytes.u32beBits(batchSize), Bytes.i64be(brokerEntryTimestampEpochMs));
        }
    }

    public record JournalRecord(RecordKind kind, Mapping mapping, JournalPosition position) {
        public JournalRecord {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mapping, "mapping");
            Objects.requireNonNull(position, "position");
            if (kind == RecordKind.RETIRED_NOT_PUBLISHED && mapping == null) {
                throw new IllegalArgumentException("retirement record requires a mapping");
            }
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(RECORD_DOMAIN, Bytes.u8(kind.wireValue()), mapping.canonicalBytes(),
                    position.canonicalBytes());
        }
    }

    public record ProducerKey(DestinationLaneId laneId, byte[] laneIncarnation, byte[] stableProducerNameHash,
                              PulsarTargetResource target) {
        public ProducerKey {
            Objects.requireNonNull(laneId, "laneId");
            Bytes.requireLength(laneIncarnation, LANE_INCARNATION_LENGTH, "laneIncarnation");
            Bytes.requireLength(stableProducerNameHash, HASH_LENGTH, "stableProducerNameHash");
            Objects.requireNonNull(target, "target");
            laneIncarnation = Bytes.copy(laneIncarnation);
            stableProducerNameHash = Bytes.copy(stableProducerNameHash);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] stableProducerNameHash() {
            return Bytes.copy(stableProducerNameHash);
        }

        private byte[] canonicalBytes() {
            return Bytes.concat(laneId.bytes(), laneIncarnation, stableProducerNameHash,
                    Bytes.lp32(Bytes.utf8(target.authenticatedClusterId())),
                    Bytes.lp32(target.resourceIncarnation()), Bytes.lp32(Bytes.utf8(target.physicalTopic())),
                    Bytes.u64beBits(target.physicalTopicCreationTimestamp()),
                    Bytes.u32beBits(target.partition()));
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof ProducerKey that)) {
                return false;
            }
            return laneId.equals(that.laneId) && target.authenticatedClusterId().equals(
                    that.target.authenticatedClusterId()) && target.physicalTopic().equals(that.target.physicalTopic())
                    && target.physicalTopicCreationTimestamp() == that.target.physicalTopicCreationTimestamp()
                    && target.partition() == that.target.partition()
                    && Arrays.equals(laneIncarnation, that.laneIncarnation)
                    && Arrays.equals(stableProducerNameHash, that.stableProducerNameHash)
                    && Arrays.equals(target.resourceIncarnation(), that.target.resourceIncarnation());
        }

        @Override
        public int hashCode() {
            return Bytes.hex(canonicalBytes()).hashCode();
        }
    }

    public record AttemptIdentity(DelayMessageId delayMessageId, int generation, byte[] publishAttemptId,
                                  byte[] preparedPublishHash, long guardedBrokerTimestampEpochMs,
                                  byte[] sourcePosition) {
        public AttemptIdentity {
            Objects.requireNonNull(delayMessageId, "delayMessageId");
            if (generation < 0 || guardedBrokerTimestampEpochMs < 0) {
                throw new IllegalArgumentException("invalid Attempt Journal mapping identity");
            }
            Bytes.requireLength(publishAttemptId, HASH_LENGTH, "publishAttemptId");
            Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            if (sourcePosition == null || sourcePosition.length == 0) {
                throw new IllegalArgumentException("sourcePosition must be non-empty");
            }
            publishAttemptId = Bytes.copy(publishAttemptId);
            preparedPublishHash = Bytes.copy(preparedPublishHash);
            sourcePosition = Bytes.copy(sourcePosition);
        }

        @Override
        public byte[] publishAttemptId() {
            return Bytes.copy(publishAttemptId);
        }

        @Override
        public byte[] preparedPublishHash() {
            return Bytes.copy(preparedPublishHash);
        }

        @Override
        public byte[] sourcePosition() {
            return Bytes.copy(sourcePosition);
        }
    }

    public static final class Mapping {
        private final ShardId shard;
        private final ProducerKey producer;
        private final long sequenceId;
        private final DelayMessageId delayMessageId;
        private final int generation;
        private final byte[] publishAttemptId;
        private final byte[] preparedPublishHash;
        private final long guardedBrokerTimestampEpochMs;
        private final byte[] sourcePosition;
        private final byte[] mappingId;

        private Mapping(final ShardId shard, final ProducerKey producer, final long sequenceId,
                        final AttemptIdentity identity) {
            this.shard = Objects.requireNonNull(shard, "shard");
            this.producer = Objects.requireNonNull(producer, "producer");
            if (sequenceId < 0) {
                throw new IllegalArgumentException("sequenceId must be non-negative");
            }
            this.sequenceId = sequenceId;
            this.delayMessageId = identity.delayMessageId();
            this.generation = identity.generation();
            this.publishAttemptId = identity.publishAttemptId();
            this.preparedPublishHash = identity.preparedPublishHash();
            this.guardedBrokerTimestampEpochMs = identity.guardedBrokerTimestampEpochMs();
            this.sourcePosition = identity.sourcePosition();
            this.mappingId = Bytes.sha256(Bytes.concat(MAPPING_ID_DOMAIN, canonicalBody()));
        }

        public static Mapping create(final ShardId shard, final ProducerKey producer, final long sequenceId,
                                     final AttemptIdentity identity) {
            return new Mapping(shard, producer, sequenceId, identity);
        }

        public ShardId shard() {
            return shard;
        }

        public ProducerKey producer() {
            return producer;
        }

        public long sequenceId() {
            return sequenceId;
        }

        public DelayMessageId delayMessageId() {
            return delayMessageId;
        }

        public int generation() {
            return generation;
        }

        public byte[] publishAttemptId() {
            return Bytes.copy(publishAttemptId);
        }

        public byte[] preparedPublishHash() {
            return Bytes.copy(preparedPublishHash);
        }

        public long guardedBrokerTimestampEpochMs() {
            return guardedBrokerTimestampEpochMs;
        }

        public byte[] sourcePosition() {
            return Bytes.copy(sourcePosition);
        }

        public byte[] mappingId() {
            return Bytes.copy(mappingId);
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(Bytes.lp32(mappingId), canonicalBody());
        }

        private byte[] canonicalBody() {
            return Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition()),
                    producer.canonicalBytes(), Bytes.u64be(sequenceId), delayMessageId.bytes(),
                    Bytes.u32beBits(generation), publishAttemptId, preparedPublishHash,
                    Bytes.i64be(guardedBrokerTimestampEpochMs), Bytes.lp32(sourcePosition));
        }

        private boolean sameCanonical(final Mapping other) {
            return Arrays.equals(canonicalBytes(), other.canonicalBytes());
        }
    }

    public static final class JournalException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final StableCode stableCode;

        private JournalException(final StableCode stableCode, final String message) {
            super(message);
            this.stableCode = stableCode;
        }

        public StableCode stableCode() {
            return stableCode;
        }
    }

    private static final class MappingState {
        private final Mapping mapping;
        private final ProducerState producer;
        private final JournalRecord mappedRecord;
        private boolean retired;
        private JournalRecord retirementRecord;

        private MappingState(final Mapping mapping, final ProducerState producer, final JournalRecord mappedRecord) {
            this.mapping = mapping;
            this.producer = producer;
            this.mappedRecord = mappedRecord;
        }
    }

    private static final class ProducerState {
        private long lastSequenceId = -1;
        private String lastMappingId;
        private String unresolvedMappingId;
    }

    private static final class LocalAppender implements DurableAppender {
        private long nextEntry;

        @Override
        public JournalPosition append(final AppendRequest request) {
            final long entry = nextEntry++;
            return new JournalPosition(0, entry, 0, 1,
                    request.mapping().guardedBrokerTimestampEpochMs());
        }
    }
}
