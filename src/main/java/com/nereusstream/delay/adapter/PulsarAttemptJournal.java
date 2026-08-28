package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Local protocol seam for the Pulsar Attempt Journal.
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
    private static final byte[] MAPPING_ID_DOMAIN = Bytes.utf8("nereus-delay-pulsar-attempt-journal-mapping-id\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8("nereus-delay-pulsar-attempt-journal-record\0");

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

    /**
     * Creates the deterministic local appender at an explicit raw entry ID.
     * This package-private seam exists only for exhaustion-boundary tests; a
     * production Pulsar adapter must obtain Journal positions from the Broker.
     */
    PulsarAttemptJournal(final ShardId shard, final long initialEntry) {
        this(shard, new LocalAppender(initialEntry), null);
    }

    /** Creates a journal seam with an injected Broker-like durable append. */
    public PulsarAttemptJournal(final ShardId shard, final DurableAppender appender) {
        this(shard, appender, null);
    }

    /** Creates a journal seam with its explicit physical Journal identity. */
    public PulsarAttemptJournal(
            final ShardId shard, final DurableAppender appender, final PulsarJournalResource journalResource) {
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

    /** Allocates the current H3 mapping without the retired clock-shift field. */
    public synchronized AppendResult appendNextCurrent(
            final ProducerKey producer, final CurrentAttemptIdentity identity) {
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
        return appendMappedInternal(Mapping.createCurrent(shard, producer, sequenceId, identity));
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
     * not reached the Journal yet. The attempt ID is not a lookup hint: it
     * is an immutable identity fence. Reusing it with a different Producer
     * or any different mapping field is an integrity failure.
     */
    public synchronized AppendResult appendOrReuse(final ProducerKey producer, final AttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        MappingState matching = null;
        for (MappingState candidate : mappings.values()) {
            if (!Arrays.equals(candidate.mapping.publishAttemptId(), identity.publishAttemptId())) {
                continue;
            }
            if (!candidate.mapping.producer().equals(producer) || !sameAttemptIdentity(candidate.mapping, identity)) {
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

    /** Reuses or allocates an exact current H3 mapping for one attempt. */
    public synchronized AppendResult appendOrReuseCurrent(
            final ProducerKey producer, final CurrentAttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        MappingState matching = null;
        for (MappingState candidate : mappings.values()) {
            if (!Arrays.equals(candidate.mapping.publishAttemptId(), identity.publishAttemptId())) {
                continue;
            }
            if (!candidate.mapping.producer().equals(producer)
                    || !sameCurrentAttemptIdentity(candidate.mapping, identity)) {
                throw conflict("publish attempt identity was reused with different current Journal mapping bytes");
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
        return appendNextCurrent(producer, identity);
    }

    /**
     * Finds an exact current mapping in any Journal state without making it
     * sendable again. This is the recovery-only counterpart to
     * {@link #appendOrReuseCurrent}: a retired mapping remains observable so
     * a fresh Worker can re-emit the same source UNKNOWN hold, but it can
     * never be passed back through the SEND path.
     */
    public synchronized Optional<Mapping> findCurrent(
            final ProducerKey producer, final CurrentAttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        Mapping matching = null;
        for (MappingState candidate : mappings.values()) {
            if (!Arrays.equals(candidate.mapping.publishAttemptId(), identity.publishAttemptId())) {
                continue;
            }
            if (!candidate.mapping.producer().equals(producer)
                    || !sameCurrentAttemptIdentity(candidate.mapping, identity)) {
                throw conflict("publish attempt identity was reused with different current Journal mapping bytes");
            }
            matching = candidate.mapping;
            break;
        }
        return Optional.ofNullable(matching);
    }

    /**
     * Durable mapping-before-send entry point for a prepared attempt. A
     * retransmission reuses the same sequence and mapping record; the target
     * sender is never invoked until the Journal append/replay gate succeeds.
     */
    public <T> CompletionStage<T> sendAfterMapped(
            final ProducerKey producer, final AttemptIdentity identity, final TargetSender<T> sender) {
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
        if (state.ownershipStarted || state.published) {
            throw conflict("a mapping after ownership start cannot be retired as not published");
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

    /** Durably marks Producer ownership before invoking the physical SEND. */
    public synchronized AppendResult markOwnershipStarted(final byte[] mappingId) {
        final MappingState state = requireMapping(mappingId);
        if (state.published || state.retired) {
            throw conflict("terminal mapping cannot enter ownership");
        }
        if (state.ownershipStarted) {
            return new AppendResult(state.ownershipRecord, true);
        }
        final JournalPosition position = append(RecordKind.OWNERSHIP_STARTED, state.mapping);
        final JournalRecord record = new JournalRecord(RecordKind.OWNERSHIP_STARTED, state.mapping, position);
        state.ownershipStarted = true;
        state.ownershipRecord = record;
        records.add(record);
        lastPosition = position;
        return new AppendResult(record, false);
    }

    /** Durably records an authenticated Broker publication for the exact mapping. */
    public synchronized AppendResult markPublished(final byte[] mappingId) {
        final MappingState state = requireMapping(mappingId);
        if (state.retired) {
            throw conflict("retired mapping cannot become published");
        }
        if (!state.ownershipStarted) {
            throw conflict("PUBLISHED requires an OWNERSHIP_STARTED predecessor");
        }
        if (state.published) {
            return new AppendResult(state.publishedRecord, true);
        }
        final JournalPosition position = append(RecordKind.PUBLISHED, state.mapping);
        final JournalRecord record = new JournalRecord(RecordKind.PUBLISHED, state.mapping, position);
        state.published = true;
        state.publishedRecord = record;
        state.producer.unresolvedMappingId = null;
        records.add(record);
        lastPosition = position;
        return new AppendResult(record, false);
    }

    /** Returns the exact four-state H3 projection for one mapping. */
    public synchronized AttemptState state(final byte[] mappingId) {
        return requireMapping(mappingId).state();
    }

    /** Marks ownership for an exact mapping, including its immutable body fence. */
    public synchronized AppendResult markOwnershipStarted(final Mapping mapping) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        final MappingState state = requireMapping(mapping.mappingId());
        if (!state.mapping.sameCanonical(mapping)) {
            throw conflict("ownership marker mapping body conflict");
        }
        return markOwnershipStarted(mapping.mappingId());
    }

    /** Marks authenticated publication for an exact mapping, including its body fence. */
    public synchronized AppendResult markPublished(final Mapping mapping) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        final MappingState state = requireMapping(mapping.mappingId());
        if (!state.mapping.sameCanonical(mapping)) {
            throw conflict("published marker mapping body conflict");
        }
        return markPublished(mapping.mappingId());
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
            final JournalRecord prior = current.record(record.kind());
            // JournalPosition is reconstructed during replay; compare its
            // canonical bytes rather than relying on record identity.
            if (prior != null) {
                if (Arrays.equals(
                        prior.position().canonicalBytes(), record.position().canonicalBytes())) {
                    return;
                }
                throw conflict("Attempt Journal state record replay conflict");
            }
            validatePosition(record.position());
            switch (record.kind()) {
                case MAPPED -> throw conflict("duplicate mapped record replay conflict");
                case OWNERSHIP_STARTED -> {
                    if (current.retired || current.published || current.ownershipStarted) {
                        throw conflict("invalid OWNERSHIP_STARTED replay transition");
                    }
                    current.ownershipStarted = true;
                    current.ownershipRecord = record;
                }
                case PUBLISHED -> {
                    if (current.retired || current.published || !current.ownershipStarted) {
                        throw conflict("invalid PUBLISHED replay transition");
                    }
                    current.published = true;
                    current.publishedRecord = record;
                    current.producer.unresolvedMappingId = null;
                }
                case RETIRED_NOT_PUBLISHED -> {
                    if (current.retired || current.published || current.ownershipStarted) {
                        throw conflict("invalid RETIRED_NOT_PUBLISHED replay transition");
                    }
                    current.retired = true;
                    current.retirementRecord = record;
                    current.producer.unresolvedMappingId = null;
                }
            }
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
     * into the typed checkpoint evidence cursor. The cursor is only a local
     * value projection: a real reader must still prove Broker retention,
     * resource identity and contiguous replay before publishing it in a
     * checkpoint manifest.
     */
    public synchronized Optional<EvidenceCursor> evidenceCursor(
            final ProducerKey producer, final long evidenceGeneration) {
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
            maxBrokerPersistedAt =
                    Math.max(maxBrokerPersistedAt, record.position().brokerEntryTimestampEpochMs());
        }
        if (latest == null) {
            return Optional.empty();
        }
        final JournalPosition position = latest.position();
        final PulsarTargetResource target = producer.target();
        final String physicalTopic = journalResource == null ? target.physicalTopic() : journalResource.physicalTopic();
        final long physicalTopicCreationTimestamp = journalResource == null
                ? target.physicalTopicCreationTimestamp()
                : journalResource.physicalTopicCreationTimestamp();
        final byte[] resourceIncarnation =
                journalResource == null ? target.resourceIncarnation() : journalResource.resourceIncarnation();
        final int physicalPartition = journalResource == null ? target.partition() : journalResource.partition();
        return Optional.of(EvidenceCursor.pulsar(
                producer.laneId().bytes(),
                producer.laneIncarnation(),
                resourceIncarnation,
                physicalPartition,
                evidenceGeneration,
                maxBrokerPersistedAt,
                physicalTopic,
                physicalTopicCreationTimestamp,
                position.ledgerId(),
                position.entryId(),
                position.batchIndex(),
                position.batchSize()));
    }

    /**
     * Builds the canonical local PUBLISHED Journal evidence branch for one
     * still-live mapping. The returned value is suitable for local codec and
     * identity tests only; Broker acknowledgement, guard attestation and
     * retention authority remain outside this class.
     */
    public synchronized PublishEvidence publishedEvidence(
            final Mapping mapping, final long evidenceGeneration, final byte[] targetAckEvidenceId) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
        if (state == null || !state.mapping.sameCanonical(mapping)) {
            throw conflict("published evidence has no exact Journal mapping");
        }
        if (state.retired || !state.published || state.publishedRecord == null) {
            throw conflict("Journal mapping has no durable PUBLISHED record");
        }
        final EvidenceCursor cursor = evidenceCursor(mapping.producer(), evidenceGeneration)
                .orElseThrow(() -> conflict("published evidence has no Journal cursor"));
        if (targetAckEvidenceId != null) {
            Bytes.requireLength(targetAckEvidenceId, HASH_LENGTH, "targetAckEvidenceId");
        }
        final JournalRecord record = state.publishedRecord;
        final JournalPosition position = record.position();
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, cursor.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, position.ledgerId());
            CanonicalProtobuf.uint64Bits(output, 3, position.entryId());
            CanonicalProtobuf.uint32Bits(output, 4, position.batchIndex());
            CanonicalProtobuf.bytes(
                    output,
                    5,
                    ExternalDeliveryIdentity.publishAttempt(mapping.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 7, mapping.producer().stableProducerNameHash());
            CanonicalProtobuf.uint64Bits(output, 8, mapping.sequenceId());
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(record.canonicalBytes()));
            if (targetAckEvidenceId != null) {
                CanonicalProtobuf.bytes(output, 10, targetAckEvidenceId);
            }
        });
        return PublishEvidence.create(
                PublishEvidenceKind.PULSAR_ATTEMPT_JOURNAL, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    /**
     * Builds the canonical local VERIFIED_NOT_PUBLISHED Journal-absence
     * branch after an exact retirement record is durable. The channel and
     * barrier digest are caller-supplied proofs from the fenced adapter; this
     * class validates their identity binding, but cannot authenticate a
     * Pulsar fencing response or prove remote retention.
     */
    public synchronized PublishEvidence notPublishedEvidence(
            final Mapping mapping,
            final long evidenceGeneration,
            final ChannelResourceIdentity fencedChannel,
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
        final EvidenceCursor cursor = evidenceCursor(mapping.producer(), evidenceGeneration)
                .orElseThrow(() -> conflict("not-published evidence has no Journal cursor"));
        validateFencedJournalChannel(mapping.producer(), fencedChannel, cursor, evidenceGeneration, shard.partition());
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, cursor.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, fencedChannel.canonicalBytes());
            CanonicalProtobuf.bytes(
                    output,
                    3,
                    ExternalDeliveryIdentity.publishAttempt(mapping.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 5, mapping.producer().stableProducerNameHash());
            CanonicalProtobuf.uint64(output, 6, mapping.sequenceId());
            CanonicalProtobuf.bytes(output, 7, retirementBarrierEvidence);
        });
        return PublishEvidence.create(
                PublishEvidenceKind.PULSAR_JOURNAL_ABSENCE, EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, branch);
    }

    private static void validateFencedJournalChannel(
            final ProducerKey producer,
            final ChannelResourceIdentity channel,
            final EvidenceCursor cursor,
            final long evidenceGeneration,
            final int journalPartition) {
        if (channel.adapterKind() != AdapterKind.PULSAR || channel.channelKind() != ChannelKind.PULSAR_DEDUP_PRODUCER) {
            throw conflict("Journal absence requires a fenced Pulsar dedup channel");
        }
        if (!Arrays.equals(channel.destinationLaneId(), producer.laneId().bytes())
                || !Arrays.equals(channel.laneIncarnation(), producer.laneIncarnation())
                || !Arrays.equals(channel.producerOrTransactionalIdentitySha256(), producer.stableProducerNameHash())) {
            throw conflict("fenced Journal channel is bound to another Lane or Producer");
        }
        if (channel.physicalPartition() != producer.target().partition()
                || channel.evidenceGeneration() == null
                || channel.evidenceGeneration() != evidenceGeneration) {
            throw conflict("fenced Journal channel partition/generation does not match evidence cursor");
        }
        final BrokerResourceIdentity expectedTarget = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                producer.target().authenticatedClusterId(),
                producer.target().resourceIncarnation(),
                producer.target().physicalTopic(),
                producer.target().physicalTopicCreationTimestamp()));
        if (!expectedTarget.equals(channel.targetResource())) {
            throw conflict("fenced Journal channel target identity differs from Producer target");
        }
        if (cursor.evidenceKind() != com.nereusstream.delay.protocol.EvidenceKind.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS
                || cursor.evidenceGeneration() != evidenceGeneration
                || cursor.physicalPartition() != journalPartition) {
            throw conflict("Journal cursor identity does not match the fenced channel");
        }
        final BrokerResourceIdentity evidenceResource = channel.evidenceResource();
        if (evidenceResource == null || evidenceResource.kind() != BrokerResourceIdentity.Kind.PULSAR) {
            throw conflict("fenced Journal channel has no Pulsar evidence resource");
        }
        final PulsarBrokerResourceIdentity evidence = evidenceResource.pulsar();
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
        if (latest.published) {
            return Resolution.published(latest.mapping);
        }
        if (evidence.brokerLastSequenceId() >= 0
                && Long.compareUnsigned(evidence.brokerLastSequenceId(), latest.mapping.sequenceId()) >= 0) {
            if (latest.retired) {
                return Resolution.divergence(
                        latest.mapping, "retired mapping has a late Broker sequence acknowledging publication");
            }
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
            if (state == null
                    || !state.mapping.sameCanonical(mapping)
                    || state.retired
                    || state.ownershipStarted
                    || state.published) {
                throw conflict("target SEND has no exact durable non-retired mapping");
            }
        }
        final CompletionStage<T> result = sender.send(mapping);
        if (result == null) {
            // A null stage is not evidence of non-publication. Keep the exact
            // mapped attempt unresolved and force the caller through its
            // UNKNOWN/evidence path instead of leaking an untyped NPE.
            throw new JournalException(
                    StableCode.PULSAR_EVIDENCE_DIVERGENCE, "target sender returned no CompletionStage");
        }
        return result;
    }

    /** Allows physical SEND only after the exact ownership marker is durable. */
    public <T> CompletionStage<T> sendAfterOwnershipStarted(final Mapping mapping, final TargetSender<T> sender) {
        Objects.requireNonNull(mapping, "mapping");
        Objects.requireNonNull(sender, "sender");
        synchronized (this) {
            requireShard(mapping);
            final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
            if (state == null
                    || !state.mapping.sameCanonical(mapping)
                    || !state.ownershipStarted
                    || state.retired
                    || state.published) {
                throw conflict("target SEND requires an exact durable ownership marker");
            }
        }
        final CompletionStage<T> result = sender.send(mapping);
        if (result == null) {
            throw new JournalException(
                    StableCode.PULSAR_EVIDENCE_DIVERGENCE, "target sender returned no CompletionStage");
        }
        return result;
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
        rejectMixedMappingGeneration(state, mapping);
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
        rejectMixedMappingGeneration(state, mapping);
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

    private static boolean sameCurrentAttemptIdentity(final Mapping mapping, final CurrentAttemptIdentity identity) {
        return mapping.isCurrentGeneration()
                && mapping.delayMessageId().equals(identity.delayMessageId())
                && mapping.generation() == identity.generation()
                && Arrays.equals(mapping.publishAttemptId(), identity.publishAttemptId())
                && Arrays.equals(mapping.preparedPublishHash(), identity.preparedPublishHash())
                && Arrays.equals(mapping.recordTemplateHash(), identity.recordTemplateHash())
                && mapping.deliveryContract() == identity.deliveryContract()
                && Arrays.equals(mapping.sourcePosition(), identity.sourceAdmissionPosition())
                && Arrays.equals(mapping.artifactGenerationSetDigest(), identity.artifactGenerationSetDigest());
    }

    private JournalPosition append(final RecordKind kind, final Mapping mapping) {
        final JournalPosition position;
        try {
            position = appender.append(new AppendRequest(kind, mapping));
        } catch (RuntimeException failure) {
            throw failure;
        }
        if (position == null) {
            throw new JournalException(
                    StableCode.PULSAR_EVIDENCE_DIVERGENCE, "Journal appender returned no durable position");
        }
        validatePosition(position);
        return position;
    }

    private void validatePosition(final JournalPosition position) {
        Objects.requireNonNull(position, "position");
        if (lastPosition != null && position.compareTo(lastPosition) <= 0) {
            throw new JournalException(
                    StableCode.PULSAR_EVIDENCE_DIVERGENCE, "Journal position is not strictly increasing");
        }
    }

    private void requireShard(final Mapping mapping) {
        if (!shard.equals(mapping.shard())) {
            throw conflict("mapping belongs to another Shard");
        }
    }

    private MappingState requireMapping(final byte[] mappingId) {
        Bytes.requireLength(mappingId, HASH_LENGTH, "mappingId");
        final MappingState state = mappings.get(Bytes.hex(mappingId));
        if (state == null) {
            throw conflict("unknown Attempt Journal mapping");
        }
        return state;
    }

    private static long nextSequence(final long lastSequenceId) {
        try {
            return Math.addExact(lastSequenceId, 1);
        } catch (ArithmeticException overflow) {
            throw conflict("Pulsar sequence domain exhausted");
        }
    }

    private void rejectMixedMappingGeneration(final ProducerState state, final Mapping incoming) {
        if (state == null || state.lastMappingId == null) {
            return;
        }
        final MappingState prior = mappings.get(state.lastMappingId);
        if (prior != null && prior.mapping.isCurrentGeneration() != incoming.isCurrentGeneration()) {
            throw conflict("Attempt Journal Producer cannot mix mapping generations");
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
        OWNERSHIP_STARTED(2),
        PUBLISHED(3),
        RETIRED_NOT_PUBLISHED(4);

        private final int wireValue;

        RecordKind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }
    }

    /** Exact H3 Attempt Journal state projection. */
    public enum AttemptState {
        MAPPED(1),
        OWNERSHIP_STARTED(2),
        PUBLISHED(3),
        RETIRED_NOT_PUBLISHED(4);

        private final int wireValue;

        AttemptState(final int wireValue) {
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

    public record BrokerSequenceEvidence(
            long brokerLastSequenceId, boolean producerSnapshotRetained, boolean inactivityHorizonValid) {
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
            return new Resolution(ResolutionKind.DIVERGENCE, mapping, StableCode.PULSAR_EVIDENCE_DIVERGENCE, detail);
        }
    }

    public record JournalPosition(
            long ledgerId, long entryId, int batchIndex, int batchSize, long brokerEntryTimestampEpochMs)
            implements Comparable<JournalPosition> {
        public JournalPosition {
            if (brokerEntryTimestampEpochMs < 0
                    || batchSize == 0
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
            return Bytes.concat(
                    Bytes.u64beBits(ledgerId),
                    Bytes.u64beBits(entryId),
                    Bytes.u32beBits(batchIndex),
                    Bytes.u32beBits(batchSize),
                    Bytes.i64be(brokerEntryTimestampEpochMs));
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
            return Bytes.concat(
                    RECORD_DOMAIN, Bytes.u8(kind.wireValue()), mapping.canonicalBytes(), position.canonicalBytes());
        }
    }

    public record ProducerKey(
            DestinationLaneId laneId,
            byte[] laneIncarnation,
            byte[] stableProducerNameHash,
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
            return Bytes.concat(
                    laneId.bytes(),
                    laneIncarnation,
                    stableProducerNameHash,
                    Bytes.lp32(Bytes.utf8(target.authenticatedClusterId())),
                    Bytes.lp32(target.resourceIncarnation()),
                    Bytes.lp32(Bytes.utf8(target.physicalTopic())),
                    Bytes.u64beBits(target.physicalTopicCreationTimestamp()),
                    Bytes.u32beBits(target.partition()));
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof ProducerKey that)) {
                return false;
            }
            return laneId.equals(that.laneId)
                    && target.authenticatedClusterId().equals(that.target.authenticatedClusterId())
                    && target.physicalTopic().equals(that.target.physicalTopic())
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

    public record AttemptIdentity(
            DelayMessageId delayMessageId,
            int generation,
            byte[] publishAttemptId,
            byte[] preparedPublishHash,
            long guardedBrokerTimestampEpochMs,
            byte[] sourcePosition) {
        public AttemptIdentity {
            Objects.requireNonNull(delayMessageId, "delayMessageId");
            if (guardedBrokerTimestampEpochMs < 0) {
                throw new IllegalArgumentException("invalid Attempt Journal mapping identity");
            }
            Bytes.requireLength(publishAttemptId, HASH_LENGTH, "publishAttemptId");
            Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            publishAttemptId = Bytes.copy(publishAttemptId);
            preparedPublishHash = Bytes.copy(preparedPublishHash);
            final var decodedSourcePosition = SourcePositionCodec.decode(sourcePosition);
            if (!delayMessageId.routingId().shardId().equals(decodedSourcePosition.shardId())) {
                throw new IllegalArgumentException("Pulsar Attempt Journal source belongs to another Shard");
            }
            sourcePosition = decodedSourcePosition.canonicalBytes();
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

    /**
     * Current H3 mapping input. The source Admission position is retained as
     * an exact source identity; no Broker deliver-at or clock-shift value is
     * accepted in this generation.
     */
    public record CurrentAttemptIdentity(
            DelayMessageId delayMessageId,
            int generation,
            byte[] publishAttemptId,
            byte[] preparedPublishHash,
            byte[] recordTemplateHash,
            DeliveryContract deliveryContract,
            byte[] sourceAdmissionPosition,
            byte[] artifactGenerationSetDigest) {
        public CurrentAttemptIdentity {
            Objects.requireNonNull(delayMessageId, "delayMessageId");
            if (generation < 0) {
                throw new IllegalArgumentException("generation must be non-negative");
            }
            Bytes.requireLength(publishAttemptId, HASH_LENGTH, "publishAttemptId");
            Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            Bytes.requireLength(recordTemplateHash, HASH_LENGTH, "recordTemplateHash");
            Objects.requireNonNull(deliveryContract, "deliveryContract");
            Bytes.requireLength(artifactGenerationSetDigest, HASH_LENGTH, "artifactGenerationSetDigest");
            final var decodedSourcePosition = SourcePositionCodec.decode(sourceAdmissionPosition);
            if (!delayMessageId.routingId().shardId().equals(decodedSourcePosition.shardId())) {
                throw new IllegalArgumentException("Pulsar Attempt Journal source belongs to another Shard");
            }
            publishAttemptId = Bytes.copy(publishAttemptId);
            preparedPublishHash = Bytes.copy(preparedPublishHash);
            recordTemplateHash = Bytes.copy(recordTemplateHash);
            sourceAdmissionPosition = decodedSourcePosition.canonicalBytes();
            artifactGenerationSetDigest = Bytes.copy(artifactGenerationSetDigest);
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
        public byte[] recordTemplateHash() {
            return Bytes.copy(recordTemplateHash);
        }

        @Override
        public byte[] sourceAdmissionPosition() {
            return Bytes.copy(sourceAdmissionPosition);
        }

        @Override
        public byte[] artifactGenerationSetDigest() {
            return Bytes.copy(artifactGenerationSetDigest);
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
        private final byte[] recordTemplateHash;
        private final DeliveryContract deliveryContract;
        private final byte[] artifactGenerationSetDigest;
        private final boolean currentGeneration;
        private final byte[] mappingId;

        private Mapping(
                final ShardId shard,
                final ProducerKey producer,
                final long sequenceId,
                final AttemptIdentity identity) {
            this.shard = Objects.requireNonNull(shard, "shard");
            this.producer = Objects.requireNonNull(producer, "producer");
            if (!shard.equals(identity.delayMessageId().routingId().shardId())
                    || !shard.equals(SourcePositionCodec.decode(identity.sourcePosition())
                            .shardId())) {
                throw new IllegalArgumentException("Pulsar Attempt Journal identity belongs to another Shard");
            }
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
            this.recordTemplateHash = null;
            this.deliveryContract = null;
            this.artifactGenerationSetDigest = null;
            this.currentGeneration = false;
            this.mappingId = Bytes.sha256(Bytes.concat(MAPPING_ID_DOMAIN, canonicalBody()));
        }

        private Mapping(
                final ShardId shard,
                final ProducerKey producer,
                final long sequenceId,
                final CurrentAttemptIdentity identity) {
            this.shard = Objects.requireNonNull(shard, "shard");
            this.producer = Objects.requireNonNull(producer, "producer");
            if (!shard.equals(identity.delayMessageId().routingId().shardId())
                    || !shard.equals(SourcePositionCodec.decode(identity.sourceAdmissionPosition())
                            .shardId())) {
                throw new IllegalArgumentException("Pulsar Attempt Journal identity belongs to another Shard");
            }
            if (sequenceId < 0) {
                throw new IllegalArgumentException("sequenceId must be non-negative");
            }
            this.sequenceId = sequenceId;
            this.delayMessageId = identity.delayMessageId();
            this.generation = identity.generation();
            this.publishAttemptId = identity.publishAttemptId();
            this.preparedPublishHash = identity.preparedPublishHash();
            this.guardedBrokerTimestampEpochMs = 0;
            this.sourcePosition = identity.sourceAdmissionPosition();
            this.recordTemplateHash = identity.recordTemplateHash();
            this.deliveryContract = identity.deliveryContract();
            this.artifactGenerationSetDigest = identity.artifactGenerationSetDigest();
            this.currentGeneration = true;
            this.mappingId = Bytes.sha256(Bytes.concat(MAPPING_ID_DOMAIN, canonicalBody()));
        }

        public static Mapping create(
                final ShardId shard,
                final ProducerKey producer,
                final long sequenceId,
                final AttemptIdentity identity) {
            return new Mapping(shard, producer, sequenceId, identity);
        }

        public static Mapping createCurrent(
                final ShardId shard,
                final ProducerKey producer,
                final long sequenceId,
                final CurrentAttemptIdentity identity) {
            return new Mapping(shard, producer, sequenceId, Objects.requireNonNull(identity, "identity"));
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
            if (currentGeneration) {
                throw new IllegalStateException("current Journal mapping has no Broker clock-shift field");
            }
            return guardedBrokerTimestampEpochMs;
        }

        public byte[] sourcePosition() {
            return Bytes.copy(sourcePosition);
        }

        public boolean isCurrentGeneration() {
            return currentGeneration;
        }

        public byte[] recordTemplateHash() {
            if (!currentGeneration) {
                throw new IllegalStateException("legacy Journal mapping has no recordTemplateHash");
            }
            return Bytes.copy(recordTemplateHash);
        }

        public DeliveryContract deliveryContract() {
            if (!currentGeneration) {
                throw new IllegalStateException("legacy Journal mapping has no deliveryContract");
            }
            return deliveryContract;
        }

        public byte[] artifactGenerationSetDigest() {
            if (!currentGeneration) {
                throw new IllegalStateException("legacy Journal mapping has no artifact generation digest");
            }
            return Bytes.copy(artifactGenerationSetDigest);
        }

        public byte[] mappingId() {
            return Bytes.copy(mappingId);
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(Bytes.lp32(mappingId), canonicalBody());
        }

        private byte[] canonicalBody() {
            if (currentGeneration) {
                return Bytes.concat(
                        shard.routeIncarnation().bytes(),
                        Bytes.u32beBits(shard.partition()),
                        producer.canonicalBytes(),
                        Bytes.u64be(sequenceId),
                        delayMessageId.bytes(),
                        Bytes.u32beBits(generation),
                        publishAttemptId,
                        preparedPublishHash,
                        recordTemplateHash,
                        Bytes.u32beBits(deliveryContract.wireValue()),
                        Bytes.lp32(sourcePosition),
                        artifactGenerationSetDigest);
            }
            return Bytes.concat(
                    shard.routeIncarnation().bytes(),
                    Bytes.u32beBits(shard.partition()),
                    producer.canonicalBytes(),
                    Bytes.u64be(sequenceId),
                    delayMessageId.bytes(),
                    Bytes.u32beBits(generation),
                    publishAttemptId,
                    preparedPublishHash,
                    Bytes.i64be(guardedBrokerTimestampEpochMs),
                    Bytes.lp32(sourcePosition));
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
        private boolean ownershipStarted;
        private JournalRecord ownershipRecord;
        private boolean published;
        private JournalRecord publishedRecord;
        private boolean retired;
        private JournalRecord retirementRecord;

        private MappingState(final Mapping mapping, final ProducerState producer, final JournalRecord mappedRecord) {
            this.mapping = mapping;
            this.producer = producer;
            this.mappedRecord = mappedRecord;
        }

        private AttemptState state() {
            if (retired) {
                return AttemptState.RETIRED_NOT_PUBLISHED;
            }
            if (published) {
                return AttemptState.PUBLISHED;
            }
            if (ownershipStarted) {
                return AttemptState.OWNERSHIP_STARTED;
            }
            return AttemptState.MAPPED;
        }

        private JournalRecord record(final RecordKind kind) {
            return switch (kind) {
                case MAPPED -> mappedRecord;
                case OWNERSHIP_STARTED -> ownershipRecord;
                case PUBLISHED -> publishedRecord;
                case RETIRED_NOT_PUBLISHED -> retirementRecord;
            };
        }
    }

    private static final class ProducerState {
        private long lastSequenceId = -1;
        private String lastMappingId;
        private String unresolvedMappingId;
    }

    private static final class LocalAppender implements DurableAppender {
        private long nextEntry;
        private boolean exhausted;

        private LocalAppender() {
            this(0);
        }

        private LocalAppender(final long initialEntry) {
            nextEntry = initialEntry;
        }

        @Override
        public JournalPosition append(final AppendRequest request) {
            if (exhausted) {
                throw new JournalException(StableCode.INTEGRITY_ERROR, "Pulsar Journal entry domain exhausted");
            }
            final long entry = nextEntry;
            final JournalPosition position =
                    new JournalPosition(0, entry, 0, 1, request.mapping().guardedBrokerTimestampEpochMs());
            // Raw u64 all-ones is a valid final entry, but it has no
            // representable successor. Keep the appender permanently
            // exhausted instead of wrapping to the first entry.
            if (entry == -1L) {
                exhausted = true;
            } else {
                nextEntry = entry + 1;
            }
            return position;
        }
    }
}
