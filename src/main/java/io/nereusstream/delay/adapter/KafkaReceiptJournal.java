package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ChannelKindV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceKindV1;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SourcePositionCodec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Local protocol seam for a V1 Kafka transactional receipt partition.
 *
 * <p>The production implementation must use one Kafka transaction containing
 * the target record and the keyed receipt record, plus a pinned
 * {@code read_committed} reader that proves the receipt cursor and LSO. This
 * class models only the local ordering, identity, replay and evidence-branch
 * invariants. An injected appender is not a Kafka transaction or an
 * authenticated Broker proof.</p>
 */
public final class KafkaReceiptJournal {
    private static final int HASH_LENGTH = 32;
    private static final int LANE_INCARNATION_LENGTH = 16;
    private static final byte[] MAPPING_ID_DOMAIN = Bytes.utf8(
            "nereus-delay-kafka-receipt-mapping-id-v1\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8(
            "nereus-delay-kafka-receipt-record-v1\0");

    private final ShardId shard;
    private final DurableAppender appender;
    private final KafkaReceiptResource receiptResource;
    private final Map<String, MappingState> mappings = new HashMap<>();
    private final Map<ProducerKey, ProducerState> producers = new HashMap<>();
    private final List<JournalRecord> records = new ArrayList<>();
    private ReceiptPosition lastPosition;

    /** Creates a deterministic in-memory appender for local tests only. */
    public KafkaReceiptJournal(final ShardId shard, final KafkaReceiptResource receiptResource) {
        this(shard, new LocalAppender(), receiptResource);
    }

    /**
     * Creates the deterministic local appender at an explicit raw offset.
     * This package-private seam exists only for exhaustion-boundary tests; a
     * production Kafka adapter must obtain positions from the Broker.
     */
    KafkaReceiptJournal(final ShardId shard, final KafkaReceiptResource receiptResource,
                        final long initialOffset) {
        this(shard, new LocalAppender(initialOffset), receiptResource);
    }

    /** Creates a receipt journal seam with an injected durable append. */
    public KafkaReceiptJournal(final ShardId shard, final DurableAppender appender,
                               final KafkaReceiptResource receiptResource) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.receiptResource = Objects.requireNonNull(receiptResource, "receiptResource");
        if (receiptResource.shardPartition() != shard.partition()
                || !receiptResource.routeIncarnation().equals(shard.routeIncarnation())) {
            throw new IllegalArgumentException("Kafka receipt resource does not belong to Shard");
        }
    }

    /** Allocates and durably appends the next sequence mapping in one local turn. */
    public synchronized AppendResult appendNext(final ProducerKey producer, final AttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        requireReceiptCluster(producer);
        final ProducerState state = producers.get(producer);
        if (state != null && state.unresolvedMappingId != null) {
            throw conflict("an unresolved lower sequence blocks this transactional channel");
        }
        final long sequenceId;
        try {
            sequenceId = state == null ? 0 : Math.addExact(state.lastSequenceId, 1);
        } catch (ArithmeticException overflow) {
            throw conflict("Kafka receipt sequence domain exhausted");
        }
        final Mapping mapping = Mapping.create(shard, producer, sequenceId, identity);
        return appendMappedInternal(mapping);
    }

    /** Appends an exact mapping; a repeated exact mapping is idempotent. */
    public synchronized AppendResult appendMapped(final Mapping mapping) {
        Objects.requireNonNull(mapping, "mapping");
        requireReceiptCluster(mapping.producer());
        return appendMappedInternal(mapping);
    }

    /**
     * Reuses an exact non-retired mapping for a retransmission, or allocates
     * the next sequence when this attempt has not reached the receipt journal.
     */
    public synchronized AppendResult appendOrReuse(final ProducerKey producer,
                                                    final AttemptIdentity identity) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(identity, "identity");
        requireReceiptCluster(producer);
        MappingState matching = null;
        for (MappingState candidate : mappings.values()) {
            if (!Arrays.equals(candidate.mapping.publishAttemptId(), identity.publishAttemptId())) {
                continue;
            }
            if (!candidate.mapping.producer().equals(producer)
                    || !sameAttemptIdentity(candidate.mapping, identity)) {
                throw conflict("publish attempt identity was reused with different receipt mapping bytes");
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
     * Persists the mapping before invoking the target transaction sender. The
     * sender is never called when the receipt append/replay gate fails.
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
        final ReceiptPosition position = append(RecordKind.RETIRED_NOT_PUBLISHED, state.mapping);
        final JournalRecord record = new JournalRecord(RecordKind.RETIRED_NOT_PUBLISHED, state.mapping, position);
        state.retired = true;
        state.retirementRecord = record;
        state.producer.unresolvedMappingId = null;
        records.add(record);
        lastPosition = position;
        return new AppendResult(record, false);
    }

    /** Replays one already-durable local receipt-journal record. */
    public synchronized void replay(final JournalRecord record) {
        Objects.requireNonNull(record, "record");
        requireShard(record.mapping());
        final String mappingId = Bytes.hex(record.mapping().mappingId());
        final MappingState current = mappings.get(mappingId);
        if (current != null) {
            if (!current.mapping.sameCanonical(record.mapping())) {
                throw conflict("Kafka receipt mapping id/body conflict");
            }
            if (record.kind() == RecordKind.MAPPED) {
                // ReceiptPosition contains a byte[] hash. The generated
                // record equals() therefore compares that array by identity;
                // replay identity is the canonical position bytes instead.
                if (current.mappedRecord.position() != null
                        && Arrays.equals(current.mappedRecord.position().canonicalBytes(),
                        record.position().canonicalBytes())) {
                    return;
                }
                throw conflict("Kafka receipt mapped record replay conflict");
            }
            if (current.retired) {
                if (current.retirementRecord != null
                        && Arrays.equals(current.retirementRecord.canonicalBytes(), record.canonicalBytes())) {
                    return;
                }
                throw conflict("Kafka receipt retirement replay conflict");
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
            throw conflict("receipt retirement record has no mapped predecessor");
        }
        validatePosition(record.position());
        appendState(record.mapping(), record.position());
    }

    /** Returns the current unresolved mapping for one transactional channel. */
    public synchronized Optional<Mapping> unresolved(final ProducerKey producer) {
        Objects.requireNonNull(producer, "producer");
        final ProducerState state = producers.get(producer);
        if (state == null || state.unresolvedMappingId == null) {
            return Optional.empty();
        }
        return Optional.of(mappings.get(state.unresolvedMappingId).mapping);
    }

    /** Returns an immutable snapshot in local append/replay order. */
    public synchronized List<JournalRecord> records() {
        return List.copyOf(records);
    }

    /**
     * Projects the latest local receipt position into a typed Kafka cursor.
     * A production reader must still prove UUID/partition, contiguous
     * read_committed replay, LSO and retention before publishing this value.
     */
    public synchronized Optional<EvidenceCursorV1> evidenceCursor(final ProducerKey producer,
                                                                    final long evidenceGeneration) {
        Objects.requireNonNull(producer, "producer");
        requireReceiptCluster(producer);
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
                    record.position().brokerLogAppendTimeEpochMs());
        }
        if (latest == null) {
            return Optional.empty();
        }
        final ReceiptPosition position = latest.position();
        return Optional.of(EvidenceCursorV1.kafka(producer.laneId().bytes(), producer.laneIncarnation(),
                uuidBytes(receiptResource.nativeTopicUuid()), receiptResource.receiptPartition(),
                evidenceGeneration, maxBrokerPersistedAt, successor(position.offset()),
                position.lastStableOffsetExclusive()));
    }

    /** Builds the local PUBLISHED Kafka transactional-receipt branch. */
    public synchronized PublishEvidenceV1 publishedEvidence(final Mapping mapping,
                                                             final long evidenceGeneration) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
        if (state == null || !state.mapping.sameCanonical(mapping)) {
            throw conflict("published evidence has no exact receipt mapping");
        }
        if (state.retired) {
            throw conflict("retired receipt mapping cannot produce PUBLISHED evidence");
        }
        final EvidenceCursorV1 cursor = evidenceCursor(mapping.producer(), evidenceGeneration).orElseThrow(() ->
                conflict("published evidence has no receipt cursor"));
        final ReceiptPosition position = state.mappedRecord.position();
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(mapping.producer().target().authenticatedClusterId(),
                        mapping.producer().target().nativeTopicUuid()));
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, cursor.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 2, position.offset());
            CanonicalProtobuf.bytes(output, 3, ExternalDeliveryIdentityV1.publishAttempt(mapping.publishAttemptId())
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 6, mapping.producer().target().partition());
            CanonicalProtobuf.bytes(output, 7, mapping.producer().transactionalIdentitySha256());
            CanonicalProtobuf.bytes(output, 8, position.receiptRecordHash());
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.KAFKA_TRANSACTIONAL_RECEIPT,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch);
    }

    /**
     * Builds local Kafka receipt-absence evidence after a durable retirement.
     * The fenced channel and barrier digest are caller-supplied identity
     * inputs; this method cannot authenticate fencing, Fetch/LSO or retention.
     */
    public synchronized PublishEvidenceV1 notPublishedEvidence(final Mapping mapping,
                                                                final long evidenceGeneration,
                                                                final ChannelResourceIdentityV1 fencedChannel,
                                                                final byte[] fenceAndLsoBarrierEvidence) {
        Objects.requireNonNull(mapping, "mapping");
        requireShard(mapping);
        Objects.requireNonNull(fencedChannel, "fencedChannel");
        Bytes.requireLength(fenceAndLsoBarrierEvidence, HASH_LENGTH, "fenceAndLsoBarrierEvidence");
        if (evidenceGeneration == 0) {
            throw new IllegalArgumentException("evidenceGeneration must be non-zero");
        }
        final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
        if (state == null || !state.mapping.sameCanonical(mapping)) {
            throw conflict("receipt absence has no exact mapping");
        }
        if (!state.retired || state.retirementRecord == null) {
            throw conflict("receipt absence requires a durable retirement");
        }
        final EvidenceCursorV1 cursor = evidenceCursor(mapping.producer(), evidenceGeneration).orElseThrow(() ->
                conflict("receipt absence has no receipt cursor"));
        validateFencedReceiptChannel(mapping.producer(), fencedChannel, cursor, evidenceGeneration);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, cursor.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, fencedChannel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, ExternalDeliveryIdentityV1.publishAttempt(mapping.publishAttemptId())
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 5, fenceAndLsoBarrierEvidence);
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.KAFKA_RECEIPT_ABSENCE,
                EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED, branch);
    }

    /**
     * Classifies one caller-supplied read-committed observation against the
     * latest unresolved mapping. Matching receipt bytes prove PUBLISHED only
     * for that exact attempt; absence is accepted only after the local
     * retirement marker and both caller-supplied LSO/retention predicates.
     * Identity or predicate drift returns a fail-closed divergence result.
     */
    public synchronized Resolution resolve(final ProducerKey producer,
                                           final ReceiptObservation observation) {
        Objects.requireNonNull(producer, "producer");
        Objects.requireNonNull(observation, "observation");
        requireReceiptCluster(producer);
        final ProducerState producerState = producers.get(producer);
        if (producerState == null || producerState.lastMappingId == null) {
            return Resolution.empty();
        }
        final MappingState state = mappings.get(producerState.lastMappingId);
        if (state == null || state.mappedRecord.position() == null) {
            return Resolution.divergence(null, "transactional channel lost its latest receipt mapping");
        }
        if (!cursorMatches(producer, observation.cursor())) {
            return Resolution.divergence(state.mapping, "receipt cursor identity does not match the channel");
        }
        final ReceiptPosition mappedPosition = state.mappedRecord.position();
        if (observation.receipt() != null) {
            if (state.retired) {
                return Resolution.divergence(state.mapping,
                        "a retired mapping cannot be classified from a published receipt");
            }
            final ReceiptMatch receipt = observation.receipt();
            if (receipt.offset() != mappedPosition.offset()
                    || !Arrays.equals(receipt.publishAttemptId(), state.mapping.publishAttemptId())
                    || !Arrays.equals(receipt.preparedPublishHash(), state.mapping.preparedPublishHash())
                    || !Arrays.equals(receipt.receiptRecordHash(), mappedPosition.receiptRecordHash())
                    || !cursorIncludesOffset(observation.cursor(), mappedPosition.offset())) {
                return Resolution.divergence(state.mapping, "receipt attempt/hash/offset does not match mapping");
            }
            return Resolution.published(state.mapping);
        }
        if (!state.retired || !observation.fenceAndLsoBarrierValid()
                || !observation.receiptRangeRetained()
                || !cursorLsoIncludesOffset(observation.cursor(), mappedPosition.offset())) {
            return Resolution.divergence(state.mapping,
                    "receipt absence lacks retirement, LSO barrier or retention proof");
        }
        return Resolution.notPublished(state.mapping);
    }

    private void validateFencedReceiptChannel(final ProducerKey producer,
                                              final ChannelResourceIdentityV1 channel,
                                              final EvidenceCursorV1 cursor,
                                              final long evidenceGeneration) {
        if (channel.adapterKind() != AdapterKindV1.KAFKA
                || channel.channelKind() != ChannelKindV1.KAFKA_TRANSACTIONAL_RECEIPT) {
            throw conflict("receipt absence requires a fenced Kafka transactional channel");
        }
        if (!Arrays.equals(channel.destinationLaneId(), producer.laneId().bytes())
                || !Arrays.equals(channel.laneIncarnation(), producer.laneIncarnation())
                || !Arrays.equals(channel.producerOrTransactionalIdentitySha256(),
                producer.transactionalIdentitySha256())) {
            throw conflict("fenced receipt channel is bound to another Lane or transaction");
        }
        if (channel.physicalPartition() != producer.target().partition()
                || channel.evidenceGeneration() == null
                || channel.evidenceGeneration() != evidenceGeneration) {
            throw conflict("fenced receipt channel partition/generation does not match cursor");
        }
        final BrokerResourceIdentityV1 expectedTarget = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1(producer.target().authenticatedClusterId(),
                        producer.target().nativeTopicUuid()));
        if (!expectedTarget.equals(channel.targetResource())) {
            throw conflict("fenced receipt channel target identity differs from Producer target");
        }
        if (cursor.evidenceKind() != EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS
                || cursor.evidenceGeneration() != evidenceGeneration
                || cursor.physicalPartition() != receiptResource.receiptPartition()
                || !Arrays.equals(cursor.topicUuid(), uuidBytes(receiptResource.nativeTopicUuid()))) {
            throw conflict("receipt cursor identity does not match the fenced channel");
        }
        final BrokerResourceIdentityV1 evidenceResource = channel.evidenceResource();
        if (evidenceResource == null || evidenceResource.kind() != BrokerResourceIdentityV1.Kind.KAFKA) {
            throw conflict("fenced receipt channel has no Kafka evidence resource");
        }
        final KafkaBrokerResourceIdentityV1 evidence = evidenceResource.kafka();
        if (!receiptResource.authenticatedClusterId().equals(evidence.authenticatedClusterId())
                || !receiptResource.nativeTopicUuid().equals(evidence.nativeTopicUuid())) {
            throw conflict("receipt cursor identity differs from the fenced evidence resource");
        }
    }

    private boolean cursorMatches(final ProducerKey producer, final EvidenceCursorV1 cursor) {
        return cursor != null
                && cursor.evidenceKind() == EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS
                && Arrays.equals(cursor.destinationLaneId(), producer.laneId().bytes())
                && Arrays.equals(cursor.laneIncarnation(), producer.laneIncarnation())
                && Arrays.equals(cursor.topicUuid(), uuidBytes(receiptResource.nativeTopicUuid()))
                && cursor.physicalPartition() == receiptResource.receiptPartition()
                && cursor.evidenceGeneration() != 0;
    }

    private static boolean cursorIncludesOffset(final EvidenceCursorV1 cursor, final long offset) {
        return Long.compareUnsigned(cursor.nextOffsetExclusive(), successor(offset)) >= 0;
    }

    private static boolean cursorLsoIncludesOffset(final EvidenceCursorV1 cursor, final long offset) {
        return Long.compareUnsigned(cursor.lastObservedLsoExclusive(), successor(offset)) >= 0;
    }

    /** Allows a target transaction only after the exact mapping is durable. */
    public <T> CompletionStage<T> sendAfterMapped(final Mapping mapping, final TargetSender<T> sender) {
        Objects.requireNonNull(mapping, "mapping");
        Objects.requireNonNull(sender, "sender");
        synchronized (this) {
            requireShard(mapping);
            final MappingState state = mappings.get(Bytes.hex(mapping.mappingId()));
            if (state == null || !state.mapping.sameCanonical(mapping) || state.retired) {
                throw conflict("target transaction has no exact durable non-retired receipt mapping");
            }
        }
        final CompletionStage<T> result = sender.send(mapping);
        if (result == null) {
            // A null stage is not evidence of non-publication. Keep the exact
            // mapped attempt unresolved and force the caller through its
            // UNKNOWN/evidence path instead of leaking an untyped NPE.
            throw new JournalException(StableCode.INTEGRITY_ERROR,
                    "target transaction sender returned no CompletionStage");
        }
        return result;
    }

    private AppendResult appendMappedInternal(final Mapping mapping) {
        requireShard(mapping);
        requireReceiptCluster(mapping.producer());
        final String mappingId = Bytes.hex(mapping.mappingId());
        final MappingState current = mappings.get(mappingId);
        if (current != null) {
            if (!current.mapping.sameCanonical(mapping)) {
                throw conflict("Kafka receipt mapping id/body conflict");
            }
            return new AppendResult(current.mappedRecord, true);
        }
        final ProducerState state = producers.get(mapping.producer());
        if (state != null && state.unresolvedMappingId != null) {
            throw conflict("an unresolved lower sequence blocks this transactional channel");
        }
        final long expectedSequence = state == null ? 0 : nextSequence(state.lastSequenceId);
        if (mapping.sequenceId() != expectedSequence) {
            throw conflict("mapping sequence is not the next transactional-channel sequence");
        }
        final ReceiptPosition position = append(RecordKind.MAPPED, mapping);
        appendState(mapping, position);
        return new AppendResult(new JournalRecord(RecordKind.MAPPED, mapping, position), false);
    }

    private ReceiptPosition append(final RecordKind kind, final Mapping mapping) {
        final ReceiptPosition position = appender.append(new AppendRequest(kind, mapping));
        if (position == null) {
            throw new JournalException(StableCode.INTEGRITY_ERROR,
                    "Kafka receipt appender returned no durable position");
        }
        validatePosition(position);
        return position;
    }

    private void appendState(final Mapping mapping, final ReceiptPosition position) {
        final ProducerState state = producers.computeIfAbsent(mapping.producer(), ignored -> new ProducerState());
        if (state.unresolvedMappingId != null) {
            throw conflict("replay would create two unresolved transactional mappings");
        }
        final long expectedSequence = state.lastSequenceId < 0 ? 0 : nextSequence(state.lastSequenceId);
        if (mapping.sequenceId() != expectedSequence) {
            throw conflict("replayed mapping sequence is not the next transactional-channel sequence");
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

    private void validatePosition(final ReceiptPosition position) {
        Objects.requireNonNull(position, "position");
        if (lastPosition != null && position.compareTo(lastPosition) <= 0) {
            throw new JournalException(StableCode.INTEGRITY_ERROR,
                    "Kafka receipt position is not strictly increasing");
        }
    }

    private void requireShard(final Mapping mapping) {
        if (!shard.equals(mapping.shard())) {
            throw conflict("mapping belongs to another Shard");
        }
    }

    private void requireReceiptCluster(final ProducerKey producer) {
        if (!receiptResource.authenticatedClusterId().equals(producer.target().authenticatedClusterId())) {
            throw conflict("Kafka target and receipt resources must use the same authenticated cluster");
        }
    }

    private static boolean sameAttemptIdentity(final Mapping mapping, final AttemptIdentity identity) {
        return mapping.delayMessageId().equals(identity.delayMessageId())
                && mapping.generation() == identity.generation()
                && Arrays.equals(mapping.publishAttemptId(), identity.publishAttemptId())
                && Arrays.equals(mapping.preparedPublishHash(), identity.preparedPublishHash())
                && mapping.guardedBrokerTimestampEpochMs() == identity.guardedBrokerTimestampEpochMs()
                && Arrays.equals(mapping.sourcePosition(), identity.sourcePosition());
    }

    private static long nextSequence(final long lastSequenceId) {
        try {
            return Math.addExact(lastSequenceId, 1);
        } catch (ArithmeticException overflow) {
            throw conflict("Kafka receipt sequence domain exhausted");
        }
    }

    private static long successor(final long offset) {
        if (offset == -1L) {
            throw conflict("Kafka receipt offset domain exhausted");
        }
        // Kafka offsets are a raw uint64 domain.  Crossing the sign bit is a
        // valid unsigned successor; only all-ones has no next value.
        return offset + 1;
    }

    private static byte[] uuidBytes(final UUID uuid) {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static JournalException conflict(final String message) {
        return new JournalException(StableCode.INTEGRITY_ERROR, message);
    }

    @FunctionalInterface
    public interface DurableAppender {
        ReceiptPosition append(AppendRequest request);
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

    public record AppendRequest(RecordKind kind, Mapping mapping) {
        public AppendRequest {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mapping, "mapping");
        }

        /** Compatibility constructor for callers that append a mapping record. */
        public AppendRequest(final Mapping mapping) {
            this(RecordKind.MAPPED, mapping);
        }
    }

    public record AppendResult(JournalRecord record, boolean idempotent) {
        public AppendResult {
            Objects.requireNonNull(record, "record");
        }
    }

    public record ReceiptPosition(long offset, long brokerLogAppendTimeEpochMs,
                                  long lastStableOffsetExclusive, byte[] receiptRecordHash)
            implements Comparable<ReceiptPosition> {
        public ReceiptPosition {
            if (brokerLogAppendTimeEpochMs < 0
                    || Long.compareUnsigned(lastStableOffsetExclusive, offset) <= 0) {
                throw new IllegalArgumentException("invalid Kafka receipt position");
            }
            Bytes.requireLength(receiptRecordHash, HASH_LENGTH, "receiptRecordHash");
            receiptRecordHash = Bytes.copy(receiptRecordHash);
        }

        @Override
        public byte[] receiptRecordHash() {
            return Bytes.copy(receiptRecordHash);
        }

        @Override
        public int compareTo(final ReceiptPosition other) {
            Objects.requireNonNull(other, "other");
            return Long.compareUnsigned(offset, other.offset);
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(Bytes.i64be(offset), Bytes.i64be(brokerLogAppendTimeEpochMs),
                    Bytes.i64be(lastStableOffsetExclusive), receiptRecordHash);
        }
    }

    public record JournalRecord(RecordKind kind, Mapping mapping, ReceiptPosition position) {
        public JournalRecord {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mapping, "mapping");
            Objects.requireNonNull(position, "position");
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(RECORD_DOMAIN, Bytes.u8(kind.wireValue()), mapping.canonicalBytes(),
                    position.canonicalBytes());
        }
    }

    /** Exact receipt record identity returned by a pinned read-committed reader. */
    public record ReceiptMatch(long offset, byte[] publishAttemptId, byte[] preparedPublishHash,
                               byte[] receiptRecordHash) {
        public ReceiptMatch {
            Bytes.requireLength(publishAttemptId, HASH_LENGTH, "publishAttemptId");
            Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            Bytes.requireLength(receiptRecordHash, HASH_LENGTH, "receiptRecordHash");
            publishAttemptId = Bytes.copy(publishAttemptId);
            preparedPublishHash = Bytes.copy(preparedPublishHash);
            receiptRecordHash = Bytes.copy(receiptRecordHash);
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
        public byte[] receiptRecordHash() {
            return Bytes.copy(receiptRecordHash);
        }
    }

    /**
     * Observation returned by the adapter's pinned receipt reader. A null
     * receipt means that the caller observed no exact record in the bounded
     * range; the two boolean predicates must still be independently proven.
     */
    public record ReceiptObservation(EvidenceCursorV1 cursor, ReceiptMatch receipt,
                                     boolean fenceAndLsoBarrierValid,
                                     boolean receiptRangeRetained) {
        public ReceiptObservation {
            Objects.requireNonNull(cursor, "cursor");
        }

        public static ReceiptObservation published(final EvidenceCursorV1 cursor, final ReceiptMatch receipt) {
            return new ReceiptObservation(cursor, Objects.requireNonNull(receipt, "receipt"), false, false);
        }

        public static ReceiptObservation absent(final EvidenceCursorV1 cursor,
                                                final boolean fenceAndLsoBarrierValid,
                                                final boolean receiptRangeRetained) {
            return new ReceiptObservation(cursor, null, fenceAndLsoBarrierValid, receiptRangeRetained);
        }
    }

    public enum ResolutionKind {
        EMPTY,
        PUBLISHED,
        NOT_PUBLISHED,
        DIVERGENCE
    }

    public record Resolution(ResolutionKind kind, Mapping mapping, StableCode stableCode, String detail) {
        public Resolution {
            Objects.requireNonNull(kind, "kind");
            if (kind == ResolutionKind.DIVERGENCE && stableCode != StableCode.INTEGRITY_ERROR) {
                throw new IllegalArgumentException("divergence must use INTEGRITY_ERROR");
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
            return new Resolution(ResolutionKind.DIVERGENCE, mapping, StableCode.INTEGRITY_ERROR, detail);
        }
    }

    public record ProducerKey(DestinationLaneId laneId, byte[] laneIncarnation,
                              byte[] transactionalIdentitySha256, KafkaTargetResource target) {
        public ProducerKey {
            Objects.requireNonNull(laneId, "laneId");
            Bytes.requireLength(laneIncarnation, LANE_INCARNATION_LENGTH, "laneIncarnation");
            Bytes.requireLength(transactionalIdentitySha256, HASH_LENGTH, "transactionalIdentitySha256");
            Objects.requireNonNull(target, "target");
            laneIncarnation = Bytes.copy(laneIncarnation);
            transactionalIdentitySha256 = Bytes.copy(transactionalIdentitySha256);
        }

        @Override
        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        @Override
        public byte[] transactionalIdentitySha256() {
            return Bytes.copy(transactionalIdentitySha256);
        }

        private byte[] canonicalBytes() {
            return Bytes.concat(laneId.bytes(), laneIncarnation, transactionalIdentitySha256,
                    Bytes.lp32(Bytes.utf8(target.authenticatedClusterId())), uuidBytes(target.nativeTopicUuid()),
                    Bytes.u32beBits(target.partition()));
        }

        @Override
        public boolean equals(final Object other) {
            if (!(other instanceof ProducerKey that)) {
                return false;
            }
            return laneId.equals(that.laneId)
                    && target.authenticatedClusterId().equals(that.target.authenticatedClusterId())
                    && target.nativeTopicUuid().equals(that.target.nativeTopicUuid())
                    && target.partition() == that.target.partition()
                    && Arrays.equals(laneIncarnation, that.laneIncarnation)
                    && Arrays.equals(transactionalIdentitySha256, that.transactionalIdentitySha256);
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
            if (guardedBrokerTimestampEpochMs < 0) {
                throw new IllegalArgumentException("invalid Kafka receipt mapping identity");
            }
            Bytes.requireLength(publishAttemptId, HASH_LENGTH, "publishAttemptId");
            Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            publishAttemptId = Bytes.copy(publishAttemptId);
            preparedPublishHash = Bytes.copy(preparedPublishHash);
            final var decodedSourcePosition = SourcePositionCodec.decode(sourcePosition);
            if (!delayMessageId.routingId().shardId().equals(decodedSourcePosition.shardId())) {
                throw new IllegalArgumentException("Kafka receipt attempt source belongs to another Shard");
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
            if (!shard.equals(identity.delayMessageId().routingId().shardId())
                    || !shard.equals(SourcePositionCodec.decode(identity.sourcePosition()).shardId())) {
                throw new IllegalArgumentException("Kafka receipt mapping identity belongs to another Shard");
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
        private long nextOffset;

        private LocalAppender() {
            this(0);
        }

        private LocalAppender(final long initialOffset) {
            nextOffset = initialOffset;
        }

        @Override
        public ReceiptPosition append(final AppendRequest request) {
            final long offset = nextOffset;
            // 0xffffffffffffffff has no representable exclusive successor.
            // Compute and validate that successor before mutating the local
            // cursor, so a failed append cannot wrap the seam into offset 0.
            final long lastStableOffsetExclusive = successor(offset);
            final ReceiptPosition position = new ReceiptPosition(offset,
                    request.mapping().guardedBrokerTimestampEpochMs(), lastStableOffsetExclusive,
                    Bytes.sha256(RECORD_DOMAIN, request.mapping().canonicalBytes()));
            nextOffset = lastStableOffsetExclusive;
            return position;
        }
    }
}
