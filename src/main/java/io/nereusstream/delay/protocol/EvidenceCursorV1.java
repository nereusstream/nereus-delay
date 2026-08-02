package io.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical Kafka/Pulsar evidence cursor used by ReadyCertificateV1. */
public final class EvidenceCursorV1 implements Comparable<EvidenceCursorV1> {
    private final EvidenceKindV1 evidenceKind;
    private final byte[] destinationLaneId;
    private final byte[] laneIncarnation;
    private final byte[] evidenceResourceIncarnation;
    private final int physicalPartition;
    private final long evidenceGeneration;
    private final long maxBrokerPersistedAtThroughCursor;
    private final boolean kafka;
    private final byte[] topicUuid;
    private final long nextOffsetExclusive;
    private final long lastObservedLsoExclusive;
    private final byte[] resourceToken;
    private final String physicalTopic;
    private final long physicalTopicCreationTimestamp;
    private final long ledgerId;
    private final long entryId;
    private final int normalizedBatchIndex;
    private final int batchSize;

    private EvidenceCursorV1(final EvidenceKindV1 evidenceKind, final byte[] destinationLaneId,
                             final byte[] laneIncarnation, final byte[] evidenceResourceIncarnation,
                             final int physicalPartition, final long evidenceGeneration,
                             final long maxBrokerPersistedAtThroughCursor, final byte[] topicUuid,
                             final long nextOffsetExclusive, final long lastObservedLsoExclusive,
                             final byte[] resourceToken, final String physicalTopic,
                             final long physicalTopicCreationTimestamp, final long ledgerId, final long entryId,
                             final int normalizedBatchIndex, final int batchSize) {
        this.evidenceKind = Objects.requireNonNull(evidenceKind, "evidenceKind");
        this.destinationLaneId = fixed(destinationLaneId, 32, "destinationLaneId");
        this.laneIncarnation = fixed(laneIncarnation, 16, "laneIncarnation");
        this.evidenceResourceIncarnation = nonEmpty(evidenceResourceIncarnation, "evidenceResourceIncarnation");
        if (physicalPartition < 0 || evidenceGeneration <= 0 || maxBrokerPersistedAtThroughCursor < 0) {
            throw new IllegalArgumentException("invalid evidence cursor counters");
        }
        this.physicalPartition = physicalPartition;
        this.evidenceGeneration = evidenceGeneration;
        this.maxBrokerPersistedAtThroughCursor = maxBrokerPersistedAtThroughCursor;
        this.kafka = evidenceKind == EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS;
        this.topicUuid = topicUuid == null ? null : fixed(topicUuid, 16, "topicUuid");
        this.nextOffsetExclusive = nonNegative(nextOffsetExclusive, "nextOffsetExclusive");
        this.lastObservedLsoExclusive = nonNegative(lastObservedLsoExclusive, "lastObservedLsoExclusive");
        this.resourceToken = resourceToken == null ? null : fixed(resourceToken, 32, "resourceToken");
        this.physicalTopic = physicalTopic == null ? null : nfc(physicalTopic, "physicalTopic");
        this.physicalTopicCreationTimestamp = nonNegative(physicalTopicCreationTimestamp,
                "physicalTopicCreationTimestamp");
        this.ledgerId = nonNegative(ledgerId, "ledgerId");
        this.entryId = nonNegative(entryId, "entryId");
        if (normalizedBatchIndex < 0 || (kafka && batchSize != 0)
                || (!kafka && (batchSize <= 0 || normalizedBatchIndex >= batchSize))) {
            throw new IllegalArgumentException("invalid evidence batch cursor");
        }
        this.normalizedBatchIndex = normalizedBatchIndex;
        this.batchSize = batchSize;
        if (kafka && (this.topicUuid == null || resourceToken != null || physicalTopic != null)) {
            throw new IllegalArgumentException("Kafka cursor branch fields mismatch");
        }
        if (!kafka && (this.resourceToken == null || this.physicalTopic == null || topicUuid != null)) {
            throw new IllegalArgumentException("Pulsar cursor branch fields mismatch");
        }
        if (!Arrays.equals(this.evidenceResourceIncarnation, kafka ? this.topicUuid : this.resourceToken)) {
            throw new IllegalArgumentException("evidence resource does not match cursor branch");
        }
    }

    public static EvidenceCursorV1 kafka(final byte[] destinationLaneId, final byte[] laneIncarnation,
                                         final byte[] topicUuid, final int physicalPartition,
                                         final long evidenceGeneration, final long maxBrokerPersistedAtThroughCursor,
                                         final long nextOffsetExclusive, final long lastObservedLsoExclusive) {
        return new EvidenceCursorV1(EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS, destinationLaneId, laneIncarnation,
                topicUuid, physicalPartition, evidenceGeneration, maxBrokerPersistedAtThroughCursor, topicUuid,
                nextOffsetExclusive, lastObservedLsoExclusive, null, null, 0, 0, 0, 0, 0);
    }

    public static EvidenceCursorV1 pulsar(final byte[] destinationLaneId, final byte[] laneIncarnation,
                                          final byte[] resourceToken, final int physicalPartition,
                                          final long evidenceGeneration, final long maxBrokerPersistedAtThroughCursor,
                                          final String physicalTopic, final long physicalTopicCreationTimestamp,
                                          final long ledgerId, final long entryId, final int normalizedBatchIndex,
                                          final int batchSize) {
        return new EvidenceCursorV1(EvidenceKindV1.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS, destinationLaneId,
                laneIncarnation, resourceToken, physicalPartition, evidenceGeneration,
                maxBrokerPersistedAtThroughCursor, null, 0, 0, resourceToken, physicalTopic,
                physicalTopicCreationTimestamp, ledgerId, entryId, normalizedBatchIndex, batchSize);
    }

    public EvidenceKindV1 evidenceKind() {
        return evidenceKind;
    }

    public byte[] destinationLaneId() {
        return Bytes.copy(destinationLaneId);
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public byte[] evidenceResourceIncarnation() {
        return Bytes.copy(evidenceResourceIncarnation);
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public long evidenceGeneration() {
        return evidenceGeneration;
    }

    public long maxBrokerPersistedAtThroughCursor() {
        return maxBrokerPersistedAtThroughCursor;
    }

    public byte[] topicUuid() {
        return topicUuid == null ? null : Bytes.copy(topicUuid);
    }

    public long nextOffsetExclusive() {
        return nextOffsetExclusive;
    }

    public long lastObservedLsoExclusive() {
        return lastObservedLsoExclusive;
    }

    public byte[] resourceToken() {
        return resourceToken == null ? null : Bytes.copy(resourceToken);
    }

    public String physicalTopic() {
        return physicalTopic;
    }

    public long physicalTopicCreationTimestamp() {
        return physicalTopicCreationTimestamp;
    }

    public long ledgerId() {
        return ledgerId;
    }

    public long entryId() {
        return entryId;
    }

    public int normalizedBatchIndex() {
        return normalizedBatchIndex;
    }

    public int batchSize() {
        return batchSize;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, evidenceKind.wireValue());
            CanonicalProtobuf.bytes(output, 2, destinationLaneId);
            CanonicalProtobuf.bytes(output, 3, laneIncarnation);
            CanonicalProtobuf.bytes(output, 4, evidenceResourceIncarnation);
            CanonicalProtobuf.uint32(output, 5, physicalPartition);
            CanonicalProtobuf.uint64(output, 6, evidenceGeneration);
            CanonicalProtobuf.int64(output, 7, maxBrokerPersistedAtThroughCursor);
            if (kafka) {
                CanonicalProtobuf.bytes(output, 10, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, topicUuid);
                    CanonicalProtobuf.uint64(fields, 2, nextOffsetExclusive);
                    CanonicalProtobuf.uint64(fields, 3, lastObservedLsoExclusive);
                }));
            } else {
                CanonicalProtobuf.bytes(output, 11, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, resourceToken);
                    CanonicalProtobuf.bytes(fields, 2, physicalTopic.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.uint64(fields, 3, physicalTopicCreationTimestamp);
                    CanonicalProtobuf.uint64(fields, 4, ledgerId);
                    CanonicalProtobuf.uint64(fields, 5, entryId);
                    CanonicalProtobuf.uint32(fields, 6, normalizedBatchIndex);
                    CanonicalProtobuf.uint32(fields, 7, batchSize);
                }));
            }
        });
    }

    public static EvidenceCursorV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "EvidenceCursorV1");
        if (fields.size() != 8 || fields.get(0).number() != 1 || fields.get(6).number() != 7
                || (fields.get(7).number() != 10 && fields.get(7).number() != 11)) {
            throw new IllegalArgumentException("invalid EvidenceCursorV1 field order");
        }
        final EvidenceKindV1 kind = EvidenceKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final byte[] lane = QueryCodecSupport.fixed(fields.get(1), 2, 32);
        final byte[] incarnation = QueryCodecSupport.fixed(fields.get(2), 3, 16);
        final byte[] resource = QueryCodecSupport.bytes(fields.get(3), 4);
        final int partition = QueryCodecSupport.uint32(fields.get(4), 5);
        final long generation = QueryCodecSupport.uint(fields.get(5), 6);
        final long maxPersisted = QueryCodecSupport.uint(fields.get(6), 7);
        final EvidenceCursorV1 result;
        if (fields.get(7).number() == 10) {
            if (kind != EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS) {
                throw new IllegalArgumentException("EvidenceCursor kind/branch mismatch");
            }
            final List<CanonicalProtobuf.Reader.Field> cursor = QueryCodecSupport.read(
                    QueryCodecSupport.nested(fields.get(7), 10), "KafkaReceiptCursorV1");
            QueryCodecSupport.requireNumbers(cursor, new int[]{1, 2, 3}, "KafkaReceiptCursorV1");
            result = kafka(lane, incarnation, fixed(resource, 16, "topicUuid"), partition,
                    generation, maxPersisted, QueryCodecSupport.uint(cursor.get(1), 2),
                    QueryCodecSupport.uint(cursor.get(2), 3));
        } else {
            if (kind != EvidenceKindV1.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS) {
                throw new IllegalArgumentException("EvidenceCursor kind/branch mismatch");
            }
            final List<CanonicalProtobuf.Reader.Field> cursor = QueryCodecSupport.read(
                    QueryCodecSupport.nested(fields.get(7), 11), "PulsarJournalCursorV1");
            QueryCodecSupport.requireNumbers(cursor, new int[]{1, 2, 3, 4, 5, 6, 7}, "PulsarJournalCursorV1");
            result = pulsar(lane, incarnation, fixed(resource, 32, "resourceToken"), partition,
                    generation, maxPersisted, utf8(QueryCodecSupport.bytes(cursor.get(1), 2)),
                    QueryCodecSupport.uint(cursor.get(2), 3), QueryCodecSupport.uint(cursor.get(3), 4),
                    QueryCodecSupport.uint(cursor.get(4), 5), QueryCodecSupport.uint32(cursor.get(5), 6),
                    QueryCodecSupport.uint32(cursor.get(6), 7));
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EvidenceCursorV1");
        return result;
    }

    @Override
    public int compareTo(final EvidenceCursorV1 other) {
        int result = Integer.compare(evidenceKind.wireValue(), other.evidenceKind.wireValue());
        if (result != 0) {
            return result;
        }
        result = compareUnsigned(destinationLaneId, other.destinationLaneId);
        if (result != 0) {
            return result;
        }
        result = compareUnsigned(laneIncarnation, other.laneIncarnation);
        if (result != 0) {
            return result;
        }
        result = compareUnsigned(evidenceResourceIncarnation, other.evidenceResourceIncarnation);
        if (result != 0) {
            return result;
        }
        result = Integer.compare(physicalPartition, other.physicalPartition);
        if (result != 0) {
            return result;
        }
        return Long.compareUnsigned(evidenceGeneration, other.evidenceGeneration);
    }

    /**
     * Returns whether two cursors describe the same comparable evidence
     * stream. The evidence generation is part of this identity; cursors from
     * different generations are intentionally incomparable.
     */
    public boolean sameIdentity(final EvidenceCursorV1 other) {
        return other != null && compareTo(other) == 0;
    }

    /**
     * Returns whether this cursor dominates {@code older} on the same
     * evidence stream. Kafka advances both the exclusive offset and LSO
     * watermark; Pulsar advances the inclusive ledger/entry/batch member.
     * Both branches must retain a non-regressing Broker-time anchor.
     */
    public boolean dominates(final EvidenceCursorV1 older) {
        if (!sameIdentity(older)
                || maxBrokerPersistedAtThroughCursor < older.maxBrokerPersistedAtThroughCursor) {
            return false;
        }
        if (kafka) {
            return nextOffsetExclusive >= older.nextOffsetExclusive
                    && lastObservedLsoExclusive >= older.lastObservedLsoExclusive;
        }
        return comparePulsarMember(older) >= 0;
    }

    /** Returns whether this cursor is a strict successor of {@code older}. */
    public boolean strictlyDominates(final EvidenceCursorV1 older) {
        return dominates(older) && !equals(older);
    }

    private int comparePulsarMember(final EvidenceCursorV1 other) {
        int result = Long.compareUnsigned(ledgerId, other.ledgerId);
        if (result != 0) {
            return result;
        }
        result = Long.compareUnsigned(entryId, other.entryId);
        if (result != 0) {
            return result;
        }
        return Integer.compare(normalizedBatchIndex, other.normalizedBatchIndex);
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static String utf8(final byte[] value) {
        final String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException("physicalTopic is not valid UTF-8");
        }
        return result;
    }

    private static String nfc(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC");
        }
        return value;
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EvidenceCursorV1 that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
