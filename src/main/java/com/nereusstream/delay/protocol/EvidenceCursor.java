package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical Kafka/Pulsar evidence cursor used by ReadyCertificate. */
public final class EvidenceCursor implements Comparable<EvidenceCursor> {
    /** Target producer namespace, independent of Owner/channel renewal and incompatible with Lane identity. */
    public record TargetScope(ShardId sourceShard, TargetPartitionId target, TargetKeyCodec.Domain domain,
            byte[] accountingIncarnation, byte[] producerNameHash) {
        public static final int MAX_CANONICAL_BYTES = 128;

        public TargetScope {
            Objects.requireNonNull(sourceShard, "sourceShard");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(domain, "domain");
            if (domain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS) {
                throw new IllegalArgumentException("Target evidence domain exceeds its registered slot range");
            }
            accountingIncarnation = TargetCompatibilityCodec.assigned(
                    accountingIncarnation, 16, "accountingIncarnation");
            producerNameHash = TargetCompatibilityCodec.assigned(producerNameHash, 32, "producerNameHash");
        }

        @Override
        public byte[] accountingIncarnation() { return Bytes.copy(accountingIncarnation); }
        @Override
        public byte[] producerNameHash() { return Bytes.copy(producerNameHash); }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, Bytes.concat(sourceShard.routeIncarnation().bytes(),
                        Bytes.u32beBits(sourceShard.partition())));
                CanonicalProtobuf.bytes(out, 3, target.bytes());
                CanonicalProtobuf.uint32(out, 4, domain.slot());
                CanonicalProtobuf.uint64Bits(out, 5, domain.generation());
                CanonicalProtobuf.bytes(out, 6, accountingIncarnation);
                CanonicalProtobuf.bytes(out, 7, producerNameHash);
            });
        }

        public static TargetScope decode(final byte[] encoded) {
            final var fields = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 7, false, "TargetEvidenceScope");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "TargetEvidenceScope");
            if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
                throw new IllegalArgumentException("unknown Target evidence scope version");
            }
            final byte[] shard = QueryCodecSupport.fixed(fields.get(1), 2, 20);
            final var result = new TargetScope(new ShardId(new RouteIncarnation(Arrays.copyOf(shard, 16)),
                    (int) Bytes.readU32be(shard, 16)),
                    new TargetPartitionId(QueryCodecSupport.fixed(fields.get(2), 3, 32)),
                    new TargetKeyCodec.Domain(QueryCodecSupport.uint32(fields.get(3), 4),
                            QueryCodecSupport.uint64Bits(fields.get(4), 5)),
                    QueryCodecSupport.fixed(fields.get(5), 6, 16), QueryCodecSupport.fixed(fields.get(6), 7, 32));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetEvidenceScope");
            return result;
        }
    }

    private final TargetScope targetScope;
    private final EvidenceKind evidenceKind;
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

    private EvidenceCursor(
            final EvidenceKind evidenceKind,
            final byte[] destinationLaneId,
            final byte[] laneIncarnation,
            final byte[] evidenceResourceIncarnation,
            final int physicalPartition,
            final long evidenceGeneration,
            final long maxBrokerPersistedAtThroughCursor,
            final byte[] topicUuid,
            final long nextOffsetExclusive,
            final long lastObservedLsoExclusive,
            final byte[] resourceToken,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final long ledgerId,
            final long entryId,
            final int normalizedBatchIndex,
            final int batchSize) {
        this(evidenceKind, destinationLaneId, laneIncarnation, evidenceResourceIncarnation, physicalPartition,
                evidenceGeneration, maxBrokerPersistedAtThroughCursor, topicUuid, nextOffsetExclusive,
                lastObservedLsoExclusive, resourceToken, physicalTopic, physicalTopicCreationTimestamp,
                ledgerId, entryId, normalizedBatchIndex, batchSize, null);
    }

    private EvidenceCursor(
            final EvidenceKind evidenceKind, final byte[] destinationLaneId, final byte[] laneIncarnation,
            final byte[] evidenceResourceIncarnation, final int physicalPartition, final long evidenceGeneration,
            final long maxBrokerPersistedAtThroughCursor, final byte[] topicUuid, final long nextOffsetExclusive,
            final long lastObservedLsoExclusive, final byte[] resourceToken, final String physicalTopic,
            final long physicalTopicCreationTimestamp, final long ledgerId, final long entryId,
            final int normalizedBatchIndex, final int batchSize, final TargetScope targetScope) {
        this.targetScope = targetScope;
        this.evidenceKind = Objects.requireNonNull(evidenceKind, "evidenceKind");
        if (targetScope != null && (destinationLaneId != null || laneIncarnation != null
                || evidenceKind != EvidenceKind.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS)) {
            throw new IllegalArgumentException("Target cursor cannot alias a Lane or Kafka receipt namespace");
        }
        this.destinationLaneId = targetScope == null ? fixed(destinationLaneId, 32, "destinationLaneId") : null;
        this.laneIncarnation = targetScope == null ? fixed(laneIncarnation, 16, "laneIncarnation") : null;
        this.evidenceResourceIncarnation = nonEmpty(evidenceResourceIncarnation, "evidenceResourceIncarnation");
        if (evidenceGeneration == 0 || maxBrokerPersistedAtThroughCursor < 0) {
            throw new IllegalArgumentException("invalid evidence cursor counters");
        }
        this.physicalPartition = physicalPartition;
        this.evidenceGeneration = evidenceGeneration;
        this.maxBrokerPersistedAtThroughCursor = maxBrokerPersistedAtThroughCursor;
        this.kafka = evidenceKind == EvidenceKind.KAFKA_RECEIPT_CONTIGUOUS;
        this.topicUuid = topicUuid == null ? null : fixed(topicUuid, 16, "topicUuid");
        this.nextOffsetExclusive = nextOffsetExclusive;
        this.lastObservedLsoExclusive = lastObservedLsoExclusive;
        this.resourceToken = resourceToken == null ? null : fixed(resourceToken, 32, "resourceToken");
        this.physicalTopic = physicalTopic == null ? null : nfc(physicalTopic, "physicalTopic");
        this.physicalTopicCreationTimestamp = physicalTopicCreationTimestamp;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        if ((kafka && batchSize != 0)
                || (!kafka && (batchSize == 0 || Integer.compareUnsigned(normalizedBatchIndex, batchSize) >= 0))) {
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

    public static EvidenceCursor kafka(
            final byte[] destinationLaneId,
            final byte[] laneIncarnation,
            final byte[] topicUuid,
            final int physicalPartition,
            final long evidenceGeneration,
            final long maxBrokerPersistedAtThroughCursor,
            final long nextOffsetExclusive,
            final long lastObservedLsoExclusive) {
        return new EvidenceCursor(
                EvidenceKind.KAFKA_RECEIPT_CONTIGUOUS,
                destinationLaneId,
                laneIncarnation,
                topicUuid,
                physicalPartition,
                evidenceGeneration,
                maxBrokerPersistedAtThroughCursor,
                topicUuid,
                nextOffsetExclusive,
                lastObservedLsoExclusive,
                null,
                null,
                0,
                0,
                0,
                0,
                0);
    }

    public static EvidenceCursor pulsar(
            final byte[] destinationLaneId,
            final byte[] laneIncarnation,
            final byte[] resourceToken,
            final int physicalPartition,
            final long evidenceGeneration,
            final long maxBrokerPersistedAtThroughCursor,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final long ledgerId,
            final long entryId,
            final int normalizedBatchIndex,
            final int batchSize) {
        return new EvidenceCursor(
                EvidenceKind.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS,
                destinationLaneId,
                laneIncarnation,
                resourceToken,
                physicalPartition,
                evidenceGeneration,
                maxBrokerPersistedAtThroughCursor,
                null,
                0,
                0,
                resourceToken,
                physicalTopic,
                physicalTopicCreationTimestamp,
                ledgerId,
                entryId,
                normalizedBatchIndex,
                batchSize);
    }

    public EvidenceKind evidenceKind() {
        return evidenceKind;
    }

    public static EvidenceCursor targetPulsar(
            final TargetScope scope, final byte[] resourceToken, final int physicalPartition,
            final long evidenceGeneration, final long maxBrokerPersistedAtThroughCursor,
            final String physicalTopic, final long physicalTopicCreationTimestamp,
            final long ledgerId, final long entryId, final int normalizedBatchIndex, final int batchSize) {
        return new EvidenceCursor(EvidenceKind.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS, null, null,
                resourceToken, physicalPartition, evidenceGeneration, maxBrokerPersistedAtThroughCursor,
                null, 0, 0, resourceToken, physicalTopic, physicalTopicCreationTimestamp,
                ledgerId, entryId, normalizedBatchIndex, batchSize, Objects.requireNonNull(scope, "Target scope"));
    }

    public boolean isTarget() { return targetScope != null; }

    public TargetScope targetScope() {
        if (targetScope == null) {
            throw new IllegalStateException("Lane evidence cursor has no Target scope");
        }
        return targetScope;
    }

    public byte[] destinationLaneId() {
        if (targetScope != null) {
            throw new IllegalStateException("Target evidence cursor has no Lane identity");
        }
        return Bytes.copy(destinationLaneId);
    }

    public byte[] laneIncarnation() {
        if (targetScope != null) {
            throw new IllegalStateException("Target evidence cursor has no Lane incarnation");
        }
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
            if (targetScope == null) {
                CanonicalProtobuf.bytes(output, 2, destinationLaneId);
                CanonicalProtobuf.bytes(output, 3, laneIncarnation);
            }
            CanonicalProtobuf.bytes(output, 4, evidenceResourceIncarnation);
            CanonicalProtobuf.uint32Bits(output, 5, physicalPartition);
            CanonicalProtobuf.uint64Bits(output, 6, evidenceGeneration);
            CanonicalProtobuf.int64(output, 7, maxBrokerPersistedAtThroughCursor);
            if (targetScope != null) {
                CanonicalProtobuf.bytes(output, 8, targetScope.canonicalBytes());
            }
            if (kafka) {
                CanonicalProtobuf.bytes(output, 10, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, topicUuid);
                    CanonicalProtobuf.uint64Bits(fields, 2, nextOffsetExclusive);
                    CanonicalProtobuf.uint64Bits(fields, 3, lastObservedLsoExclusive);
                }));
            } else {
                CanonicalProtobuf.bytes(output, 11, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, resourceToken);
                    CanonicalProtobuf.bytes(fields, 2, physicalTopic.getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.uint64Bits(fields, 3, physicalTopicCreationTimestamp);
                    CanonicalProtobuf.uint64Bits(fields, 4, ledgerId);
                    CanonicalProtobuf.uint64Bits(fields, 5, entryId);
                    CanonicalProtobuf.uint32Bits(fields, 6, normalizedBatchIndex);
                    CanonicalProtobuf.uint32Bits(fields, 7, batchSize);
                }));
            }
        });
    }

    public static EvidenceCursor decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "EvidenceCursor");
        if (fields.size() == 7) {
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 4, 5, 6, 7, 8, 11}, "TargetEvidenceCursor");
            if (EvidenceKind.fromWire(QueryCodecSupport.uint(fields.getFirst(), 1))
                    != EvidenceKind.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS) {
                throw new IllegalArgumentException("Target cursor requires its Pulsar Journal branch");
            }
            final var member = QueryCodecSupport.read(
                    QueryCodecSupport.nested(fields.get(6), 11), "PulsarJournalCursor");
            QueryCodecSupport.requireNumbers(member, new int[] {1, 2, 3, 4, 5, 6, 7}, "PulsarJournalCursor");
            final byte[] resource = QueryCodecSupport.fixed(fields.get(1), 4, 32);
            if (!Arrays.equals(resource, QueryCodecSupport.fixed(member.getFirst(), 1, 32))) {
                throw new IllegalArgumentException("Target cursor resource differs from its Journal member");
            }
            final var result = targetPulsar(TargetScope.decode(QueryCodecSupport.nested(fields.get(5), 8)), resource,
                    QueryCodecSupport.uint32Bits(fields.get(2), 5), QueryCodecSupport.uint64Bits(fields.get(3), 6),
                    QueryCodecSupport.uint(fields.get(4), 7), utf8(QueryCodecSupport.bytes(member.get(1), 2)),
                    QueryCodecSupport.uint64Bits(member.get(2), 3), QueryCodecSupport.uint64Bits(member.get(3), 4),
                    QueryCodecSupport.uint64Bits(member.get(4), 5), QueryCodecSupport.uint32Bits(member.get(5), 6),
                    QueryCodecSupport.uint32Bits(member.get(6), 7));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetEvidenceCursor");
            return result;
        }
        if (fields.size() != 8
                || fields.get(0).number() != 1
                || fields.get(6).number() != 7
                || (fields.get(7).number() != 10 && fields.get(7).number() != 11)) {
            throw new IllegalArgumentException("invalid EvidenceCursor field order");
        }
        final EvidenceKind kind = EvidenceKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final byte[] lane = QueryCodecSupport.fixed(fields.get(1), 2, 32);
        final byte[] incarnation = QueryCodecSupport.fixed(fields.get(2), 3, 16);
        final byte[] resource = QueryCodecSupport.bytes(fields.get(3), 4);
        final int partition = QueryCodecSupport.uint32Bits(fields.get(4), 5);
        final long generation = QueryCodecSupport.uint(fields.get(5), 6);
        final long maxPersisted = QueryCodecSupport.uint(fields.get(6), 7);
        final EvidenceCursor result;
        if (fields.get(7).number() == 10) {
            if (kind != EvidenceKind.KAFKA_RECEIPT_CONTIGUOUS) {
                throw new IllegalArgumentException("EvidenceCursor kind/branch mismatch");
            }
            final List<CanonicalProtobuf.Reader.Field> cursor =
                    QueryCodecSupport.read(QueryCodecSupport.nested(fields.get(7), 10), "KafkaReceiptCursor");
            QueryCodecSupport.requireNumbers(cursor, new int[] {1, 2, 3}, "KafkaReceiptCursor");
            result = kafka(
                    lane,
                    incarnation,
                    fixed(resource, 16, "topicUuid"),
                    partition,
                    generation,
                    maxPersisted,
                    QueryCodecSupport.uint(cursor.get(1), 2),
                    QueryCodecSupport.uint(cursor.get(2), 3));
        } else {
            if (kind != EvidenceKind.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS) {
                throw new IllegalArgumentException("EvidenceCursor kind/branch mismatch");
            }
            final List<CanonicalProtobuf.Reader.Field> cursor =
                    QueryCodecSupport.read(QueryCodecSupport.nested(fields.get(7), 11), "PulsarJournalCursor");
            QueryCodecSupport.requireNumbers(cursor, new int[] {1, 2, 3, 4, 5, 6, 7}, "PulsarJournalCursor");
            result = pulsar(
                    lane,
                    incarnation,
                    fixed(resource, 32, "resourceToken"),
                    partition,
                    generation,
                    maxPersisted,
                    utf8(QueryCodecSupport.bytes(cursor.get(1), 2)),
                    QueryCodecSupport.uint(cursor.get(2), 3),
                    QueryCodecSupport.uint(cursor.get(3), 4),
                    QueryCodecSupport.uint(cursor.get(4), 5),
                    QueryCodecSupport.uint32Bits(cursor.get(5), 6),
                    QueryCodecSupport.uint32Bits(cursor.get(6), 7));
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EvidenceCursor");
        return result;
    }

    @Override
    public int compareTo(final EvidenceCursor other) {
        int result = Integer.compare(evidenceKind.wireValue(), other.evidenceKind.wireValue());
        if (result != 0) {
            return result;
        }
        result = Boolean.compare(isTarget(), other.isTarget());
        if (result != 0) {
            return result;
        }
        if (isTarget()) {
            result = compareUnsigned(targetScope.canonicalBytes(), other.targetScope.canonicalBytes());
        } else {
            result = compareUnsigned(destinationLaneId, other.destinationLaneId);
        }
        if (result != 0) {
            return result;
        }
        result = isTarget() ? 0 : compareUnsigned(laneIncarnation, other.laneIncarnation);
        if (result != 0) {
            return result;
        }
        result = compareUnsigned(evidenceResourceIncarnation, other.evidenceResourceIncarnation);
        if (result != 0) {
            return result;
        }
        result = Integer.compareUnsigned(physicalPartition, other.physicalPartition);
        if (result != 0) {
            return result;
        }
        return Long.compareUnsigned(evidenceGeneration, other.evidenceGeneration);
    }

    /**
     * Returns whether two cursors describe the same comparable evidence
     * stream. The evidence generation is part of this identity; cursors from
     * different generations are intentionally incomparable. For the Pulsar
     * branch, the journal's physical topic and creation timestamp are part of
     * the resource identity as well: a reused resource token must not make a
     * replacement topic look like a continuation of the old cursor.
     */
    public boolean sameIdentity(final EvidenceCursor other) {
        if (other == null
                || evidenceKind != other.evidenceKind
                || isTarget() != other.isTarget()
                || isTarget() && !Arrays.equals(targetScope.canonicalBytes(), other.targetScope.canonicalBytes())
                || !Arrays.equals(destinationLaneId, other.destinationLaneId)
                || !Arrays.equals(laneIncarnation, other.laneIncarnation)
                || !Arrays.equals(evidenceResourceIncarnation, other.evidenceResourceIncarnation)
                || physicalPartition != other.physicalPartition
                || evidenceGeneration != other.evidenceGeneration) {
            return false;
        }
        if (kafka) {
            return Arrays.equals(topicUuid, other.topicUuid);
        }
        return Arrays.equals(resourceToken, other.resourceToken)
                && physicalTopic.equals(other.physicalTopic)
                && physicalTopicCreationTimestamp == other.physicalTopicCreationTimestamp;
    }

    /**
     * Returns whether this cursor dominates {@code older} on the same
     * evidence stream. Kafka advances both the exclusive offset and LSO
     * watermark; Pulsar advances the inclusive ledger/entry/batch member.
     * Both branches must retain a non-regressing Broker-time anchor.
     */
    public boolean dominates(final EvidenceCursor older) {
        if (!sameIdentity(older) || maxBrokerPersistedAtThroughCursor < older.maxBrokerPersistedAtThroughCursor) {
            return false;
        }
        if (kafka) {
            return Long.compareUnsigned(nextOffsetExclusive, older.nextOffsetExclusive) >= 0
                    && Long.compareUnsigned(lastObservedLsoExclusive, older.lastObservedLsoExclusive) >= 0;
        }
        return comparePulsarMember(older) >= 0;
    }

    /** Returns whether this cursor is a strict successor of {@code older}. */
    public boolean strictlyDominates(final EvidenceCursor older) {
        return dominates(older) && !equals(older);
    }

    private int comparePulsarMember(final EvidenceCursor other) {
        int result = Long.compareUnsigned(ledgerId, other.ledgerId);
        if (result != 0) {
            return result;
        }
        result = Long.compareUnsigned(entryId, other.entryId);
        if (result != 0) {
            return result;
        }
        return Integer.compareUnsigned(normalizedBatchIndex, other.normalizedBatchIndex);
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
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
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
        return other instanceof EvidenceCursor that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
