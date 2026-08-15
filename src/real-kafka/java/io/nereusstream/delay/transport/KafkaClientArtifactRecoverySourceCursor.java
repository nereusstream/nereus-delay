package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Native Kafka replay input for {@code OwnerRecoveryCoordinator}.
 *
 * <p>This cursor never commits a group offset and has no ACK path. The
 * coordinator retains the same decoded record until the bounded Store apply
 * outcome is proven, then calls {@link #next()} to release only the local
 * look-ahead. The caller supplies the already validated durable replay start
 * offset; the Route activation barrier remains an identity fence, not a
 * substitute for local Store recovery metadata.</p>
 */
public final class KafkaClientArtifactRecoverySourceCursor
        implements Iterator<SourceReplayEntry>, AutoCloseable {
    private final GuardedConsumer<byte[], byte[]> consumer;
    private final String authenticatedClusterId;
    private final java.util.UUID nativeTopicUuid;
    private final ShardId shard;
    private final TopicPartition topicPartition;
    private final ConsumerResourceGuard expectedGuard;
    private final Duration pollTimeout;
    private final ArrayDeque<BufferedRecord> buffered = new ArrayDeque<>();
    private SourceReplayEntry current;
    private boolean closed;

    public KafkaClientArtifactRecoverySourceCursor(final GuardedConsumer<byte[], byte[]> consumer,
                                                   final SourceAssignment assignment,
                                                   final String physicalTopic,
                                                   final long startOffsetInclusive,
                                                   final Duration pollTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        final SourceAssignment accepted = Objects.requireNonNull(assignment, "assignment");
        if (!(accepted.activationBarrier() instanceof KafkaActivationBarrier barrier)) {
            throw new IllegalArgumentException("Kafka recovery cursor requires a Kafka activation barrier");
        }
        if (startOffsetInclusive < 0 || barrier.exclusiveOffset() < 0) {
            throw new IllegalArgumentException("Kafka recovery offsets must be non-negative");
        }
        this.authenticatedClusterId = barrier.authenticatedClusterId();
        this.nativeTopicUuid = barrier.nativeTopicUuid();
        this.shard = accepted.shardId();
        final String topic = requirePhysicalTopic(physicalTopic);
        if (shard.partition() < 0) {
            throw new IllegalArgumentException("Kafka recovery partition must be non-negative");
        }
        this.topicPartition = new TopicPartition(topic, shard.partition());
        this.expectedGuard = new ConsumerResourceGuard(authenticatedClusterId, topic, toKafkaUuid(nativeTopicUuid),
                shard.partition());
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka guarded recovery consumer has a different resource guard");
        }
        consumer.assign(java.util.List.of(topicPartition));
        consumer.seek(topicPartition, startOffsetInclusive);
    }

    @Override
    public synchronized boolean hasNext() {
        ensureOpen();
        if (current != null) {
            return true;
        }
        while (buffered.isEmpty()) {
            final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(pollTimeout);
            final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, expectedGuard);
            for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, expectedGuard);
                buffered.addLast(new BufferedRecord(record, evidence));
            }
            if (records.isEmpty()) {
                return false;
            }
        }
        final BufferedRecord fetched = buffered.removeFirst();
        final ConsumerRecord<byte[], byte[]> record = fetched.record();
        try {
            if (record.offset() < 0 || record.timestamp() < 0) {
                throw new IllegalArgumentException("Kafka recovery record lacks a bounded broker position");
            }
            final KafkaSourcePosition position = new KafkaSourcePosition(shard, authenticatedClusterId,
                    nativeTopicUuid, record.offset(), record.leaderEpoch().orElse(null), record.timestamp());
            current = KafkaClientArtifactSourceRecordDecoder.decode(requireValue(record), shard, position, null, null);
            return true;
        } catch (RuntimeException | Error failure) {
            buffered.addFirst(fetched);
            throw failure;
        }
    }

    @Override
    public synchronized SourceReplayEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Kafka recovery source is exhausted");
        }
        final SourceReplayEntry result = current;
        current = null;
        return result;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            consumer.close();
        }
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private static byte[] requireValue(final ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null || record.value().length == 0) {
            throw new IllegalArgumentException("Kafka recovery record has no NDL1 value");
        }
        return record.value();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Kafka recovery source is closed");
        }
    }

    private static org.apache.kafka.common.Uuid toKafkaUuid(final java.util.UUID uuid) {
        return new org.apache.kafka.common.Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private record BufferedRecord(ConsumerRecord<byte[], byte[]> record, GuardedFetchEvidence evidence) {
    }
}
