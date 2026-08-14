package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.SourceAcknowledgement;
import io.nereusstream.delay.ownership.SourceRecordConsumer;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Source-set Kafka binding for one assigned V1 Shard Log partition.
 *
 * <p>The consumer deliberately has no Nereus apply authority: it decodes a
 * canonical NDL1 Client Command, exposes the exact Kafka Source Position, and
 * advances the broker cursor only after {@code commitSync} returns.  A commit
 * exception is always {@code UNKNOWN}; the caller must retain the record and
 * retry it after a fresh consumer/owner boundary.</p>
 */
public final class KafkaClientArtifactSourceRecordConsumer implements SourceRecordConsumer {
    private final Consumer<byte[], byte[]> consumer;
    private final String authenticatedClusterId;
    private final UUID nativeTopicUuid;
    private final ShardId shard;
    private final TopicPartition topicPartition;
    private final Duration pollTimeout;
    private final ArrayDeque<ConsumerRecord<byte[], byte[]>> buffered = new ArrayDeque<>();
    private boolean closed;

    public KafkaClientArtifactSourceRecordConsumer(final Consumer<byte[], byte[]> consumer,
                                                   final String authenticatedClusterId,
                                                   final UUID nativeTopicUuid,
                                                   final ShardId shard,
                                                   final String physicalTopic,
                                                   final Duration pollTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.authenticatedClusterId = Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        this.nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.topicPartition = new TopicPartition(Objects.requireNonNull(physicalTopic, "physicalTopic"),
                shard.partition());
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        consumer.assign(List.of(topicPartition));
    }

    @Override
    public synchronized Optional<PolledSourceRecord> poll() {
        ensureOpen();
        if (buffered.isEmpty()) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                buffered.addLast(record);
            }
        }
        if (buffered.isEmpty()) {
            return Optional.empty();
        }
        final ConsumerRecord<byte[], byte[]> record = buffered.removeFirst();
        try {
            final PreparedCommand command = CommandCodec.decodeFrameV1(requireValue(record));
            if (!command.shardId().equals(shard)) {
                throw new IllegalArgumentException("Kafka source command belongs to another Shard");
            }
            if (record.timestamp() < 0 || record.offset() == -1L) {
                throw new IllegalArgumentException("Kafka source record lacks a bounded broker position");
            }
            final KafkaSourcePosition position = new KafkaSourcePosition(shard, authenticatedClusterId,
                    nativeTopicUuid, record.offset(), record.leaderEpoch().orElse(null), record.timestamp());
            final SourceReplayRecord entry = new SourceReplayRecord(command, position, null, null);
            return Optional.of(new PolledSourceRecord(entry,
                    (candidate, ignoredOutcome) -> acknowledge(record, entry, candidate)));
        } catch (RuntimeException failure) {
            buffered.addFirst(record);
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            consumer.close();
        }
    }

    private SourceAcknowledgement.AcknowledgementResult acknowledge(
            final ConsumerRecord<byte[], byte[]> record,
            final SourceReplayRecord expected,
            final io.nereusstream.delay.ownership.SourceReplayEntry candidate) {
        if (candidate != expected) {
            return SourceAcknowledgement.AcknowledgementResult.unknown(
                    new IllegalStateException("Kafka source ACK entry identity changed"));
        }
        try {
            consumer.commitSync(java.util.Map.of(topicPartition, new OffsetAndMetadata(record.offset() + 1)));
            return SourceAcknowledgement.AcknowledgementResult.acked();
        } catch (RuntimeException failure) {
            return SourceAcknowledgement.AcknowledgementResult.unknown(failure);
        }
    }

    private static byte[] requireValue(final ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null || record.value().length == 0) {
            throw new IllegalArgumentException("Kafka source record has no NDL1 value");
        }
        return record.value();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Kafka source consumer is closed");
        }
    }
}
