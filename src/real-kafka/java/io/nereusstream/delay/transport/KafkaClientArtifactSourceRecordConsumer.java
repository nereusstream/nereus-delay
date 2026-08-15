package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.SourceAcknowledgement;
import io.nereusstream.delay.ownership.SourceRecordConsumer;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
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
 * <p>The consumer deliberately has no Nereus apply authority: it decodes one
 * canonical Client Command or signed System Mutation, exposes the exact
 * Kafka Source Position, and advances the broker cursor only after
 * {@code commitSync} returns. A commit exception is always {@code UNKNOWN};
 * the caller must retain the record and retry it after a fresh
 * consumer/owner boundary.</p>
 */
public final class KafkaClientArtifactSourceRecordConsumer implements SourceRecordConsumer {
    private final GuardedConsumer<byte[], byte[]> consumer;
    private final String authenticatedClusterId;
    private final UUID nativeTopicUuid;
    private final ShardId shard;
    private final TopicPartition topicPartition;
    private final ConsumerResourceGuard expectedGuard;
    private final Duration pollTimeout;
    private final ArrayDeque<BufferedRecord> buffered = new ArrayDeque<>();
    private BufferedRecord inFlight;
    private boolean closed;

    public KafkaClientArtifactSourceRecordConsumer(final GuardedConsumer<byte[], byte[]> consumer,
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
        this.expectedGuard = new ConsumerResourceGuard(authenticatedClusterId, physicalTopic,
                toKafkaUuid(nativeTopicUuid), shard.partition());
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka guarded source consumer has a different resource guard");
        }
        consumer.assign(List.of(topicPartition));
    }

    @Override
    public synchronized Optional<PolledSourceRecord> poll() {
        ensureOpen();
        if (inFlight != null) {
            throw new IllegalStateException("previous Kafka source record has not been ACKed");
        }
        if (buffered.isEmpty()) {
            final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(pollTimeout);
            final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, expectedGuard);
            for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, expectedGuard);
                buffered.addLast(new BufferedRecord(record, evidence));
            }
        }
        if (buffered.isEmpty()) {
            return Optional.empty();
        }
        final BufferedRecord fetched = buffered.removeFirst();
        final ConsumerRecord<byte[], byte[]> record = fetched.record();
        try {
            if (record.timestamp() < 0 || record.offset() == -1L) {
                throw new IllegalArgumentException("Kafka source record lacks a bounded broker position");
            }
            final KafkaSourcePosition position = new KafkaSourcePosition(shard, authenticatedClusterId,
                    nativeTopicUuid, record.offset(), record.leaderEpoch().orElse(null), record.timestamp());
            final SourceReplayEntry entry = KafkaClientArtifactSourceRecordDecoder.decode(requireValue(record), shard,
                    position, null, null);
            inFlight = fetched;
            return Optional.of(new PolledSourceRecord(entry,
                    (candidate, ignoredOutcome) -> acknowledge(fetched, entry, candidate)));
        } catch (RuntimeException failure) {
            buffered.addFirst(fetched);
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
            final BufferedRecord fetched, final SourceReplayEntry expected, final SourceReplayEntry candidate) {
        if (candidate != expected) {
            return SourceAcknowledgement.AcknowledgementResult.unknown(
                    new IllegalStateException("Kafka source ACK entry identity changed"));
        }
        synchronized (this) {
            if (inFlight != fetched || closed) {
                return SourceAcknowledgement.AcknowledgementResult.unknown(
                        new IllegalStateException("Kafka source ACK state changed"));
            }
        }
        try {
            consumer.commitSync(java.util.Map.of(topicPartition,
                    new OffsetAndMetadata(fetched.record().offset() + 1)));
            synchronized (this) {
                if (inFlight != fetched) {
                    return SourceAcknowledgement.AcknowledgementResult.unknown(
                            new IllegalStateException("Kafka source ACK state changed"));
                }
                inFlight = null;
            }
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

    private static org.apache.kafka.common.Uuid toKafkaUuid(final UUID uuid) {
        return new org.apache.kafka.common.Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private record BufferedRecord(ConsumerRecord<byte[], byte[]> record, GuardedFetchEvidence evidence) {
    }
}
