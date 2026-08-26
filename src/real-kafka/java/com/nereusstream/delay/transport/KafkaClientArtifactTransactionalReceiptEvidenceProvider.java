package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.KafkaTransactionalDestinationRequest;
import com.nereusstream.delay.adapter.KafkaTransactionalPublishEvidence;
import com.nereusstream.delay.protocol.EvidenceCursor;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

/**
 * Resolves a committed K2 transaction through a pinned guarded Fetch v13.
 *
 * <p>The producer response proves the exact receipt offset reservation, but
 * not that EndTxn became visible to a read_committed reader. This provider
 * binds that response to a fresh consumer whose guard fixes cluster, topic
 * UUID and partition, then emits the shared typed K2 evidence only after the
 * exact keyed receipt and an LSO covering that offset are observed.</p>
 */
public final class KafkaClientArtifactTransactionalReceiptEvidenceProvider
        implements KafkaClientArtifactTransactionalDestinationTransport.PublishEvidenceProvider {
    private static final Duration RESOLUTION_TIMEOUT = Duration.ofSeconds(15);

    private final Map<String, Object> consumerConfiguration;
    private final long evidenceGeneration;
    private final Duration pollTimeout;

    public KafkaClientArtifactTransactionalReceiptEvidenceProvider(
            final Map<String, Object> consumerConfiguration,
            final long evidenceGeneration,
            final Duration pollTimeout) {
        Objects.requireNonNull(consumerConfiguration, "consumerConfiguration");
        if (evidenceGeneration == 0) {
            throw new IllegalArgumentException("evidenceGeneration must be non-zero");
        }
        Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        this.consumerConfiguration = Map.copyOf(consumerConfiguration);
        this.evidenceGeneration = evidenceGeneration;
        this.pollTimeout = pollTimeout;
    }

    @Override
    public Optional<byte[]> resolve(
            final KafkaTransactionalDestinationRequest request,
            final GuardedRecordMetadata targetMetadata,
            final GuardedRecordMetadata receiptMetadata) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(targetMetadata, "targetMetadata");
        Objects.requireNonNull(receiptMetadata, "receiptMetadata");
        final long receiptOffset = receiptMetadata.recordMetadata().offset();
        if (receiptOffset < 0
                || receiptMetadata.responseEvidence().logAppendTimeMs() < 0
                || !request.receiptPhysicalTopic()
                        .equals(receiptMetadata.recordMetadata().topic())
                || request.receiptResource().receiptPartition()
                        != receiptMetadata.recordMetadata().partition()
                || !request.receiptPhysicalTopic()
                        .equals(receiptMetadata.responseEvidence().canonicalTopic())
                || request.receiptResource().receiptPartition()
                        != receiptMetadata.responseEvidence().partition()
                || !request.receiptResource()
                        .nativeTopicUuid()
                        .equals(toJavaUuid(receiptMetadata.responseEvidence().expectedTopicId()))) {
            throw new IllegalArgumentException("Kafka receipt metadata does not match the pinned request");
        }

        final ConsumerResourceGuard guard = new ConsumerResourceGuard(
                request.receiptResource().authenticatedClusterId(),
                request.receiptPhysicalTopic(),
                toKafkaUuid(request.receiptResource().nativeTopicUuid()),
                request.receiptResource().receiptPartition());
        final Map<String, Object> configuration = new HashMap<>(consumerConfiguration);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "nereus-delay-k2-evidence-" + UUID.randomUUID());
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        configuration.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration,
                request.receiptResource().authenticatedClusterId(),
                request.receiptPhysicalTopic(),
                request.receiptResource().nativeTopicUuid(),
                request.receiptResource().receiptPartition());
        final TopicPartition topicPartition = new TopicPartition(
                request.receiptPhysicalTopic(), request.receiptResource().receiptPartition());
        try {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, receiptOffset);
            final long deadline = System.nanoTime() + RESOLUTION_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(pollTimeout);
                final GuardedFetchEvidence fetchEvidence =
                        KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                if (fetchEvidence == null) {
                    continue;
                }
                if (fetchEvidence.lastStableOffset() < 0) {
                    throw new IllegalStateException("Kafka read_committed Fetch omitted the LSO");
                }
                for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, fetchEvidence, guard);
                    if (record.offset() != receiptOffset) {
                        continue;
                    }
                    if (!Arrays.equals(request.receiptKey(), record.key())
                            || !Arrays.equals(request.receiptValue(), record.value())) {
                        throw new IllegalStateException("Kafka receipt record bytes do not match the exact request");
                    }
                    if (Long.compareUnsigned(fetchEvidence.lastStableOffset(), receiptOffset) <= 0) {
                        throw new IllegalStateException("Kafka read_committed Fetch LSO does not cover the receipt");
                    }
                    final EvidenceCursor cursor = EvidenceCursor.kafka(
                            request.mapping().producer().laneId().bytes(),
                            request.mapping().producer().laneIncarnation(),
                            uuidBytes(request.receiptResource().nativeTopicUuid()),
                            request.receiptResource().receiptPartition(),
                            evidenceGeneration,
                            receiptMetadata.responseEvidence().logAppendTimeMs(),
                            successor(receiptOffset),
                            fetchEvidence.lastStableOffset());
                    return Optional.of(KafkaTransactionalPublishEvidence.published(
                                    request,
                                    cursor,
                                    receiptOffset,
                                    KafkaTransactionalDestinationRequest.canonicalReceiptRecordHash(
                                            record.key(), record.value()))
                            .canonicalBytes());
                }
            }
            return Optional.empty();
        } finally {
            consumer.close();
        }
    }

    private static long successor(final long offset) {
        if (offset == -1L) {
            throw new IllegalArgumentException("Kafka receipt offset domain exhausted");
        }
        return offset + 1;
    }

    private static byte[] uuidBytes(final UUID uuid) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    private static Uuid toKafkaUuid(final UUID uuid) {
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private static UUID toJavaUuid(final Uuid uuid) {
        return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
}
