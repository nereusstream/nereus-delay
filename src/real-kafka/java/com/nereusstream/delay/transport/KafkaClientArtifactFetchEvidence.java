package com.nereusstream.delay.transport;

import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.common.TopicPartition;

/** Validates the K1 Fetch proof before a record enters the Delay source SPI. */
final class KafkaClientArtifactFetchEvidence {
    private KafkaClientArtifactFetchEvidence() {}

    static <K, V> GuardedFetchEvidence requireBatch(
            final GuardedConsumerRecords<K, V> records, final ConsumerResourceGuard guard) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(guard, "guard");
        for (TopicPartition partition : records.partitions()) {
            if (!guard.topicPartition().equals(partition)) {
                throw new IllegalStateException("Kafka guarded Fetch returned a foreign topic partition");
            }
        }
        if (records.isEmpty()) {
            return null;
        }
        final GuardedFetchEvidence evidence = records.fetchEvidence();
        if (evidence == null
                || evidence.requestVersion() < 13
                || !guard.authenticatedClusterId().equals(evidence.authenticatedClusterId())
                || !guard.canonicalTopic().equals(evidence.canonicalTopic())
                || !guard.expectedTopicId().equals(evidence.expectedTopicId())
                || !guard.topicPartition().equals(evidence.topicPartition())
                || evidence.firstRecordOffset() < 0
                || evidence.lastRecordOffset() < evidence.firstRecordOffset()) {
            throw new IllegalStateException("Kafka guarded Fetch evidence does not match the source guard");
        }
        return evidence;
    }

    static void requireRecord(
            final ConsumerRecord<?, ?> record, final GuardedFetchEvidence evidence, final ConsumerResourceGuard guard) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(evidence, "evidence");
        if (!guard.topicPartition().topic().equals(record.topic())
                || guard.partition() != record.partition()
                || record.offset() < evidence.fetchOffset()
                || record.offset() < evidence.firstRecordOffset()
                || record.offset() > evidence.lastRecordOffset()) {
            throw new IllegalStateException("Kafka source record is outside its guarded Fetch proof");
        }
    }
}
