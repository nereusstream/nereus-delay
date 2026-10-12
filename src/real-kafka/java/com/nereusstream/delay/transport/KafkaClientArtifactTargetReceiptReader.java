package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetKafkaReceiptWire;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

/** Bounded resumable read_committed lookup from the original protected range, independent of old producer callbacks. */
public final class KafkaClientArtifactTargetReceiptReader implements AutoCloseable {
    public enum Status { WAITING, ADVANCED, PUBLISHED }
    public record Turn(Status status, long nextOffset, Observation observation) {}

    /** Private construction follows an actual guarded Broker read; it is never a DTO publication permission. */
    public static final class Observation {
        private final TargetKafkaReceiptMapping mapping;
        private final PublishEvidence evidence;
        private final GuardedFetchEvidence fetch;
        private final long brokerTime;
        private Observation(TargetKafkaReceiptMapping mapping, PublishEvidence evidence,
                GuardedFetchEvidence fetch, long brokerTime) {
            this.mapping = mapping; this.evidence = evidence; this.fetch = fetch; this.brokerTime = brokerTime;
        }
        public TargetKafkaReceiptMapping mapping() { return mapping; }
        public PublishEvidence evidence() { return evidence; }
        public GuardedFetchEvidence fetch() { return fetch; }
        public long brokerPersistenceTime() { return brokerTime; }
        public void requireEvidence(final PublishEvidence candidate) {
            if (!Arrays.equals(evidence.canonicalBytes(), candidate.canonicalBytes())) {
                throw new IllegalArgumentException("Target receipt candidate differs from actual protected read");
            }
        }
    }

    /** Real Source/Owner/resource/credential/retention/pin rights must survive each poll and observation. */
    @FunctionalInterface
    public interface Protection { void requireCurrent(TargetKafkaReceiptMapping mapping, long nextOffset); }

    private final TargetKafkaReceiptMapping mapping;
    private final GuardedConsumer<byte[], byte[]> consumer;
    private final ConsumerResourceGuard guard;
    private final TopicPartition partition;
    private final Protection protection;
    private final Duration timeout;
    private final int maximumRecords;
    private final long maximumBytes;
    private long nextOffset;
    private long maxBrokerTime;
    private Observation observed;
    private boolean closed;

    public KafkaClientArtifactTargetReceiptReader(final Map<String, Object> configuration,
            final TargetKafkaReceiptMapping mapping, final Protection protection,
            final Duration timeout, final int maximumRecords, final int maximumBytes) {
        this.mapping = Objects.requireNonNull(mapping, "actual retained mapping");
        this.protection = Objects.requireNonNull(protection, "protected read authority");
        this.timeout = Objects.requireNonNull(timeout, "poll deadline");
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofSeconds(60)) > 0
                || maximumRecords <= 0 || maximumBytes <= 0) {
            throw new IllegalArgumentException("Target receipt read requires finite positive bounds");
        }
        this.maximumRecords = maximumRecords;
        this.maximumBytes = maximumBytes;
        nextOffset = mapping.admission().kafkaReceipt().scanStartOffset();
        protection.requireCurrent(mapping, nextOffset);
        final var receipt = mapping.admission().kafkaReceipt();
        final var resource = receipt.receiptResource();
        guard = new ConsumerResourceGuard(resource.authenticatedClusterId(), receipt.receiptPhysicalTopic(),
                new Uuid(resource.receiptTopicUuid().getMostSignificantBits(), resource.receiptTopicUuid().getLeastSignificantBits()),
                receipt.receiptPartition());
        partition = guard.topicPartition();
        final Map<String, Object> config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
        if (config.containsKey(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)) {
            throw new IllegalArgumentException("Target receipt evidence cannot run record-transforming interceptors");
        }
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maximumRecords);
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maximumBytes);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maximumBytes);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        consumer = KafkaClientArtifactSourceConsumerFactory.create(config, resource.authenticatedClusterId(),
                receipt.receiptPhysicalTopic(), resource.receiptTopicUuid(), receipt.receiptPartition());
        try {
            consumer.assign(List.of(partition));
            consumer.seek(partition, nextOffset);
            protection.requireCurrent(mapping, nextOffset);
        } catch (RuntimeException | Error failure) {
            try { consumer.close(); } catch (RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    /** Performs one SDK poll and retains its bounded continuation; empty responses never certify absence. */
    public synchronized Turn runTurn() {
        if (closed) { throw new IllegalStateException("Target receipt reader is closed"); }
        protection.requireCurrent(mapping, nextOffset);
        if (observed != null) { return new Turn(Status.PUBLISHED, nextOffset, observed); }
        final var records = consumer.pollGuarded(timeout);
        final var proof = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
        if (proof == null) { protection.requireCurrent(mapping, nextOffset); return new Turn(Status.WAITING, nextOffset, null); }
        if (proof.lastStableOffset() < 0) {
            throw new IllegalStateException("Target receipt Fetch omits LSO");
        }
        int count = 0;
        long bytes = 0;
        for (var record : records.records(partition)) {
            KafkaClientArtifactFetchEvidence.requireRecord(record, proof, guard);
            if (record.offset() < nextOffset || record.offset() >= proof.lastStableOffset()
                    || ++count > maximumRecords) { throw new IllegalStateException("Target receipt batch violates its range/LSO/record bound"); }
            bytes = Math.addExact(bytes, (record.key() == null ? 0 : record.key().length)
                    + (long) (record.value() == null ? 0 : record.value().length));
            if (bytes > maximumBytes) { throw new IllegalStateException("Target receipt batch exceeds byte bound"); }
            nextOffset = Math.addExact(record.offset(), 1);
            if (record.timestampType() != TimestampType.LOG_APPEND_TIME || record.timestamp() < 0) {
                throw new IllegalStateException("Target receipt resource requires actual Broker LogAppendTime");
            }
            maxBrokerTime = Math.max(maxBrokerTime, record.timestamp());
            if (Arrays.equals(TargetKafkaReceiptWire.key(mapping), record.key())) {
                TargetKafkaReceiptWire.requireRecord(mapping, record.key(), record.value());
                final var uuid = mapping.admission().kafkaReceipt().receiptResource().receiptTopicUuid();
                final var cursor = EvidenceCursor.targetKafka(TargetKafkaReceiptWire.scope(mapping),
                        ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array(),
                        partition.partition(), mapping.admission().channel().context().evidenceGeneration(),
                        maxBrokerTime, nextOffset, proof.lastStableOffset());
                final var evidence = TargetKafkaReceiptWire.published(mapping, cursor, record.offset());
                protection.requireCurrent(mapping, nextOffset);
                observed = new Observation(mapping, evidence, proof, record.timestamp());
                return new Turn(Status.PUBLISHED, nextOffset, observed);
            }
        }
        // Re-seek the explicit processed continuation; no offset from an unprocessed batch becomes a proof.
        consumer.seek(partition, nextOffset);
        protection.requireCurrent(mapping, nextOffset);
        return new Turn(Status.ADVANCED, nextOffset, null);
    }

    public synchronized Optional<Observation> observation() { return Optional.ofNullable(observed); }
    @Override public synchronized void close() { if (!closed) { closed = true; consumer.close(); } }
}
