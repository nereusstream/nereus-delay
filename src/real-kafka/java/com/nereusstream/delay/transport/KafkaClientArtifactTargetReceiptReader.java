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
    public static final String REQUIRED_K1_SOURCE_COMMIT = "a707e84e023d0eb174fdf52c879dc1f9f1d6d2ce";
    public enum Status { WAITING, ADVANCED, PUBLISHED }
    public record Turn(Status status, long nextOffset, Observation observation) {}

    /** Private construction follows an actual guarded Broker read; it is never a DTO publication permission. */
    public static final class Observation {
        private final TargetKafkaReceiptMapping mapping;
        private final PublishEvidence evidence;
        private final GuardedFetchEvidence fetch;
        private final long brokerTime;
        private final long nextOffset;
        private Observation(TargetKafkaReceiptMapping mapping, PublishEvidence evidence,
                GuardedFetchEvidence fetch, long brokerTime, long nextOffset) {
            this.mapping = mapping; this.evidence = evidence; this.fetch = fetch; this.brokerTime = brokerTime;
            this.nextOffset = nextOffset;
        }
        public TargetKafkaReceiptMapping mapping() { return mapping; }
        public PublishEvidence evidence() { return evidence; }
        public GuardedFetchEvidence fetch() { return fetch; }
        public long brokerPersistenceTime() { return brokerTime; }
        public long nextOffset() { return nextOffset; }
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
    private final byte[] sdkJarSha256;
    private long nextOffset;
    private long maxBrokerTime;
    private Observation observed;
    private GuardedFetchEvidence rangeObservation;
    private long processedNextOffset = -1;
    private boolean closed;

    public KafkaClientArtifactTargetReceiptReader(final Map<String, Object> configuration,
            final TargetKafkaReceiptMapping mapping, final Protection protection,
            final Duration timeout, final int maximumRecords, final int maximumBytes) {
        sdkJarSha256 = requireSdkIdentity();
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
        try { return pollTurn(); }
        catch (RuntimeException | Error failure) {
            // Processing failure must not permit a later poll to skip the partially inspected response.
            try { close(); } catch (RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    private Turn pollTurn() {
        if (closed) { throw new IllegalStateException("Target receipt reader is closed"); }
        protection.requireCurrent(mapping, nextOffset);
        if (observed != null) { return new Turn(Status.PUBLISHED, nextOffset, observed); }
        rangeObservation = null;
        processedNextOffset = -1;
        final var records = consumer.pollGuarded(timeout);
        final var proof = KafkaClientArtifactFetchEvidence.requireResponse(records, guard);
        if (proof == null) { protection.requireCurrent(mapping, nextOffset); return new Turn(Status.WAITING, nextOffset, null); }
        if (proof.logStartOffset() < 0 || proof.lastStableOffset() < proof.logStartOffset()
                || proof.highWatermark() < proof.lastStableOffset()
                || proof.logStartOffset() > mapping.admission().kafkaReceipt().scanStartOffset()) {
            throw new IllegalStateException("Target receipt Fetch lacks same-response log-start/LSO/HW or has lost its original range");
        }
        protection.requireCurrent(mapping, nextOffset);
        rangeObservation = proof;
        final var processed = records.nextOffsets().get(partition);
        if (processed == null || proof.fetchOffset() > nextOffset || processed.offset() < nextOffset
                || processed.offset() > proof.lastStableOffset()) {
            throw new IllegalStateException("Target receipt response lacks its paired actual processed continuation");
        }
        final long before = nextOffset;
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
                observed = new Observation(mapping, evidence, proof, record.timestamp(), nextOffset);
                return new Turn(Status.PUBLISHED, nextOffset, observed);
            }
        }
        if (processed.offset() < nextOffset) {
            throw new IllegalStateException("Target receipt processed continuation precedes returned records");
        }
        // Adopt SDK continuation only after all returned records were validated. It includes actually
        // processed control/aborted batches; never infer progress from HW/LSO or consumer.position().
        nextOffset = processed.offset();
        processedNextOffset = nextOffset;
        protection.requireCurrent(mapping, nextOffset);
        return new Turn(nextOffset > before ? Status.ADVANCED : Status.WAITING, nextOffset, null);
    }

    /** Actual same-response range facts; this is not a contiguous-scan or NOT_PUBLISHED certification. */
    public synchronized Optional<GuardedFetchEvidence> rangeObservation() { return Optional.ofNullable(rangeObservation); }
    public synchronized long processedNextOffset() { return processedNextOffset; }

    public synchronized Optional<byte[]> rangeObservationImage() {
        if (rangeObservation == null) { return Optional.empty(); }
        final var e = rangeObservation;
        return Optional.of(com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 1, 2);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 2, com.nereusstream.delay.protocol.Bytes.utf8(REQUIRED_K1_SOURCE_COMMIT));
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 3, com.nereusstream.delay.protocol.Bytes.utf8(e.authenticatedClusterId()));
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 4, com.nereusstream.delay.protocol.Bytes.utf8(e.canonicalTopic()));
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 5, ByteBuffer.allocate(16)
                    .putLong(e.expectedTopicId().getMostSignificantBits()).putLong(e.expectedTopicId().getLeastSignificantBits()).array());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 6, e.partition());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 7, e.requestVersion());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32Bits(out, 8, e.correlationId());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 9, e.brokerNodeId());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 10, e.sessionId());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 11, e.fetchOffset());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 12, e.firstRecordOffset());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 13, e.lastRecordOffset());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 14, e.highWatermark());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 15, e.lastStableOffset());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(out, 16, e.logStartOffset());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 17, e.fetchResponseBodySha256());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 18, sdkJarSha256);
        }));
    }


    /** Reads the actual loaded SDK JAR, rather than trusting a caller's source-lock string. */
    static byte[] requireSdkIdentity() {
        try {
            final var location = Objects.requireNonNull(GuardedFetchEvidence.class.getProtectionDomain().getCodeSource(),
                    "loaded K1 code source").getLocation().toURI();
            final var path = java.nio.file.Path.of(location);
            final long size = java.nio.file.Files.size(path);
            if (!java.nio.file.Files.isRegularFile(path) || size <= 0 || size > 64L * 1024 * 1024) {
                throw new IllegalStateException("Target K2 requires a finite source-locked K1 JAR");
            }
            try (var jar = new java.util.jar.JarFile(path.toFile())) {
                final var entry = Objects.requireNonNull(jar.getJarEntry("kafka/kafka-version.properties"), "K1 version identity");
                final byte[] bytes;
                try (var stream = jar.getInputStream(entry)) { bytes = stream.readNBytes(1025); }
                if (bytes.length > 1024) { throw new IllegalStateException("K1 version identity exceeds bound"); }
                final var properties = new java.util.Properties();
                properties.load(new java.io.ByteArrayInputStream(bytes));
                if (!REQUIRED_K1_SOURCE_COMMIT.substring(0, 16).equals(properties.getProperty("commitId"))) {
                    throw new IllegalStateException("Target K2 requires the activated K1 empty-Fetch/log-start source lock");
                }
            }
            final var digest = java.security.MessageDigest.getInstance("SHA-256");
            final byte[] buffer = new byte[65536];
            long read = 0;
            try (var stream = java.nio.file.Files.newInputStream(path)) {
                for (int count; (count = stream.read(buffer)) != -1;) {
                    read = Math.addExact(read, count);
                    if (read > 64L * 1024 * 1024) { throw new IllegalStateException("K1 JAR changed beyond byte bound"); }
                    digest.update(buffer, 0, count);
                }
            }
            if (read != size) { throw new IllegalStateException("K1 JAR identity changed during read"); }
            return digest.digest();
        } catch (java.io.IOException | java.net.URISyntaxException | java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("actual K1 JAR/source identity is unavailable", failure);
        }
    }

    public synchronized Optional<Observation> observation() { return Optional.ofNullable(observed); }
    @Override public synchronized void close() { if (!closed) { closed = true; consumer.close(); } }
}
