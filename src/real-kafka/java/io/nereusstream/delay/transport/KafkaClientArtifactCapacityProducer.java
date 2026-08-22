package io.nereusstream.delay.transport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.GuardedCallback;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.clients.producer.ResourceGuardException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Source-locked physical Kafka capacity producer.
 *
 * <p>This class is deliberately separate from the functional Gateway smokes.
 * It measures the guarded K1 append path against a real broker and writes a
 * self-contained observation.  The enclosing shell campaign is responsible
 * for Worker/Object Store observations and for assembling the full matrix.</p>
 */
public final class KafkaClientArtifactCapacityProducer {
    private static final int ARGUMENT_COUNT = 17;
    private static final long MAX_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long PRODUCER_EPOCH_RECORDS = 500_000L;

    private KafkaClientArtifactCapacityProducer() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != ARGUMENT_COUNT) {
            throw new IllegalArgumentException("usage: <bootstrap> <topic> <artifact> <records> <payload-bytes> "
                    + "<arrival> <ordering> <consistency> <target-health> <placement> <payload-mode> "
                    + "<partitions> <batch-bytes> <linger-ms> <rate-per-second> <max-in-flight> <delete-topic>");
        }
        final Configuration configuration = Configuration.parse(arguments);
        final Map<String, Object> adminConfiguration = new HashMap<>();
        adminConfiguration.put("bootstrap.servers", configuration.bootstrap());
        adminConfiguration.put("request.timeout.ms", "30000");
        adminConfiguration.put("default.api.timeout.ms", "30000");
        try (Admin admin = Admin.create(adminConfiguration)) {
            final String clusterId = admin.describeCluster().clusterId().get(30, TimeUnit.SECONDS);
            final TopicDescription description = ensureTopic(admin, configuration);
            if (description.partitions().size() != configuration.partitions()) {
                throw new IllegalStateException("Kafka topic partition count changed: expected="
                        + configuration.partitions() + " actual=" + description.partitions().size());
            }
            if (Uuid.ZERO_UUID.equals(description.topicId())) {
                throw new IllegalStateException("Kafka topic has no non-zero topic ID: " + configuration.topic());
            }
            final Observation observation = produce(configuration, clusterId, description.topicId());
            writeObservation(configuration.artifact(), observation);
            if (configuration.deleteTopic()) {
                admin.deleteTopics(java.util.List.of(configuration.topic())).all().get(30, TimeUnit.SECONDS);
            }
            if (!observation.pass()) {
                throw new IllegalStateException("Kafka capacity observation failed: " + observation.failure());
            }
        }
        System.out.println("Kafka guarded capacity observation passed: " + configuration.artifact());
    }

    private static TopicDescription ensureTopic(final Admin admin, final Configuration configuration)
            throws Exception {
        try {
            return admin.describeTopics(java.util.List.of(configuration.topic())).allTopicNames()
                    .get(30, TimeUnit.SECONDS).get(configuration.topic());
        } catch (ExecutionException missing) {
            final NewTopic topic = new NewTopic(configuration.topic(), configuration.partitions(), (short) 3);
            admin.createTopics(java.util.List.of(topic)).all().get(30, TimeUnit.SECONDS);
            return admin.describeTopics(java.util.List.of(configuration.topic())).allTopicNames()
                    .get(30, TimeUnit.SECONDS).get(configuration.topic());
        }
    }

    private static Observation produce(final Configuration configuration, final String clusterId,
                                       final Uuid topicId) throws Exception {
        final long startNanos = System.nanoTime();
        final AtomicLong accepted = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        final AtomicLong guardedEvidence = new AtomicLong();
        final AtomicLong badAccepted = new AtomicLong();
        final AtomicLong minOffset = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong maxOffset = new AtomicLong(-1L);
        final AtomicReference<String> firstFailure = new AtomicReference<>();
        final AtomicLongArray partitionCounts = new AtomicLongArray(configuration.partitions());
        long recordStart = 0L;
        boolean pass = true;
        while (recordStart < configuration.records()) {
            final int epochRecords = (int) Math.min(PRODUCER_EPOCH_RECORDS,
                    configuration.records() - recordStart);
            final Observation epoch = produceEpoch(configuration, clusterId, topicId, recordStart, epochRecords);
            accepted.addAndGet(epoch.accepted());
            rejected.addAndGet(epoch.rejected());
            errors.addAndGet(epoch.errors());
            guardedEvidence.addAndGet(epoch.guardedEvidence());
            badAccepted.addAndGet(epoch.badAccepted());
            updateMinimum(minOffset, epoch.minOffset());
            updateMaximum(maxOffset, epoch.maxOffset());
            for (int partition = 0; partition < partitionCounts.length(); partition++) {
                partitionCounts.addAndGet(partition, epoch.partitions().get(partition));
            }
            if (!epoch.pass()) {
                pass = false;
                firstFailure.compareAndSet(null, epoch.failure());
            }
            recordStart += epochRecords;
        }
        final long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);
        final boolean countsMatch = accepted.get() == configuration.records() - expectedBadRecords(configuration)
                && rejected.get() == expectedBadRecords(configuration);
        pass = pass && countsMatch && errors.get() == 0 && badAccepted.get() == 0
                && guardedEvidence.get() == accepted.get() && maxOffset.get() >= 0;
        return new Observation(configuration, clusterId, topicId, accepted.get(), rejected.get(), errors.get(),
                guardedEvidence.get(), badAccepted.get(), payload(configuration.payloadBytes(),
                configuration.payloadMode(), configuration.topic()).length, elapsedNanos, minOffset.get(),
                maxOffset.get(), partitionCounts, pass, firstFailure.get());
    }

    private static Observation produceEpoch(final Configuration configuration, final String clusterId,
                                            final Uuid topicId, final long recordStart,
                                            final int epochRecords) throws Exception {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrap());
        properties.put(ProducerConfig.ACKS_CONFIG, configuration.strong() ? "all" : "1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(configuration.strong()));
        properties.put("allow.auto.create.topics", "false");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(configuration.batchBytes()));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, Long.toString(configuration.lingerMs()));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                Integer.toString(configuration.ordered() ? 1 : 5));
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                Long.toString(Math.max(32L * 1024 * 1024, configuration.payloadBytes() * 64L)));
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-capacity-" + configuration.profileName());

        final AtomicLong accepted = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        final AtomicLong guardedEvidence = new AtomicLong();
        final AtomicLong badAccepted = new AtomicLong();
        final AtomicLong minOffset = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong maxOffset = new AtomicLong(-1L);
        final AtomicReference<String> firstFailure = new AtomicReference<>();
        final AtomicLongArray partitionCounts = new AtomicLongArray(configuration.partitions());
        final long expectedBad = expectedBadRecords(configuration, recordStart, epochRecords);
        final Semaphore permits = new Semaphore(configuration.maxInFlight());
        final CountDownLatch completions = new CountDownLatch(epochRecords);
        final long startNanos = System.nanoTime();
        final byte[] payload = payload(configuration.payloadBytes(), configuration.payloadMode(),
                configuration.topic());
        final Uuid badTopicId = Uuid.randomUuid();
        final ProducerResourceGuard[] healthyGuards = guards(clusterId, configuration.topic(), topicId,
                configuration.partitions());
        final ProducerResourceGuard[] badGuards = guards(clusterId, configuration.topic(), badTopicId,
                configuration.partitions());
        final GuardedCallback callback = (metadata, failure) -> {
            final ProducerResourceGuard completionGuard = metadata != null
                    ? metadata.resourceGuard()
                    : failure instanceof ResourceGuardException
                    ? ((ResourceGuardException) failure).guard() : null;
            final boolean bad = completionGuard != null && !topicId.equals(completionGuard.expectedTopicId());
            final int partition = completionGuard == null ? -1 : completionGuard.partition();
            try {
                if (failure != null) {
                    if (bad) {
                        rejected.incrementAndGet();
                    } else {
                        recordFailure(errors, firstFailure, failure);
                    }
                } else if (completionGuard != null
                        && valid(metadata, completionGuard, configuration.topic(), partition)) {
                    if (bad) {
                        badAccepted.incrementAndGet();
                    } else {
                        accepted.incrementAndGet();
                        guardedEvidence.incrementAndGet();
                        partitionCounts.incrementAndGet(partition);
                        final long offset = metadata.recordMetadata().offset();
                        updateMinimum(minOffset, offset);
                        updateMaximum(maxOffset, offset);
                    }
                } else {
                    recordFailure(errors, firstFailure,
                            new IllegalStateException("guarded Kafka response evidence mismatch"));
                }
            } finally {
                completions.countDown();
                permits.release();
            }
        };

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties,
                new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            for (long relativeRecord = 0; relativeRecord < epochRecords; relativeRecord++) {
                final long record = recordStart + relativeRecord;
                waitForRate(startNanos, relativeRecord, configuration.ratePerSecond());
                permits.acquire();
                final int partition = choosePartition(configuration, record);
                final boolean bad = isBadRecord(configuration, record);
                final ProducerResourceGuard guard = bad ? badGuards[partition] : healthyGuards[partition];
                final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(configuration.topic(),
                        partition, key(record), payload);
                try {
                    guarded.sendGuarded(producerRecord, guard, callback);
                } catch (RuntimeException failure) {
                    try {
                        if (bad) {
                            rejected.incrementAndGet();
                        } else {
                            recordFailure(errors, firstFailure, failure);
                        }
                    } finally {
                        completions.countDown();
                        permits.release();
                    }
                }
            }
            final long timeoutSeconds = Math.max(120L,
                    Math.min(86_400L, epochRecords / 1_000L + 120L));
            if (!completions.await(timeoutSeconds, TimeUnit.SECONDS)) {
                recordFailure(errors, firstFailure, new IllegalStateException("guarded Kafka completions timed out"));
            }
            producer.flush();
        }
        final long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);
        final boolean countsMatch = accepted.get() == epochRecords - expectedBad
                && rejected.get() == expectedBad;
        final boolean pass = countsMatch && errors.get() == 0 && badAccepted.get() == 0
                && guardedEvidence.get() == accepted.get() && maxOffset.get() >= 0;
        return new Observation(configuration, clusterId, topicId, accepted.get(), rejected.get(), errors.get(),
                guardedEvidence.get(), badAccepted.get(), payload.length, elapsedNanos, minOffset.get(), maxOffset.get(),
                partitionCounts, pass, firstFailure.get());
    }

    private static ProducerResourceGuard[] guards(final String clusterId, final String topic,
                                                  final Uuid topicId, final int partitions) {
        final ProducerResourceGuard[] guards = new ProducerResourceGuard[partitions];
        for (int partition = 0; partition < partitions; partition++) {
            guards[partition] = new ProducerResourceGuard(clusterId, topic, topicId, partition);
        }
        return guards;
    }

    private static boolean valid(final GuardedRecordMetadata metadata, final ProducerResourceGuard guard,
                                 final String topic, final int partition) {
        return metadata != null && metadata.resourceGuard().equals(guard)
                && metadata.recordMetadata() != null && topic.equals(metadata.recordMetadata().topic())
                && metadata.recordMetadata().partition() == partition && metadata.recordMetadata().offset() >= 0
                && metadata.responseEvidence() != null
                && metadata.responseEvidence().canonicalTopic().equals(topic)
                && metadata.responseEvidence().partition() == partition
                && metadata.responseEvidence().errorCode() == 0;
    }

    private static void recordFailure(final AtomicLong errors, final AtomicReference<String> firstFailure,
                                      final Throwable failure) {
        errors.incrementAndGet();
        firstFailure.compareAndSet(null, failure.getClass().getName() + ": " + String.valueOf(failure.getMessage()));
    }

    private static long expectedBadRecords(final Configuration configuration) {
        return configuration.targetHealth().equals("bad") ? (configuration.records() + 99L) / 100L : 0L;
    }

    private static long expectedBadRecords(final Configuration configuration, final long recordStart,
                                           final long recordCount) {
        if (!configuration.targetHealth().equals("bad")) {
            return 0L;
        }
        final long firstBad = recordStart % 100L == 0L
                ? recordStart : recordStart + (100L - recordStart % 100L);
        if (firstBad >= recordStart + recordCount) {
            return 0L;
        }
        return ((recordStart + recordCount - 1L - firstBad) / 100L) + 1L;
    }

    private static boolean isBadRecord(final Configuration configuration, final long record) {
        return configuration.targetHealth().equals("bad") && record % 100L == 0L;
    }

    private static int choosePartition(final Configuration configuration, final long record) {
        if (configuration.partitions() == 1) {
            return 0;
        }
        if (!configuration.arrival().equals("zipf")) {
            return (int) (record % configuration.partitions());
        }
        final double sample = ((record * 1103515245L + 12345L) & 0x7fffffffL) / (double) Integer.MAX_VALUE;
        double cumulative = 0.0d;
        double normalizer = 0.0d;
        for (int partition = 1; partition <= configuration.partitions(); partition++) {
            normalizer += 1.0d / partition;
        }
        for (int partition = 1; partition <= configuration.partitions(); partition++) {
            cumulative += (1.0d / partition) / normalizer;
            if (sample <= cumulative) {
                return partition - 1;
            }
        }
        return configuration.partitions() - 1;
    }

    private static byte[] key(final long record) {
        final byte[] key = new byte[8];
        for (int index = 7; index >= 0; index--) {
            key[index] = (byte) record;
        }
        return key;
    }

    private static byte[] payload(final int size, final String payloadMode, final String topic) {
        if (payloadMode.equals("object")) {
            return ("s3://nereus-delay-capacity/" + topic + "/payload").getBytes(StandardCharsets.UTF_8);
        }
        final byte[] payload = new byte[size];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) (index * 31 + size);
        }
        return payload;
    }

    private static void waitForRate(final long startNanos, final long record, final long ratePerSecond) {
        if (ratePerSecond <= 0) {
            return;
        }
        final long target = startNanos + (long) ((record * 1_000_000_000.0d) / ratePerSecond);
        long remaining;
        while ((remaining = target - System.nanoTime()) > 0) {
            LockSupport.parkNanos(Math.min(remaining, MAX_SLEEP_NANOS));
        }
    }

    private static void updateMinimum(final AtomicLong value, final long candidate) {
        long current;
        do {
            current = value.get();
            if (candidate >= current) {
                return;
            }
        } while (!value.compareAndSet(current, candidate));
    }

    private static void updateMaximum(final AtomicLong value, final long candidate) {
        long current;
        do {
            current = value.get();
            if (candidate <= current) {
                return;
            }
        } while (!value.compareAndSet(current, candidate));
    }

    private static void writeObservation(final Path artifact, final Observation observation) throws IOException {
        final Path absolute = artifact.toAbsolutePath();
        if (absolute.getParent() != null) {
            Files.createDirectories(absolute.getParent());
        }
        Files.writeString(absolute, observation.json());
    }

    private record Configuration(String bootstrap, String topic, Path artifact, long records, int payloadBytes,
                                 String arrival, String ordering, String consistency, String targetHealth,
                                 String placement, String payloadMode, int partitions, int batchBytes, long lingerMs,
                                 long ratePerSecond, int maxInFlight, boolean deleteTopic) {
        private static Configuration parse(final String[] arguments) {
            final long records = positiveLong(arguments[3], "records");
            if (records > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("records must be <= " + Integer.MAX_VALUE
                        + " because the completion latch is bounded");
            }
            final Configuration parsed = new Configuration(arguments[0], nonBlank(arguments[1], "topic"),
                    Path.of(arguments[2]), (int) records, positiveInt(arguments[4], "payload-bytes"), arguments[5],
                    arguments[6], arguments[7], arguments[8], arguments[9], arguments[10],
                    positiveInt(arguments[11], "partitions"), positiveInt(arguments[12], "batch-bytes"),
                    nonNegativeLong(arguments[13], "linger-ms"), nonNegativeLong(arguments[14], "rate-per-second"),
                    positiveInt(arguments[15], "max-in-flight"), Boolean.parseBoolean(arguments[16]));
            if (!parsed.arrival().equals("burst") && !parsed.arrival().equals("uniform")
                    && !parsed.arrival().equals("zipf")) {
                throw new IllegalArgumentException("unsupported arrival pattern: " + parsed.arrival());
            }
            if (!parsed.ordering().equals("ordered") && !parsed.ordering().equals("unordered")) {
                throw new IllegalArgumentException("unsupported ordering mode: " + parsed.ordering());
            }
            if (!parsed.consistency().equals("baseline") && !parsed.consistency().equals("strong")) {
                throw new IllegalArgumentException("unsupported consistency mode: " + parsed.consistency());
            }
            if (!parsed.targetHealth().equals("healthy") && !parsed.targetHealth().equals("bad")) {
                throw new IllegalArgumentException("unsupported target health: " + parsed.targetHealth());
            }
            if (!parsed.placement().equals("single-shard") && !parsed.placement().equals("multi-shard")) {
                throw new IllegalArgumentException("unsupported placement mode: " + parsed.placement());
            }
            if (!parsed.payloadMode().equals("inline") && !parsed.payloadMode().equals("object")) {
                throw new IllegalArgumentException("unsupported payload mode: " + parsed.payloadMode());
            }
            if (parsed.placement().equals("single-shard") != (parsed.partitions() == 1)) {
                throw new IllegalArgumentException("placement and partition count disagree");
            }
            return parsed;
        }

        private boolean strong() {
            return consistency.equals("strong");
        }

        private boolean ordered() {
            return ordering.equals("ordered");
        }

        private String profileName() {
            return arrival + "-" + ordering + "-" + consistency;
        }

        private static String nonBlank(final String value, final String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank");
            }
            return value;
        }

        private static int positiveInt(final String value, final String name) {
            final long parsed = positiveLong(value, name);
            return Math.toIntExact(parsed);
        }

        private static long positiveLong(final String value, final String name) {
            try {
                final long parsed = Long.parseLong(value);
                if (parsed <= 0) {
                    throw new IllegalArgumentException(name + " must be positive");
                }
                return parsed;
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(name + " is not numeric: " + value, failure);
            }
        }

        private static long nonNegativeLong(final String value, final String name) {
            try {
                final long parsed = Long.parseLong(value);
                if (parsed < 0) {
                    throw new IllegalArgumentException(name + " must not be negative");
                }
                return parsed;
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(name + " is not numeric: " + value, failure);
            }
        }
    }

    private record Observation(Configuration configuration, String clusterId, Uuid topicId, long accepted,
                               long rejected, long errors, long guardedEvidence, long badAccepted,
                               int brokerValueBytes, long elapsedNanos, long minOffset, long maxOffset,
                               AtomicLongArray partitions,
                               boolean pass, String failure) {
        private String json() {
            final long inputBytes = Math.multiplyExact(accepted, configuration.payloadBytes());
            final double elapsedSeconds = elapsedNanos / 1_000_000_000.0d;
            final long recordsPerSecond = Math.round(accepted / elapsedSeconds);
            final long bytesPerSecond = Math.round(inputBytes / elapsedSeconds);
            final StringBuilder json = new StringBuilder(4_096);
            json.append("{\n");
            field(json, "schema", "nereus-delay-v1-physical-kafka-capacity-observation-v1", true);
            field(json, "status", pass ? "PASS" : "FAIL", true);
            field(json, "broker", "kafka-k1", true);
            field(json, "cluster_id", clusterId, true);
            field(json, "topic", configuration.topic(), true);
            field(json, "topic_id", topicId.toString(), true);
            field(json, "arrival_pattern", configuration.arrival(), true);
            field(json, "ordering_mode", configuration.ordering(), true);
            field(json, "consistency_mode", configuration.consistency(), true);
            field(json, "target_health", configuration.targetHealth(), true);
            field(json, "placement_mode", configuration.placement(), true);
            field(json, "payload_mode", configuration.payloadMode(), true);
            numberField(json, "requested_records", configuration.records(), true);
            numberField(json, "accepted_records", accepted, true);
            numberField(json, "expected_rejected_records", expectedBadRecords(configuration), true);
            numberField(json, "rejected_records", rejected, true);
            numberField(json, "error_count", errors, true);
            numberField(json, "guarded_evidence_count", guardedEvidence, true);
            numberField(json, "bad_target_accepted_records", badAccepted, true);
            numberField(json, "payload_bytes", configuration.payloadBytes(), true);
            numberField(json, "broker_value_bytes", brokerValueBytes, true);
            numberField(json, "input_bytes", inputBytes, true);
            numberField(json, "elapsed_nanos", elapsedNanos, true);
            numberField(json, "records_per_second", recordsPerSecond, true);
            numberField(json, "bytes_per_second", bytesPerSecond, true);
            numberField(json, "min_offset", minOffset == Long.MAX_VALUE ? -1 : minOffset, true);
            numberField(json, "max_offset", maxOffset, true);
            json.append("  \"partition_counts\": [");
            for (int index = 0; index < partitions.length(); index++) {
                if (index > 0) {
                    json.append(",");
                }
                json.append(partitions.get(index));
            }
            json.append("],\n");
            json.append("  \"configuration\": {");
            field(json, "batch_bytes", configuration.batchBytes(), true, false);
            field(json, "linger_ms", configuration.lingerMs(), true, false);
            field(json, "fsync_authority", "broker-log-append-ack", true);
            field(json, "source_lock_delay", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_DELAY"), true, true);
            field(json, "source_lock_kafka", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_KAFKA"), true, true);
            field(json, "source_lock_pulsar", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_PULSAR"), true, true);
            field(json, "source_lock_oxia", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_OXIA"), false, true);
            json.append("  },\n");
            json.append("  \"invariants\": [\"topic ID and partition were pinned before send\","
                    + "\"every accepted record has K1 guarded response evidence\","
                    + "\"bad target never produced an accepted record\"],\n");
            field(json, "failure", failure == null ? "" : failure, false);
            json.append("}\n");
            return json.toString();
        }

        private static String sourceLock(final String name) {
            return System.getenv().getOrDefault(name, "unspecified");
        }
    }

    private static void field(final StringBuilder json, final String name, final Object value,
                              final boolean comma) {
        field(json, name, value, comma, true);
    }

    private static void field(final StringBuilder json, final String name, final Object value,
                              final boolean comma, final boolean quoted) {
        json.append("  \"").append(name).append("\": ");
        if (quoted) {
            json.append('"').append(escape(String.valueOf(value))).append('"');
        } else {
            json.append(value);
        }
        if (comma) {
            json.append(',');
        }
        json.append('\n');
    }

    private static void numberField(final StringBuilder json, final String name, final long value,
                                    final boolean comma) {
        field(json, name, value, comma, false);
    }

    private static String escape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
