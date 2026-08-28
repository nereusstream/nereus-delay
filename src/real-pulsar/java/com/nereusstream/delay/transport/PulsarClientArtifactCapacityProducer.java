package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.Bytes;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Source-locked physical Pulsar capacity producer for the P1 guarded path. */
public final class PulsarClientArtifactCapacityProducer {
    private static final int ARGUMENT_COUNT = 18;
    private static final long MAX_SLEEP_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long PRODUCER_EPOCH_RECORDS = 500_000L;

    private PulsarClientArtifactCapacityProducer() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != ARGUMENT_COUNT) {
            throw new IllegalArgumentException("usage: <service-url> <admin-urls> <topic-base> <artifact> <records> "
                    + "<payload-bytes> <arrival> <ordering> <consistency> <target-health> <placement> "
                    + "<payload-mode> <partitions> <batch-messages> <batch-bytes> <linger-ms> "
                    + "<rate-per-second> <max-in-flight>");
        }
        final Configuration configuration = Configuration.parse(arguments);
        final byte[] incarnation = Bytes.sha256(Bytes.utf8("nereus-delay-capacity:" + configuration.topicBase()));
        final long creationTimestamp = System.currentTimeMillis();
        final List<String> adminUrls = Arrays.asList(configuration.adminUrls().split(","));
        final HttpClient adminClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        final String topic = "persistent://public/default/" + configuration.topicBase();
        PulsarClientArtifactLargePayloadGatewaySmoke.createPartitionedTopic(
                adminClient,
                adminUrls.get(0),
                configuration.topicBase(),
                configuration.partitions(),
                incarnation,
                creationTimestamp,
                adminUrls);
        Observation observation;
        try {
            observation = produce(configuration, topic, incarnation, creationTimestamp);
            writeObservation(configuration.artifact(), observation);
        } finally {
            PulsarClientArtifactLargePayloadGatewaySmoke.deletePartitionedTopic(
                    adminClient, adminUrls, configuration.topicBase());
        }
        if (!observation.pass()) {
            throw new IllegalStateException("Pulsar capacity observation failed: " + observation.failure());
        }
        System.out.println("Pulsar guarded capacity observation passed: " + configuration.artifact());
    }

    private static Observation produce(
            final Configuration configuration,
            final String topic,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
        final TopicResourceGuard goodGuard = new TopicResourceGuard(
                PulsarClientArtifactClientBuilder.clusterId(), incarnation, creationTimestamp);
        final TopicResourceGuard badGuard = new TopicResourceGuard(
                PulsarClientArtifactClientBuilder.clusterId(),
                Bytes.sha256(Bytes.utf8("wrong-resource:" + configuration.topicBase())),
                creationTimestamp);
        final AtomicLong accepted = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        final AtomicLong guardedEvidence = new AtomicLong();
        final AtomicLong badAccepted = new AtomicLong();
        final AtomicLong minEntry = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong maxEntry = new AtomicLong(-1L);
        final AtomicLongArray partitionCounts = new AtomicLongArray(configuration.partitions());
        final AtomicReference<String> firstFailure = new AtomicReference<>();
        final long expectedBad = expectedBadRecords(configuration);
        final long startNanos = System.nanoTime();
        final byte[] payload = payload(configuration.payloadBytes(), configuration.payloadMode(), topic);
        boolean pass = true;

        try (PulsarClient client =
                PulsarClientArtifactClientBuilder.builder(configuration.serviceUrl()).build()) {
            long recordStart = 0L;
            long epochIndex = 0L;
            while (recordStart < configuration.records()) {
                final int epochRecords = (int) Math.min(PRODUCER_EPOCH_RECORDS, configuration.records() - recordStart);
                final Observation epoch = produceEpoch(
                        configuration, client, topic, goodGuard, badGuard, recordStart, epochRecords, epochIndex);
                accepted.addAndGet(epoch.accepted());
                rejected.addAndGet(epoch.rejected());
                errors.addAndGet(epoch.errors());
                guardedEvidence.addAndGet(epoch.guardedEvidence());
                badAccepted.addAndGet(epoch.badAccepted());
                updateMinimum(minEntry, epoch.minEntry());
                updateMaximum(maxEntry, epoch.maxEntry());
                for (int partition = 0; partition < partitionCounts.length(); partition++) {
                    partitionCounts.addAndGet(partition, epoch.partitions().get(partition));
                }
                if (!epoch.pass()) {
                    pass = false;
                    firstFailure.compareAndSet(null, epoch.failure());
                }
                recordStart += epochRecords;
                epochIndex++;
            }
        }
        final long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);
        final boolean countsMatch =
                accepted.get() == configuration.records() - expectedBad && rejected.get() == expectedBad;
        pass = pass
                && countsMatch
                && errors.get() == 0
                && badAccepted.get() == 0
                && guardedEvidence.get() == accepted.get()
                && maxEntry.get() >= 0;
        return new Observation(
                configuration,
                topic,
                accepted.get(),
                rejected.get(),
                errors.get(),
                guardedEvidence.get(),
                badAccepted.get(),
                payload.length,
                elapsedNanos,
                minEntry.get(),
                maxEntry.get(),
                partitionCounts,
                pass,
                firstFailure.get());
    }

    private static Observation produceEpoch(
            final Configuration configuration,
            final PulsarClient client,
            final String topic,
            final TopicResourceGuard goodGuard,
            final TopicResourceGuard badGuard,
            final long recordStart,
            final int epochRecords,
            final long epochIndex)
            throws Exception {
        final AtomicLong accepted = new AtomicLong();
        final AtomicLong rejected = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        final AtomicLong guardedEvidence = new AtomicLong();
        final AtomicLong badAccepted = new AtomicLong();
        final AtomicLong minEntry = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong maxEntry = new AtomicLong(-1L);
        final AtomicLongArray partitionCounts = new AtomicLongArray(configuration.partitions());
        final AtomicReference<String> firstFailure = new AtomicReference<>();
        final Semaphore permits = new Semaphore(configuration.maxInFlight());
        final CountDownLatch completions = new CountDownLatch(epochRecords);
        final long expectedBad = expectedBadRecords(configuration, recordStart, epochRecords);
        final long startNanos = System.nanoTime();
        final byte[] payload = payload(configuration.payloadBytes(), configuration.payloadMode(), topic);
        final List<Producer<byte[]>> producers = new ArrayList<>(configuration.partitions());
        final List<Producer<byte[]>> badProducers = new ArrayList<>(configuration.partitions());
        String badCreationFailure = "";
        try {
            for (int partition = 0; partition < configuration.partitions(); partition++) {
                producers.add(createProducer(
                        client,
                        physicalTopic(topic, partition),
                        goodGuard,
                        configuration,
                        false,
                        partition,
                        epochIndex));
            }
            if (configuration.targetHealth().equals("bad")) {
                for (int partition = 0; partition < configuration.partitions(); partition++) {
                    try {
                        badProducers.add(createProducer(
                                client,
                                physicalTopic(topic, partition),
                                badGuard,
                                configuration,
                                true,
                                partition,
                                epochIndex));
                    } catch (Exception failure) {
                        badProducers.add(null);
                        if (badCreationFailure.isEmpty()) {
                            badCreationFailure = failure.getClass().getName() + ": " + failure.getMessage();
                        }
                    }
                }
            }
            for (long relativeRecord = 0; relativeRecord < epochRecords; relativeRecord++) {
                final long record = recordStart + relativeRecord;
                waitForRate(startNanos, relativeRecord, configuration.ratePerSecond());
                final boolean bad = isBadRecord(configuration, record);
                final int requestedPartition = choosePartition(configuration, record);
                final Producer<byte[]> selected = bad
                        ? (requestedPartition < badProducers.size() ? badProducers.get(requestedPartition) : null)
                        : producers.get(requestedPartition);
                if (bad && selected == null) {
                    rejected.incrementAndGet();
                    completions.countDown();
                    continue;
                }
                permits.acquire();
                try {
                    final CompletableFuture<org.apache.pulsar.client.api.MessageId> send = selected.newMessage()
                            .property("nereus.capacity.partition", Integer.toString(requestedPartition))
                            .keyBytes(key(record))
                            .value(payload)
                            .sendAsync();
                    send.whenComplete((messageId, failure) -> {
                        try {
                            if (failure != null) {
                                if (bad) {
                                    rejected.incrementAndGet();
                                } else {
                                    recordFailure(errors, firstFailure, failure);
                                }
                            } else if (bad && valid(messageId, badGuard, configuration.partitions())) {
                                badAccepted.incrementAndGet();
                            } else if (!bad && valid(messageId, goodGuard, configuration.partitions())) {
                                final GuardedMessageId guarded = (GuardedMessageId) messageId;
                                accepted.incrementAndGet();
                                guardedEvidence.incrementAndGet();
                                partitionCounts.incrementAndGet(guarded.partition());
                                final MessageIdAdv advanced = (MessageIdAdv) messageId;
                                updateMinimum(minEntry, advanced.getEntryId());
                                updateMaximum(maxEntry, advanced.getEntryId());
                            } else {
                                recordFailure(
                                        errors,
                                        firstFailure,
                                        new IllegalStateException("guarded Pulsar response evidence mismatch"));
                            }
                        } finally {
                            completions.countDown();
                            permits.release();
                        }
                    });
                } catch (RuntimeException failure) {
                    if (bad) {
                        rejected.incrementAndGet();
                    } else {
                        recordFailure(errors, firstFailure, failure);
                    }
                    completions.countDown();
                    permits.release();
                }
            }
            final long timeoutSeconds = Math.max(120L, Math.min(86_400L, epochRecords / 1_000L + 120L));
            if (!completions.await(timeoutSeconds, TimeUnit.SECONDS)) {
                recordFailure(errors, firstFailure, new IllegalStateException("guarded Pulsar completions timed out"));
            }
            final long elapsedNanos = Math.max(1L, System.nanoTime() - startNanos);
            final boolean countsMatch = accepted.get() == epochRecords - expectedBad && rejected.get() == expectedBad;
            final boolean pass = countsMatch
                    && errors.get() == 0
                    && badAccepted.get() == 0
                    && guardedEvidence.get() == accepted.get()
                    && maxEntry.get() >= 0;
            return new Observation(
                    configuration,
                    topic,
                    accepted.get(),
                    rejected.get(),
                    errors.get(),
                    guardedEvidence.get(),
                    badAccepted.get(),
                    payload.length,
                    elapsedNanos,
                    minEntry.get(),
                    maxEntry.get(),
                    partitionCounts,
                    pass,
                    firstFailure.get() == null ? badCreationFailure : firstFailure.get());
        } finally {
            closeProducers(badProducers);
            closeProducers(producers);
        }
    }

    private static Producer<byte[]> createProducer(
            final PulsarClient client,
            final String topic,
            final TopicResourceGuard guard,
            final Configuration configuration,
            final boolean bad,
            final int partition,
            final long epochIndex)
            throws Exception {
        final ProducerBuilder<byte[]> builder = client.newProducer(Schema.BYTES)
                .topic(topic)
                .resourceGuard(guard)
                .producerName("nereus-delay-capacity-" + (bad ? "bad" : "good") + "-" + configuration.topicBase()
                        + "-epoch-" + epochIndex + "-partition-" + partition)
                .enableBatching(configuration.batchMessages() > 0)
                // P1 guarded ACK evidence is per message; the client rejects a
                // multi-message batch before it can construct GuardedMessageId.
                .batchingMaxMessages(1)
                .batchingMaxBytes(configuration.batchBytes())
                .maxPendingMessages(configuration.maxInFlight())
                .blockIfQueueFull(true)
                .autoUpdatePartitions(false)
                .enableChunking(false);
        if (configuration.lingerMs() > 0) {
            builder.batchingMaxPublishDelay(configuration.lingerMs(), TimeUnit.MILLISECONDS);
        }
        return builder.create();
    }

    private static String physicalTopic(final String topic, final int partition) {
        return topic + "-partition-" + partition;
    }

    private static void closeProducers(final List<Producer<byte[]>> producers) throws Exception {
        for (Producer<byte[]> producer : producers) {
            if (producer != null) {
                producer.close();
            }
        }
    }

    private static boolean valid(
            final org.apache.pulsar.client.api.MessageId messageId,
            final TopicResourceGuard guard,
            final int partitions) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !guard.equals(guarded.resourceGuard())
                || guarded.partition() < 0
                || !(messageId instanceof MessageIdAdv)
                || guarded.partition() >= partitions
                || guarded.physicalTopic() == null
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null) {
            return false;
        }
        return guarded.responseEvidence()
                .attestation()
                .equals(new org.apache.pulsar.client.api.TopicResourceGuardAttestation(
                        guard, guarded.physicalTopic(), guarded.partition()));
    }

    private static long expectedBadRecords(final Configuration configuration) {
        return configuration.targetHealth().equals("bad") ? ((configuration.records() - 1L) / 100L) + 1L : 0L;
    }

    private static long expectedBadRecords(
            final Configuration configuration, final long recordStart, final long recordCount) {
        if (!configuration.targetHealth().equals("bad")) {
            return 0L;
        }
        final long firstBad = recordStart % 100L == 0L ? recordStart : recordStart + (100L - recordStart % 100L);
        if (firstBad >= recordStart + recordCount) {
            return 0L;
        }
        return ((recordStart + recordCount - 1L - firstBad) / 100L) + 1L;
    }

    private static boolean isBadRecord(final Configuration configuration, final long record) {
        return configuration.targetHealth().equals("bad") && record % 100L == 0L;
    }

    private static int choosePartition(final Configuration configuration, final long record) {
        if (configuration.partitions() == 1 || !configuration.arrival().equals("zipf")) {
            return (int) (record % configuration.partitions());
        }
        final double sample = ((record * 1103515245L + 12345L) & 0x7fffffffL) / (double) Integer.MAX_VALUE;
        double normalizer = 0.0d;
        for (int partition = 1; partition <= configuration.partitions(); partition++) {
            normalizer += 1.0d / partition;
        }
        double cumulative = 0.0d;
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

    private static void recordFailure(
            final AtomicLong errors, final AtomicReference<String> firstFailure, final Throwable failure) {
        errors.incrementAndGet();
        firstFailure.compareAndSet(null, failure.getClass().getName() + ": " + failure.getMessage());
    }

    private static void writeObservation(final Path artifact, final Observation observation) throws IOException {
        final Path absolute = artifact.toAbsolutePath();
        if (absolute.getParent() != null) {
            Files.createDirectories(absolute.getParent());
        }
        Files.writeString(absolute, observation.json());
    }

    private record Configuration(
            String serviceUrl,
            String adminUrls,
            String topicBase,
            Path artifact,
            long records,
            int payloadBytes,
            String arrival,
            String ordering,
            String consistency,
            String targetHealth,
            String placement,
            String payloadMode,
            int partitions,
            int batchMessages,
            int batchBytes,
            long lingerMs,
            long ratePerSecond,
            int maxInFlight) {
        private static Configuration parse(final String[] arguments) {
            final long records = positiveLong(arguments[4], "records");
            final Configuration parsed = new Configuration(
                    nonBlank(arguments[0], "service-url"),
                    nonBlank(arguments[1], "admin-urls"),
                    nonBlank(arguments[2], "topic-base"),
                    Path.of(arguments[3]),
                    records,
                    positiveInt(arguments[5], "payload-bytes"),
                    arguments[6],
                    arguments[7],
                    arguments[8],
                    arguments[9],
                    arguments[10],
                    arguments[11],
                    positiveInt(arguments[12], "partitions"),
                    nonNegativeInt(arguments[13], "batch-messages"),
                    positiveInt(arguments[14], "batch-bytes"),
                    nonNegativeLong(arguments[15], "linger-ms"),
                    nonNegativeLong(arguments[16], "rate-per-second"),
                    positiveInt(arguments[17], "max-in-flight"));
            if (!parsed.arrival().equals("burst")
                    && !parsed.arrival().equals("uniform")
                    && !parsed.arrival().equals("zipf")) {
                throw new IllegalArgumentException("unsupported arrival pattern: " + parsed.arrival());
            }
            if (!parsed.ordering().equals("ordered") && !parsed.ordering().equals("unordered")) {
                throw new IllegalArgumentException("unsupported ordering mode: " + parsed.ordering());
            }
            if (!parsed.consistency().equals("baseline")
                    && !parsed.consistency().equals("strong")) {
                throw new IllegalArgumentException("unsupported consistency mode: " + parsed.consistency());
            }
            if (!parsed.targetHealth().equals("healthy")
                    && !parsed.targetHealth().equals("bad")) {
                throw new IllegalArgumentException("unsupported target health: " + parsed.targetHealth());
            }
            if (!parsed.placement().equals("single-shard")
                    && !parsed.placement().equals("multi-shard")) {
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

        private static String nonBlank(final String value, final String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank");
            }
            return value;
        }

        private static int positiveInt(final String value, final String name) {
            return Math.toIntExact(positiveLong(value, name));
        }

        private static int nonNegativeInt(final String value, final String name) {
            return Math.toIntExact(nonNegativeLong(value, name));
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

    private record Observation(
            Configuration configuration,
            String topic,
            long accepted,
            long rejected,
            long errors,
            long guardedEvidence,
            long badAccepted,
            int brokerValueBytes,
            long elapsedNanos,
            long minEntry,
            long maxEntry,
            AtomicLongArray partitions,
            boolean pass,
            String failure) {
        private String json() {
            final long inputBytes = Math.multiplyExact(accepted, configuration.payloadBytes());
            final double elapsedSeconds = elapsedNanos / 1_000_000_000.0d;
            final long recordsPerSecond = Math.round(accepted / elapsedSeconds);
            final long bytesPerSecond = Math.round(inputBytes / elapsedSeconds);
            final StringBuilder json = new StringBuilder(4_096);
            json.append("{\n");
            field(json, "schema", "nereus-delay-physical-pulsar-capacity-observation", true);
            field(json, "status", pass ? "PASS" : "FAIL", true);
            field(json, "broker", "pulsar-p1", true);
            field(json, "topic", topic, true);
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
            numberField(json, "min_entry_id", minEntry == Long.MAX_VALUE ? -1 : minEntry, true);
            numberField(json, "max_entry_id", maxEntry, true);
            json.append(" \"partition_counts\": [");
            for (int index = 0; index < partitions.length(); index++) {
                if (index > 0) {
                    json.append(",");
                }
                json.append(partitions.get(index));
            }
            json.append("],\n");
            json.append(" \"configuration\": {");
            field(json, "batch_messages", configuration.batchMessages(), true, false);
            numberField(json, "effective_guarded_batch_messages", 1, true);
            field(json, "batch_bytes", configuration.batchBytes(), true, false);
            field(json, "linger_ms", configuration.lingerMs(), true, false);
            field(json, "guarded_batching_mode", "singleton-only", true);
            field(json, "fsync_authority", "bookkeeper-ledger-ack", true);
            field(json, "source_lock_delay", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_DELAY"), true, true);
            field(json, "source_lock_kafka", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_KAFKA"), true, true);
            field(json, "source_lock_pulsar", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_PULSAR"), true, true);
            field(json, "source_lock_oxia", sourceLock("NEREUS_DELAY_CAPACITY_SOURCE_LOCK_OXIA"), false, true);
            json.append(" },\n");
            json.append(" \"invariants\": [\"resource guard was stamped before producer creation\","
                    + "\"every accepted record has P1 guarded SEND evidence\","
                    + "\"bad target never produced an accepted record\"],\n");
            field(json, "failure", failure == null ? "" : failure, false);
            json.append("}\n");
            return json.toString();
        }

        private static String sourceLock(final String name) {
            return System.getenv().getOrDefault(name, "unspecified");
        }
    }

    private static void field(final StringBuilder json, final String name, final Object value, final boolean comma) {
        field(json, name, value, comma, true);
    }

    private static void field(
            final StringBuilder json,
            final String name,
            final Object value,
            final boolean comma,
            final boolean quoted) {
        json.append(" \"").append(name).append("\": ");
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

    private static void numberField(
            final StringBuilder json, final String name, final long value, final boolean comma) {
        field(json, name, value, comma, false);
    }

    private static String escape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
