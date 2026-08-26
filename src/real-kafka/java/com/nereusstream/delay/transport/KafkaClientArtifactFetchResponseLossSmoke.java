package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Real Kafka read-committed Fetch response-loss and exact source replay smoke. */
public final class KafkaClientArtifactFetchResponseLossSmoke {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    private KafkaClientArtifactFetchResponseLossSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <source-topic>");
        }
        final String phase = System.getenv().getOrDefault("NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_PHASE", "full");
        if (!phase.equals("full") && !phase.equals("prepare") && !phase.equals("resume")) {
            throw new IllegalArgumentException(
                    "NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_PHASE must be full|prepare|resume");
        }
        final String bootstrap = arguments[0];
        final String topic = phase.equals("full") ? arguments[1] + "-" + UUID.randomUUID() : arguments[1];
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            if (phase.equals("resume")) {
                requireTopic(admin, topic);
            } else {
                ensureTopic(admin, topic);
            }
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final RouteIncarnation route = phase.equals("resume") ? routeFromEnvironment() : RouteIncarnation.random();
            final ShardId shard = new ShardId(route, 0);
            final PreparedCommand first = command(shard, "fetch-loss-one");
            final PreparedCommand second = command(shard, "fetch-loss-two");
            final String groupId = phase.equals("full")
                    ? "nereus-delay-fetch-loss-" + UUID.randomUUID()
                    : requiredEnvironment("NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_GROUP_ID");
            if (phase.equals("prepare")) {
                produce(bootstrap, clusterId, topic, topicId, first, second);
                final FetchReceipt discarded =
                        fetchAndDiscard(bootstrap, groupId, clusterId, topic, topicId, shard, first, second);
                writeStateDump(
                        "FETCH_RESPONSE_LOSS_PERSISTED",
                        topic,
                        groupId,
                        topicId,
                        shard,
                        discarded.firstOffset(),
                        discarded.lastStableOffset(),
                        null,
                        null);
                System.out.println("Kafka source Fetch response-loss process-crash cut reached: "
                        + "responseDiscardedAfterFetch=true, groupId=" + groupId);
                return;
            }
            if (phase.equals("resume")) {
                try (KafkaClientArtifactSourceRecordConsumer source =
                        source(bootstrap, groupId, clusterId, topic, topicId, shard)) {
                    final SourceRecordConsumer.PolledSourceRecord replayed =
                            pollAny(source, "replayed first source record");
                    final KafkaSourcePosition replayedPosition = position(replayed);
                    requireAcked(
                            replayed.acknowledgement().acknowledge(replayed.entry(), null),
                            "replayed first source record");
                    final SourceRecordConsumer.PolledSourceRecord secondObserved =
                            pollAny(source, "second source record after replay ACK");
                    final KafkaSourcePosition secondPosition = position(secondObserved);
                    requireAcked(
                            secondObserved.acknowledgement().acknowledge(secondObserved.entry(), null),
                            "second source record");
                    requireCommittedOffset(admin, groupId, topic, 0, secondPosition.offset() + 1);
                    final long endOffset = admin.listOffsets(Map.of(new TopicPartition(topic, 0), OffsetSpec.latest()))
                            .all()
                            .get(10, TimeUnit.SECONDS)
                            .get(new TopicPartition(topic, 0))
                            .offset();
                    if (endOffset <= secondPosition.offset()) {
                        throw new IllegalStateException("Kafka source end offset did not cover the ACKed record");
                    }
                    writeStateDump(
                            "RECOVERED_AFTER_FRESH_PROCESS",
                            topic,
                            groupId,
                            topicId,
                            shard,
                            replayedPosition.offset(),
                            null,
                            secondPosition.offset(),
                            secondPosition.offset() + 1);
                }
                System.out.println("Kafka source Fetch response-loss fresh-process recovery E2E passed: "
                        + "same group replayed the uncommitted Fetch response and committed "
                        + "the exact source position.");
                return;
            }

            produce(bootstrap, clusterId, topic, topicId, first, second);
            final FetchReceipt discarded =
                    fetchAndDiscard(bootstrap, groupId, clusterId, topic, topicId, shard, first, second);
            final SourceRecordConsumer.PolledSourceRecord replayed;
            final SourceRecordConsumer.PolledSourceRecord secondObserved;
            try (KafkaClientArtifactSourceRecordConsumer source =
                    source(bootstrap, groupId, clusterId, topic, topicId, shard)) {
                replayed = pollRequired(source, first, "replayed first source record");
                final KafkaSourcePosition replayedPosition = position(replayed);
                if (replayedPosition.offset() != discarded.firstOffset()) {
                    throw new IllegalStateException("Fetch response-loss replay changed the source offset: expected="
                            + discarded.firstOffset() + ", actual=" + replayedPosition.offset());
                }
                requireAcked(
                        replayed.acknowledgement().acknowledge(replayed.entry(), null), "replayed first source record");

                secondObserved = pollRequired(source, second, "second source record after replay ACK");
                requireAcked(
                        secondObserved.acknowledgement().acknowledge(secondObserved.entry(), null),
                        "second source record");
            }

            requireCommittedOffset(
                    admin, groupId, topic, 0, position(secondObserved).offset() + 1);
            final long endOffset = admin.listOffsets(Map.of(new TopicPartition(topic, 0), OffsetSpec.latest()))
                    .all()
                    .get(10, TimeUnit.SECONDS)
                    .get(new TopicPartition(topic, 0))
                    .offset();
            if (endOffset <= position(secondObserved).offset()) {
                throw new IllegalStateException("Kafka source end offset did not cover the ACKed record");
            }
            System.out.println("Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true"
                    + ", replayOffset=" + discarded.firstOffset()
                    + ", secondOffset=" + position(secondObserved).offset()
                    + ", fetchLso=" + discarded.lastStableOffset()
                    + ", committedAfterReplay=" + (position(secondObserved).offset() + 1));
        }
    }

    private static FetchReceipt fetchAndDiscard(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final ShardId shard,
            final PreparedCommand first,
            final PreparedCommand second) {
        final Map<String, Object> configuration = consumerConfiguration(bootstrap, groupId);
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration, clusterId, topic, toJavaUuid(topicId), shard.partition());
        final TopicPartition topicPartition = new TopicPartition(topic, shard.partition());
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic, topicId, shard.partition());
        try {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, 0);
            final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
            final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
            if (evidence == null
                    || evidence.requestVersion() < 13
                    || evidence.lastStableOffset() <= evidence.lastRecordOffset()) {
                throw new IllegalStateException(
                        "Kafka Fetch response-loss proof lacks an LSO covering the batch: " + evidence);
            }
            final List<ConsumerRecord<byte[], byte[]>> fetched = records.records(topicPartition);
            if (fetched.isEmpty()
                    || fetched.get(0).offset() != 0
                    || !java.util.Arrays.equals(fetched.get(0).value(), CommandCodec.encodeManagedFrame(first))) {
                throw new IllegalStateException(
                        "Kafka Fetch response-loss smoke did not receive the exact first record");
            }
            if (fetched.size() < 2
                    || !java.util.Arrays.equals(fetched.get(1).value(), CommandCodec.encodeManagedFrame(second))) {
                throw new IllegalStateException(
                        "Kafka Fetch response-loss smoke did not receive the exact second record");
            }
            for (ConsumerRecord<byte[], byte[]> record : fetched) {
                KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, guard);
            }
            return new FetchReceipt(fetched.get(0).offset(), evidence.lastStableOffset());
        } finally {
            consumer.close();
        }
    }

    private static KafkaClientArtifactSourceRecordConsumer source(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final ShardId shard) {
        return new KafkaClientArtifactSourceRecordConsumer(
                KafkaClientArtifactSourceConsumerFactory.create(
                        consumerConfiguration(bootstrap, groupId),
                        clusterId,
                        topic,
                        toJavaUuid(topicId),
                        shard.partition()),
                clusterId,
                toJavaUuid(topicId),
                shard,
                topic,
                Duration.ofMillis(250));
    }

    private static SourceRecordConsumer.PolledSourceRecord pollRequired(
            final KafkaClientArtifactSourceRecordConsumer source, final PreparedCommand expected, final String label) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> candidate = source.poll();
            if (candidate.isEmpty()) {
                continue;
            }
            if (!(candidate.get().entry() instanceof SourceReplayRecord record)
                    || !record.command().equals(expected)) {
                throw new IllegalStateException(label + " was not the exact expected command");
            }
            return candidate.get();
        }
        throw new IllegalStateException(label + " did not become visible within the bounded poll window");
    }

    private static SourceRecordConsumer.PolledSourceRecord pollAny(
            final KafkaClientArtifactSourceRecordConsumer source, final String label) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> candidate = source.poll();
            if (candidate.isPresent()) {
                return candidate.get();
            }
        }
        throw new IllegalStateException(label + " did not become visible within the bounded poll window");
    }

    private static KafkaSourcePosition position(final SourceRecordConsumer.PolledSourceRecord polled) {
        if (!(polled.entry() instanceof SourceReplayRecord record)
                || !(record.position() instanceof KafkaSourcePosition position)) {
            throw new IllegalStateException("Kafka source record did not retain a Kafka Source Position");
        }
        return position;
    }

    private static Map<String, Object> consumerConfiguration(final String bootstrap, final String groupId) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static void produce(
            final String bootstrap,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand first,
            final PreparedCommand second)
            throws Exception {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-fetch-response-loss");
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final ProducerResourceGuard guard = new ProducerResourceGuard(clusterId, topic, topicId, 0);
            guarded.sendGuarded(new ProducerRecord<>(topic, 0, null, CommandCodec.encodeManagedFrame(first)), guard)
                    .get(10, TimeUnit.SECONDS);
            guarded.sendGuarded(new ProducerRecord<>(topic, 0, null, CommandCodec.encodeManagedFrame(second)), guard)
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static void requireCommittedOffset(
            final Admin admin, final String groupId, final String topic, final int partition, final long expected)
            throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka Fetch response-loss group offset mismatch: expected=" + expected
                    + ", actual=" + (offset == null ? "missing" : offset.offset()));
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRef destination = new ProfileRef(
                Bytes.utf8("destination-" + identity),
                1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)),
                ProfileKind.DESTINATION);
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(
                Bytes.utf8("retry-" + identity), 1, Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                deliverAt,
                deliverAt + 10_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("source-" + identity),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        return PreparedCommand.schedule(shard, intent, deliverAt + 20_000);
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            if (describe(admin, topic) != null) {
                return;
            }
        } catch (Exception missing) {
            // Create below.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                if (describe(admin, topic) != null) {
                    return;
                }
            } catch (Exception ignored) {
                // Retry while metadata converges.
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Fetch response-loss source topic metadata did not converge");
    }

    private static void requireTopic(final Admin admin, final String topic) throws Exception {
        if (describe(admin, topic) == null) {
            throw new IllegalStateException("Fetch response-loss resume topic is missing: " + topic);
        }
    }

    private static RouteIncarnation routeFromEnvironment() {
        return RouteIncarnation.fromUuid(
                UUID.fromString(requiredEnvironment("NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ROUTE_UUID")));
    }

    private static String requiredEnvironment(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for the phased Fetch response-loss drill");
        }
        return value;
    }

    private static void writeStateDump(
            final String phase,
            final String topic,
            final String groupId,
            final Uuid topicId,
            final ShardId shard,
            final long replayOffset,
            final Long lastStableOffset,
            final Long secondOffset,
            final Long committedOffset)
            throws Exception {
        final String directoryValue = System.getenv("NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_STATE_DUMP_DIR");
        if (directoryValue == null || directoryValue.isBlank()) {
            return;
        }
        final Path directory = Path.of(directoryValue).toAbsolutePath().normalize();
        Files.createDirectories(directory);
        final String fileName = phase.equals("FETCH_RESPONSE_LOSS_PERSISTED")
                ? "before-process-crash.json"
                : "after-fresh-process.json";
        final String json = "{\n"
                + " \"schema\": \"nereus-delay-chaos-durable-state-dump\",\n"
                + " \"cell\": \"kafka-fetch-response-loss-process-crash\",\n"
                + " \"phase\": " + jsonString(phase) + ",\n"
                + " \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + " \"topic\": " + jsonString(topic) + ",\n"
                + " \"group_id\": " + jsonString(groupId) + ",\n"
                + " \"topic_id\": " + jsonString(topicId.toString()) + ",\n"
                + " \"route_uuid\": "
                + jsonString(shard.routeIncarnation().uuid().toString()) + ",\n"
                + " \"partition\": " + shard.partition() + ",\n"
                + " \"replay_offset\": " + replayOffset + ",\n"
                + " \"last_stable_offset\": " + jsonNullable(lastStableOffset) + ",\n"
                + " \"second_offset\": " + jsonNullable(secondOffset) + ",\n"
                + " \"committed_offset\": " + jsonNullable(committedOffset) + ",\n"
                + " \"response_discarded_after_fetch\": true,\n"
                + " \"durable_broker_read\": true,\n"
                + " \"dump_forced\": true\n"
                + "}\n";
        final Path target = directory.resolve(fileName);
        try (var channel = java.nio.channels.FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String jsonNullable(final Long value) {
        return value == null ? "null" : Long.toString(value);
    }

    private static org.apache.kafka.clients.admin.TopicDescription describe(final Admin admin, final String topic)
            throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result, final String label) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(label + " was not ACKED: " + result.disposition(), result.failure());
        }
    }

    private static UUID toJavaUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private record FetchReceipt(long firstOffset, long lastStableOffset) {}
}
