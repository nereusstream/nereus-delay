package io.nereusstream.delay.transport;

import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.ConsumerResourceGuardException;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.GuardedConsumerRecords;
import org.apache.kafka.clients.consumer.GuardedFetchEvidence;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real Kafka retention-floor and guarded Fetch recovery smoke. */
public final class KafkaClientArtifactRetentionFloorSmoke {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final int PRODUCE_ROUNDS = 5;
    private static final int RECORDS_PER_ROUND = 4;

    private KafkaClientArtifactRetentionFloorSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <source-topic>");
        }
        final String phase = System.getenv().getOrDefault(
                "NEREUS_DELAY_KAFKA_RETENTION_FLOOR_PHASE", "full");
        if (!phase.equals("full") && !phase.equals("prepare") && !phase.equals("resume")) {
            throw new IllegalArgumentException("NEREUS_DELAY_KAFKA_RETENTION_FLOOR_PHASE must be full|prepare|resume");
        }
        final String bootstrap = arguments[0];
        final String topic = phase.equals("full") ? arguments[1] + "-" + UUID.randomUUID() : arguments[1];
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            if (phase.equals("resume")) {
                requireTopic(admin, topic);
            } else {
                ensureTopic(admin, topic);
            }
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final TopicPartition topicPartition = new TopicPartition(topic, 0);
            if (phase.equals("prepare")) {
                final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
                produceRounds(bootstrap, clusterId, topic, topicId, shard);
                final RetentionBounds bounds = waitForRetentionFloor(admin, topicPartition);
                preserveRetainedTail(admin, topic);
                produceTail(bootstrap, clusterId, topic, topicId, shard);
                final RetentionBounds retained = waitForRetainedTail(admin, topicPartition);
                requireRetainedTail(retained);
                rejectStaleOffset(bootstrap, clusterId, topic, topicId, topicPartition, retained.beginningOffset());
                writeStateDump("RETENTION_FLOOR_REJECTED", topic, topicId, shard,
                        bounds, retained, null, null, true);
                System.out.println("Kafka source retention-floor process-crash cut reached: "
                        + "staleOffsetRejected=true, retentionFloor=" + retained.beginningOffset());
                return;
            }
            if (phase.equals("resume")) {
                final RetentionBounds retained = waitForRetainedTail(admin, topicPartition);
                requireRetainedTail(retained);
                final FetchReceipt current = fetchAtFloor(bootstrap, clusterId, topic, topicId, topicPartition,
                        retained.beginningOffset());
                if (current.offset() < retained.beginningOffset()
                        || current.lastStableOffset() <= current.offset()) {
                    throw new IllegalStateException("Kafka retention-floor Fetch did not retain a covering LSO: "
                            + current);
                }
                writeStateDump("RECOVERED_AFTER_FRESH_PROCESS", topic, topicId,
                        new ShardId(routeFromEnvironment(), 0), null, retained, current.offset(),
                        current.lastStableOffset(), true);
                System.out.println("Kafka source retention-floor fresh-process recovery E2E passed: "
                        + "the current retention floor remained readable through guarded Fetch v13 with LSO.");
                return;
            }

            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            produceRounds(bootstrap, clusterId, topic, topicId, shard);
            final RetentionBounds bounds = waitForRetentionFloor(admin, topicPartition);
            preserveRetainedTail(admin, topic);
            produceTail(bootstrap, clusterId, topic, topicId, shard);
            final RetentionBounds retained = waitForRetainedTail(admin, topicPartition);
            requireRetainedTail(retained);
            rejectStaleOffset(bootstrap, clusterId, topic, topicId, topicPartition, retained.beginningOffset());
            final FetchReceipt current = fetchAtFloor(bootstrap, clusterId, topic, topicId, topicPartition,
                    retained.beginningOffset());
            if (current.offset() < retained.beginningOffset()
                    || current.lastStableOffset() <= current.offset()) {
                throw new IllegalStateException("Kafka retention-floor Fetch did not retain a covering LSO: "
                        + current);
            }
            System.out.println("Kafka source retention-floor smoke passed: oldOffset=0"
                    + ", retentionFloor=" + retained.beginningOffset()
                    + ", endOffset=" + retained.endOffset()
                    + ", staleOffsetRejected=true"
                    + ", floorFetchOffset=" + current.offset()
                    + ", fetchLso=" + current.lastStableOffset());
        }
    }

    private static void requireRetainedTail(final RetentionBounds retained) {
        if (retained.beginningOffset() <= 0 || retained.endOffset() <= retained.beginningOffset()) {
            throw new IllegalStateException("Kafka retention floor did not advance over the old source offset: "
                    + retained);
        }
    }

    private static void produceRounds(final String bootstrap, final String clusterId, final String topic,
                                      final Uuid topicId, final ShardId shard) throws Exception {
        final Map<String, Object> configuration = producerConfiguration(bootstrap);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration,
                new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final ProducerResourceGuard guard = new ProducerResourceGuard(clusterId, topic, topicId, 0);
            for (int round = 0; round < PRODUCE_ROUNDS; round++) {
                for (int record = 0; record < RECORDS_PER_ROUND; record++) {
                    final PreparedCommand command = command(shard, "retention-" + round + "-" + record);
                    guarded.sendGuarded(new ProducerRecord<>(topic, 0, null, CommandCodec.encodeFrameV1(command)),
                            guard).get(10, TimeUnit.SECONDS);
                }
                producer.flush();
                if (round + 1 < PRODUCE_ROUNDS) {
                    TimeUnit.MILLISECONDS.sleep(1_250);
                }
            }
        }
    }

    private static void produceTail(final String bootstrap, final String clusterId, final String topic,
                                    final Uuid topicId, final ShardId shard) throws Exception {
        final Map<String, Object> configuration = producerConfiguration(bootstrap);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration,
                new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final ProducerResourceGuard guard = new ProducerResourceGuard(clusterId, topic, topicId, 0);
            guarded.sendGuarded(new ProducerRecord<>(topic, 0, null,
                    CommandCodec.encodeFrameV1(command(shard, "retention-tail"))), guard)
                    .get(10, TimeUnit.SECONDS);
            producer.flush();
        }
    }

    private static Map<String, Object> producerConfiguration(final String bootstrap) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        configuration.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-retention-floor");
        return configuration;
    }

    private static RetentionBounds waitForRetentionFloor(final Admin admin, final TopicPartition topicPartition)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        RetentionBounds latest = null;
        while (System.nanoTime() < deadline) {
            latest = offsets(admin, topicPartition);
            if (latest.beginningOffset() > 0) {
                return latest;
            }
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
        throw new IllegalStateException("Kafka retention floor did not advance within the bounded window: " + latest);
    }

    private static RetentionBounds waitForRetainedTail(final Admin admin, final TopicPartition topicPartition)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        RetentionBounds latest = null;
        while (System.nanoTime() < deadline) {
            latest = offsets(admin, topicPartition);
            if (latest.endOffset() > latest.beginningOffset()) {
                return latest;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Kafka retention-floor tail did not remain readable: " + latest);
    }

    private static void preserveRetainedTail(final Admin admin, final String topic) throws Exception {
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                new ConfigEntry("retention.ms", "60000"), AlterConfigOp.OpType.SET))))
                .all().get(10, TimeUnit.SECONDS);
    }

    private static RetentionBounds offsets(final Admin admin, final TopicPartition topicPartition) throws Exception {
        final Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> offsets =
                admin.listOffsets(Map.of(topicPartition, OffsetSpec.earliest())).all()
                        .get(10, TimeUnit.SECONDS);
        final Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> latest =
                admin.listOffsets(Map.of(topicPartition, OffsetSpec.latest())).all()
                        .get(10, TimeUnit.SECONDS);
        return new RetentionBounds(offsets.get(topicPartition).offset(), latest.get(topicPartition).offset());
    }

    private static void rejectStaleOffset(final String bootstrap, final String clusterId, final String topic,
                                          final Uuid topicId, final TopicPartition topicPartition,
                                          final long retentionFloor) {
        final Map<String, Object> configuration = consumerConfiguration(bootstrap,
                "nereus-delay-retention-stale-" + UUID.randomUUID(), "none");
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration, clusterId, topic, toJavaUuid(topicId), topicPartition.partition());
        try {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, 0);
            for (int attempt = 0; attempt < 20; attempt++) {
                try {
                    final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
                    if (!records.isEmpty()) {
                        throw new IllegalStateException("Kafka stale offset unexpectedly returned records below floor "
                                + retentionFloor);
                    }
                } catch (ConsumerResourceGuardException expected) {
                    if (expected.getMessage() != null
                            && expected.getMessage().contains("OFFSET_OUT_OF_RANGE")) {
                        return;
                    }
                    throw expected;
                }
            }
        } finally {
            consumer.close();
        }
        throw new IllegalStateException("Kafka stale offset 0 was not rejected after retention floor "
                + retentionFloor + " advanced");
    }

    private static FetchReceipt fetchAtFloor(final String bootstrap, final String clusterId, final String topic,
                                             final Uuid topicId, final TopicPartition topicPartition,
                                             final long retentionFloor) {
        final Map<String, Object> configuration = consumerConfiguration(bootstrap,
                "nereus-delay-retention-floor-" + UUID.randomUUID(), "earliest");
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                configuration, clusterId, topic, toJavaUuid(topicId), topicPartition.partition());
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic, topicId,
                topicPartition.partition());
        try {
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, retentionFloor);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
                final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
                if (evidence == null) {
                    continue;
                }
                for (ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, guard);
                    if (record.offset() < retentionFloor) {
                        throw new IllegalStateException("Kafka retention-floor Fetch returned an old record");
                    }
                    return new FetchReceipt(record.offset(), evidence.lastStableOffset());
                }
            }
        } finally {
            consumer.close();
        }
        throw new IllegalStateException("Kafka current retention-floor record did not become readable");
    }

    private static Map<String, Object> consumerConfiguration(final String bootstrap, final String groupId,
                                                              final String autoOffsetReset) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return configuration;
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 60_000;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of(
                "cleanup.policy", "delete",
                "file.delete.delay.ms", "0",
                "message.timestamp.type", "LogAppendTime",
                "retention.ms", "5000",
                "segment.bytes", "1048576",
                "segment.ms", "1000"));
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
        throw new IllegalStateException("Kafka retention-floor topic metadata did not converge");
    }

    private static void requireTopic(final Admin admin, final String topic) throws Exception {
        if (describe(admin, topic) == null) {
            throw new IllegalStateException("Kafka retention-floor resume topic is missing: " + topic);
        }
    }

    private static RouteIncarnation routeFromEnvironment() {
        final String value = System.getenv("NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ROUTE_UUID");
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ROUTE_UUID is required for the phased retention drill");
        }
        return RouteIncarnation.fromUuid(UUID.fromString(value));
    }

    private static void writeStateDump(final String phase, final String topic, final Uuid topicId,
                                       final ShardId shard, final RetentionBounds before,
                                       final RetentionBounds after, final Long floorFetchOffset,
                                       final Long fetchLso, final boolean staleOffsetRejected) throws Exception {
        final String directoryValue = System.getenv("NEREUS_DELAY_KAFKA_RETENTION_FLOOR_STATE_DUMP_DIR");
        if (directoryValue == null || directoryValue.isBlank()) {
            return;
        }
        final Path directory = Path.of(directoryValue).toAbsolutePath().normalize();
        Files.createDirectories(directory);
        final String fileName = phase.equals("RETENTION_FLOOR_REJECTED")
                ? "before-process-crash.json" : "after-fresh-process.json";
        final String json = "{\n"
                + "  \"schema\": \"nereus-delay-chaos-durable-state-dump-v1\",\n"
                + "  \"cell\": \"kafka-retention-floor-process-crash\",\n"
                + "  \"phase\": " + jsonString(phase) + ",\n"
                + "  \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + "  \"topic\": " + jsonString(topic) + ",\n"
                + "  \"topic_id\": " + jsonString(topicId.toString()) + ",\n"
                + "  \"route_uuid\": " + jsonString(shard.routeIncarnation().uuid().toString()) + ",\n"
                + "  \"partition\": " + shard.partition() + ",\n"
                + "  \"old_offset\": 0,\n"
                + "  \"retention_floor\": " + jsonNullable(after == null ? null : after.beginningOffset()) + ",\n"
                + "  \"end_offset\": " + jsonNullable(after == null ? null : after.endOffset()) + ",\n"
                + "  \"before_floor_observation\": " + jsonNullable(before == null ? null : before.beginningOffset()) + ",\n"
                + "  \"floor_fetch_offset\": " + jsonNullable(floorFetchOffset) + ",\n"
                + "  \"fetch_lso\": " + jsonNullable(fetchLso) + ",\n"
                + "  \"stale_offset_rejected\": " + staleOffsetRejected + ",\n"
                + "  \"durable_broker_read\": true,\n"
                + "  \"dump_forced\": true\n"
                + "}\n";
        final Path target = directory.resolve(fileName);
        try (var channel = java.nio.channels.FileChannel.open(target, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
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
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static UUID toJavaUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private record RetentionBounds(long beginningOffset, long endOffset) {
    }

    private record FetchReceipt(long offset, long lastStableOffset) {
    }
}
