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
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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

/** Real Kafka process-crash cut and same-group source cursor recovery smoke. */
public final class KafkaClientArtifactProcessCrashRecoverySmoke {
    private static final int CRASH_EXIT_CODE = 86;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final long FIXED_DELIVER_AT = 1_900_000_000_000L;

    private KafkaClientArtifactProcessCrashRecoverySmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <source-topic> <crash|resume>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1];
        final String mode = arguments[2];
        if (!mode.equals("crash") && !mode.equals("resume")) {
            throw new IllegalArgumentException("unknown process-crash mode: " + mode);
        }
        final String groupId = groupId(topic);
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final ShardId shard = restartShard(topic);
            final PreparedCommand first = command(shard, "process-crash-first");
            final PreparedCommand second = command(shard, "process-crash-second");
            if (mode.equals("crash")) {
                produce(bootstrap, clusterId, topic, topicId, first, second);
                crashAfterFetch(bootstrap, groupId, clusterId, topic, topicId, first, second);
                throw new IllegalStateException("process-crash cut returned without halting the JVM");
            }

            final SourceRecordConsumer.PolledSourceRecord replayedFirst;
            final SourceRecordConsumer.PolledSourceRecord replayedSecond;
            try (KafkaClientArtifactSourceRecordConsumer source =
                    source(bootstrap, groupId, clusterId, topic, topicId, shard)) {
                replayedFirst = pollRequired(source, first, "replayed first source record");
                requireOffset(replayedFirst, 0, "replayed first source record");
                requireAcked(
                        replayedFirst.acknowledgement().acknowledge(replayedFirst.entry(), null),
                        "replayed first source record");

                replayedSecond = pollRequired(source, second, "replayed second source record");
                requireOffset(replayedSecond, 1, "replayed second source record");
                requireAcked(
                        replayedSecond.acknowledgement().acknowledge(replayedSecond.entry(), null),
                        "replayed second source record");
            }
            requireCommittedOffset(admin, groupId, topic, 2);
            System.out.println("Kafka source process-crash recovery smoke passed: crashExit=" + CRASH_EXIT_CODE
                    + ", replayOffset=" + position(replayedFirst).offset()
                    + ", secondOffset=" + position(replayedSecond).offset()
                    + ", committedAfterRecovery=2");
        }
    }

    private static void crashAfterFetch(
            final String bootstrap,
            final String groupId,
            final String clusterId,
            final String topic,
            final Uuid topicId,
            final PreparedCommand first,
            final PreparedCommand second) {
        final GuardedConsumer<byte[], byte[]> consumer = KafkaClientArtifactSourceConsumerFactory.create(
                consumerConfiguration(bootstrap, groupId), clusterId, topic, toJavaUuid(topicId), 0);
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        final ConsumerResourceGuard guard = new ConsumerResourceGuard(clusterId, topic, topicId, 0);
        consumer.assign(List.of(topicPartition));
        consumer.seek(topicPartition, 0);
        final GuardedConsumerRecords<byte[], byte[]> records = consumer.pollGuarded(POLL_TIMEOUT);
        final GuardedFetchEvidence evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, guard);
        final List<ConsumerRecord<byte[], byte[]>> fetched = records.records(topicPartition);
        if (evidence == null
                || evidence.requestVersion() < 13
                || evidence.lastStableOffset() <= evidence.lastRecordOffset()) {
            throw new IllegalStateException(
                    "Kafka process-crash Fetch lacks an LSO covering the fetched records: " + evidence);
        }
        if (fetched.size() < 2
                || fetched.get(0).offset() != 0
                || fetched.get(1).offset() != 1
                || !Arrays.equals(fetched.get(0).value(), CommandCodec.encodeManagedFrame(first))
                || !Arrays.equals(fetched.get(1).value(), CommandCodec.encodeManagedFrame(second))) {
            throw new IllegalStateException("Kafka process-crash Fetch did not receive the exact source records");
        }
        for (ConsumerRecord<byte[], byte[]> record : fetched) {
            KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, guard);
        }
        System.out.println("Kafka source process-crash cut reached: fetchedOffsets=0,1, fetchLso="
                + evidence.lastStableOffset() + ", responseAcked=false, consumerClosed=false");
        System.out.flush();
        Runtime.getRuntime().halt(CRASH_EXIT_CODE);
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
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
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
        throw new IllegalStateException(label + " did not become visible within the bounded recovery window");
    }

    private static void requireOffset(
            final SourceRecordConsumer.PolledSourceRecord record, final long expected, final String label) {
        if (position(record).offset() != expected) {
            throw new IllegalStateException(label + " offset mismatch: expected=" + expected + ", actual="
                    + position(record).offset());
        }
    }

    private static KafkaSourcePosition position(final SourceRecordConsumer.PolledSourceRecord record) {
        if (!(record.entry() instanceof SourceReplayRecord replay)
                || !(replay.position() instanceof KafkaSourcePosition position)) {
            throw new IllegalStateException("Kafka process-crash replay did not retain a Kafka Source Position");
        }
        return position;
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result, final String label) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(label + " was not ACKED: " + result.disposition(), result.failure());
        }
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
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-process-crash");
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
            final Admin admin, final String groupId, final String topic, final long expected) throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        final OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS)
                .get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka process-crash group offset mismatch: expected=" + expected
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
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                retryPolicy,
                FIXED_DELIVER_AT,
                FIXED_DELIVER_AT + 10_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("source-" + identity),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        return PreparedCommand.schedule(
                shard, uuidV7(identity + "-message"), uuidV7(identity + "-command"), intent, FIXED_DELIVER_AT + 20_000);
    }

    private static UUID uuidV7(final String identity) {
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-process-crash-uuid/" + identity));
        final long randomA = ((digest[0] & 0xffL) << 4) | ((digest[1] >>> 4) & 0x0fL);
        final long most = (FIXED_DELIVER_AT << 16) | 0x7000L | randomA;
        final long randomB = ByteBuffer.wrap(digest, 8, Long.BYTES).getLong();
        final long least = (randomB & 0x3fff_ffff_ffff_ffffL) | 0x8000_0000_0000_0000L;
        return new UUID(most, least);
    }

    private static String groupId(final String topic) {
        return "nereus-delay-process-crash-" + topic;
    }

    private static ShardId restartShard(final String topic) {
        return new ShardId(
                new RouteIncarnation(Arrays.copyOf(
                        Bytes.sha256(Bytes.utf8("nereus-delay-kafka-process-crash/" + topic)),
                        RouteIncarnation.LENGTH)),
                0);
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
        throw new IllegalStateException("Kafka process-crash source topic metadata did not converge");
    }

    private static org.apache.kafka.clients.admin.TopicDescription describe(final Admin admin, final String topic)
            throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    private static UUID toJavaUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }
}
