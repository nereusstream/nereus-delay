package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Real guarded append/replay/ACK smoke for one signed System Mutation. */
public final class KafkaClientArtifactMutationSmoke {
    private KafkaClientArtifactMutationSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <mutation-topic>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
            final SystemMutation mutation = timeFence(shard);
            final KafkaSourcePosition appendedPosition;
            try (KafkaProducer<byte[], byte[]> producer = producer(bootstrap);
                    KafkaClientArtifactShardLogMutationAppender appender =
                            new KafkaClientArtifactShardLogMutationAppender(
                                    (GuardedProducer<byte[], byte[]>) producer,
                                    shard,
                                    clusterId,
                                    topic,
                                    toUuid(topicId),
                                    Duration.ofSeconds(15))) {
                final var outcome = appender.append(mutation);
                if (outcome.disposition()
                        != com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                    throw new IllegalStateException(
                            "Kafka mutation append was not persisted: " + outcome.disposition());
                }
                appendedPosition = (KafkaSourcePosition) outcome.sourcePosition();
            }

            final SourceAssignment assignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("kafka-mutation-assignment")),
                    1,
                    new KafkaActivationBarrier(shard, clusterId, toUuid(topicId), 0));
            final String recoveryGroup = "nereus-delay-mutation-recovery-" + UUID.randomUUID();
            try (KafkaClientArtifactRecoverySourceCursor recovery = new KafkaClientArtifactRecoverySourceCursor(
                    recoveryConsumer(bootstrap, recoveryGroup, clusterId, topic, toUuid(topicId), shard),
                    assignment,
                    topic,
                    0,
                    Duration.ofMillis(250))) {
                final SourceReplayEntry recovered = recovery.next();
                if (!(recovered instanceof SourceReplayMutation replayed)) {
                    throw new IllegalStateException("Kafka recovery returned a non-mutation entry: "
                            + recovered.getClass().getName());
                }
                if (!mutation.equals(replayed.mutation())) {
                    throw new IllegalStateException("Kafka recovery mutation bytes changed");
                }
                requireReplayPosition(appendedPosition, replayed.position(), "recovery");
                if (recovery.hasNext()) {
                    throw new IllegalStateException("Kafka mutation recovery exposed an unexpected second entry");
                }
            }

            final String group = "nereus-delay-mutation-source-" + UUID.randomUUID();
            try (KafkaClientArtifactSourceRecordConsumer source =
                    source(bootstrap, group, clusterId, topic, toUuid(topicId), shard)) {
                final SourceRecordConsumer.PolledSourceRecord polled = poll(source);
                if (!(polled.entry() instanceof SourceReplayMutation replayed)
                        || !mutation.equals(replayed.mutation())) {
                    throw new IllegalStateException("Kafka active source did not expose the exact System Mutation");
                }
                requireReplayPosition(appendedPosition, replayed.position(), "active source");
                final SourceAcknowledgement.AcknowledgementResult ack =
                        polled.acknowledgement().acknowledge(polled.entry(), null);
                if (ack.disposition() != SourceAcknowledgement.Disposition.ACKED) {
                    throw new IllegalStateException(
                            "Kafka mutation ACK was not durable: " + ack.disposition(), ack.failure());
                }
            }
            System.out.println("Kafka Shard Log mutation append/replay/ACK smoke passed: topicId=" + topicId
                    + ", offset=" + appendedPosition.offset()
                    + ", record=TIME_FENCE, guarded Producer, ordered mutation replay, commitSync ACK");
        }
    }

    private static void requireReplayPosition(
            final KafkaSourcePosition appended, final SourcePosition replayedPosition, final String phase) {
        if (!(replayedPosition instanceof KafkaSourcePosition replayed)
                || !appended.shardId().equals(replayed.shardId())
                || !appended.sameSourceIdentity(replayed)
                || appended.offset() != replayed.offset()
                || appended.brokerLogAppendTimeEpochMs() != replayed.brokerLogAppendTimeEpochMs()
                || (appended.leaderEpoch() != null && !appended.leaderEpoch().equals(replayed.leaderEpoch()))) {
            throw new IllegalStateException(
                    "Kafka " + phase + " position changed: appended=" + appended + ", replayed=" + replayedPosition);
        }
    }

    private static SystemMutation timeFence(final ShardId shard) throws Exception {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                2_000,
                2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("kafka-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("kafka-mutation-evidence")),
                0,
                null);
        final int keyVersion = 1;
        final long closeThrough = 1_000;
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof\0"),
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(keyVersion),
                Bytes.lp32(evidence.canonicalBytes()));
        final byte[] body = com.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    output, 1, new ShardSubject(shard).canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(
                    output, 2, SystemMutationType.TIME_FENCE.wireValue());
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 3, 9_000);
            com.nereusstream.delay.protocol.CanonicalProtobuf.int64(output, 10, closeThrough);
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32Bits(output, 11, keyVersion);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 12, proofId);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 13, evidence.canonicalBytes());
        });
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        return SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                9_000,
                proofId,
                body,
                AuthorIdentity.fence(Bytes.utf8("kafka-mutation-fence"), keyVersion)
                        .canonicalBytes(),
                keyVersion,
                keyPair.getPrivate());
    }

    private static KafkaProducer<byte[], byte[]> producer(final String bootstrap) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-mutation-smoke");
        return new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static SourceRecordConsumer.PolledSourceRecord poll(final KafkaClientArtifactSourceRecordConsumer source) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                return polled.orElseThrow();
            }
        }
        throw new IllegalStateException("Kafka mutation source record did not become visible");
    }

    private static KafkaClientArtifactSourceRecordConsumer source(
            final String bootstrap,
            final String group,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaClientArtifactSourceRecordConsumer(
                KafkaClientArtifactSourceConsumerFactory.create(
                        configuration, clusterId, topic, topicId, shard.partition()),
                clusterId,
                topicId,
                shard,
                topic,
                Duration.ofMillis(250));
    }

    private static GuardedConsumer<byte[], byte[]> recoveryConsumer(
            final String bootstrap,
            final String group,
            final String clusterId,
            final String topic,
            final UUID topicId,
            final ShardId shard) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return KafkaClientArtifactSourceConsumerFactory.create(
                configuration, clusterId, topic, topicId, shard.partition());
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
        throw new IllegalStateException("mutation topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }
}
