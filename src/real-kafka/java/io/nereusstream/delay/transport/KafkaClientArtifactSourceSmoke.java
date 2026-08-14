package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.SourceAcknowledgement;
import io.nereusstream.delay.ownership.SourceRecordConsumer;
import io.nereusstream.delay.ownership.SourceReplayRecord;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real Kafka source poll/ACK/restart smoke for one V1 Shard Log partition. */
public final class KafkaClientArtifactSourceSmoke {
    private KafkaClientArtifactSourceSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <source-topic>");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1] + "-" + UUID.randomUUID();
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = describe(admin, topic).topicId();
            final RouteIncarnation route = RouteIncarnation.random();
            final ShardId shard = new ShardId(route, 0);
            final PreparedCommand first = command(shard, "source-one");
            final PreparedCommand second = command(shard, "source-two");
            produce(bootstrap, clusterId, topic, topicId, first, second);

            final String groupId = "nereus-delay-source-e2e-" + UUID.randomUUID();
            final PolledSource firstUnacknowledged =
                    pollFirst(bootstrap, groupId, clusterId, topic, toUuid(topicId), shard, first, true);
            final KafkaSourcePosition firstPosition = position(firstUnacknowledged.record());
            requirePosition(firstPosition, clusterId, toUuid(topicId), shard);
            final long firstOffset = firstPosition.offset();
            firstUnacknowledged.close();

            final PolledSource replayed =
                    pollFirst(bootstrap, groupId, clusterId, topic, toUuid(topicId), shard, first, true);
            final KafkaSourcePosition replayedPosition = position(replayed.record());
            if (!replayedPosition.equals(firstPosition)
                    || !((SourceReplayRecord) replayed.record().entry()).command().equals(first)) {
                replayed.close();
                throw new IllegalStateException("unacknowledged Kafka source record did not replay exactly");
            }
            requireAcked(replayed.record().acknowledgement().acknowledge(replayed.record().entry(), null),
                    "first source record");
            requireCommittedOffset(admin, groupId, topic, 0, firstOffset + 1);

            final PolledSource secondObserved =
                    pollNext(bootstrap, groupId, clusterId, topic, toUuid(topicId), shard, second);
            final KafkaSourcePosition secondPosition = position(secondObserved.record());
            requirePosition(secondPosition, clusterId, toUuid(topicId), shard);
            final long secondOffset = secondPosition.offset();
            if (secondOffset == firstOffset) {
                secondObserved.close();
                throw new IllegalStateException("Kafka source cursor did not move after ACK");
            }
            requireAcked(secondObserved.record().acknowledgement().acknowledge(secondObserved.record().entry(), null),
                    "second source record");
            requireCommittedOffset(admin, groupId, topic, 0, secondOffset + 1);
            secondObserved.close();

            final PolledSource afterRestart =
                    pollFirst(bootstrap, groupId, clusterId, topic, toUuid(topicId), shard, null, false);
            if (afterRestart != null) {
                afterRestart.close();
                throw new IllegalStateException("Kafka source replay returned a record after both ACKs");
            }
            System.out.println("Kafka source ACK smoke passed: topicId=" + topicId
                    + ", firstOffset=" + firstOffset + ", secondOffset=" + secondOffset
                    + ", committedAfterRestart=empty");
        }
    }

    private static void produce(final String bootstrap, final String clusterId, final String topic,
                                final Uuid topicId, final PreparedCommand first,
                                final PreparedCommand second) throws Exception {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-source-smoke");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration,
                new ByteArraySerializer(), new ByteArraySerializer())) {
            final GuardedProducer<byte[], byte[]> guarded = (GuardedProducer<byte[], byte[]>) producer;
            final ProducerResourceGuard guard = new ProducerResourceGuard(clusterId, topic,
                    topicId, 0);
            guarded.sendGuarded(new ProducerRecord<>(topic, 0, null, CommandCodec.encodeFrameV1(first)), guard)
                    .get(10, TimeUnit.SECONDS);
            guarded.sendGuarded(new ProducerRecord<>(topic, 0, null, CommandCodec.encodeFrameV1(second)), guard)
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static PolledSource pollFirst(final String bootstrap, final String groupId,
                                          final String clusterId, final String topic,
                                          final UUID topicId, final ShardId shard,
                                          final PreparedCommand expected,
                                          final boolean requireRecord) {
        final KafkaClientArtifactSourceRecordConsumer source = source(bootstrap, groupId, clusterId, topic, topicId,
                shard);
        try {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                final java.util.Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
                if (polled.isPresent()) {
                    if (expected != null
                            && !((SourceReplayRecord) polled.get().entry()).command().equals(expected)) {
                        throw new IllegalStateException("Kafka source returned an unexpected command");
                    }
                    return new PolledSource(source, polled.get());
                }
            }
        } catch (RuntimeException | Error failure) {
            source.close();
            throw failure;
        }
        source.close();
        if (requireRecord) {
            throw new IllegalStateException("Kafka source record did not become visible");
        }
        return null;
    }

    private static PolledSource pollNext(final String bootstrap, final String groupId,
                                         final String clusterId, final String topic,
                                         final UUID topicId, final ShardId shard,
                                         final PreparedCommand expected) {
        return pollFirst(bootstrap, groupId, clusterId, topic, topicId, shard, expected, true);
    }

    private static KafkaClientArtifactSourceRecordConsumer source(final String bootstrap, final String groupId,
                                                                   final String clusterId, final String topic,
                                                                   final UUID topicId, final ShardId shard) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaClientArtifactSourceRecordConsumer(new KafkaConsumer<>(configuration), clusterId, topicId,
                shard, topic, Duration.ofMillis(250));
    }

    private static void requireCommittedOffset(final Admin admin, final String groupId, final String topic,
                                               final int partition, final long expected) throws Exception {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS).get(topicPartition);
        if (offset == null || offset.offset() != expected) {
            throw new IllegalStateException("Kafka source group offset mismatch: expected=" + expected
                    + ", actual=" + (offset == null ? "missing" : offset.offset()));
        }
    }

    private static SourceReplayRecord sourceRecord(final SourceRecordConsumer.PolledSourceRecord polled) {
        return (SourceReplayRecord) polled.entry();
    }

    private static KafkaSourcePosition position(final SourceRecordConsumer.PolledSourceRecord polled) {
        return (KafkaSourcePosition) sourceRecord(polled).position();
    }

    private static void requirePosition(final KafkaSourcePosition position, final String clusterId,
                                        final UUID topicId, final ShardId shard) {
        if (!position.shardId().equals(shard)
                || !position.authenticatedClusterId().equals(clusterId)
                || !position.nativeTopicUuid().equals(topicId)
                || position.offset() < 0
                || position.brokerLogAppendTimeEpochMs() < 0) {
            throw new IllegalStateException("Kafka source position did not retain exact broker identity");
        }
    }

    private static void requireAcked(final SourceAcknowledgement.AcknowledgementResult result,
                                     final String label) {
        if (result.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(label + " was not ACKED: " + result.disposition(), result.failure());
        }
    }

    private record PolledSource(KafkaClientArtifactSourceRecordConsumer consumer,
                                SourceRecordConsumer.PolledSourceRecord record) implements AutoCloseable {
        @Override
        public void close() {
            consumer.close();
        }
    }

    private static PreparedCommand command(final ShardId shard, final String identity) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + identity), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + identity)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + identity), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + identity)));
        final long deliverAt = System.currentTimeMillis() + 1_000;
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination, retryPolicy, deliverAt,
                deliverAt + 10_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                Bytes.utf8("source-" + identity), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        return PreparedCommand.scheduleV1(shard, intent, deliverAt + 20_000);
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
        throw new IllegalStateException("source topic metadata did not converge");
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static UUID toUuid(final Uuid value) {
        return new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

}
