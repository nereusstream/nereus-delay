package com.nereusstream.delay.transport;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

/** Moves one replicated source partition to a requested live broker without stopping the other broker. */
public final class KafkaClientArtifactLeaderPlacementSmoke {
    private static final Duration DEADLINE = Duration.ofSeconds(120);

    private KafkaClientArtifactLeaderPlacementSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2 && arguments.length != 3) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <topic> [worker-group-id]");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1];
        final String workerGroup = arguments.length == 3 ? arguments[2] : null;
        final TopicPartition partition = new TopicPartition(topic, 0);
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000))) {
            if (workerGroup != null) {
                ensureConsumerGroup(bootstrap, topic, workerGroup);
            }
            final TopicDescription before = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .get(topic);
            if (before.partitions().size() != 1
                    || before.partitions().get(0).replicas().size() != 3) {
                throw new IllegalStateException("expected one three-replica partition for " + topic);
            }
            final int targetLeader = targetLeader();
            final List<Integer> targetReplicas = targetLeader == 1 ? List.of(1, 2, 3) : List.of(2, 3, 1);
            placeAndAwait(admin, partition, targetReplicas, targetLeader);
            final TopicDescription latest = describe(admin, topic);
            final var info = latest.partitions().get(0);
            final List<Integer> replicas =
                    info.replicas().stream().map(replica -> replica.id()).toList();
            if (info.leader().id() != targetLeader) {
                throw new IllegalStateException("source leader did not converge to Broker-" + targetLeader + ": "
                        + info.leader().id());
            }
            System.out.println("Kafka source leader placement passed: topic=" + topic
                    + ", leader=" + info.leader().id() + ", replicas=" + replicas
                    + ", broker1Alive=true");
            if (workerGroup != null) {
                final TopicDescription offsets = awaitTopic(admin, "__consumer_offsets");
                final int offsetsPartition =
                        Utils.abs(workerGroup.hashCode()) % offsets.partitions().size();
                final TopicPartition coordinatorPartition = new TopicPartition("__consumer_offsets", offsetsPartition);
                placeAndAwait(admin, coordinatorPartition, targetReplicas, targetLeader);
                final TopicDescription updatedOffsets = describe(admin, "__consumer_offsets");
                final var coordinatorInfo = updatedOffsets.partitions().get(offsetsPartition);
                final List<Integer> coordinatorReplicas = coordinatorInfo.replicas().stream()
                        .map(replica -> replica.id())
                        .toList();
                if (coordinatorInfo.leader().id() != targetLeader) {
                    throw new IllegalStateException("group coordinator leader did not converge to Broker-"
                            + targetLeader + ": " + coordinatorInfo.leader().id());
                }
                System.out.println("Kafka group coordinator placement passed: group=" + workerGroup
                        + ", offsetsPartition=" + offsetsPartition + ", leader="
                        + coordinatorInfo.leader().id()
                        + ", replicas=" + coordinatorReplicas + ", broker1Alive=true");
            }
        }
    }

    private static int targetLeader() {
        final String configured = System.getenv().getOrDefault("NEREUS_DELAY_KAFKA_LEADER_PLACEMENT_TARGET", "2");
        final int target;
        try {
            target = Integer.parseInt(configured);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("leader placement target must be Broker 1 or 2", failure);
        }
        if (target != 1 && target != 2) {
            throw new IllegalArgumentException("leader placement target must be Broker 1 or 2");
        }
        return target;
    }

    private static void ensureConsumerGroup(final String bootstrap, final String topic, final String groupId) {
        final Map<String, Object> configuration = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(configuration)) {
            consumer.subscribe(List.of(topic));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250));
                if (!consumer.assignment().isEmpty()) {
                    return;
                }
            }
            throw new IllegalStateException("worker group did not receive a source assignment: " + groupId);
        }
    }

    private static TopicDescription awaitTopic(final Admin admin, final String topic) throws Exception {
        Exception lastFailure = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try {
                return describe(admin, topic);
            } catch (Exception failure) {
                lastFailure = failure;
                TimeUnit.MILLISECONDS.sleep(250);
            }
        }
        throw new IllegalStateException("Kafka topic did not become describable: " + topic, lastFailure);
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    private static void placeAndAwait(
            final Admin admin,
            final TopicPartition partition,
            final List<Integer> targetReplicas,
            final int targetLeader)
            throws Exception {
        TopicDescription before = describe(admin, partition.topic());
        final var current = before.partitions().get(partition.partition());
        if (current.leader().id() != targetLeader
                || !current.replicas().stream()
                        .map(replica -> replica.id())
                        .toList()
                        .equals(targetReplicas)) {
            admin.alterPartitionReassignments(
                            Map.of(partition, Optional.of(new NewPartitionReassignment(targetReplicas))))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }
        tryPreferredLeader(admin, partition);
        final long deadline = System.nanoTime() + DEADLINE.toNanos();
        while (System.nanoTime() < deadline) {
            before = describe(admin, partition.topic());
            final var info = before.partitions().get(partition.partition());
            final List<Integer> replicas =
                    info.replicas().stream().map(replica -> replica.id()).toList();
            if (info.leader().id() == targetLeader && replicas.equals(targetReplicas)) {
                return;
            }
            if (replicas.equals(targetReplicas) && info.leader().id() != targetLeader) {
                tryPreferredLeader(admin, partition);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        final var info = describe(admin, partition.topic()).partitions().get(partition.partition());
        final List<Integer> replicas =
                info.replicas().stream().map(replica -> replica.id()).toList();
        throw new IllegalStateException("partition did not converge to Broker " + targetLeader + ": " + partition
                + " leader=" + info.leader().id() + " replicas=" + replicas);
    }

    private static void tryPreferredLeader(final Admin admin, final TopicPartition partition) {
        try {
            admin.electLeaders(ElectionType.PREFERRED, Set.of(partition)).all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // The preferred election can race replica reassignment; the bounded convergence loop retries it.
        }
    }
}
