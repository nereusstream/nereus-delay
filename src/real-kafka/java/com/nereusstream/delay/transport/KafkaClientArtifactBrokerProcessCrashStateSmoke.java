package com.nereusstream.delay.transport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

/** Captures real Kafka metadata around a Broker-1 process or network recovery boundary. */
public final class KafkaClientArtifactBrokerProcessCrashStateSmoke {
    private static final int CRASHED_BROKER_ID = 1;
    private static final List<Integer> EXPECTED_REPLICAS = List.of(1, 2, 3);

    private KafkaClientArtifactBrokerProcessCrashStateSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <topic>");
        }
        final String cell =
                valueOrDefault("NEREUS_DELAY_KAFKA_BROKER_RECOVERY_STATE_CELL", "kafka-broker-process-crash");
        final Boundary boundary = boundary(cell);
        final String phase = valueOrDefault(
                "NEREUS_DELAY_KAFKA_BROKER_RECOVERY_STATE_PHASE",
                valueOrDefault("NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_PHASE", ""));
        if (!phase.equals("before") && !phase.equals("after")) {
            throw new IllegalArgumentException("Kafka Broker recovery state phase must be before|after");
        }
        final String directoryValue = environmentOrFallback(
                "NEREUS_DELAY_KAFKA_BROKER_RECOVERY_STATE_DUMP_DIR",
                "NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_STATE_DUMP_DIR");
        final String bootstrap = arguments[0];
        final String topic = arguments[1];
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                10_000))) {
            final TopicDescription description =
                    phase.equals("before") ? ensureTopic(admin, topic) : requireTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid topicId = description.topicId();
            final TopicPartition partition = new TopicPartition(topic, 0);
            final PartitionState state = phase.equals("before")
                    ? awaitBeforeRecovery(admin, topic, partition)
                    : readAfterRecovery(admin, topic, partition);
            if (phase.equals("before") && state.endOffset() < 1) {
                throw new IllegalStateException(
                        "Broker recovery state was captured before the guarded record: " + state.endOffset());
            }
            if (cell.equals("kafka-broker-leader-failover") && phase.equals("before") && state.leaderId() != 1) {
                throw new IllegalStateException("source leader was not Broker-1 before placement: " + state.leaderId());
            }
            if (cell.equals("kafka-broker-leader-failover") && phase.equals("after") && state.leaderId() != 2) {
                throw new IllegalStateException("source leader did not move to Broker-2: " + state.leaderId());
            }
            if (phase.equals("after") && !state.liveBrokerIds().contains(CRASHED_BROKER_ID)) {
                throw new IllegalStateException(
                        "Broker-1 was not visible in the live cluster after recovery: " + state.liveBrokerIds());
            }
            writeStateDump(directoryValue, boundary, phase, topic, clusterId, topicId, state);
            System.out.println("Kafka Broker " + cell + " durable state dump passed: phase=" + phase
                    + ", topic=" + topic + ", leader=" + state.leaderId()
                    + ", liveBrokerIds=" + state.liveBrokerIds()
                    + ", isrBrokerIds=" + state.isrBrokerIds()
                    + ", endOffset=" + state.endOffset());
        }
    }

    private static PartitionState awaitBeforeRecovery(
            final Admin admin, final String topic, final TopicPartition partition) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        TopicDescription latest = null;
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                latest = describe(admin, topic);
                final PartitionState state = state(admin, latest, partition);
                if (state.liveBrokerIds().containsAll(EXPECTED_REPLICAS)
                        && state.isrBrokerIds().containsAll(EXPECTED_REPLICAS)
                        && state.leaderId() > 0) {
                    return state;
                }
            } catch (Exception failure) {
                lastFailure = failure;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException(
                "Kafka Broker-1 was not a live ISR member before recovery boundary: "
                        + (latest == null ? "no topic metadata" : state(admin, latest, partition)),
                lastFailure);
    }

    private static PartitionState readAfterRecovery(
            final Admin admin, final String topic, final TopicPartition partition) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                final TopicDescription description = describe(admin, topic);
                final PartitionState state = state(admin, description, partition);
                if (state.liveBrokerIds().contains(CRASHED_BROKER_ID)
                        && state.isrBrokerIds().contains(CRASHED_BROKER_ID)) {
                    return state;
                }
            } catch (Exception failure) {
                lastFailure = failure;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Kafka Broker-1 did not return to the live ISR: " + topic, lastFailure);
    }

    private static PartitionState state(
            final Admin admin, final TopicDescription description, final TopicPartition partition) throws Exception {
        if (description.partitions().size() != 1) {
            throw new IllegalStateException("expected one partition for " + description.name() + ", actual="
                    + description.partitions().size());
        }
        final var info = description.partitions().get(partition.partition());
        final List<Integer> replicas =
                info.replicas().stream().map(node -> node.id()).sorted().toList();
        final List<Integer> isr =
                info.isr().stream().map(node -> node.id()).sorted().toList();
        if (!replicas.equals(EXPECTED_REPLICAS)) {
            throw new IllegalStateException("expected replicas " + EXPECTED_REPLICAS + ", actual=" + replicas);
        }
        final List<Integer> live = new TreeSet<>(admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).stream()
                        .map(node -> node.id())
                        .toList())
                .stream().toList();
        final long endOffset = admin.listOffsets(Map.of(partition, org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all()
                .get(10, TimeUnit.SECONDS)
                .get(partition)
                .offset();
        return new PartitionState(info.leader().id(), replicas, isr, live, endOffset);
    }

    private static TopicDescription ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            return describe(admin, topic);
        } catch (Exception missing) {
            final NewTopic newTopic = new NewTopic(topic, Map.of(0, EXPECTED_REPLICAS));
            newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
            return describe(admin, topic);
        }
    }

    private static TopicDescription requireTopic(final Admin admin, final String topic) throws Exception {
        return describe(admin, topic);
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(10, TimeUnit.SECONDS)
                .get(topic);
    }

    private static void writeStateDump(
            final String directoryValue,
            final Boundary boundary,
            final String phase,
            final String topic,
            final String clusterId,
            final Uuid topicId,
            final PartitionState state)
            throws Exception {
        final Path directory = Path.of(directoryValue).toAbsolutePath().normalize();
        Files.createDirectories(directory);
        final String fileName = phase.equals("before") ? "before-process-crash.json" : "after-fresh-process.json";
        final String json = "{\n"
                + "  \"schema\": \"nereus-delay-chaos-durable-state-dump-v1\",\n"
                + "  \"cell\": " + jsonString(boundary.cell()) + ",\n"
                + "  \"phase\": " + jsonString(phase.equals("before") ? boundary.beforePhase() : boundary.afterPhase())
                + ",\n"
                + "  \"process_pid\": " + ProcessHandle.current().pid() + ",\n"
                + "  \"topic\": " + jsonString(topic) + ",\n"
                + "  \"cluster_id\": " + jsonString(clusterId) + ",\n"
                + "  \"topic_id\": " + jsonString(topicId.toString()) + ",\n"
                + "  \"partition\": 0,\n"
                + "  \"leader_id\": " + state.leaderId() + ",\n"
                + "  \"replica_ids\": " + jsonArray(state.replicaIds()) + ",\n"
                + "  \"isr_ids\": " + jsonArray(state.isrBrokerIds()) + ",\n"
                + "  \"live_broker_ids\": " + jsonArray(state.liveBrokerIds()) + ",\n"
                + "  \"end_offset\": " + state.endOffset() + ",\n"
                + "  \"broker_1_rejoined\": "
                + (boundary.cell().equals("kafka-broker-process-crash")
                        && phase.equals("after")
                        && state.liveBrokerIds().contains(CRASHED_BROKER_ID)
                        && state.isrBrokerIds().contains(CRASHED_BROKER_ID))
                + ",\n"
                + "  \"broker_1_recovery_observed\": "
                + (phase.equals("after")
                        && state.liveBrokerIds().contains(CRASHED_BROKER_ID)
                        && state.isrBrokerIds().contains(CRASHED_BROKER_ID))
                + ",\n"
                + "  \"leader_moved_without_broker_loss\": "
                + (boundary.cell().equals("kafka-broker-leader-failover")
                        && phase.equals("after")
                        && state.leaderId() == 2
                        && state.liveBrokerIds().contains(CRASHED_BROKER_ID)
                        && state.isrBrokerIds().contains(CRASHED_BROKER_ID))
                + ",\n"
                + "  \"durable_broker_read\": true,\n"
                + "  \"dump_forced\": true\n"
                + "}\n";
        final Path target = directory.resolve(fileName);
        try (var channel = java.nio.channels.FileChannel.open(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)));
            channel.force(true);
        }
    }

    private static String requiredEnvironment(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for the phased Broker recovery drill");
        }
        return value;
    }

    private static String valueOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String environmentOrFallback(final String preferredName, final String fallbackName) {
        final String preferred = System.getenv(preferredName);
        return preferred == null || preferred.isBlank() ? requiredEnvironment(fallbackName) : preferred;
    }

    private static Boundary boundary(final String cell) {
        return switch (cell) {
            case "kafka-broker-process-crash" ->
                new Boundary(cell, "BROKER_PROCESS_CRASH_READY", "RECOVERED_AFTER_BROKER_REJOIN");
            case "kafka-broker-tcp-cut" -> new Boundary(cell, "BROKER_TCP_CUT_READY", "RECOVERED_AFTER_BROKER_TCP_CUT");
            case "kafka-broker-network-partition" ->
                new Boundary(cell, "BROKER_NETWORK_PARTITION_READY", "RECOVERED_AFTER_BROKER_NETWORK_REJOIN");
            case "kafka-broker-leader-failover" ->
                new Boundary(cell, "BROKER_LEADER_FAILOVER_READY", "RECOVERED_AFTER_BROKER_LEADER_FAILOVER");
            case "kafka-half-open" -> new Boundary(cell, "HALF_OPEN_READY", "RECOVERED_AFTER_HALF_OPEN");
            default -> throw new IllegalArgumentException("unsupported Kafka Broker recovery cell: " + cell);
        };
    }

    private static String jsonString(final String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String jsonArray(final List<Integer> values) {
        return values.toString();
    }

    private record PartitionState(
            int leaderId,
            List<Integer> replicaIds,
            List<Integer> isrBrokerIds,
            List<Integer> liveBrokerIds,
            long endOffset) {}

    private record Boundary(String cell, String beforePhase, String afterPhase) {}
}
