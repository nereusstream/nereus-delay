package io.nereusstream.delay.transport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Real survivor-side topic-leader recovery after a Broker network partition. */
public final class KafkaClientArtifactSurvivorLeaderRecoverySmoke {
    private static final Duration RETRY_DELAY = Duration.ofSeconds(2);
    private static final long DEADLINE_NANOS = Duration.ofSeconds(120).toNanos();

    private KafkaClientArtifactSurvivorLeaderRecoverySmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <comma-separated-topics>");
        }
        final String bootstrap = arguments[0];
        final List<String> topics = Arrays.stream(arguments[1].split(",", -1))
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .toList();
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("at least one survivor topic is required");
        }
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        final long deadline = System.nanoTime() + DEADLINE_NANOS;
        Exception lastFailure = null;
        try (Admin admin = Admin.create(adminConfiguration)) {
            while (System.nanoTime() < deadline) {
                final Map<String, Integer> leaders = new LinkedHashMap<>();
                boolean converged = true;
                try {
                    for (String topic : topics) {
                        final TopicDescription description = admin.describeTopics(List.of(topic))
                                .allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
                        final List<TopicPartitionInfo> partitions = description.partitions();
                        if (partitions.size() != 1) {
                            throw new IllegalStateException("expected one partition for " + topic
                                    + ", actual=" + partitions.size());
                        }
                        final int leader = partitions.get(0).leader().id();
                        leaders.put(topic, leader);
                        if (leader == 1 || leader < 0) {
                            converged = false;
                        }
                    }
                } catch (Exception failure) {
                    lastFailure = failure;
                    converged = false;
                }
                if (converged) {
                    System.out.println("Kafka survivor topic leader recovery passed: leaders=" + leaders);
                    return;
                }
                TimeUnit.NANOSECONDS.sleep(RETRY_DELAY.toNanos());
            }
        }
        throw new IllegalStateException("Kafka survivor topic leaders did not converge away from kafka-1"
                + (lastFailure == null ? "" : ": " + lastFailure), lastFailure);
    }
}
