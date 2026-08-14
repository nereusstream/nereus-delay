package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.KafkaProduceRequest;
import io.nereusstream.delay.adapter.KafkaProduceResult;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Small real-broker smoke for the source-locked K1-to-Delay binding. */
public final class KafkaClientArtifactSmoke {
    private KafkaClientArtifactSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length < 2 || arguments.length > 3
                || arguments.length == 3 && !"preserve".equals(arguments[2])) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <topic> [preserve]");
        }
        final String bootstrap = arguments[0];
        final String topic = arguments[1];
        final boolean preserveTopic = arguments.length == 3;
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, topic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final TopicDescription initialDescription = describe(admin, topic);
            requireThreeReplicas(initialDescription, topic);
            final Uuid originalTopicId = initialDescription.topicId();
            final Map<String, Object> producerConfiguration = producerConfiguration(bootstrap);
            if (preserveTopic) {
                try (KafkaProducer<byte[], byte[]> producer = newProducer(producerConfiguration)) {
                    final KafkaClientArtifactProduceTransport transport = transport(producer);
                    requirePersisted(send(transport, clusterId, topic, originalTopicId, 0, 1),
                            "original topic incarnation");
                    final KafkaProduceResult replacement = send(transport, clusterId, topic, originalTopicId, 0, 4);
                    requirePersisted(replacement, "failover topic incarnation");
                }
                printSuccess(admin, clusterId, topic, originalTopicId, originalTopicId, true);
                return;
            }

            try (KafkaProducer<byte[], byte[]> producer = newProducer(producerConfiguration)) {
                final KafkaClientArtifactProduceTransport transport = transport(producer);
                requirePersisted(send(transport, clusterId, topic, originalTopicId, 0, 1),
                        "original topic incarnation");
            }
            admin.deleteTopics(Collections.singleton(topic)).all().get(10, TimeUnit.SECONDS);
            waitForTopicGone(admin, topic);
            ensureTopic(admin, topic);
            final Uuid replacementTopicId = describe(admin, topic).topicId();
            if (originalTopicId.equals(replacementTopicId)) {
                throw new IllegalStateException("delete/recreate did not produce a new TopicId");
            }
            try (KafkaProducer<byte[], byte[]> producer = newProducer(producerConfiguration)) {
                final KafkaClientArtifactProduceTransport transport = transport(producer);
                final KafkaProduceResult stale = send(transport, clusterId, topic, originalTopicId, 0, 2);
                if (stale.disposition() == KafkaProduceResult.Disposition.PERSISTED) {
                    throw new IllegalStateException("stale TopicId was accepted as persisted");
                }
                requirePersisted(send(transport, clusterId, topic, replacementTopicId, 0, 3),
                        "replacement topic incarnation");
            }
            printSuccess(admin, clusterId, topic, originalTopicId, replacementTopicId, false);
        }
    }

    private static Map<String, Object> producerConfiguration(final String bootstrap) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-k1-smoke");
        configuration.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        configuration.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        return configuration;
    }

    private static KafkaProducer<byte[], byte[]> newProducer(final Map<String, Object> configuration) {
        return new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static KafkaClientArtifactProduceTransport transport(final KafkaProducer<byte[], byte[]> producer) {
        return new KafkaClientArtifactProduceTransport((GuardedProducer<byte[], byte[]>) producer);
    }

    private static void printSuccess(final Admin admin, final String clusterId, final String topic,
                                     final Uuid originalTopicId, final Uuid replacementTopicId,
                                     final boolean preserveTopic) throws Exception {
        final TopicPartitionInfo partitionInfo = describe(admin, topic).partitions().get(0);
        System.out.println("K1 Delay binding smoke passed: cluster=" + clusterId
                + ", originalTopicId=" + originalTopicId + ", replacementTopicId=" + replacementTopicId
                + ", leader=" + partitionInfo.leader().id() + ", replicas=" + partitionInfo.replicas().size()
                + ", preserveTopic=" + preserveTopic);
    }

    private static KafkaProduceResult send(final KafkaClientArtifactProduceTransport transport,
                                            final String clusterId, final String topic, final Uuid topicId,
                                            final int partition, final int sequence) {
        final java.util.UUID nativeTopicId = new java.util.UUID(topicId.getMostSignificantBits(),
                topicId.getLeastSignificantBits());
        final ShardId shard = new ShardId(RouteIncarnation.random(), partition);
        final KafkaProduceRequest request = new KafkaProduceRequest(clusterId, topic, nativeTopicId, partition,
                CommandId.random(shard), Bytes.utf8("nereus-delay-k1-smoke-" + sequence));
        return transport.produce(request).toCompletableFuture().join();
    }

    private static void requirePersisted(final KafkaProduceResult result, final String label) {
        if (result.disposition() != KafkaProduceResult.Disposition.PERSISTED) {
            throw new IllegalStateException(label + " was not persisted: " + result.disposition()
                    + "/" + result.stableCode());
        }
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            if (describe(admin, topic) != null) {
                return;
            }
        } catch (Exception missing) {
            // The Admin API reports an unknown topic through the future.  Keep
            // creation in this small helper so the smoke remains idempotent.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(Collections.singleton(newTopic))
                .all().get(10, TimeUnit.SECONDS);
        waitForTopic(admin, topic);
    }

    private static void requireThreeReplicas(final TopicDescription description, final String topic) {
        if (description.partitions().size() != 1 || description.partitions().get(0).replicas().size() != 3) {
            throw new IllegalStateException("expected one three-replica partition for " + topic);
        }
    }

    private static void waitForTopic(final Admin admin, final String topic) throws Exception {
        Exception lastFailure = null;
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                describe(admin, topic);
                return;
            } catch (Exception failure) {
                lastFailure = failure;
                TimeUnit.MILLISECONDS.sleep(250);
            }
        }
        throw new IllegalStateException("topic metadata did not converge: " + topic, lastFailure);
    }

    private static void waitForTopicGone(final Admin admin, final String topic) throws Exception {
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                describe(admin, topic);
                TimeUnit.MILLISECONDS.sleep(250);
            } catch (Exception gone) {
                return;
            }
        }
        throw new IllegalStateException("topic deletion did not converge: " + topic);
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        final TopicDescription description = admin.describeTopics(Collections.singleton(topic))
                .allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
        if (description == null) {
            throw new IllegalStateException("topic was not returned by describe: " + topic);
        }
        return description;
    }
}
