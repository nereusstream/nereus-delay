package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPublishRequest;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.KafkaReceiptJournal;
import io.nereusstream.delay.adapter.KafkaReceiptResource;
import io.nereusstream.delay.adapter.KafkaTargetResource;
import io.nereusstream.delay.adapter.KafkaTransactionalDestinationAdapter;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real three-broker K2 smoke for the target-plus-keyed-receipt transaction. */
public final class KafkaClientArtifactTransactionalSmoke {
    private KafkaClientArtifactTransactionalSmoke() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <bootstrap-server> <target-topic> <receipt-topic>");
        }
        final String bootstrap = arguments[0];
        final String targetTopic = arguments[1];
        final String receiptTopic = arguments[2];
        final Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (Admin admin = Admin.create(adminConfiguration)) {
            ensureTopic(admin, targetTopic);
            ensureTopic(admin, receiptTopic);
            final String clusterId = admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            final Uuid initialTargetId = describe(admin, targetTopic).topicId();
            final Uuid receiptTopicId = describe(admin, receiptTopic).topicId();
            final RouteIncarnation route = RouteIncarnation.random();
            final ShardId shard = new ShardId(route, 0);
            final KafkaTargetResource initialTarget = new KafkaTargetResource(clusterId,
                    toUuid(initialTargetId), 0);
            final KafkaReceiptResource receipt = new KafkaReceiptResource(clusterId, toUuid(receiptTopicId),
                    route, 0, 0, 1, 1, 0);
            final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("k2-e2e-lane"));
            final byte[] laneIncarnation = bytes(16, 3);
            final byte[] transactionIdentity = bytes(32, 5);
            final byte[] preparedHash = bytes(32, 7);
            final DestinationPublishRequest firstRequest = request(shard, lane, laneIncarnation, 1, 11);
            final KafkaSourcePosition source = new KafkaSourcePosition(shard, "source-cluster", UUID.randomUUID(),
                    17, 2, 1_000);

            final Map<String, Object> producerConfiguration = producerConfiguration(bootstrap, "k2-e2e-initial");
            try (KafkaProducer<byte[], byte[]> producer = newProducer(producerConfiguration)) {
                producer.initTransactions();
                final KafkaClientArtifactTransactionalDestinationTransport transport =
                        new KafkaClientArtifactTransactionalDestinationTransport(
                                (GuardedTransactionalProducer<byte[], byte[]>) producer);
                final KafkaTransactionalDestinationAdapter adapter = newAdapter(initialTarget, receipt,
                        targetTopic, receiptTopic, shard, lane, laneIncarnation, transactionIdentity, transport);
                requirePublished(adapter.publish(firstRequest, source, preparedHash), "initial target and receipt");
                requireCommittedCounts(bootstrap, targetTopic, receiptTopic, 1, 1);
                abortDirectPair(producer, initialTarget, receipt, targetTopic, receiptTopic, firstRequest, preparedHash);
                requireCommittedCounts(bootstrap, targetTopic, receiptTopic, 1, 1);
                adapter.close();
            }

            admin.deleteTopics(Collections.singleton(targetTopic)).all().get(10, TimeUnit.SECONDS);
            waitForTopicGone(admin, targetTopic);
            ensureTopic(admin, targetTopic);
            final Uuid replacementTargetId = describe(admin, targetTopic).topicId();
            if (initialTargetId.equals(replacementTargetId)) {
                throw new IllegalStateException("K2 delete/recreate did not change target TopicId");
            }
            final DestinationPublishRequest staleRequest = request(shard, lane, laneIncarnation, 2, 13);
            try (KafkaProducer<byte[], byte[]> producer = newProducer(
                    producerConfiguration(bootstrap, "k2-e2e-stale"))) {
                producer.initTransactions();
                final KafkaClientArtifactTransactionalDestinationTransport transport =
                        new KafkaClientArtifactTransactionalDestinationTransport(
                                (GuardedTransactionalProducer<byte[], byte[]>) producer);
                final KafkaTransactionalDestinationAdapter staleAdapter = newAdapter(initialTarget, receipt,
                        targetTopic, receiptTopic, shard, lane, laneIncarnation, transactionIdentity, transport);
                requireDefinitelyNotPublished(staleAdapter.publish(staleRequest, source, bytes(32, 17)),
                        "stale target incarnation");
                staleAdapter.close();
            }
            requireCommittedCounts(bootstrap, targetTopic, receiptTopic, 0, 1);

            final KafkaTargetResource replacementTarget = new KafkaTargetResource(clusterId,
                    toUuid(replacementTargetId), 0);
            final DestinationPublishRequest replacementRequest = request(shard, lane, laneIncarnation, 3, 19);
            try (KafkaProducer<byte[], byte[]> producer = newProducer(
                    producerConfiguration(bootstrap, "k2-e2e-replacement"))) {
                producer.initTransactions();
                final KafkaClientArtifactTransactionalDestinationTransport transport =
                        new KafkaClientArtifactTransactionalDestinationTransport(
                                (GuardedTransactionalProducer<byte[], byte[]>) producer);
                final KafkaTransactionalDestinationAdapter replacementAdapter = newAdapter(replacementTarget,
                        receipt, targetTopic, receiptTopic, shard, lane, laneIncarnation, transactionIdentity, transport);
                requirePublished(replacementAdapter.publish(replacementRequest, source, bytes(32, 23)),
                        "replacement target and receipt");
                replacementAdapter.close();
            }
            requireCommittedCounts(bootstrap, targetTopic, receiptTopic, 1, 2);
            System.out.println("K2 Delay real-client smoke passed: atomic target+receipt commit, abort, and "
                    + "same-name delete/recreate rejection. targetTopicId=" + initialTargetId
                    + " replacementTopicId=" + replacementTargetId);
        }
    }

    private static KafkaTransactionalDestinationAdapter newAdapter(final KafkaTargetResource target,
                                                                     final KafkaReceiptResource receipt,
                                                                     final String targetTopic,
                                                                     final String receiptTopic,
                                                                     final ShardId shard,
                                                                     final DestinationLaneId lane,
                                                                     final byte[] laneIncarnation,
                                                                     final byte[] transactionIdentity,
                                                                     final KafkaClientArtifactTransactionalDestinationTransport transport) {
        return new KafkaTransactionalDestinationAdapter(target, receipt, targetTopic, receiptTopic,
                new KafkaReceiptJournal(shard, receipt), lane, laneIncarnation, transactionIdentity, transport);
    }

    private static void abortDirectPair(final KafkaProducer<byte[], byte[]> producer,
                                        final KafkaTargetResource target,
                                        final KafkaReceiptResource receipt,
                                        final String targetTopic,
                                        final String receiptTopic,
                                        final DestinationPublishRequest request,
                                        final byte[] preparedHash) throws Exception {
        final GuardedTransactionalProducer<byte[], byte[]> guarded =
                (GuardedTransactionalProducer<byte[], byte[]>) producer;
        final ProducerResourceGuard targetGuard = new ProducerResourceGuard(target.authenticatedClusterId(),
                targetTopic, toKafkaUuid(target.nativeTopicUuid()), target.partition());
        final ProducerResourceGuard receiptGuard = new ProducerResourceGuard(receipt.authenticatedClusterId(),
                receiptTopic, toKafkaUuid(receipt.nativeTopicUuid()), receipt.receiptPartition());
        guarded.beginTransaction();
        guarded.sendGuardedInTransaction(new ProducerRecord<>(targetTopic, target.partition(), null,
                request.payload()), targetGuard).get(10, TimeUnit.SECONDS);
        guarded.sendGuardedInTransaction(new ProducerRecord<>(receiptTopic, receipt.receiptPartition(), null,
                Bytes.sha256(Bytes.utf8("abort-key")), preparedHash), receiptGuard).get(10, TimeUnit.SECONDS);
        guarded.abortTransaction();
    }

    private static DestinationPublishRequest request(final ShardId shard, final DestinationLaneId lane,
                                                     final byte[] laneIncarnation, final int generation,
                                                     final int seed) {
        return new DestinationPublishRequest(lane, laneIncarnation, DelayMessageId.random(shard), generation,
                bytes(32, seed), 2_000, 2_000, Bytes.utf8("k2-payload-" + seed), Bytes.utf8("k2-metadata"));
    }

    private static void requirePublished(final java.util.concurrent.CompletionStage<DestinationPublishResult> stage,
                                         final String label) {
        final DestinationPublishResult result = stage.toCompletableFuture().join();
        if (result.disposition() != DestinationPublishResult.Disposition.PUBLISHED) {
            throw new IllegalStateException(label + " was not published: " + result.disposition()
                    + "/" + result.stableCode());
        }
    }

    private static void requireDefinitelyNotPublished(
            final java.util.concurrent.CompletionStage<DestinationPublishResult> stage, final String label) {
        final DestinationPublishResult result = stage.toCompletableFuture().join();
        if (result.disposition() != DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED) {
            throw new IllegalStateException(label + " was not definitively rejected: " + result.disposition()
                    + "/" + result.stableCode());
        }
    }

    private static void requireCommittedCounts(final String bootstrap, final String targetTopic,
                                               final String receiptTopic, final int targetCount,
                                               final int receiptCount) throws Exception {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "k2-e2e-reader-" + UUID.randomUUID());
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(configuration)) {
            final List<TopicPartition> partitions = List.of(new TopicPartition(targetTopic, 0),
                    new TopicPartition(receiptTopic, 0));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
                    records.add(record);
                }
                final long targets = records.stream().filter(record -> record.topic().equals(targetTopic)).count();
                final long receipts = records.stream().filter(record -> record.topic().equals(receiptTopic)).count();
                if (targets >= targetCount && receipts >= receiptCount) {
                    if (targets == targetCount && receipts == receiptCount) {
                        return;
                    }
                    records.clear();
                    consumer.seekToBeginning(partitions);
                }
            }
            throw new IllegalStateException("read_committed counts did not converge: target=" + targetCount
                    + ", receipt=" + receiptCount + ", observed=" + records.size());
        }
    }

    private static Map<String, Object> producerConfiguration(final String bootstrap, final String transactionId) {
        final Map<String, Object> configuration = new HashMap<>();
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        configuration.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId + "-" + UUID.randomUUID());
        configuration.put(ProducerConfig.ACKS_CONFIG, "all");
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put("allow.auto.create.topics", false);
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "nereus-delay-k2-smoke");
        configuration.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        configuration.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        return configuration;
    }

    private static KafkaProducer<byte[], byte[]> newProducer(final Map<String, Object> configuration) {
        return new KafkaProducer<>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static void ensureTopic(final Admin admin, final String topic) throws Exception {
        try {
            if (describe(admin, topic) != null) {
                return;
            }
        } catch (Exception missing) {
            // Create below; Admin exposes unknown topics through the future.
        }
        final NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
        newTopic.configs(Map.of("message.timestamp.type", "LogAppendTime"));
        admin.createTopics(Collections.singleton(newTopic)).all().get(10, TimeUnit.SECONDS);
        waitForTopic(admin, topic);
    }

    private static TopicDescription describe(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(Collections.singleton(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
    }

    private static void waitForTopic(final Admin admin, final String topic) throws Exception {
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
        throw new IllegalStateException("topic metadata did not converge: " + topic);
    }

    private static void waitForTopicGone(final Admin admin, final String topic) throws Exception {
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                if (describe(admin, topic) == null) {
                    return;
                }
            } catch (Exception gone) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("topic deletion did not converge: " + topic);
    }

    private static java.util.UUID toUuid(final Uuid value) {
        return new java.util.UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static Uuid toKafkaUuid(final java.util.UUID value) {
        return new Uuid(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }
}
