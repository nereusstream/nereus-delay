package com.nereusstream.delay.transport;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Creates the only producer shape admitted by the P1 Delay binding. */
public final class PulsarClientArtifactProducerFactory {
    private PulsarClientArtifactProducerFactory() {}

    public static Producer<byte[]> create(
            final PulsarClient client,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final String producerName)
            throws PulsarClientException {
        Objects.requireNonNull(client, "client");
        return createBounded(
                client,
                builder(
                        client,
                        authenticatedClusterId,
                        resourceIncarnation,
                        physicalTopic,
                        physicalTopicCreationTimestamp,
                        producerName));
    }

    /** Exclusive Target Journal writer; it cannot reverse-fence a replacement writer that already owns the topic. */
    public static Producer<byte[]> createExclusiveJournal(
            final PulsarClient client,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final String producerName)
            throws PulsarClientException {
        return createBounded(
                client,
                builder(
                                client,
                                authenticatedClusterId,
                                resourceIncarnation,
                                physicalTopic,
                                physicalTopicCreationTimestamp,
                                producerName)
                        .accessMode(ProducerAccessMode.Exclusive));
    }

    static ProducerBuilder<byte[]> builder(
            final PulsarClient client,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final String producerName) {
        Objects.requireNonNull(client, "client");
        return client.newProducer(Schema.BYTES)
                .topic(Objects.requireNonNull(physicalTopic, "physicalTopic"))
                .resourceGuard(new TopicResourceGuard(
                        authenticatedClusterId, resourceIncarnation, physicalTopicCreationTimestamp))
                .producerName(Objects.requireNonNull(producerName, "producerName"))
                .enableBatching(false)
                .enableChunking(false)
                .maxPendingMessages(1)
                .blockIfQueueFull(false)
                .autoUpdatePartitions(false);
    }

    private static Producer<byte[]> createBounded(PulsarClient client, ProducerBuilder<byte[]> builder)
            throws PulsarClientException {
        final var group = PulsarClientArtifactOwnedBackendGroup.find(client);
        if (group == null) {
            return builder.create();
        }
        try {
            return PulsarClientArtifactOwnedBackendGroup.createProducer(client, builder, null)
                    .get(group.operationTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw PulsarClientException.unwrap(failure);
        }
    }

    public static Producer<byte[]> create(
            PulsarClientArtifactOwnedBackendGroup group,
            String cluster,
            byte[] incarnation,
            String topic,
            long createdAt,
            String name)
            throws PulsarClientException {
        return create(group.nativeClientForFactory(), cluster, incarnation, topic, createdAt, name);
    }

    public static Producer<byte[]> createExclusiveJournal(
            PulsarClientArtifactOwnedBackendGroup group,
            String cluster,
            byte[] incarnation,
            String topic,
            long createdAt,
            String name)
            throws PulsarClientException {
        return createExclusiveJournal(group.nativeClientForFactory(), cluster, incarnation, topic, createdAt, name);
    }
}
