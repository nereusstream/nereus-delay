package com.nereusstream.delay.transport;

import java.util.Objects;
import org.apache.pulsar.client.api.Producer;
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
        return client.newProducer(Schema.BYTES)
                .topic(Objects.requireNonNull(physicalTopic, "physicalTopic"))
                .resourceGuard(new TopicResourceGuard(
                        authenticatedClusterId, resourceIncarnation, physicalTopicCreationTimestamp))
                .producerName(Objects.requireNonNull(producerName, "producerName"))
                .enableBatching(false)
                .enableChunking(false)
                .maxPendingMessages(1)
                .blockIfQueueFull(false)
                .autoUpdatePartitions(false)
                .create();
    }
}
