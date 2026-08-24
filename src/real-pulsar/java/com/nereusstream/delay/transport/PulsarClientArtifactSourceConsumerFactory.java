package com.nereusstream.delay.transport;

import java.util.Objects;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Creates the single-topic, receipt-enabled P1 source consumer shape. */
public final class PulsarClientArtifactSourceConsumerFactory {
    private PulsarClientArtifactSourceConsumerFactory() {}

    public static GuardedConsumer<byte[]> create(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final String subscriptionName)
            throws PulsarClientException {
        Objects.requireNonNull(client, "client");
        final Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(Objects.requireNonNull(physicalTopic, "physicalTopic"))
                .subscriptionName(Objects.requireNonNull(subscriptionName, "subscriptionName"))
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .isAckReceiptEnabled(true)
                .autoUpdatePartitions(false)
                .resourceGuard(Objects.requireNonNull(guard, "guard"))
                .subscribe();
        if (!(consumer instanceof GuardedConsumer<?> guarded)) {
            try {
                consumer.close();
            } catch (PulsarClientException failure) {
                throw new IllegalStateException("P1 guarded consumer did not implement the proof API", failure);
            }
            throw new IllegalStateException("P1 guarded consumer did not implement the proof API");
        }
        @SuppressWarnings("unchecked")
        final GuardedConsumer<byte[]> result = (GuardedConsumer<byte[]>) guarded;
        return result;
    }
}
