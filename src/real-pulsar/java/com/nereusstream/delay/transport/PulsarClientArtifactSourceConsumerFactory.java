package com.nereusstream.delay.transport;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
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
    // The public P1 Consumer API does not expose its ACK receipt setting. Track
    // only the exact consumers built below with receipt mode enabled; a direct
    // adapter constructor may still process source records, but cannot attest
    // a checkpoint cut without this construction evidence.
    private static final List<WeakReference<GuardedConsumer<?>>> RECEIPT_ENABLED = new ArrayList<>();

    private PulsarClientArtifactSourceConsumerFactory() {}

    static synchronized boolean isReceiptEnabled(final GuardedConsumer<?> consumer) {
        boolean found = false;
        final var references = RECEIPT_ENABLED.iterator();
        while (references.hasNext()) {
            final GuardedConsumer<?> current = references.next().get();
            if (current == null) {
                references.remove();
            } else if (current == consumer) {
                found = true;
            }
        }
        return found;
    }

    public static GuardedConsumer<byte[]> create(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final String subscriptionName)
            throws PulsarClientException {
        return create(client, guard, physicalTopic, subscriptionName, SubscriptionType.Exclusive);
    }

    /** Creates a guarded consumer with an explicit subscription risk profile. */
    public static GuardedConsumer<byte[]> create(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final String subscriptionName,
            final SubscriptionType subscriptionType)
            throws PulsarClientException {
        Objects.requireNonNull(client, "client");
        final Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(Objects.requireNonNull(physicalTopic, "physicalTopic"))
                .subscriptionName(Objects.requireNonNull(subscriptionName, "subscriptionName"))
                .subscriptionType(Objects.requireNonNull(subscriptionType, "subscriptionType"))
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
        synchronized (PulsarClientArtifactSourceConsumerFactory.class) {
            isReceiptEnabled(result);
            RECEIPT_ENABLED.add(new WeakReference<>(result));
        }
        return result;
    }
}
