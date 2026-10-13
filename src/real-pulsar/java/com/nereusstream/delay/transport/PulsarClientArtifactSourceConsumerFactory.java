package com.nereusstream.delay.transport;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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

    public static GuardedConsumer<byte[]> createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt, TopicResourceGuard guard,
            String physicalTopic, String subscriptionName,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time) throws PulsarClientException {
        group.addSourceAuthorization(receipt);
        final var authorization = receipt.authorization();
        final var resource = authorization.resource().pulsar();
        if (!physicalTopic.equals(authorization.physicalTopic()) || !guard.authenticatedClusterId().equals(resource.authenticatedClusterId())
                || !java.util.Arrays.equals(guard.resourceIncarnation(), resource.resourceIncarnation())
                || guard.topicCreationTimestamp() != resource.physicalTopicCreationTimestamp()) {
            throw new IllegalStateException("P1 Source subscribe differs from its signed resource");
        }
        authorization.requireName(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.CONSUME, subscriptionName);
        receipt.requireOperation(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        receipt.requireOperation(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.CONSUME, time.get());
        receipt.requireOperation(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.ACK, time.get());
        final var consumer = create(group, guard, physicalTopic, subscriptionName);
        try { receipt.requireActive(time.get()); return consumer; }
        catch (RuntimeException | Error failure) {
            try { PulsarClientArtifactOwnedBackendGroup.closeConsumer(consumer).get(group.operationTimeoutMs(), TimeUnit.MILLISECONDS); }
            catch (Exception cleanup) { if (cleanup instanceof InterruptedException) Thread.currentThread().interrupt(); failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

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
        final var actualGroup = PulsarClientArtifactOwnedBackendGroup.find(client);
        if (actualGroup != null) { actualGroup.requireSourceSubscription(guard, physicalTopic, subscriptionName, subscriptionType); }
        final var builder = client.newConsumer(Schema.BYTES)
                .topic(Objects.requireNonNull(physicalTopic, "physicalTopic"))
                .subscriptionName(Objects.requireNonNull(subscriptionName, "subscriptionName"))
                .subscriptionType(Objects.requireNonNull(subscriptionType, "subscriptionType"))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .isAckReceiptEnabled(true)
                .autoUpdatePartitions(false)
                .resourceGuard(Objects.requireNonNull(guard, "guard"));
        if (actualGroup != null && actualGroup.sourceCredentialReceipt() != null) {
            builder.subscriptionMode(org.apache.pulsar.client.api.SubscriptionMode.Durable);
        }
        final Consumer<byte[]> consumer;
        final var owned = PulsarClientArtifactOwnedBackendGroup.find(client);
        if (owned == null) { consumer = builder.subscribe(); }
        else {
            try { consumer = PulsarClientArtifactOwnedBackendGroup.subscribe(client, builder)
                    .get(owned.operationTimeoutMs(), TimeUnit.MILLISECONDS); }
            catch (Exception failure) {
                if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                throw PulsarClientException.unwrap(failure);
            }
        }
        if (!(consumer instanceof GuardedConsumer<?> guarded)) {
            try {
                if (owned == null) { consumer.close(); }
                else { owned.closeNative(consumer).get(owned.operationTimeoutMs(), TimeUnit.MILLISECONDS); }
            } catch (Exception failure) {
                if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
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

    public static GuardedConsumer<byte[]> create(PulsarClientArtifactOwnedBackendGroup group, TopicResourceGuard guard,
            String physicalTopic, String subscriptionName) throws PulsarClientException {
        return create(group.nativeClientForFactory(), guard, physicalTopic, subscriptionName);
    }
}
