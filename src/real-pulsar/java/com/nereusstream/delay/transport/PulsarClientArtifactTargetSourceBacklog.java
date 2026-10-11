package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetSourceBacklog;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

/** Finite, no-ACK P1 prefix scan on an owned non-durable cursor, including its exact retained applied anchor. */
public final class PulsarClientArtifactTargetSourceBacklog {
    private PulsarClientArtifactTargetSourceBacklog() {}

    public static TargetSourceBacklog.Snapshot read(final PulsarClient client, final TopicResourceGuard resource,
            final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
        if (!(Objects.requireNonNull(cut, "cut").applied() instanceof PulsarSourcePosition applied)
                || !Bytes.constantTimeEquals(applied.brokerResourceIncarnation(), resource.resourceIncarnation())) {
            throw new IllegalArgumentException("P1 backlog requires the exact applied Source/resource incarnation");
        }
        final var collector = new TargetSourceBacklog.Collector(cut, limits, System::nanoTime, protection);
        final var opening = Objects.requireNonNull(client, "client").newConsumer(Schema.BYTES)
                .topic(applied.physicalTopic())
                .subscriptionName("nereus-target-source-backlog-" + UUID.randomUUID())
                .subscriptionType(SubscriptionType.Exclusive).subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).receiverQueueSize(1)
                .autoUpdatePartitions(false).startMessageIdInclusive().resourceGuard(resource).subscribeAsync();
        Consumer<byte[]> nativeConsumer = null;
        Throwable failure = null;
        try {
            nativeConsumer = opening.get(remainingMs(collector), TimeUnit.MILLISECONDS);
            if (!(nativeConsumer instanceof GuardedConsumer<?>)) {
                throw new IllegalStateException("P1 backlog requires guarded Source connection proof");
            }
            @SuppressWarnings("unchecked")
            final var consumer = (GuardedConsumer<byte[]>) nativeConsumer;
            consumer.seekAsync(messageId(applied)).get(remainingMs(collector), TimeUnit.MILLISECONDS);
            final var before = PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(consumer,
                    resource, applied.physicalTopic(), applied.shardId().partition(),
                    Duration.ofMillis(remainingMs(collector)));
            // This RPC is addressed to the consumer ID on its guarded SUBSCRIBE connection, not a topic-name lookup.
            final var lastIds = consumer.getLastMessageIdsAsync().get(remainingMs(collector), TimeUnit.MILLISECONDS);
            if (lastIds.size() != 1 || !applied.physicalTopic().equals(lastIds.getFirst().getOwnerTopic())
                    || !(lastIds.getFirst() instanceof MessageIdAdv tail)
                    || tail.getLedgerId() < 0 || tail.getEntryId() < 0) {
                throw new IllegalStateException("P1 backlog lacks its native retained tail");
            }
            while (true) {
                collector.requireCurrent();
                final var message = consumer.receive(Math.toIntExact(remainingMs(collector)), TimeUnit.MILLISECONDS);
                final var after = PulsarClientArtifactRecoverySourcePositioner.requireCurrentProof(consumer,
                        resource, applied.physicalTopic(), applied.shardId().partition());
                if (before.connectionGeneration() != after.connectionGeneration()
                        || !Bytes.constantTimeEquals(before.attestationDigest(), after.attestationDigest())) {
                    throw new IllegalStateException("P1 backlog connection changed across its tail/receive RPC");
                }
                if (message == null || message.getData() == null) {
                    throw new IllegalStateException("P1 backlog retained prefix was unavailable before its deadline");
                }
                final var decoded = PulsarClientArtifactSourceRecordConsumer.decodeTargetReplayRecord(message,
                        applied.shardId(), applied.physicalTopic(), after.attestation(), after.connectionGeneration(),
                        after.attestationDigest());
                final var position = (PulsarSourcePosition) decoded.position();
                collector.add(position, message.getData().length);
                final int relative = compare(position, tail);
                if (relative > 0) {
                    throw new IllegalStateException("P1 backlog skipped its captured native tail");
                }
                if (relative == 0) { return collector.finish(position); }
            }
        } catch (Exception failed) {
            failure = failed;
            if (failed instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            throw new IllegalStateException("P1 Source backlog observation failed", failed);
        } catch (Error fatal) {
            failure = fatal;
            throw fatal;
        } finally {
            if (nativeConsumer == null) {
                opening.whenComplete((late, error) -> { if (late != null) { late.closeAsync(); } });
            } else {
                try {
                    final long closeMs = Math.max(1, Duration.ofNanos(limits.elapsedNanos()).toMillis());
                    nativeConsumer.closeAsync().get(closeMs, TimeUnit.MILLISECONDS);
                } catch (Exception closeFailure) {
                    if (closeFailure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                    if (failure == null) { throw new IllegalStateException("P1 backlog cursor close failed", closeFailure); }
                    failure.addSuppressed(closeFailure);
                }
            }
        }
    }

    private static long remainingMs(final TargetSourceBacklog.Collector collector) {
        final long millis = TimeUnit.NANOSECONDS.toMillis(collector.remainingNanos());
        if (millis < 1 || millis > Integer.MAX_VALUE) {
            throw new IllegalStateException("P1 backlog finite deadline exhausted");
        }
        return millis;
    }

    private static int compare(final PulsarSourcePosition position, final MessageIdAdv tail) {
        final int ledger = Long.compare(position.ledgerId(), tail.getLedgerId());
        if (ledger != 0) { return ledger; }
        final int entry = Long.compare(position.entryId(), tail.getEntryId());
        return entry == 0 ? Integer.compare(position.normalizedBatchIndex(), Math.max(0, tail.getBatchIndex())) : entry;
    }

    private static MessageId messageId(final PulsarSourcePosition position) {
        return position.batchSize() > 1
                ? new BatchMessageIdImpl(position.ledgerId(), position.entryId(), -1,
                        position.normalizedBatchIndex(), position.batchSize(), null)
                : new MessageIdImpl(position.ledgerId(), position.entryId(), -1);
    }
}
