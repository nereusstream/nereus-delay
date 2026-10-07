package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

/** P1 exact history read on an owned non-durable cursor; active Worker subscriptions and ACK state are untouched. */
public final class PulsarClientArtifactTargetAdmissionHistory {
    public static final int MAX_FRAME_BYTES = 2 * TargetPublishAdmissionBody.MAX_MATERIALIZED_BYTES + 2048;

    private PulsarClientArtifactTargetAdmissionHistory() {}

    /** History retention, credential and Owner authority remain mandatory; this reader grants no SEND or GC. */
    public static SystemMutation read(
            final PulsarClient client, final TopicResourceGuard resource,
            final TargetPublishRecoveryDiscovery.Reference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        if (!(ref.source() instanceof PulsarSourcePosition expected)
                || !Arrays.equals(expected.brokerResourceIncarnation(), resource.resourceIncarnation())) {
            throw new IllegalArgumentException("P1 history requires its exact Pulsar source/resource incarnation");
        }
        final long timeoutMs = Objects.requireNonNull(timeout, "timeout").toMillis();
        if (timeoutMs < 1 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("P1 history timeout must fit a positive millisecond int");
        }
        final long timeoutNanos = Math.multiplyExact(timeoutMs, 1_000_000L);
        final long started = System.nanoTime();
        guard.run();
        final var opening = Objects.requireNonNull(client, "client").newConsumer(Schema.BYTES)
                .topic(expected.physicalTopic())
                .subscriptionName("nereus-target-admission-history-" + UUID.randomUUID())
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .autoUpdatePartitions(false)
                .startMessageIdInclusive()
                .resourceGuard(Objects.requireNonNull(resource, "resource"))
                .subscribeAsync();
        Consumer<byte[]> nativeConsumer = null;
        Throwable failure = null;
        try {
            nativeConsumer = opening.get(remainingMs(started, timeoutNanos), TimeUnit.MILLISECONDS);
            if (!(nativeConsumer instanceof GuardedConsumer<?>)) {
                throw new IllegalStateException("P1 history requires the guarded source proof API");
            }
            @SuppressWarnings("unchecked")
            final GuardedConsumer<byte[]> consumer = (GuardedConsumer<byte[]>) nativeConsumer;
            guard.run();
            consumer.seekAsync(messageId(expected)).get(remainingMs(started, timeoutNanos), TimeUnit.MILLISECONDS);
            final var before = PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(consumer,
                    resource, expected.physicalTopic(), expected.shardId().partition(),
                    Duration.ofMillis(remainingMs(started, timeoutNanos)));
            guard.run();
            final var message = consumer.receive(Math.toIntExact(remainingMs(started, timeoutNanos)),
                    TimeUnit.MILLISECONDS);
            if (message == null || message.getData() == null || message.getData().length > MAX_FRAME_BYTES) {
                throw new IllegalStateException("P1 history lacks the exact bounded retained Admission record");
            }
            final var after = PulsarClientArtifactRecoverySourcePositioner.requireCurrentProof(consumer,
                    resource, expected.physicalTopic(), expected.shardId().partition());
            if (before.connectionGeneration() != after.connectionGeneration()
                    || !Bytes.constantTimeEquals(before.attestationDigest(), after.attestationDigest())) {
                throw new IllegalStateException("P1 history guard proof changed during receive");
            }
            final var decoded = PulsarClientArtifactSourceRecordConsumer.decodeReplayRecord(message,
                    expected.shardId(), expected.physicalTopic(), after.attestation(),
                    after.connectionGeneration(), after.attestationDigest());
            if (!(decoded instanceof SourceReplayMutation mutation)) {
                throw new IllegalStateException("P1 Admission history returned a Command");
            }
            final var image = ref.requireImage(mutation.mutation(), mutation.position());
            guard.run();
            remainingMs(started, timeoutNanos);
            return image;
        } catch (Exception failed) {
            failure = failed;
            if (failed instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("P1 exact Admission history read failed", failed);
        } catch (Error fatal) {
            failure = fatal;
            throw fatal;
        } finally {
            if (nativeConsumer == null) {
                // A timeout does not cancel Broker creation; close any late-created owned cursor.
                opening.whenComplete((late, error) -> { if (late != null) { late.closeAsync(); } });
            } else {
                close(nativeConsumer, timeoutMs, failure);
            }
        }
    }

    private static long remainingMs(long started, long timeoutNanos) {
        final long remaining = timeoutNanos - (System.nanoTime() - started);
        if (remaining <= 0) {
            throw new IllegalStateException("P1 Admission history deadline exhausted");
        }
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
    }

    private static MessageId messageId(PulsarSourcePosition source) {
        if (source.entryKind() == PulsarSourcePosition.EntryKind.BATCH) {
            return new BatchMessageIdImpl(source.ledgerId(), source.entryId(), -1,
                    source.normalizedBatchIndex(), source.batchSize(), null);
        }
        return new MessageIdImpl(source.ledgerId(), source.entryId(), -1);
    }

    private static void close(Consumer<byte[]> consumer, long timeoutMs, Throwable failure) {
        try {
            consumer.closeAsync().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception closeFailure) {
            if (closeFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure == null) {
                throw new IllegalStateException("P1 owned history cursor close was not confirmed", closeFailure);
            }
            failure.addSuppressed(closeFailure);
        }
    }
}
