package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedSourceCursor;
import com.nereusstream.delay.ownership.TargetSourceBacklog;
import com.nereusstream.delay.ownership.TargetSourceBacklogRead;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

    public static Query open(final PulsarClient client, final TopicResourceGuard resource,
            final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
        return new Query(client, resource, cut, limits, protection);
    }

    public static TargetSourceBacklog.Snapshot read(final PulsarClient client, final TopicResourceGuard resource,
            final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
        final var operation = open(client, resource, cut, limits, protection);
        try {
            final var result = operation.read();
            operation.requestClose().get(Math.max(1, Duration.ofNanos(limits.elapsedNanos()).toMillis()),
                    TimeUnit.MILLISECONDS);
            return result;
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            operation.requestClose();
            throw new TargetSourceBacklogRead.Failure(operation, failure);
        }
    }

    /** Caller retains this operation until cleanupComplete, even if the read future times out or is cancelled. */
    public static final class Query implements TargetSourceBacklogRead {
        private final PulsarClient client;
        private final TopicResourceGuard resource;
        private final PulsarSourcePosition applied;
        private final TargetSourceBacklog.Collector collector;
        private final CompletableFuture<Consumer<byte[]>> opening = new CompletableFuture<>();
        private final OwnedSourceCursor<Consumer<byte[]>> owned;
        private final PulsarClientArtifactOwnedBackendGroup backend;
        private boolean readStarted;
        private boolean openStarted;
        private boolean closing;

        private Query(final PulsarClient client, final TopicResourceGuard resource,
                final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
            this.client = Objects.requireNonNull(client, "client");
            backend = PulsarClientArtifactOwnedBackendGroup.find(client);
            owned = new OwnedSourceCursor<>(opening, PulsarClientArtifactOwnedBackendGroup::closeConsumer, backend);
            if (backend != null) { backend.registerCleanup(owned, owned::operationExited, owned::cleanupComplete, proof -> {
                proof.requireClient(client); requestClose(); owned.backendShutdownCompleted(backend);
            }); }
            this.resource = Objects.requireNonNull(resource, "resource");
            if (!(Objects.requireNonNull(cut, "cut").applied() instanceof PulsarSourcePosition position)
                    || !Bytes.constantTimeEquals(position.brokerResourceIncarnation(), resource.resourceIncarnation())) {
                throw new IllegalArgumentException("P1 backlog requires exact applied Source/resource incarnation");
            }
            applied = position;
            collector = new TargetSourceBacklog.Collector(cut, limits, System::nanoTime, protection);
        }

        @Override
        public TargetSourceBacklog.Snapshot read() {
            try {
                synchronized (this) {
                    if (closing || readStarted) { throw new IllegalStateException("P1 backlog read already used/closed"); }
                    readStarted = true;
                }
                collector.requireCurrent();
                synchronized (this) {
                    if (closing) { throw new IllegalStateException("P1 backlog read was closed before subscribe"); }
                    openStarted = true;
                    try {
                        final var builder = client.newConsumer(Schema.BYTES).topic(applied.physicalTopic())
                                .subscriptionName("nereus-target-source-backlog-" + UUID.randomUUID())
                                .subscriptionType(SubscriptionType.Exclusive).subscriptionMode(SubscriptionMode.NonDurable)
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).receiverQueueSize(1)
                                .autoUpdatePartitions(false).startMessageIdInclusive().resourceGuard(resource);
                        PulsarClientArtifactOwnedBackendGroup.subscribe(client, builder).whenComplete((cursor, failure) -> {
                                    if (failure == null) { opening.complete(cursor); }
                                    else { opening.completeExceptionally(failure); }
                                });
                    } catch (RuntimeException | Error failure) {
                        opening.completeExceptionally(failure);
                        throw failure;
                    }
                }
                return owned.read(remainingMs(collector), this::readCursor);
            } catch (Exception failure) {
                if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                throw new TargetSourceBacklogRead.Failure(this, failure);
            } finally {
                requestClose();
                if (backend != null) { backend.cleanupProgress(); }
            }
        }

        private TargetSourceBacklog.Snapshot readCursor(final Consumer<byte[]> nativeCursor) {
            try {
                if (!(nativeCursor instanceof GuardedConsumer<?>)) {
                    throw new IllegalStateException("P1 backlog requires guarded Source connection proof");
                }
                @SuppressWarnings("unchecked")
                final var consumer = (GuardedConsumer<byte[]>) nativeCursor;
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
            } catch (Exception failure) {
                if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                throw new IllegalStateException("P1 backlog cursor read failed", failure);
            }
        }

        @Override
        public synchronized CompletableFuture<Void> requestClose() {
            closing = true;
            if (!openStarted && !owned.cleanupComplete()) { owned.abandonBeforeOpen(); }
            final var completion = owned.requestClose();
            if (backend != null) { backend.cleanupProgress(); }
            return completion;
        }

        @Override
        public CompletableFuture<Void> retryClose() { return owned.retryClose(); }

        @Override
        public boolean cleanupComplete() { return owned.cleanupComplete(); }

        @Override
        public Throwable cleanupFailure() { return owned.cleanupFailure(); }

        /** Explicit backend-owner shutdown; never invoked by read timeout or cache cleanup. Closes this SDK client. */
        public CompletableFuture<Void> closeClientBackendAfterFailedOpen() {
            requestClose();
            if (!owned.failedOpenNeedsBackendShutdown()) {
                throw new IllegalStateException("P1 Source observer has no failed-open backend shutdown obligation");
            }
            if (backend == null) { return CompletableFuture.failedFuture(new IllegalStateException("P1 observer client is borrowed")); }
            return backend.requestHostQuarantine();
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
