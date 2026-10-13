package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetNativePolicyControlBody;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
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
    public static final int MAX_CONTROL_FRAME_BYTES = 1024 + Math.max(
            Math.max(TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES, TargetMembershipControlBody.MAX_CANONICAL_BYTES),
            Math.max(TargetNativePolicyControlBody.MAX_CANONICAL_BYTES, TargetCloseBody.MAX_CANONICAL_BYTES));

    private PulsarClientArtifactTargetAdmissionHistory() {}

    /** History retention, credential and Owner authority remain mandatory; this reader grants no SEND or GC. */
    public static SystemMutation read(
            final PulsarClient client, final TopicResourceGuard resource,
            final TargetPublishRecoveryDiscovery.Reference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        return read(client, resource, ref.source(), timeout, historyGuard, MAX_FRAME_BYTES, ref::requireImage);
    }

    /** Exact original Control history on an owned cursor; this does not authenticate the Control issuer. */
    public static SystemMutation readControl(final PulsarClient client, final TopicResourceGuard resource,
            final TargetCheckpointDependencies.ControlReference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        return read(client, resource, ref.source(), timeout, historyGuard, MAX_CONTROL_FRAME_BYTES, ref::requireImage);
    }

    /** Caller owns a bounded executor and operation/pin obligations; cancellation is not native-cursor cleanup. */
    public static java.util.concurrent.CompletableFuture<SystemMutation> readControlAsync(final PulsarClient client,
            final TopicResourceGuard resource, final TargetCheckpointDependencies.ControlReference reference,
            final Duration timeout, final Runnable historyGuard,
            final java.util.concurrent.ThreadPoolExecutor executor) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(resource, "resource");
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        return readAsync(timeout, guard, executor, (remaining, active) ->
                readControl(client, resource, ref, remaining, active));
    }

    /** Exact original Outcome history, including rejected first results, on an independent no-ACK cursor. */
    public static SystemMutation readOutcome(final PulsarClient client, final TopicResourceGuard resource,
            final TargetCheckpointDependencies.OutcomeReference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        return read(client, resource, ref.source(), timeout, historyGuard, MAX_FRAME_BYTES, ref::requireImage);
    }

    /** Caller owns a bounded executor and operation/pin obligations; cancellation is not native-cursor cleanup. */
    public static java.util.concurrent.CompletableFuture<SystemMutation> readOutcomeAsync(final PulsarClient client,
            final TopicResourceGuard resource, final TargetCheckpointDependencies.OutcomeReference reference,
            final Duration timeout, final Runnable historyGuard,
            final java.util.concurrent.ThreadPoolExecutor executor) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(resource, "resource");
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        return readAsync(timeout, guard, executor, (remaining, active) ->
                readOutcome(client, resource, ref, remaining, active));
    }

    /** Bounded original Admission read; caller owns the executor and all history/cleanup obligations. */
    public static java.util.concurrent.CompletableFuture<SystemMutation> readAdmissionAsync(PulsarClient client,
            TopicResourceGuard resource, TargetPublishRecoveryDiscovery.Reference reference, Duration timeout,
            Runnable historyGuard, java.util.concurrent.ThreadPoolExecutor executor) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(resource, "resource");
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        return readAsync(timeout, guard, executor, (remaining, active) -> read(client, resource, ref, remaining, active));
    }

    private static java.util.concurrent.CompletableFuture<SystemMutation> readAsync(Duration timeout, Runnable guard,
            java.util.concurrent.ThreadPoolExecutor executor,
            java.util.function.BiFunction<Duration, Runnable, SystemMutation> operation) {
        final var pool = Objects.requireNonNull(executor, "executor");
        if (pool.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) pool.getQueue().size() + pool.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || pool.getRejectedExecutionHandler().getClass()
                        != java.util.concurrent.ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("history executor must have finite threads/queue and abort on overflow");
        }
        final long timeoutMs = Objects.requireNonNull(timeout, "timeout").toMillis();
        if (timeoutMs < 1 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("async history timeout must fit a positive millisecond int");
        }
        final long started = System.nanoTime();
        final long timeoutNanos = timeoutMs * 1_000_000L;
        final Runnable boundedGuard = () -> {
            remainingMs(started, timeoutNanos);
            guard.run();
            remainingMs(started, timeoutNanos);
        };
        return com.nereusstream.delay.ownership.OwnedCheckpointIo.submit(active -> {
            final long remaining = timeoutNanos - (System.nanoTime() - started);
            return operation.apply(Duration.ofNanos(remaining), active);
        }, boundedGuard, pool).receipt();
    }

    private static SystemMutation read(final PulsarClient client, final TopicResourceGuard resource,
            final SourcePosition expectedSource, final Duration timeout, final Runnable historyGuard,
            final int maximumFrameBytes,
            final java.util.function.BiFunction<SystemMutation, SourcePosition, SystemMutation> requireImage) {
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        if (!(expectedSource instanceof PulsarSourcePosition expected)
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
        final var builder = Objects.requireNonNull(client, "client").newConsumer(Schema.BYTES)
                .topic(expected.physicalTopic())
                .subscriptionName("nereus-target-admission-history-" + UUID.randomUUID())
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1)
                .autoUpdatePartitions(false)
                .startMessageIdInclusive()
                .resourceGuard(Objects.requireNonNull(resource, "resource"));
        final var backend = PulsarClientArtifactOwnedBackendGroup.find(client);
        final var opening = new java.util.concurrent.CompletableFuture<Consumer<byte[]>>();
        final var reading = new java.util.concurrent.atomic.AtomicBoolean(true);
        final var cleanup = new com.nereusstream.delay.ownership.OwnedSourceCursor<>(opening,
                PulsarClientArtifactOwnedBackendGroup::closeConsumer, backend);
        if (backend != null) { backend.registerCleanup(cleanup, () -> !reading.get() && cleanup.operationExited(), cleanup::cleanupComplete, proof -> {
            proof.requireClient(client); cleanup.requestClose(); cleanup.backendShutdownCompleted(backend);
        }); }
        Consumer<byte[]> nativeConsumer = null;
        Throwable failure = null;
        try {
            try {
                PulsarClientArtifactOwnedBackendGroup.subscribe(client, builder).whenComplete((value, failed) -> {
                    if (failed == null) { opening.complete(value); }
                    else { opening.completeExceptionally(failed); }
                });
            } catch (RuntimeException | Error failed) { opening.completeExceptionally(failed); throw failed; }
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
            if (message == null || message.getData() == null || message.getData().length > maximumFrameBytes) {
                throw new IllegalStateException("P1 history lacks the exact bounded retained Source record");
            }
            final var after = PulsarClientArtifactRecoverySourcePositioner.requireCurrentProof(consumer,
                    resource, expected.physicalTopic(), expected.shardId().partition());
            if (before.connectionGeneration() != after.connectionGeneration()
                    || !Bytes.constantTimeEquals(before.attestationDigest(), after.attestationDigest())) {
                throw new IllegalStateException("P1 history guard proof changed during receive");
            }
            final var decoded = PulsarClientArtifactSourceRecordConsumer.decodeTargetReplayRecord(message,
                    expected.shardId(), expected.physicalTopic(), after.attestation(),
                    after.connectionGeneration(), after.attestationDigest());
            if (!(decoded instanceof SourceReplayMutation mutation)) {
                throw new IllegalStateException("P1 Target Source history returned a Command");
            }
            final var image = requireImage.apply(mutation.mutation(), mutation.position());
            guard.run();
            remainingMs(started, timeoutNanos);
            return image;
        } catch (Exception failed) {
            failure = failed;
            if (failed instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("P1 exact Target Source history read failed", failed);
        } catch (Error fatal) {
            failure = fatal;
            throw fatal;
        } finally {
            reading.set(false);
            if (backend != null) { backend.cleanupProgress(); }
            try {
                cleanup.requestClose().get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception unconfirmed) {
                if (unconfirmed instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                final var retained = new CleanupFailure(cleanup, client,
                        failure == null ? unconfirmed : failure);
                if (failure != null && unconfirmed != failure) { retained.addSuppressed(unconfirmed); }
                throw retained;
            }
        }
    }

    /** Retains exact native cleanup; callback/result failure alone cannot release provider pins/charges. */
    public static final class CleanupFailure extends IllegalStateException
            implements com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient com.nereusstream.delay.ownership.OwnedSourceCursor<?> cleanup;
        private final transient PulsarClient client;

        private CleanupFailure(com.nereusstream.delay.ownership.OwnedSourceCursor<?> cleanup,
                PulsarClient client, Throwable failure) {
            super("P1 Source history cleanup is not confirmed", failure);
            this.cleanup = cleanup;
            this.client = client;
        }

        public boolean cleanupComplete() { return cleanup.cleanupComplete(); }
        public Throwable cleanupFailure() { return cleanup.cleanupFailure(); }
        public java.util.concurrent.CompletableFuture<Void> retryClose() { return cleanup.retryClose(); }

        @Override
        public java.util.concurrent.CompletableFuture<Void> retryCleanup() { return retryClose(); }

        /** Explicit original-client backend-owner shutdown only; never automatic shared-client timeout cleanup. */
        public java.util.concurrent.CompletableFuture<Void> closeClientBackendAfterFailedOpen() {
            if (!cleanup.failedOpenNeedsBackendShutdown()) {
                throw new IllegalStateException("history operation has no failed-open backend shutdown obligation");
            }
            final var owner = PulsarClientArtifactOwnedBackendGroup.find(client);
            if (owner == null) { return java.util.concurrent.CompletableFuture.failedFuture(
                    new IllegalStateException("P1 history client is borrowed")); }
            return owner.requestHostQuarantine();
        }
    }

    private static long remainingMs(long started, long timeoutNanos) {
        final long remaining = timeoutNanos - (System.nanoTime() - started);
        if (remaining <= 0) {
            throw new IllegalStateException("P1 Target Source history deadline exhausted");
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

}
