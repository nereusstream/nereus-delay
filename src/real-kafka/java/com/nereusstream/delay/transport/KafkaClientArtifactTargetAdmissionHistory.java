package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;

/** Exact K1 Target Source history read using a dedicated manual-assignment consumer, with no offset commit path. */
public final class KafkaClientArtifactTargetAdmissionHistory {
    public static final int MAX_FRAME_BYTES = 2 * TargetPublishAdmissionBody.MAX_MATERIALIZED_BYTES + 2048;
    public static final int MAX_CONTROL_FRAME_BYTES = 1024 + Math.max(
            Math.max(TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES, TargetMembershipControlBody.MAX_CANONICAL_BYTES),
            Math.max(TargetNativePolicyControlBody.MAX_CANONICAL_BYTES, TargetCloseBody.MAX_CANONICAL_BYTES));

    private KafkaClientArtifactTargetAdmissionHistory() {}

    /**
     * The caller must protect source retention and authorize the history reader's credential/Owner lifetime.
     * This mandatory guard runs before opening, before Fetch and after exact image validation. A successful
     * read proves only these retained bytes and the authenticated resource; it never authorizes SEND or GC.
     */
    public static SystemMutation read(
            final Map<String, Object> configuration, final String physicalTopic,
            final TargetPublishRecoveryDiscovery.Reference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        return read(configuration, physicalTopic, ref.source(), timeout, historyGuard, MAX_FRAME_BYTES,
                ref::requireImage);
    }

    /** Reads an exact original Control input, including rejected first results, without altering the active group. */
    public static SystemMutation readControl(final Map<String, Object> configuration, final String physicalTopic,
            final TargetCheckpointDependencies.ControlReference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        return read(configuration, physicalTopic, ref.source(), timeout, historyGuard, MAX_CONTROL_FRAME_BYTES,
                ref::requireImage);
    }

    /** Caller owns a bounded executor and all operation/pin obligations, including after cancellation or failure. */
    public static java.util.concurrent.CompletableFuture<SystemMutation> readControlAsync(
            final Map<String, Object> configuration, final String physicalTopic,
            final TargetCheckpointDependencies.ControlReference reference, final Duration timeout,
            final Runnable historyGuard, final java.util.concurrent.ThreadPoolExecutor executor) {
        final var config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        return readAsync(timeout, guard, executor, (remaining, active) ->
                readControl(config, physicalTopic, ref, remaining, active));
    }

    /** Exact original Outcome history, including rejected first results, on an independent no-commit cursor. */
    public static SystemMutation readOutcome(final Map<String, Object> configuration, final String physicalTopic,
            final TargetCheckpointDependencies.OutcomeReference reference, final Duration timeout,
            final Runnable historyGuard) {
        final var ref = Objects.requireNonNull(reference, "reference");
        return read(configuration, physicalTopic, ref.source(), timeout, historyGuard, MAX_FRAME_BYTES,
                ref::requireImage);
    }

    /** Caller owns a bounded executor and all operation/pin obligations, including after cancellation or failure. */
    public static java.util.concurrent.CompletableFuture<SystemMutation> readOutcomeAsync(
            final Map<String, Object> configuration, final String physicalTopic,
            final TargetCheckpointDependencies.OutcomeReference reference, final Duration timeout,
            final Runnable historyGuard, final java.util.concurrent.ThreadPoolExecutor executor) {
        final var config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        return readAsync(timeout, guard, executor, (remaining, active) ->
                readOutcome(config, physicalTopic, ref, remaining, active));
    }

    /** Bounded original Admission read; caller owns the executor and all history/cleanup obligations. */
    public static java.util.concurrent.CompletableFuture<SystemMutation> readAdmissionAsync(
            Map<String, Object> configuration,
            String physicalTopic, TargetPublishRecoveryDiscovery.Reference reference, Duration timeout,
            Runnable historyGuard, java.util.concurrent.ThreadPoolExecutor executor) {
        final var config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
        final var ref = Objects.requireNonNull(reference, "reference");
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        return readAsync(timeout, guard, executor, (remaining, active) -> read(config, physicalTopic, ref, remaining, active));
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

    private static SystemMutation read(final Map<String, Object> configuration, final String physicalTopic,
            final SourcePosition expectedSource, final Duration timeout, final Runnable historyGuard,
            final int maximumFrameBytes,
            final java.util.function.BiFunction<SystemMutation, SourcePosition, SystemMutation> requireImage) {
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        if (!(expectedSource instanceof KafkaSourcePosition expected)) {
            throw new IllegalArgumentException("K1 history requires a Kafka Source locator");
        }
        final long timeoutMs = Objects.requireNonNull(timeout, "timeout").toMillis();
        if (timeoutMs < 1 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("K1 history timeout must fit a positive millisecond int");
        }
        final long timeoutNanos = Math.multiplyExact(timeoutMs, 1_000_000L);
        final var config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
        config.remove(ConsumerConfig.GROUP_ID_CONFIG);
        config.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maximumFrameBytes);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maximumFrameBytes);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        guard.run();
        final long started = System.nanoTime();
        final var consumer = KafkaClientArtifactSourceConsumerFactory.create(config,
                expected.authenticatedClusterId(), physicalTopic, expected.nativeTopicUuid(),
                expected.shardId().partition());
        Throwable failure = null;
        try {
            final var partition = new TopicPartition(physicalTopic, expected.shardId().partition());
            consumer.assign(List.of(partition));
            consumer.seek(partition, expected.offset());
            final var resource = consumer.resourceGuard();
            while (true) {
                guard.run();
                final long remaining = timeoutNanos - (System.nanoTime() - started);
                if (remaining <= 0) {
                    throw new IllegalStateException("K1 Target Source history was unavailable before its deadline");
                }
                final var records = consumer.pollGuarded(Duration.ofNanos(remaining));
                final var evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, resource);
                for (var record : records.records(partition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, resource);
                    if (record.offset() != expected.offset() || record.timestamp() < 0
                            || record.value() == null || record.value().length > maximumFrameBytes) {
                        throw new IllegalStateException("K1 history lacks the exact bounded retained Source record");
                    }
                    final var source = new KafkaSourcePosition(expected.shardId(), expected.authenticatedClusterId(),
                            expected.nativeTopicUuid(), record.offset(), record.leaderEpoch().orElse(null),
                            record.timestamp());
                    final var decoded = KafkaClientArtifactSourceRecordDecoder.decodeTarget(
                            record.value(), expected.shardId(), source, null, null);
                    if (!(decoded instanceof SourceReplayMutation mutation)) {
                        throw new IllegalStateException("K1 Target Source history returned a Command");
                    }
                    final var image = requireImage.apply(mutation.mutation(), source);
                    guard.run();
                    if (System.nanoTime() - started >= timeoutNanos) {
                        throw new IllegalStateException("K1 Target Source history verification exceeded its deadline");
                    }
                    return image;
                }
            }
        } catch (RuntimeException | Error failed) {
            failure = failed;
            throw failed;
        } finally {
            close(consumer, Duration.ofMillis(timeoutMs), failure);
        }
    }

    private static void close(GuardedConsumer<byte[], byte[]> consumer, Duration timeout, Throwable failure) {
        try {
            consumer.close(CloseOptions.timeout(timeout));
        } catch (RuntimeException | Error closeFailure) {
            final var retained = new CleanupFailure(consumer, timeout, failure == null ? closeFailure : failure);
            if (failure != null && closeFailure != failure) { retained.addSuppressed(closeFailure); }
            throw retained;
        }
    }

    private static long remainingMs(long started, long timeoutNanos) {
        final long remaining = timeoutNanos - (System.nanoTime() - started);
        if (remaining < 1_000_000L) { throw new IllegalStateException("K1 history deadline exhausted"); }
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining);
    }

    /** Exact private cursor remains charged after failed close; retry never opens a replacement. */
    public static final class CleanupFailure extends IllegalStateException
            implements com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient GuardedConsumer<byte[], byte[]> consumer;
        private final transient Duration timeout;
        private volatile boolean complete;

        private CleanupFailure(GuardedConsumer<byte[], byte[]> consumer, Duration timeout, Throwable failure) {
            super("K1 Source history cleanup is not confirmed", failure);
            this.consumer = consumer;
            this.timeout = timeout;
        }

        public boolean cleanupComplete() { return complete; }
        public synchronized void retryClose() {
            if (!complete) {
                consumer.close(CloseOptions.timeout(timeout));
                complete = true;
            }
        }
    }

}
