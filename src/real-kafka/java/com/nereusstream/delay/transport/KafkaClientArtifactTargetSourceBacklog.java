package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedSourceCursor;
import com.nereusstream.delay.ownership.TargetSourceBacklog;
import com.nereusstream.delay.ownership.TargetSourceBacklogRead;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/** Finite, no-commit K1 scan of actual frames from the protected applied anchor through a guarded high watermark. */
public final class KafkaClientArtifactTargetSourceBacklog {
    private KafkaClientArtifactTargetSourceBacklog() {}

    public static Query openVerified(Map<String,Object> configuration, String topic, TargetSourceBacklog.Cut cut,
            TargetSourceBacklog.Limits limits, Runnable protection,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt credential,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time, byte[] sdkJar) {
        com.nereusstream.delay.ownership.TargetSdkArtifactIdentity.requireExact(org.apache.kafka.clients.consumer.KafkaConsumer.class, sdkJar);
        final var role = com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.OBSERVATION;
        final String name = credential.authorization().newReaderName(role);
        final var guard = credential.guardRead(role, cut.applied(), topic, name, time, protection);
        final var config = new HashMap<>(KafkaClientArtifactMtlsConfiguration.applySource(configuration, credential));
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, name);
        return open(config, topic, cut, limits, guard);
    }

    public static Query open(final Map<String, Object> configuration, final String physicalTopic,
            final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
        return new Query(configuration, physicalTopic, cut, limits, protection);
    }

    public static TargetSourceBacklog.Snapshot read(final Map<String, Object> configuration, final String physicalTopic,
            final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
        final var operation = open(configuration, physicalTopic, cut, limits, protection);
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

    public static final class Query implements TargetSourceBacklogRead {
        private final String physicalTopic;
        private final KafkaSourcePosition applied;
        private final TargetSourceBacklog.Collector collector;
        private final long timeoutMs;
        private final Map<String, Object> config;
        private final CompletableFuture<GuardedConsumer<byte[], byte[]>> opening = new CompletableFuture<>();
        private final OwnedSourceCursor<GuardedConsumer<byte[], byte[]>> owned;
        private boolean readStarted;
        private boolean openStarted;
        private boolean closing;

        private Query(final Map<String, Object> configuration, final String physicalTopic,
                final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
            this.physicalTopic = Objects.requireNonNull(physicalTopic, "physicalTopic");
            if (!(Objects.requireNonNull(cut, "cut").applied() instanceof KafkaSourcePosition position)
                    || position.offset() < 0) {
                throw new IllegalArgumentException("K1 backlog requires a retained native Kafka applied frontier");
            }
            applied = position;
            collector = new TargetSourceBacklog.Collector(cut, limits, System::nanoTime, protection);
            timeoutMs = Duration.ofNanos(limits.elapsedNanos()).toMillis();
            if (timeoutMs < 1 || timeoutMs > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("K1 backlog deadline must fit a positive millisecond int");
            }
            config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
            config.remove(ConsumerConfig.GROUP_ID_CONFIG);
            config.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
            config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, limits.maximumFrameBytes());
            config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, limits.maximumFrameBytes());
            config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
            config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
            collector.requireCurrent();
            owned = new OwnedSourceCursor<>(opening, consumer -> {
                final var completion = new CompletableFuture<Void>();
                try {
                    consumer.closeGuardedResources(Duration.ofMillis(timeoutMs));
                    if (!consumer.guardedResourcesTerminated()) throw new IllegalStateException("K1 observer native resources are still live");
                    completion.complete(null);
                } catch (RuntimeException | Error failure) {
                    completion.completeExceptionally(failure);
                }
                return completion;
            });
        }

        @Override
        public TargetSourceBacklog.Snapshot read() {
            try {
                synchronized (this) {
                    if (closing || readStarted) { throw new IllegalStateException("K1 backlog read already used/closed"); }
                    readStarted = true;
                }
                collector.requireCurrent();
                synchronized (this) {
                    if (closing) { throw new IllegalStateException("K1 backlog closed before SDK opening"); }
                    openStarted = true;
                    try {
                        opening.complete(KafkaClientArtifactSourceConsumerFactory.create(config,
                                applied.authenticatedClusterId(), physicalTopic, applied.nativeTopicUuid(),
                                applied.shardId().partition()));
                    } catch (RuntimeException | Error failure) {
                        opening.completeExceptionally(failure);
                        throw failure;
                    }
                }
                return owned.read(timeoutMs, this::readCursor);
            } catch (Exception failure) {
                if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                throw new TargetSourceBacklogRead.Failure(this, failure);
            } finally {
                requestClose();
            }
        }

        private TargetSourceBacklog.Snapshot readCursor(final GuardedConsumer<byte[], byte[]> consumer) {
            final var partition = new TopicPartition(physicalTopic, applied.shardId().partition());
            consumer.assign(List.of(partition));
            consumer.seek(partition, applied.offset());
            final var resource = consumer.resourceGuard();
            long tailExclusive = -1;
            KafkaSourcePosition last = null;
            while (true) {
                collector.requireCurrent();
                final var records = consumer.pollGuarded(Duration.ofNanos(collector.remainingNanos()));
                collector.requireCurrent();
                final var evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, resource);
                if (evidence == null) { continue; }
                if (tailExclusive < 0) {
                    tailExclusive = evidence.highWatermark();
                    if (tailExclusive <= applied.offset()) {
                        throw new IllegalStateException("guarded Source high watermark is behind the applied anchor");
                    }
                }
                for (var record : records.records(partition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, resource);
                    if (record.offset() >= tailExclusive) { break; }
                    if (record.value() == null || record.timestamp() < 0
                            || record.timestampType() != TimestampType.LOG_APPEND_TIME) {
                        throw new IllegalStateException("K1 backlog requires actual framed LogAppendTime records");
                    }
                    final var position = new KafkaSourcePosition(applied.shardId(), applied.authenticatedClusterId(),
                            applied.nativeTopicUuid(), record.offset(), record.leaderEpoch().orElse(null),
                            record.timestamp());
                    collector.add(position, record.value().length);
                    KafkaClientArtifactSourceRecordDecoder.decodeTarget(record.value(), applied.shardId(),
                            position, null, null);
                    last = position;
                }
                // The private cursor advances through every data frame/control marker in its guarded fetch.
                if (last != null && consumer.position(partition, Duration.ofNanos(collector.remainingNanos()))
                        >= tailExclusive) {
                    return collector.finish(last);
                }
            }
        }

        @Override
        public synchronized CompletableFuture<Void> requestClose() {
            closing = true;
            if (!openStarted && !owned.cleanupComplete()) { owned.abandonBeforeOpen(); }
            return owned.requestClose();
        }

        @Override
        public CompletableFuture<Void> retryClose() { return owned.retryClose(); }

        @Override
        public boolean cleanupComplete() { return owned.cleanupComplete(); }

        @Override
        public Throwable cleanupFailure() { return owned.cleanupFailure(); }
    }

}
