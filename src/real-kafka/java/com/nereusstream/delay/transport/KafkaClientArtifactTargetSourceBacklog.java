package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetSourceBacklog;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/** Finite, no-commit K1 scan of actual frames from the protected applied anchor through a guarded high watermark. */
public final class KafkaClientArtifactTargetSourceBacklog {
    private KafkaClientArtifactTargetSourceBacklog() {}

    public static TargetSourceBacklog.Snapshot read(final Map<String, Object> configuration, final String physicalTopic,
            final TargetSourceBacklog.Cut cut, final TargetSourceBacklog.Limits limits, final Runnable protection) {
        if (!(Objects.requireNonNull(cut, "cut").applied() instanceof KafkaSourcePosition applied)
                || applied.offset() < 0) {
            throw new IllegalArgumentException("K1 backlog requires a retained native Kafka applied frontier");
        }
        final var collector = new TargetSourceBacklog.Collector(cut, limits, System::nanoTime, protection);
        final long timeoutMs = Duration.ofNanos(limits.elapsedNanos()).toMillis();
        if (timeoutMs < 1 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("K1 backlog deadline must fit a positive millisecond int");
        }
        final var config = new HashMap<>(Objects.requireNonNull(configuration, "configuration"));
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
        final var consumer = KafkaClientArtifactSourceConsumerFactory.create(config,
                applied.authenticatedClusterId(), physicalTopic, applied.nativeTopicUuid(), applied.shardId().partition());
        Throwable failure = null;
        try {
            final var partition = new TopicPartition(physicalTopic, applied.shardId().partition());
            consumer.assign(List.of(partition));
            consumer.seek(partition, applied.offset());
            final var resource = consumer.resourceGuard();
            long tailExclusive = -1;
            KafkaSourcePosition last = null;
            while (true) {
                final var records = consumer.pollGuarded(Duration.ofNanos(collector.remainingNanos()));
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
        } catch (RuntimeException | Error failed) {
            failure = failed;
            throw failed;
        } finally {
            try {
                consumer.close(CloseOptions.timeout(Duration.ofMillis(timeoutMs)));
            } catch (RuntimeException | Error closeFailure) {
                if (failure == null) { throw closeFailure; }
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
