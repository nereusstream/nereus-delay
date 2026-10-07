package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;

/** Exact K1 Admission history read using a dedicated manual-assignment consumer, with no offset commit path. */
public final class KafkaClientArtifactTargetAdmissionHistory {
    public static final int MAX_FRAME_BYTES = 2 * TargetPublishAdmissionBody.MAX_MATERIALIZED_BYTES + 2048;

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
        final var guard = Objects.requireNonNull(historyGuard, "historyGuard");
        if (!(ref.source() instanceof KafkaSourcePosition expected)) {
            throw new IllegalArgumentException("K1 history requires a Kafka Admission source");
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
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, MAX_FRAME_BYTES);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, MAX_FRAME_BYTES);
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
                    throw new IllegalStateException("K1 Admission history was unavailable before its deadline");
                }
                final var records = consumer.pollGuarded(Duration.ofNanos(remaining));
                final var evidence = KafkaClientArtifactFetchEvidence.requireBatch(records, resource);
                for (var record : records.records(partition)) {
                    KafkaClientArtifactFetchEvidence.requireRecord(record, evidence, resource);
                    if (record.offset() != expected.offset() || record.timestamp() < 0
                            || record.value() == null || record.value().length > MAX_FRAME_BYTES) {
                        throw new IllegalStateException("K1 history lacks the exact bounded retained Admission record");
                    }
                    final var source = new KafkaSourcePosition(expected.shardId(), expected.authenticatedClusterId(),
                            expected.nativeTopicUuid(), record.offset(), record.leaderEpoch().orElse(null),
                            record.timestamp());
                    final var decoded = KafkaClientArtifactSourceRecordDecoder.decode(
                            record.value(), expected.shardId(), source, null, null);
                    if (!(decoded instanceof SourceReplayMutation mutation)) {
                        throw new IllegalStateException("K1 Admission history returned a Command");
                    }
                    final var image = ref.requireImage(mutation.mutation(), source);
                    guard.run();
                    if (System.nanoTime() - started >= timeoutNanos) {
                        throw new IllegalStateException("K1 Admission history verification exceeded its deadline");
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
            if (failure == null) {
                throw closeFailure;
            }
            failure.addSuppressed(closeFailure);
        }
    }
}
