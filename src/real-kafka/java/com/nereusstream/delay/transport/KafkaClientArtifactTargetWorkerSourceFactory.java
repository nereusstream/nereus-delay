package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardFactory;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;

/** Binds the guarded Kafka source to an already activated Target Worker runtime. */
public final class KafkaClientArtifactTargetWorkerSourceFactory {
    private KafkaClientArtifactTargetWorkerSourceFactory() {}

    /**
     * Creates one Target Worker source for the exact accepted Kafka assignment. The caller must complete Store
     * bootstrap/reopen and Owner activation first; this factory never creates or authorizes a Target root.
     */
    public static TargetWorkerShardRuntime create(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String physicalTopic,
            final Duration pollTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime sourceRuntime,
            final TargetWorkerShardRuntime.Maintenance maintenance) {
        final var exactConsumer = Objects.requireNonNull(consumer, "consumer");
        final var assignment = Objects.requireNonNull(acceptedAssignment, "acceptedAssignment");
        final var runtime = Objects.requireNonNull(sourceRuntime, "sourceRuntime");
        runtime.requireAcceptedAssignment(assignment);
        final String topic = requirePhysicalTopic(physicalTopic);
        if (!(assignment.activationBarrier() instanceof KafkaActivationBarrier barrier)) {
            throw new IllegalArgumentException("Kafka Target Worker requires a Kafka activation barrier");
        }
        if (barrier.exclusiveOffset() < 0 || assignment.shardId().partition() < 0) {
            throw new IllegalArgumentException("Kafka Target Worker activation position is invalid");
        }
        final var expectedGuard = new ConsumerResourceGuard(
                barrier.authenticatedClusterId(),
                topic,
                new org.apache.kafka.common.Uuid(
                        barrier.nativeTopicUuid().getMostSignificantBits(),
                        barrier.nativeTopicUuid().getLeastSignificantBits()),
                assignment.shardId().partition());
        if (!expectedGuard.equals(exactConsumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka Target Worker consumer has a different resource guard");
        }

        final KafkaClientArtifactSourceRecordConsumer source;
        try {
            source = new KafkaClientArtifactSourceRecordConsumer(
                    exactConsumer,
                    barrier.authenticatedClusterId(),
                    barrier.nativeTopicUuid(),
                    assignment.shardId(),
                    topic,
                    Objects.requireNonNull(pollTimeout, "pollTimeout"));
            final TopicPartition topicPartition = new TopicPartition(topic, assignment.shardId().partition());
            exactConsumer.seek(topicPartition, barrier.exclusiveOffset());
        } catch (RuntimeException | Error failure) {
            closeConsumerAfterFailure(exactConsumer, failure);
            throw failure;
        }
        return TargetWorkerShardFactory.create(
                source, assignment, workClasses, store, resources, runtime, maintenance);
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private static void closeConsumerAfterFailure(
            final GuardedConsumer<byte[], byte[]> consumer, final Throwable failure) {
        try {
            consumer.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
