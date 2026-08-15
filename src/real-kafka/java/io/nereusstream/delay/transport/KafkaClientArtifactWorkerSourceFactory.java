package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.WorkerCommandRuntime;
import io.nereusstream.delay.ownership.WorkerSchedulingRuntime;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.WorkerCheckpointRuntime;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;

import java.security.PublicKey;
import java.time.Duration;
import java.util.Objects;

/**
 * Creates the Kafka-backed Worker source runtime after the accepted Route
 * assignment and activation barrier have been checked.
 *
 * <p>The caller owns the assignment/lease catch-up and activation protocol.
 * This factory is the post-activation composition boundary: it refuses a
 * stale local assignment, seeks the native cursor to the exact exclusive
 * Kafka barrier, and only then exposes the source to {@link WorkerShardRuntime}.
 * A failed composition closes the supplied Kafka consumer so a partially
 * admitted native source cannot outlive the failed Worker graph.</p>
 */
public final class KafkaClientArtifactWorkerSourceFactory {
    private KafkaClientArtifactWorkerSourceFactory() {
    }

    /** Creates one active Kafka source runtime for the exact accepted assignment. */
    public static WorkerShardRuntime create(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String physicalTopic,
            final Duration pollTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey) {
        return create(consumer, physicalTopic, pollTimeout, acceptedAssignment, workClasses, ownedShard, store,
                resources, authority, verificationKey, null, null, null);
    }

    /**
     * Creates one Kafka source runtime with the complete shared Worker graph.
     * The supplied scheduling, command and checkpoint runtimes must already be
     * built from the same registry, Store, resources and Owner composition;
     * {@link WorkerShardRuntime} repeats those identity checks before exposing
     * the source.
     */
    public static WorkerShardRuntime create(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String physicalTopic,
            final Duration pollTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final WorkerSchedulingRuntime schedulingRuntime,
            final WorkerCommandRuntime commandRuntime,
            final WorkerCheckpointRuntime checkpointRuntime) {
        Objects.requireNonNull(consumer, "consumer");
        final String topic = requirePhysicalTopic(physicalTopic);
        final SourceAssignment assignment = requireActiveAssignment(acceptedAssignment, ownedShard);
        if (!(assignment.activationBarrier() instanceof KafkaActivationBarrier barrier)) {
            throw new IllegalArgumentException("Kafka Worker source requires a Kafka activation barrier");
        }
        if (barrier.exclusiveOffset() < 0) {
            throw new IllegalArgumentException("Kafka activation barrier offset must be non-negative");
        }
        if (assignment.shardId().partition() < 0) {
            throw new IllegalArgumentException("Kafka Worker source partition must be non-negative");
        }
        final ConsumerResourceGuard expectedGuard = new ConsumerResourceGuard(barrier.authenticatedClusterId(),
                topic, new org.apache.kafka.common.Uuid(barrier.nativeTopicUuid().getMostSignificantBits(),
                        barrier.nativeTopicUuid().getLeastSignificantBits()), assignment.shardId().partition());
        if (!expectedGuard.equals(consumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka Worker source consumer has a different resource guard");
        }

        final KafkaClientArtifactSourceRecordConsumer source =
                new KafkaClientArtifactSourceRecordConsumer(consumer, barrier.authenticatedClusterId(),
                        barrier.nativeTopicUuid(), assignment.shardId(), topic, pollTimeout);
        try {
            final TopicPartition topicPartition = new TopicPartition(topic, assignment.shardId().partition());
            // assign() is performed by the source adapter. Kafka permits seek
            // on an assigned partition before the first poll, so the first
            // Worker turn starts exactly after the persisted Route barrier.
            consumer.seek(topicPartition, barrier.exclusiveOffset());
            return new WorkerShardRuntime(source, Objects.requireNonNull(workClasses, "workClasses"), ownedShard,
                    Objects.requireNonNull(store, "store"), Objects.requireNonNull(resources, "resources"),
                    Objects.requireNonNull(authority, "authority"), Objects.requireNonNull(verificationKey,
                            "verificationKey"), schedulingRuntime, commandRuntime, checkpointRuntime);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(source, failure);
            throw failure;
        }
    }

    private static SourceAssignment requireActiveAssignment(final SourceAssignment acceptedAssignment,
                                                            final OwnedDelayShard ownedShard) {
        final SourceAssignment assignment = Objects.requireNonNull(acceptedAssignment, "acceptedAssignment");
        final OwnedDelayShard owner = Objects.requireNonNull(ownedShard, "ownedShard");
        if (!assignment.sameIdentity(owner.sourceAssignment())) {
            throw new IllegalArgumentException("Kafka Worker assignment is not the Owner's accepted assignment");
        }
        if (owner.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Kafka Worker source requires an ACTIVE_FOR_COMMANDS shard");
        }
        return assignment;
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private static void closeAfterFailure(final KafkaClientArtifactSourceRecordConsumer source,
                                          final Throwable failure) {
        try {
            source.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
