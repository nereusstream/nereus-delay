package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.OxiaOwnerLeaseStore;
import io.nereusstream.delay.ownership.OwnedDelayShard;
import io.nereusstream.delay.ownership.ShardLifecycleState;
import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.WorkerShardRuntime;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

import java.security.PublicKey;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * Creates the Pulsar-backed Worker source runtime after the accepted Route
 * assignment and guarded SUBSCRIBE proof have been checked.
 *
 * <p>The guarded consumer is already the P1 native capability boundary. This
 * factory binds its current resource incarnation, physical topic, partition,
 * attestation digest, and connection generation to the exact activation
 * barrier before the common Worker source loop can apply a record.</p>
 */
public final class PulsarClientArtifactWorkerSourceFactory {
    private PulsarClientArtifactWorkerSourceFactory() {
    }

    /** Creates one active Pulsar source runtime for the exact accepted assignment. */
    public static WorkerShardRuntime create(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final Duration receiveTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey) {
        Objects.requireNonNull(consumer, "consumer");
        final TopicResourceGuard guard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        final String topic = requirePhysicalTopic(physicalTopic);
        final SourceAssignment assignment = requireActiveAssignment(acceptedAssignment, ownedShard);
        if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)) {
            throw new IllegalArgumentException("Pulsar Worker source requires a Pulsar activation barrier");
        }
        if (assignment.shardId().partition() < 0) {
            throw new IllegalArgumentException("Pulsar Worker source partition must be non-negative");
        }
        if (!topic.equals(barrier.physicalTopic())
                || !Arrays.equals(barrier.brokerResourceIncarnation(), guard.resourceIncarnation())) {
            throw new IllegalArgumentException("Pulsar Worker source does not match the activation resource identity");
        }
        requireCurrentGuardProof(consumer, guard, barrier, topic, assignment.shardId().partition());

        final PulsarClientArtifactSourceRecordConsumer source =
                new PulsarClientArtifactSourceRecordConsumer(consumer, guard, assignment.shardId(), topic,
                        receiveTimeout);
        try {
            return new WorkerShardRuntime(source, Objects.requireNonNull(workClasses, "workClasses"), ownedShard,
                    Objects.requireNonNull(store, "store"), Objects.requireNonNull(resources, "resources"),
                    Objects.requireNonNull(authority, "authority"), Objects.requireNonNull(verificationKey,
                            "verificationKey"));
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
            throw new IllegalArgumentException("Pulsar Worker assignment is not the Owner's accepted assignment");
        }
        if (owner.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalStateException("Pulsar Worker source requires an ACTIVE_FOR_COMMANDS shard");
        }
        return assignment;
    }

    private static void requireCurrentGuardProof(final GuardedConsumer<byte[]> consumer,
                                                 final TopicResourceGuard expectedGuard,
                                                 final PulsarActivationBarrier barrier,
                                                 final String physicalTopic,
                                                 final int partition) {
        if (!expectedGuard.equals(consumer.resourceGuard()) || !physicalTopic.equals(consumer.getTopic())) {
            throw new IllegalArgumentException("Pulsar Worker consumer is bound to a different guarded topic");
        }
        if (consumer.connectionGeneration() != barrier.guardedSourceConnectionGeneration()) {
            throw new IllegalArgumentException("Pulsar Worker consumer generation differs from activation barrier");
        }
        final TopicResourceGuardAttestation attestation = consumer.resourceGuardAttestation()
                .orElseThrow(() -> new IllegalStateException("Pulsar Worker consumer has no current guard proof"));
        if (!expectedGuard.authenticatedClusterId().equals(attestation.authenticatedClusterId())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), attestation.resourceIncarnation())
                || !physicalTopic.equals(attestation.physicalTopic())
                || attestation.partition() != partition
                || !Bytes.constantTimeEquals(barrier.resourceGuardAttestationDigest(),
                        PulsarClientArtifactSourceRecordConsumer.attestationDigest(attestation))) {
            throw new IllegalArgumentException("Pulsar Worker consumer proof differs from activation barrier");
        }
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private static void closeAfterFailure(final PulsarClientArtifactSourceRecordConsumer source,
                                          final Throwable failure) {
        try {
            source.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
