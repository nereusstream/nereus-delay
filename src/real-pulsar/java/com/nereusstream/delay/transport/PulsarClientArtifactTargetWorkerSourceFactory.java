package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardFactory;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

/** Binds the guarded Pulsar source to an already activated Target Worker runtime. */
public final class PulsarClientArtifactTargetWorkerSourceFactory {
    private PulsarClientArtifactTargetWorkerSourceFactory() {}

    /**
     * Creates one Target Worker source for the exact accepted Pulsar assignment. The caller must complete Store
     * bootstrap/reopen and Owner activation first; this factory never creates or authorizes a Target root.
     */
    public static TargetWorkerShardRuntime create(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final Duration receiveTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime sourceRuntime,
            final TargetWorkerShardRuntime.Maintenance maintenance) {
        final var exactConsumer = Objects.requireNonNull(consumer, "consumer");
        final var guard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        final var topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        final var assignment = Objects.requireNonNull(acceptedAssignment, "acceptedAssignment");
        final var runtime = Objects.requireNonNull(sourceRuntime, "sourceRuntime");
        runtime.requireAcceptedAssignment(assignment);
        if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)) {
            throw new IllegalArgumentException("Pulsar Target Worker requires a Pulsar activation barrier");
        }
        if (assignment.shardId().partition() < 0) {
            throw new IllegalArgumentException("Pulsar Target Worker partition must be non-negative");
        }
        if (!topic.equals(barrier.physicalTopic())
                || !Arrays.equals(barrier.brokerResourceIncarnation(), guard.resourceIncarnation())) {
            throw new IllegalArgumentException("Pulsar Target Worker differs from the activation resource identity");
        }
        requireCurrentGuardProof(exactConsumer, guard, barrier, topic, assignment.shardId().partition());

        final PulsarClientArtifactSourceRecordConsumer source;
        try {
            source = new PulsarClientArtifactSourceRecordConsumer(
                    exactConsumer, guard, assignment.shardId(), topic, Objects.requireNonNull(receiveTimeout));
        } catch (RuntimeException | Error failure) {
            closeConsumerAfterFailure(exactConsumer, failure);
            throw failure;
        }
        return TargetWorkerShardFactory.create(
                source, assignment, workClasses, store, resources, runtime, maintenance);
    }

    private static void requireCurrentGuardProof(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final PulsarActivationBarrier barrier,
            final String physicalTopic,
            final int partition) {
        if (!expectedGuard.equals(consumer.resourceGuard()) || !physicalTopic.equals(consumer.getTopic())) {
            throw new IllegalArgumentException("Pulsar Target Worker consumer is bound to a different topic");
        }
        if (consumer.connectionGeneration() != barrier.guardedSourceConnectionGeneration()) {
            throw new IllegalArgumentException("Pulsar Target Worker connection differs from activation barrier");
        }
        final TopicResourceGuardAttestation attestation = consumer.resourceGuardAttestation()
                .orElseThrow(() -> new IllegalStateException("Pulsar Target Worker has no current guard proof"));
        if (!expectedGuard.authenticatedClusterId().equals(attestation.authenticatedClusterId())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), attestation.resourceIncarnation())
                || !physicalTopic.equals(attestation.physicalTopic())
                || attestation.partition() != partition
                || !Bytes.constantTimeEquals(
                        barrier.resourceGuardAttestationDigest(),
                        PulsarClientArtifactSourceRecordConsumer.attestationDigest(attestation))) {
            throw new IllegalArgumentException("Pulsar Target Worker proof differs from activation barrier");
        }
    }

    private static void closeConsumerAfterFailure(final GuardedConsumer<byte[]> consumer, final Throwable failure) {
        try {
            consumer.close();
        } catch (Exception | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
