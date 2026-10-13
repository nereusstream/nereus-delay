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

    /** Completes this process root's actual Source startup and retains its original SDK owner on failure. */
    public static TargetWorkerShardRuntime createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard guard, String topic, Duration receiveTimeout,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        startup.registerBackend(group);
        try (var original = startup.beginWorkerConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph();
            final var worker = createVerifiedSource(group, consumer, guard, topic, receiveTimeout,
                    startup.activation(), graph.classes(), startup.store(), graph.resources(), maintenance, identity, time, clock);
            startup.admitWorker(worker);
            return worker;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Allocates the actual verified client and later receipt-enabled consumer on the original process graph. */
    public static Opening openVerifiedSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            PulsarClientArtifactOwnedBackendGroup.Settings settings, byte[] sdkJar,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
            TopicResourceGuard guard, String topic, String subscription, Duration receiveTimeout,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, com.nereusstream.delay.protocol.CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.LongSupplier clock) {
        try (var original = startup.beginWorkerConstruction()) {
            original.requireCurrent();
            startup.requireCurrent();
            Objects.requireNonNull(maintenance); Objects.requireNonNull(identity); Objects.requireNonNull(time); Objects.requireNonNull(clock);
            final var assignment = startup.activation().assignment();
            if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                    || !assignment.shardId().equals(receipt.authorization().shard()) || assignment.shardId().partition() < 0
                    || !topic.equals(barrier.physicalTopic())
                    || !Arrays.equals(guard.resourceIncarnation(), barrier.brokerResourceIncarnation())
                    || receiveTimeout == null || receiveTimeout.toMillis() < 1 || receiveTimeout.toMillis() > 60_000) {
                throw new IllegalArgumentException("P1 SDK opening changes its original accepted Source resource/time bound");
            }
            final var graph = startup.graph();
            final var group = PulsarClientArtifactOwnedBackendGroup.openVerifiedSourceMtls(settings, sdkJar, receipt,
                    time, startup::requireCurrent, budget, cost, graph.classes(), graph.resources(), graph.io());
            startup.registerBackend(group);
            startup.requireCurrent();
            return new Opening(startup, group, receipt, guard, topic, subscription, receiveTimeout, maintenance, identity, time, clock);
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    public static final class Opening {
        private final com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup;
        private final PulsarClientArtifactOwnedBackendGroup group;
        private final com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt;
        private final TopicResourceGuard guard;
        private final String topic;
        private final String subscription;
        private final Duration timeout;
        private final TargetWorkerShardRuntime.Maintenance maintenance;
        private final com.nereusstream.delay.protocol.OwnerIdentity identity;
        private final java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time;
        private final java.util.function.LongSupplier clock;
        private TargetWorkerShardRuntime worker;
        private boolean failed;
        private Opening(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
                PulsarClientArtifactOwnedBackendGroup group, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
                TopicResourceGuard guard, String topic, String subscription, Duration timeout,
                TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
                java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
                java.util.function.LongSupplier clock) {
            this.startup = startup; this.group = group; this.receipt = receipt; this.guard = guard;
            this.topic = topic; this.subscription = subscription; this.timeout = timeout;
            this.maintenance = maintenance; this.identity = identity; this.time = time; this.clock = clock;
        }
        /** Explicit driver turn; subscribe/native proof waits retain their original finite SDK bounds. */
        public synchronized java.util.Optional<TargetWorkerShardRuntime> advance() {
            if (failed) throw new IllegalStateException("original P1 Source opening failed");
            if (worker != null) return java.util.Optional.of(worker);
            try {
                startup.requireCurrent();
                if (!group.finishStartup()) return java.util.Optional.empty();
                try (var original = startup.beginWorkerConstruction()) {
                    original.requireCurrent();
                    final var consumer = PulsarClientArtifactSourceConsumerFactory.createVerifiedSource(
                            group, receipt, guard, topic, subscription, time);
                    final var graph = startup.graph();
                    worker = createVerifiedSource(group, consumer, guard, topic, timeout, startup.activation(),
                            graph.classes(), startup.store(), graph.resources(), maintenance, identity, time, clock);
                    startup.admitWorker(worker);
                }
                return java.util.Optional.of(worker);
            } catch (org.apache.pulsar.client.api.PulsarClientException | RuntimeException | Error failure) {
                failed = true; startup.retain(failure);
                if (failure instanceof RuntimeException runtime) throw runtime;
                if (failure instanceof Error fatal) throw fatal;
                throw new IllegalStateException("original P1 Source subscribe failed", failure);
            }
        }
    }

    /** Actual service path consumes only the repository's privately constructed Source activation. */
    public static TargetWorkerShardRuntime createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard guard, String topic, Duration receiveTimeout,
            com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Activated activation,
            WorkClassExecutionRegistry classes, ShardStore store, SharedRocksDbResources resources,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        activation.requireGraph(classes, resources, store);
        try { return createVerifiedSource(group, consumer, guard, topic, receiveTimeout, activation.assignment(),
                classes, store, resources, activation.runtime(), maintenance, identity, time, clock);
        } catch (RuntimeException | Error failure) {
            throw com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.sourceConstructionFailed(activation, failure);
        }
    }

    public static TargetWorkerShardRuntime createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard, String physicalTopic, Duration receiveTimeout,
            SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses, ShardStore store,
            SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime, TargetWorkerShardRuntime.Maintenance maintenance,
            com.nereusstream.delay.protocol.OwnerIdentity owner,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        final var worker = create(group, consumer, expectedGuard, physicalTopic, receiveTimeout, acceptedAssignment,
                workClasses, store, resources, sourceRuntime, maintenance);
        try { group.bindSourceCredential(consumer, worker, acceptedAssignment, owner, time, clock); return worker; }
        catch (RuntimeException | Error failure) {
            try { PulsarClientArtifactOwnedBackendGroup.closeConsumer(consumer).get(group.operationTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS); }
            catch (Exception cleanup) { if (cleanup instanceof InterruptedException) Thread.currentThread().interrupt(); failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Reject an unrelated/shared consumer before handing an actual owned group into the Worker graph. */
    public static TargetWorkerShardRuntime create(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard, String physicalTopic,
            Duration receiveTimeout, SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses,
            ShardStore store, SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime,
            TargetWorkerShardRuntime.Maintenance maintenance) {
        Objects.requireNonNull(group, "actual P1 group").requireWorkerGraph(workClasses, resources);
        if (PulsarClientArtifactOwnedBackendGroup.findNative(consumer) != group)
            throw new IllegalArgumentException("P1 Source consumer is not owned by this actual backend group");
        return create(consumer, expectedGuard, physicalTopic, receiveTimeout, acceptedAssignment, workClasses,
                store, resources, sourceRuntime, maintenance);
    }

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
            source = PulsarClientArtifactSourceRecordConsumer.forTarget(
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
