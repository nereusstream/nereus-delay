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
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;

/** Binds the guarded Kafka source to an already activated Target Worker runtime. */
public final class KafkaClientArtifactTargetWorkerSourceFactory {
    private KafkaClientArtifactTargetWorkerSourceFactory() {}

    /** Completes this process root's actual Source startup and retains its original SDK owner on failure. */
    public static TargetWorkerShardRuntime createVerifiedSource(KafkaClientArtifactOwnedSourceBackend owner,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        startup.registerBackend(owner);
        try (var original = startup.beginWorkerConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph();
            final var worker = createVerifiedSource(owner, startup.activation(), graph.classes(), startup.store(),
                    graph.resources(), maintenance, identity, time, clock);
            startup.admitWorker(worker);
            return worker;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Allocates the actual verified SDK Source on this startup's original finite process pool. */
    public static Opening openVerifiedSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            KafkaClientArtifactOwnedSourceBackend.Settings settings, java.util.Map<String, Object> operational,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, com.nereusstream.delay.protocol.CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.LongSupplier clock) {
        try (var original = startup.beginWorkerConstruction()) {
            original.requireCurrent();
            startup.requireCurrent();
            Objects.requireNonNull(maintenance); Objects.requireNonNull(identity); Objects.requireNonNull(time); Objects.requireNonNull(clock);
            final var assignment = startup.activation().assignment();
            if (!(assignment.activationBarrier() instanceof KafkaActivationBarrier barrier)
                    || !assignment.shardId().equals(settings.shard()) || assignment.shardId().partition() < 0
                    || !barrier.authenticatedClusterId().equals(settings.authenticatedClusterId())
                    || !barrier.nativeTopicUuid().equals(settings.topicId())) {
                throw new IllegalArgumentException("K1 SDK opening changes its original accepted Source resource");
            }
            final var graph = startup.graph();
            final var owner = KafkaClientArtifactOwnedSourceBackend.openVerifiedSourceMtls(settings, operational, sdkJar,
                    receipt, time, startup::requireCurrent, budget, cost, graph.classes(), graph.resources(), graph.io());
            startup.registerBackend(owner);
            startup.requireCurrent();
            return new Opening(startup, owner, maintenance, identity, time, clock);
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    public static final class Opening {
        private final com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup;
        private final KafkaClientArtifactOwnedSourceBackend owner;
        private final TargetWorkerShardRuntime.Maintenance maintenance;
        private final com.nereusstream.delay.protocol.OwnerIdentity identity;
        private final java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time;
        private final java.util.function.LongSupplier clock;
        private TargetWorkerShardRuntime worker;
        private boolean failed;
        private Opening(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
                KafkaClientArtifactOwnedSourceBackend owner, TargetWorkerShardRuntime.Maintenance maintenance,
                com.nereusstream.delay.protocol.OwnerIdentity identity,
                java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
                java.util.function.LongSupplier clock) {
            this.startup = startup; this.owner = owner; this.maintenance = maintenance;
            this.identity = identity; this.time = time; this.clock = clock;
        }
        /** Explicit driver turn; positioning/credential binding use the existing finite SDK timeout. */
        public synchronized java.util.Optional<TargetWorkerShardRuntime> advance() {
            if (failed) throw new IllegalStateException("original K1 Source opening failed");
            if (worker != null) return java.util.Optional.of(worker);
            try {
                startup.requireCurrent();
                if (!owner.finishStartup()) return java.util.Optional.empty();
                worker = createVerifiedSource(owner, startup, maintenance, identity, time, clock);
                return java.util.Optional.of(worker);
            } catch (RuntimeException | Error failure) { failed = true; startup.retain(failure); throw failure; }
        }
    }

    /** Actual service path consumes only the repository's privately constructed Source activation. */
    public static TargetWorkerShardRuntime createVerifiedSource(KafkaClientArtifactOwnedSourceBackend owner,
            com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Activated activation,
            WorkClassExecutionRegistry classes, ShardStore store, SharedRocksDbResources resources,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        activation.requireGraph(classes, resources, store);
        try {
            final var worker = createVerifiedSource(owner, activation.assignment(), classes, store, resources, activation.runtime(),
                    maintenance, identity, time, clock);
            worker.bindActualSourceSuccessor(activation);
            return worker;
        } catch (RuntimeException | Error failure) {
            throw com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.sourceConstructionFailed(activation, failure);
        }
    }

    public static TargetWorkerShardRuntime createVerifiedSource(KafkaClientArtifactOwnedSourceBackend owner,
            SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses, ShardStore store,
            SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        final var worker = create(owner, acceptedAssignment, workClasses, store, resources, sourceRuntime, maintenance);
        try { owner.bindSourceCredential(worker, acceptedAssignment, identity, time, clock); return worker; }
        catch (RuntimeException | Error failure) {
            try { owner.source().close(); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Actual owned Source startup uses the same native instance for positioning, Worker composition and teardown. */
    public static TargetWorkerShardRuntime create(KafkaClientArtifactOwnedSourceBackend owner,
            SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses, ShardStore store,
            SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime,
            TargetWorkerShardRuntime.Maintenance maintenance) {
        Objects.requireNonNull(sourceRuntime, "sourceRuntime").requireAcceptedAssignment(acceptedAssignment);
        Objects.requireNonNull(owner, "actual K1 Source owner").requireWorkerGraph(workClasses, resources);
        owner.positionAcceptedAssignment(acceptedAssignment);
        return TargetWorkerShardFactory.create(owner.source(), acceptedAssignment, workClasses, store,
                resources, sourceRuntime, maintenance);
    }

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
            source = KafkaClientArtifactSourceRecordConsumer.forTarget(
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
