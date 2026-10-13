package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Actual Source producer in the original shared P1 client; a producer close does not claim client termination. */
public final class PulsarClientArtifactOwnedSystemWriter implements OwnedSourceSystemWriter {
    private final Object gate = new Object();
    private final TargetWorkerShardRuntime worker;
    private final PulsarClientArtifactOwnedBackendGroup group;
    private final SystemWriterAppendBudget.Writer writer;
    private final TargetDestinationSdkBudget.Client allocation;
    private final CompletableFuture<Producer<byte[]>> creating = new CompletableFuture<>();
    private final OwnedSourceCursor<Producer<byte[]>> nativeOpening;
    private OwnedCheckpointIo<PulsarClientArtifactShardLogMutationAppender> opening;
    private PulsarClientArtifactProducerClose closing;
    private Producer<byte[]> producer;
    private SourceTransportCredentialUse credentialUse;
    private PulsarClientArtifactShardLogMutationAppender appender;
    private boolean ready, nativeClosed;
    private int activeAppends;

    private PulsarClientArtifactOwnedSystemWriter(TargetWorkerShardRuntime worker, PulsarClientArtifactOwnedBackendGroup group,
            SystemWriterAppendBudget.Writer writer, TargetDestinationSdkBudget.Client allocation, Duration timeout) {
        this.worker = worker; this.group = group; this.writer = writer; this.allocation = allocation;
        nativeOpening = new OwnedSourceCursor<>(creating, value -> {
            writer.beginClose();
            synchronized (gate) { if (closing == null) closing = new PulsarClientArtifactProducerClose(group.clientForProof(), value, timeout, allocation); }
            return closing.closeFuture().thenRun(this::releaseNative);
        }, group);
    }
    public static PulsarClientArtifactOwnedSystemWriter open(String allocationId, PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> source, TargetWorkerRuntimeConfig configuration, TargetWorkerShardRuntime worker,
            SourceAssignment assignment, OwnerIdentity owner, LocalBrokerMtlsResolver.SourceReceipt credential,
            Duration timeout, Supplier<TrustedUtcIntervalEvidence> time, LongSupplier clock, TargetDestinationSdkBudget pool,
            CapacityVector cost, SystemWriterAppendBudget writers, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io) {
        configuration.requireRuntime(classes, resources); worker.requireFleetComposition(classes, resources); pool.requireWorker(classes, resources);
        group.requireWorkerGraph(classes, resources);
        if (PulsarClientArtifactOwnedBackendGroup.findNative(source) != group || timeout.toMillis() < 1 || timeout.toMillis() > 60_000)
            throw new IllegalArgumentException("Source writer requires the original P1 consumer and finite timeout");
        credential.authorization().requireAssignment(assignment);
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        final var resource = credential.authorization().resource().pulsar();
        final var guard = new TopicResourceGuard(resource.authenticatedClusterId(), resource.resourceIncarnation(), resource.physicalTopicCreationTimestamp());
        group.requireSourceReadResource(credential, guard);
        if (!source.resourceGuard().equals(guard) || !source.getTopic().equals(resource.physicalTopic()))
            throw new IllegalArgumentException("P1 system writer changes its actual Source resource");
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = configuration.maximumSourceFrameBytes();
        if (!cost.covers(new CapacityVector(minimum))) throw new IllegalArgumentException("P1 system writer cannot serve the configured Source frame");
        final var allocation = pool.reserve(allocationId, cost);
        final SystemWriterAppendBudget.Writer writer;
        try { writer = writers.registerWriter(assignment.shardId()); }
        catch (RuntimeException | Error failed) { allocation.releaseUnstarted(); throw failed; }
        final var backend = new PulsarClientArtifactOwnedSystemWriter(worker, group, writer, allocation, timeout);
        final var client = group.nativeClientForFactory();
        try {
            group.registerCleanup(backend, () -> {
                synchronized (backend.gate) { return backend.activeAppends == 0 && backend.opening != null && backend.opening.ioExited(); }
            }, backend::resourcesTerminated, proof -> { proof.requireClient(client); writer.beginClose(); backend.nativeOpening.requestClose();
                backend.nativeOpening.backendShutdownCompleted(group); backend.releaseNative(); });
            worker.registerSystemWriterBackend(allocationId, backend, pool, clock);
            backend.opening = OwnedCheckpointIo.submit(active -> {
                active.run(); worker.requirePublicationOwner(owner, clock);
                credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
                credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                final var builder = PulsarClientArtifactProducerFactory.builder(client, resource.authenticatedClusterId(),
                        resource.resourceIncarnation(), resource.physicalTopic(), resource.physicalTopicCreationTimestamp(),
                        "nereus-source-system-" + java.util.UUID.randomUUID()).sendTimeout(Math.toIntExact(timeout.toMillis()), java.util.concurrent.TimeUnit.MILLISECONDS);
                PulsarClientArtifactOwnedBackendGroup.createProducer(client, builder, allocation).whenComplete((value, failed) -> {
                    if (failed == null) backend.creating.complete(value); else backend.creating.completeExceptionally(failed);
                });
                try { return backend.nativeOpening.read(timeout.toMillis(), value -> {
                    active.run(); worker.requirePublicationOwner(owner, clock); group.requireProducerArtifact(value);
                    if (!resource.physicalTopic().equals(value.getTopic())) throw new IllegalStateException("P1 Source producer changes its signed topic");
                    synchronized (backend.gate) { backend.producer = value; }
                    backend.credentialUse = worker.bindSystemWriterCredential(credential, assignment, owner, value, time, clock);
                    return new PulsarClientArtifactShardLogMutationAppender(value, source, assignment.shardId(), resource.authenticatedClusterId(),
                            resource.resourceIncarnation(), resource.physicalTopic(), resource.physicalTopicCreationTimestamp(), timeout, writer, backend::requireAppend);
                }); } catch (Exception failed) {
                    if (failed instanceof InterruptedException) Thread.currentThread().interrupt();
                    throw new IllegalStateException("Original P1 Source producer opening did not complete", failed);
                }
            }, () -> { credential.requireActive(time.get()); }, io);
            backend.opening.receipt().whenComplete((value, failed) -> {
                if (failed != null) {
                    backend.creating.completeExceptionally(failed);
                    try { allocation.releaseUnstarted(); writer.abandonUnstarted(); synchronized (backend.gate) { backend.nativeClosed = true; } }
                    catch (IllegalStateException started) { backend.nativeOpening.requestClose(); }
                }
                group.cleanupProgress();
            });
            return backend;
        } catch (RuntimeException | Error failed) {
            if (backend.opening == null) { allocation.releaseUnstarted(); writer.abandonUnstarted(); backend.nativeClosed = true; }
            else backend.nativeOpening.requestClose();
            throw failed;
        }
    }
    public boolean finishStartup() {
        synchronized (gate) {
            if (nativeClosed) throw new IllegalStateException("P1 Source writer is closed");
            if (!opening.ioExited()) return false;
            appender = opening.completedValue().orElseThrow(() -> new IllegalStateException("P1 Source writer opening failed", CheckpointInputFuture.ownedFailure(opening.receipt())));
            ready = true; return true;
        }
    }
    private void requireAppend() {
        final SourceTransportCredentialUse use; final Producer<byte[]> actual;
        synchronized (gate) { if (!ready || nativeClosed) throw new IllegalStateException("P1 Source producer is unavailable"); use = credentialUse; actual = producer; }
        try { use.require(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, actual); }
        catch (RuntimeException | Error failed) { try { group.requestHostQuarantine(); } catch (RuntimeException | Error cleanup) { failed.addSuppressed(cleanup); } throw failed; }
    }
    @Override public AppendOutcome append(SystemMutation mutation) {
        final PulsarClientArtifactShardLogMutationAppender current;
        synchronized (gate) { if (!ready || nativeClosed || !group.workerReady(worker)) return AppendOutcome.definitelyNotPersisted(); activeAppends++; current = appender; }
        try { return current.append(mutation); }
        finally { synchronized (gate) { activeAppends--; } group.cleanupProgress(); }
    }
    private void releaseNative() { synchronized (gate) { if (nativeClosed) return; writer.producerClosed(); allocation.closed(); nativeClosed = true; ready = false; } }
    @Override public TargetWorkerShardRuntime sourceWriterWorker() { return worker; }
    @Override public TargetOwnedBackendGroup ownedBackendGroup() { return group; }
    @Override public boolean readyForAppending() { synchronized (gate) { return ready && !nativeClosed && group.workerReady(worker); } }
    @Override public boolean resourcesTerminated() { synchronized (gate) { return nativeClosed; } }
}
