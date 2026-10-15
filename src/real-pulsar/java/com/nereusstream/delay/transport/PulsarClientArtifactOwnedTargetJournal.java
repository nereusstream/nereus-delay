package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.adapter.TargetPulsarAttemptJournalRecordCodec;
import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.OwnedTargetJournal;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One actually opened/replayed managed Journal; its reservation survives until its shared SDK owner terminates. */
public final class PulsarClientArtifactOwnedTargetJournal implements OwnedTargetJournal {
    public record Configuration(String allocationId, PulsarJournalResource resource, String producerName,
            String replaySubscription, Duration timeout, int maximumRecords, long maximumRecordBytes) {
        public Configuration {
            Objects.requireNonNull(resource); Objects.requireNonNull(timeout);
            for (var value : new String[] {allocationId, producerName, replaySubscription}) {
                if (value == null || value.isBlank() || value.length() > 512) throw new IllegalArgumentException("finite Journal identity required");
            }
            if (timeout.toMillis() < 1 || timeout.toMillis() > 60_000 || maximumRecords < 1
                    || maximumRecords == Integer.MAX_VALUE || maximumRecordBytes < 1 || maximumRecordBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("managed Journal requires finite replay/close bounds");
            }
            Math.multiplyExact(3L, maximumRecordBytes);
        }
    }
    private final TargetWorkerSourceStartup startup;
    private final TargetWorkerShardRuntime worker;
    private final PulsarClientArtifactOwnedBackendGroup group;
    private final Configuration configuration;
    private final TargetDestinationSdkBudget.Client allocation;
    private volatile OwnedCheckpointIo<PulsarClientArtifactAttemptJournal> opening;
    private volatile PulsarClientArtifactAttemptJournal actual;
    private volatile boolean stopping, ready;
    private long closeStarted;
    private boolean closeRequested, quarantineRequested;

    private PulsarClientArtifactOwnedTargetJournal(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
            PulsarClientArtifactOwnedBackendGroup group, Configuration configuration, TargetDestinationSdkBudget.Client allocation) {
        this.startup = startup; this.worker = worker; this.group = group; this.configuration = configuration; this.allocation = allocation;
    }

    public static PulsarClientArtifactOwnedTargetJournal open(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
            PulsarClientArtifactOwnedBackendGroup group, Configuration configuration, TargetDestinationSdkBudget budget,
            CapacityVector cost, OwnerIdentity owner, PulsarClientArtifactAttemptJournal.TargetWriterGuard writerGuard,
            PulsarAttemptJournal.NegativeReplayAuthority originalNegativeReader,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier ownerClock) {
        Objects.requireNonNull(configuration); Objects.requireNonNull(writerGuard); Objects.requireNonNull(originalNegativeReader);
        Objects.requireNonNull(time); Objects.requireNonNull(ownerClock);
        try (var construction = startup.beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            final var graph = startup.graph();
            group.requireWorkerGraph(graph.classes(), graph.resources()); budget.requireWorker(graph.classes(), graph.resources());
            if (group.applicationReady()) throw new IllegalStateException("managed Journal opening requires its original pre-Host SDK assembly");
            worker.requirePublicationOwner(owner, ownerClock);
            startup.requireActiveJournalOpening(configuration.resource(), configuration.producerName());
            final var originalLease = startup.ownerLease();
            final var receipt = Objects.requireNonNull(group.credentialReceipt(), "actual managed Journal mTLS material");
            group.requireLoadedMaterial(receipt); receipt.requireActive(time.get());
            if (!configuration.resource().authenticatedClusterId().equals(group.authenticatedClusterId())
                    || configuration.resource().partition() != worker.shardId().partition()) {
                throw new IllegalArgumentException("Journal resource differs from its actual Worker/SDK cluster");
            }
            writerGuard.requireActive(worker.shardId(), configuration.resource());
            writerGuard.requireRecordVersion(worker.shardId(), configuration.resource(), TargetPulsarAttemptJournalRecordCodec.SETTLEMENT_VERSION);
            final long[] minimum = new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
            minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = Math.addExact(configuration.maximumRecords(), 2L);
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = Math.multiplyExact(3L, configuration.maximumRecordBytes());
            if (!cost.covers(new CapacityVector(minimum))) throw new IllegalArgumentException("Journal reservation cannot cover replay and native buffers");
            final var reserved = budget.reserve(configuration.allocationId(), cost);
            final var backend = new PulsarClientArtifactOwnedTargetJournal(startup, worker, group, configuration, reserved);
            try {
                final var client = group.nativeClientForFactory();
                group.registerCleanup(backend, backend::operationExited, reserved::released, proof -> {
                    proof.requireClient(client);
                    final var journal = backend.actual;
                    if (journal != null) journal.backendTerminated(proof);
                    backend.stopping = true; backend.ready = false; reserved.closed();
                });
                worker.registerJournalBackend(backend, budget, ownerClock);
                final var guarded = new PulsarClientArtifactAttemptJournal.TargetWriterGuard() {
                    private long lastNow = -1;
                    private void requireCurrentOwner() {
                        backend.requireOpening(); receipt.requireActive(time.get());
                        final var current = graph.oxia().providers().leases().current(worker.shardId()).orElseThrow();
                        final long now = ownerClock.getAsLong();
                        if (!originalLease.sameIdentity(current) || !current.validAt(now) || now < 0
                                || !com.nereusstream.delay.protocol.Bytes.constantTimeEquals(current.sessionIdentity(), graph.oxia().providers().sessionIdentity())
                                || current.state() != com.nereusstream.delay.ownership.ShardLifecycleState.ACTIVE_FOR_COMMANDS
                                    && current.state() != com.nereusstream.delay.ownership.ShardLifecycleState.DRAINING) {
                            throw new IllegalStateException("managed Journal lost its original actual Owner/session");
                        }
                        synchronized (this) {
                            if (now < lastNow) throw new IllegalStateException("managed Journal Owner clock regressed");
                            lastNow = now;
                        }
                    }
                    @Override public void requireActive(com.nereusstream.delay.protocol.ShardId shard, PulsarJournalResource resource) {
                        requireCurrentOwner(); writerGuard.requireActive(shard, resource);
                    }
                    @Override public void requireRecordVersion(com.nereusstream.delay.protocol.ShardId shard, PulsarJournalResource resource, int version) {
                        requireCurrentOwner(); writerGuard.requireRecordVersion(shard, resource, version);
                    }
                };
                backend.opening = OwnedCheckpointIo.submit(active -> {
                    active.run(); backend.requireOpening(); worker.requirePublicationOwner(owner, ownerClock);
                    receipt.requireActive(time.get()); reserved.beginConstruction();
                    try {
                        backend.actual = PulsarClientArtifactAttemptJournal.openTarget(client, worker.shardId(), configuration.resource(),
                                configuration.producerName(), configuration.replaySubscription(), configuration.timeout(),
                                configuration.maximumRecords(), configuration.maximumRecordBytes(), guarded, originalNegativeReader);
                    } catch (org.apache.pulsar.client.api.PulsarClientException failed) { throw new java.util.concurrent.CompletionException(failed); }
                    active.run(); worker.requirePublicationOwner(owner, ownerClock); receipt.requireActive(time.get());
                    return backend.actual;
                }, backend::requireOpening, graph.io());
                backend.opening.receipt().whenComplete((journal, failure) -> {
                    if (failure != null) {
                        startup.retain(failure);
                        try { reserved.releaseUnstarted(); } catch (IllegalStateException started) { /* original SDK termination still owns it */ }
                        group.requestHostQuarantine();
                    }
                    group.cleanupProgress();
                });
                construction.requireCurrent();
                return backend;
            } catch (RuntimeException | Error failure) {
                if (backend.opening == null) reserved.releaseUnstarted();
                startup.retain(failure); group.requestHostQuarantine(); throw failure;
            }
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }
    private void requireOpening() {
        if (stopping || allocation.released()) throw new IllegalStateException("original managed Journal is stopped");
    }
    private boolean operationExited() {
        final var original = opening;
        final var wire = actual;
        return (original == null ? allocation.released() : original.operationExited()) && (wire == null || wire.operationsExited());
    }
    public boolean finishStartup() {
        final var original = opening;
        if (original == null || !original.operationExited()) return false;
        final var result = original.receipt();
        if (!result.isDone()) return false;
        try {
            if (result.join() != actual) throw new IllegalStateException("Journal opening changed its original wire");
            requireOpening(); startup.requireAdmittedWorker(worker); ready = true; return true;
        } catch (RuntimeException | Error failure) { startup.retain(failure); group.requestHostQuarantine(); throw failure; }
    }
    @Override public TargetWorkerShardRuntime journalWorker() { return worker; }
    @Override public PulsarAttemptJournal journal() {
        if (!ready || stopping) throw new IllegalStateException("original managed Journal replay is not ready");
        return actual.journal();
    }
    @Override public PulsarClientArtifactOwnedBackendGroup ownedBackendGroup() { return group; }
    @Override public boolean readyForJournal() { return ready && !stopping && group.workerReady(worker); }
    @Override public boolean closed() {
        final var wire = actual;
        return allocation.released() || operationExited() && wire != null && wire.managedNativeClosed();
    }
    @Override public boolean settleClose() {
        stopping = true; ready = false;
        synchronized (this) { if (!closeRequested) { closeRequested = true; closeStarted = System.nanoTime(); } }
        final var original = opening;
        if (original != null) original.requestStop();
        final var wire = actual;
        if (wire != null) wire.requestManagedClose();
        if (closed()) return true;
        boolean quarantine = false;
        synchronized (this) {
            if (!quarantineRequested && System.nanoTime() - closeStarted >= configuration.timeout().toNanos()) {
                quarantineRequested = true; quarantine = true;
            }
        }
        if (quarantine) group.requestHostQuarantine();
        group.cleanupProgress();
        return false;
    }
}
