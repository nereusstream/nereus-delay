package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** One actual Source producer generation, including its partial construction and original append obligations. */
public final class KafkaClientArtifactOwnedSystemWriter implements OwnedSourceSystemWriter, TargetOwnedBackendGroup {
    public record Settings(String allocationId, int maximumBrokers, long bufferBytes, int socketBytes, Duration timeout) {
        public Settings {
            if (allocationId.isBlank() || maximumBrokers < 1 || maximumBrokers > 256 || bufferBytes < 1
                    || bufferBytes >= Integer.MAX_VALUE || socketBytes < 1 || timeout.toMillis() < 1000
                    || timeout.compareTo(Duration.ofSeconds(60)) > 0) throw new IllegalArgumentException("Source producer settings must be finite");
        }
    }
    private final Object gate = new Object();
    private final Object construction = new Object();
    private final TargetWorkerShardRuntime worker;
    private final WorkClassExecutionRegistry classes;
    private final SharedRocksDbResources resources;
    private final LocalBrokerMtlsResolver.SourceReceipt credential;
    private final TargetDestinationSdkBudget.Client allocation;
    private final SystemWriterAppendBudget.Writer writer;
    private final ThreadPoolExecutor io;
    private final Settings settings;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private final CompletableFuture<Void> firstClose = new CompletableFuture<>();
    private OwnedCheckpointIo<KafkaClientArtifactShardLogMutationAppender> opening;
    private OwnedCheckpointIo<Boolean> cleanup;
    private GuardedProducer<byte[],byte[]> producer;
    private SourceTransportCredentialUse credentialUse;
    private KafkaClientArtifactShardLogMutationAppender appender;
    private TargetWorkerHostRuntime host;
    private TargetWorkerHostRuntime.BackendClosePermit permit;
    private List<Object> frozenRegistrations;
    private CompletableFuture<Void> exposureCleanup;
    private boolean ready, frozen, sdkTerminated, cleanupStarting, exposureCleanupStarting;
    private int activeAppends;
    private Throwable failure;
    private TargetScopeExposureSource.CopyContract payloadSummaryCopies;
    private TargetPayloadSummaryInitializationAppendPermit payloadSummaryPermit;
    private SystemMutation payloadSummaryInput, lastPayloadSummaryInput;

    private KafkaClientArtifactOwnedSystemWriter(Settings settings, TargetWorkerShardRuntime worker,
            LocalBrokerMtlsResolver.SourceReceipt credential, TargetDestinationSdkBudget.Client allocation,
            SystemWriterAppendBudget.Writer writer, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io) {
        this.settings = settings; this.worker = worker; this.credential = credential; this.allocation = allocation;
        this.writer = writer; this.classes = classes; this.resources = resources; this.io = io;
    }

    public static KafkaClientArtifactOwnedSystemWriter open(Settings settings, Map<String,Object> operational,
            byte[] sdkJar, TargetWorkerRuntimeConfig configuration, TargetWorkerShardRuntime worker,
            SourceAssignment assignment, OwnerIdentity owner, LocalBrokerMtlsResolver.SourceReceipt credential,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier clock, TargetDestinationSdkBudget pool,
            CapacityVector cost, SystemWriterAppendBudget writers, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io) {
        return open(settings, operational, sdkJar, configuration, worker, assignment, owner, credential, time,
                clock, pool, cost, writers, classes, resources, io, null);
    }
    public static KafkaClientArtifactOwnedSystemWriter open(Settings settings, Map<String,Object> operational,
            byte[] sdkJar, TargetWorkerRuntimeConfig configuration, TargetWorkerShardRuntime worker,
            SourceAssignment assignment, OwnerIdentity owner, LocalBrokerMtlsResolver.SourceReceipt credential,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier clock, TargetDestinationSdkBudget pool,
            CapacityVector cost, SystemWriterAppendBudget writers, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io, TargetScopeExposureSource.Issuer exposureIssuer) {
        configuration.requireRuntime(classes, resources); worker.requireFleetComposition(classes, resources); pool.requireWorker(classes, resources);
        credential.authorization().requireAssignment(assignment);
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        final var auth = credential.authorization(); final var resource = auth.resource().kafka();
        final var config = new HashMap<>(KafkaClientArtifactMtlsConfiguration.applySource(operational, credential));
        if (config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG) || settings.bufferBytes() < configuration.maximumSourceFrameBytes())
            throw new IllegalArgumentException("Source system writer must serve the full nontransactional frame");
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = 1;
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = 2L * settings.maximumBrokers();
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = 2L * settings.maximumBrokers();
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = settings.bufferBytes();
        if (!cost.covers(new CapacityVector(minimum))) throw new IllegalArgumentException("Source producer cost does not cover its settings");
        final int millis = Math.toIntExact(settings.timeout().toMillis());
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.bufferBytes());
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Math.toIntExact(configuration.maximumSourceFrameBytes()));
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, settings.socketBytes()); config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, settings.socketBytes());
        config.put(ProducerConfig.ACKS_CONFIG, "all"); config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, 1);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); config.put(ProducerConfig.LINGER_MS_CONFIG, 0L);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) millis); config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, millis);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(10_000, millis)); config.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        final var sealed = Map.copyOf(config);
        final var checked = new HashMap<>(sealed);
        checked.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        checked.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        final var frozen = new ProducerConfig(checked);
        final byte[] activatedJar = Bytes.copy(sdkJar);
        TargetSdkArtifactIdentity.requireExact(KafkaProducer.class, activatedJar);
        final var copyContract = new TargetScopeExposureSource.CopyContract(2, Bytes.sha256(
                Bytes.utf8("nereus-k1-guarded-send-custody-v1\n"), activatedJar,
                Bytes.utf8("retries=" + frozen.getInt(ProducerConfig.RETRIES_CONFIG)
                        + "\nacks=" + frozen.getString(ProducerConfig.ACKS_CONFIG)
                        + "\nenable.idempotence=" + frozen.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)
                        + "\nmax.in.flight.requests.per.connection=" + frozen.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
                        + "\nmax.block.ms=" + frozen.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                        + "\ndelivery.timeout.ms=" + frozen.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)
                        + "\nrequest.timeout.ms=" + frozen.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG) + "\n")));
        final var allocation = pool.reserve(settings.allocationId(), cost);
        final SystemWriterAppendBudget.Writer writer;
        try { writer = writers.registerWriter(assignment.shardId()); }
        catch (RuntimeException | Error failed) { allocation.releaseUnstarted(); throw failed; }
        final var backend = new KafkaClientArtifactOwnedSystemWriter(settings, worker, credential, allocation, writer, classes, resources, io);
        backend.payloadSummaryCopies = copyContract;
        try {
            worker.registerSystemWriterBackend(settings.allocationId(), backend, pool, clock);
            backend.opening = OwnedCheckpointIo.submit(guard -> {
                guard.run(); worker.requirePublicationOwner(owner, clock);
                credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
                credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                allocation.beginConstruction();
                final var nativeProducer = new KafkaProducer<byte[],byte[]>(sealed, new ByteArraySerializer(), new ByteArraySerializer());
                synchronized (backend.gate) { backend.producer = nativeProducer; }
                writer.bindProducer(nativeProducer);
                final var uuid = resource.nativeTopicUuid();
                nativeProducer.requireResourceGuard(new ProducerResourceGuard(resource.authenticatedClusterId(), auth.physicalTopic(),
                        new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()), assignment.shardId().partition()));
                guard.run(); worker.requirePublicationOwner(owner, clock);
                backend.credentialUse = worker.bindSystemWriterCredential(credential, assignment, owner, nativeProducer, time, clock);
                return new KafkaClientArtifactShardLogMutationAppender(nativeProducer, assignment.shardId(), resource.authenticatedClusterId(),
                        auth.physicalTopic(), uuid, settings.timeout(), writer, backend::requireAppend, copyContract, exposureIssuer);
            }, () -> { synchronized (backend.gate) { if (backend.frozen) throw new IllegalStateException("Source producer opening is frozen"); } }, io);
            backend.opening.receipt().whenComplete((value, failed) -> {
                if (failed != null) backend.remember(CheckpointInputFuture.ownedFailure(backend.opening.receipt()));
                backend.cleanupProgress();
            });
            return backend;
        } catch (RuntimeException | Error failed) {
            backend.remember(failed);
            if (backend.opening == null) {
                allocation.releaseUnstarted(); writer.abandonUnstarted();
                synchronized (backend.gate) { backend.frozen = true; backend.sdkTerminated = true; }
                backend.firstClose.completeExceptionally(failed); backend.terminated.complete(null);
            } else {
                backend.freezeRegistrations();
                if (backend.host != null) backend.host.quarantineOwnedBackends(); else backend.advanceCleanup();
            }
            throw failed;
        }
    }

    public boolean finishStartup() {
        synchronized (gate) {
            if (frozen) throw new IllegalStateException("Source producer is frozen", openingFailure());
            if (!opening.operationExited()) return false;
            final var result = opening.receipt();
            if (!result.isDone()) return false;
            result.join();
            if (!opening.ioExited()) return false;
            appender = opening.completedValue().orElseThrow(() -> new IllegalStateException("Source producer opening failed", openingFailure()));
            ready = true; return true;
        }
    }
    private Throwable openingFailure() { return CheckpointInputFuture.ownedFailure(opening.receipt()); }
    private void requireAppend() {
        final SourceTransportCredentialUse use; final Object current;
        final TargetPayloadSummaryInitializationAppendPermit summary;
        final SystemMutation exactSummary;
        synchronized (gate) {
            if (!ready || frozen || host == null) throw new IllegalStateException("Source producer is unbound or frozen");
            use = credentialUse; current = producer;
            summary = payloadSummaryPermit; exactSummary = payloadSummaryInput;
        }
        try {
            use.require(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, current);
            if (summary != null) summary.beforeSdk(this, exactSummary, payloadSummaryCopies);
        }
        catch (RuntimeException | Error failed) {
            remember(failed);
            try { host.quarantineOwnedBackends(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failed) failed.addSuppressed(cleanup); }
            throw failed;
        }
    }
    @Override public AppendOutcome append(SystemMutation mutation) {
        if (TargetPayloadSummaryInitializationBody.isControl(mutation))
            throw new IllegalArgumentException("summary initialization requires its independent typed PLATFORM_OPERATOR SDK permit");
        final KafkaClientArtifactShardLogMutationAppender current;
        synchronized (gate) {
            if (!ready || frozen || host == null || payloadSummaryPermit != null) return AppendOutcome.definitelyNotPersisted();
            activeAppends++; current = appender;
        }
        try { return current.append(mutation); }
        finally { synchronized (gate) { activeAppends--; } cleanupProgress(); }
    }
    @Override public TargetScopeExposureSource.CopyContract payloadSummaryCopyContract() {
        synchronized (gate) {
            if (!ready || frozen || host == null) throw new IllegalStateException("original Source producer is not live");
            return payloadSummaryCopies;
        }
    }
    @Override public boolean payloadSummaryOperationsExited() {
        synchronized (gate) {
            if (activeAppends != 0) return false;
            if (lastPayloadSummaryInput != null && appender != null && !appender.nativeOperationsExited(lastPayloadSummaryInput)) return false;
            lastPayloadSummaryInput = null; return true;
        }
    }
    @Override public AppendOutcome appendPayloadSummary(SystemMutation input, TargetPayloadSummaryInitializationAppendPermit permit) {
        Objects.requireNonNull(permit); TargetPayloadSummaryInitializationBody.decode(input.canonicalBody()).requireInput(input);
        if (!TargetPayloadSummaryInitializationBody.isControl(input) || !worker.shardId().equals(input.shardId()))
            throw new IllegalArgumentException("summary initialization changed its actual Source Worker/purpose");
        final KafkaClientArtifactShardLogMutationAppender current;
        synchronized (gate) {
            if (!ready || frozen || host == null) return AppendOutcome.definitelyNotPersisted();
            if (activeAppends != 0 || payloadSummaryPermit != null
                    || lastPayloadSummaryInput != null && !appender.nativeOperationsExited(lastPayloadSummaryInput))
                throw new IllegalStateException("original Source native handoff is still owned");
            activeAppends = 1; payloadSummaryPermit = permit; payloadSummaryInput = input;
            lastPayloadSummaryInput = input; current = appender;
        }
        try { return current.append(input); }
        finally { synchronized (gate) { activeAppends = 0; payloadSummaryPermit = null; payloadSummaryInput = null; } cleanupProgress(); }
    }
    @Override public void requireWorkerGraph(WorkClassExecutionRegistry expected, SharedRocksDbResources shared) {
        if (classes != expected || resources != shared) throw new IllegalArgumentException("Source producer belongs to another Worker graph");
    }
    @Override public void requireHostComposition(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        worker.requireFleetComposition(classes, resources);
        synchronized (gate) { if (frozen || !fleet.contains(worker) || host != null && host != expected)
            throw new IllegalStateException("Source producer requires its actual Worker fleet"); }
    }
    @Override public void bindHost(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        requireHostComposition(expected, fleet); synchronized (gate) { host = expected; }
    }
    @Override public void admitWorker(TargetWorkerHostRuntime expected, TargetWorkerShardRuntime candidate) {
        synchronized (gate) { if (host != expected || candidate != worker || frozen) throw new IllegalStateException("Source writer cannot change Worker"); }
    }
    @Override public boolean applicationReady() { synchronized (gate) { return ready && host != null && !frozen; } }
    @Override public boolean workerReady(TargetWorkerShardRuntime expected) { return expected == worker && applicationReady(); }
    @Override public LocalBrokerMtlsResolver.SourceReceipt sourceCredentialReceipt() { return credential; }
    @Override public TargetWorkerShardRuntime sourceWriterWorker() { return worker; }
    @Override public TargetOwnedBackendGroup ownedBackendGroup() { return this; }
    @Override public boolean readyForAppending() { return workerReady(worker); }
    @Override public List<Object> freezeRegistrations() {
        synchronized (gate) {
            frozen = true;
            if (frozenRegistrations == null) frozenRegistrations = producer == null ? List.of(construction) : List.of(construction, producer);
        }
        writer.beginClose(); return frozenRegistrations;
    }
    @Override public CompletableFuture<Void> closeOwnedBackend(TargetWorkerHostRuntime.BackendClosePermit original) {
        original.requireOriginalGroup(this, frozenRegistrations);
        synchronized (gate) { if (permit != null && permit != original) throw new IllegalStateException("Source writer close permit changed"); permit = original; }
        advanceCleanup(); return terminated.copy();
    }
    public CompletableFuture<Void> closeBeforeHostBinding() {
        synchronized (gate) { if (host != null) return CompletableFuture.failedFuture(new IllegalStateException("Source writer requires its Host permit")); }
        freezeRegistrations(); advanceCleanup(); return terminated.copy();
    }
    public CompletableFuture<Void> firstCloseResult() { return firstClose.copy(); }
    public void retryCleanup() { synchronized (gate) { if (cleanup != null && !cleanup.ioExited()) return; } advanceCleanup(); }
    private void advanceCleanup() {
        final boolean nativeExited;
        synchronized (gate) { nativeExited = sdkTerminated; }
        if (nativeExited) { advanceExposureCleanup(true); finishCleanup(); return; }
        synchronized (gate) {
            if (!frozen || sdkTerminated || cleanupStarting || activeAppends != 0 || opening == null || !opening.ioExited()
                    || cleanup != null && !cleanup.ioExited() || host != null && permit == null) return;
            cleanupStarting = true;
            try {
                cleanup = OwnedCheckpointIo.submit(ignored -> {
                    try {
                        if (producer != null) {
                            producer.closeGuardedResources(settings.timeout());
                            if (!producer.guardedResourcesTerminated()) throw new IllegalStateException("Source producer native cleanup is incomplete");
                            final var current = appender == null ? opening.completedValue().orElse(null) : appender;
                            if (current != null) current.producerNativeClosed();
                            if (producer.guardedFirstCloseFailure() != null) firstClose.completeExceptionally(producer.guardedFirstCloseFailure());
                        } else if (openingFailure() instanceof KafkaProducer.ConstructionCleanupFailure partial) {
                            partial.reconcile(settings.timeout());
                            if (!partial.resourcesTerminated()) throw new IllegalStateException("Source producer partial construction remains live");
                            if (partial.firstCleanupFailure() != null) firstClose.completeExceptionally(partial.firstCleanupFailure());
                        } else allocation.releaseUnstarted();
                        synchronized (gate) { sdkTerminated = true; }
                        firstClose.complete(null); return true;
                    } catch (RuntimeException | Error failed) { firstClose.completeExceptionally(failed); remember(failed); throw failed; }
                }, () -> {}, io);
                cleanup.receipt().whenComplete((done, failed) -> { if (failed != null) remember(failed); cleanupProgress(); });
            } catch (RuntimeException | Error failed) { firstClose.completeExceptionally(failed); remember(failed); }
            finally { cleanupStarting = false; }
        }
    }
    private void cleanupProgress() {
        final boolean nativeExited;
        synchronized (gate) {
            nativeExited = sdkTerminated;
            if (!frozen || cleanupStarting || cleanup != null && !cleanup.ioExited()) return;
        }
        if (nativeExited) { advanceExposureCleanup(false); finishCleanup(); return; }
        // Retry remains explicit after a terminal cleanup failure; this only advances a late opening/appender exit.
        if (cleanup == null) { try { io.execute(this::advanceCleanup); } catch (RuntimeException failed) { remember(failed); } }
    }
    private void advanceExposureCleanup(boolean retryFailed) {
        final KafkaClientArtifactShardLogMutationAppender current;
        synchronized (gate) {
            if (!sdkTerminated || activeAppends != 0 || cleanup != null && !cleanup.ioExited()
                    || terminated.isDone() || exposureCleanupStarting
                    || exposureCleanup != null && (!exposureCleanup.isDone() || !retryFailed)) return;
            exposureCleanupStarting = true;
            current = appender == null ? opening.completedValue().orElse(null) : appender;
        }
        final CompletableFuture<Void> attempt;
        try { attempt = current == null ? CompletableFuture.completedFuture(null) : current.retryExposureCleanup(); }
        catch (RuntimeException | Error failed) {
            synchronized (gate) { exposureCleanupStarting = false; exposureCleanup = CompletableFuture.failedFuture(failed); }
            remember(failed); return;
        }
        synchronized (gate) { exposureCleanup = attempt; exposureCleanupStarting = false; }
        attempt.whenComplete((ignored, failed) -> {
            if (failed != null) remember(failed);
            finishCleanup();
        });
    }
    private void finishCleanup() {
        final KafkaClientArtifactShardLogMutationAppender current;
        synchronized (gate) {
            if (!sdkTerminated || activeAppends != 0 || cleanup == null || !cleanup.ioExited()
                    || exposureCleanupStarting || exposureCleanup == null || !exposureCleanup.isDone()
                    || exposureCleanup.isCompletedExceptionally() || terminated.isDone()) return;
            current = appender == null ? opening.completedValue().orElse(null) : appender;
        }
        if (current != null && !current.exposureLoansExited()) return;
        synchronized (gate) {
            if (terminated.isDone()) return;
            writer.producerClosed(); allocation.closed(); terminated.complete(null);
        }
    }
    private void remember(Throwable failed) { synchronized (gate) { if (failure == null) failure = failed; else if (failed != failure) failure.addSuppressed(failed); } }
    @Override public boolean resourcesTerminated() { return terminated.isDone() && !terminated.isCompletedExceptionally(); }
    @Override public boolean nativeResourcesTerminated() { synchronized (gate) { return sdkTerminated; } }
    @Override public Throwable cleanupFailure() { synchronized (gate) { return failure; } }
}
