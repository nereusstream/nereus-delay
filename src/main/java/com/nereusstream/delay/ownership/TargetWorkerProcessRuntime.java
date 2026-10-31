package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourceActivationBarrier;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter;
import com.nereusstream.delay.scheduler.OrdinaryObservationConfiguration;
import com.nereusstream.delay.scheduler.OrdinaryObservationContext;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.store.NativeResourceUsage;
import com.nereusstream.delay.store.RocksDbUsageLimits;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerNativeResourceLedger;
import io.oxia.client.api.Authentication;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Owns configured process resources; application assembly remains on the original Source/SDK factory paths. */
public final class TargetWorkerProcessRuntime {
    public record Limits(int ioThreads, int ioPendingTasks, long ioNativeBytes, long metricNativeBytes,
            BoundedAsyncMetricExporter.Limits metrics) {
        public Limits {
            Objects.requireNonNull(metrics, "metrics");
            if (ioThreads < 1 || ioThreads == Integer.MAX_VALUE || ioPendingTasks < 1
                    || ioPendingTasks == Integer.MAX_VALUE || ioNativeBytes < 1 || ioNativeBytes == Long.MAX_VALUE
                    || (long) ioThreads + ioPendingTasks < 3
                    || metricNativeBytes < 1 || metricNativeBytes == Long.MAX_VALUE
                    || metrics.maximumPendingRecords() == Integer.MAX_VALUE
                    || metrics.maximumPendingBytes() == Long.MAX_VALUE) {
                throw new IllegalArgumentException("process resources require explicit finite bounds");
            }
            Math.addExact(ioNativeBytes, metricNativeBytes);
        }
    }

    public record ResourceMonitoring(Duration runtimeInterval, Duration rocksDbInterval,
            RocksDbUsageLimits physicalUsage, long nativeBytes) {
        public ResourceMonitoring {
            Objects.requireNonNull(runtimeInterval); Objects.requireNonNull(rocksDbInterval);
            Objects.requireNonNull(physicalUsage);
            if (runtimeInterval.isNegative() || runtimeInterval.isZero()
                    || rocksDbInterval.isNegative() || rocksDbInterval.isZero()
                    || runtimeInterval.toNanos() == Long.MAX_VALUE || rocksDbInterval.toNanos() == Long.MAX_VALUE
                    || nativeBytes <= 0 || nativeBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("resource monitoring requires finite periods and an explicit native reservation");
            }
            for (long limit : new long[]{physicalUsage.maxWalBytesPerDb(), physicalUsage.maxTotalWalBytes(),
                    physicalUsage.maxManifestBytesPerDb(), physicalUsage.maxTotalManifestBytes(),
                    physicalUsage.maxLiveSstBytesPerDb(), physicalUsage.maxTotalLiveSstBytes(),
                    physicalUsage.maxLocalLiveDataBytes(), physicalUsage.minimumFilesystemFreeBytes(),
                    physicalUsage.maxCompactionPendingBytesPerDb(), physicalUsage.maxTotalCompactionPendingBytes()}) {
                if (limit == Long.MAX_VALUE) throw new IllegalArgumentException("physical usage byte limits must be finite");
            }
            for (int limit : new int[]{physicalUsage.maxWalFilesPerDb(), physicalUsage.maxTotalWalFiles(),
                    physicalUsage.maxManifestFilesPerDb(), physicalUsage.maxTotalManifestFiles(),
                    physicalUsage.maxSstFilesPerDb(), physicalUsage.maxTotalSstFiles(), physicalUsage.maxL0FilesPerDb()}) {
                if (limit == Integer.MAX_VALUE) throw new IllegalArgumentException("physical usage file limits must be finite");
            }
        }
    }

    public record Configuration(TargetWorkerRuntimeConfig worker, Limits process,
            TargetWorkerOxiaRuntime.Connection oxia, ResourceMonitoring monitoring,
            OrdinaryObservationConfiguration observation) {
        /** Component compatibility; the configured service requires the explicit typed observation configuration. */
        public Configuration(TargetWorkerRuntimeConfig worker, Limits process,
                TargetWorkerOxiaRuntime.Connection oxia, ResourceMonitoring monitoring) {
            this(worker, process, oxia, monitoring, null);
        }

        public Configuration {
            Objects.requireNonNull(worker); Objects.requireNonNull(process); Objects.requireNonNull(oxia);
            Objects.requireNonNull(monitoring);
            long[] fixed = new long[CapacityDimension.COUNT];
            fixed[CapacityDimension.OTHER_NATIVE_BYTES.wireValue() - 1] = Math.addExact(
                    Math.addExact(Math.addExact(process.ioNativeBytes(), process.metricNativeBytes()), monitoring.nativeBytes()),
                    Math.multiplyExact(3L, oxia.nativeBytesPerClient()));
            worker.requireCapacity(List.of(), new CapacityVector(fixed), CapacityVector.empty());
            final long metricHeap = metricRetainedHeapBytes(process, observation);
            if (metricHeap > worker.resources().certifiedJvmHeapBytes()
                    || observation != null && (observation.maximumSources() > worker.store().maxOwnedShards()
                        || process.metrics().maximumPendingBytes() < BoundedAsyncMetricExporter.OBSERVED_EVENT_PAYLOAD_BYTES)) {
                throw new IllegalArgumentException("ordinary telemetry exceeds its original process heap/Source/queue envelope");
            }
        }

        public long maximumMetricRetainedHeapBytes() { return metricRetainedHeapBytes(process, observation); }

        private static long metricRetainedHeapBytes(Limits process, OrdinaryObservationConfiguration observation) {
            return observation == null ? Math.addExact(4096L, Math.addExact(process.metrics().maximumPendingBytes(),
                    Math.multiplyExact(256L, process.metrics().maximumPendingRecords())))
                    : observation.maximumRetainedHeapBytes(process.metrics());
        }
    }

    /** This graph is borrowed only by original factories, and remains owned here until actual Host exit. */
    public record Assembly(TargetWorkerRuntimeConfig config, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io, TargetWorkerOxiaRuntime oxia) {}

    public record DrainResult(TargetWorkerHostRuntime.Result host, boolean processTerminated) {
        public boolean complete() { return host.complete() && processTerminated; }
    }

    private static final AtomicLong THREAD_IDS = new AtomicLong();
    private final Configuration configuration;
    private final LongSupplier nanos;
    private final List<Thread> ioThreads = new ArrayList<>();
    private final java.util.Map<com.nereusstream.delay.protocol.ShardId, TargetWorkerSourceStartup> sources =
            new java.util.LinkedHashMap<>();
    private final java.util.Set<TargetWorkerSourceStartup.Construction> sourceConstructions =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    /** Actual CONTROL receipt backing, kept after Worker users exit and before the original Oxia session closes. */
    public interface ReceiptManagement {
        byte[] identity();
        void requireGraph(WorkClassExecutionRegistry classes, SharedRocksDbResources resources);
        boolean ready();
        void requestStop();
        boolean closeForProcess(ReceiptManagementClosePermit permit);
        Throwable cleanupFailure();
        default void requireOriginalEvidenceCatalog(com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog catalog){
            throw new IllegalStateException("original receipt Root/Catalog binding is unavailable");
        }
    }
    private final java.util.Map<String, ReceiptManagement> receiptManagement = new java.util.LinkedHashMap<>();
    private final java.util.Set<ReceiptManagementConstruction> receiptConstructions =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final java.util.Map<ReceiptManagement, ReceiptManagementClosePermit> receiptClosePermits = new java.util.IdentityHashMap<>();
    private boolean receiptManagementFailed;
    private boolean receiptManagementClosing;
    private SharedRocksDbResources resources;
    private SharedRocksDbResources.NativeOpeningFailure partialSharedOpening;
    private WorkClassExecutionRegistry classes;
    private WorkerNativeResourceLedger.Reservation ioCost;
    private WorkerNativeResourceLedger.Reservation metricCost;
    private WorkerNativeResourceLedger.Reservation monitorCost;
    private ThreadPoolExecutor io;
    private BoundedAsyncMetricExporter metrics;
    private final java.util.UUID observationProcessInstance = java.util.UUID.randomUUID();
    private boolean ordinaryObservationContextInstalled;
    private int ordinaryObservationSourceCount;
    private SystemWriterAppendBudget systemWriters;
    private TargetDestinationSdkBudget destinationSdkBudget;
    private TargetWorkerPublicationDispatch publicationDispatch;
    private static final class CheckpointComposition {
        private final TargetWorkerCheckpointConfiguration configuration;
        private final TargetWorkerCheckpointComposition owner;
        private boolean heapReleased;
        private CheckpointComposition(TargetWorkerCheckpointConfiguration configuration,
                TargetWorkerCheckpointComposition owner) {
            this.configuration = configuration; this.owner = owner;
        }
    }
    private final java.util.Map<ShardId, CheckpointComposition> checkpointCompositions = new java.util.LinkedHashMap<>();
    private long checkpointInputBytes;
    private record ChannelInputs(TargetWorkerChannelInventory inventory, long bytes) {}
    private final java.util.Map<TargetWorkerSourceStartup, ChannelInputs> channelInputs = new java.util.IdentityHashMap<>();
    private long channelInputBytes;
    private final java.util.Map<TargetWorkerSourceStartup, ReclaimInputs> reclaimInputs = new java.util.IdentityHashMap<>();
    private long reclaimInputBytes;
    private final java.util.Map<TargetWorkerSourceStartup, ScopeInputs> scopeInputs = new java.util.IdentityHashMap<>();
    private long scopeInputBytes;
    private final java.util.Map<ShardId, TargetMigrationProcessDriver> migrations = new java.util.LinkedHashMap<>();
    private final java.util.Set<TargetMigrationProcessDriver> migrationReservations =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private long migrationInputBytes;
    private final java.util.Map<ShardId, MigrationMaintenanceAdmission> migrationMaintenance = new java.util.LinkedHashMap<>();
    private long migrationMaintenanceInputBytes;
    private int migrationMaintenanceRotation;
    private final java.util.Map<ShardId, PayloadSummaryAdmission> payloadSummaryMaintenance = new java.util.LinkedHashMap<>();
    private final java.util.Map<TargetWorkerSourceStartup, PayloadSummaryPrefixInputs> payloadSummaryPrefixes = new java.util.IdentityHashMap<>();
    private long payloadSummaryInputBytes, payloadSummaryPrefixInputBytes;
    private int payloadSummaryRotation;
    private final java.util.Map<TargetWorkerSourceStartup, WriterOriginInputs> writerOriginInputs = new java.util.IdentityHashMap<>();
    private long writerOriginInputBytes;
    private TargetWorkerOxiaRuntime oxia;
    private TargetWorkerHostRuntime host;
    private TargetWorkerHostRuntime.PreparationAttempt hostPreparation;
    private List<TargetWorkerShardRuntime> hostPreparationWorkers = List.of();
    private boolean hostPreparationStarted;
    private boolean hostPreparationRetired;
    private Throwable controlledStartupFailure;
    private boolean assemblyBorrowed;
    private boolean preparingHost;
    private boolean startupTurnActive;
    private boolean hostTransferred;
    private boolean stopping;
    private boolean dependenciesExited;
    private boolean resourcesClosed;
    private Throwable firstFailure;

    private TargetWorkerProcessRuntime(Configuration configuration, LongSupplier nanos) {
        this.configuration = Objects.requireNonNull(configuration);
        this.nanos = Objects.requireNonNull(nanos);
    }

    /** Loads the entire closed configuration before the runtime probe or any native allocation. */
    public static TargetWorkerProcessRuntime open(Path configuration, TargetWorkerOxiaRuntime.Registries registries,
            Authentication authentication, TrustedClock clock, LongSupplier nanos,
            BoundedAsyncMetricExporter.Exporter exporter, Runnable originalStartupGuard) throws IOException {
        return open(TargetWorkerRuntimeConfigLoader.loadService(configuration), registries, authentication,
                clock, nanos, exporter, originalStartupGuard);
    }

    public static TargetWorkerProcessRuntime open(Configuration configuration,
            TargetWorkerOxiaRuntime.Registries registries, Authentication authentication, TrustedClock clock,
            LongSupplier nanos, BoundedAsyncMetricExporter.Exporter exporter, Runnable originalStartupGuard) {
        Objects.requireNonNull(registries); Objects.requireNonNull(clock); Objects.requireNonNull(exporter);
        Objects.requireNonNull(originalStartupGuard);
        if (configuration.observation() != null) BoundedAsyncMetricExporter.requireObservationExporter(exporter);
        var root = new TargetWorkerProcessRuntime(configuration, nanos);
        try {
            originalStartupGuard.run();
            root.resources = configuration.worker().openResources();
            root.monitorCost = root.resources.reserveNativeResource("target-process-resource-monitors",
                    NativeResourceUsage.zero(), configuration.monitoring().nativeBytes());
            root.resources.startRuntimeResourceMonitor(configuration.monitoring().runtimeInterval());
            root.resources.startRocksDbUsageMonitor(configuration.monitoring().physicalUsage(),
                    configuration.monitoring().rocksDbInterval());
            root.classes = configuration.worker().newWorkClasses(nanos);
            root.ioCost = root.resources.reserveNativeResource("target-process-io", NativeResourceUsage.zero(),
                    configuration.process().ioNativeBytes());
            root.metricCost = root.resources.reserveNativeResource("target-process-metrics", NativeResourceUsage.zero(),
                    configuration.process().metricNativeBytes());
            root.io = new ThreadPoolExecutor(configuration.process().ioThreads(), configuration.process().ioThreads(),
                    0L, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<>(configuration.process().ioPendingTasks()),
                    root::newIoThread, new ThreadPoolExecutor.AbortPolicy());
            root.metrics = new BoundedAsyncMetricExporter(configuration.process().metrics(), exporter, configuration.observation());
            root.oxia = TargetWorkerOxiaRuntime.open(configuration.oxia(), registries, authentication,
                    configuration.worker(), root.classes, root.resources, root.io, clock, nanos, originalStartupGuard);
            return root;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof SharedRocksDbResources.NativeOpeningFailure partial) root.partialSharedOpening = partial;
            if (failure instanceof TargetWorkerOxiaRuntime.StartupFailure partial) root.oxia = partial.root();
            root.retain(failure);
            try { root.stopBeforeAssembly(); }
            catch (RuntimeException | Error cleanup) { root.retain(cleanup); }
            throw new StartupFailure(failure, root);
        }
    }

    private Thread newIoThread(Runnable work) {
        var thread = new Thread(work, "nereus-delay-process-io-" + THREAD_IDS.incrementAndGet());
        thread.setDaemon(true);
        synchronized (ioThreads) {
            // Only a terminated original Thread can be forgotten; a not-yet-started allocation stays owned.
            ioThreads.removeIf(prior -> prior.getState() == Thread.State.TERMINATED);
            ioThreads.add(thread);
        }
        return thread;
    }

    public boolean advanceStartup() {
        synchronized (this) { requireAssemblyAdmission(); }
        try { return oxia.advanceStartup(); }
        catch (RuntimeException | Error failure) {
            retain(failure);
            synchronized (this) { if (assemblyBorrowed) throw new StartupFailure(failure, this); }
            try { stopBeforeAssembly(); }
            catch (RuntimeException | Error cleanup) { retain(cleanup); }
            throw new StartupFailure(failure, this);
        }
    }

    public synchronized Assembly assembly() {
        requireAssemblyAdmission();
        oxia.providers();
        assemblyBorrowed = true;
        return new Assembly(configuration.worker(), classes, resources, io, oxia);
    }

    /** One destination window for this actual Process, shared by every controlled Source generation. */
    public synchronized TargetDestinationSdkBudget createDestinationSdkBudget(List<ShardCapacityEnvelope> shards,
            CapacityVector otherFixedCost, CapacityVector transitionDemand, CapacityVector limit, int maximumClients) {
        requireAssemblyAdmission(); oxia.providers();
        if (destinationSdkBudget != null) throw new IllegalStateException("original Process destination SDK budget already exists");
        destinationSdkBudget = new TargetDestinationSdkBudget(configuration.worker(), List.copyOf(shards),
                otherFixedCost, transitionDemand, limit, maximumClients, classes, resources);
        return destinationSdkBudget;
    }

    /** Registers the original local owner and heap charge before its first external preparation attempt. */
    public TargetWorkerCheckpointComposition openCheckpointComposition(TargetWorkerCheckpointConfiguration requested,
            TargetWorkerCheckpointComposition.TrustedDeployment deployment,
            TargetWorkerCheckpointComposition.Histories histories, Clock clock) {
        Objects.requireNonNull(requested); Objects.requireNonNull(deployment);
        Objects.requireNonNull(histories); Objects.requireNonNull(clock);
        final TargetWorkerCheckpointComposition original;
        synchronized (this) {
            requireAssemblyAdmission(); oxia.providers();
            final long next = Math.addExact(checkpointInputBytes, requested.retainedHeapBytes());
            if (assemblyBorrowed || checkpointCompositions.containsKey(requested.scope().shard())
                    || checkpointCompositions.size() >= configuration.worker().store().maxOwnedShards()
                    || Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes), scopeInputBytes), next)
                            > maximumInputHeapBytes())
                throw new IllegalStateException("checkpoint composition exceeds its original Process Shard/heap envelope");
            original = TargetWorkerCheckpointComposition.create(
                    new Assembly(configuration.worker(), classes, resources, io, oxia),
                    requested, deployment, histories, clock, nanos);
            checkpointCompositions.put(requested.scope().shard(), new CheckpointComposition(requested, original));
            checkpointInputBytes = next;
        }
        try { original.startPreparation(); return original; }
        catch (RuntimeException | Error failure) {
            retain(failure);
            try { original.close(); }
            catch (RuntimeException | Error cleanup) { retain(cleanup); }
            throw failure;
        }
    }

    synchronized void requireCheckpointComposition(TargetWorkerSourceStartup source,
            TargetWorkerCheckpointComposition original, byte[] recoveryLineageId) {
        requireSourceAssemblyAdmission(source);
        final var retained = checkpointCompositions.get(source.sourceScopeForExitBinding().shard());
        if (retained == null || retained.owner != original || retained.heapReleased
                || !retained.configuration.scope().equals(source.sourceScopeForExitBinding())
                || !Bytes.constantTimeEquals(retained.configuration.recoveryLineageId(), recoveryLineageId))
            throw new IllegalArgumentException("checkpoint composition differs from this original Process Source/lineage");
    }

    /** Opens the actual Target Store and commits the genuine initial grant before exposing a Source activation. */
    public TargetWorkerSourceStartup initializeSource(TargetWorkerSourceStartup.Owner owner,
            TargetWorkerSourceStartup.Initial initial, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor successor) {
        final TargetWorkerSourceStartup startup;
        synchronized (this) {
            requireAssemblyAdmission(); oxia.providers();
            requireMigrationAdopted(initial.scope().shard());
            if (sources.containsKey(initial.scope().shard()) || sources.size() >= configuration.worker().store().maxOwnedShards()
                    || initial.entry().mutation().canonicalEnvelope().length > configuration.worker().maximumSourceFrameBytes()) {
                throw new IllegalStateException("Source bootstrap exceeds its original process Shard/frame envelope");
            }
            startup = new TargetWorkerSourceStartup(this,
                    new Assembly(configuration.worker(), classes, resources, io, oxia), owner, initial, content, successor, nanos);
            sources.put(initial.scope().shard(), startup);
        }
        try { startup.initialize(); return startup; }
        catch (RuntimeException | Error failure) { retain(failure); throw failure; }
    }

    /** Same-DB reopen never bootstraps an empty root or bypasses the original Catalog/Pin checkpoint install gates. */
    public TargetWorkerSourceStartup reopenSource(TargetWorkerSourceStartup.Owner owner,
            com.nereusstream.delay.protocol.TargetQuotaScope scope, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor actualActiveSuccessor) {
        final TargetWorkerSourceStartup startup;
        synchronized (this) {
            requireAssemblyAdmission(); oxia.providers();
            requireMigrationAdopted(scope.shard());
            if (sources.containsKey(scope.shard()) || sources.size() >= configuration.worker().store().maxOwnedShards()) {
                throw new IllegalStateException("Source reopen exceeds its original process Shard envelope");
            }
            startup = new TargetWorkerSourceStartup(this,
                    new Assembly(configuration.worker(), classes, resources, io, oxia), owner, scope, content, actualActiveSuccessor, nanos);
            sources.put(scope.shard(), startup);
        }
        try { startup.openReopened(); return startup; }
        catch (RuntimeException | Error failure) { retain(failure); throw failure; }
    }

    /** Acquires the actual Source lease before creating its published-checkpoint pin or download/install request. */
    public TargetWorkerSourceStartup checkpointSource(TargetWorkerSourceStartup.Owner owner,
            com.nereusstream.delay.protocol.TargetQuotaScope scope, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor actualActiveSuccessor) {
        final TargetWorkerSourceStartup startup;
        synchronized (this) {
            requireAssemblyAdmission(); oxia.providers();
            requireMigrationAdopted(scope.shard());
            if (sources.containsKey(scope.shard()) || sources.size() >= configuration.worker().store().maxOwnedShards()) {
                throw new IllegalStateException("checkpoint Source exceeds its original process Shard envelope");
            }
            startup = new TargetWorkerSourceStartup(this,
                    new Assembly(configuration.worker(), classes, resources, io, oxia), owner, scope, content, actualActiveSuccessor, nanos);
            sources.put(scope.shard(), startup);
        }
        try { startup.acquireCheckpoint(); return startup; }
        catch (RuntimeException | Error failure) { retain(failure); throw failure; }
    }

    /** Drives original queued restore/recovery actions with the configured finite startup budget, including failed-stop settlement. */
    public List<com.nereusstream.delay.scheduler.WorkClassTask> runStartupWorkTurn() {
        synchronized (this) {
            if (host != null || preparingHost || startupTurnActive || dependenciesExited || resourcesClosed) {
                throw new IllegalStateException("original process startup work is no longer admitted");
            }
            startupTurnActive = true;
        }
        try { return classes.runTurn(configuration.worker().checkpoint().settlementTurn()); }
        finally { synchronized (this) { startupTurnActive = false; } }
    }

    /**
     * Freezes this original Process's actual ready Source frontiers before Host activation. The captured
     * Store cuts/assignment versions describe the boundary; they do not grant population or signed SLO coverage.
     */
    public void installOrdinaryObservationContext(LongSupplier ownerClock) {
        Objects.requireNonNull(ownerClock, "actual Owner clock");
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) {
            requireAssemblyAdmission();
            if (configuration.observation() == null || ordinaryObservationContextInstalled || !sourceConstructions.isEmpty()) {
                throw new IllegalStateException("configured ordinary observation needs its original ready pre-Host Sources");
            }
            original = List.copyOf(sources.values());
            configuration.observation().requireSourceCount(original.size());
        }
        final var cuts = new ArrayList<TargetSourceBacklog.Cut>(original.size());
        final var boundaries = new ArrayList<OrdinaryObservationContext.SourceBoundary>(original.size());
        long contextBytes = OrdinaryObservationContext.CANONICAL_HEADER_BYTES;
        for (var source : original) {
            source.requireCurrent();
            final var worker = source.workerForHost(this);
            final var activation = source.activation();
            activation.requireGraph(classes, resources, source.store());
            final var cut = worker.captureSourceBacklogCut(ownerClock);
            contextBytes = Math.addExact(contextBytes,
                    OrdinaryObservationContext.SourceBoundary.maximumCanonicalPayloadBytes(cut.applied()));
            if (contextBytes > configuration.observation().maximumContextBytes()) {
                throw new IllegalStateException("actual Source boundary exceeds its ordinary context byte reservation");
            }
            final var assignment = activation.assignment();
            final Long connectionGeneration = assignment.activationBarrier() instanceof PulsarActivationBarrier pulsar
                    ? pulsar.guardedSourceConnectionGeneration() : null;
            boundaries.add(new OrdinaryObservationContext.SourceBoundary(cut.applied(), assignment.assignmentEpoch(),
                    source.ownerLease().ownerEpoch(), connectionGeneration));
            cut.requireCurrent();
            cuts.add(cut);
        }
        for (var cut : cuts) cut.requireCurrent();
        final var context = new OrdinaryObservationContext(configuration.observation(), observationProcessInstance,
                configuration.process().metrics(), configuration.maximumMetricRetainedHeapBytes(),
                configuration.process().metricNativeBytes(), boundaries);
        synchronized (this) {
            requireAssemblyAdmission();
            if (ordinaryObservationContextInstalled || !original.equals(List.copyOf(sources.values()))
                    || !sourceConstructions.isEmpty()) {
                throw new IllegalStateException("original Source assembly changed during ordinary context capture");
            }
            metrics.installObservationContext(context);
            ordinaryObservationContextInstalled = true;
            ordinaryObservationSourceCount = original.size();
        }
    }

    /** The original metric queue/context charge shares the existing process input heap envelope. */
    private long maximumInputHeapBytes() {
        return Math.subtractExact(Math.subtractExact(Math.subtractExact(Math.subtractExact(Math.subtractExact(Math.subtractExact(
                configuration.worker().resources().certifiedJvmHeapBytes(), configuration.maximumMetricRetainedHeapBytes()),
                migrationInputBytes), migrationMaintenanceInputBytes), payloadSummaryInputBytes), payloadSummaryPrefixInputBytes), writerOriginInputBytes);
    }
    synchronized WriterOriginInputs reserveWriterOrigin(TargetWorkerSourceStartup source, long bytes) {
        requireSourceAssemblyAdmission(source);
        if (bytes < 1 || bytes == Long.MAX_VALUE || writerOriginInputs.containsKey(source)
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes), scopeInputBytes),
                        checkpointInputBytes), bytes) > maximumInputHeapBytes())
            throw new IllegalStateException("actual Source writer preparation window exceeds its original Process input envelope");
        final var token = new WriterOriginInputs(this, source, bytes); writerOriginInputs.put(source, token);
        writerOriginInputBytes = Math.addExact(writerOriginInputBytes, bytes); return token;
    }
    static final class WriterOriginInputs {
        private final TargetWorkerProcessRuntime process; private final TargetWorkerSourceStartup source; private final long bytes;
        private WriterOriginInputs(TargetWorkerProcessRuntime process, TargetWorkerSourceStartup source, long bytes) {
            this.process = process; this.source = source; this.bytes = bytes;
        }
        void release() {
            synchronized (process) {
                if (!process.writerOriginInputs.remove(source, this)) throw new IllegalStateException("writer history preparation changed its original owner");
                process.writerOriginInputBytes = Math.subtractExact(process.writerOriginInputBytes, bytes);
            }
        }
    }

    synchronized PayloadSummaryPrefixInputs reservePayloadSummaryPrefix(TargetWorkerSourceStartup source, long bytes) {
        requireSourceAssemblyAdmission(source);
        if (bytes < 1 || bytes == Long.MAX_VALUE || payloadSummaryPrefixes.containsKey(source)
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes), scopeInputBytes),
                        checkpointInputBytes), bytes) > maximumInputHeapBytes())
            throw new IllegalStateException("original controlled prefix exceeds the actual Process retained-input envelope");
        final var original = new PayloadSummaryPrefixInputs(this, source, bytes);
        payloadSummaryPrefixes.put(source, original); payloadSummaryPrefixInputBytes = Math.addExact(payloadSummaryPrefixInputBytes, bytes);
        return original;
    }
    static final class PayloadSummaryPrefixInputs {
        private final TargetWorkerProcessRuntime process;
        private final TargetWorkerSourceStartup source;
        private final long bytes;
        private boolean released;
        private PayloadSummaryPrefixInputs(TargetWorkerProcessRuntime process, TargetWorkerSourceStartup source, long bytes) {
            this.process = process; this.source = source; this.bytes = bytes;
        }
        void requireCurrent() {
            synchronized (process) {
                if (released || process.payloadSummaryPrefixes.get(source) != this)
                    throw new IllegalStateException("actual controlled prefix input share was released");
            }
        }
        void release() {
            synchronized (process) {
                if (released || !process.payloadSummaryPrefixes.remove(source, this))
                    throw new IllegalStateException("controlled prefix release changed its original Source owner");
                released = true; process.payloadSummaryPrefixInputBytes = Math.subtractExact(process.payloadSummaryPrefixInputBytes, bytes);
            }
        }
    }

    synchronized TargetPayloadSummaryInitializationMaintenance configurePayloadSummaryInitialization(TargetWorkerSourceStartup source,
            TargetWorkerShardRuntime worker, OwnedSourceSystemWriter writer,
            TargetPayloadSummaryInitializationMaintenance.Configuration requested,
            TargetPayloadSummaryInitializationMaintenance.Inputs.Provider provider) {
        requireSourceAssemblyAdmission(source); source.requireAdmittedWorker(worker);
        if (sources.get(requested.scope().shard()) != source || !source.sourceScopeForExitBinding().equals(requested.scope())
                || writer.sourceWriterWorker() != worker || payloadSummaryMaintenance.containsKey(requested.scope().shard())
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes), scopeInputBytes),
                        checkpointInputBytes), requested.maximumRetainedBytes()) > maximumInputHeapBytes())
            throw new IllegalStateException("summary initialization lacks actual Process/Worker/Source-writer/finite input ownership");
        final var original = new PayloadSummaryAdmission(this, source, worker, writer, requested);
        final var actual = new TargetPayloadSummaryInitializationMaintenance(original, requested, provider, nanos);
        original.maintenance = actual; payloadSummaryMaintenance.put(requested.scope().shard(), original);
        payloadSummaryInputBytes = Math.addExact(payloadSummaryInputBytes, requested.maximumRetainedBytes()); return actual;
    }
    private static final class PayloadSummaryAdmission implements TargetPayloadSummaryInitializationMaintenance.Admission {
        private final TargetWorkerProcessRuntime process;
        private final TargetWorkerSourceStartup source;
        private final TargetWorkerShardRuntime worker;
        private final OwnedSourceSystemWriter writer;
        private final TargetPayloadSummaryInitializationMaintenance.Configuration configuration;
        private final com.nereusstream.delay.store.TargetStoreBackend backend;
        private final OwnerIdentity identity;
        private final Runnable localOwner;
        private TargetPayloadSummaryInitializationMaintenance maintenance;
        private boolean released;
        PayloadSummaryAdmission(TargetWorkerProcessRuntime process, TargetWorkerSourceStartup source,
                TargetWorkerShardRuntime worker, OwnedSourceSystemWriter writer,
                TargetPayloadSummaryInitializationMaintenance.Configuration configuration) {
            this.process = process; this.source = source; this.worker = worker; this.writer = writer; this.configuration = configuration;
            backend = source.migrationMaintenanceBackend(worker); identity = source.migrationMaintenanceOwner(worker);
            localOwner = source.managementMaintenanceLocalToken(worker, identity);
        }
        @Override public Assembly graph() { return source.graph(); }
        @Override public TargetWorkerShardRuntime worker() { return worker; }
        @Override public com.nereusstream.delay.store.ShardStore store() { return source.store(); }
        @Override public com.nereusstream.delay.store.TargetStoreBackend backend() { return backend; }
        @Override public com.nereusstream.delay.store.TargetStoreBackend.ReadAuthority reads() { return source.payloadSummaryMaintenanceReads(maintenance, worker); }
        @Override public OwnedSourceSystemWriter writer() { return writer; }
        @Override public OwnerIdentity ownerIdentity() { return identity; }
        @Override public long reservedBytes() { return configuration.maximumRetainedBytes(); }
        @Override public void requireCurrent(TargetPayloadSummaryInitializationMaintenance actual) {
            process.requirePayloadSummaryMaintenance(source, worker, actual); source.requirePayloadSummaryMaintenanceCurrent(actual, worker); localOwner.run();
        }
        @Override public void requireLocalCurrent(TargetPayloadSummaryInitializationMaintenance actual) {
            process.requirePayloadSummaryMaintenance(source, worker, actual); localOwner.run();
        }
        @Override public void release(TargetPayloadSummaryInitializationMaintenance actual) {
            synchronized (process) {
                if (actual != maintenance || released || process.payloadSummaryMaintenance.get(configuration.scope().shard()) != this)
                    throw new IllegalStateException("summary initialization release changed its original Process owner");
                released = true; process.payloadSummaryInputBytes = Math.subtractExact(process.payloadSummaryInputBytes, configuration.maximumRetainedBytes());
            }
        }
    }
    void requirePayloadSummaryMaintenance(TargetWorkerSourceStartup source, TargetWorkerShardRuntime worker,
            TargetPayloadSummaryInitializationMaintenance actual) {
        final TargetWorkerHostRuntime current;
        synchronized (this) {
            final var original = payloadSummaryMaintenance.get(source.sourceScopeForExitBinding().shard());
            if (stopping || resourcesClosed || controlledStartupFailure != null || receiptManagementFailed
                    || original == null || original.source != source || original.worker != worker || original.maintenance != actual
                    || original.released || sources.get(worker.shardId()) != source
                    || host == null || !hostTransferred)
                throw new IllegalStateException("platform summary initialization lost its actual Process/Source/Host input ownership");
            current = host;
        }
        if (!current.migrationMaintenanceAdmitted(worker)) throw new IllegalStateException("Host no longer admits this actual platform upgrade Worker");
    }
    void runNextPayloadSummaryMaintenanceTurn() {
        final PayloadSummaryAdmission next;
        final TargetWorkerHostRuntime current;
        synchronized (this) {
            if (stopping || host == null || !hostTransferred || payloadSummaryMaintenance.isEmpty()) return;
            final var all = new ArrayList<>(payloadSummaryMaintenance.values());
            next = all.get(Math.floorMod(payloadSummaryRotation++, all.size())); current = host;
        }
        if (!next.maintenance.ioExited() && current.migrationMaintenanceAdmitted(next.worker)) next.maintenance.runTurn();
    }

    /** Registers finite live custody before Host transfer; its SDK acquisition occurs on ordinary maintenance I/O. */
    synchronized TargetMigrationMaintenance configureMigrationMaintenance(TargetWorkerSourceStartup source,
            TargetWorkerShardRuntime worker, TargetMigrationMaintenanceConfiguration requested,
            TargetMigrationMaintenanceInputs.Provider provider, TargetDestinationSdkBudget originalSdk,
            java.util.function.Function<TargetMigrationSourceOpening, TargetMigrationTransport> transport) {
        requireSourceAssemblyAdmission(source); source.requireAdmittedWorker(worker); Objects.requireNonNull(requested);
        final var shard = requested.migration().scope().shard();
        if (sources.get(shard) != source || originalSdk != destinationSdkBudget || migrationMaintenance.containsKey(shard)
                || !requested.migration().scope().equals(source.sourceScopeForExitBinding())
                || migrationMaintenance.size() >= configuration.worker().store().maxOwnedShards()
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes), scopeInputBytes),
                        checkpointInputBytes), requested.maximumRetainedBytes()) > maximumInputHeapBytes())
            throw new IllegalStateException("live migration lacks its original Process/Source/SDK/input reservation");
        originalSdk.requireWorker(classes, resources);
        if (systemWriters == null) systemWriters = configuration.worker().newSystemWriterBudget(classes, resources);
        final var admitted = new MigrationMaintenanceAdmission(this, source, worker, requested, originalSdk, systemWriters);
        final var actual = new TargetMigrationMaintenance(admitted, requested, provider, transport, nanos);
        admitted.maintenance = actual; migrationMaintenance.put(shard, admitted);
        migrationMaintenanceInputBytes = Math.addExact(migrationMaintenanceInputBytes, requested.maximumRetainedBytes());
        return actual;
    }

    private static final class MigrationMaintenanceAdmission implements TargetMigrationMaintenance.Admission {
        private final TargetWorkerProcessRuntime process;
        private final TargetWorkerSourceStartup source;
        private final TargetWorkerShardRuntime worker;
        private final TargetMigrationMaintenanceConfiguration configuration;
        private final TargetDestinationSdkBudget sdk;
        private final SystemWriterAppendBudget writers;
        private final com.nereusstream.delay.store.TargetStoreBackend backend;
        private final OwnerIdentity identity;
        private final Runnable localOwner;
        private TargetMigrationMaintenance maintenance;
        private Thread construction;
        private boolean released;
        private MigrationMaintenanceAdmission(TargetWorkerProcessRuntime process, TargetWorkerSourceStartup source,
                TargetWorkerShardRuntime worker, TargetMigrationMaintenanceConfiguration configuration,
                TargetDestinationSdkBudget sdk, SystemWriterAppendBudget writers) {
            this.process = process; this.source = source; this.worker = worker; this.configuration = configuration;
            this.sdk = sdk; this.writers = writers;
            backend = source.migrationMaintenanceBackend(worker);
            identity = source.migrationMaintenanceOwner(worker);
            localOwner = source.managementMaintenanceLocalToken(worker, identity);
        }
        @Override public Assembly graph() { return source.graph(); }
        @Override public TargetWorkerSourceStartup startup() { return source; }
        @Override public TargetWorkerShardRuntime worker() { return worker; }
        @Override public com.nereusstream.delay.store.TargetStoreBackend backend() { return backend; }
        @Override public com.nereusstream.delay.store.TargetStoreBackend.ReadAuthority reads() { return source.migrationMaintenanceReads(maintenance, worker); }
        @Override public TargetDestinationSdkBudget sdkBudget() { return sdk; }
        @Override public SystemWriterAppendBudget systemWriters() { return writers; }
        @Override public OwnerIdentity ownerIdentity() { return identity; }
        @Override public LongSupplier clock() { return source.migrationMaintenanceClock(); }
        @Override public com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.Held openArchive(
                TargetMigrationMaintenance actual, TargetMigrationMaintenance.Opening opening,
                com.nereusstream.delay.runtime.TargetMigrationRetirementStore.AppliedIntent intent,
                com.nereusstream.delay.runtime.TargetMigrationArchivePreparation.Limits limits,
                com.nereusstream.delay.store.BoundedReadBudget cumulative) {
            requireCurrent(actual);
            return process.migrationForMaintenance(this).openArchiveHold(opening, intent, limits, cumulative);
        }
        @Override public boolean exitArchive(TargetMigrationMaintenance actual,
                com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover takeover) {
            requireCurrent(actual);
            return process.migrationForMaintenance(this).exitAfterArchiveTakeover(takeover, source.activation().runtime());
        }
        @Override public boolean archiveActive(TargetMigrationMaintenance actual,
                com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover takeover) {
            requireLocalCurrent(actual);
            return process.migrationForMaintenance(this).archiveTakeoverActive(takeover, source.activation().runtime());
        }
        @Override public long reservedBytes() { return configuration.maximumRetainedBytes(); }
        @Override public void requireCurrent(TargetMigrationMaintenance actual) {
            process.requireMigrationMaintenance(this, actual); source.requireMigrationMaintenanceCurrent(actual, worker); localOwner.run();
        }
        @Override public void requireLocalCurrent(TargetMigrationMaintenance actual) {
            process.requireMigrationMaintenance(this, actual); localOwner.run();
        }
        @Override public TargetMigrationMaintenance.Construction beginConstruction(TargetMigrationMaintenance actual) {
            requireCurrent(actual);
            synchronized (this) {
                if (construction != null) throw new IllegalStateException("original live SDK acquisition is already owned");
                construction = Thread.currentThread();
            }
            return () -> {
                synchronized (MigrationMaintenanceAdmission.this) {
                    if (construction != Thread.currentThread()) throw new IllegalStateException("live SDK acquisition changed its actual I/O owner");
                    construction = null;
                }
            };
        }
        @Override public void release(TargetMigrationMaintenance actual) {
            synchronized (process) {
                if (maintenance != actual || released || construction != null
                        || process.migrationMaintenance.get(configuration.migration().scope().shard()) != this)
                    throw new IllegalStateException("live migration input release changed its original Process acquisition");
                released = true;
                process.migrationMaintenanceInputBytes = Math.subtractExact(process.migrationMaintenanceInputBytes, configuration.maximumRetainedBytes());
            }
        }
    }

    private void requireMigrationMaintenance(MigrationMaintenanceAdmission original, TargetMigrationMaintenance actual) {
        final TargetWorkerHostRuntime current;
        synchronized (this) {
            if (stopping || resourcesClosed || controlledStartupFailure != null || receiptManagementFailed
                    || original.released || original.maintenance != actual
                    || migrationMaintenance.get(original.configuration.migration().scope().shard()) != original
                    || sources.get(original.configuration.migration().scope().shard()) != original.source
                    || !hostTransferred || host == null)
                throw new IllegalStateException("live migration lost its actual Process/Host/Source/input ownership");
            current = host;
        }
        if (!current.migrationMaintenanceAdmitted(original.worker))
            throw new IllegalStateException("original Host no longer admits this live migration Worker");
    }
    private synchronized TargetMigrationProcessDriver migrationForMaintenance(MigrationMaintenanceAdmission original) {
        if (migrationMaintenance.get(original.configuration.migration().scope().shard()) != original
                || sources.get(original.configuration.migration().scope().shard()) != original.source || original.released)
            throw new IllegalStateException("archive action lost its original Process/Source maintenance acquisition");
        return Objects.requireNonNull(migrations.get(original.configuration.migration().scope().shard()),
                "actual original migration Driver for archive and old-only loan exit");
    }
    TargetSourceAuthorityLoader.HeldInputs loadMigrationMaintenanceSource(TargetWorkerSourceStartup source,
            TargetMigrationMaintenanceInputs.Provider provider, SourceReplayMutation entry, Runnable current) {
        final MigrationMaintenanceAdmission actual;
        synchronized (this) { actual = migrationMaintenance.get(source.sourceScopeForExitBinding().shard()); }
        if (actual == null || actual.source != source || actual.released || actual.maintenance.ioExited())
            return provider.loadSource(entry, current);
        return actual.maintenance.loadSource(entry, provider, current);
    }
    void requireMigrationMaintenance(TargetWorkerSourceStartup source, TargetWorkerShardRuntime worker, TargetMigrationMaintenance actual) {
        final MigrationMaintenanceAdmission original;
        synchronized (this) { original = migrationMaintenance.get(source.sourceScopeForExitBinding().shard()); }
        if (original == null || original.source != source || original.worker != worker)
            throw new IllegalStateException("live migration changed its original Startup/Worker");
        requireMigrationMaintenance(original, actual);
    }
    /** One original caller per tick, sharing the Host's existing maintenance rotation. */
    void runNextMigrationMaintenanceTurn() {
        final MigrationMaintenanceAdmission next;
        synchronized (this) {
            if (stopping || host == null || !hostTransferred || migrationMaintenance.isEmpty()) return;
            final var all = new ArrayList<>(migrationMaintenance.values());
            next = all.get(Math.floorMod(migrationMaintenanceRotation++, all.size()));
        }
        if (next.maintenance.ioExited()) return;
        if (!host.migrationMaintenanceAdmitted(next.worker)) return;
        next.maintenance.runTurn();
    }
    boolean stopMigrationMaintenanceForWorker(TargetWorkerShardRuntime worker) {
        final MigrationMaintenanceAdmission original;
        final PayloadSummaryAdmission summary;
        synchronized (this) { original = migrationMaintenance.get(worker.shardId()); summary = payloadSummaryMaintenance.get(worker.shardId()); }
        boolean stopped = true;
        if (original != null && original.worker == worker && !original.maintenance.ioExited()) {
            original.maintenance.requestStop(); stopped = original.maintenance.retryCleanup();
        }
        if (summary != null && summary.worker == worker && !summary.maintenance.ioExited())
            stopped &= summary.maintenance.prepareForHostDrain(summary.writer);
        return stopped;
    }
    private boolean closeMigrationMaintenance() {
        final List<MigrationMaintenanceAdmission> original;
        synchronized (this) { original = List.copyOf(migrationMaintenance.values()); }
        boolean done = true;
        for (var held : original) {
            try { held.maintenance.requestStop(); done &= held.maintenance.retryCleanup(); }
            catch (RuntimeException | Error failure) { retain(failure); done = false; }
        }
        return done;
    }
    private boolean closePayloadSummaryMaintenance() {
        final List<PayloadSummaryAdmission> original;
        synchronized (this) { original = List.copyOf(payloadSummaryMaintenance.values()); }
        boolean done = true;
        for (var held : original) {
            try { held.maintenance.requestStop(); held.maintenance.retryCleanup(); done &= held.maintenance.cleanupComplete(); }
            catch (RuntimeException | Error failure) { retain(failure); done = false; }
        }
        return done;
    }
    boolean closePayloadSummaryMaintenanceForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(oxia); return closePayloadSummaryMaintenance();
    }

    /** Maintenance runs before the normal Source constructor on this same bounded Process graph. */
    public synchronized TargetMigrationProcessDriver beginMigration(TargetMigrationConfiguration request,
            TargetWorkerSourceStartup.Owner owner, TargetMigrationInputs.Provider provider,
            TargetDestinationSdkBudget originalSdk) {
        requireAssemblyAdmission(); oxia.providers(); Objects.requireNonNull(request);
        if (originalSdk != destinationSdkBudget || sources.containsKey(request.scope().shard())
                || migrations.containsKey(request.scope().shard())
                || migrations.size() >= configuration.worker().store().maxOwnedShards()
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes),
                        scopeInputBytes), checkpointInputBytes), request.maximumRetainedBytes()) > maximumInputHeapBytes())
            throw new IllegalStateException("migration requires its original pre-Source graph and finite Process reservation");
        final var driver = new TargetMigrationProcessDriver(this,
                new Assembly(configuration.worker(), classes, resources, io, oxia), request, owner, provider,
                originalSdk, nanos);
        migrations.put(request.scope().shard(), driver); migrationReservations.add(driver);
        migrationInputBytes = Math.addExact(migrationInputBytes, request.maximumRetainedBytes());
        return driver;
    }

    /** Registers only the recovery custody owner; the already activated Native root is read after actual open/install. */
    public synchronized TargetMigrationProcessDriver recoverMigrationCompatibility(TargetMigrationConfiguration request,
            TargetWorkerSourceStartup.Owner owner, TargetMigrationInputs.Provider provider,
            TargetDestinationSdkBudget originalSdk) {
        final var original = beginMigration(request, owner, provider, originalSdk);
        original.recoverCompatibility(); return original;
    }

    void bindMigrationRoot(TargetWorkerSourceStartup source, com.nereusstream.delay.store.ShardStore actual,
            com.nereusstream.delay.protocol.TargetMigrationRoot root) {
        final TargetMigrationProcessDriver original;
        synchronized (this) {
            requireSourceAssemblyAdmission(source);
            original = migrations.get(source.sourceScopeForExitBinding().shard());
        }
        if (root == null) {
            if (original != null && original.compatibilityRecovery())
                throw new IllegalStateException("configured compatibility recovery has no actual Native migration root");
            return;
        }
        if (original == null)
            throw new IllegalStateException("migrated Source requires its original protected compatibility input provider");
        if (original.compatibilityRecovery()) original.attachReopened(source, actual, root);
    }

    private synchronized TargetMigrationProcessDriver migrationRecovery(TargetWorkerSourceStartup source) {
        requireSourceAssemblyAdmission(source);
        final var original = migrations.get(source.sourceScopeForExitBinding().shard());
        return original != null && original.compatibilityRecovery() ? original : null;
    }

    boolean prepareMigrationRecovery(TargetWorkerSourceStartup source) {
        final var original = migrationRecovery(source);
        return original == null || original.prepareCompatibilityRecovery();
    }

    boolean migrationHistoryRequired(TargetWorkerSourceStartup source) {
        final var original = migrationRecovery(source); return original != null && original.historyRequired();
    }

    com.nereusstream.delay.protocol.SourcePosition migrationReaderCut(TargetWorkerSourceStartup source) {
        final var original = migrationRecovery(source);
        return original != null && original.historyRequired() ? original.recoveryReaderCut() : null;
    }

    boolean advanceMigrationHistory(TargetWorkerSourceStartup source, TargetWorkerRecoverySource reader) {
        final var original = migrationRecovery(source);
        if (original == null) throw new IllegalStateException("actual history reader has no registered compatibility recovery");
        return original.advanceHistory(reader);
    }

    void requireMigrationRecoveryAdmission(TargetMigrationProcessDriver original, TargetWorkerSourceStartup source,
            com.nereusstream.delay.store.ShardStore actual) {
        synchronized (this) {
            if (stopping || resourcesClosed || host != null || hostPreparationStarted
                    || migrations.get(original.configuration().scope().shard()) != original
                    || !migrationReservations.contains(original)
                    || sources.get(original.configuration().scope().shard()) != source)
                throw new IllegalStateException("compatibility recovery lost its original Process/Source/reservation");
        }
        if (source == null || !source.hasMigrationStore(actual))
            throw new IllegalStateException("compatibility recovery changed its actual opened Source Store");
    }

    synchronized void requireMigrationAdmission(TargetMigrationProcessDriver original) {
        if (stopping || controlledStartupFailure != null || receiptManagementFailed || host != null
                || hostPreparationStarted || preparingHost
                || migrations.get(original.configuration().scope().shard()) != original
                || !migrationReservations.contains(original))
            throw new IllegalStateException("migration lost its original Process registration/reservation");
    }

    synchronized SystemWriterAppendBudget migrationSystemWriterBudget(TargetMigrationProcessDriver original) {
        requireMigrationAdmission(original);
        if (systemWriters == null) systemWriters = configuration.worker().newSystemWriterBudget(classes, resources);
        return systemWriters;
    }

    synchronized void releaseMigrationReservation(TargetMigrationProcessDriver original) {
        if (migrations.get(original.configuration().scope().shard()) != original
                || !migrationReservations.remove(original))
            throw new IllegalStateException("migration release changed its original Process owner");
        migrationInputBytes = Math.subtractExact(migrationInputBytes, original.configuration().maximumRetainedBytes());
    }

    void requireMigrationCompatibility(TargetMigrationProcessDriver original, TargetSourceApplyRuntime runtime,
            com.nereusstream.delay.store.ShardStore actualStore) {
        final TargetWorkerSourceStartup source;
        synchronized (this) {
            if (stopping || resourcesClosed || migrations.get(original.configuration().scope().shard()) != original
                    || !migrationReservations.contains(original))
                throw new IllegalStateException("legacy custody lost its original Process owner/reservation");
            source = sources.get(original.configuration().scope().shard());
        }
        if (source == null || !source.hasMigrationStore(actualStore) || runtime == null)
            throw new IllegalStateException("legacy compatibility is not bound to this actual Process Source Store");
        runtime.requireWorkerStore(actualStore, resources);
    }

    void bindMigration(TargetWorkerSourceStartup source, TargetOwnerRecoveryCoordinator recovery) {
        final TargetMigrationProcessDriver original;
        synchronized (this) {
            requireSourceAssemblyAdmission(source);
            original = migrations.get(source.sourceScopeForExitBinding().shard());
        }
        if (original != null) recovery.configureMigration(original.compatibility(), original.requiredReplayCut());
    }

    /** A candidate/SDK acknowledgement cannot enter any normal Source bootstrap constructor. */
    private void requireMigrationAdopted(ShardId shard) {
        final var migration = migrations.get(shard);
        if (migration != null) {
            if (!migration.compatibilityRecovery() && !migration.serviceAdmitted())
                throw new IllegalStateException("original migration has not adopted actual Source A");
        }
    }

    private boolean closeMigrationsAfterDependencies() {
        final List<TargetMigrationProcessDriver> original;
        synchronized (this) { original = List.copyOf(migrations.values()); }
        boolean complete = true;
        for (var migration : original) {
            try { migration.requestStop(); complete &= migration.retryCleanup(); }
            catch (RuntimeException | Error failure) { retain(failure); complete = false; }
        }
        return complete;
    }

    /** Builds the original Host from this root's privately retained Source startups, not caller-built Workers. */
    public TargetWorkerHostRuntime prepareHost(TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
            LongSupplier checkpointClock, Consumer<Throwable> failures) {
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) {
            requireAssemblyAdmission();
            for (var migration : migrations.values()) if (!migration.serviceAdmitted())
                throw new IllegalStateException("original migration resources/activation/adoption have not closed");
            original = List.copyOf(sources.values());
        }
        if (original.isEmpty()) throw new IllegalStateException("process has no actual Source startups");
        final var workers = original.stream().filter(source -> !source.failedCleanupComplete())
                .map(source -> source.workerForHost(this)).toList();
        if (workers.isEmpty()) throw new IllegalStateException("all original Source startups have retired");
        return prepareHost(workers, checkpoints, checkpointClock, failures);
    }

    public TargetWorkerHostRuntime prepareHostWithDeferredCheckpoints(
            TargetCheckpointCandidateSchedule.DeferredCollectedSemanticRequestFactory checkpoints,
            LongSupplier checkpointClock, Consumer<Throwable> failures) {
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) {
            requireAssemblyAdmission();
            for (var migration : migrations.values()) if (!migration.serviceAdmitted())
                throw new IllegalStateException("original migration resources/activation/adoption have not closed");
            original = List.copyOf(sources.values());
        }
        if (original.isEmpty()) throw new IllegalStateException("process has no actual Source startups");
        final var workers = original.stream().filter(source -> !source.failedCleanupComplete())
                .map(source -> source.workerForHost(this)).toList();
        if (workers.isEmpty()) throw new IllegalStateException("all original Source startups have retired");
        return prepareHost(workers, null, Objects.requireNonNull(checkpoints), checkpointClock, failures);
    }

    private void requireAssemblyAdmission() {
        if (stopping || controlledStartupFailure != null || receiptManagementFailed || host != null
                || hostPreparationStarted || preparingHost || startupTurnActive)
            throw new IllegalStateException("process assembly is closed");
    }

    synchronized void requireSourceAssemblyAdmission(TargetWorkerSourceStartup original) {
        if (stopping || controlledStartupFailure != null || receiptManagementFailed || host != null
                || hostPreparationStarted || preparingHost || !sources.containsValue(original)) {
            throw new IllegalStateException("original process Source assembly is no longer admitted");
        }
    }

    /** Registration precedes native allocation; the retained actual owner also owns late failed construction. */
    public synchronized ReceiptManagementConstruction beginReceiptManagementConstruction(
            ReceiptManagement original, TargetDestinationSdkBudget budget) {
        requireAssemblyAdmission(); oxia.providers();
        if (assemblyBorrowed) throw new IllegalStateException("receipt management requires the original unborrowed process graph");
        Objects.requireNonNull(budget).requireWorker(classes, resources);
        Objects.requireNonNull(original).requireGraph(classes, resources);
        final byte[] identity = original.identity();
        com.nereusstream.delay.protocol.Bytes.requireLength(identity, 32, "original receipt management identity");
        final String id = com.nereusstream.delay.protocol.Bytes.hex(identity);
        if (receiptManagement.size() >= budget.maximumClients() || receiptManagement.containsKey(id))
            throw new IllegalStateException("original receipt management identity/window is unavailable");
        final var construction = new ReceiptManagementConstruction(this, original, Thread.currentThread());
        receiptManagement.put(id, original); receiptConstructions.add(construction); return construction;
    }

    synchronized void requireReceiptManagement(TargetWorkerSourceStartup source, ReceiptManagement original) {
        requireSourceAssemblyAdmission(source);
        if (!receiptManagement.containsValue(original) || !original.ready())
            throw new IllegalArgumentException("receipt backing is not this original process's ready management owner");
    }
    /** Existing Native/history reads remain usable after Host transfer; this does not admit construction or SEND. */
    synchronized void requireReceiptManagementRead(TargetWorkerSourceStartup source, ReceiptManagement original) {
        if (stopping || receiptManagementFailed || preparingHost || !sources.containsValue(source)
                || !receiptManagement.containsValue(original) || !original.ready()) {
            throw new IllegalStateException("original Source/receipt read graph is unavailable");
        }
    }

    public static final class ReceiptManagementConstruction implements AutoCloseable {
        private final TargetWorkerProcessRuntime root;
        private final ReceiptManagement original;
        private final Thread thread;
        private boolean closed;
        private ReceiptManagementConstruction(TargetWorkerProcessRuntime root, ReceiptManagement original, Thread thread) {
            this.root = root; this.original = original; this.thread = thread;
        }
        public Assembly graph() {
            synchronized (root) {
                requireCurrent(); return new Assembly(root.configuration.worker(), root.classes, root.resources, root.io, root.oxia);
            }
        }
        public void requireCurrent() {
            synchronized (root) {
                root.requireAssemblyAdmission();
                if (closed || thread != Thread.currentThread() || !root.receiptConstructions.contains(this)
                        || !root.receiptManagement.containsValue(original))
                    throw new IllegalStateException("receipt management construction lost its original process scope");
            }
        }
        @Override public void close() {
            synchronized (root) {
                if (closed || thread != Thread.currentThread() || !root.receiptConstructions.remove(this))
                    throw new IllegalStateException("receipt management construction exit changed its original caller");
                closed = true;
            }
        }
    }

    /** Startup failures fence the same original Sources; no partially opened backing can escape into a Host. */
    public void failReceiptManagement(ReceiptManagement original, Throwable failure) {
        final List<TargetWorkerSourceStartup> affected;
        synchronized (this) {
            if (!receiptManagement.containsValue(original)) throw new IllegalArgumentException("foreign receipt management owner");
            receiptManagementFailed = true; retain(failure); affected = List.copyOf(sources.values());
        }
        for (var source : affected) {
            try { source.failResourceStartup(failure); }
            catch (RuntimeException | Error cleanup) { retain(cleanup); }
        }
    }

    public static final class ReceiptManagementClosePermit {
        private final TargetWorkerProcessRuntime root;
        private final ReceiptManagement original;
        private ReceiptManagementClosePermit(TargetWorkerProcessRuntime root, ReceiptManagement original) {
            this.root = root; this.original = original;
        }
        public void requireOriginal(ReceiptManagement expected) {
            synchronized (root) {
                if (original != expected || !root.receiptManagementClosing || !root.stopping
                        || root.receiptClosePermits.get(original) != this || !root.receiptConstructions.isEmpty())
                    throw new IllegalStateException("receipt management close lacks its original dependency exit permit");
            }
        }
    }

    /** Host issued this only after all original Shard/Store/SDK users exited; Oxia must remain open meanwhile. */
    boolean closeReceiptManagementForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(oxia);
        synchronized (this) { stopping = true; }
        return closeReceiptManagementAfterDependencies();
    }

    private boolean closeReceiptManagementAfterDependencies() {
        final List<ReceiptManagement> original;
        synchronized (this) {
            if (!stopping || !receiptConstructions.isEmpty()) return false;
            receiptManagementClosing = true; original = List.copyOf(receiptManagement.values());
            for (var owner : original) receiptClosePermits.computeIfAbsent(owner,
                    ignored -> new ReceiptManagementClosePermit(this, owner));
        }
        // Freeze every original owner before any bounded native close; do not hold the process monitor over SDK I/O.
        boolean exited = true;
        for (var owner : original) {
            try { owner.requestStop(); }
            catch (RuntimeException | Error failure) { retain(failure); exited = false; }
        }
        for (var owner : original) {
            final ReceiptManagementClosePermit closing;
            synchronized (this) { closing = receiptClosePermits.get(owner); }
            try {
                exited &= owner.closeForProcess(closing);
                final var history = owner.cleanupFailure();
                if (history != null) retain(history);
            } catch (RuntimeException | Error failure) { retain(failure); exited = false; }
        }
        return exited;
    }

    synchronized void beginSourceConstruction(TargetWorkerSourceStartup original, TargetWorkerSourceStartup.Construction construction) {
        requireSourceAssemblyAdmission(original);
        if (sourceConstructions.size() >= configuration.worker().store().maxOwnedShards()
                || !sourceConstructions.add(construction)) {
            throw new IllegalStateException("original Source construction window is unavailable");
        }
    }

    synchronized void endSourceConstruction(TargetWorkerSourceStartup.Construction original) {
        if (!sourceConstructions.remove(original)) throw new IllegalStateException("Source construction lost its original process registration");
    }

    /** One original configured append window shared by all controlled Source producers in this process graph. */
    synchronized SystemWriterAppendBudget systemWriterBudget(TargetWorkerSourceStartup original) {
        requireSourceAssemblyAdmission(original);
        if (systemWriters == null) systemWriters = configuration.worker().newSystemWriterBudget(classes, resources);
        return systemWriters;
    }

    synchronized TargetWorkerPublicationDispatch publicationDispatch(TargetWorkerSourceStartup original,
            TargetWorkerPublicationDispatch.Configuration requested) {
        requireSourceAssemblyAdmission(original);
        if (publicationDispatch == null) publicationDispatch = new TargetWorkerPublicationDispatch(requested,
                new Assembly(configuration.worker(), classes, resources, io, oxia), metrics);
        publicationDispatch.requireConfiguration(requested);
        return publicationDispatch;
    }

    synchronized void requirePublicationDispatch(TargetWorkerSourceStartup original, TargetWorkerPublicationDispatch expected) {
        requireSourceAssemblyAdmission(original);
        if (publicationDispatch != expected) throw new IllegalArgumentException("publication dispatch belongs to another original process");
    }

    synchronized void reserveChannelInventory(TargetWorkerSourceStartup source, TargetWorkerChannelInventory original) {
        requireSourceAssemblyAdmission(source);
        final long bytes = Math.addExact(original.limits().maximumRetainedBytes(), original.limits().readBytes());
        final long next = Math.addExact(channelInputBytes, bytes);
        if (channelInputs.containsKey(source)
                || Math.addExact(Math.addExact(Math.addExact(next, reclaimInputBytes), scopeInputBytes), checkpointInputBytes)
                > maximumInputHeapBytes()) {
            throw new IllegalStateException("original channel input reservations exceed the process heap envelope");
        }
        channelInputs.put(source, new ChannelInputs(original, bytes)); channelInputBytes = next;
    }

    synchronized void releaseChannelInventory(TargetWorkerSourceStartup source, TargetWorkerChannelInventory original) {
        final var retained = channelInputs.get(source);
        if (retained == null || retained.inventory() != original) throw new IllegalStateException("channel input release changed its original owner");
        channelInputs.remove(source); channelInputBytes = Math.subtractExact(channelInputBytes, retained.bytes());
    }

    /** Extends the original process input envelope; no fake SDK client or independent memory pool. */
    synchronized ReclaimInputs reserveReclaimInputs(TargetWorkerSourceStartup source, long bytes) {
        requireSourceAssemblyAdmission(source);
        if (bytes <= 0 || bytes == Long.MAX_VALUE || reclaimInputs.containsKey(source)
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes),
                        scopeInputBytes), checkpointInputBytes), bytes)
                        > maximumInputHeapBytes()) {
            throw new IllegalStateException("original Native GC input reservation exceeds process envelope or is already owned");
        }
        final var retained = new ReclaimInputs(this, source, bytes);
        reclaimInputs.put(source, retained); reclaimInputBytes = Math.addExact(reclaimInputBytes, bytes);
        return retained;
    }

    static final class ReclaimInputs {
        private final TargetWorkerProcessRuntime process;
        private final TargetWorkerSourceStartup source;
        private final long bytes;
        private ReclaimInputs(TargetWorkerProcessRuntime process, TargetWorkerSourceStartup source, long bytes) {
            this.process = process; this.source = source; this.bytes = bytes;
        }
        void requireCurrent() {
            synchronized (process) {
                if (process.reclaimInputs.get(source) != this) throw new IllegalStateException("original Native GC input reservation released");
            }
        }
        long bytes() { return bytes; }
        /** Only after the controller's original operations, holds and handoff have actually exited. */
        void release() {
            synchronized (process) {
                if (!process.reclaimInputs.remove(source, this)) throw new IllegalStateException("Native GC input release changed original owner");
                process.reclaimInputBytes = Math.subtractExact(process.reclaimInputBytes, bytes);
            }
        }
    }

    /** Scope owner, Root and every permitted local proof share the existing process input heap envelope. */
    synchronized ScopeInputs reserveScopeInputs(TargetWorkerSourceStartup source, long bytes) {
        requireSourceAssemblyAdmission(source);
        if (bytes <= 0 || bytes == Long.MAX_VALUE || scopeInputs.containsKey(source)
                || Math.addExact(Math.addExact(Math.addExact(Math.addExact(channelInputBytes, reclaimInputBytes),
                        scopeInputBytes), checkpointInputBytes), bytes)
                        > maximumInputHeapBytes())
            throw new IllegalStateException("original Scope inputs exceed the process heap envelope or are already retained");
        final var retained = new ScopeInputs(this, source, bytes);
        scopeInputs.put(source, retained); scopeInputBytes = Math.addExact(scopeInputBytes, bytes);
        return retained;
    }

    static final class ScopeInputs {
        private final TargetWorkerProcessRuntime process;
        private final TargetWorkerSourceStartup source;
        private final long bytes;
        private volatile boolean active = true;
        private ScopeInputs(TargetWorkerProcessRuntime process, TargetWorkerSourceStartup source, long bytes) {
            this.process = process; this.source = source; this.bytes = bytes;
        }
        void requireCurrent() {
            if (!active) throw new IllegalStateException("original Scope input reservation was released");
        }
        void release() {
            synchronized (process) {
                if (!process.scopeInputs.remove(source, this)) throw new IllegalStateException("Scope input release changed its original owner");
                process.scopeInputBytes = Math.subtractExact(process.scopeInputBytes, bytes);
                active = false;
            }
        }
    }

    /** Fences original construction immediately; an in-flight Host handoff must actually return before exit. */
    public boolean failControlledStartup(Throwable cause, TargetOwnerDrainCoordinator.Request request) {
        Objects.requireNonNull(cause); Objects.requireNonNull(request);
        final TargetWorkerHostRuntime.PreparationAttempt preparation;
        synchronized (this) {
            if (assemblyBorrowed) throw new IllegalStateException("borrowed application graph must exit through its original Host");
            retain(cause);
            if (controlledStartupFailure == null) controlledStartupFailure = cause;
            preparation = hostPreparation;
        }
        if (preparation != null) preparation.requestStop();
        if (!retireUntransferredHostPreparation()) return false;
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) {
            if (host != null) throw new IllegalStateException("transferred application graph must exit through its original Host");
            if (preparingHost) return false;
            stopping = true;
            original = List.copyOf(sources.values());
        }
        for (var source : original) {
            try { source.failResourceStartup(cause); }
            catch (RuntimeException | Error failure) { retain(failure); }
        }
        if (original.isEmpty()) {
            stopBeforeAssembly();
            return cleanupComplete();
        }
        return stopFailedSourceAssembly(request);
    }

    private boolean retireUntransferredHostPreparation() {
        final TargetWorkerHostRuntime.PreparationAttempt preparation;
        final List<TargetWorkerShardRuntime> original;
        synchronized (this) {
            if (preparingHost) return false;
            if (host != null) throw new IllegalStateException("transferred application graph must exit through its original Host");
            if (!hostPreparationStarted || hostPreparationRetired) return true;
            preparation = hostPreparation;
            original = hostPreparationWorkers;
            // The original call has returned without invoking any Host factory.
            if (preparation == null) { hostPreparationRetired = true; return true; }
        }
        if (preparation.backendTransferAttempted())
            throw new IllegalStateException("original Host transfer must settle through its retained Host");
        final Optional<TargetWorkerHostRuntime.NoTransferCompletion> completion;
        try { completion = preparation.retryNoTransferCleanup(); }
        catch (RuntimeException | Error failure) { retain(failure); return false; }
        if (completion.isEmpty()) return false;
        completion.orElseThrow().requireOriginal(this, preparation, original);
        synchronized (this) {
            if (hostPreparation != preparation || preparingHost || host != null)
                throw new IllegalStateException("original Host preparation changed during no-transfer cleanup");
            hostPreparationRetired = true;
        }
        return true;
    }

    /** Failed controlled startups retain the authority/pool until every original local dependency has actually exited. */
    public boolean stopFailedSourceAssembly(TargetOwnerDrainCoordinator.Request request) {
        Objects.requireNonNull(request);
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost
                    || hostPreparationStarted && !hostPreparationRetired || sources.isEmpty()) {
                throw new IllegalStateException("failed controlled Sources require their original unborrowed assembly graph");
            }
            original = List.copyOf(sources.values());
        }
        // Keep remote/native cleanup outside the process monitor. Successful startup is still admitted.
        for (var source : original) {
            if (source.firstFailure().isEmpty()) throw new IllegalStateException("Source startup is still admitted");
        }
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost || hostPreparationStarted && !hostPreparationRetired
                    || !original.equals(List.copyOf(sources.values()))) {
                throw new IllegalStateException("Source assembly changed during failed startup stop");
            }
            stopping = true;
        }
        boolean exited = true;
        for (var source : original) exited &= source.retryFailedStartupCleanup(request);
        if (!exited) return false;
        synchronized (this) { if (startupTurnActive) return false; dependenciesExited = true; }
        retryCleanup();
        return cleanupComplete();
    }

    synchronized void requireFailedSourceCleanup(TargetWorkerSourceStartup original) {
        if (!stopping || host != null || preparingHost || assemblyBorrowed
                || hostPreparationStarted && !hostPreparationRetired || !sources.containsValue(original)) {
            throw new IllegalStateException("failed Source cleanup lacks its original stopped pre-Host process");
        }
    }

    /** Transfers actual Source Workers into the prepared Host before starting any background loop. */
    public TargetWorkerHostRuntime prepareHost(List<TargetWorkerShardRuntime> workers,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
            LongSupplier checkpointClock, Consumer<Throwable> failures) {
        return prepareHost(workers, Objects.requireNonNull(checkpoints), null, checkpointClock, failures);
    }

    private TargetWorkerHostRuntime prepareHost(List<TargetWorkerShardRuntime> workers,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
            TargetCheckpointCandidateSchedule.DeferredCollectedSemanticRequestFactory deferredCheckpoints,
            LongSupplier checkpointClock, Consumer<Throwable> failures) {
        final var original = List.copyOf(workers);
        final List<TargetWorkerSourceStartup> controlled;
        synchronized (this) { requireAssemblyAdmission(); controlled = List.copyOf(sources.values()); }
        if (!controlled.isEmpty()) {
            final var expected = controlled.stream().filter(source -> !source.failedCleanupComplete())
                    .map(source -> source.workerForHost(this)).toList();
            if (expected.size() != original.size() || expected.stream().anyMatch(worker -> !original.contains(worker))) {
                throw new IllegalArgumentException("prepared Host must include every unretired original Source startup");
            }
        }
        synchronized (this) {
            requireAssemblyAdmission();
            if (!controlled.equals(List.copyOf(sources.values()))) throw new IllegalStateException("Source assembly changed");
            if (!sourceConstructions.isEmpty() || !receiptConstructions.isEmpty())
                throw new IllegalStateException("original Source/receipt construction has not exited before Host transfer");
            if (receiptManagement.values().stream().anyMatch(owner -> !owner.ready()))
                throw new IllegalStateException("original receipt management backing is not ready for Host transfer");
            hostPreparationWorkers = original;
            hostPreparationStarted = true;
            preparingHost = true;
        }
        try {
            for (var worker : original) oxia.requireWorker(worker);
            final TargetWorkerHostRuntime.PreparationAttempt preparation;
            synchronized (this) {
                if (controlledStartupFailure != null)
                    throw new IllegalStateException("original Host preparation was stopped before its factory", controlledStartupFailure);
                preparation = deferredCheckpoints == null
                        ? TargetWorkerHostRuntime.newPreparationAttempt(this, configuration.worker(), classes,
                                resources, original, checkpoints, checkpointClock, failures)
                        : TargetWorkerHostRuntime.newDeferredPreparationAttempt(this, configuration.worker(), classes,
                                resources, original, deferredCheckpoints, checkpointClock, failures);
                hostPreparation = preparation;
            }
            final var prepared = preparation.prepare();
            synchronized (this) { host = prepared; }
            retryHostTransfer();
            for (var source : controlled) {
                final var failed = source.firstFailure();
                if (failed.isPresent()) {
                    final var failure = new IllegalStateException("original Source resource startup failed during Host transfer", failed.orElseThrow());
                    try { prepared.quarantineOwnedBackends(); }
                    catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
            }
            final Throwable stopped;
            synchronized (this) { stopped = controlledStartupFailure; }
            if (stopped != null) {
                final var failure = new IllegalStateException("original Host preparation failed controlled startup", stopped);
                try { prepared.quarantineOwnedBackends(); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            return prepared;
        } catch (RuntimeException | Error failure) {
            final TargetWorkerHostRuntime.PreparationAttempt preparation;
            synchronized (this) { preparation = hostPreparation; }
            if (preparation != null && preparation.backendTransferAttempted()) {
                synchronized (this) { host = preparation.failedHost().orElseThrow(); }
                try { retryHostTransfer(); }
                catch (RuntimeException | Error transfer) { if (transfer != failure) failure.addSuppressed(transfer); }
            }
            retain(failure);
            throw new StartupFailure(failure, this);
        } finally { synchronized (this) { preparingHost = false; } }
    }

    /** Retries the original retained Host transfer; never composes a replacement around transferred Workers. */
    public void retryHostTransfer() {
        final TargetWorkerHostRuntime actual;
        synchronized (this) {
            if (hostTransferred) return;
            if (host == null || stopping) throw new IllegalStateException("no original prepared Host to transfer");
            actual = host;
        }
        actual.retryCompositionBackendTransfers();
        if (actual.hasPendingCompositionBackendTransfers())
            throw new IllegalStateException("original Host backend transfer remains pending");
        actual.registerOxiaRuntime(oxia);
        actual.registerProcessRuntime(this);
        synchronized (this) { hostTransferred = true; }
    }

    void requireHost(TargetWorkerOxiaRuntime authority, TargetWorkerHostRuntime actual,
            List<TargetWorkerShardRuntime> workers) {
        synchronized (this) {
            if (host != actual || authority != oxia || stopping) {
                throw new IllegalStateException("process resources belong to another Host/authority graph");
            }
        }
        for (var worker : workers) oxia.requireWorker(worker);
    }

    void requireRetainedHostComposition(TargetWorkerHostRuntime actual,
            TargetWorkerHostRuntime.PreparationAttempt original, List<TargetWorkerShardRuntime> workers) {
        synchronized (this) {
            if (assemblyBorrowed || preparingHost || host != actual || hostPreparation != original
                    || !hostPreparationStarted || hostPreparationRetired
                    || !sourceConstructions.isEmpty() || !receiptConstructions.isEmpty()
                    || workers.size() != hostPreparationWorkers.size())
                throw new IllegalStateException("retained Host composition differs from its original unborrowed Process graph");
            for (int index = 0; index < workers.size(); index++) {
                if (workers.get(index) != hostPreparationWorkers.get(index))
                    throw new IllegalArgumentException("retained Host composition changed its original Worker identity/order");
            }
        }
        for (var worker : workers) oxia.requireWorker(worker);
    }

    /** Installs the configured observer and existing Source/publisher/maintenance loops on the owned graph. */
    public TargetWorkerOrdinaryLoop activate(BiFunction<TargetWorkerShardRuntime, TargetSourceBacklog.Cut,
            TargetSourceBacklogRead> observations, TargetWorkerOrdinaryDrr.Requests requests,
            LongSupplier ownerClock, LongSupplier schedulerClock, Consumer<Throwable> failures) {
        retryHostTransfer();
        final TargetWorkerHostRuntime actual;
        synchronized (this) {
            if (host == null || preparingHost || stopping || controlledStartupFailure != null)
                throw new IllegalStateException("process Host is not prepared");
            metrics.requireObservationContext();
            if (configuration.observation() != null && ordinaryObservationSourceCount != sources.size()) {
                throw new IllegalStateException("configured Source set changed after its ordinary observation boundary");
            }
            actual = host;
        }
        return actual.activateConfiguredService(configuration.worker().newSourceBacklogCache(io, nanos),
                observations, requests, ownerClock, schedulerClock, nanos, metrics, failures);
    }

    public DrainResult drain(TargetOwnerDrainCoordinator.Request request, LongSupplier ownerClock) {
        synchronized (this) {
            if (preparingHost) throw new IllegalStateException("original Host preparation has not returned");
        }
        retryHostTransfer();
        final TargetWorkerHostRuntime actual = host().orElseThrow();
        var result = actual.drainAll(request, configuration.worker().sourceTurn(), ownerClock);
        retryCleanup();
        return new DrainResult(result, cleanupComplete());
    }

    public List<TargetWorkerHostRuntime.QuarantinedShardExit> exitForReplay(
            TargetOwnerDrainCoordinator.Request request, LongSupplier ownerClock) {
        synchronized (this) {
            if (preparingHost) throw new IllegalStateException("original Host preparation has not returned");
        }
        retryHostTransfer();
        final TargetWorkerHostRuntime actual = host().orElseThrow();
        actual.quarantineRetainedCompositionForReplay(this);
        var result = actual.exitQuarantinedForReplay(request, ownerClock);
        retryCleanup();
        return result;
    }

    /** A caller-created Result cannot release the process resources. */
    void closeForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(oxia);
        synchronized (this) { stopping = true; dependenciesExited = true; }
        retryCleanup();
    }

    boolean releaseRecoveryPinsForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(oxia);
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) { original = List.copyOf(sources.values()); }
        boolean released = true;
        for (var source : original) released &= source.releaseCheckpointPinForHost(permit);
        return released;
    }

    /** The actual Host dependency-exit permit precedes checkpoint loans, owned Pins and shared Oxia closure. */
    boolean closeCheckpointCompositionsForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(oxia);
        synchronized (this) { stopping = true; }
        return closeMigrationsAfterDependencies() && closeCheckpointCompositionsAfterDependencies();
    }

    /** The failed Source retains an alias; the original Process registration remains the only close owner. */
    boolean closeSourceCheckpointComposition(TargetWorkerSourceStartup source) {
        final CheckpointComposition original;
        synchronized (this) {
            requireFailedSourceCleanup(source);
            original = checkpointCompositions.get(source.sourceScopeForExitBinding().shard());
        }
        return original == null || closeCheckpointComposition(original);
    }

    private boolean closeCheckpointCompositionsAfterDependencies() {
        final List<CheckpointComposition> original;
        synchronized (this) { original = List.copyOf(checkpointCompositions.values()); }
        boolean exited = true;
        for (var composition : original) {
            try { composition.owner.requestStop(); }
            catch (RuntimeException | Error failure) { retain(failure); exited = false; }
        }
        for (var composition : original) exited &= closeCheckpointComposition(composition);
        return exited;
    }

    private boolean closeCheckpointComposition(CheckpointComposition original) {
        try {
            original.owner.close();
            final var failure = original.owner.pendingIoFailure();
            if (failure != null) retain(failure);
            if (!original.owner.ioExited()) return false;
            synchronized (this) {
                if (!original.heapReleased) {
                    checkpointInputBytes = Math.subtractExact(checkpointInputBytes, original.configuration.retainedHeapBytes());
                    original.heapReleased = true;
                }
            }
            return true;
        } catch (RuntimeException | Error failure) { retain(failure); return false; }
    }

    /** Host has already observed actual Source/producer/SDK exit; keep I/O and Oxia alive for their final loans. */
    boolean closeScopeResourcesForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        permit.requireOriginalRoot(oxia);
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) { original = List.copyOf(sources.values()); }
        boolean exited = true;
        for (var source : original) exited &= source.closeScopeResourcesAfterDependencies();
        return exited;
    }

    /** Before the graph escapes, only SDK startup IO exists. Borrowed assembly must retire through its actual Host. */
    public void stopBeforeAssembly() {
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost
                    || hostPreparationStarted && !hostPreparationRetired || !sources.isEmpty()) {
                throw new IllegalStateException("borrowed application graph must exit through its original Host");
            }
            stopping = true; dependenciesExited = true;
        }
        retryCleanup();
    }

    /** Non-waiting original-handle cleanup; never discards queued OwnedCheckpointIo or waits on a pool/export thread. */
    public void retryCleanup() {
        synchronized (this) { if (!stopping || !dependenciesExited || resourcesClosed) return; }
        if (!closeMigrationMaintenance() || !closePayloadSummaryMaintenance()) return;
        if (!closeMigrationsAfterDependencies()) return;
        if (!closeCheckpointCompositionsAfterDependencies()) return;
        if (!closeReceiptManagementAfterDependencies()) return;
        final List<TargetWorkerSourceStartup> originalSources;
        synchronized (this) { originalSources = List.copyOf(sources.values()); }
        for (var source : originalSources) if (!source.closeScopeResourcesAfterDependencies()) return;
        synchronized (this) { retryProcessCleanup(); }
    }

    private void retryProcessCleanup() {
        if (!stopping || !dependenciesExited || resourcesClosed) return;
        try {
            if (systemWriters != null) {
                final var usage = systemWriters.usage();
                if (usage.writers() != 0 || usage.outstandingRecords() != 0 || usage.outstandingBytes() != 0) return;
            }
            if (checkpointInputBytes != 0 || !channelInputs.isEmpty() || channelInputBytes != 0
                    || !reclaimInputs.isEmpty() || reclaimInputBytes != 0
                    || !scopeInputs.isEmpty() || scopeInputBytes != 0
                    || !migrationReservations.isEmpty() || migrationInputBytes != 0 || migrationMaintenanceInputBytes != 0
                    || payloadSummaryInputBytes != 0 || payloadSummaryPrefixInputBytes != 0 || writerOriginInputBytes != 0) return;
            if (publicationDispatch != null && !publicationDispatch.closeIfDrained()) return;
            if (partialSharedOpening != null) {
                partialSharedOpening.retryCleanup();
                if (!partialSharedOpening.cleanupComplete()) return;
            }
            if (oxia != null) {
                if (host == null) oxia.stopUnattached();
                oxia.retryCleanup();
                if (!oxia.cleanupComplete()) return;
            }
            if (metrics != null) {
                metrics.close();
                if (!metrics.terminationComplete()) return;
            }
            if (metricCost != null) metricCost.close();
            if (io != null) {
                io.shutdown();
                if (!io.isTerminated()) return;
                synchronized (ioThreads) { if (ioThreads.stream().anyMatch(Thread::isAlive)) return; }
            }
            if (ioCost != null) ioCost.close();
            if (resources != null) {
                if (!resources.stopResourceMonitors()) return;
                if (monitorCost != null) monitorCost.close();
                resources.close();
                if (!resources.cleanupComplete()) return;
            }
            resourcesClosed = true;
        } catch (RuntimeException | Error failure) { retain(failure); }
    }

    /**
     * Certifies only an actual Source from this already-exited Process. Completed cleanup is monotonic;
     * public drain/replay DTOs and a different graph's empty queues cannot construct this receipt.
     */
    public SourceExitCustody sourceExitCustody(ShardId shard) {
        final TargetWorkerSourceStartup source;
        synchronized(this){
            if(!resourcesClosed)throw new IllegalStateException("original Process cleanup has not actually completed");
            source=Objects.requireNonNull(sources.get(Objects.requireNonNull(shard)),"actual original Source startup");
        }
        // Startup already calls Process admission elsewhere: never acquire its monitor while holding Process.
        final var binding=source.sourceExitBinding();
        synchronized(this){
            if(!resourcesClosed||sources.get(shard)!=source||binding.process!=this
                    ||binding.graph.resources()!=resources||binding.graph.io()!=io||binding.graph.oxia()!=oxia
                    ||!binding.scope.shard().equals(shard)||binding.assignment==null||binding.verifiedOwner==null
                    ||binding.lineage==null||!binding.assignment.shardId().equals(shard))
                throw new IllegalStateException("exited Process lacks this actual authenticated Source/Owner/Root binding");
            return new SourceExitCustody(this,binding);
        }
    }

    /**
     * Finite immutable custody of an observed actual Process exit. It proves no old legal-domain
     * completeness, REGISTER authority, deadline, time or permission to reclaim. Weak graph links
     * reject accidental reuse without retaining the old Process, SDKs, Stores or executors.
     */
    public static final class SourceExitCustody {
        private final WeakReference<TargetWorkerProcessRuntime> process;
        private final WeakReference<SharedRocksDbResources> resources;
        private final WeakReference<ThreadPoolExecutor> io;
        private final WeakReference<TargetWorkerOxiaRuntime> oxia;
        private final TargetQuotaScope scope;
        private final SourceAssignment assignment;
        private final OwnerIdentity verifiedOwner;
        private final byte[] lineage;
        private final long retainedBytes;

        private SourceExitCustody(TargetWorkerProcessRuntime original,TargetWorkerSourceStartup.SourceExitBinding binding){
            process=new WeakReference<>(original);resources=new WeakReference<>(binding.graph.resources());
            io=new WeakReference<>(binding.graph.io());oxia=new WeakReference<>(binding.graph.oxia());
            scope=binding.scope;assignment=binding.assignment;verifiedOwner=binding.verifiedOwner;
            lineage=Bytes.copy(binding.lineage);
            // Conservative receipt/weak-link overhead plus immutable canonical-data envelope, including UTF-16 strings.
            long bytes=Math.addExact(512L,lineage.length);
            bytes=Math.addExact(bytes,Math.multiplyExact(2L,scope.canonicalBytes().length));
            bytes=Math.addExact(bytes,Math.multiplyExact(2L,assignment.canonicalBytes().length));
            retainedBytes=Math.addExact(bytes,Math.multiplyExact(2L,verifiedOwner.canonicalBytes().length));
        }

        public long retainedBytes(){return retainedBytes;}
        public TargetQuotaScope scope(){return scope;}
        public SourceAssignment assignment(){return assignment;}
        public OwnerIdentity verifiedOwner(){return verifiedOwner;}
        public byte[] lineage(){return Bytes.copy(lineage);}

        /** Available before the new audit; no current-Owner I/O and no recursive input-hold validation. */
        public void requireOpeningBinding(TargetWorkerSourceStartup.RecoveryInputsOpening opening){
            final var actual=Objects.requireNonNull(opening,"actual new recovery opening");
            final var startup=actual.startup();final var graph=startup.graph();
            if(sameLiveObject(process,startup.sourceProcessForExitBinding())||sameLiveObject(resources,graph.resources())
                    ||sameLiveObject(io,graph.io())||sameLiveObject(oxia,graph.oxia()))
                throw new IllegalStateException("old Process exit custody cannot stop or qualify its own/shared new Source graph");
            if(!scope.equals(startup.sourceScopeForExitBinding())
                    ||!Arrays.equals(verifiedOwner.deploymentId(),actual.owner().deploymentId())
                    ||!samePhysicalSource(assignment.activationBarrier(),actual.assignment().activationBarrier()))
                throw new IllegalStateException("old Process exit custody changed deployment/Scope/physical Source");
        }

        /** Actual completed Process exit plus this new recovery's exact audited Scope/lineage/physical Source. */
        public void requireExited(TargetWorkerSourceStartup.RecoveryInputsOpening opening,
                TargetStoreBootstrap.SloRecoveryAudit audit){
            requireOpeningBinding(opening);
            final var actual=Objects.requireNonNull(audit,"actual Source recovery audit");
            if(!scope.equals(actual.scope())||!Arrays.equals(lineage,actual.lineage()))
                throw new IllegalStateException("old Process exit custody changed its authenticated Scope/recovery lineage");
            assignment.activationBarrier().validatePosition(actual.cut().source());
            opening.assignment().activationBarrier().validatePosition(actual.cut().source());
        }

        private static boolean sameLiveObject(WeakReference<?> original,Object actual){
            final var live=original.get();return live!=null&&live==actual;
        }

        private static boolean samePhysicalSource(SourceActivationBarrier original,SourceActivationBarrier actual){
            if(!original.shardId().equals(actual.shardId()))return false;
            if(original instanceof KafkaActivationBarrier oldKafka&&actual instanceof KafkaActivationBarrier newKafka)
                return oldKafka.authenticatedClusterId().equals(newKafka.authenticatedClusterId())
                        &&oldKafka.nativeTopicUuid().equals(newKafka.nativeTopicUuid());
            if(original instanceof PulsarActivationBarrier oldPulsar&&actual instanceof PulsarActivationBarrier newPulsar)
                return Arrays.equals(oldPulsar.brokerResourceIncarnation(),newPulsar.brokerResourceIncarnation())
                        &&oldPulsar.physicalTopic().equals(newPulsar.physicalTopic());
            return false;
        }
    }

    public synchronized boolean cleanupComplete() { return resourcesClosed; }
    public synchronized Optional<TargetWorkerHostRuntime> host() { return Optional.ofNullable(host); }
    public synchronized boolean hostPreparationPending() { return preparingHost; }
    public synchronized Optional<Throwable> firstFailure() { return Optional.ofNullable(firstFailure); }
    public BoundedAsyncMetricExporter.Snapshot metricSnapshot() { return metrics.snapshot(); }

    private synchronized void retain(Throwable failure) {
        if (firstFailure == null) firstFailure = failure;
        else if (firstFailure != failure) {
            for (var prior : firstFailure.getSuppressed()) if (prior == failure) return;
            firstFailure.addSuppressed(failure);
        }
    }

    public static final class StartupFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerProcessRuntime root;
        private StartupFailure(Throwable failure, TargetWorkerProcessRuntime root) {
            super("Target process startup failed; original resource owner is retained", failure); this.root = root;
        }
        public TargetWorkerProcessRuntime root() { return root; }
    }
}
