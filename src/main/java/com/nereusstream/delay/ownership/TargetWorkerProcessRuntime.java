package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.store.NativeResourceUsage;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerNativeResourceLedger;
import io.oxia.client.api.Authentication;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
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

    public record Configuration(TargetWorkerRuntimeConfig worker, Limits process,
            TargetWorkerOxiaRuntime.Connection oxia) {
        public Configuration {
            Objects.requireNonNull(worker); Objects.requireNonNull(process); Objects.requireNonNull(oxia);
            long[] fixed = new long[CapacityDimension.COUNT];
            fixed[CapacityDimension.OTHER_NATIVE_BYTES.wireValue() - 1] = Math.addExact(
                    Math.addExact(process.ioNativeBytes(), process.metricNativeBytes()),
                    Math.multiplyExact(3L, oxia.nativeBytesPerClient()));
            worker.requireCapacity(List.of(), new CapacityVector(fixed), CapacityVector.empty());
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
    private SharedRocksDbResources resources;
    private SharedRocksDbResources.NativeOpeningFailure partialSharedOpening;
    private WorkClassExecutionRegistry classes;
    private WorkerNativeResourceLedger.Reservation ioCost;
    private WorkerNativeResourceLedger.Reservation metricCost;
    private ThreadPoolExecutor io;
    private BoundedAsyncMetricExporter metrics;
    private SystemWriterAppendBudget systemWriters;
    private TargetWorkerPublicationDispatch publicationDispatch;
    private record ChannelInputs(TargetWorkerChannelInventory inventory, long bytes) {}
    private final java.util.Map<TargetWorkerSourceStartup, ChannelInputs> channelInputs = new java.util.IdentityHashMap<>();
    private long channelInputBytes;
    private TargetWorkerOxiaRuntime oxia;
    private TargetWorkerHostRuntime host;
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
        var root = new TargetWorkerProcessRuntime(configuration, nanos);
        try {
            originalStartupGuard.run();
            root.resources = configuration.worker().openResources();
            root.classes = configuration.worker().newWorkClasses(nanos);
            root.ioCost = root.resources.reserveNativeResource("target-process-io", NativeResourceUsage.zero(),
                    configuration.process().ioNativeBytes());
            root.metricCost = root.resources.reserveNativeResource("target-process-metrics", NativeResourceUsage.zero(),
                    configuration.process().metricNativeBytes());
            root.io = new ThreadPoolExecutor(configuration.process().ioThreads(), configuration.process().ioThreads(),
                    0L, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<>(configuration.process().ioPendingTasks()),
                    root::newIoThread, new ThreadPoolExecutor.AbortPolicy());
            root.metrics = new BoundedAsyncMetricExporter(configuration.process().metrics(), exporter);
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

    /** Opens the actual Target Store and commits the genuine initial grant before exposing a Source activation. */
    public TargetWorkerSourceStartup initializeSource(TargetWorkerSourceStartup.Owner owner,
            TargetWorkerSourceStartup.Initial initial, TargetWorkerSourceRuntimeFactory.Content content,
            SourceReplaySuccessor successor) {
        final TargetWorkerSourceStartup startup;
        synchronized (this) {
            requireAssemblyAdmission(); oxia.providers();
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

    /** Builds the original Host from this root's privately retained Source startups, not caller-built Workers. */
    public TargetWorkerHostRuntime prepareHost(TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
            LongSupplier checkpointClock, Consumer<Throwable> failures) {
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) { requireAssemblyAdmission(); original = List.copyOf(sources.values()); }
        if (original.isEmpty()) throw new IllegalStateException("process has no actual Source startups");
        final var workers = original.stream().filter(source -> !source.failedCleanupComplete())
                .map(source -> source.workerForHost(this)).toList();
        if (workers.isEmpty()) throw new IllegalStateException("all original Source startups have retired");
        return prepareHost(workers, checkpoints, checkpointClock, failures);
    }

    private void requireAssemblyAdmission() {
        if (stopping || host != null || preparingHost || startupTurnActive) throw new IllegalStateException("process assembly is closed");
    }

    synchronized void requireSourceAssemblyAdmission(TargetWorkerSourceStartup original) {
        if (stopping || host != null || preparingHost || !sources.containsValue(original)) {
            throw new IllegalStateException("original process Source assembly is no longer admitted");
        }
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
        if (channelInputs.containsKey(source) || next > configuration.worker().resources().certifiedJvmHeapBytes()) {
            throw new IllegalStateException("original channel input reservations exceed the process heap envelope");
        }
        channelInputs.put(source, new ChannelInputs(original, bytes)); channelInputBytes = next;
    }

    synchronized void releaseChannelInventory(TargetWorkerSourceStartup source, TargetWorkerChannelInventory original) {
        final var retained = channelInputs.get(source);
        if (retained == null || retained.inventory() != original) throw new IllegalStateException("channel input release changed its original owner");
        channelInputs.remove(source); channelInputBytes = Math.subtractExact(channelInputBytes, retained.bytes());
    }

    /** Failed controlled startups retain the authority/pool until every original local dependency has actually exited. */
    public boolean stopFailedSourceAssembly(TargetOwnerDrainCoordinator.Request request) {
        Objects.requireNonNull(request);
        final List<TargetWorkerSourceStartup> original;
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost || sources.isEmpty()) {
                throw new IllegalStateException("failed controlled Sources require their original unborrowed assembly graph");
            }
            original = List.copyOf(sources.values());
        }
        // Keep remote/native cleanup outside the process monitor. Successful startup is still admitted.
        for (var source : original) {
            if (source.firstFailure().isEmpty()) throw new IllegalStateException("Source startup is still admitted");
        }
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost || !original.equals(List.copyOf(sources.values()))) {
                throw new IllegalStateException("Source assembly changed during failed startup stop");
            }
            stopping = true;
        }
        boolean exited = true;
        for (var source : original) exited &= source.retryFailedStartupCleanup(request);
        if (!exited) return false;
        synchronized (this) { if (startupTurnActive) return false; dependenciesExited = true; }
        oxia.stopUnattached(); retryCleanup();
        return cleanupComplete();
    }

    synchronized void requireFailedSourceCleanup(TargetWorkerSourceStartup original) {
        if (!stopping || host != null || preparingHost || assemblyBorrowed || !sources.containsValue(original)) {
            throw new IllegalStateException("failed Source cleanup lacks its original stopped pre-Host process");
        }
    }

    /** Transfers actual Source Workers into the prepared Host before starting any background loop. */
    public TargetWorkerHostRuntime prepareHost(List<TargetWorkerShardRuntime> workers,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
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
            if (!sourceConstructions.isEmpty()) throw new IllegalStateException("original Source construction has not exited before Host transfer");
            preparingHost = true; assemblyBorrowed = true;
        }
        try {
            for (var worker : original) oxia.requireWorker(worker);
            final var prepared = TargetWorkerHostRuntime.prepareWithCollectedCheckpoints(configuration.worker(),
                    classes, resources, original, checkpoints, checkpointClock, failures);
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
            return prepared;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerHostRuntime.CompositionFailure partial) {
                synchronized (this) { host = partial.failedHost(); }
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

    /** Installs the configured observer and existing Source/publisher/maintenance loops on the owned graph. */
    public TargetWorkerOrdinaryLoop activate(BiFunction<TargetWorkerShardRuntime, TargetSourceBacklog.Cut,
            TargetSourceBacklogRead> observations, TargetWorkerOrdinaryDrr.Requests requests,
            LongSupplier ownerClock, LongSupplier schedulerClock, Consumer<Throwable> failures) {
        retryHostTransfer();
        final TargetWorkerHostRuntime actual;
        synchronized (this) {
            if (host == null || preparingHost || stopping) throw new IllegalStateException("process Host is not prepared");
            actual = host;
        }
        return actual.activateConfiguredService(configuration.worker().newSourceBacklogCache(io, nanos),
                observations, requests, ownerClock, schedulerClock, nanos, metrics, failures);
    }

    public DrainResult drain(TargetOwnerDrainCoordinator.Request request, LongSupplier ownerClock) {
        retryHostTransfer();
        final TargetWorkerHostRuntime actual = host().orElseThrow();
        var result = actual.drainAll(request, configuration.worker().sourceTurn(), ownerClock);
        retryCleanup();
        return new DrainResult(result, cleanupComplete());
    }

    public List<TargetWorkerHostRuntime.QuarantinedShardExit> exitForReplay(
            TargetOwnerDrainCoordinator.Request request, LongSupplier ownerClock) {
        retryHostTransfer();
        final TargetWorkerHostRuntime actual = host().orElseThrow();
        actual.quarantineOwnedBackends();
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

    /** Before the graph escapes, only SDK startup IO exists. Borrowed assembly must retire through its actual Host. */
    public void stopBeforeAssembly() {
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost || !sources.isEmpty()) {
                throw new IllegalStateException("borrowed application graph must exit through its original Host");
            }
            stopping = true; dependenciesExited = true;
        }
        if (oxia != null) oxia.stopUnattached();
        retryCleanup();
    }

    /** Non-waiting original-handle cleanup; never discards queued OwnedCheckpointIo or waits on a pool/export thread. */
    public synchronized void retryCleanup() {
        if (!stopping || !dependenciesExited || resourcesClosed) return;
        try {
            if (systemWriters != null) {
                final var usage = systemWriters.usage();
                if (usage.writers() != 0 || usage.outstandingRecords() != 0 || usage.outstandingBytes() != 0) return;
            }
            if (!channelInputs.isEmpty() || channelInputBytes != 0) return;
            if (publicationDispatch != null && !publicationDispatch.closeIfDrained()) return;
            if (partialSharedOpening != null) {
                partialSharedOpening.retryCleanup();
                if (!partialSharedOpening.cleanupComplete()) return;
            }
            if (oxia != null) {
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
                resources.close();
                if (!resources.cleanupComplete()) return;
            }
            resourcesClosed = true;
        } catch (RuntimeException | Error failure) { retain(failure); }
    }

    public synchronized boolean cleanupComplete() { return resourcesClosed; }
    public synchronized Optional<TargetWorkerHostRuntime> host() { return Optional.ofNullable(host); }
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
