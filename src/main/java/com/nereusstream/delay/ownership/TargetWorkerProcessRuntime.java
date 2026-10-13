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
    private SharedRocksDbResources resources;
    private SharedRocksDbResources.NativeOpeningFailure partialSharedOpening;
    private WorkClassExecutionRegistry classes;
    private WorkerNativeResourceLedger.Reservation ioCost;
    private WorkerNativeResourceLedger.Reservation metricCost;
    private ThreadPoolExecutor io;
    private BoundedAsyncMetricExporter metrics;
    private TargetWorkerOxiaRuntime oxia;
    private TargetWorkerHostRuntime host;
    private boolean assemblyBorrowed;
    private boolean preparingHost;
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

    private void requireAssemblyAdmission() {
        if (stopping || host != null || preparingHost) throw new IllegalStateException("process assembly is closed");
    }

    /** Transfers actual Source Workers into the prepared Host before starting any background loop. */
    public TargetWorkerHostRuntime prepareHost(List<TargetWorkerShardRuntime> workers,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
            LongSupplier checkpointClock, Consumer<Throwable> failures) {
        final var original = List.copyOf(workers);
        synchronized (this) { requireAssemblyAdmission(); preparingHost = true; assemblyBorrowed = true; }
        try {
            for (var worker : original) oxia.requireWorker(worker);
            final var prepared = TargetWorkerHostRuntime.prepareWithCollectedCheckpoints(configuration.worker(),
                    classes, resources, original, checkpoints, checkpointClock, failures);
            synchronized (this) { host = prepared; }
            retryHostTransfer();
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

    /** Before the graph escapes, only SDK startup IO exists. Borrowed assembly must retire through its actual Host. */
    public void stopBeforeAssembly() {
        synchronized (this) {
            if (assemblyBorrowed || host != null || preparingHost) {
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
