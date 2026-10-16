package com.nereusstream.delay.ownership;

import com.nereusstream.delay.store.NativeResourceUsage;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerNativeResourceLedger;
import io.oxia.client.OxiaClientCleanup;
import io.oxia.client.OxiaClientLifecycle;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SyncOxiaClient;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/** One original configured Oxia allocation, opening task and native cleanup, on the Worker's existing I/O pool. */
public final class TargetWorkerOxiaClientOwner {
    private final ThreadPoolExecutor executor;
    private final WorkerNativeResourceLedger.Reservation cost;
    private final Duration cleanupBudget;
    private final CompletableFuture<SyncOxiaClient> ready = new CompletableFuture<>();
    private OwnedCheckpointIo<OxiaClientLifecycle.Opening> openingTask;
    private OwnedCheckpointIo<OxiaClientCleanup.Status> closingTask;
    private OxiaClientCleanup cleanup;
    private boolean stopping;
    private boolean openingEntered;
    private boolean openingReturned;
    private boolean released;
    private Throwable firstFailure;
    private Duration configuredRequestTimeout;
    private byte[] storageNamespaceBinding;
    private TargetWorkerOxiaRuntime runtime;

    public static TargetWorkerOxiaClientOwner open(OxiaClientBuilder builder, ThreadPoolExecutor executor,
            SharedRocksDbResources resources, String allocationId, long nativeBytes, Duration cleanupBudget,
            Runnable startupGuard) {
        if (!(builder instanceof OxiaClientBuilderImpl) || nativeBytes <= 0 || nativeBytes == Long.MAX_VALUE
                || cleanupBudget == null || cleanupBudget.isZero() || cleanupBudget.isNegative()
                || cleanupBudget.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Oxia owner requires actual SDK, finite native reservation and cleanup budget");
        }
        requireLockedArtifacts();
        var pool = Objects.requireNonNull(executor, "original bounded I/O executor");
        if (pool.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) pool.getQueue().size() + pool.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || pool.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("Oxia owner requires finite threads/queue and AbortPolicy");
        }
        var shared = Objects.requireNonNull(resources, "actual Worker resources");
        shared.requireRuntimeBusinessAdmission();
        var reservation = shared.reserveNativeResource("oxia-sdk/" + Objects.requireNonNull(allocationId),
                NativeResourceUsage.zero(), nativeBytes);
        var owner = new TargetWorkerOxiaClientOwner(pool, reservation, cleanupBudget);
        try {
            owner.openingTask = OwnedCheckpointIo.submit(guard -> {
                synchronized (owner) { owner.openingEntered = true; }
                try {
                    var opened = OxiaClientLifecycle.openSync(builder);
                    synchronized (owner) {
                        owner.cleanup = opened.cleanup();
                        owner.configuredRequestTimeout = opened.configuration().requestTimeout();
                        final var configuration=opened.configuration();
                        owner.storageNamespaceBinding=com.nereusstream.delay.protocol.Bytes.sha256(
                                com.nereusstream.delay.protocol.Bytes.utf8("nereus-delay-original-oxia-storage-namespace\0"),
                                com.nereusstream.delay.protocol.Bytes.lp32(com.nereusstream.delay.protocol.Bytes.utf8(configuration.serviceAddress())),
                                com.nereusstream.delay.protocol.Bytes.lp32(com.nereusstream.delay.protocol.Bytes.utf8(configuration.namespace())),
                                new byte[]{(byte)(configuration.enableTls()?1:0)});
                    }
                    opened.client().whenComplete((client, failure) -> {
                        final boolean transfer;
                        synchronized (owner) {
                            if (failure != null) { owner.stopping = true; owner.retain(failure); }
                            transfer = !owner.stopping && failure == null;
                        }
                        if (transfer) owner.ready.complete(client);
                        else owner.ready.completeExceptionally(failure != null ? failure
                                : new IllegalStateException("Oxia opening stopped before transfer"));
                        if (failure != null) owner.retryCleanup();
                    });
                    return opened;
                } catch (Throwable failure) {
                    synchronized (owner) {
                        owner.retain(failure);
                        for (Throwable current = failure; current != null; current = current.getCause()) {
                            if (current instanceof OxiaClientLifecycle.OpeningFailure failed) owner.cleanup = failed.cleanup();
                        }
                    }
                    owner.ready.completeExceptionally(failure);
                    throw failure;
                } finally { synchronized (owner) { owner.openingReturned = true; } }
            }, Objects.requireNonNull(startupGuard, "original startup guard"), pool);
            owner.openingTask.receipt().whenComplete((opened, failure) -> {
                synchronized (owner) {
                    if (failure != null) { owner.stopping = true; owner.retain(failure); }
                }
                if (failure != null) owner.ready.completeExceptionally(failure);
                if (owner.stopping) owner.retryCleanup();
            });
            return owner;
        } catch (RuntimeException | Error rejection) {
            owner.retain(rejection);
            owner.stopping = true;
            if (owner.openingTask == null && !owner.openingEntered) {
                reservation.close(); owner.released = true;
            }
            throw new OpenFailure(rejection, owner);
        }
    }

    private static void requireLockedArtifacts() {
        var lock = new java.util.Properties();
        try (var input = TargetWorkerOxiaClientOwner.class.getResourceAsStream(
                "/META-INF/nereus-delay-oxia-client-lifecycle.properties")) {
            if (input == null) throw new IllegalStateException("Oxia lifecycle artifact lock missing");
            lock.load(input);
        } catch (java.io.IOException failure) { throw new IllegalStateException("cannot read Oxia lifecycle lock", failure); }
        var hex = java.util.HexFormat.of();
        TargetSdkArtifactIdentity.requireExact(OxiaClientLifecycle.class,
                hex.parseHex(lock.getProperty("clientJarSha256")));
        TargetSdkArtifactIdentity.requireExact(SyncOxiaClient.class,
                hex.parseHex(lock.getProperty("apiJarSha256")));
    }

    private TargetWorkerOxiaClientOwner(ThreadPoolExecutor executor, WorkerNativeResourceLedger.Reservation cost,
            Duration cleanupBudget) { this.executor = executor; this.cost = cost; this.cleanupBudget = cleanupBudget; }

    public synchronized Duration requireConfiguredRequestTimeout() {
        if (configuredRequestTimeout == null || configuredRequestTimeout.toMillis() < 1
                || configuredRequestTimeout.toMillis() > 60_000) {
            throw new IllegalStateException("actual Oxia opening has no bounded request timeout");
        }
        return configuredRequestTimeout;
    }
    /** Captured from the actual SDK opening; it excludes process/session/client identifiers and current credentials. */
    synchronized byte[] requireStorageNamespaceBinding(){
        requireClient();
        return com.nereusstream.delay.protocol.Bytes.copy(Objects.requireNonNull(storageNamespaceBinding,"original SDK namespace binding"));
    }

    public static final class OpenFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerOxiaClientOwner owner;
        private OpenFailure(Throwable failure, TargetWorkerOxiaClientOwner owner) {
            super("original Oxia opening owner retained", failure); this.owner = owner;
        }
        public TargetWorkerOxiaClientOwner owner() { return owner; }
    }

    public CompletableFuture<SyncOxiaClient> clientReady() { return ready.copy(); }

    public synchronized boolean readyForUse() {
        return !stopping && !released && openingTask != null && openingReturned && openingTask.ioExited()
                && ready.isDone() && !ready.isCompletedExceptionally();
    }

    public synchronized SyncOxiaClient requireClient() {
        if (stopping || released || openingTask == null || !openingReturned || !openingTask.ioExited()
                || !ready.isDone() || ready.isCompletedExceptionally()) {
            throw new IllegalStateException("original Oxia opening is not active");
        }
        return ready.join();
    }

    /** Root calls this only after Host/Source/Store no longer need this authority for drain or fault cleanup. */
    synchronized void bindRuntime(TargetWorkerOxiaRuntime expected) {
        if (runtime != null && runtime != expected) throw new IllegalStateException("Oxia client belongs to another root");
        runtime = Objects.requireNonNull(expected);
    }

    public void requestClose() {
        final TargetWorkerOxiaRuntime root;
        synchronized (this) { root = runtime; }
        if (root != null) root.requireClientCloseAllowed();
        synchronized (this) {
            stopping = true;
        }
        ready.completeExceptionally(new IllegalStateException("Oxia owner closing"));
        final OwnedCheckpointIo<OxiaClientLifecycle.Opening> original;
        synchronized (this) { original = openingTask; }
        if (original != null) original.requestStop();
        retryCleanup();
    }

    public synchronized void retryCleanup() {
        if (!stopping || released || openingTask == null || !openingTask.ioExited()
                || closingTask != null && !closingTask.ioExited()) return;
        if (cleanup == null) {
            if (!openingEntered) { cost.close(); released = true; }
            // An unexpected failure after entering SDK opening is unknown until its actual cleanup handle is retained.
            return;
        }
        if (cleanup.status().terminated()) { cost.close(); released = true; return; }
        try {
            closingTask = OwnedCheckpointIo.submit(ignored -> {
                var result = cleanup.close(cleanupBudget);
                synchronized (this) { retain(result.firstFailure()); }
                return result;
            }, () -> {}, executor);
        } catch (RuntimeException | Error rejected) { retain(rejected); }
    }

    public synchronized boolean cleanupComplete() {
        if (!released && stopping && openingReturned && openingTask != null && openingTask.ioExited()
                && (closingTask == null || closingTask.ioExited()) && cleanup != null && cleanup.status().terminated()) {
            cost.close(); released = true;
        }
        return released;
    }

    public java.util.Optional<OxiaClientCleanup.Status> nativeStatus() {
        final OxiaClientCleanup original;
        synchronized (this) { original = cleanup; }
        return original == null ? java.util.Optional.empty() : java.util.Optional.of(original.status());
    }
    public synchronized java.util.Optional<Throwable> firstFailure() { return java.util.Optional.ofNullable(firstFailure); }
    private synchronized void retain(Throwable failure) {
        if (failure == null || failure == firstFailure) return;
        if (firstFailure == null) firstFailure = failure;
        else { for (var prior : firstFailure.getSuppressed()) if (prior == failure) return; firstFailure.addSuppressed(failure); }
    }
}
