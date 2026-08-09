package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.ShardId;
import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.WriteBufferManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Process-level resources shared by all open shard DBs in one Worker. */
public final class SharedRocksDbResources implements AutoCloseable {
    private static final int SHARED_NATIVE_RESERVATION_COUNT = 2;

    static {
        RocksDbNativeLoader.load();
    }

    private final ShardStoreConfig config;
    private final WorkerRuntimeSafetyGate runtimeSafetyGate;
    private final Cache blockCache;
    private final Env env;
    private final WriteBufferManager writeBufferManager;
    private final RateLimiter rateLimiter;
    private final Semaphore shardAcquireSlots;
    private final Semaphore ownedShardSlots;
    private final Semaphore openDbSlots;
    private final Semaphore checkpointCreateSlots;
    private final Semaphore checkpointUploadSlots;
    private final Semaphore checkpointDownloadSlots;
    private final Semaphore drainSlots;
    private final WorkerNativeResourceLedger nativeResourceLedger;
    private final WorkerNativeResourceLedger.Reservation sharedBlockCacheReservation;
    private final WorkerNativeResourceLedger.Reservation sharedWriteBufferReservation;
    private final AtomicBoolean closed = new AtomicBoolean();
    private WorkerRuntimeResourceMonitor runtimeResourceMonitor;
    private WorkerRocksDbUsageMonitor rocksDbUsageMonitor;
    /** Set after the in-flight check passes; all callers are then fenced. */
    private boolean closeStarted;
    private boolean rateLimiterClosed;
    private boolean writeBufferManagerClosed;
    private boolean blockCacheClosed;
    /**
     * The logical ownership slot is identity-bound, not merely a counter.
     * Without this set two concurrent opens of the same shard could each
     * create a fresh incarnation before either one installs ACTIVE.
     */
    private final Set<ShardId> ownedShardIdentities = new HashSet<>();
    private final Map<ShardId, PhysicalUsageSource> physicalUsageSources = new HashMap<>();
    private int shardAcquireCount;
    private int ownedShardCount;
    private int openDbCount;
    private int checkpointCreateCount;
    private int checkpointUploadCount;
    private int checkpointDownloadCount;
    private int drainCount;

    public SharedRocksDbResources(final ShardStoreConfig config) {
        this(config, null, null);
    }

    /**
     * Creates shared resources after validating an explicit process/container
     * capacity proof.  The legacy constructor remains for embedded tests that
     * do not claim a production resource envelope.
     */
    public SharedRocksDbResources(final ShardStoreConfig config, final WorkerResourceEnvelope envelope) {
        this(config, envelope, null);
    }

    /**
     * Creates shared resources after validating an envelope against a
     * previously captured runtime observation.  The observation must come
     * from the same process/container and root filesystem as the config.
     */
    public SharedRocksDbResources(final ShardStoreConfig config,
                                  final WorkerResourceEnvelope envelope,
                                  final WorkerRuntimeResourceObservation observation) {
        this.config = Objects.requireNonNull(config, "config");
        if (envelope != null) {
            if (observation == null) {
                envelope.validate(this.config);
            } else {
                envelope.validate(this.config, observation);
            }
        } else if (observation != null) {
            throw new IllegalArgumentException("runtime observation requires a Worker resource envelope");
        }
        runtimeSafetyGate = envelope == null ? null
                : new WorkerRuntimeSafetyGate(this.config, envelope, observation);
        nativeResourceLedger = envelope == null ? null
                : new WorkerNativeResourceLedger(envelope.maxRocksDbNativeBytes(), envelope.maxOtherNativeBytes());
        if (nativeResourceLedger == null) {
            sharedBlockCacheReservation = null;
            sharedWriteBufferReservation = null;
        } else {
            // Reserve the configured shared native budgets before creating
            // JNI objects.  The buckets are disjoint by construction: cache
            // and mutable/immutable memtables cannot silently consume the
            // other-native envelope.
            sharedBlockCacheReservation = nativeResourceLedger.reserve("shared-block-cache",
                    NativeResourceUsage.blockCache(this.config.sharedBlockCacheBytes()), 0);
            sharedWriteBufferReservation = nativeResourceLedger.reserve("shared-write-buffer",
                    NativeResourceUsage.memtable(this.config.sharedWriteBufferBudgetBytes()), 0);
        }
        env = Env.getDefault();
        env.setBackgroundThreads(this.config.maxBackgroundJobs());
        blockCache = new LRUCache(this.config.sharedBlockCacheBytes());
        writeBufferManager = new WriteBufferManager(this.config.sharedWriteBufferBudgetBytes(), blockCache);
        // Use the worker-wide checkpoint/compaction I/O budget for every DB
        // opened by this process.  A zero-byte limiter would silently disable
        // the global bound even though the config declares one.
        rateLimiter = new RateLimiter(this.config.checkpointIoBytesPerSecond());
        shardAcquireSlots = new Semaphore(this.config.maxConcurrentAcquiresPerWorker(), true);
        ownedShardSlots = new Semaphore(this.config.maxOwnedShards(), true);
        openDbSlots = new Semaphore(this.config.maxOpenShardDbs(), true);
        checkpointCreateSlots = new Semaphore(this.config.maxConcurrentCheckpointCreatesPerWorker(), true);
        checkpointUploadSlots = new Semaphore(this.config.maxConcurrentCheckpointUploadsPerWorker(), true);
        checkpointDownloadSlots = new Semaphore(this.config.maxConcurrentCheckpointDownloadsPerWorker(), true);
        drainSlots = new Semaphore(this.config.maxConcurrentDrainsPerWorker(), true);
    }

    /** Probes the current process/container before opening shared resources. */
    public static SharedRocksDbResources withRuntimeProbe(final ShardStoreConfig config,
                                                           final WorkerResourceEnvelope envelope) {
        return new SharedRocksDbResources(config, envelope,
                WorkerRuntimeResourceProbe.observe(config.rootPath()));
    }

    public Cache blockCache() {
        return blockCache;
    }

    /** Returns the process-shared RocksDB Env/background thread pool. */
    public Env env() {
        return env;
    }

    public WriteBufferManager writeBufferManager() {
        return writeBufferManager;
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    /** Returns the native bucket ledger for envelope-bound Workers. */
    public synchronized WorkerNativeResourceLedger nativeResourceLedger() {
        ensureOpen();
        if (nativeResourceLedger == null) {
            throw new IllegalStateException("native resource ledger requires a Worker resource envelope");
        }
        return nativeResourceLedger;
    }

    /** Reserves an explicitly attributed native allocation for one DB or runtime component. */
    public synchronized WorkerNativeResourceLedger.Reservation reserveNativeResource(
            final String allocationId, final NativeResourceUsage rocksDbUsage, final long otherNativeBytes) {
        return nativeResourceLedger().reserve(allocationId, rocksDbUsage, otherNativeBytes);
    }

    /** Starts the owner-managed fixed-delay runtime probe after startup validation. */
    public synchronized WorkerRuntimeResourceMonitor startRuntimeResourceMonitor(final Duration interval) {
        ensureOpen();
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("runtime safety monitor requires a Worker resource envelope");
        }
        if (runtimeResourceMonitor == null || runtimeResourceMonitor.isClosed()) {
            runtimeResourceMonitor = WorkerRuntimeResourceMonitor.start(config.rootPath(), interval, this);
        }
        return runtimeResourceMonitor;
    }

    /**
     * Starts the owner-managed dynamic per-DB usage monitor. The supplied
     * limits are applied to every complete Worker observation, including the
     * exact filesystem safety floor.
     */
    public synchronized WorkerRocksDbUsageMonitor startRocksDbUsageMonitor(final RocksDbUsageLimits limits,
                                                                             final Duration interval) {
        ensureOpen();
        Objects.requireNonNull(limits, "limits");
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("RocksDB usage monitor requires a Worker resource envelope");
        }
        if (rocksDbUsageMonitor == null || rocksDbUsageMonitor.isClosed()) {
            rocksDbUsageMonitor = WorkerRocksDbUsageMonitor.start(interval,
                    () -> collectPhysicalUsage(limits), this::recordRuntimeProbeFailure);
        }
        return rocksDbUsageMonitor;
    }

    /**
     * Returns a point-in-time snapshot of all registered open shard DBs and
     * validates the Worker aggregate against the supplied limits.
     *
     * <p>Sources are copied under the resource lock and invoked afterwards;
     * this avoids a lock inversion with {@link ShardStore#physicalUsage()} and
     * the Store close path.</p>
     */
    public List<RocksDbUsageSnapshot> collectPhysicalUsage(final RocksDbUsageLimits limits) {
        Objects.requireNonNull(limits, "limits");
        final List<PhysicalUsageSource> sources;
        synchronized (this) {
            ensureOpen();
            sources = new ArrayList<>(physicalUsageSources.values());
        }
        final List<RocksDbUsageSnapshot> snapshots = new ArrayList<>(sources.size());
        for (PhysicalUsageSource source : sources) {
            try {
                final RocksDbUsageSnapshot snapshot = Objects.requireNonNull(source.snapshot().get(),
                        "physical usage source returned null");
                if (!source.shardId().equals(snapshot.shardId())) {
                    throw new IllegalStateException("physical usage source returned another shard identity");
                }
                snapshots.add(snapshot);
            } catch (RuntimeException failure) {
                synchronized (this) {
                    if (physicalUsageSources.get(source.shardId()) != source) {
                        continue;
                    }
                }
                throw failure;
            }
        }
        limits.validate(snapshots, config.rootPath());
        return List.copyOf(snapshots);
    }

    /** Returns the number of open stores currently registered for observation. */
    public synchronized int registeredPhysicalUsageSources() {
        return physicalUsageSources.size();
    }

    void registerPhysicalUsage(final ShardId shardId, final Supplier<RocksDbUsageSnapshot> source) {
        synchronized (this) {
            ensureOpen();
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(source, "source");
            if (physicalUsageSources.containsKey(shardId)) {
                throw new IllegalStateException("physical usage source already registered for " + shardId);
            }
            physicalUsageSources.put(shardId, new PhysicalUsageSource(shardId, source));
        }
    }

    void unregisterPhysicalUsage(final ShardId shardId, final Supplier<RocksDbUsageSnapshot> source) {
        synchronized (this) {
            final PhysicalUsageSource existing = physicalUsageSources.get(shardId);
            if (existing != null && existing.snapshot() == source) {
                physicalUsageSources.remove(shardId);
            }
        }
    }

    /** Revalidates the startup envelope against a fresh runtime observation. */
    public synchronized void revalidateRuntime(final WorkerRuntimeResourceObservation observation) {
        ensureOpen();
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("runtime safety gate requires a Worker resource envelope");
        }
        runtimeSafetyGate.observe(observation);
    }

    /** Fences the Worker when a scheduled runtime probe cannot produce evidence. */
    public synchronized void recordRuntimeProbeFailure(final Throwable failure) {
        ensureOpen();
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("runtime safety gate requires a Worker resource envelope");
        }
        runtimeSafetyGate.rejectProbeFailure(failure);
    }

    /** Stages a new envelope and fences new ownership before drain/migration. */
    public synchronized void stageRuntimeEnvelope(final WorkerResourceEnvelope envelope,
                                                   final WorkerRuntimeResourceObservation observation) {
        ensureOpen();
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("runtime safety gate requires a Worker resource envelope");
        }
        runtimeSafetyGate.stage(envelope, observation);
    }

    /** Explicitly moves a staged/unsafe Worker into drain or migration. */
    public synchronized void beginRuntimeDrainOrMigrate() {
        ensureOpen();
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("runtime safety gate requires a Worker resource envelope");
        }
        runtimeSafetyGate.beginDrainOrMigrate();
    }

    /** Reopens the gate only after the old physical ownership boundary is empty. */
    public synchronized void activateRuntimeAfterDrain(final long ownedShardDbs,
                                                        final long openShardDbs,
                                                        final boolean transitionInFlight,
                                                        final WorkerRuntimeResourceObservation observation) {
        ensureOpen();
        if (runtimeSafetyGate == null) {
            throw new IllegalStateException("runtime safety gate requires a Worker resource envelope");
        }
        runtimeSafetyGate.activateAfterDrain(ownedShardDbs, openShardDbs, transitionInFlight, observation);
    }

    public synchronized WorkerRuntimeSafetyGate.State runtimeSafetyState() {
        return runtimeSafetyGate == null ? null : runtimeSafetyGate.state();
    }

    /** Fences a new Claim/Admission attempt after a shared runtime breach. */
    public synchronized void requireRuntimeBusinessAdmission() {
        ensureOpen();
        if (runtimeSafetyGate != null) {
            runtimeSafetyGate.requireActive("Claim/Admission");
        }
    }

    /** Reserves one slot for the bounded shard-open/restore acquisition phase. */
    public synchronized void acquireShardAcquireSlot() {
        ensureOpen();
        requireRuntimeOwnershipAdmission();
        if (!shardAcquireSlots.tryAcquire()) {
            throw new IllegalStateException("worker concurrent shard acquire limit reached");
        }
        shardAcquireCount++;
    }

    public synchronized void releaseShardAcquireSlot() {
        if (shardAcquireCount <= 0) {
            throw new IllegalStateException("shard acquire slot released without an active acquisition");
        }
        shardAcquireCount--;
        shardAcquireSlots.release();
    }

    /**
     * Reserves one logical shard ownership slot for the lifetime of an open
     * active shard DB.  This is separate from the physical DB slot because
     * restore validation may briefly open a DB without owning the shard.
     */
    public synchronized void acquireOwnedShardSlot() {
        ensureOpen();
        requireRuntimeOwnershipAdmission();
        if (!ownedShardSlots.tryAcquire()) {
            throw new IllegalStateException("worker maxOwnedShards limit reached");
        }
        ownedShardCount++;
    }

    /** Reserves the logical owned slot for one exact Shard identity. */
    public synchronized void acquireOwnedShardSlot(final ShardId shardId) {
        ensureOpen();
        requireRuntimeOwnershipAdmission();
        if (shardId == null) {
            throw new NullPointerException("shardId");
        }
        if (ownedShardIdentities.contains(shardId)) {
            throw new IllegalStateException("worker already owns shard " + shardId);
        }
        if (!ownedShardSlots.tryAcquire()) {
            throw new IllegalStateException("worker maxOwnedShards limit reached");
        }
        ownedShardIdentities.add(shardId);
        ownedShardCount++;
    }

    public synchronized void releaseOwnedShardSlot() {
        if (ownedShardCount <= 0) {
            throw new IllegalStateException("owned shard slot released without an owned shard");
        }
        ownedShardCount--;
        ownedShardSlots.release();
    }

    /** Releases the exact logical owned slot reserved for one Shard. */
    public synchronized void releaseOwnedShardSlot(final ShardId shardId) {
        if (shardId == null || !ownedShardIdentities.remove(shardId)) {
            throw new IllegalStateException("owned shard identity released without an active shard: " + shardId);
        }
        if (ownedShardCount <= 0) {
            // Keep the identity set and semaphore accounting fail-closed if a
            // caller has mixed the legacy counter-only API with this one.
            ownedShardIdentities.add(shardId);
            throw new IllegalStateException("owned shard slot released without an owned shard");
        }
        ownedShardCount--;
        ownedShardSlots.release();
    }

    public synchronized void acquireDbSlot() {
        ensureOpen();
        requireRuntimeOwnershipAdmission();
        if (!openDbSlots.tryAcquire()) {
            throw new IllegalStateException("worker maxOpenShardDbs limit reached");
        }
        openDbCount++;
    }

    public synchronized void releaseDbSlot() {
        if (openDbCount <= 0) {
            throw new IllegalStateException("RocksDB DB slot released without an open DB");
        }
        openDbCount--;
        openDbSlots.release();
    }

    public synchronized void acquireCheckpointCreateSlot() {
        ensureOpen();
        if (!checkpointCreateSlots.tryAcquire()) {
            throw new IllegalStateException("worker checkpoint create concurrency limit reached");
        }
        checkpointCreateCount++;
    }

    public synchronized void releaseCheckpointCreateSlot() {
        if (checkpointCreateCount <= 0) {
            throw new IllegalStateException("checkpoint create slot released without an active operation");
        }
        checkpointCreateCount--;
        checkpointCreateSlots.release();
    }

    public synchronized void acquireCheckpointUploadSlot() {
        ensureOpen();
        if (!checkpointUploadSlots.tryAcquire()) {
            throw new IllegalStateException("worker checkpoint upload concurrency limit reached");
        }
        checkpointUploadCount++;
    }

    public synchronized void releaseCheckpointUploadSlot() {
        if (checkpointUploadCount <= 0) {
            throw new IllegalStateException("checkpoint upload slot released without an active operation");
        }
        checkpointUploadCount--;
        checkpointUploadSlots.release();
    }

    /** Reserves the process-wide slot for checkpoint download/restore staging. */
    public synchronized void acquireCheckpointDownloadSlot() {
        ensureOpen();
        requireRuntimeOwnershipAdmission();
        if (!checkpointDownloadSlots.tryAcquire()) {
            throw new IllegalStateException("worker checkpoint download concurrency limit reached");
        }
        checkpointDownloadCount++;
    }

    public synchronized void releaseCheckpointDownloadSlot() {
        if (checkpointDownloadCount <= 0) {
            throw new IllegalStateException("checkpoint download slot released without an active operation");
        }
        checkpointDownloadCount--;
        checkpointDownloadSlots.release();
    }

    /** Reserves one process-wide slot for the complete owner-drain window. */
    public synchronized void acquireDrainSlot() {
        ensureOpen();
        if (!drainSlots.tryAcquire()) {
            throw new IllegalStateException("worker drain concurrency limit reached");
        }
        drainCount++;
    }

    public synchronized void releaseDrainSlot() {
        if (drainCount <= 0) {
            throw new IllegalStateException("drain slot released without an active drain");
        }
        drainCount--;
        drainSlots.release();
    }

    @Override
    public synchronized void close() {
        if (closed.get()) {
            return;
        }
        if (!closeStarted && (shardAcquireCount != 0 || openDbCount != 0 || ownedShardCount != 0
                || !ownedShardIdentities.isEmpty()
                || !physicalUsageSources.isEmpty()
                || checkpointCreateCount != 0 || checkpointUploadCount != 0
                || checkpointDownloadCount != 0 || drainCount != 0
                || (nativeResourceLedger != null
                && nativeResourceLedger.snapshot().activeAllocations() != SHARED_NATIVE_RESERVATION_COUNT))) {
            throw new IllegalStateException("cannot close shared RocksDB resources while work is in flight");
        }
        closeStarted = true;
        if (runtimeResourceMonitor != null) {
            runtimeResourceMonitor.close();
        }
        if (rocksDbUsageMonitor != null) {
            rocksDbUsageMonitor.close();
        }
        // Every process-level native resource must be attempted even if an
        // earlier close reports a JNI/runtime failure.  Otherwise a failed
        // rate-limiter shutdown can strand the shared write-buffer manager or
        // block cache for the lifetime of the Worker.
        RuntimeException closeFailure = null;
        if (!rateLimiterClosed) {
            try {
                rateLimiter.close();
                rateLimiterClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (!writeBufferManagerClosed) {
            try {
                writeBufferManager.close();
                writeBufferManagerClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (writeBufferManagerClosed && sharedWriteBufferReservation != null
                && !sharedWriteBufferReservation.isReleased()) {
            try {
                sharedWriteBufferReservation.close();
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (!blockCacheClosed) {
            try {
                blockCache.close();
                blockCacheClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (blockCacheClosed && sharedBlockCacheReservation != null
                && !sharedBlockCacheReservation.isReleased()) {
            try {
                sharedBlockCacheReservation.close();
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        final boolean sharedReservationsReleased = (sharedWriteBufferReservation == null
                || sharedWriteBufferReservation.isReleased())
                && (sharedBlockCacheReservation == null || sharedBlockCacheReservation.isReleased());
        if (closeFailure == null && rateLimiterClosed && writeBufferManagerClosed && blockCacheClosed
                && sharedReservationsReleased) {
            closed.set(true);
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private static RuntimeException appendCloseFailure(final RuntimeException first,
                                                       final RuntimeException failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private void ensureOpen() {
        if (closed.get() || closeStarted) {
            throw new IllegalStateException("shared RocksDB resources are closed");
        }
    }

    private void requireRuntimeOwnershipAdmission() {
        if (runtimeSafetyGate != null) {
            runtimeSafetyGate.requireActive("ownership/restore");
        }
    }

    private record PhysicalUsageSource(ShardId shardId, Supplier<RocksDbUsageSnapshot> snapshot) {
    }
}
