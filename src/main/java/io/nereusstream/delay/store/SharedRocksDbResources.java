package io.nereusstream.delay.store;

import org.rocksdb.Cache;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.WriteBufferManager;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/** Process-level resources shared by all open shard DBs in one Worker. */
public final class SharedRocksDbResources implements AutoCloseable {
    static {
        RocksDbNativeLoader.load();
    }

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
    private final AtomicBoolean closed = new AtomicBoolean();
    private int shardAcquireCount;
    private int ownedShardCount;
    private int openDbCount;
    private int checkpointCreateCount;
    private int checkpointUploadCount;
    private int checkpointDownloadCount;
    private int drainCount;

    public SharedRocksDbResources(final ShardStoreConfig config) {
        this(config, null);
    }

    /**
     * Creates shared resources after validating an explicit process/container
     * capacity proof.  The legacy constructor remains for embedded tests that
     * do not claim a production resource envelope.
     */
    public SharedRocksDbResources(final ShardStoreConfig config, final WorkerResourceEnvelope envelope) {
        if (envelope != null) {
            envelope.validate(config);
        }
        env = Env.getDefault();
        env.setBackgroundThreads(config.maxBackgroundJobs());
        blockCache = new LRUCache(config.sharedBlockCacheBytes());
        writeBufferManager = new WriteBufferManager(config.sharedWriteBufferBudgetBytes(), blockCache);
        // Use the worker-wide checkpoint/compaction I/O budget for every DB
        // opened by this process.  A zero-byte limiter would silently disable
        // the global bound even though the config declares one.
        rateLimiter = new RateLimiter(config.checkpointIoBytesPerSecond());
        shardAcquireSlots = new Semaphore(config.maxConcurrentAcquiresPerWorker(), true);
        ownedShardSlots = new Semaphore(config.maxOwnedShards(), true);
        openDbSlots = new Semaphore(config.maxOpenShardDbs(), true);
        checkpointCreateSlots = new Semaphore(config.maxConcurrentCheckpointCreatesPerWorker(), true);
        checkpointUploadSlots = new Semaphore(config.maxConcurrentCheckpointUploadsPerWorker(), true);
        checkpointDownloadSlots = new Semaphore(config.maxConcurrentCheckpointDownloadsPerWorker(), true);
        drainSlots = new Semaphore(config.maxConcurrentDrainsPerWorker(), true);
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

    /** Reserves one slot for the bounded shard-open/restore acquisition phase. */
    public synchronized void acquireShardAcquireSlot() {
        ensureOpen();
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
        if (!ownedShardSlots.tryAcquire()) {
            throw new IllegalStateException("worker maxOwnedShards limit reached");
        }
        ownedShardCount++;
    }

    public synchronized void releaseOwnedShardSlot() {
        if (ownedShardCount <= 0) {
            throw new IllegalStateException("owned shard slot released without an owned shard");
        }
        ownedShardCount--;
        ownedShardSlots.release();
    }

    public synchronized void acquireDbSlot() {
        ensureOpen();
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
        if (shardAcquireCount != 0 || openDbCount != 0 || ownedShardCount != 0
                || checkpointCreateCount != 0 || checkpointUploadCount != 0
                || checkpointDownloadCount != 0 || drainCount != 0) {
            throw new IllegalStateException("cannot close shared RocksDB resources while work is in flight");
        }
        closed.set(true);
        // Every process-level native resource must be attempted even if an
        // earlier close reports a JNI/runtime failure.  Otherwise a failed
        // rate-limiter shutdown can strand the shared write-buffer manager or
        // block cache for the lifetime of the Worker.
        RuntimeException closeFailure = null;
        closeFailure = closeResource(closeFailure, rateLimiter::close);
        closeFailure = closeResource(closeFailure, writeBufferManager::close);
        closeFailure = closeResource(closeFailure, blockCache::close);
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private static RuntimeException closeResource(final RuntimeException first, final Runnable closeAction) {
        try {
            closeAction.run();
            return first;
        } catch (RuntimeException failure) {
            if (first == null) {
                return failure;
            }
            if (failure != first) {
                first.addSuppressed(failure);
            }
            return first;
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("shared RocksDB resources are closed");
        }
    }
}
