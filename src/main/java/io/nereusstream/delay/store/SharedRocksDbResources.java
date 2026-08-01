package io.nereusstream.delay.store;

import org.rocksdb.Cache;
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
    private final WriteBufferManager writeBufferManager;
    private final RateLimiter rateLimiter;
    private final Semaphore openDbSlots;
    private final Semaphore checkpointCreateSlots;
    private final Semaphore checkpointUploadSlots;
    private final AtomicBoolean closed = new AtomicBoolean();

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
        blockCache = new LRUCache(config.sharedBlockCacheBytes());
        writeBufferManager = new WriteBufferManager(config.sharedWriteBufferBudgetBytes(), blockCache);
        // Use the worker-wide checkpoint/compaction I/O budget for every DB
        // opened by this process.  A zero-byte limiter would silently disable
        // the global bound even though the config declares one.
        rateLimiter = new RateLimiter(config.checkpointIoBytesPerSecond());
        openDbSlots = new Semaphore(config.maxOpenShardDbs(), true);
        checkpointCreateSlots = new Semaphore(config.maxConcurrentCheckpointCreatesPerWorker(), true);
        checkpointUploadSlots = new Semaphore(config.maxConcurrentCheckpointUploadsPerWorker(), true);
    }

    public Cache blockCache() {
        return blockCache;
    }

    public WriteBufferManager writeBufferManager() {
        return writeBufferManager;
    }

    public RateLimiter rateLimiter() {
        return rateLimiter;
    }

    public void acquireDbSlot() {
        ensureOpen();
        if (!openDbSlots.tryAcquire()) {
            throw new IllegalStateException("worker maxOpenShardDbs limit reached");
        }
    }

    public void releaseDbSlot() {
        openDbSlots.release();
    }

    public void acquireCheckpointCreateSlot() {
        ensureOpen();
        if (!checkpointCreateSlots.tryAcquire()) {
            throw new IllegalStateException("worker checkpoint create concurrency limit reached");
        }
    }

    public void releaseCheckpointCreateSlot() {
        checkpointCreateSlots.release();
    }

    public void acquireCheckpointUploadSlot() {
        ensureOpen();
        if (!checkpointUploadSlots.tryAcquire()) {
            throw new IllegalStateException("worker checkpoint upload concurrency limit reached");
        }
    }

    public void releaseCheckpointUploadSlot() {
        checkpointUploadSlots.release();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            rateLimiter.close();
            writeBufferManager.close();
            blockCache.close();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("shared RocksDB resources are closed");
        }
    }
}
