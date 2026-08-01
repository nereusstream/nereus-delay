package io.nereusstream.delay.store;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.WriteBufferManager;

/** Process-level resources shared by all open shard DBs in one Worker. */
public final class SharedRocksDbResources implements AutoCloseable {
    static {
        RocksDbNativeLoader.load();
    }

    private final Cache blockCache;
    private final WriteBufferManager writeBufferManager;
    private final RateLimiter rateLimiter;

    public SharedRocksDbResources(final ShardStoreConfig config) {
        blockCache = new LRUCache(config.sharedBlockCacheBytes());
        writeBufferManager = new WriteBufferManager(config.sharedWriteBufferBudgetBytes(), blockCache);
        rateLimiter = new RateLimiter(0);
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

    @Override
    public void close() {
        rateLimiter.close();
        writeBufferManager.close();
        blockCache.close();
    }
}
