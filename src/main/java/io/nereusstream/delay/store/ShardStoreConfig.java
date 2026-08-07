package io.nereusstream.delay.store;

import java.nio.file.Path;
import java.util.Objects;

/** Worker-level RocksDB resource envelope. */
public record ShardStoreConfig(
        Path rootPath,
        int maxOwnedShards,
        int maxOpenShardDbs,
        int maxOpenFilesPerDb,
        int maxTotalOpenFiles,
        int maxBackgroundJobs,
        long sharedBlockCacheBytes,
        long sharedWriteBufferBudgetBytes,
        int maxConcurrentCheckpointCreatesPerWorker,
        int maxConcurrentCheckpointUploadsPerWorker,
        int maxConcurrentCheckpointDownloadsPerWorker,
        long checkpointIoBytesPerSecond,
        int maxConcurrentDrainsPerWorker,
        long maxWriteBufferBytesPerDb) {
    public ShardStoreConfig {
        Objects.requireNonNull(rootPath, "rootPath");
        if (maxOwnedShards <= 0 || maxOpenShardDbs < maxOwnedShards
                || maxOpenFilesPerDb <= 0 || maxTotalOpenFiles <= 0
                || (long) maxTotalOpenFiles < (long) maxOpenFilesPerDb * maxOpenShardDbs
                || maxBackgroundJobs <= 0 || sharedBlockCacheBytes <= 0 || sharedWriteBufferBudgetBytes <= 0
                || maxConcurrentCheckpointCreatesPerWorker <= 0
                || maxConcurrentCheckpointUploadsPerWorker <= 0
                || maxConcurrentCheckpointDownloadsPerWorker <= 0 || checkpointIoBytesPerSecond <= 0
                || maxConcurrentDrainsPerWorker <= 0 || maxWriteBufferBytesPerDb <= 0) {
            throw new IllegalArgumentException("RocksDB resource limits must be positive");
        }
    }

    /** Backwards-compatible full constructor before the per-DB ceiling was explicit. */
    public ShardStoreConfig(final Path rootPath, final int maxOwnedShards, final int maxOpenShardDbs,
                            final int maxOpenFilesPerDb, final int maxTotalOpenFiles, final int maxBackgroundJobs,
                            final long sharedBlockCacheBytes, final long sharedWriteBufferBudgetBytes,
                            final int maxConcurrentCheckpointCreatesPerWorker,
                            final int maxConcurrentCheckpointUploadsPerWorker,
                            final int maxConcurrentCheckpointDownloadsPerWorker,
                            final long checkpointIoBytesPerSecond, final int maxConcurrentDrainsPerWorker) {
        this(rootPath, maxOwnedShards, maxOpenShardDbs, maxOpenFilesPerDb, maxTotalOpenFiles, maxBackgroundJobs,
                sharedBlockCacheBytes, sharedWriteBufferBudgetBytes, maxConcurrentCheckpointCreatesPerWorker,
                maxConcurrentCheckpointUploadsPerWorker, maxConcurrentCheckpointDownloadsPerWorker,
                checkpointIoBytesPerSecond, maxConcurrentDrainsPerWorker, sharedWriteBufferBudgetBytes);
    }

    /** Backwards-compatible constructor for callers that predate download fencing. */
    public ShardStoreConfig(final Path rootPath, final int maxOwnedShards, final int maxOpenShardDbs,
                            final int maxOpenFilesPerDb, final int maxTotalOpenFiles, final int maxBackgroundJobs,
                            final long sharedBlockCacheBytes, final long sharedWriteBufferBudgetBytes,
                            final int maxConcurrentCheckpointCreatesPerWorker,
                            final int maxConcurrentCheckpointUploadsPerWorker,
                            final long checkpointIoBytesPerSecond) {
        this(rootPath, maxOwnedShards, maxOpenShardDbs, maxOpenFilesPerDb, maxTotalOpenFiles, maxBackgroundJobs,
                sharedBlockCacheBytes, sharedWriteBufferBudgetBytes, maxConcurrentCheckpointCreatesPerWorker,
                maxConcurrentCheckpointUploadsPerWorker, 1, checkpointIoBytesPerSecond, 1,
                sharedWriteBufferBudgetBytes);
    }

    /** Backwards-compatible constructor for callers that already configure download fencing. */
    public ShardStoreConfig(final Path rootPath, final int maxOwnedShards, final int maxOpenShardDbs,
                            final int maxOpenFilesPerDb, final int maxTotalOpenFiles, final int maxBackgroundJobs,
                            final long sharedBlockCacheBytes, final long sharedWriteBufferBudgetBytes,
                            final int maxConcurrentCheckpointCreatesPerWorker,
                            final int maxConcurrentCheckpointUploadsPerWorker,
                            final int maxConcurrentCheckpointDownloadsPerWorker,
                            final long checkpointIoBytesPerSecond) {
        this(rootPath, maxOwnedShards, maxOpenShardDbs, maxOpenFilesPerDb, maxTotalOpenFiles, maxBackgroundJobs,
                sharedBlockCacheBytes, sharedWriteBufferBudgetBytes, maxConcurrentCheckpointCreatesPerWorker,
                maxConcurrentCheckpointUploadsPerWorker, maxConcurrentCheckpointDownloadsPerWorker,
                checkpointIoBytesPerSecond, 1, sharedWriteBufferBudgetBytes);
    }

    public static ShardStoreConfig defaults(final Path rootPath) {
        return new ShardStoreConfig(rootPath, 32, 32, 256, 32 * 256, 2,
                64L * 1024 * 1024, 64L * 1024 * 1024, 1, 2, 1, 64L * 1024 * 1024, 1,
                64L * 1024 * 1024);
    }
}
