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
        long checkpointIoBytesPerSecond) {
    public ShardStoreConfig {
        Objects.requireNonNull(rootPath, "rootPath");
        if (maxOwnedShards <= 0 || maxOpenShardDbs < maxOwnedShards
                || maxOpenFilesPerDb <= 0 || maxTotalOpenFiles <= 0
                || (long) maxTotalOpenFiles < (long) maxOpenFilesPerDb * maxOpenShardDbs
                || maxBackgroundJobs <= 0 || sharedBlockCacheBytes <= 0 || sharedWriteBufferBudgetBytes <= 0
                || maxConcurrentCheckpointCreatesPerWorker <= 0
                || maxConcurrentCheckpointUploadsPerWorker <= 0
                || maxConcurrentCheckpointDownloadsPerWorker <= 0 || checkpointIoBytesPerSecond <= 0) {
            throw new IllegalArgumentException("RocksDB resource limits must be positive");
        }
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
                maxConcurrentCheckpointUploadsPerWorker, 1, checkpointIoBytesPerSecond);
    }

    public static ShardStoreConfig defaults(final Path rootPath) {
        return new ShardStoreConfig(rootPath, 32, 32, 256, 32 * 256, 2,
                64L * 1024 * 1024, 64L * 1024 * 1024, 1, 2, 1, 64L * 1024 * 1024);
    }
}
