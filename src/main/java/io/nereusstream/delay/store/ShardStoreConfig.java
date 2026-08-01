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
        long checkpointIoBytesPerSecond) {
    public ShardStoreConfig {
        Objects.requireNonNull(rootPath, "rootPath");
        if (maxOwnedShards <= 0 || maxOpenShardDbs < maxOwnedShards
                || maxOpenFilesPerDb <= 0 || maxTotalOpenFiles <= 0
                || (long) maxTotalOpenFiles < (long) maxOpenFilesPerDb * maxOpenShardDbs
                || maxBackgroundJobs <= 0 || sharedBlockCacheBytes <= 0 || sharedWriteBufferBudgetBytes <= 0
                || maxConcurrentCheckpointCreatesPerWorker <= 0
                || maxConcurrentCheckpointUploadsPerWorker <= 0 || checkpointIoBytesPerSecond <= 0) {
            throw new IllegalArgumentException("RocksDB resource limits must be positive");
        }
    }

    public static ShardStoreConfig defaults(final Path rootPath) {
        return new ShardStoreConfig(rootPath, 32, 32, 256, 32 * 256, 2,
                64L * 1024 * 1024, 64L * 1024 * 1024, 1, 2, 64L * 1024 * 1024);
    }
}
