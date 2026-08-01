package io.nereusstream.delay.store;

import java.nio.file.Path;
import java.util.Objects;

/** Worker-level RocksDB resource envelope. */
public record ShardStoreConfig(
        Path rootPath,
        int maxOpenFilesPerDb,
        int maxBackgroundJobs,
        long sharedBlockCacheBytes,
        long sharedWriteBufferBudgetBytes) {
    public ShardStoreConfig {
        Objects.requireNonNull(rootPath, "rootPath");
        if (maxOpenFilesPerDb <= 0 || maxBackgroundJobs <= 0
                || sharedBlockCacheBytes <= 0 || sharedWriteBufferBudgetBytes <= 0) {
            throw new IllegalArgumentException("RocksDB resource limits must be positive");
        }
    }

    public static ShardStoreConfig defaults(final Path rootPath) {
        return new ShardStoreConfig(rootPath, 256, 2, 64L * 1024 * 1024, 64L * 1024 * 1024);
    }
}

