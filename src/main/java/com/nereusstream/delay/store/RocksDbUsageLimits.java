package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.ShardId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Per-DB and Worker hard limits for a physical RocksDB usage observation.
 *
 * <p>This guard is intentionally explicit and checked: it never treats a
 * missing or overflowing counter as unlimited.  Dynamic admission, cgroup
 * probes and placement authority still belong to the Worker/runtime layer.</p>
 */
public record RocksDbUsageLimits(
        long maxWalBytesPerDb,
        long maxTotalWalBytes,
        int maxWalFilesPerDb,
        int maxTotalWalFiles,
        long maxManifestBytesPerDb,
        long maxTotalManifestBytes,
        int maxManifestFilesPerDb,
        int maxTotalManifestFiles,
        long maxLiveSstBytesPerDb,
        long maxTotalLiveSstBytes,
        int maxSstFilesPerDb,
        int maxTotalSstFiles,
        long maxLocalLiveDataBytes,
        long minimumFilesystemFreeBytes,
        long maxCompactionPendingBytesPerDb,
        long maxTotalCompactionPendingBytes,
        int maxL0FilesPerDb) {
    public RocksDbUsageLimits {
        if (maxWalBytesPerDb <= 0
                || maxTotalWalBytes <= 0
                || maxWalFilesPerDb <= 0
                || maxTotalWalFiles <= 0
                || maxManifestBytesPerDb <= 0
                || maxTotalManifestBytes <= 0
                || maxManifestFilesPerDb <= 0
                || maxTotalManifestFiles <= 0
                || maxLiveSstBytesPerDb <= 0
                || maxTotalLiveSstBytes <= 0
                || maxSstFilesPerDb <= 0
                || maxTotalSstFiles <= 0
                || maxLocalLiveDataBytes <= 0
                || minimumFilesystemFreeBytes < 0
                || maxCompactionPendingBytesPerDb <= 0
                || maxTotalCompactionPendingBytes <= 0
                || maxL0FilesPerDb <= 0) {
            throw new IllegalArgumentException("RocksDB usage limits must be positive");
        }
    }

    /**
     * Validates per-DB limits, checked Worker totals and the exact filesystem
     * backing {@code rootPath}.
     */
    public void validate(final List<RocksDbUsageSnapshot> snapshots, final Path rootPath) {
        Objects.requireNonNull(snapshots, "snapshots");
        Objects.requireNonNull(rootPath, "rootPath");
        final Set<ShardId> identities = new HashSet<>();
        long walBytes = 0;
        long manifestBytes = 0;
        long liveSstBytes = 0;
        long localBytes = 0;
        long compactionPendingBytes = 0;
        int walFiles = 0;
        int manifestFiles = 0;
        int liveSstFiles = 0;
        for (RocksDbUsageSnapshot snapshot : snapshots) {
            Objects.requireNonNull(snapshot, "snapshot");
            if (!identities.add(snapshot.shardId())) {
                throw new IllegalArgumentException("duplicate RocksDB usage snapshot identity");
            }
            if (snapshot.walBytes() > maxWalBytesPerDb || snapshot.walFiles() > maxWalFilesPerDb) {
                throw new IllegalArgumentException("RocksDB per-DB WAL limit exceeded for " + snapshot.shardId());
            }
            if (snapshot.manifestBytes() > maxManifestBytesPerDb || snapshot.manifestFiles() > maxManifestFilesPerDb) {
                throw new IllegalArgumentException("RocksDB per-DB MANIFEST limit exceeded for " + snapshot.shardId());
            }
            if (snapshot.liveSstBytes() > maxLiveSstBytesPerDb || snapshot.liveSstFiles() > maxSstFilesPerDb) {
                throw new IllegalArgumentException("RocksDB per-DB SST limit exceeded for " + snapshot.shardId());
            }
            if (snapshot.compactionPendingBytes() > maxCompactionPendingBytesPerDb
                    || snapshot.l0Files() > maxL0FilesPerDb) {
                throw new IllegalArgumentException(
                        "RocksDB per-DB compaction limit exceeded for " + snapshot.shardId());
            }
            walBytes = add(walBytes, snapshot.walBytes(), "Worker WAL bytes");
            manifestBytes = add(manifestBytes, snapshot.manifestBytes(), "Worker MANIFEST bytes");
            liveSstBytes = add(liveSstBytes, snapshot.liveSstBytes(), "Worker SST bytes");
            localBytes = add(localBytes, snapshot.localBytes(), "Worker local bytes");
            compactionPendingBytes =
                    add(compactionPendingBytes, snapshot.compactionPendingBytes(), "Worker compaction pending bytes");
            walFiles = add(walFiles, snapshot.walFiles(), "Worker WAL files");
            manifestFiles = add(manifestFiles, snapshot.manifestFiles(), "Worker MANIFEST files");
            liveSstFiles = add(liveSstFiles, snapshot.liveSstFiles(), "Worker SST files");
        }
        if (walBytes > maxTotalWalBytes || walFiles > maxTotalWalFiles) {
            throw new IllegalArgumentException("Worker WAL limit exceeded");
        }
        if (manifestBytes > maxTotalManifestBytes || manifestFiles > maxTotalManifestFiles) {
            throw new IllegalArgumentException("Worker MANIFEST limit exceeded");
        }
        if (liveSstBytes > maxTotalLiveSstBytes || liveSstFiles > maxTotalSstFiles) {
            throw new IllegalArgumentException("Worker SST limit exceeded");
        }
        if (localBytes > maxLocalLiveDataBytes) {
            throw new IllegalArgumentException("Worker local live-data limit exceeded");
        }
        if (compactionPendingBytes > maxTotalCompactionPendingBytes) {
            throw new IllegalArgumentException("Worker compaction-pending limit exceeded");
        }
        final long usableBytes;
        try {
            usableBytes = Files.getFileStore(rootPath).getUsableSpace();
        } catch (IOException exception) {
            throw new IllegalStateException("cannot inspect filesystem capacity for " + rootPath, exception);
        }
        if (usableBytes < minimumFilesystemFreeBytes) {
            throw new IllegalArgumentException("filesystem free space is below the RocksDB safety floor");
        }
    }

    private static long add(final long left, final long right, final String name) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(name + " overflows", exception);
        }
    }

    private static int add(final int left, final int right, final String name) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(name + " overflows", exception);
        }
    }
}
