package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.ShardId;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.LogFile;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * A point-in-time physical usage observation for one open shard database.
 *
 * <p>The values are observations, not reservations.  They are deliberately
 * kept separate from the immutable capacity grant because RocksDB can change
 * WAL/SST/manifest state between two observations.</p>
 */
public record RocksDbUsageSnapshot(
        ShardId shardId,
        long liveSstBytes,
        long walBytes,
        long manifestBytes,
        long localBytes,
        long compactionPendingBytes,
        int liveSstFiles,
        int walFiles,
        int manifestFiles,
        int localFiles,
        int l0Files) {
    public RocksDbUsageSnapshot {
        Objects.requireNonNull(shardId, "shardId");
        if (liveSstBytes < 0 || walBytes < 0 || manifestBytes < 0 || localBytes < 0
                || compactionPendingBytes < 0 || liveSstFiles < 0 || walFiles < 0
                || manifestFiles < 0 || localFiles < 0 || l0Files < 0) {
            throw new IllegalArgumentException("RocksDB usage must be non-negative");
        }
    }

    /** Collects the RocksDB and filesystem counters while the DB is open. */
    public static RocksDbUsageSnapshot collect(final ShardId shardId, final RocksDB db, final Path dbPath) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(dbPath, "dbPath");
        if (db.isClosed()) {
            throw new IllegalStateException("cannot inspect a closed RocksDB");
        }
        try {
            if (Files.isSymbolicLink(dbPath)
                    || !Files.isDirectory(dbPath, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("RocksDB usage root must be a real directory: " + dbPath);
            }
            long liveSstBytes = 0;
            int liveSstFiles = 0;
            int l0Files = 0;
            final List<LiveFileMetaData> liveFiles = db.getLiveFilesMetaData();
            for (LiveFileMetaData file : liveFiles) {
                liveSstBytes = add(liveSstBytes, nonNegative(file.size(), "live SST size"), "live SST bytes");
                liveSstFiles = increment(liveSstFiles, "live SST file count");
                if (file.level() == 0) {
                    l0Files = increment(l0Files, "L0 file count");
                }
            }

            long walBytes = 0;
            int walFiles = 0;
            for (LogFile file : db.getSortedWalFiles()) {
                walBytes = add(walBytes, nonNegative(file.sizeFileBytes(), "WAL size"), "WAL bytes");
                walFiles = increment(walFiles, "WAL file count");
            }

            long manifestBytes = 0;
            int manifestFiles = 0;
            long localBytes = 0;
            int localFiles = 0;
            try (var paths = Files.walk(dbPath)) {
                for (Path path : paths.toList()) {
                    if (Files.isSymbolicLink(path)) {
                        throw new IllegalStateException("RocksDB usage path must not contain a symbolic link: " + path);
                    }
                    if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                        continue;
                    }
                    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IllegalStateException("RocksDB usage path contains a non-regular file: " + path);
                    }
                    final long size = nonNegative(LocalStatePathGuard.sizeRegularFileNoFollow(path,
                            "RocksDB local file size"), "local file size");
                    localBytes = add(localBytes, size, "local bytes");
                    localFiles = increment(localFiles, "local file count");
                    final String name = path.getFileName().toString();
                    if (name.startsWith("MANIFEST-")) {
                        manifestBytes = add(manifestBytes, size, "MANIFEST bytes");
                        manifestFiles = increment(manifestFiles, "MANIFEST file count");
                    }
                }
            }

            final long compactionPendingBytes = nonNegative(db.getLongProperty(
                    "rocksdb.estimate-pending-compaction-bytes"), "compaction pending bytes");
            return new RocksDbUsageSnapshot(shardId, liveSstBytes, walBytes, manifestBytes, localBytes,
                    compactionPendingBytes, liveSstFiles, walFiles, manifestFiles, localFiles, l0Files);
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("cannot collect RocksDB physical usage for " + shardId, exception);
        }
    }

    private static long add(final long left, final long right, final String name) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException exception) {
            throw new IllegalStateException(name + " overflows", exception);
        }
    }

    private static int increment(final int value, final String name) {
        if (value == Integer.MAX_VALUE) {
            throw new IllegalStateException(name + " overflows");
        }
        return value + 1;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalStateException(name + " is negative");
        }
        return value;
    }
}
