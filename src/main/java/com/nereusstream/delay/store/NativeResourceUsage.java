package com.nereusstream.delay.store;

import java.util.Objects;

/**
 * Disjoint RocksDB-native memory buckets for one Worker reservation.
 *
 * <p>The values are reservations or attributed observations, not JVM heap
 * measurements.  Keeping the buckets explicit prevents pinned blocks,
 * iterators and compaction scratch from being silently counted in a second
 * native category.</p>
 */
public record NativeResourceUsage(
        long blockCacheBytes,
        long memtableBytes,
        long tableReaderMetadataBytes,
        long pinnedBlockBytes,
        long pinnedIteratorBytes,
        long flushCompactionScratchBytes) {
    public NativeResourceUsage {
        if (blockCacheBytes < 0
                || memtableBytes < 0
                || tableReaderMetadataBytes < 0
                || pinnedBlockBytes < 0
                || pinnedIteratorBytes < 0
                || flushCompactionScratchBytes < 0) {
            throw new IllegalArgumentException("native resource buckets must be non-negative");
        }
    }

    public static NativeResourceUsage zero() {
        return new NativeResourceUsage(0, 0, 0, 0, 0, 0);
    }

    public static NativeResourceUsage blockCache(final long bytes) {
        return new NativeResourceUsage(bytes, 0, 0, 0, 0, 0);
    }

    public static NativeResourceUsage memtable(final long bytes) {
        return new NativeResourceUsage(0, bytes, 0, 0, 0, 0);
    }

    /** Returns the checked sum of all RocksDB-native buckets. */
    public long rocksDbNativeBytes() {
        try {
            return Math.addExact(
                    Math.addExact(
                            Math.addExact(blockCacheBytes, memtableBytes),
                            Math.addExact(tableReaderMetadataBytes, pinnedBlockBytes)),
                    Math.addExact(pinnedIteratorBytes, flushCompactionScratchBytes));
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("RocksDB native bucket sum overflows", overflow);
        }
    }

    /** Adds two disjoint usage vectors with checked arithmetic. */
    public NativeResourceUsage add(final NativeResourceUsage other) {
        Objects.requireNonNull(other, "other");
        try {
            return new NativeResourceUsage(
                    Math.addExact(blockCacheBytes, other.blockCacheBytes),
                    Math.addExact(memtableBytes, other.memtableBytes),
                    Math.addExact(tableReaderMetadataBytes, other.tableReaderMetadataBytes),
                    Math.addExact(pinnedBlockBytes, other.pinnedBlockBytes),
                    Math.addExact(pinnedIteratorBytes, other.pinnedIteratorBytes),
                    Math.addExact(flushCompactionScratchBytes, other.flushCompactionScratchBytes));
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("native resource bucket addition overflows", overflow);
        }
    }
}
