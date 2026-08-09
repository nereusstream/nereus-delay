package io.nereusstream.delay.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Checked Worker-level attribution for disjoint RocksDB-native and other-
 * native reservations.
 *
 * <p>Each allocation has an immutable identity and must be released through
 * its returned handle.  This makes shared cache/WBM reservations and later
 * per-DB observations auditable without allowing a caller to count one
 * allocation twice or treat an overflow as unlimited capacity.</p>
 */
public final class WorkerNativeResourceLedger {
    private final long maxRocksDbNativeBytes;
    private final long maxOtherNativeBytes;
    private final Map<String, Reservation> reservations = new HashMap<>();
    private NativeResourceUsage rocksDbNativeUsage = NativeResourceUsage.zero();
    private long otherNativeBytes;

    public WorkerNativeResourceLedger(final long maxRocksDbNativeBytes,
                                      final long maxOtherNativeBytes) {
        if (maxRocksDbNativeBytes <= 0 || maxOtherNativeBytes <= 0) {
            throw new IllegalArgumentException("native resource limits must be positive");
        }
        this.maxRocksDbNativeBytes = maxRocksDbNativeBytes;
        this.maxOtherNativeBytes = maxOtherNativeBytes;
    }

    /** Reserves both buckets under one immutable allocation identity. */
    public synchronized Reservation reserve(final String allocationId,
                                            final NativeResourceUsage rocksDbUsage,
                                            final long otherNativeBytes) {
        Objects.requireNonNull(allocationId, "allocationId");
        Objects.requireNonNull(rocksDbUsage, "rocksDbUsage");
        if (allocationId.isBlank()) {
            throw new IllegalArgumentException("native allocation identity must not be blank");
        }
        if (otherNativeBytes < 0) {
            throw new IllegalArgumentException("other-native bytes must be non-negative");
        }
        if (reservations.containsKey(allocationId)) {
            throw new IllegalArgumentException("duplicate native allocation identity: " + allocationId);
        }
        final NativeResourceUsage nextRocksDbUsage;
        final long nextOtherNativeBytes;
        try {
            nextRocksDbUsage = rocksDbNativeUsage.add(rocksDbUsage);
            nextOtherNativeBytes = Math.addExact(this.otherNativeBytes, otherNativeBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("native resource usage overflows", overflow);
        } catch (IllegalStateException overflow) {
            throw new IllegalArgumentException("native resource usage overflows", overflow);
        }
        if (nextRocksDbUsage.rocksDbNativeBytes() > maxRocksDbNativeBytes) {
            throw new IllegalArgumentException("RocksDB-native resource envelope exceeded");
        }
        if (nextOtherNativeBytes > maxOtherNativeBytes) {
            throw new IllegalArgumentException("other-native resource envelope exceeded");
        }
        final Reservation reservation = new Reservation(allocationId, rocksDbUsage, otherNativeBytes);
        reservations.put(allocationId, reservation);
        rocksDbNativeUsage = nextRocksDbUsage;
        this.otherNativeBytes = nextOtherNativeBytes;
        return reservation;
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(rocksDbNativeUsage, otherNativeBytes, reservations.size(),
                maxRocksDbNativeBytes, maxOtherNativeBytes);
    }

    private synchronized void release(final Reservation reservation) {
        final Reservation current = reservations.get(reservation.allocationId);
        if (current != reservation) {
            throw new IllegalStateException("native allocation is not active: " + reservation.allocationId);
        }
        // Compute both successor buckets before changing the identity map.  A
        // corrupted or otherwise inconsistent projection must leave the
        // reservation active so a later close can retry; removing it first
        // would make the failed release look completed while leaking capacity
        // (and would make the handle's idempotence flag impossible to repair).
        final NativeResourceUsage nextRocksDbUsage;
        final long nextOtherNativeBytes;
        try {
            nextRocksDbUsage = subtract(rocksDbNativeUsage, reservation.rocksDbUsage);
            nextOtherNativeBytes = Math.subtractExact(otherNativeBytes, reservation.otherNativeBytes);
        } catch (ArithmeticException | IllegalStateException overflow) {
            throw new IllegalStateException("native resource release underflows", overflow);
        }
        reservations.remove(reservation.allocationId);
        rocksDbNativeUsage = nextRocksDbUsage;
        otherNativeBytes = nextOtherNativeBytes;
    }

    private static NativeResourceUsage subtract(final NativeResourceUsage left,
                                                final NativeResourceUsage right) {
        try {
            return new NativeResourceUsage(
                    Math.subtractExact(left.blockCacheBytes(), right.blockCacheBytes()),
                    Math.subtractExact(left.memtableBytes(), right.memtableBytes()),
                    Math.subtractExact(left.tableReaderMetadataBytes(), right.tableReaderMetadataBytes()),
                    Math.subtractExact(left.pinnedBlockBytes(), right.pinnedBlockBytes()),
                    Math.subtractExact(left.pinnedIteratorBytes(), right.pinnedIteratorBytes()),
                    Math.subtractExact(left.flushCompactionScratchBytes(), right.flushCompactionScratchBytes()));
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IllegalStateException("native resource bucket release underflows", failure);
        }
    }

    public record Snapshot(NativeResourceUsage rocksDbUsage,
                           long otherNativeBytes,
                           int activeAllocations,
                           long maxRocksDbNativeBytes,
                           long maxOtherNativeBytes) {
        public Snapshot {
            Objects.requireNonNull(rocksDbUsage, "rocksDbUsage");
            if (otherNativeBytes < 0 || activeAllocations < 0
                    || maxRocksDbNativeBytes <= 0 || maxOtherNativeBytes <= 0) {
                throw new IllegalArgumentException("invalid native resource snapshot");
            }
        }
    }

    /** Idempotent handle for one exact ledger allocation. */
    public final class Reservation implements AutoCloseable {
        private final String allocationId;
        private final NativeResourceUsage rocksDbUsage;
        private final long otherNativeBytes;
        private boolean released;

        private Reservation(final String allocationId,
                            final NativeResourceUsage rocksDbUsage,
                            final long otherNativeBytes) {
            this.allocationId = allocationId;
            this.rocksDbUsage = rocksDbUsage;
            this.otherNativeBytes = otherNativeBytes;
        }

        public String allocationId() {
            return allocationId;
        }

        public synchronized boolean isReleased() {
            return released;
        }

        @Override
        public void close() {
            synchronized (this) {
                if (released) {
                    return;
                }
                WorkerNativeResourceLedger.this.release(this);
                released = true;
            }
        }
    }
}
