package com.nereusstream.delay.store;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Process-local fixed-window export budget for one SLO outbox.
 *
 * <p>The budget limits export attempts by both record count and encoded bytes
 * in each monotonic one-second window. It is deliberately not durable: a
 * restart may start a fresh window, while the durable outbox and collector
 * digest acknowledgement remain the correctness boundary.</p>
 */
public final class SloObservationOutboxExportRate {
    private static final long WINDOW_NANOS = 1_000_000_000L;

    private final Limits limits;
    private final LongSupplier monotonicNanos;
    private boolean initialized;
    private long windowStartNanos;
    private long lastNowNanos;
    private long exportedRecords;
    private long exportedBytes;

    public SloObservationOutboxExportRate(final Limits limits) {
        this(limits, System::nanoTime);
    }

    SloObservationOutboxExportRate(final Limits limits, final LongSupplier monotonicNanos) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
    }

    /** Attempts to charge one bounded export turn to the current window. */
    public synchronized boolean tryAcquire(final int records, final long bytes) {
        if (records < 0 || bytes < 0) {
            throw new IllegalArgumentException("SLO export usage cannot be negative");
        }
        final long now = monotonicNanos.getAsLong();
        if (!initialized) {
            windowStartNanos = now;
            initialized = true;
        } else if (now < lastNowNanos) {
            throw new IllegalStateException("SLO export clock regressed");
        } else {
            final long elapsed;
            try {
                elapsed = Math.subtractExact(now, windowStartNanos);
            } catch (ArithmeticException overflow) {
                throw new IllegalStateException("SLO export clock arithmetic overflow", overflow);
            }
            if (elapsed >= WINDOW_NANOS) {
                windowStartNanos = now;
                exportedRecords = 0;
                exportedBytes = 0;
            }
        }
        lastNowNanos = now;
        final long nextRecords;
        final long nextBytes;
        try {
            nextRecords = Math.addExact(exportedRecords, records);
            nextBytes = Math.addExact(exportedBytes, bytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("SLO export rate arithmetic overflow", overflow);
        }
        if (nextRecords > limits.maxRecordsPerSecond() || nextBytes > limits.maxBytesPerSecond()) {
            return false;
        }
        exportedRecords = nextRecords;
        exportedBytes = nextBytes;
        return true;
    }

    public synchronized Usage usage() {
        return new Usage(exportedRecords, exportedBytes);
    }

    public record Limits(long maxRecordsPerSecond, long maxBytesPerSecond) {
        public Limits {
            if (maxRecordsPerSecond <= 0 || maxBytesPerSecond <= 0) {
                throw new IllegalArgumentException("SLO export rate limits must be positive");
            }
        }
    }

    public record Usage(long records, long bytes) {
        public Usage {
            if (records < 0 || bytes < 0) {
                throw new IllegalArgumentException("SLO export usage cannot be negative");
            }
        }
    }
}
