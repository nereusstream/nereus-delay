package com.nereusstream.delay.store;

import java.util.Objects;
import java.util.function.LongSupplier;

/** Shared byte/elapsed budget for one bounded local read action. */
public final class BoundedReadBudget {
    private final long maxBytes;
    private final long maxElapsedNanos;
    private final LongSupplier monotonicClockNanos;
    private final long startedNanos;
    private long lastObservedNanos;
    private long chargedBytes;

    public BoundedReadBudget(final long maxBytes, final long maxElapsedNanos, final LongSupplier monotonicClockNanos) {
        if (maxBytes <= 0 || maxElapsedNanos <= 0) {
            throw new IllegalArgumentException("bounded read limits must be positive");
        }
        this.maxBytes = maxBytes;
        this.maxElapsedNanos = maxElapsedNanos;
        this.monotonicClockNanos = Objects.requireNonNull(monotonicClockNanos, "monotonicClockNanos");
        startedNanos = readClock();
        lastObservedNanos = startedNanos;
    }

    /** Returns false when the elapsed envelope is exhausted before the next read. */
    public boolean beforeRead() {
        final long now = readClock();
        if (now < lastObservedNanos) {
            throw new IllegalStateException("bounded read monotonic clock moved backwards");
        }
        lastObservedNanos = now;
        return now - startedNanos < maxElapsedNanos;
    }

    /**
     * Charges one exact key/value projection. Oversized single records fail
     * closed; a later record that does not fit is left for another turn.
     */
    public boolean tryCharge(final int keyBytes, final int valueBytes) {
        if (keyBytes < 0 || valueBytes < 0) {
            throw new IllegalArgumentException("bounded read byte lengths must be non-negative");
        }
        final long entryBytes;
        try {
            entryBytes = Math.addExact((long) keyBytes, valueBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("bounded read entry byte charge overflow", overflow);
        }
        if (entryBytes > maxBytes) {
            throw new IllegalStateException("bounded read entry exceeds byte budget");
        }
        if (entryBytes > maxBytes - chargedBytes) {
            return false;
        }
        chargedBytes = Math.addExact(chargedBytes, entryBytes);
        return true;
    }

    public long chargedBytes() {
        return chargedBytes;
    }

    public long maxBytes() {
        return maxBytes;
    }

    private long readClock() {
        final long value = monotonicClockNanos.getAsLong();
        if (value < 0) {
            throw new IllegalStateException("bounded read monotonic clock returned a negative value");
        }
        return value;
    }
}
