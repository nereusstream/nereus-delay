package com.nereusstream.delay.store;

import java.util.Objects;
import java.util.function.LongSupplier;

/** Shared byte/elapsed budget for one bounded local read action. */
public final class BoundedReadBudget {
    private final long maxBytes;
    private final int maxRecords;
    private final long maxElapsedNanos;
    private final LongSupplier monotonicClockNanos;
    private final long startedNanos;
    private long lastObservedNanos;
    private long chargedBytes;
    private long actualBytes;
    private long actualRecords;
    private long deniedReads;
    private Exhaustion exhaustion;

    public enum Exhaustion {
        RECORDS,
        BYTES,
        ELAPSED
    }

    public BoundedReadBudget(final long maxBytes, final long maxElapsedNanos, final LongSupplier monotonicClockNanos) {
        this(Integer.MAX_VALUE, maxBytes, maxElapsedNanos, monotonicClockNanos);
    }

    public BoundedReadBudget(
            final int maxRecords,
            final long maxBytes,
            final long maxElapsedNanos,
            final LongSupplier monotonicClockNanos) {
        if (maxRecords <= 0 || maxBytes <= 0 || maxElapsedNanos <= 0) {
            throw new IllegalArgumentException("bounded read limits must be positive");
        }
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.maxElapsedNanos = maxElapsedNanos;
        this.monotonicClockNanos = Objects.requireNonNull(monotonicClockNanos, "monotonicClockNanos");
        startedNanos = readClock();
        lastObservedNanos = startedNanos;
    }

    /** Checks all shared limits before another seek, entry or point read is initiated. */
    public boolean beforeRead() {
        final long now = observeClock();
        if (exhaustion == null) {
            if (now - startedNanos >= maxElapsedNanos) {
                exhaustion = Exhaustion.ELAPSED;
            } else if (actualRecords >= maxRecords) {
                exhaustion = Exhaustion.RECORDS;
            } else if (chargedBytes >= maxBytes) {
                exhaustion = Exhaustion.BYTES;
            }
        }
        if (exhaustion != null) {
            deniedReads++;
            return false;
        }
        return true;
    }

    /**
     * Deadline check for completing an already-read projection outside the Store
     * lock. A physical-read byte/record limit does not invalidate completed inputs.
     */
    public boolean beforeTimedWork() {
        if (observeClock() - startedNanos >= maxElapsedNanos) {
            if (exhaustion == null) {
                exhaustion = Exhaustion.ELAPSED;
            }
            return false;
        }
        return true;
    }

    private long observeClock() {
        final long now = readClock();
        if (now < lastObservedNanos) {
            throw new IllegalStateException("bounded read monotonic clock moved backwards");
        }
        lastObservedNanos = now;
        return now;
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
        actualBytes = Math.addExact(actualBytes, entryBytes);
        actualRecords = Math.addExact(actualRecords, 1);
        if (entryBytes > maxBytes) {
            throw new IllegalStateException("bounded read entry exceeds byte budget");
        }
        if (exhaustion != null || actualRecords > maxRecords || entryBytes > maxBytes - chargedBytes) {
            if (exhaustion == null) {
                exhaustion = actualRecords > maxRecords ? Exhaustion.RECORDS : Exhaustion.BYTES;
            }
            deniedReads++;
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

    /** Includes a key/value returned by the store even when it did not fit the remaining allowance. */
    public long actualBytes() {
        return actualBytes;
    }

    public long actualRecords() {
        return actualRecords;
    }

    public long deniedReads() {
        return deniedReads;
    }

    public Exhaustion exhaustion() {
        return exhaustion;
    }

    public ReadIncompleteException incomplete() {
        if (exhaustion == null) {
            throw new IllegalStateException("read budget has not been exhausted");
        }
        return new ReadIncompleteException(exhaustion);
    }

    private long readClock() {
        final long value = monotonicClockNanos.getAsLong();
        if (value < 0) {
            throw new IllegalStateException("bounded read monotonic clock returned a negative value");
        }
        return value;
    }
}
