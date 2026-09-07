package com.nereusstream.delay.runtime;

import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Process-local limits for one complete message head plan, shared by every affected Lane. */
public record HeadReadPolicy(int maxRecords, long maxBytes, long maxElapsedNanos, LongSupplier clockNanos) {
    public HeadReadPolicy(final int maxRecords, final long maxBytes, final long maxElapsedNanos) {
        this(maxRecords, maxBytes, maxElapsedNanos, System::nanoTime);
    }

    public HeadReadPolicy {
        if (maxRecords <= 0 || maxBytes <= 0 || maxElapsedNanos <= 0) {
            throw new IllegalArgumentException("head read limits must be positive");
        }
        Objects.requireNonNull(clockNanos, "clockNanos");
    }

    BoundedReadBudget newBudget() {
        return new BoundedReadBudget(maxRecords, maxBytes, maxElapsedNanos, clockNanos);
    }

    /** Existing constructor compatibility only; this is not a certified activation envelope. */
    static HeadReadPolicy compatibility() {
        return new HeadReadPolicy(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, () -> 0);
    }
}
