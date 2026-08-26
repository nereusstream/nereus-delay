package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.SourcePosition;
import java.util.Objects;

/** Immutable Route policy for the lifetime of a queued receipt query boundary. */
public record QueuedReceiptQueryPolicy(long policyVersion, long queuedReceiptQueryWindowMs) {
    public QueuedReceiptQueryPolicy {
        if (policyVersion <= 0 || queuedReceiptQueryWindowMs <= 0) {
            throw new IllegalArgumentException("queued receipt query policy must be positive and bounded");
        }
    }

    /**
     * Computes the only receipt boundary permitted for this policy. The
     * addition is checked because overflow makes the Route configuration
     * uncertifiable rather than producing a wrapped retention deadline.
     */
    public long queryUntil(final SourcePosition sourcePosition) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        try {
            return Math.addExact(sourcePosition.brokerPersistenceTimeEpochMs(), queuedReceiptQueryWindowMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("queued receipt query boundary overflows epoch milliseconds", overflow);
        }
    }
}
