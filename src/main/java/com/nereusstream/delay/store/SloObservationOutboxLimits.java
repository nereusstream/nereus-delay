package com.nereusstream.delay.store;

/**
 * Bounded local capacity envelope for one shard's SLO observation outbox.
 *
 * <p>The envelope covers the durable records and their encoded
 * {@link ValueEnvelope} bytes. It is deliberately separate from the
 * collector/export rate policy: reaching this bound is a local evidence
 * capacity failure, not permission to drop or silently shrink the SLO
 * denominator.</p>
 */
public record SloObservationOutboxLimits(int maxRecords, long maxBytes) {
    public SloObservationOutboxLimits {
        if (maxRecords <= 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("SLO outbox limits must be positive");
        }
    }
}
