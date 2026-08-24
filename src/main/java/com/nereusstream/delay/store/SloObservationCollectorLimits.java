package com.nereusstream.delay.store;

/**
 * Bounded local memory envelope for one SLO collector projection.
 *
 * <p>The byte limit counts the canonical {@code SloObservationOutboxV1}
 * projection currently retained for each sample.  It is a safety bound, not
 * permission to drop samples: a merge that would exceed it fails closed and
 * leaves the previous projection unchanged.</p>
 */
public record SloObservationCollectorLimits(int maxSamples, long maxCanonicalBytes) {
    public SloObservationCollectorLimits {
        if (maxSamples <= 0 || maxCanonicalBytes <= 0) {
            throw new IllegalArgumentException("SLO collector limits must be positive");
        }
    }
}
