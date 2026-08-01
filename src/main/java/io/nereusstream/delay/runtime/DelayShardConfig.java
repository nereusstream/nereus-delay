package io.nereusstream.delay.runtime;

/** Deterministic timing and admission limits used by one shard. */
public record DelayShardConfig(
        long maxDelayHorizonMs,
        long minDeliveryWindowMs,
        long maxMessageLifetimeMs,
        long maxPendingMessages,
        long maxPendingBytes,
        long maxLanes,
        long inlinePayloadThresholdBytes,
        long maxPayloadBytes,
        long maxReservationTtlMs) {
    public DelayShardConfig {
        if (maxDelayHorizonMs < 0 || minDeliveryWindowMs < 0 || maxMessageLifetimeMs < 0
                || maxPendingMessages <= 0 || maxPendingBytes <= 0 || maxLanes <= 0
                || inlinePayloadThresholdBytes < 0 || maxPayloadBytes <= 0 || maxReservationTtlMs <= 0) {
            throw new IllegalArgumentException("timing limits must be non-negative");
        }
        if (maxDelayHorizonMs > maxMessageLifetimeMs) {
            throw new IllegalArgumentException("delay horizon cannot exceed message lifetime");
        }
        if (inlinePayloadThresholdBytes > maxPayloadBytes) {
            throw new IllegalArgumentException("inline payload threshold cannot exceed max payload");
        }
    }

    public static DelayShardConfig defaults() {
        return new DelayShardConfig(365L * 24 * 60 * 60 * 1000, 1, 365L * 24 * 60 * 60 * 1000,
                1_000_000, 1L << 30, 10_000, 1L << 20, 1L << 32, 24L * 60 * 60 * 1000);
    }
}
