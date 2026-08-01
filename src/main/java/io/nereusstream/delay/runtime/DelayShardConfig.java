package io.nereusstream.delay.runtime;

/** Deterministic timing and admission limits used by one shard. */
public record DelayShardConfig(
        long maxDelayHorizonMs,
        long minDeliveryWindowMs,
        long maxMessageLifetimeMs) {
    public DelayShardConfig {
        if (maxDelayHorizonMs < 0 || minDeliveryWindowMs < 0 || maxMessageLifetimeMs < 0) {
            throw new IllegalArgumentException("timing limits must be non-negative");
        }
        if (maxDelayHorizonMs > maxMessageLifetimeMs) {
            throw new IllegalArgumentException("delay horizon cannot exceed message lifetime");
        }
    }

    public static DelayShardConfig defaults() {
        return new DelayShardConfig(365L * 24 * 60 * 60 * 1000, 1, 365L * 24 * 60 * 60 * 1000);
    }
}

