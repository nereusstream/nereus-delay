package io.nereusstream.delay.client;

/**
 * Bounded client-side buffering used by the embedded conformance service.
 * Real Broker adapters must expose equivalent Producer/batch/timeout settings
 * from their transport-specific configuration.
 */
public record EmbeddedDelayServiceConfig(int maxPendingCommandCount, long maxPendingCommandBytes) {
    private static final int DEFAULT_MAX_PENDING_COMMAND_COUNT = 1_024;
    private static final long DEFAULT_MAX_PENDING_COMMAND_BYTES = 16L * 1024 * 1024;

    public EmbeddedDelayServiceConfig {
        if (maxPendingCommandCount <= 0 || maxPendingCommandBytes <= 0) {
            throw new IllegalArgumentException("embedded pending command limits must be positive");
        }
    }

    public static EmbeddedDelayServiceConfig defaults() {
        return new EmbeddedDelayServiceConfig(DEFAULT_MAX_PENDING_COMMAND_COUNT,
                DEFAULT_MAX_PENDING_COMMAND_BYTES);
    }
}
