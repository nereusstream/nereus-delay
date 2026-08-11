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
        long maxReservationTtlMs,
        int maxPublishAdmissions,
        int maxUncertainRetries,
        long maxOutcomeReserveBytes,
        long maxOutcomeReserveRecords,
        long maxIngressBrokerTimestampDivergenceMs,
        long maximumAdmissionMutationEnqueueAgeMs,
        long commandRetryWindowMs,
        long maximumPreparationAgeMs,
        long maximumUuidFutureSkewMs,
        long timeFenceSafetyMarginMs) {
    /**
     * Compatibility constructor for the pre-retry-policy embedded config.
     * The safety cap follows the shard's pending-message bound until a pinned
     * RetryPolicySemanticV1 supplies a smaller per-generation value.
     */
    public DelayShardConfig(final long maxDelayHorizonMs, final long minDeliveryWindowMs,
                            final long maxMessageLifetimeMs, final long maxPendingMessages,
                            final long maxPendingBytes, final long maxLanes,
                            final long inlinePayloadThresholdBytes, final long maxPayloadBytes,
                            final long maxReservationTtlMs) {
        this(maxDelayHorizonMs, minDeliveryWindowMs, maxMessageLifetimeMs, maxPendingMessages,
                maxPendingBytes, maxLanes, inlinePayloadThresholdBytes, maxPayloadBytes,
                maxReservationTtlMs, boundedAdmissionCap(maxPendingMessages), 0,
                defaultOutcomeReserveBytes(maxPendingBytes), defaultOutcomeReserveRecords(
                        boundedAdmissionCap(maxPendingMessages)), 0, maxMessageLifetimeMs, 0, 0, 0, 0);
    }

    /** Compatibility constructor for callers that already specify admission budgets. */
    public DelayShardConfig(final long maxDelayHorizonMs, final long minDeliveryWindowMs,
                            final long maxMessageLifetimeMs, final long maxPendingMessages,
                            final long maxPendingBytes, final long maxLanes,
                            final long inlinePayloadThresholdBytes, final long maxPayloadBytes,
                            final long maxReservationTtlMs, final int maxPublishAdmissions,
                            final int maxUncertainRetries) {
        this(maxDelayHorizonMs, minDeliveryWindowMs, maxMessageLifetimeMs, maxPendingMessages,
                maxPendingBytes, maxLanes, inlinePayloadThresholdBytes, maxPayloadBytes,
                maxReservationTtlMs, maxPublishAdmissions, maxUncertainRetries,
                defaultOutcomeReserveBytes(maxPendingBytes), defaultOutcomeReserveRecords(maxPublishAdmissions), 0,
                maxMessageLifetimeMs, 0, 0, 0, 0);
    }

    /** Compatibility constructor for callers that specify the complete pre-broker-time config. */
    public DelayShardConfig(final long maxDelayHorizonMs, final long minDeliveryWindowMs,
                            final long maxMessageLifetimeMs, final long maxPendingMessages,
                            final long maxPendingBytes, final long maxLanes,
                            final long inlinePayloadThresholdBytes, final long maxPayloadBytes,
                            final long maxReservationTtlMs, final int maxPublishAdmissions,
                            final int maxUncertainRetries, final long maxOutcomeReserveBytes,
                            final long maxOutcomeReserveRecords) {
        this(maxDelayHorizonMs, minDeliveryWindowMs, maxMessageLifetimeMs, maxPendingMessages,
                maxPendingBytes, maxLanes, inlinePayloadThresholdBytes, maxPayloadBytes,
                maxReservationTtlMs, maxPublishAdmissions, maxUncertainRetries, maxOutcomeReserveBytes,
                maxOutcomeReserveRecords, 0, maxMessageLifetimeMs, 0, 0, 0, 0);
    }

    /**
     * Compatibility constructor for callers that specify the complete
     * pre-broker-time config. Strict command-identity timing is disabled until
     * the Route supplies its immutable ingress policy.
     */
    public DelayShardConfig(final long maxDelayHorizonMs, final long minDeliveryWindowMs,
                            final long maxMessageLifetimeMs, final long maxPendingMessages,
                            final long maxPendingBytes, final long maxLanes,
                            final long inlinePayloadThresholdBytes, final long maxPayloadBytes,
                            final long maxReservationTtlMs, final int maxPublishAdmissions,
                            final int maxUncertainRetries, final long maxOutcomeReserveBytes,
                            final long maxOutcomeReserveRecords, final long maxIngressBrokerTimestampDivergenceMs,
                            final long maximumAdmissionMutationEnqueueAgeMs) {
        this(maxDelayHorizonMs, minDeliveryWindowMs, maxMessageLifetimeMs, maxPendingMessages,
                maxPendingBytes, maxLanes, inlinePayloadThresholdBytes, maxPayloadBytes,
                maxReservationTtlMs, maxPublishAdmissions, maxUncertainRetries, maxOutcomeReserveBytes,
                maxOutcomeReserveRecords, maxIngressBrokerTimestampDivergenceMs,
                maximumAdmissionMutationEnqueueAgeMs, 0, 0, 0, 0);
    }

    public DelayShardConfig {
        if (maxDelayHorizonMs < 0 || minDeliveryWindowMs < 0 || maxMessageLifetimeMs < 0
                || maxPendingMessages <= 0 || maxPendingBytes <= 0 || maxLanes <= 0
                || inlinePayloadThresholdBytes < 0 || maxPayloadBytes <= 0 || maxReservationTtlMs <= 0
                || maxPublishAdmissions <= 0 || maxUncertainRetries < 0
                || maxUncertainRetries >= maxPublishAdmissions || maxOutcomeReserveBytes <= 0
                || maxOutcomeReserveRecords <= 0 || maxIngressBrokerTimestampDivergenceMs < 0
                || maximumAdmissionMutationEnqueueAgeMs < 0 || commandRetryWindowMs < 0
                || maximumPreparationAgeMs < 0 || maximumUuidFutureSkewMs < 0
                || timeFenceSafetyMarginMs < 0) {
            throw new IllegalArgumentException("timing limits must be non-negative");
        }
        final boolean strictIdentityPolicy = commandRetryWindowMs != 0 || maximumPreparationAgeMs != 0
                || maximumUuidFutureSkewMs != 0;
        if (strictIdentityPolicy && (commandRetryWindowMs <= 0 || maximumPreparationAgeMs <= 0)) {
            throw new IllegalArgumentException(
                    "strict identity policy requires positive command retry and preparation windows");
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
                1_000_000, 1L << 30, 10_000, 1L << 20, 1L << 32, 24L * 60 * 60 * 1000,
                1_000_000, 0, 64L << 20, 4_000_000, 0, 365L * 24 * 60 * 60 * 1000);
    }

    private static int boundedAdmissionCap(final long maxPendingMessages) {
        if (maxPendingMessages <= 0) {
            throw new IllegalArgumentException("maxPendingMessages must be positive");
        }
        return maxPendingMessages >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxPendingMessages;
    }

    private static long defaultOutcomeReserveBytes(final long maxPendingBytes) {
        return Math.max(1L << 20, Math.min(maxPendingBytes, 64L << 20));
    }

    private static long defaultOutcomeReserveRecords(final int maxPublishAdmissions) {
        return Math.max(256L, Math.multiplyExact((long) maxPublishAdmissions, 4L));
    }
}
