package com.nereusstream.delay.runtime;

import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Process-local limits for one complete message head plan, shared by every affected Lane. */
public record HeadReadPolicy(int maxRecords, long maxBytes, long maxElapsedNanos, LongSupplier clockNanos) {
    private static final int MAX_AFFECTED_LANES_PER_MUTATION = 3;
    private static final int LANE_VALUE_READS_PER_LANE = 2;
    private static final int STANDARD_CANDIDATE_NAMESPACES_PER_LANE = 2;
    private static final int NATIVE_CANDIDATE_NAMESPACES_PER_LANE = 1;
    private static final int MAX_PREFIX_RECORDS_PER_NAMESPACE = 2;
    private static final int MESSAGE_READS_PER_CANDIDATE = 1;
    private static final int LEGACY_BINDING_AND_MESSAGE_READS_PER_ACTION_LOOKUP = 2;
    private static final int NATIVE_MESSAGE_READS_PER_CANDIDATE = 1;
    private static final int ADDITIONAL_LEGACY_ACTION_LOOKUPS_PER_MUTATION = 2;

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

    boolean hasFiniteLimits() {
        return maxRecords < Integer.MAX_VALUE && maxBytes < Long.MAX_VALUE && maxElapsedNanos < Long.MAX_VALUE;
    }

    static long maximumMutationPlanRecords(final DelayShardConfig config) {
        Objects.requireNonNull(config, "config");
        final long openAttemptScanLimit = Math.max(config.maxPendingMessages(), config.maxOutcomeReserveRecords());
        try {
            // One legacy DUE/ORDERED candidate scans the bounded open-Attempt set and may then
            // read its ScheduleBinding plus the Message behind it. The included replacement and
            // excluded prior legacy Message can each need one additional actionAt lookup.
            final long legacyActionLookup = Math.addExact(
                    openAttemptScanLimit, LEGACY_BINDING_AND_MESSAGE_READS_PER_ACTION_LOOKUP);
            final long legacyStandardCandidate = Math.addExact(
                    Math.addExact(MAX_PREFIX_RECORDS_PER_NAMESPACE, MESSAGE_READS_PER_CANDIDATE),
                    legacyActionLookup);
            final long nativeCandidate = Math.addExact(
                    MAX_PREFIX_RECORDS_PER_NAMESPACE, NATIVE_MESSAGE_READS_PER_CANDIDATE);
            final long perLane = Math.addExact(
                    LANE_VALUE_READS_PER_LANE,
                    Math.addExact(
                            Math.multiplyExact(STANDARD_CANDIDATE_NAMESPACES_PER_LANE, legacyStandardCandidate),
                            Math.multiplyExact(NATIVE_CANDIDATE_NAMESPACES_PER_LANE, nativeCandidate)));
            return Math.addExact(
                    Math.multiplyExact(MAX_AFFECTED_LANES_PER_MUTATION, perLane),
                    Math.multiplyExact(ADDITIONAL_LEGACY_ACTION_LOOKUPS_PER_MUTATION, legacyActionLookup));
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    boolean canReadMaximumMutationPlanRecords(final DelayShardConfig config) {
        return maxRecords >= maximumMutationPlanRecords(config);
    }

    /** Existing constructor compatibility only; this is not a certified activation envelope. */
    static HeadReadPolicy compatibility() {
        return new HeadReadPolicy(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, () -> 0);
    }
}
