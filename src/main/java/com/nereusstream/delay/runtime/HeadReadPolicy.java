package com.nereusstream.delay.runtime;

import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Process-local limits for one complete message head plan, shared by every affected Lane. */
public record HeadReadPolicy(int maxRecords, long maxBytes, long maxElapsedNanos, LongSupplier clockNanos) {
    // One mutation can project the old Message Lane, replacement Lane and reservation Lane. Per Lane,
    // it reads the Lane envelope twice and probes three candidate namespaces; each probe can visit
    // one excluded old head plus one retained candidate, whose validator point-reads one Message.
    private static final int MAX_AFFECTED_LANES_PER_MUTATION = 3;
    private static final int LANE_VALUE_READS_PER_LANE = 2;
    private static final int CANDIDATE_NAMESPACES_PER_LANE = 3;
    private static final int MAX_PREFIX_RECORDS_PER_NAMESPACE = 2;
    private static final int MAX_CANDIDATE_MESSAGE_READS_PER_NAMESPACE = 1;
    static final int MAX_MUTATION_PLAN_RECORDS = MAX_AFFECTED_LANES_PER_MUTATION
            * (LANE_VALUE_READS_PER_LANE
                    + CANDIDATE_NAMESPACES_PER_LANE
                            * (MAX_PREFIX_RECORDS_PER_NAMESPACE + MAX_CANDIDATE_MESSAGE_READS_PER_NAMESPACE));

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

    boolean canReadMaximumMutationPlanRecords() {
        return maxRecords >= MAX_MUTATION_PLAN_RECORDS;
    }

    /** Existing constructor compatibility only; this is not a certified activation envelope. */
    static HeadReadPolicy compatibility() {
        return new HeadReadPolicy(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, () -> 0);
    }
}
