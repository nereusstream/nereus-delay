package io.nereusstream.delay.scheduler;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Complete required configuration for the V1 Worker work-class runtime.
 *
 * <p>No benchmark-derived default is provided. A release configuration must
 * explicitly cover all eight frozen classes, protect the correctness classes'
 * record and byte minima, and fit their aggregate minima within the shared
 * pool before an event loop can be constructed.</p>
 */
public record WorkClassRuntimeConfig(
        Map<WorkClass, WorkClassPolicy> policies,
        long maxEventLoopClassDelayNanos,
        long maxBorrowedResourceHoldNanos,
        long totalResourceRecords,
        long totalResourceBytes) {
    private static final EnumSet<WorkClass> CORRECTNESS_MINIMUM_CLASSES = EnumSet.of(
            WorkClass.LEASE_FENCE,
            WorkClass.SOURCE_APPLY,
            WorkClass.OUTCOME_AND_CONTROL,
            WorkClass.EXPIRY,
            WorkClass.DUE_SCHEDULER,
            WorkClass.GC);

    public WorkClassRuntimeConfig {
        Objects.requireNonNull(policies, "policies");
        if (!EnumSet.allOf(WorkClass.class).equals(policies.keySet())) {
            throw new IllegalArgumentException("work-class runtime policies must cover every V1 class exactly");
        }
        if (maxEventLoopClassDelayNanos <= 0 || maxBorrowedResourceHoldNanos <= 0
                || totalResourceRecords <= 0 || totalResourceBytes <= 0) {
            throw new IllegalArgumentException("work-class runtime limits must be positive");
        }

        final EnumMap<WorkClass, WorkClassPolicy> copied = new EnumMap<>(WorkClass.class);
        long minimumRecords = 0;
        long minimumBytes = 0;
        try {
            for (WorkClass workClass : WorkClass.values()) {
                final WorkClassPolicy policy = Objects.requireNonNull(policies.get(workClass),
                        "policy for " + workClass);
                if (policy.preemptive() != (workClass == WorkClass.LEASE_FENCE)) {
                    throw new IllegalArgumentException("only LEASE_FENCE is preemptive in V1");
                }
                if (CORRECTNESS_MINIMUM_CLASSES.contains(workClass)
                        && (policy.nonBorrowableMinimumRecords() == 0
                        || policy.nonBorrowableMinimumBytes() == 0)) {
                    throw new IllegalArgumentException("work class requires non-borrowable record and byte minima: "
                            + workClass);
                }
                minimumRecords = Math.addExact(minimumRecords, policy.nonBorrowableMinimumRecords());
                minimumBytes = Math.addExact(minimumBytes, policy.nonBorrowableMinimumBytes());
                copied.put(workClass, policy);
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("work-class non-borrowable minima overflow", overflow);
        }
        if (minimumRecords > totalResourceRecords || minimumBytes > totalResourceBytes) {
            throw new IllegalArgumentException("work-class non-borrowable minima exceed the shared pool");
        }
        policies = Map.copyOf(copied);
    }

    /** Constructs the bounded event-loop/resource composition from this exact configuration. */
    public WorkClassEventLoop newEventLoop(final LongSupplier monotonicClockNanos) {
        final LongSupplier clock = Objects.requireNonNull(monotonicClockNanos, "monotonicClockNanos");
        return new WorkClassEventLoop(
                new WorkClassScheduler(policies, maxEventLoopClassDelayNanos, clock),
                new WorkClassResourcePool(policies, totalResourceRecords, totalResourceBytes,
                        maxBorrowedResourceHoldNanos, clock));
    }

    /** Constructs a complete handler dispatcher over the configured event loop. */
    public WorkClassDispatcher newDispatcher(
            final LongSupplier monotonicClockNanos,
            final Map<WorkClass, ? extends Consumer<WorkClassTask>> handlers) {
        return new WorkClassDispatcher(newEventLoop(monotonicClockNanos), handlers);
    }
}
