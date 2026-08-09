package io.nereusstream.delay.scheduler;

/** Frozen V1 Worker event-loop work classes. */
public enum WorkClass {
    LEASE_FENCE,
    SOURCE_APPLY,
    OUTCOME_AND_CONTROL,
    EXPIRY,
    DUE_SCHEDULER,
    QUERY,
    GC
}
