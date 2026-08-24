package com.nereusstream.delay.scheduler;

/** Per-visit and process-level scheduler caps. */
public record SchedulerBudget(int maxMessages, long maxBytes, long maxElapsedNanos) {
    public SchedulerBudget {
        if (maxMessages <= 0 || maxBytes <= 0 || maxElapsedNanos <= 0) {
            throw new IllegalArgumentException("scheduler budget must be positive");
        }
    }
}
