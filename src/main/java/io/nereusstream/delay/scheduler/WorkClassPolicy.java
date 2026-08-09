package io.nereusstream.delay.scheduler;

/** Bounded queue and turn policy for one Worker work class. */
public record WorkClassPolicy(
        int weight,
        int maxQueueRecords,
        long maxQueueBytes,
        int maxRecordsPerTurn,
        long maxBytesPerTurn,
        long maxTimePerTurnNanos,
        long nonBorrowableMinimumRecords,
        long nonBorrowableMinimumBytes,
        boolean preemptive) {
    public WorkClassPolicy {
        if (weight <= 0 || maxQueueRecords <= 0 || maxQueueBytes <= 0
                || maxRecordsPerTurn <= 0 || maxBytesPerTurn <= 0 || maxTimePerTurnNanos <= 0
                || nonBorrowableMinimumRecords < 0 || nonBorrowableMinimumBytes < 0) {
            throw new IllegalArgumentException("work-class limits must be positive and bounded");
        }
    }
}
