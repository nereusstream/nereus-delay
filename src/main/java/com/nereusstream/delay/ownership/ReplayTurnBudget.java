package com.nereusstream.delay.ownership;

/** Hard limits for one source replay turn. */
public record ReplayTurnBudget(int maxRecords, long maxCanonicalBytes, long maxElapsedNanos) {
    public ReplayTurnBudget {
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive");
        }
        if (maxCanonicalBytes <= 0) {
            throw new IllegalArgumentException("maxCanonicalBytes must be positive");
        }
        if (maxElapsedNanos <= 0) {
            throw new IllegalArgumentException("maxElapsedNanos must be positive");
        }
    }

    /** Compatibility value for the legacy whole-iterable replay methods. */
    public static ReplayTurnBudget unbounded() {
        return new ReplayTurnBudget(Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    }
}
