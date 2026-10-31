package com.nereusstream.delay.scheduler;

import java.util.Objects;

/** Explicit process telemetry configuration. It grants no signed SLO or Source authority. */
public record OrdinaryObservationConfiguration(
        Mode mode,
        long generation,
        int maximumSources,
        long maximumContextBytes,
        int maximumStatusAttempts,
        long statusRetryNanos) {
    public enum Mode {
        ASYNC_BEST_EFFORT
    }

    public OrdinaryObservationConfiguration {
        Objects.requireNonNull(mode, "ordinary observation mode");
        if (generation <= 0
                || generation == Long.MAX_VALUE
                || maximumSources <= 0
                || maximumSources == Integer.MAX_VALUE
                || maximumContextBytes < OrdinaryObservationContext.CANONICAL_HEADER_BYTES
                || maximumContextBytes == Long.MAX_VALUE
                || maximumStatusAttempts <= 0
                || maximumStatusAttempts > 16
                || statusRetryNanos <= 0
                || statusRetryNanos == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "ordinary observation requires an explicit generation and finite bounds");
        }
        Math.multiplyExact((long) maximumStatusAttempts, statusRetryNanos);
    }

    public void requireSourceCount(int actualSources) {
        if (actualSources <= 0 || actualSources > maximumSources) {
            throw new IllegalArgumentException("actual ordinary observation Sources exceed the configured bound");
        }
    }

    /** Includes shared context construction/copies and bounded Java event/queue projections. */
    public long maximumRetainedHeapBytes(BoundedAsyncMetricExporter.Limits queue) {
        Objects.requireNonNull(queue, "metric queue");
        return Math.addExact(
                4096L,
                Math.addExact(
                        Math.addExact(
                                queue.maximumPendingBytes(), Math.multiplyExact(256L, queue.maximumPendingRecords())),
                        Math.addExact(
                                Math.multiplyExact(4L, maximumContextBytes),
                                Math.multiplyExact(256L, maximumSources))));
    }
}
