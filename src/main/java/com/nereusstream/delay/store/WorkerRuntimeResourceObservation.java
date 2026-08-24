package com.nereusstream.delay.store;

/**
 * Runtime limits and observations used to validate a Worker resource envelope.
 *
 * <p>All values are finite positive byte/count measurements.  A missing,
 * unlimited, or malformed platform value must be rejected by the probe before
 * an observation is constructed; zero is never treated as unlimited.</p>
 */
public record WorkerRuntimeResourceObservation(
        long actualJvmHeapBytes,
        long actualMaxDirectMemoryBytes,
        long currentProcessRssBytes,
        long effectiveCgroupMemoryLimitBytes,
        long maxProcessOpenFiles,
        long currentProcessOpenFiles,
        long maxFilesystemBytes,
        long usableFilesystemBytes) {
    /**
     * Compatibility constructor for embedded callers that predate the
     * process-FD observation.  Embedded callers use a minimal positive value;
     * production probes must use the full constructor so the live
     * {@code /proc/self/fd} count is carried explicitly.
     */
    public WorkerRuntimeResourceObservation(
            final long actualJvmHeapBytes,
            final long actualMaxDirectMemoryBytes,
            final long currentProcessRssBytes,
            final long effectiveCgroupMemoryLimitBytes,
            final long maxProcessOpenFiles,
            final long maxFilesystemBytes,
            final long usableFilesystemBytes) {
        this(
                actualJvmHeapBytes,
                actualMaxDirectMemoryBytes,
                currentProcessRssBytes,
                effectiveCgroupMemoryLimitBytes,
                maxProcessOpenFiles,
                1,
                maxFilesystemBytes,
                usableFilesystemBytes);
    }

    public WorkerRuntimeResourceObservation {
        if (actualJvmHeapBytes <= 0
                || actualMaxDirectMemoryBytes <= 0
                || currentProcessRssBytes <= 0
                || effectiveCgroupMemoryLimitBytes <= 0
                || maxProcessOpenFiles <= 0
                || currentProcessOpenFiles <= 0
                || maxFilesystemBytes <= 0
                || usableFilesystemBytes <= 0) {
            throw new IllegalArgumentException("runtime resource observations must be positive and bounded");
        }
        if (currentProcessOpenFiles > maxProcessOpenFiles) {
            throw new IllegalArgumentException("current process open files exceed the process limit");
        }
        if (usableFilesystemBytes > maxFilesystemBytes) {
            throw new IllegalArgumentException("usable filesystem bytes exceed filesystem capacity");
        }
    }
}
