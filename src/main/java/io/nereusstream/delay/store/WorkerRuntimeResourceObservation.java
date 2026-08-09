package io.nereusstream.delay.store;

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
        long maxFilesystemBytes,
        long usableFilesystemBytes) {
    public WorkerRuntimeResourceObservation {
        if (actualJvmHeapBytes <= 0 || actualMaxDirectMemoryBytes <= 0 || currentProcessRssBytes <= 0
                || effectiveCgroupMemoryLimitBytes <= 0 || maxProcessOpenFiles <= 0
                || maxFilesystemBytes <= 0 || usableFilesystemBytes <= 0) {
            throw new IllegalArgumentException("runtime resource observations must be positive and bounded");
        }
        if (usableFilesystemBytes > maxFilesystemBytes) {
            throw new IllegalArgumentException("usable filesystem bytes exceed filesystem capacity");
        }
    }
}
