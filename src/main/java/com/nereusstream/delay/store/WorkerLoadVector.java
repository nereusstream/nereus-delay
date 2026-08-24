package com.nereusstream.delay.store;

import java.util.Objects;

/**
 * Bounded, low-cardinality load observation used only for local placement
 * scoring.  It is not a capacity grant and it never authorizes ownership.
 */
public record WorkerLoadVector(
        long activeMessages,
        long activeBytes,
        long commandIngressRate,
        long duePublishRate,
        long rocksDbLiveBytes,
        long memtableBytes,
        long compactionPendingBytes,
        long walFsyncMillis,
        long stallMillis,
        long checkpointSizeBytes,
        long checkpointAgeMillis,
        long sourceLag,
        long dueLag,
        long laneCount,
        long laneFailures,
        long localDiskBytes) {

    public WorkerLoadVector {
        requireNonNegative(activeMessages, "activeMessages");
        requireNonNegative(activeBytes, "activeBytes");
        requireNonNegative(commandIngressRate, "commandIngressRate");
        requireNonNegative(duePublishRate, "duePublishRate");
        requireNonNegative(rocksDbLiveBytes, "rocksDbLiveBytes");
        requireNonNegative(memtableBytes, "memtableBytes");
        requireNonNegative(compactionPendingBytes, "compactionPendingBytes");
        requireNonNegative(walFsyncMillis, "walFsyncMillis");
        requireNonNegative(stallMillis, "stallMillis");
        requireNonNegative(checkpointSizeBytes, "checkpointSizeBytes");
        requireNonNegative(checkpointAgeMillis, "checkpointAgeMillis");
        requireNonNegative(sourceLag, "sourceLag");
        requireNonNegative(dueLag, "dueLag");
        requireNonNegative(laneCount, "laneCount");
        requireNonNegative(laneFailures, "laneFailures");
        requireNonNegative(localDiskBytes, "localDiskBytes");
    }

    public static WorkerLoadVector empty() {
        return new WorkerLoadVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /**
     * Returns the largest observed-to-ceiling ratio.  A zero ceiling with a
     * non-zero observation is infinite, so a caller cannot silently treat an
     * unmeasured resource as free capacity.
     */
    public double dominantUtilization(final WorkerLoadVector ceilings) {
        Objects.requireNonNull(ceilings, "ceilings");
        double result = 0.0d;
        result = ratio(result, activeMessages, ceilings.activeMessages());
        result = ratio(result, activeBytes, ceilings.activeBytes());
        result = ratio(result, commandIngressRate, ceilings.commandIngressRate());
        result = ratio(result, duePublishRate, ceilings.duePublishRate());
        result = ratio(result, rocksDbLiveBytes, ceilings.rocksDbLiveBytes());
        result = ratio(result, memtableBytes, ceilings.memtableBytes());
        result = ratio(result, compactionPendingBytes, ceilings.compactionPendingBytes());
        result = ratio(result, walFsyncMillis, ceilings.walFsyncMillis());
        result = ratio(result, stallMillis, ceilings.stallMillis());
        result = ratio(result, checkpointSizeBytes, ceilings.checkpointSizeBytes());
        result = ratio(result, checkpointAgeMillis, ceilings.checkpointAgeMillis());
        result = ratio(result, sourceLag, ceilings.sourceLag());
        result = ratio(result, dueLag, ceilings.dueLag());
        result = ratio(result, laneCount, ceilings.laneCount());
        result = ratio(result, laneFailures, ceilings.laneFailures());
        return ratio(result, localDiskBytes, ceilings.localDiskBytes());
    }

    private static double ratio(final double current, final long observed, final long ceiling) {
        if (ceiling == 0) {
            return observed == 0 ? current : Double.POSITIVE_INFINITY;
        }
        return Math.max(current, (double) observed / (double) ceiling);
    }

    private static void requireNonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
