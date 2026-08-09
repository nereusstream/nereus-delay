package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Explicit process/container capacity proof for a multi-DB Worker.
 *
 * <p>This is the configuration-level core of the V1 envelope.  Runtime
 * adapters must populate the values from the actual JVM and cgroup/rlimit
 * observations before opening DBs; zero is deliberately treated as unknown,
 * not as unlimited.</p>
 */
public record WorkerResourceEnvelope(
        long certifiedJvmHeapBytes,
        long maxDirectMemoryBytes,
        long maxRocksDbNativeBytes,
        long maxOtherNativeBytes,
        long minInProcessControlHeadroomBytes,
        long maxProcessRssBytes,
        long minContainerHeadroomBytes,
        long effectiveCgroupMemoryLimitBytes,
        long maxProcessOpenFiles,
        long fdHeadroom,
        long maxFilesystemBytes,
        long physicalDiskSafetyWatermarkBytes,
        long checkpointRestoreTempHeadroomBytes,
        long compactionTempHeadroomBytes,
        long controlReserveBytes,
        long controlReserveRecords) {
    public WorkerResourceEnvelope {
        if (certifiedJvmHeapBytes <= 0 || maxDirectMemoryBytes <= 0 || maxRocksDbNativeBytes <= 0
                || maxOtherNativeBytes <= 0 || minInProcessControlHeadroomBytes < 0 || maxProcessRssBytes < 0
                || minContainerHeadroomBytes < 0 || effectiveCgroupMemoryLimitBytes < 0
                || maxProcessOpenFiles < 0 || fdHeadroom < 0 || maxFilesystemBytes < 0
                || physicalDiskSafetyWatermarkBytes < 0 || checkpointRestoreTempHeadroomBytes < 0
                || compactionTempHeadroomBytes < 0 || controlReserveBytes <= 0 || controlReserveRecords <= 0) {
            throw new IllegalArgumentException("worker resource envelope values must be bounded and positive");
        }
    }

    /** Validates cross-bucket inequalities before any DB is opened. */
    public void validate(final ShardStoreConfig config) {
        Objects.requireNonNull(config, "config");
        if (maxProcessRssBytes == 0 || effectiveCgroupMemoryLimitBytes == 0 || maxProcessOpenFiles == 0
                || maxFilesystemBytes == 0) {
            throw new IllegalArgumentException("runtime memory, FD, or filesystem limit is unknown");
        }
        final long processMemory;
        try {
            processMemory = Math.addExact(Math.addExact(Math.addExact(certifiedJvmHeapBytes,
                            maxDirectMemoryBytes), Math.addExact(maxRocksDbNativeBytes, maxOtherNativeBytes)),
                    minInProcessControlHeadroomBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("worker memory envelope overflows", overflow);
        }
        if (processMemory > maxProcessRssBytes) {
            throw new IllegalArgumentException("certified process memory exceeds maxProcessRssBytes");
        }
        final long configuredRocksDbBudget;
        try {
            // The explicit shared cache and WriteBufferManager budgets are
            // both native-memory reservations. Keep their lower bound inside
            // the certified RocksDB-native bucket before opening any JNI
            // resource; otherwise the aggregate RSS equation could pass while
            // the configured shared budgets already exceed their own bucket.
            configuredRocksDbBudget = Math.addExact(config.sharedBlockCacheBytes(),
                    config.sharedWriteBufferBudgetBytes());
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("configured RocksDB native budget overflows", overflow);
        }
        if (configuredRocksDbBudget > maxRocksDbNativeBytes) {
            throw new IllegalArgumentException("configured RocksDB shared budgets exceed native envelope");
        }
        final long containerMemory;
        try {
            containerMemory = Math.addExact(maxProcessRssBytes, minContainerHeadroomBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("container memory envelope overflows", overflow);
        }
        if (containerMemory > effectiveCgroupMemoryLimitBytes) {
            throw new IllegalArgumentException("process RSS envelope exceeds cgroup memory limit");
        }
        final long usableFds;
        try {
            usableFds = Math.subtractExact(maxProcessOpenFiles, fdHeadroom);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("FD envelope overflows", overflow);
        }
        if (usableFds <= 0 || config.maxTotalOpenFiles() > usableFds) {
            throw new IllegalArgumentException("RocksDB open-file envelope exceeds process FD headroom");
        }
        if (physicalDiskSafetyWatermarkBytes > maxFilesystemBytes) {
            throw new IllegalArgumentException("disk safety watermark exceeds filesystem capacity");
        }
        final long tempHeadroom;
        try {
            tempHeadroom = Math.addExact(checkpointRestoreTempHeadroomBytes, compactionTempHeadroomBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("filesystem temp envelope overflows", overflow);
        }
        if (tempHeadroom > maxFilesystemBytes - physicalDiskSafetyWatermarkBytes) {
            throw new IllegalArgumentException("checkpoint/compaction temp headroom exceeds disk safety margin");
        }
    }

    /**
     * Validates the envelope against authoritative runtime observations before
     * any native Worker resource is opened.
     */
    public void validate(final ShardStoreConfig config, final WorkerRuntimeResourceObservation observation) {
        validate(config);
        Objects.requireNonNull(observation, "observation");
        if (observation.actualJvmHeapBytes() > certifiedJvmHeapBytes) {
            throw new IllegalArgumentException("actual JVM heap limit exceeds certified envelope");
        }
        if (observation.actualMaxDirectMemoryBytes() > maxDirectMemoryBytes) {
            throw new IllegalArgumentException("actual direct-memory limit exceeds certified envelope");
        }
        if (observation.currentProcessRssBytes() > maxProcessRssBytes) {
            throw new IllegalArgumentException("current process RSS exceeds certified envelope");
        }
        if (observation.effectiveCgroupMemoryLimitBytes() < effectiveCgroupMemoryLimitBytes) {
            throw new IllegalArgumentException("certified cgroup memory limit exceeds runtime limit");
        }
        if (observation.maxProcessOpenFiles() < maxProcessOpenFiles) {
            throw new IllegalArgumentException("certified open-file limit exceeds runtime limit");
        }
        if (observation.maxFilesystemBytes() < maxFilesystemBytes) {
            throw new IllegalArgumentException("certified filesystem capacity exceeds runtime capacity");
        }
        if (observation.usableFilesystemBytes() < physicalDiskSafetyWatermarkBytes) {
            throw new IllegalArgumentException("filesystem usable space is below the safety watermark");
        }
    }

    /** Stable digest for placement/configuration identity and audit evidence. */
    public byte[] digest() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-worker-resource-envelope-v1\0"), canonicalBytes());
    }

    public byte[] canonicalBytes() {
        return ByteBuffer.allocate(4 + 16 * 8)
                .putInt(1)
                .putLong(certifiedJvmHeapBytes).putLong(maxDirectMemoryBytes).putLong(maxRocksDbNativeBytes)
                .putLong(maxOtherNativeBytes).putLong(minInProcessControlHeadroomBytes).putLong(maxProcessRssBytes)
                .putLong(minContainerHeadroomBytes).putLong(effectiveCgroupMemoryLimitBytes)
                .putLong(maxProcessOpenFiles).putLong(fdHeadroom).putLong(maxFilesystemBytes)
                .putLong(physicalDiskSafetyWatermarkBytes).putLong(checkpointRestoreTempHeadroomBytes)
                .putLong(compactionTempHeadroomBytes).putLong(controlReserveBytes).putLong(controlReserveRecords)
                .array();
    }
}
