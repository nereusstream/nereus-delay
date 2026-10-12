package com.nereusstream.delay.store;

/** Test-only Worker resource fixture with a synthetic, explicitly non-host observation. */
public final class WorkerRuntimeTestSupport {
    private static final long MIB = 1L << 20;
    private static final long GIB = 1L << 30;

    private WorkerRuntimeTestSupport() {}

    public static SharedRocksDbResources openWithSyntheticObservation(final ShardStoreConfig config) {
        final var envelope = envelope(config);
        final var observation = new WorkerRuntimeResourceObservation(
                256 * MIB, 128 * MIB, 256 * MIB, envelope.effectiveCgroupMemoryLimitBytes(),
                envelope.maxProcessOpenFiles(), 1, envelope.maxFilesystemBytes(), 12 * GIB);
        return new SharedRocksDbResources(config, envelope, observation);
    }

    /** Exact numeric fixture reused by configured Host tests; never a real-host observation or capacity declaration. */
    public static WorkerResourceEnvelope envelope(final ShardStoreConfig config) {
        final long rocksDbNativeBytes =
                Math.addExact(config.sharedBlockCacheBytes(), config.sharedWriteBufferBudgetBytes());
        final long heapBytes = 512 * MIB;
        final long directBytes = 256 * MIB;
        final long otherNativeBytes = 256 * MIB;
        final long inProcessHeadroomBytes = 64 * MIB;
        final long processRssBytes = Math.addExact(
                Math.addExact(
                        Math.addExact(heapBytes, directBytes), Math.addExact(rocksDbNativeBytes, otherNativeBytes)),
                inProcessHeadroomBytes);
        final long containerHeadroomBytes = 128 * MIB;
        final long cgroupLimitBytes = Math.addExact(processRssBytes, containerHeadroomBytes);
        final long maxProcessOpenFiles = Math.addExact(config.maxTotalOpenFiles(), 10_000L);
        final long filesystemBytes = 16 * GIB;
        return new WorkerResourceEnvelope(
                heapBytes,
                directBytes,
                rocksDbNativeBytes,
                otherNativeBytes,
                inProcessHeadroomBytes,
                processRssBytes,
                containerHeadroomBytes,
                cgroupLimitBytes,
                maxProcessOpenFiles,
                1_024,
                filesystemBytes,
                2 * GIB,
                GIB,
                GIB,
                64 * MIB,
                10_000);
    }
}
