package com.nereusstream.delay.transport;

import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerResourceEnvelope;
import com.nereusstream.delay.store.WorkerRuntimeResourceObservation;

/** Test-only resource envelope for Broker smokes; it is not host-capacity evidence. */
final class KafkaSmokeWorkerResources {
    private static final long MIB = 1L << 20;
    private static final long GIB = 1L << 30;

    private KafkaSmokeWorkerResources() {}

    static SharedRocksDbResources open(final ShardStoreConfig config) {
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
        final var envelope = new WorkerResourceEnvelope(
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
        final var observation = new WorkerRuntimeResourceObservation(
                256 * MIB,
                128 * MIB,
                256 * MIB,
                cgroupLimitBytes,
                maxProcessOpenFiles,
                1,
                filesystemBytes,
                12 * GIB);
        return new SharedRocksDbResources(config, envelope, observation);
    }
}
