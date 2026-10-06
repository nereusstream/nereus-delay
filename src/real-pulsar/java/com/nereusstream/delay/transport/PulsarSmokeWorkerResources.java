package com.nereusstream.delay.transport;

import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.HeadReadPolicy;
import com.nereusstream.delay.runtime.PayloadProofTrustSetControlCatalog;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerResourceEnvelope;
import com.nereusstream.delay.store.WorkerRuntimeResourceObservation;
import java.util.concurrent.TimeUnit;

/** Test-only resource and bounded shard fixtures for Broker smokes; neither is production capacity evidence. */
final class PulsarSmokeWorkerResources {
    private static final long MIB = 1L << 20;
    private static final long GIB = 1L << 30;
    private static final long SMOKE_PENDING_MESSAGES = 64;
    private static final int SMOKE_HEAD_READ_RECORDS = 4_096;

    private PulsarSmokeWorkerResources() {}

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

    static DelayShardConfig delayShardConfig() {
        final DelayShardConfig defaults = DelayShardConfig.defaults();
        return new DelayShardConfig(
                defaults.maxDelayHorizonMs(),
                defaults.minDeliveryWindowMs(),
                defaults.maxMessageLifetimeMs(),
                SMOKE_PENDING_MESSAGES,
                defaults.maxPendingBytes(),
                defaults.maxLanes(),
                defaults.inlinePayloadThresholdBytes(),
                defaults.maxPayloadBytes(),
                defaults.maxReservationTtlMs());
    }

    static HeadReadPolicy headReadPolicy() {
        return new HeadReadPolicy(SMOKE_HEAD_READ_RECORDS, 64 * MIB, TimeUnit.SECONDS.toNanos(60));
    }

    static DelayShard openDelayShard(final ShardStore store) {
        return new DelayShard(store, delayShardConfig(), headReadPolicy());
    }

    static DelayShard openDelayShard(final ShardStore store, final ScheduleResolver scheduleResolver) {
        return openDelayShard(store, scheduleResolver, null);
    }

    static DelayShard openDelayShard(
            final ShardStore store,
            final ScheduleResolver scheduleResolver,
            final PayloadProofTrustSetControlCatalog controlCatalog) {
        return new DelayShard(
                store,
                delayShardConfig(),
                null,
                null,
                scheduleResolver,
                controlCatalog,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                headReadPolicy());
    }
}
