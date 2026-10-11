package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.WorkerResourceEnvelope;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Closed explicit properties schema. Missing/unknown/unbounded values fail before resource allocation. */
public final class TargetWorkerRuntimeConfigLoader {
    private TargetWorkerRuntimeConfigLoader() {}

    public static TargetWorkerRuntimeConfig load(Map<String, String> values) {
        final var f = new Fields(values);
        final var store = new ShardStoreConfig(Path.of(f.text("worker.rootPath")),
                f.integer("worker.maxOwnedShards"), f.integer("worker.maxOpenShardDbs"),
                f.integer("worker.maxOpenFilesPerDb"), f.integer("worker.maxTotalOpenFiles"),
                f.integer("worker.maxBackgroundJobs"), f.number("worker.sharedBlockCacheBytes"),
                f.number("worker.sharedWriteBufferBudgetBytes"), f.integer("recovery.checkpointCreates"),
                f.integer("recovery.checkpointUploads"), f.integer("recovery.checkpointDownloads"),
                f.number("recovery.checkpointIoBytesPerSecond"), f.integer("recovery.drains"),
                f.number("worker.maxWriteBufferBytesPerDb"), f.integer("worker.reservedFlushJobs"),
                f.integer("worker.maxCompactionJobs"), f.integer("worker.maxBackgroundJobsPerDb"),
                f.integer("worker.maxConcurrentAcquires"));
        final var resources = new WorkerResourceEnvelope(f.number("worker.heapBytes"),
                f.number("worker.directBytes"), f.number("worker.rocksDbNativeBytes"),
                f.number("worker.otherNativeBytes"), f.number("worker.controlMemoryHeadroomBytes"),
                f.number("worker.processRssBytes"), f.number("worker.containerHeadroomBytes"),
                f.number("worker.cgroupBytes"), f.number("worker.processOpenFiles"), f.number("worker.fdHeadroom"),
                f.number("worker.filesystemBytes"), f.number("worker.diskSafetyWatermarkBytes"),
                f.number("recovery.checkpointRestoreTempBytes"), f.number("recovery.compactionTempBytes"),
                f.number("worker.controlReserveBytes"), f.number("worker.controlReserveRecords"));
        final long[] caps = new long[CapacityDimension.COUNT];
        for (final var dimension : CapacityDimension.values()) {
            caps[dimension.wireValue() - 1] = f.number("worker.hardCap." + dimension.name());
        }
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (final var workClass : WorkClass.values()) {
            final String key = "scheduling.class." + workClass.name() + '.';
            policies.put(workClass, new WorkClassPolicy(f.integer(key + "weight"),
                    f.integer(key + "queueRecords"), f.number(key + "queueBytes"), f.integer(key + "turnRecords"),
                    f.number(key + "turnBytes"), f.number(key + "turnNanos"), f.number(key + "minimumRecords"),
                    f.number(key + "minimumBytes"), workClass == WorkClass.LEASE_FENCE));
        }
        final var workClasses = new WorkClassRuntimeConfig(policies, f.number("scheduling.maxClassDelayNanos"),
                f.number("scheduling.maxResourceHoldNanos"), f.number("scheduling.poolRecords"),
                f.number("scheduling.poolBytes"));
        final var source = new TargetSourceApplyRuntime.Limits(f.integer("target.sourceReadRecords"),
                f.number("target.sourceReadBytes"), f.number("target.sourceReadNanos"),
                f.integer("target.maximumAffectedCounters"), f.integer("target.maximumDomains"));
        final var inventory = new TargetWorkerTargetInventory.Limits(store.maxOwnedShards(),
                f.integer("target.maximumWorkerTargets"), f.integer("target.inventoryPageTargets"),
                f.integer("target.inventoryPagesPerShard"), f.integer("target.inventoryReadRecords"),
                f.number("target.inventoryReadBytes"), f.number("target.inventoryReadNanos"));
        final var ordinary = new TargetWorkerOrdinaryDrr.Limits(f.number("scheduling.quantumBytes"),
                f.number("scheduling.maximumCostBytes"), f.number("scheduling.sendEnvelopeBytes"),
                f.integer("scheduling.maximumVisits"), f.integer("scheduling.readRecords"),
                f.number("scheduling.readBytes"), f.number("scheduling.readNanos"));
        final var turn = new SchedulerBudget(f.integer("scheduling.turnMessages"),
                f.number("scheduling.turnBytes"), f.number("scheduling.turnNanos"));
        final var config = new TargetWorkerRuntimeConfig(store, resources, new CapacityVector(caps), workClasses,
                source, f.number("target.maximumSourceFrameBytes"), inventory, ordinary, turn,
                Duration.ofNanos(f.number("scheduling.recheckNanos")),
                new SchedulerBudget(f.integer("recovery.maintenanceTurnMessages"),
                        f.number("recovery.maintenanceTurnBytes"), f.number("recovery.maintenanceTurnNanos")),
                Duration.ofNanos(f.number("recovery.maintenanceIntervalNanos")));
        f.requireComplete();
        return config;
    }

    private static final class Fields {
        private final Map<String, String> remaining;

        Fields(Map<String, String> values) {
            remaining = new HashMap<>(Objects.requireNonNull(values, "values"));
        }

        String text(String key) {
            final String value = remaining.remove(key);
            if (value == null || value.isBlank() || !value.equals(value.trim())) {
                throw new IllegalArgumentException("missing or malformed configuration: " + key);
            }
            return value;
        }

        long number(String key) {
            final String value = text(key);
            if (!value.chars().allMatch(Character::isDigit)) {
                throw new IllegalArgumentException("configuration requires an explicit decimal bound: " + key);
            }
            try {
                final long parsed = Long.parseLong(value);
                if (parsed == Long.MAX_VALUE) { throw new NumberFormatException("unbounded sentinel"); }
                return parsed;
            } catch (NumberFormatException invalid) {
                throw new IllegalArgumentException("configuration bound overflows: " + key, invalid);
            }
        }

        int integer(String key) {
            final long value = number(key);
            if (value >= Integer.MAX_VALUE) {
                throw new IllegalArgumentException("configuration count is unbounded: " + key);
            }
            return Math.toIntExact(value);
        }

        void requireComplete() {
            if (!remaining.isEmpty()) {
                throw new IllegalArgumentException("unknown Target Worker configuration keys: "
                        + remaining.keySet().stream().sorted().toList());
            }
        }
    }
}
