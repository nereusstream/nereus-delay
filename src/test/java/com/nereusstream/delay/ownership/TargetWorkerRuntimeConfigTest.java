package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerRuntimeResourceObservation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetWorkerRuntimeConfigTest {
    @TempDir
    Path root;

    @Test
    void explicitSchemaDerivesOneOwnershipBoundaryAndSeparatesReadAndActionCosts() {
        final var loaded = TargetWorkerRuntimeConfigLoader.load(values());
        assertEquals(loaded.store().maxOwnedShards(), loaded.inventory().maximumShards());
        assertEquals(64, loaded.source().records());
        assertEquals(1, loaded.workClasses().policies().get(WorkClass.SOURCE_APPLY).maxRecordsPerTurn());
        assertEquals(1, loaded.sourceTurn().maxMessages());
        assertEquals(1024, loaded.sourceTurn().maxBytes());
        assertEquals(8, loaded.maximumTargetSummaries());
        assertEquals(32, loaded.maximumDomainHeads());
        loaded.newWorkClasses(() -> 0);
        loaded.requireCapacity(java.util.List.of(), CapacityVector.empty(), CapacityVector.empty());
        assertEquals(false, Files.exists(root.resolve("db")));
    }

    @Test
    void missingUnknownUnlimitedAndOvercommittedConfigurationRejectsBeforeIo() {
        for (String missing : java.util.List.of("worker.heapBytes", "worker.hardCap.ZOMBIE_BYTES",
                "scheduling.class.OUTCOME_AND_CONTROL.minimumRecords", "target.maximumSourceFrameBytes",
                "worker.systemWriterRecords",
                "recovery.maintenanceTurnBytes", "recovery.maintenanceIntervalNanos")) {
            final var map = values();
            map.remove(missing);
            assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(map));
        }
        for (String invalid : java.util.List.of("-1", "unlimited", Long.toString(Long.MAX_VALUE), "1 ")) {
            final var map = values();
            map.put("worker.heapBytes", invalid);
            assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(map));
        }
        final var unknown = values();
        unknown.put("target.futureSwitch", "1");
        assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(unknown));
        final var nativeOverflow = values();
        nativeOverflow.put("worker.rocksDbNativeBytes", "127");
        assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(nativeOverflow));
        final var reserveMismatch = values();
        reserveMismatch.put("worker.hardCap.CONTROL_RESERVE_BYTES", "0");
        assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(reserveMismatch));
        assertEquals(false, Files.exists(root.resolve("db")));
    }

    @Test
    void schedulerCannotAdmitAnEnvelopeItCanNeverServe() {
        for (String key : java.util.List.of("scheduling.class.SOURCE_APPLY.turnBytes",
                "scheduling.class.DUE_SCHEDULER.turnBytes", "scheduling.turnBytes")) {
            final var map = values();
            map.put(key, "1");
            assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(map));
        }
        final var insufficientInventory = values();
        insufficientInventory.put("target.inventoryPagesPerShard", "1");
        assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(insufficientInventory));
        final var noMaintenance = values();
        noMaintenance.put("recovery.maintenanceIntervalNanos", "0");
        assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(noMaintenance));
        for (String key : java.util.List.of("worker.systemWriterBytes",
                "worker.hardCap.SYSTEM_WRITER_RESERVED_RECORDS", "worker.hardCap.PHYSICAL_REQUESTS",
                "worker.hardCap.ZOMBIE_REQUESTS", "worker.hardCap.ADAPTER_PRODUCERS")) {
            final var insufficientWriter = values();
            insufficientWriter.put(key, "1");
            assertThrows(IllegalArgumentException.class,
                    () -> TargetWorkerRuntimeConfigLoader.load(insufficientWriter));
        }
    }

    @Test
    void hostChecksActualDispatcherAndCertifiedResourcesBeforeStartingSchedulers() {
        final var loaded = TargetWorkerRuntimeConfigLoader.load(values());
        final var classes = loaded.newWorkClasses(() -> 0);
        // The injected observation is synthetic; this test makes no current-host probe claim.
        final var observation = new WorkerRuntimeResourceObservation(128, 128, 128, 640, 32, 1, 4096, 4096);
        try (var resources = new SharedRocksDbResources(loaded.store(), loaded.resources(), observation)) {
            loaded.requireRuntime(classes, resources);
            final var differentPolicies = values();
            differentPolicies.put("scheduling.maxClassDelayNanos", "1001");
            final var different = TargetWorkerRuntimeConfigLoader.load(differentPolicies);
            assertEquals("work-class dispatcher configuration differs from the Target Worker",
                    assertThrows(IllegalArgumentException.class, () -> TargetWorkerHostRuntime.start(
                            loaded, different.newWorkClasses(() -> 0), resources, java.util.List.of(), ignored -> {}))
                            .getMessage());
            final var differentRoot = values();
            differentRoot.put("worker.rootPath", root.resolve("other-db").toString());
            assertThrows(IllegalArgumentException.class,
                    () -> TargetWorkerRuntimeConfigLoader.load(differentRoot).requireRuntime(classes, resources));
            final var differentEnvelope = values();
            differentEnvelope.put("worker.filesystemBytes", "8192");
            assertEquals("certified resource envelope differs from the Target Worker",
                    assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(
                            differentEnvelope).requireRuntime(classes, resources)).getMessage());
            resources.recordRuntimeProbeFailure(new IllegalStateException("fixture probe failed"));
            assertThrows(IllegalStateException.class, () -> loaded.requireRuntime(classes, resources));
        }
        try (var unprobed = new SharedRocksDbResources(loaded.store(), loaded.resources())) {
            assertEquals("Worker activation requires a runtime-probed resource envelope",
                    assertThrows(IllegalStateException.class, () -> TargetWorkerHostRuntime.start(
                            loaded, classes, unprobed, java.util.List.of(), ignored -> {})).getMessage());
        }
        assertEquals(false, Files.exists(root.resolve("other-db")));
    }

    /** Synthetic numeric fixture, not a release capacity declaration or runtime observation. */
    private Map<String, String> values() {
        final var v = new HashMap<String, String>();
        v.put("worker.rootPath", root.resolve("db").toString());
        v.put("worker.maxOwnedShards", "2");
        v.put("worker.maxOpenShardDbs", "2");
        v.put("worker.maxOpenFilesPerDb", "8");
        v.put("worker.maxTotalOpenFiles", "16");
        v.put("worker.maxBackgroundJobs", "4");
        v.put("worker.sharedBlockCacheBytes", "64");
        v.put("worker.sharedWriteBufferBudgetBytes", "64");
        v.put("worker.maxWriteBufferBytesPerDb", "16");
        v.put("worker.reservedFlushJobs", "1");
        v.put("worker.maxCompactionJobs", "1");
        v.put("worker.maxBackgroundJobsPerDb", "2");
        v.put("worker.maxConcurrentAcquires", "1");
        v.put("worker.heapBytes", "128");
        v.put("worker.directBytes", "128");
        v.put("worker.rocksDbNativeBytes", "128");
        v.put("worker.otherNativeBytes", "128");
        v.put("worker.controlMemoryHeadroomBytes", "64");
        v.put("worker.processRssBytes", "576");
        v.put("worker.containerHeadroomBytes", "64");
        v.put("worker.cgroupBytes", "640");
        v.put("worker.processOpenFiles", "32");
        v.put("worker.fdHeadroom", "8");
        v.put("worker.filesystemBytes", "4096");
        v.put("worker.diskSafetyWatermarkBytes", "512");
        v.put("worker.controlReserveBytes", "64");
        v.put("worker.controlReserveRecords", "8");
        v.put("worker.systemWriterRecords", "2");
        v.put("worker.systemWriterBytes", "1024");
        v.put("worker.systemWriterGenerations", "2");
        for (final var dimension : CapacityDimension.values()) {
            long value = 1024;
            if (dimension == CapacityDimension.CONTROL_RESERVE_BYTES) { value = 64; }
            if (dimension == CapacityDimension.CONTROL_RESERVE_RECORDS) { value = 8; }
            if (java.util.List.of(CapacityDimension.ROCKSDB_NATIVE_BYTES, CapacityDimension.DIRECT_BUFFER_BYTES,
                    CapacityDimension.OTHER_NATIVE_BYTES).contains(dimension)) { value = 128; }
            v.put("worker.hardCap." + dimension.name(), Long.toString(value));
        }
        for (String key : java.util.List.of("checkpointCreates", "checkpointUploads", "checkpointDownloads",
                "drains", "checkpointIoBytesPerSecond")) { v.put("recovery." + key, "1"); }
        v.put("recovery.checkpointRestoreTempBytes", "64");
        v.put("recovery.compactionTempBytes", "64");
        v.put("recovery.maintenanceTurnMessages", "1");
        v.put("recovery.maintenanceTurnBytes", "1024");
        v.put("recovery.maintenanceTurnNanos", "1000");
        v.put("recovery.maintenanceIntervalNanos", "1000");
        for (final var workClass : WorkClass.values()) {
            final String p = "scheduling.class." + workClass.name() + '.';
            v.put(p + "weight", "1");
            v.put(p + "queueRecords", "8");
            v.put(p + "queueBytes", "2048");
            v.put(p + "turnRecords", "1");
            v.put(p + "turnBytes", "1024");
            v.put(p + "turnNanos", "1000");
            v.put(p + "minimumRecords", "1");
            v.put(p + "minimumBytes", "128");
        }
        v.put("scheduling.maxClassDelayNanos", "1000");
        v.put("scheduling.maxResourceHoldNanos", "1000");
        v.put("scheduling.poolRecords", "8");
        v.put("scheduling.poolBytes", "1024");
        v.put("target.sourceReadRecords", "64");
        v.put("target.sourceReadBytes", "4096");
        v.put("target.sourceReadNanos", "1000");
        v.put("target.maximumAffectedCounters", "4");
        v.put("target.maximumDomains", "2");
        v.put("target.maximumWorkerTargets", "4");
        v.put("target.inventoryPageTargets", "2");
        v.put("target.inventoryPagesPerShard", "2");
        v.put("target.inventoryReadRecords", "16");
        v.put("target.inventoryReadBytes", "4096");
        v.put("target.inventoryReadNanos", "1000");
        v.put("target.maximumSourceFrameBytes", "1024");
        v.put("target.backlogReadRecords", "8");
        v.put("target.backlogReadBytes", "8192");
        v.put("target.backlogReadNanos", "1000");
        v.put("target.maximumUnappliedRecords", "4");
        v.put("target.maximumUnappliedBytes", "4096");
        v.put("target.maximumUnappliedAgeMillis", "1000");
        v.put("target.maximumBacklogObservationAgeNanos", "1000");
        v.put("scheduling.quantumBytes", "256");
        v.put("scheduling.maximumCostBytes", "1024");
        v.put("scheduling.sendEnvelopeBytes", "1024");
        v.put("scheduling.maximumVisits", "4");
        v.put("scheduling.readRecords", "16");
        v.put("scheduling.readBytes", "4096");
        v.put("scheduling.readNanos", "1000");
        v.put("scheduling.turnMessages", "1");
        v.put("scheduling.turnBytes", "1024");
        v.put("scheduling.turnNanos", "1000");
        v.put("scheduling.recheckNanos", "1000");
        return v;
    }
}
