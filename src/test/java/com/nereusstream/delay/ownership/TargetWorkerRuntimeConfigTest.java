package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.TargetSourceOperationEnvelope;
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
    private static final TargetSourceOperationEnvelope.Bounds FIXED_SOURCE = TargetSourceOperationEnvelope.basic();

    @TempDir
    Path root;

    @Test
    void explicitSchemaDerivesOneOwnershipBoundaryAndSeparatesReadAndActionCosts() {
        final var settings = values();
        final var loaded = TargetWorkerRuntimeConfigLoader.load(settings);
        assertEquals(loaded.store().maxOwnedShards(), loaded.inventory().maximumShards());
        assertEquals(Integer.parseInt(settings.get("target.sourceReadRecords")), loaded.source().records());
        assertEquals(Long.parseLong(settings.get("target.sourceReadBytes")), loaded.source().bytes());
        assertEquals(1, loaded.workClasses().policies().get(WorkClass.SOURCE_APPLY).maxRecordsPerTurn());
        assertEquals(1, loaded.sourceTurn().maxMessages());
        assertEquals(loaded.maximumSourceFrameBytes() + TargetSourcePosition.MAX_CANONICAL_BYTES,
                loaded.sourceTurn().maxBytes());
        assertTrue(loaded.source().records() > loaded.sourceTurn().maxMessages());
        assertTrue(loaded.source().bytes() > loaded.sourceTurn().maxBytes());
        assertEquals(8, loaded.maximumTargetSummaries());
        // Two Shards, four Targets each, and two physical heads for every one of the 64 domain slots.
        assertEquals(1024, loaded.maximumDomainHeads());
        loaded.newWorkClasses(() -> 0);
        loaded.requireCapacity(java.util.List.of(), CapacityVector.empty(), CapacityVector.empty());
        assertThrows(TargetSourceAuthorityPreparation.InputUnavailable.class,
                () -> loaded.requireSourceOperationEnvelope(FIXED_SOURCE));
        assertEquals(false, Files.exists(root.resolve("db")));
    }

    @Test
    void missingUnknownUnlimitedAndOvercommittedConfigurationRejectsBeforeIo() {
        for (String missing : java.util.List.of("worker.heapBytes", "worker.hardCap.ZOMBIE_BYTES",
                "scheduling.class.OUTCOME_AND_CONTROL.minimumRecords", "target.maximumSourceFrameBytes",
                "worker.systemWriterRecords",
                "recovery.maintenanceTurnBytes", "recovery.maintenanceIntervalNanos", "checkpoint.intervalMillis",
                "checkpoint.registryLookupTimeoutNanos", "checkpoint.ledgerPointBytes")) {
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
        for (String key : java.util.List.of("target.sourceReadRecords", "target.sourceReadBytes",
                "target.maximumAffectedCounters", "target.maximumDomains", "target.maximumSourceFrameBytes")) {
            final var map = values();
            map.put(key, key.equals("target.maximumAffectedCounters") ? "2" : "1");
            assertEquals("Source limits cannot serve the complete legal Native operation family",
                    assertThrows(IllegalArgumentException.class,
                            () -> TargetWorkerRuntimeConfigLoader.load(map)).getMessage());
        }
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
        final var observation = new WorkerRuntimeResourceObservation(
                loaded.resources().certifiedJvmHeapBytes(), 128, 128,
                loaded.resources().effectiveCgroupMemoryLimitBytes(), 32, 1, 4096, 4096);
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

    @Test
    void checkpointEnvelopeAndScheduleRejectMixedSettingsBeforeAdmission() {
        for (String key : java.util.List.of("checkpoint.turnBytes", "checkpoint.semanticRecords",
                "checkpoint.semanticAdmissionFrameBytes", "scheduling.class.CHECKPOINT.turnBytes",
                "scheduling.poolBytes")) {
            final var map = values();
            map.put(key, "1");
            assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(map));
        }
        final var invalidJitter = values();
        invalidJitter.put("checkpoint.jitterPercent", "100");
        assertThrows(IllegalArgumentException.class, () -> TargetWorkerRuntimeConfigLoader.load(invalidJitter));
        final var loaded = TargetWorkerRuntimeConfigLoader.load(values());
        final var checkpoint = loaded.checkpoint();
        assertEquals(8192, checkpoint.settlementTurn().maxBytes());
        assertEquals(100, checkpoint.readRecords());
        assertThrows(IllegalArgumentException.class, () -> checkpoint.requireSchedule(
                checkpoint.newScheduler(1), loaded.store().maxOwnedShards(), checkpoint.settlementTurn()));
        assertThrows(IllegalArgumentException.class, () -> checkpoint.requireSchedule(
                new com.nereusstream.delay.store.CheckpointScheduler(1001, 10, 2), 2,
                checkpoint.settlementTurn()));
        assertThrows(IllegalArgumentException.class, () -> checkpoint.requireSchedule(
                checkpoint.newScheduler(2), 2, new com.nereusstream.delay.scheduler.SchedulerBudget(1, 8193, 1000)));
        checkpoint.requireSchedule(checkpoint.newScheduler(2), 2, checkpoint.settlementTurn());
        assertEquals(false, Files.exists(root.resolve("db")));
    }

    /** Synthetic numeric fixture, not a release capacity declaration or runtime observation. */
    private Map<String, String> values() {
        // Satisfy the current fixed Native calculation only; the original Source frame hold is still absent.
        final long frameBytes = FIXED_SOURCE.fixedMaximumFrameBytes();
        final long sourceTaskBytes = Math.addExact(frameBytes, TargetSourcePosition.MAX_CANONICAL_BYTES);
        final long sourceReadRecords = Math.addExact(64, Math.max(
                FIXED_SOURCE.readRecords(), FIXED_SOURCE.writeRecords()));
        final long sourceReadBytes = Math.max(Math.multiplyExact(2, sourceTaskBytes),
                Math.max(FIXED_SOURCE.fixedReadBytes(), FIXED_SOURCE.fixedWriteBytes()));
        // One Source action remains serviceable while the other seven classes retain their 128-byte minima.
        final long poolBytes = Math.addExact(sourceTaskBytes, 7 * 128);
        final long writerBytes = Math.multiplyExact(2, frameBytes);
        final long perShardInputBytes = Math.addExact(FIXED_SOURCE.fixedRetainedBytes(), sourceReadBytes);
        final long heapBytes = Math.addExact(65536, Math.addExact(poolBytes, Math.multiplyExact(2, perShardInputBytes)));
        final long rssBytes = Math.addExact(heapBytes, 3 * 128 + 64);
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
        v.put("worker.heapBytes", Long.toString(heapBytes));
        v.put("worker.directBytes", "128");
        v.put("worker.rocksDbNativeBytes", "128");
        v.put("worker.otherNativeBytes", "128");
        v.put("worker.controlMemoryHeadroomBytes", "64");
        v.put("worker.processRssBytes", Long.toString(rssBytes));
        v.put("worker.containerHeadroomBytes", "64");
        v.put("worker.cgroupBytes", Long.toString(Math.addExact(rssBytes, 64)));
        v.put("worker.processOpenFiles", "32");
        v.put("worker.fdHeadroom", "8");
        v.put("worker.filesystemBytes", "4096");
        v.put("worker.diskSafetyWatermarkBytes", "512");
        v.put("worker.controlReserveBytes", "64");
        v.put("worker.controlReserveRecords", "8");
        v.put("worker.systemWriterRecords", "2");
        v.put("worker.systemWriterBytes", Long.toString(writerBytes));
        v.put("worker.systemWriterGenerations", "2");
        for (final var dimension : CapacityDimension.values()) {
            long value = 1024;
            if (dimension == CapacityDimension.CONTROL_RESERVE_BYTES) { value = 64; }
            if (dimension == CapacityDimension.CONTROL_RESERVE_RECORDS) { value = 8; }
            if (java.util.List.of(CapacityDimension.ROCKSDB_NATIVE_BYTES, CapacityDimension.DIRECT_BUFFER_BYTES,
                    CapacityDimension.OTHER_NATIVE_BYTES).contains(dimension)) { value = 128; }
            if (java.util.List.of(CapacityDimension.SYSTEM_WRITER_RESERVED_BYTES, CapacityDimension.PHYSICAL_BYTES,
                    CapacityDimension.ZOMBIE_BYTES).contains(dimension)) { value = writerBytes; }
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
        v.put("checkpoint.intervalMillis", "1000");
        v.put("checkpoint.jitterPercent", "10");
        v.put("checkpoint.turnMessages", "1");
        v.put("checkpoint.turnBytes", "8192");
        v.put("checkpoint.turnNanos", "1000");
        v.put("checkpoint.readRecords", "100");
        v.put("checkpoint.readBytes", "65536");
        v.put("checkpoint.readNanos", "1000");
        v.put("checkpoint.registryLookupTimeoutNanos", "1000");
        v.put("checkpoint.controlInputs", "4");
        v.put("checkpoint.controlFrameBytes", "4096");
        v.put("checkpoint.outcomeInputs", "4");
        v.put("checkpoint.outcomeFrameBytes", "4096");
        v.put("checkpoint.admissionInputs", "4");
        v.put("checkpoint.admissionFrameBytes", "4096");
        v.put("checkpoint.registryUses", "8");
        v.put("checkpoint.registryResponseBytes", "4096");
        v.put("checkpoint.semanticRecords", "32");
        v.put("checkpoint.semanticBytes", "4096");
        v.put("checkpoint.semanticAdmissionFrameBytes", "4096");
        v.put("checkpoint.files", "16");
        v.put("checkpoint.totalFileBytes", "4096");
        v.put("checkpoint.individualFileBytes", "4096");
        v.put("checkpoint.pathBytes", "256");
        v.put("checkpoint.manifestBytes", "8192");
        v.put("checkpoint.evidenceCursors", "16");
        v.put("checkpoint.objectIdentityBytes", "128");
        v.put("checkpoint.quotaRecords", "100");
        v.put("checkpoint.quotaBytes", "65536");
        v.put("checkpoint.ledgerRecords", "100");
        v.put("checkpoint.ledgerBytes", "65536");
        v.put("checkpoint.ledgerPointReads", "100");
        v.put("checkpoint.ledgerPointBytes", "65536");
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
        v.put("scheduling.class.CHECKPOINT.queueBytes", "8192");
        v.put("scheduling.class.CHECKPOINT.turnBytes", "8192");
        v.put("scheduling.class.SOURCE_APPLY.queueBytes", Long.toString(Math.multiplyExact(2, sourceTaskBytes)));
        v.put("scheduling.class.SOURCE_APPLY.turnBytes", Long.toString(sourceTaskBytes));
        v.put("scheduling.class.OUTCOME_AND_CONTROL.queueBytes", Long.toString(writerBytes));
        v.put("scheduling.class.OUTCOME_AND_CONTROL.turnBytes", Long.toString(frameBytes));
        v.put("scheduling.maxClassDelayNanos", "1000");
        v.put("scheduling.maxResourceHoldNanos", "1000");
        v.put("scheduling.poolRecords", "8");
        v.put("scheduling.poolBytes", Long.toString(poolBytes));
        v.put("target.sourceReadRecords", Long.toString(sourceReadRecords));
        v.put("target.sourceReadBytes", Long.toString(sourceReadBytes));
        v.put("target.sourceReadNanos", "1000");
        v.put("target.maximumAffectedCounters", Integer.toString(FIXED_SOURCE.maximumCounters()));
        v.put("target.maximumDomains", Integer.toString(TargetQueueState.MAX_DOMAIN_SLOTS));
        v.put("target.maximumWorkerTargets", "4");
        v.put("target.inventoryPageTargets", "2");
        v.put("target.inventoryPagesPerShard", "2");
        v.put("target.inventoryReadRecords", "16");
        v.put("target.inventoryReadBytes", "4096");
        v.put("target.inventoryReadNanos", "1000");
        v.put("target.maximumSourceFrameBytes", Long.toString(frameBytes));
        v.put("target.backlogReadRecords", "8");
        v.put("target.backlogReadBytes", Long.toString(writerBytes));
        v.put("target.backlogReadNanos", "1000");
        v.put("target.maximumUnappliedRecords", "4");
        v.put("target.maximumUnappliedBytes", Long.toString(writerBytes));
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
