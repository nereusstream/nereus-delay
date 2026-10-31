package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.WorkerResourceEnvelope;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Closed explicit properties schema. Missing/unknown/unbounded values fail before resource allocation. */
public final class TargetWorkerRuntimeConfigLoader {
    private static final int MAX_CONFIGURATION_BYTES = 256 << 10;
    private static final int MAX_CONFIGURATION_FIELDS = 512;

    private TargetWorkerRuntimeConfigLoader() {}

    /** Reads the deployment's explicit UTF-8 properties before any Worker/native resource allocation. */
    public static TargetWorkerRuntimeConfig load(Path path) throws IOException {
        return load(readValues(path));
    }

    /** The service schema shares the same bounded, duplicate-rejecting file read. */
    public static TargetWorkerProcessRuntime.Configuration loadService(Path path) throws IOException {
        return loadService(readValues(path));
    }

    public static TargetWorkerPublicationDispatch.Configuration loadPublicationDispatch(Path path) throws IOException {
        return TargetWorkerPublicationDispatch.Configuration.load(readValues(path));
    }

    public static TargetWorkerChannelInventory.Limits loadChannelInventory(Path path) throws IOException {
        return TargetWorkerChannelInventory.Limits.load(readValues(path));
    }

    /** Same closed file parser for the actual SDK modules' explicit deployment schemas. */
    public static Fields loadFields(Path path) throws IOException {
        return new Fields(readValues(path));
    }

    private static Map<String, String> readValues(Path path) throws IOException {
        final byte[] image = loadBoundedImage(path, MAX_CONFIGURATION_BYTES);
        final var decoded = StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(image));
        final var properties = new ClosedProperties();
        properties.load(new StringReader(decoded.toString()));
        final var values = new HashMap<String, String>();
        properties.forEach((key, value) -> values.put((String) key, (String) value));
        return values;
    }

    /** Reads a bounded deployment image; authentication remains with the original Route/material verifier. */
    public static byte[] loadBoundedImage(Path path, int maximumBytes) throws IOException {
        if (maximumBytes < 1 || maximumBytes > MAX_CONFIGURATION_BYTES) {
            throw new IllegalArgumentException("deployment image must use a finite supported file bound");
        }
        final var file = Objects.requireNonNull(path, "configuration path")
                .toAbsolutePath()
                .normalize();
        final var before = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!before.isRegularFile() || before.size() > maximumBytes) {
            throw new IOException("Target Worker configuration requires a bounded ordinary file");
        }
        final byte[] image;
        try (var input = Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            image = input.readNBytes(maximumBytes + 1);
        }
        final var after = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (image.length > maximumBytes
                || !after.isRegularFile()
                || image.length != before.size()
                || before.size() != after.size()
                || !Objects.equals(before.fileKey(), after.fileKey())
                || !before.lastModifiedTime().equals(after.lastModifiedTime())) {
            throw new IOException("Target Worker configuration changed during bounded loading");
        }
        return image;
    }

    public static TargetWorkerProcessRuntime.Configuration loadService(Map<String, String> values) {
        final var f = new Fields(values);
        final var process = new TargetWorkerProcessRuntime.Limits(
                f.integer("process.ioThreads"),
                f.integer("process.ioPendingTasks"),
                f.number("process.ioNativeBytes"),
                f.number("process.metricNativeBytes"),
                new com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter.Limits(
                        f.integer("process.metricPendingRecords"), f.number("process.metricPendingBytes")));
        final String observationPrefix = "process.observation.";
        final var observation = new com.nereusstream.delay.scheduler.OrdinaryObservationConfiguration(
                com.nereusstream.delay.scheduler.OrdinaryObservationConfiguration.Mode.valueOf(
                        f.text(observationPrefix + "mode")),
                f.number(observationPrefix + "generation"),
                f.integer(observationPrefix + "maximumSources"),
                f.number(observationPrefix + "maximumContextBytes"),
                f.integer(observationPrefix + "maximumStatusAttempts"),
                f.number(observationPrefix + "statusRetryNanos"));
        final var oxia = new TargetWorkerOxiaRuntime.Connection(
                f.text("oxia.serviceAddress"),
                f.text("oxia.namespace"),
                f.text("oxia.clientIdentifier"),
                f.text("oxia.ownerPrefix"),
                f.text("oxia.routeSessionPrefix"),
                Duration.ofMillis(f.number("oxia.sessionTimeoutMs")),
                Duration.ofMillis(f.number("oxia.requestTimeoutMs")),
                Duration.ofMillis(f.number("oxia.startupTimeoutMs")),
                Duration.ofMillis(f.number("oxia.cleanupBudgetMs")),
                f.number("oxia.nativeBytesPerClient"));
        final String prefix = "process.monitor.";
        final var monitoring = new TargetWorkerProcessRuntime.ResourceMonitoring(
                Duration.ofNanos(f.number(prefix + "runtimeIntervalNanos")),
                Duration.ofNanos(f.number(prefix + "rocksDbIntervalNanos")),
                new com.nereusstream.delay.store.RocksDbUsageLimits(
                        f.number(prefix + "maxWalBytesPerDb"),
                        f.number(prefix + "maxTotalWalBytes"),
                        f.integer(prefix + "maxWalFilesPerDb"),
                        f.integer(prefix + "maxTotalWalFiles"),
                        f.number(prefix + "maxManifestBytesPerDb"),
                        f.number(prefix + "maxTotalManifestBytes"),
                        f.integer(prefix + "maxManifestFilesPerDb"),
                        f.integer(prefix + "maxTotalManifestFiles"),
                        f.number(prefix + "maxLiveSstBytesPerDb"),
                        f.number(prefix + "maxTotalLiveSstBytes"),
                        f.integer(prefix + "maxSstFilesPerDb"),
                        f.integer(prefix + "maxTotalSstFiles"),
                        f.number(prefix + "maxLocalLiveDataBytes"),
                        f.number(prefix + "minimumFilesystemFreeBytes"),
                        f.number(prefix + "maxCompactionPendingBytesPerDb"),
                        f.number(prefix + "maxTotalCompactionPendingBytes"),
                        f.integer(prefix + "maxL0FilesPerDb")),
                f.number(prefix + "nativeBytes"));
        return new TargetWorkerProcessRuntime.Configuration(load(f.remaining), process, oxia, monitoring, observation);
    }

    /** Properties normally discards duplicate keys; deployment settings must have one unambiguous value. */
    private static final class ClosedProperties extends Properties {
        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Object put(Object key, Object value) {
            if (!(key instanceof String name)
                    || !(value instanceof String text)
                    || name.isBlank()
                    || name.length() > 256
                    || text.length() > 4096
                    || size() >= MAX_CONFIGURATION_FIELDS
                    || containsKey(name)) {
                throw new IllegalArgumentException(
                        "Target Worker configuration field is duplicated or exceeds its bound");
            }
            return super.put(name, text);
        }
    }

    public static TargetWorkerRuntimeConfig load(Map<String, String> values) {
        final var f = new Fields(values);
        final var store = new ShardStoreConfig(
                Path.of(f.text("worker.rootPath")),
                f.integer("worker.maxOwnedShards"),
                f.integer("worker.maxOpenShardDbs"),
                f.integer("worker.maxOpenFilesPerDb"),
                f.integer("worker.maxTotalOpenFiles"),
                f.integer("worker.maxBackgroundJobs"),
                f.number("worker.sharedBlockCacheBytes"),
                f.number("worker.sharedWriteBufferBudgetBytes"),
                f.integer("recovery.checkpointCreates"),
                f.integer("recovery.checkpointUploads"),
                f.integer("recovery.checkpointDownloads"),
                f.number("recovery.checkpointIoBytesPerSecond"),
                f.integer("recovery.drains"),
                f.number("worker.maxWriteBufferBytesPerDb"),
                f.integer("worker.reservedFlushJobs"),
                f.integer("worker.maxCompactionJobs"),
                f.integer("worker.maxBackgroundJobsPerDb"),
                f.integer("worker.maxConcurrentAcquires"));
        final var resources = new WorkerResourceEnvelope(
                f.number("worker.heapBytes"),
                f.number("worker.directBytes"),
                f.number("worker.rocksDbNativeBytes"),
                f.number("worker.otherNativeBytes"),
                f.number("worker.controlMemoryHeadroomBytes"),
                f.number("worker.processRssBytes"),
                f.number("worker.containerHeadroomBytes"),
                f.number("worker.cgroupBytes"),
                f.number("worker.processOpenFiles"),
                f.number("worker.fdHeadroom"),
                f.number("worker.filesystemBytes"),
                f.number("worker.diskSafetyWatermarkBytes"),
                f.number("recovery.checkpointRestoreTempBytes"),
                f.number("recovery.compactionTempBytes"),
                f.number("worker.controlReserveBytes"),
                f.number("worker.controlReserveRecords"));
        final long[] caps = new long[CapacityDimension.COUNT];
        for (final var dimension : CapacityDimension.values()) {
            caps[dimension.wireValue() - 1] = f.number("worker.hardCap." + dimension.name());
        }
        final var policies = new EnumMap<WorkClass, WorkClassPolicy>(WorkClass.class);
        for (final var workClass : WorkClass.values()) {
            final String key = "scheduling.class." + workClass.name() + '.';
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            f.integer(key + "weight"),
                            f.integer(key + "queueRecords"),
                            f.number(key + "queueBytes"),
                            f.integer(key + "turnRecords"),
                            f.number(key + "turnBytes"),
                            f.number(key + "turnNanos"),
                            f.number(key + "minimumRecords"),
                            f.number(key + "minimumBytes"),
                            workClass == WorkClass.LEASE_FENCE));
        }
        final var workClasses = new WorkClassRuntimeConfig(
                policies,
                f.number("scheduling.maxClassDelayNanos"),
                f.number("scheduling.maxResourceHoldNanos"),
                f.number("scheduling.poolRecords"),
                f.number("scheduling.poolBytes"));
        final var source = new TargetSourceApplyRuntime.Limits(
                f.integer("target.sourceReadRecords"),
                f.number("target.sourceReadBytes"),
                f.number("target.sourceReadNanos"),
                f.integer("target.maximumAffectedCounters"),
                f.integer("target.maximumDomains"));
        final var inventory = new TargetWorkerTargetInventory.Limits(
                store.maxOwnedShards(),
                f.integer("target.maximumWorkerTargets"),
                f.integer("target.inventoryPageTargets"),
                f.integer("target.inventoryPagesPerShard"),
                f.integer("target.inventoryReadRecords"),
                f.number("target.inventoryReadBytes"),
                f.number("target.inventoryReadNanos"));
        final var ordinary = new TargetWorkerOrdinaryDrr.Limits(
                f.number("scheduling.quantumBytes"),
                f.number("scheduling.maximumCostBytes"),
                f.number("scheduling.sendEnvelopeBytes"),
                f.integer("scheduling.maximumVisits"),
                f.integer("scheduling.readRecords"),
                f.number("scheduling.readBytes"),
                f.number("scheduling.readNanos"));
        final var turn = new SchedulerBudget(
                f.integer("scheduling.turnMessages"),
                f.number("scheduling.turnBytes"),
                f.number("scheduling.turnNanos"));
        final long maximumFrame = f.number("target.maximumSourceFrameBytes");
        final var writer = new SystemWriterAppendBudget.Limits(
                f.integer("worker.systemWriterRecords"),
                f.number("worker.systemWriterBytes"),
                maximumFrame,
                f.integer("worker.systemWriterGenerations"));
        if (maximumFrame >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("target.maximumSourceFrameBytes must fit a finite native frame int");
        }
        final var backlog = new TargetSourceBacklog.Limits(
                f.integer("target.backlogReadRecords"),
                f.number("target.backlogReadBytes"),
                (int) maximumFrame,
                f.number("target.backlogReadNanos"));
        final var growth = new TargetSourceBacklogAdmission.Limits(
                f.number("target.maximumUnappliedRecords"),
                f.number("target.maximumUnappliedBytes"),
                f.number("target.maximumUnappliedAgeMillis"),
                f.number("target.maximumBacklogObservationAgeNanos"));
        final var checkpoint = new TargetCheckpointRuntimeConfig(
                f.number("checkpoint.intervalMillis"),
                f.integer("checkpoint.jitterPercent"),
                new SchedulerBudget(
                        f.integer("checkpoint.turnMessages"),
                        f.number("checkpoint.turnBytes"),
                        f.number("checkpoint.turnNanos")),
                f.integer("checkpoint.readRecords"),
                f.number("checkpoint.readBytes"),
                f.number("checkpoint.readNanos"),
                Duration.ofNanos(f.number("checkpoint.registryLookupTimeoutNanos")),
                new TargetCheckpointControlInputs.Limits(
                        f.integer("checkpoint.controlInputs"), f.number("checkpoint.controlFrameBytes")),
                new TargetCheckpointOutcomeInputs.Limits(
                        f.integer("checkpoint.outcomeInputs"), f.number("checkpoint.outcomeFrameBytes")),
                new TargetCheckpointAdmissionInputs.Limits(
                        f.integer("checkpoint.admissionInputs"), f.number("checkpoint.admissionFrameBytes")),
                new TargetCheckpointRegistryInputs.Limits(
                        f.integer("checkpoint.registryUses"), f.number("checkpoint.registryResponseBytes")),
                new com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits(
                        f.integer("checkpoint.semanticRecords"),
                        f.integer("checkpoint.semanticBytes"),
                        f.number("checkpoint.semanticAdmissionFrameBytes")),
                new com.nereusstream.delay.store.CheckpointManifestLimits(
                        f.integer("checkpoint.files"),
                        f.number("checkpoint.totalFileBytes"),
                        f.number("checkpoint.individualFileBytes"),
                        f.integer("checkpoint.pathBytes"),
                        f.integer("checkpoint.manifestBytes"),
                        f.integer("checkpoint.evidenceCursors"),
                        f.integer("checkpoint.objectIdentityBytes")),
                new com.nereusstream.delay.store.TargetCheckpointRootVerifier.QuotaAuditLimits(
                        f.integer("checkpoint.quotaRecords"), f.number("checkpoint.quotaBytes")),
                new com.nereusstream.delay.store.TargetCheckpointRootVerifier.LedgerAuditLimits(
                        f.integer("checkpoint.ledgerRecords"), f.number("checkpoint.ledgerBytes"),
                        f.integer("checkpoint.ledgerPointReads"), f.number("checkpoint.ledgerPointBytes")));
        final var config = new TargetWorkerRuntimeConfig(
                store,
                resources,
                new CapacityVector(caps),
                workClasses,
                source,
                maximumFrame,
                writer,
                backlog,
                growth,
                inventory,
                ordinary,
                turn,
                Duration.ofNanos(f.number("scheduling.recheckNanos")),
                new SchedulerBudget(
                        f.integer("recovery.maintenanceTurnMessages"),
                        f.number("recovery.maintenanceTurnBytes"),
                        f.number("recovery.maintenanceTurnNanos")),
                Duration.ofNanos(f.number("recovery.maintenanceIntervalNanos")),
                checkpoint);
        f.requireComplete();
        return config;
    }

    public static final class Fields {
        private final Map<String, String> remaining;

        private Fields(Map<String, String> values) {
            remaining = new HashMap<>(Objects.requireNonNull(values, "values"));
        }

        public String text(String key) {
            final String value = remaining.remove(key);
            if (value == null || value.isBlank() || !value.equals(value.trim())) {
                throw new IllegalArgumentException("missing or malformed configuration: " + key);
            }
            return value;
        }

        public long number(String key) {
            final String value = text(key);
            if (!value.chars().allMatch(Character::isDigit)) {
                throw new IllegalArgumentException("configuration requires an explicit decimal bound: " + key);
            }
            try {
                final long parsed = Long.parseLong(value);
                if (parsed == Long.MAX_VALUE) {
                    throw new NumberFormatException("unbounded sentinel");
                }
                return parsed;
            } catch (NumberFormatException invalid) {
                throw new IllegalArgumentException("configuration bound overflows: " + key, invalid);
            }
        }

        public int integer(String key) {
            final long value = number(key);
            if (value >= Integer.MAX_VALUE) {
                throw new IllegalArgumentException("configuration count is unbounded: " + key);
            }
            return Math.toIntExact(value);
        }

        public byte[] hex(String key, int minimumBytes, int maximumBytes) {
            if (minimumBytes < 1 || maximumBytes < minimumBytes || maximumBytes > 2048) {
                throw new IllegalArgumentException("invalid deployment byte field bound");
            }
            final String value = text(key);
            if ((value.length() & 1) != 0 || value.length() / 2 < minimumBytes || value.length() / 2 > maximumBytes) {
                throw new IllegalArgumentException("deployment hex field exceeds its byte bound: " + key);
            }
            try {
                return java.util.HexFormat.of().parseHex(value);
            } catch (IllegalArgumentException invalid) {
                throw new IllegalArgumentException("invalid deployment hex field: " + key);
            }
        }

        /** Protocol identities may use the entire uint64 range; resource bounds still use number(). */
        public long unsigned64(String key) {
            final String value = text(key);
            if (!value.matches("[0-9]{1,20}")) {
                throw new IllegalArgumentException("deployment identity requires uint64: " + key);
            }
            try {
                return Long.parseUnsignedLong(value);
            } catch (NumberFormatException invalid) {
                throw new IllegalArgumentException("deployment uint64 overflows: " + key);
            }
        }

        public Path absolutePath(String key) {
            final Path value = Path.of(text(key));
            if (!value.isAbsolute()) {
                throw new IllegalArgumentException("deployment file requires an absolute path: " + key);
            }
            return value.normalize();
        }

        public byte[] image(String key, int maximumBytes) throws IOException {
            return loadBoundedImage(absolutePath(key), maximumBytes);
        }

        public CapacityVector capacity(String prefix) {
            final long[] amounts = new long[CapacityDimension.COUNT];
            for (var dimension : CapacityDimension.values()) {
                amounts[dimension.wireValue() - 1] = number(prefix + dimension.name());
            }
            return new CapacityVector(amounts);
        }

        public Map<String, String> prefixed(String prefix, int maximumFields) {
            if (prefix.isEmpty() || maximumFields < 1 || maximumFields > MAX_CONFIGURATION_FIELDS) {
                throw new IllegalArgumentException("invalid finite deployment field prefix");
            }
            final var result = new HashMap<String, String>();
            for (String key : java.util.List.copyOf(remaining.keySet())) {
                if (!key.startsWith(prefix)) {
                    continue;
                }
                final String name = key.substring(prefix.length());
                if (name.isBlank() || result.size() >= maximumFields) {
                    throw new IllegalArgumentException("deployment field prefix is empty or full");
                }
                result.put(name, text(key));
            }
            return Map.copyOf(result);
        }

        public void requireComplete() {
            if (!remaining.isEmpty()) {
                throw new IllegalArgumentException("unknown Target Worker configuration keys: "
                        + remaining.keySet().stream().sorted().toList());
            }
        }
    }
}
