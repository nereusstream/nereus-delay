package io.nereusstream.delay.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerRuntimeResourceProbeTest {
    @TempDir
    Path tempDir;

    @Test
    void parsesKernelMeasurementsAndByteSuffixes() {
        assertEquals(128L * 1024, WorkerRuntimeResourceProbe.parseKernelMeasurement("128 kB", "VmRSS"));
        assertEquals(2L * 1024 * 1024, WorkerRuntimeResourceProbe.parseByteSize("2MiB", "bytes"));
        assertEquals(4L * 1024 * 1024 * 1024, WorkerRuntimeResourceProbe.parseByteSize("4g", "bytes"));
    }

    @Test
    void rejectsUnboundedCgroupAndOverflowValues() {
        assertThrows(IllegalStateException.class,
                () -> WorkerRuntimeResourceProbe.parseCgroupMemoryLimit("max"));
        assertThrows(IllegalStateException.class,
                () -> WorkerRuntimeResourceProbe.parseCgroupMemoryLimit(Long.toString(1L << 60)));
        assertThrows(IllegalArgumentException.class,
                () -> WorkerRuntimeResourceProbe.parseByteSize("9223372036854775807g", "bytes"));
    }

    @Test
    void readsProcAndCgroupFilesThroughFailClosedParsers() throws Exception {
        final Path status = tempDir.resolve("status");
        final Path limits = tempDir.resolve("limits");
        final Path cgroup = tempDir.resolve("memory.max");
        Files.writeString(status, "Name:\ttest\nVmRSS:\t256 kB\n");
        Files.writeString(limits, "Limit\tSoft Limit\tHard Limit\nMax open files\t4096\t8192\n");
        Files.writeString(cgroup, Long.toString(512L * 1024 * 1024) + "\n");

        assertEquals(256L * 1024, WorkerRuntimeResourceProbe.readProcessRss(status));
        assertEquals(4096, WorkerRuntimeResourceProbe.readMaxProcessOpenFiles(limits));
        assertEquals(512L * 1024 * 1024,
                WorkerRuntimeResourceProbe.readCgroupMemoryLimit(List.of(cgroup)));
    }

    @Test
    void countsLiveProcessDescriptorsOnlyFromARealProcDirectory() throws Exception {
        final Path descriptors = tempDir.resolve("fd");
        Files.createDirectory(descriptors);
        Files.createFile(descriptors.resolve("0"));
        Files.createFile(descriptors.resolve("1"));
        Files.createFile(descriptors.resolve("2"));

        assertEquals(3, WorkerRuntimeResourceProbe.readCurrentProcessOpenFiles(descriptors));

        final Path link = tempDir.resolve("fd-link");
        Files.createSymbolicLink(link, descriptors);
        assertThrows(IllegalStateException.class,
                () -> WorkerRuntimeResourceProbe.readCurrentProcessOpenFiles(link));
    }

    @Test
    void runtimeObservationMustFitCertifiedEnvelope() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resources"));
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, 640L * 1024 * 1024, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);
        final WorkerRuntimeResourceObservation observation = new WorkerRuntimeResourceObservation(
                128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                8L * 1024 * 1024 * 1024);

        assertDoesNotThrow(() -> envelope.validate(config, observation));
        assertThrows(IllegalArgumentException.class, () -> envelope.validate(config,
                new WorkerRuntimeResourceObservation(
                        512L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                        1024L * 1024 * 1024, 10_000, 10L * 1024 * 1024 * 1024,
                        8L * 1024 * 1024 * 1024)));
    }

    @Test
    void runtimeObservationPreservesProcessFdHeadroom() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fd-headroom"));
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024, 128L * 1024 * 1024, 128L * 1024 * 1024, 64L * 1024 * 1024,
                64L * 1024 * 1024, 640L * 1024 * 1024, 64L * 1024 * 1024, 1024L * 1024 * 1024,
                10_000, 1_000, 10L * 1024 * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024, 256L * 1024 * 1024, 16L * 1024 * 1024, 10_000);

        assertDoesNotThrow(() -> envelope.validate(config, new WorkerRuntimeResourceObservation(
                128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                1024L * 1024 * 1024, 10_000, 9_000,
                10L * 1024 * 1024 * 1024, 8L * 1024 * 1024 * 1024)));
        assertThrows(IllegalArgumentException.class, () -> envelope.validate(config,
                new WorkerRuntimeResourceObservation(
                        128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                        1024L * 1024 * 1024, 10_000, 9_001,
                        10L * 1024 * 1024 * 1024, 8L * 1024 * 1024 * 1024)));
        assertThrows(IllegalArgumentException.class, () -> new WorkerRuntimeResourceObservation(
                128L * 1024 * 1024, 64L * 1024 * 1024, 128L * 1024 * 1024,
                1024L * 1024 * 1024, 10_000, 10_001,
                10L * 1024 * 1024 * 1024, 8L * 1024 * 1024 * 1024));
    }
}
