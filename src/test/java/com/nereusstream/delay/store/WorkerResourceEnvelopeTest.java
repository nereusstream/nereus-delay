package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkerResourceEnvelopeTest {
    @TempDir
    Path tempDir;

    @Test
    void validatesMutuallyExclusiveMemoryFdAndDiskBuckets() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024,
                128L * 1024 * 1024,
                128L * 1024 * 1024,
                64L * 1024 * 1024,
                64L * 1024 * 1024,
                640L * 1024 * 1024,
                64L * 1024 * 1024,
                1024L * 1024 * 1024,
                10_000,
                1_000,
                10L * 1024 * 1024 * 1024,
                2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024,
                256L * 1024 * 1024,
                16L * 1024 * 1024,
                10_000);
        assertDoesNotThrow(() -> envelope.validate(config));
        assertDoesNotThrow(() -> new SharedRocksDbResources(config, envelope).close());
    }

    @Test
    void rejectsUnknownOrOvercommittedRuntimeLimits() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("invalid"));
        final WorkerResourceEnvelope unknownCgroup =
                new WorkerResourceEnvelope(1, 1, 1, 1, 0, 4, 0, 0, 10_000, 1, 100, 0, 0, 0, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> unknownCgroup.validate(config));

        final WorkerResourceEnvelope insufficientMemory =
                new WorkerResourceEnvelope(256, 256, 256, 256, 256, 512, 0, 1024, 10_000, 1, 100, 0, 0, 0, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> insufficientMemory.validate(config));

        final WorkerResourceEnvelope insufficientFds =
                new WorkerResourceEnvelope(1, 1, 1, 1, 0, 16, 0, 32, 100, 99, 100, 0, 0, 0, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> insufficientFds.validate(config));
    }

    @Test
    void rejectsSharedRocksDbBudgetsOutsideTheCertifiedNativeBucket() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("native-budget"));
        final WorkerResourceEnvelope envelope = new WorkerResourceEnvelope(
                256L * 1024 * 1024,
                128L * 1024 * 1024,
                100L * 1024 * 1024,
                64L * 1024 * 1024,
                64L * 1024 * 1024,
                640L * 1024 * 1024,
                64L * 1024 * 1024,
                1024L * 1024 * 1024,
                10_000,
                1_000,
                10L * 1024 * 1024 * 1024,
                2L * 1024 * 1024 * 1024,
                256L * 1024 * 1024,
                256L * 1024 * 1024,
                16L * 1024 * 1024,
                10_000);

        assertThrows(IllegalArgumentException.class, () -> envelope.validate(config));
    }
}
