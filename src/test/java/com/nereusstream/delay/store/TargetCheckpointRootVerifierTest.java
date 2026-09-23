package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetCheckpointRootVerifierTest {
    @TempDir
    Path tempDir;

    @Test
    void refusesEmptyTargetWrongShardAndFormatOneImages() {
        final var shard = new ShardId(RouteIncarnation.random(), 2);
        final var another = new ShardId(RouteIncarnation.random(), 3);
        final var targetConfig = ShardStoreConfig.defaults(tempDir.resolve("target"));
        final Path targetDb;
        try (var resources = new SharedRocksDbResources(targetConfig);
                var store = ShardStore.openTarget(targetConfig, shard, resources)) {
            targetDb = store.dbPath();
        }
        final var limits = new CheckpointManifestLimits(100, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(targetDb, shard, CheckpointManifestLimits.unbounded()));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validate(
                                targetDb,
                                shard,
                                new CheckpointManifestLimits(1, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024)))
                .getMessage()
                .contains("file count"));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validate(targetDb, shard, limits))
                .getMessage()
                .contains("missing source position"));
        assertThrows(
                IllegalArgumentException.class, () -> TargetCheckpointRootVerifier.validate(targetDb, another, limits));

        final var laneConfig = ShardStoreConfig.defaults(tempDir.resolve("lane"));
        final Path laneDb;
        try (var resources = new SharedRocksDbResources(laneConfig);
                var store = ShardStore.open(laneConfig, shard, resources)) {
            laneDb = store.dbPath();
        }
        assertThrows(
                IllegalArgumentException.class, () -> TargetCheckpointRootVerifier.validate(laneDb, shard, limits));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(tempDir.resolve("absent"), shard, limits));
    }
}
