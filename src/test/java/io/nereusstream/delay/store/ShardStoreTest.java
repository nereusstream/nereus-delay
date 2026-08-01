package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShardStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void oneShardUsesIndependentDbAndAtomicBatchSurvivesReopen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final byte[] key = KeyCodec.metaFixed(6);
        final byte[] payload = Bytes.utf8("source-position");
        final Path checkpoint;
        final byte[] dbIdentity;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            store.write(batch -> batch.putValue(ColumnFamily.META, 6, key, payload));
            assertArrayEquals(payload, store.getValue(ColumnFamily.META, key, 6).payload());
            checkpoint = tempDir.resolve("checkpoint");
            store.createCheckpoint(checkpoint);
            assertNotNull(store.latestSequenceNumber());
        }
        assertTrueFile(checkpoint.resolve("CURRENT"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertArrayEquals(dbIdentity, reopened.metadata().dbIdentity());
            assertArrayEquals(payload, reopened.getValue(ColumnFamily.META, key, 6).payload());
            final List<Path> dbs;
            try (var stream = Files.walk(tempDir.resolve("shards"))) {
                dbs = stream.filter(path -> path.getFileName().toString().equals("CURRENT")).toList();
            }
            assertEquals(1, dbs.size());
        }
    }

    @Test
    void completeCheckpointRestoresIntoFreshStoreIncarnation() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("source"));
        final Path checkpoint = tempDir.resolve("checkpoint-for-restore");
        final byte[] key = KeyCodec.metaFixed(7);
        final byte[] payload = Bytes.utf8("checkpoint-value");
        final byte[] originalStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            originalStoreIncarnation = store.metadata().storeIncarnation();
            store.write(batch -> batch.putValue(ColumnFamily.META, 7, key, payload));
            store.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("restored"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 7).payload());
            org.junit.jupiter.api.Assertions.assertFalse(
                    java.util.Arrays.equals(originalStoreIncarnation, restored.metadata().storeIncarnation()));
            assertNotEquals(sourceConfig.rootPath(), restoreConfig.rootPath());
        }
    }

    @Test
    void catalogBoundRestoreRequiresPublishedFloorEligibleManifest() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("catalog-source"));
        final Path checkpoint = tempDir.resolve("catalog-checkpoint");
        final byte[] key = KeyCodec.metaFixed(7);
        final byte[] payload = Bytes.utf8("catalog-value");
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> batch.putValue(ColumnFamily.META, 7, key, payload));
            store.createCheckpoint(checkpoint);
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(bytes(30), bytes(31), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(32), bytes(33), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "TEST_CLOCK", bytes(34), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 0, new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 0, null, 1_000),
                new byte[32], new byte[32], files);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        final ShardStoreConfig unpublishedConfig = ShardStoreConfig.defaults(tempDir.resolve("unpublished-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(unpublishedConfig)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.restoreFromCheckpoint(
                    unpublishedConfig, shardId, resources, checkpoint, manifest, catalog));
        }
        catalog.publish(manifest, 0);
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("catalog-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources,
                     checkpoint, manifest, catalog)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 7).payload());
        }
    }

    @Test
    void workerDbSlotLimitFailsBeforeOpeningAnotherShard() {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("bounded"), 1, 1, 32, 32,
                1, 1024 * 1024, 1024 * 1024, 1, 1, 1024);
        final ShardId first = new ShardId(RouteIncarnation.random(), 1);
        final ShardId second = new ShardId(RouteIncarnation.random(), 2);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore firstStore = ShardStore.open(config, first, resources)) {
            assertNotNull(firstStore.metadata());
            assertThrows(IllegalStateException.class, () -> ShardStore.open(config, second, resources));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore secondStore = ShardStore.open(config, second, resources)) {
            assertNotNull(secondStore.metadata());
        }
    }

    @Test
    void restoreWithManifestRejectsFileIdentityDrift() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("manifest-source"));
        final Path checkpoint = tempDir.resolve("manifest-checkpoint");
        final byte[] key = KeyCodec.metaFixed(8);
        final byte[] payload = Bytes.utf8("manifest-value");
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> batch.putValue(ColumnFamily.META, 8, key, payload));
            store.createCheckpoint(checkpoint);
        }
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpoint);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(bytes(10), bytes(11), 1, null, null,
                new CheckpointManifest.CreatedBy(bytes(12), bytes(13), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "TEST_CLOCK", bytes(14), 1, 1, 1,
                        Bytes.sha256(Bytes.utf8("evidence")), 1, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 1, new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 0, null, 1_000),
                new byte[32], new byte[32], files);

        final Path firstFile = checkpoint.resolve(inventory.get(0).name());
        Files.writeString(firstFile, "tampered");
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("manifest-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint, manifest));
        }
    }

    private static byte[] bytes(final int last) {
        final byte[] value = new byte[16];
        value[15] = (byte) last;
        return value;
    }

    private static void assertTrueFile(final Path path) {
        if (!Files.isRegularFile(path)) {
            throw new AssertionError("expected regular file: " + path);
        }
    }
}
