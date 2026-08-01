package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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

    private static void assertTrueFile(final Path path) {
        if (!Files.isRegularFile(path)) {
            throw new AssertionError("expected regular file: " + path);
        }
    }
}
