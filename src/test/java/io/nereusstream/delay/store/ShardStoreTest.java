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

    private static void assertTrueFile(final Path path) {
        if (!Files.isRegularFile(path)) {
            throw new AssertionError("expected regular file: " + path);
        }
    }
}
