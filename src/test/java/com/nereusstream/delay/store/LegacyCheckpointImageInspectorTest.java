package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LegacyCheckpointImageInspectorTest {
    @TempDir
    Path tempDir;

    @Test
    void verifiesFormatOneCheckpointIdentityAndLeavesTheImageUnchanged() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("store"));
        final Path image = tempDir.resolve("legacy-checkpoint");
        final byte[] checkpointId = bytes(16, 1);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final CheckpointManifest manifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                        source.canonicalBytes());
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(17));
                final DelayMessageId messageId = DelayMessageId.random(shard);
                final MessageRecord scheduled = MessageRecord.current(
                        MessageStatus.SCHEDULED,
                        1,
                        1,
                        5_000,
                        8_000,
                        DestinationLaneId.derive(Bytes.utf8("legacy-inventory-lane")),
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload"),
                        source.canonicalBytes());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), scheduled.encode());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final List<CheckpointFileInventory> before = CheckpointFileInventory.collect(image, finiteLimits());
        final LegacyCheckpointImageInspector.ImageProof proof =
                LegacyCheckpointImageInspector.inspect(image, shard, manifest, finiteLimits());
        final List<CheckpointFileInventory> after = CheckpointFileInventory.collect(image, finiteLimits());

        assertEquals(1, proof.metadata().storeFormatVersion());
        assertEquals(shard, proof.metadata().shardId());
        assertEquals(manifest.sourceStoreIncarnation(), proof.metadata().storeIncarnationUuid());
        assertEquals(17, proof.mutationSequence());
        assertEquals(source, proof.appliedSourcePosition());
        assertTrue(Bytes.constantTimeEquals(checkpointId, proof.checkpointId()));
        assertEquals(proof.physicalBytes(), before.stream().mapToLong(CheckpointFileInventory::length).sum());
        assertFileInventoriesEqual(before, after);

        final LegacyCheckpointStateInventory.Inventory state = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertEquals(1, state.messageStatuses().get(MessageStatus.SCHEDULED));
        assertTrue(state.recordsByFamily().get(ColumnFamily.ID) > 0);
        assertEquals(
                state.scannedRecords(),
                state.recordsByFamily().values().stream().mapToLong(Long::longValue).sum());
        assertTrue(state.scannedBytes() > 0);
        assertEquals(state.scannedBytes(), state.chargedBytes());
        assertThrows(
                ReadIncompleteException.class,
                () -> LegacyCheckpointStateInventory.inspect(
                        image,
                        shard,
                        manifest,
                        finiteLimits(),
                        new LegacyCheckpointStateInventory.ReadLimits(1, 1 << 20, 60_000_000_000L)));
    }

    @Test
    void rejectsAnImageThatChangesAfterItsManifestWasCaptured() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("changed-store"));
        final Path image = tempDir.resolve("changed-checkpoint");
        final byte[] checkpointId = bytes(16, 2);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final CheckpointManifest manifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            store.write(batch -> batch.putValue(
                    ColumnFamily.META,
                    ShardStore.META_FIXED_VALUE_TYPE,
                    KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                    source.canonicalBytes()));
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        Files.writeString(image.resolve("unexpected-migration-input"), "changed");
        final IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> LegacyCheckpointImageInspector.inspect(image, shard, manifest, finiteLimits()));
        assertTrue(failure.getMessage().contains("physical files"));
    }

    @Test
    void rejectsMalformedMessageStateWithoutChangingTheCheckpoint() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-store"));
        final Path image = tempDir.resolve("malformed-checkpoint");
        final byte[] checkpointId = bytes(16, 3);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final CheckpointManifest manifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                        source.canonicalBytes());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(DelayMessageId.random(shard)),
                        Bytes.utf8("malformed-message-record"));
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final List<CheckpointFileInventory> before = CheckpointFileInventory.collect(image, finiteLimits());
        assertThrows(
                IllegalArgumentException.class,
                () -> LegacyCheckpointStateInventory.inspect(
                        image,
                        shard,
                        manifest,
                        finiteLimits(),
                        new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L)));
        final List<CheckpointFileInventory> after = CheckpointFileInventory.collect(image, finiteLimits());
        assertFileInventoriesEqual(before, after);
    }

    private static CheckpointManifest manifestFor(
            final Path image,
            final ShardId shard,
            final ShardStore store,
            final byte[] checkpointId,
            final KafkaSourcePosition source) {
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(image).stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("legacy/" + file.name()),
                        Bytes.utf8("version-1"),
                        null))
                .toList();
        return new CheckpointManifest(
                checkpointId,
                bytes(16, 11),
                1,
                null,
                null,
                new CheckpointManifest.CreatedBy(bytes(8, 12), bytes(8, 13), 1),
                new CheckpointManifest.CreatedAt(
                        900,
                        1_000,
                        "CERTIFIED_HOST_CLOCK",
                        bytes(8, 14),
                        1,
                        2,
                        3,
                        Bytes.sha256(Bytes.utf8("migration-time")),
                        0,
                        null),
                shard,
                store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(),
                1,
                store.shardMutationSequence(),
                source,
                new byte[32],
                bytes(32, 15),
                List.of(),
                files);
    }

    private static CheckpointManifestLimits finiteLimits() {
        return new CheckpointManifestLimits(1_024, 1L << 30, 1L << 28, 4_096, 1 << 20, 256, 4_096);
    }

    private static void assertFileInventoriesEqual(
            final List<CheckpointFileInventory> expected, final List<CheckpointFileInventory> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).name(), actual.get(index).name());
            assertEquals(expected.get(index).length(), actual.get(index).length());
            assertTrue(Bytes.constantTimeEquals(expected.get(index).checksum(), actual.get(index).checksum()));
        }
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
