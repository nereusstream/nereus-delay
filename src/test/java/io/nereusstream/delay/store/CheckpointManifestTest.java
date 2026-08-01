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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointManifestTest {
    @TempDir
    Path tempDir;

    @Test
    void manifestUsesStableJcsProjectionAndChecksummedInventory() throws Exception {
        final Path root = tempDir.resolve("checkpoint");
        Files.createDirectories(root.resolve("nested"));
        Files.writeString(root.resolve("CURRENT"), "MANIFEST-1\n");
        Files.writeString(root.resolve("nested").resolve("000001.sst"), "sst-bytes");
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(root);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster-a", UUID.randomUUID(), 9,
                3, 1000);
        final CheckpointManifest manifest = new CheckpointManifest(
                bytes(1), bytes(2), 4, null, null,
                new CheckpointManifest.CreatedBy(bytes(3), bytes(4), 42),
                new CheckpointManifest.CreatedAt(1000, 1001, "TEST_CLOCK", bytes(5), 1, 2, 3,
                        Bytes.sha256(Bytes.utf8("evidence")), 1, null),
                shardId, Bytes.sha256(Bytes.utf8("db")), UUID.randomUUID(), 1, 7, position,
                new byte[32], new byte[32], files);

        final String first = manifest.canonicalJson();
        assertEquals(first, manifest.canonicalJson());
        assertTrue(first.startsWith("{\"appliedShardLogPosition\":"));
        assertTrue(first.contains("\"evidenceCursors\":[]"));
        assertEquals(32, manifest.manifestSha256().length);
    }

    @Test
    void inventoryRejectsSymlinkedCheckpointFiles() throws Exception {
        final Path root = tempDir.resolve("symlink-checkpoint");
        Files.createDirectories(root);
        final Path target = tempDir.resolve("outside");
        Files.writeString(target, "outside-bytes");
        try {
            Files.createSymbolicLink(root.resolve("CURRENT"), target);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        assertThrows(IllegalArgumentException.class, () -> CheckpointFileInventory.collect(root));
    }

    private static byte[] bytes(final int last) {
        final byte[] value = new byte[16];
        value[15] = (byte) last;
        return value;
    }
}
