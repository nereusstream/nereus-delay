package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
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
        final UUID evidenceTopicUuid = UUID.randomUUID();
        final EvidenceCursorV1 kafkaCursor = EvidenceCursorV1.kafka(
                filled(32, 1), filled(16, 2), uuidBytes(evidenceTopicUuid), 1, 4, 100, 11, 10);
        final EvidenceCursorV1 pulsarCursor = EvidenceCursorV1.pulsar(
                filled(32, 4), filled(16, 5), filled(32, 6), 2, 7, 200,
                "persistent://tenant/ns/topic", 8, 9, 10, 1, 2);
        final CheckpointManifest manifest = new CheckpointManifest(
                bytes(1), bytes(2), 4, null, null,
                new CheckpointManifest.CreatedBy(bytes(3), bytes(4), 42),
                new CheckpointManifest.CreatedAt(1000, 1001, "CERTIFIED_HOST_CLOCK", bytes(5), 1, 2, 3,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null),
                shardId, Bytes.sha256(Bytes.utf8("db")), UUID.randomUUID(), 1, 7, position,
                new byte[32], new byte[32], List.of(pulsarCursor, kafkaCursor), files);

        final String first = manifest.canonicalJson();
        assertEquals(first, manifest.canonicalJson());
        assertTrue(first.startsWith("{\"appliedShardLogPosition\":"));
        assertTrue(first.contains("\"evidenceCursors\":[{"));
        assertEquals(32, manifest.manifestSha256().length);
        assertEquals(manifest.canonicalJson(), CheckpointManifest.decodeCanonicalJson(manifest.canonicalJsonBytes())
                .canonicalJson());
        assertThrows(IllegalArgumentException.class,
                () -> CheckpointManifest.decodeCanonicalJson((" " + first).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> CheckpointManifest.decodeCanonicalJson(
                first.replace("\"manifestVersion\":1", "\"manifestVersion\":2")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThrows(IllegalArgumentException.class, () -> new CheckpointManifest.CreatedAt(1, 1, "TEST_CLOCK",
                bytes(5), 1, 2, 3, Bytes.sha256(Bytes.utf8("evidence")), 0, null));
    }

    @Test
    void manifestRoundTripsUnsignedSourceAndEvidencePositions() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), -1);
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster-a", UUID.randomUUID(),
                Long.MIN_VALUE, 3, 1000);
        final EvidenceCursorV1 kafkaCursor = EvidenceCursorV1.kafka(
                filled(32, 1), filled(16, 2), filled(16, 3), 1, 4, 100,
                Long.MIN_VALUE, -1L);
        final EvidenceCursorV1 pulsarCursor = EvidenceCursorV1.pulsar(
                filled(32, 4), filled(16, 5), filled(32, 6), 2, 7, 200,
                "persistent://tenant/ns/topic", 8, Long.MIN_VALUE, -1L, 0, 1);
        final CheckpointManifest manifest = new CheckpointManifest(
                bytes(1), bytes(2), 4, null, null,
                new CheckpointManifest.CreatedBy(bytes(3), bytes(4), 42),
                new CheckpointManifest.CreatedAt(1000, 1001, "CERTIFIED_HOST_CLOCK", bytes(5), 1, 2, 3,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null),
                shardId, Bytes.sha256(Bytes.utf8("db")), UUID.randomUUID(), 1, 7, position,
                new byte[32], new byte[32], List.of(pulsarCursor, kafkaCursor), List.of(file("a.sst", 1)));

        final String json = manifest.canonicalJson();
        assertTrue(json.contains("\"partition\":4294967295"));
        assertTrue(json.contains("\"offset\":\"9223372036854775808\""));
        assertTrue(json.contains("\"nextOffsetExclusive\":\"9223372036854775808\""));
        assertTrue(json.contains("\"lastObservedLsoExclusive\":\"18446744073709551615\""));
        assertTrue(json.contains("\"ledgerId\":\"9223372036854775808\""));
        assertTrue(json.contains("\"entryId\":\"18446744073709551615\""));
        final CheckpointManifest decoded = CheckpointManifest.decodeCanonicalJson(manifest.canonicalJsonBytes());
        assertEquals(json, decoded.canonicalJson());
        assertEquals(Long.MIN_VALUE, ((KafkaSourcePosition) decoded.appliedShardLogPosition()).offset());
        assertTrue(decoded.evidenceCursors().stream().anyMatch(cursor ->
                cursor.evidenceKind().name().equals("KAFKA_RECEIPT_CONTIGUOUS")
                        && cursor.nextOffsetExclusive() == Long.MIN_VALUE
                        && cursor.lastObservedLsoExclusive() == -1L));
        assertTrue(decoded.evidenceCursors().stream().anyMatch(cursor ->
                cursor.evidenceKind().name().equals("PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS")
                        && cursor.ledgerId() == Long.MIN_VALUE && cursor.entryId() == -1L));
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

    @Test
    void inventoryRejectsNonCanonicalPathBeforeHashing() throws Exception {
        final Path root = tempDir.resolve("non-canonical-name-checkpoint");
        Files.createDirectories(root);
        Files.writeString(root.resolve("e\u0301.sst"), "bytes");
        assertThrows(IllegalArgumentException.class, () -> CheckpointFileInventory.collect(root));
    }

    @Test
    void inventoryAndManifestDecodeHonorExplicitPhysicalLimits() throws Exception {
        final Path root = tempDir.resolve("bounded-checkpoint");
        Files.createDirectories(root.resolve("nested"));
        Files.writeString(root.resolve("CURRENT"), "MANIFEST-1\n");
        Files.writeString(root.resolve("nested").resolve("000001.sst"), "sst-bytes");
        final CheckpointManifestLimits oneFile = new CheckpointManifestLimits(
                1, 1L << 20, 1L << 20, 1024, 1 << 20, 10, 1024);
        assertThrows(IllegalArgumentException.class, () -> CheckpointFileInventory.collect(root, oneFile));

        final CheckpointManifestLimits tinyManifest = new CheckpointManifestLimits(
                10, 1L << 20, 1L << 20, 1024, 1, 10, 1024);
        final byte[] manifestBytes = manifestFixture(root).canonicalJsonBytes();
        assertThrows(IllegalArgumentException.class,
                () -> CheckpointManifest.decodeCanonicalJson(manifestBytes, tinyManifest));

        final CheckpointManifestLimits oneManifestFile = new CheckpointManifestLimits(
                1, 1L << 20, 1L << 20, 1024, 1 << 20, 10, 1024);
        assertThrows(IllegalArgumentException.class,
                () -> CheckpointManifest.decodeCanonicalJson(manifestBytes, oneManifestFile));
    }

    @Test
    void manifestTotalFileBytesOverflowFailsAsValidationError() {
        final CheckpointManifest manifest = manifestWithFiles(List.of(
                file("a.sst", Long.MAX_VALUE), file("b.sst", 1)));

        assertThrows(IllegalArgumentException.class,
                () -> CheckpointManifestLimits.unbounded().validateManifest(manifest));
    }

    @Test
    void checkpointFilesUseUnsignedUtf8NameOrder() throws Exception {
        final Path root = tempDir.resolve("unicode-checkpoint");
        Files.createDirectories(root);
        final String bmpName = "a\uE000";
        final String supplementaryName = "a\uD800\uDC00";
        Files.writeString(root.resolve(supplementaryName), "supplementary");
        Files.writeString(root.resolve(bmpName), "bmp");
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(root);
        assertEquals(List.of(bmpName, supplementaryName), inventory.stream()
                .map(CheckpointFileInventory::name).toList());

        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        final CheckpointManifest manifest = manifestWithFiles(files);
        assertEquals(List.of(bmpName, supplementaryName), manifest.files().stream()
                .map(CheckpointManifest.FileEntry::name).toList());
    }

    private CheckpointManifest manifestFixture(final Path root) throws Exception {
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(root);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        return manifestWithFiles(files);
    }

    private CheckpointManifest manifestWithFiles(final List<CheckpointManifest.FileEntry> files) {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster-a", UUID.randomUUID(), 9,
                3, 1000);
        return new CheckpointManifest(bytes(1), bytes(2), 4, null, null,
                new CheckpointManifest.CreatedBy(bytes(3), bytes(4), 42),
                new CheckpointManifest.CreatedAt(1000, 1001, "CERTIFIED_HOST_CLOCK", bytes(5), 1, 2, 3,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null),
                shardId, Bytes.sha256(Bytes.utf8("db")), UUID.randomUUID(), 1, 7, position,
                new byte[32], new byte[32], List.of(), files);
    }

    private static CheckpointManifest.FileEntry file(final String name, final long length) {
        final byte[] checksum = new byte[32];
        java.util.Arrays.fill(checksum, (byte) 1);
        return new CheckpointManifest.FileEntry(name, length, checksum, Bytes.utf8("object/" + name),
                Bytes.utf8("version-1"), null);
    }

    private static byte[] bytes(final int last) {
        final byte[] value = new byte[16];
        value[15] = (byte) last;
        return value;
    }

    private static byte[] filled(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }
}
