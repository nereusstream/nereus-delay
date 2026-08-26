package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilesystemCheckpointDownloadAdapterTest {
    @TempDir
    Path tempDir;

    @Test
    void verifiesManifestAndStreamsEveryImmutableObjectBeforeAtomicPublication() throws Exception {
        final Fixture fixture = fixture();
        final Path objectRoot = tempDir.resolve("objects");
        final FilesystemCheckpointUploadAdapter upload = new FilesystemCheckpointUploadAdapter(objectRoot, "container");
        final CheckpointResource resource = upload.upload(new CheckpointUploadRequest(
                fixture.pending(),
                fixture.manifest(),
                fixture.sourceDirectory(),
                fixture.manifest().canonicalJsonBytes()));

        final FilesystemCheckpointDownloadAdapter download = new FilesystemCheckpointDownloadAdapter(
                objectRoot, new CheckpointManifestLimits(10, 1 << 20, 1 << 20, 1024, 1 << 20, 10, 1024));
        final Path restored = download.download(
                new CheckpointDownloadRequest(fixture.manifest(), resource),
                tempDir.resolve("restored").resolve("db"));

        assertTrue(Files.isDirectory(restored));
        final List<CheckpointFileInventory> actual = CheckpointFileInventory.collect(restored);
        assertEquals(fixture.manifest().files().size(), actual.size());
        for (int index = 0; index < actual.size(); index++) {
            assertEquals(
                    fixture.manifest().files().get(index).name(),
                    actual.get(index).name());
            assertEquals(
                    fixture.manifest().files().get(index).length(),
                    actual.get(index).length());
            assertArrayEquals(
                    fixture.manifest().files().get(index).checksum(),
                    actual.get(index).checksum());
        }
        assertArrayEquals(
                Files.readAllBytes(fixture.sourceDirectory().resolve("CURRENT")),
                Files.readAllBytes(restored.resolve("CURRENT")));
        assertArrayEquals(
                Files.readAllBytes(fixture.sourceDirectory().resolve("000001.sst")),
                Files.readAllBytes(restored.resolve("000001.sst")));
    }

    @Test
    void corruptObjectFailsClosedAndDoesNotPublishPartialTarget() throws Exception {
        final Fixture fixture = fixture();
        final Path objectRoot = tempDir.resolve("corrupt-objects");
        final FilesystemCheckpointUploadAdapter upload = new FilesystemCheckpointUploadAdapter(objectRoot, "container");
        final CheckpointResource resource = upload.upload(new CheckpointUploadRequest(
                fixture.pending(),
                fixture.manifest(),
                fixture.sourceDirectory(),
                fixture.manifest().canonicalJsonBytes()));
        final CheckpointManifest.FileEntry file = fixture.manifest().files().get(0);
        final Path object = objectRoot
                .resolve("checkpoints")
                .resolve(Bytes.hex(fixture.manifest().recoveryLineageId()))
                .resolve(Bytes.hex(fixture.manifest().checkpointId()))
                .resolve("objects")
                .resolve(FilesystemCheckpointUploadAdapter.objectFileName(file.objectKey(), file.objectVersion()));
        Files.write(object, bytes(file.length() == 0 ? 1 : Math.toIntExact(file.length()), 99));

        final Path target = tempDir.resolve("corrupt-target");
        final FilesystemCheckpointDownloadAdapter download = new FilesystemCheckpointDownloadAdapter(objectRoot);
        assertThrows(
                IllegalStateException.class,
                () -> download.download(new CheckpointDownloadRequest(fixture.manifest(), resource), target));
        assertFalse(Files.exists(target, java.nio.file.LinkOption.NOFOLLOW_LINKS));
        try (var entries = Files.list(target.getParent())) {
            assertTrue(entries.noneMatch(path -> path.getFileName().toString().contains("checkpoint-download")));
        }
    }

    @Test
    void existingTargetIsNeverMergedIntoOrOverwritten() throws Exception {
        final Fixture fixture = fixture();
        final Path objectRoot = tempDir.resolve("collision-objects");
        final FilesystemCheckpointUploadAdapter upload = new FilesystemCheckpointUploadAdapter(objectRoot, "container");
        final CheckpointResource resource = upload.upload(new CheckpointUploadRequest(
                fixture.pending(),
                fixture.manifest(),
                fixture.sourceDirectory(),
                fixture.manifest().canonicalJsonBytes()));
        final Path target = tempDir.resolve("existing-target");
        Files.createDirectories(target);
        Files.writeString(target.resolve("sentinel"), "keep");
        final FilesystemCheckpointDownloadAdapter download = new FilesystemCheckpointDownloadAdapter(objectRoot);
        assertThrows(
                IllegalStateException.class,
                () -> download.download(new CheckpointDownloadRequest(fixture.manifest(), resource), target));
        assertEquals("keep", Files.readString(target.resolve("sentinel")));
    }

    private Fixture fixture() throws Exception {
        final Path directory = tempDir.resolve("source-" + UUID.randomUUID());
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("CURRENT"), "MANIFEST-1\n");
        Files.writeString(directory.resolve("000001.sst"), "sst-bytes");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] lineage = bytes(16, 2);
        final byte[] checkpoint = bytes(16, 3);
        final UUID storeIncarnation = UUID.randomUUID();
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, bytes(32, 4), ProfileKind.OBJECT_STORE);
        final OwnerIdentity owner = new OwnerIdentity(bytes(8, 5), bytes(8, 6), 42, bytes(32, 7));
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(directory);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("object/" + file.name()),
                        Bytes.utf8("version-1"),
                        null))
                .toList();
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, 3, 1_000);
        final CheckpointManifest manifest = new CheckpointManifest(
                checkpoint,
                lineage,
                0,
                null,
                null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                new CheckpointManifest.CreatedAt(
                        900, 1_000, "CERTIFIED_HOST_CLOCK", bytes(8, 8), 1, 2, 3, bytes(32, 9), 0, null),
                shard,
                bytes(32, 10),
                storeIncarnation,
                1,
                7,
                position,
                bytes(32, 11),
                bytes(32, 12),
                List.of(),
                files);
        final CheckpointUploadIntent pending = new CheckpointUploadIntent(
                new ShardSubject(shard),
                lineage,
                checkpoint,
                owner,
                uuidBytes(storeIncarnation),
                bytes(32, 13),
                1,
                null,
                null,
                profile,
                evidence(900),
                5_000,
                CheckpointUploadState.PENDING_UPLOAD,
                1,
                null,
                null);
        return new Fixture(directory, profile, manifest, pending);
    }

    private record Fixture(
            Path sourceDirectory, ProfileRef profile, CheckpointManifest manifest, CheckpointUploadIntent pending) {}

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, 14),
                1,
                2,
                3,
                bytes(32, 15),
                0,
                null);
    }
}
