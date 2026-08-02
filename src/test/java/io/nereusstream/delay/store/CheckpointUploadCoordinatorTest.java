package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckpointUploadCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void inventoriesCheckpointAndPublishesOnlyAfterExactResourceValidation() throws Exception {
        final Fixture fixture = fixture();
        final CheckpointUploadIntentStore intentStore = new CheckpointUploadIntentStore();
        intentStore.create(fixture.pending());
        final AtomicBoolean called = new AtomicBoolean();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("resources")))) {
            final CheckpointUploadIntentV1 published = new CheckpointUploadCoordinator(resources, intentStore)
                    .upload(fixture.directory(), fixture.pending(), fixture.manifest(), 1_000, request -> {
                        called.set(true);
                        assertArrayEquals(fixture.manifest().canonicalJsonBytes(), request.manifestBytes());
                        assertEquals(fixture.directory(), request.checkpointDirectory());
                        return fixture.resource();
                    });
            assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
            assertEquals(fixture.resource(), published.publishedManifest());
            assertEquals(published, intentStore.current().orElseThrow());
            assertEquals(true, called.get());
        }
    }

    @Test
    void providerFailureAndWrongIdentityLeavePendingIntentForRetry() throws Exception {
        final Fixture fixture = fixture();
        final CheckpointUploadIntentStore intentStore = new CheckpointUploadIntentStore();
        intentStore.create(fixture.pending());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("retry")))) {
            final CheckpointUploadCoordinator coordinator = new CheckpointUploadCoordinator(resources, intentStore);
            assertThrows(IllegalStateException.class, () -> coordinator.upload(fixture.directory(), fixture.pending(),
                    fixture.manifest(), 1_000, request -> {
                        throw new IllegalStateException("provider response lost");
                    }));
            assertEquals(fixture.pending(), intentStore.current().orElseThrow());

            final CheckpointResourceV1 wrong = new CheckpointResourceV1(fixture.pending().recoveryLineageId(),
                    bytes(16, 90), fixture.profile(), bytes(4, 1), bytes(4, 2), bytes(4, 3),
                    fixture.manifest().canonicalJsonBytes().length, fixture.manifest().manifestSha256());
            assertThrows(IllegalArgumentException.class, () -> coordinator.upload(fixture.directory(),
                    fixture.pending(), fixture.manifest(), 1_000, request -> wrong));
            assertEquals(fixture.pending(), intentStore.current().orElseThrow());

            final CheckpointUploadIntentV1 published = coordinator.upload(fixture.directory(), fixture.pending(),
                    fixture.manifest(), 1_000, request -> fixture.resource());
            assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
        }
    }

    @Test
    void rejectsDeadlineAndLocalFileDriftBeforeAdapterInvocation() throws Exception {
        final Fixture fixture = fixture();
        final CheckpointUploadIntentStore intentStore = new CheckpointUploadIntentStore();
        intentStore.create(fixture.pending());
        final AtomicBoolean called = new AtomicBoolean();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(
                ShardStoreConfig.defaults(tempDir.resolve("reject")))) {
            final CheckpointUploadCoordinator coordinator = new CheckpointUploadCoordinator(resources, intentStore);
            assertThrows(IllegalStateException.class, () -> coordinator.upload(fixture.directory(), fixture.pending(),
                    fixture.manifest(), 5_001, request -> {
                        called.set(true);
                        return fixture.resource();
                    }));
            assertEquals(false, called.get());
            Files.writeString(fixture.directory().resolve("CURRENT"), "tampered\n");
            assertThrows(IllegalArgumentException.class, () -> coordinator.upload(fixture.directory(), fixture.pending(),
                    fixture.manifest(), 1_000, request -> {
                        called.set(true);
                        return fixture.resource();
                    }));
            assertEquals(false, called.get());
            assertEquals(fixture.pending(), intentStore.current().orElseThrow());
        }
    }

    private Fixture fixture() throws Exception {
        final Path directory = tempDir.resolve("checkpoint-" + UUID.randomUUID());
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("CURRENT"), "MANIFEST-1\n");
        Files.writeString(directory.resolve("000001.sst"), "sst-bytes");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] lineage = bytes(16, 2);
        final byte[] checkpoint = bytes(16, 3);
        final UUID storeIncarnation = UUID.randomUUID();
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, bytes(32, 4),
                ProfileKindV1.OBJECT_STORE);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 5), bytes(8, 6), 42, bytes(32, 7));
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(directory);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9,
                3, 1_000);
        final CheckpointManifest manifest = new CheckpointManifest(checkpoint, lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                new CheckpointManifest.CreatedAt(900, 1_000, "TEST_CLOCK", bytes(8, 8), 1, 2, 3,
                        bytes(32, 9), 1, null), shard, bytes(32, 10), storeIncarnation, 1, 7, position,
                bytes(32, 11), bytes(32, 12), List.of(), files);
        final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard.routeIncarnation(), shard.partition()), lineage, checkpoint, owner,
                uuidBytes(storeIncarnation), bytes(32, 13), 11, null, null, profile,
                evidence(900), 5_000, CheckpointUploadStateV1.PENDING_UPLOAD,
                1, null, null);
        final CheckpointResourceV1 resource = new CheckpointResourceV1(lineage, checkpoint, profile,
                bytes(4, 16), bytes(8, 17), bytes(8, 18), manifest.canonicalJsonBytes().length,
                manifest.manifestSha256());
        return new Fixture(directory, profile, manifest, pending, resource);
    }

    private record Fixture(Path directory, ProfileRefV1 profile, CheckpointManifest manifest,
                           CheckpointUploadIntentV1 pending, CheckpointResourceV1 resource) {
    }

    private static byte[] bytes(final int length, final int seed) {
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

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 14), 1, 2, 3,
                bytes(32, 15), 0, null);
    }
}
