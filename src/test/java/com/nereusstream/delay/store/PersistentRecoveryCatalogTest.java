package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResourceV1;
import com.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import com.nereusstream.delay.protocol.CheckpointUploadStateV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentityV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import com.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import com.nereusstream.delay.protocol.RecoveryFloorRefV1;
import com.nereusstream.delay.protocol.RecoveryPinV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PersistentRecoveryCatalogTest {
    @TempDir
    Path tempDirectory;

    @Test
    void catalogSnapshotSurvivesReopenWithFloorPinAndAncestry() {
        final Path stateFile = tempDirectory.resolve("catalog.state");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(1);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(2), 0, 1, 1, null);
        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(3),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));

        final PersistentRecoveryCatalog catalog = new PersistentRecoveryCatalog(stateFile);
        catalog.publish(genesis, 0);
        catalog.publish(child, 1);
        final RecoveryFloorRefV1 floor = catalog.advanceFloor(child.checkpointId(), 2, List.of());
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT,
                lineage,
                child.checkpointId(),
                child.manifestSha256(),
                null);
        final RecoveryPinV1 pin = new RecoveryPinV1(
                id16(4),
                new ShardSubjectV1(shard),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(5)),
                candidate,
                floor,
                floor.catalogGeneration(),
                id32(6));
        catalog.createRecoveryPin(pin);

        final PersistentRecoveryCatalog reopened = new PersistentRecoveryCatalog(stateFile);
        assertManifestEquals(genesis, reopened.manifest(genesis.checkpointId()).orElseThrow());
        assertManifestEquals(child, reopened.manifest(child.checkpointId()).orElseThrow());
        assertEquals(floor, reopened.currentFloorRef().orElseThrow());
        assertEquals(pin, reopened.activeRecoveryPin().orElseThrow());
        final List<CheckpointManifest> ancestry = reopened.proveFloorCoverage(
                        child.checkpointId(), 2, child.appliedShardLogPosition())
                .orElseThrow()
                .ancestry();
        assertEquals(1, ancestry.size());
        assertManifestEquals(child, ancestry.get(0));
        reopened.validatePublishedRestoreCandidate(child);
    }

    @Test
    void historicalRecoveryPinSurvivesReopenAfterFloorAdvances() {
        final Path stateFile = tempDirectory.resolve("historical-pin.state");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 71);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(171);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(172), 0, 1, 1, null);
        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(173),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final CheckpointManifest grandchild = manifest(
                shard,
                topic,
                lineage,
                id16(174),
                2,
                3,
                3,
                new CheckpointManifest.ParentCheckpoint(child.checkpointId(), Bytes.hex(child.manifestSha256())));

        final PersistentRecoveryCatalog catalog = new PersistentRecoveryCatalog(stateFile);
        catalog.publish(genesis, 0);
        catalog.publish(child, 1);
        final RecoveryFloorRefV1 childFloor = catalog.advanceFloor(child.checkpointId(), 2, List.of());
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT,
                lineage,
                child.checkpointId(),
                child.manifestSha256(),
                null);
        final RecoveryPinV1 pin = new RecoveryPinV1(
                id16(175),
                new ShardSubjectV1(shard),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(176)),
                candidate,
                childFloor,
                childFloor.catalogGeneration(),
                id32(177));
        catalog.createRecoveryPin(pin);

        catalog.publish(grandchild, childFloor.catalogGeneration());
        catalog.advanceFloor(grandchild.checkpointId(), childFloor.catalogGeneration() + 1, List.of());

        final PersistentRecoveryCatalog reopened = new PersistentRecoveryCatalog(stateFile);
        assertEquals(pin, reopened.activeRecoveryPin().orElseThrow());
        assertManifestEquals(
                grandchild,
                reopened.currentFloorRef()
                        .map(floor -> reopened.manifest(floor.checkpointId()).orElseThrow())
                        .orElseThrow());
    }

    @Test
    void separateInstancesShareTheGenerationCasBoundary() {
        final Path stateFile = tempDirectory.resolve("catalog.state");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(11);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(12), 0, 1, 1, null);
        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(13),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final PersistentRecoveryCatalog first = new PersistentRecoveryCatalog(stateFile);
        final PersistentRecoveryCatalog second = new PersistentRecoveryCatalog(stateFile);

        first.publish(genesis, 0);
        assertThrows(IllegalStateException.class, () -> second.publish(child, 0));
        assertEquals(child, second.publish(child, 1).manifest());
        assertManifestEquals(child, first.manifest(child.checkpointId()).orElseThrow());
    }

    @Test
    void checksumCorruptionFailsClosedBeforeOpeningCatalog() throws Exception {
        final Path stateFile = tempDirectory.resolve("catalog.state");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final CheckpointManifest genesis = manifest(shard, UUID.randomUUID(), id16(21), id16(22), 0, 1, 1, null);
        new PersistentRecoveryCatalog(stateFile).publish(genesis, 0);
        final byte[] corrupted = Files.readAllBytes(stateFile);
        corrupted[corrupted.length - 1] ^= 0x01;
        Files.write(stateFile, corrupted);

        assertThrows(IllegalStateException.class, () -> new PersistentRecoveryCatalog(stateFile));
    }

    @Test
    void snapshotEncoderRejectsResourceMapAliasBeforeEmittingBytes() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 90);
        final CheckpointManifest checkpoint = manifest(shard, UUID.randomUUID(), id16(91), id16(92), 0, 1, 1, null);
        final ProfileRefV1 profile =
                new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(93), ProfileKindV1.OBJECT_STORE);
        final CheckpointResourceV1 resource = new CheckpointResourceV1(
                checkpoint.recoveryLineageId(),
                checkpoint.checkpointId(),
                profile,
                Bytes.utf8("bucket"),
                Bytes.utf8("manifest"),
                Bytes.utf8("version"),
                checkpoint.canonicalJsonBytes().length,
                checkpoint.manifestSha256());
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, shard, List.of(checkpoint), java.util.Map.of("alias", resource), null, null, null);

        assertThrows(IllegalStateException.class, () -> PersistentRecoveryCatalog.encodeSnapshot(snapshot));
    }

    @Test
    void snapshotEncoderRejectsForeignCatalogShardBeforeEmittingBytes() {
        final ShardId manifestShard = new ShardId(RouteIncarnation.random(), 94);
        final ShardId catalogShard = new ShardId(RouteIncarnation.random(), 95);
        final CheckpointManifest checkpoint =
                manifest(manifestShard, UUID.randomUUID(), id16(96), id16(97), 0, 1, 1, null);
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                1, catalogShard, List.of(checkpoint), java.util.Map.of(), null, null, null);

        assertThrows(IllegalArgumentException.class, () -> PersistentRecoveryCatalog.encodeSnapshot(snapshot));
    }

    @Test
    void rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary() throws Exception {
        final Path parentRoot = tempDirectory.resolve("catalog-parent");
        final Path outside = tempDirectory.resolve("catalog-outside");
        Files.createDirectories(parentRoot);
        Files.createDirectories(outside);
        Files.createSymbolicLink(parentRoot.resolve("nested"), outside);

        final Path stateFile = parentRoot.resolve("nested/state.bin");
        assertThrows(IllegalStateException.class, () -> new PersistentRecoveryCatalog(stateFile));
        assertFalse(Files.exists(outside.resolve("state.bin"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.exists(outside.resolve("state.bin.lock"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void publishedObjectIdentitySurvivesReopenAndRejectsAConflictingRetry() {
        final Path stateFile = tempDirectory.resolve("catalog.state");
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final byte[] lineage = id16(51);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), lineage, id16(52), 0, 1, 1, null);
        final ProfileRefV1 profile =
                new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(53), ProfileKindV1.OBJECT_STORE);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(
                manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(),
                manifest.createdBy().ownerEpoch(),
                id32(54));
        final CheckpointResourceV1 resource = new CheckpointResourceV1(
                lineage,
                manifest.checkpointId(),
                profile,
                Bytes.utf8("bucket"),
                Bytes.utf8("checkpoint/52/manifest"),
                Bytes.utf8("version-1"),
                manifest.canonicalJsonBytes().length,
                manifest.manifestSha256());
        final CheckpointUploadIntentV1 published = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard),
                lineage,
                manifest.checkpointId(),
                owner,
                uuidBytes(manifest.sourceStoreIncarnation()),
                id32(55),
                1,
                null,
                null,
                profile,
                evidence(5_000),
                6_000,
                CheckpointUploadStateV1.PUBLISHED,
                2,
                resource,
                null);

        final PersistentRecoveryCatalog catalog = new PersistentRecoveryCatalog(stateFile);
        catalog.publish(manifest, 0);
        assertEquals(
                1, catalog.publishUploadedCheckpoint(published, manifest, 1).catalogGeneration());
        final PersistentRecoveryCatalog reopened = new PersistentRecoveryCatalog(stateFile);
        assertEquals(
                1, reopened.publishUploadedCheckpoint(published, manifest, 1).catalogGeneration());

        final CheckpointUploadIntentV1 conflicting = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard),
                lineage,
                manifest.checkpointId(),
                owner,
                uuidBytes(manifest.sourceStoreIncarnation()),
                id32(56),
                1,
                null,
                null,
                profile,
                evidence(5_000),
                6_000,
                CheckpointUploadStateV1.PUBLISHED,
                2,
                new CheckpointResourceV1(
                        lineage,
                        manifest.checkpointId(),
                        profile,
                        Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/52/manifest"),
                        Bytes.utf8("version-2"),
                        manifest.canonicalJsonBytes().length,
                        manifest.manifestSha256()),
                null);
        assertThrows(IllegalStateException.class, () -> reopened.publishUploadedCheckpoint(conflicting, manifest, 0));
    }

    @Test
    void scalarFloorEncodingIsCanonicalAndStrictlyDecoded() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(31), id16(32), 0, 1, 1, null);
        final RecoveryFloor floor = RecoveryFloor.create(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                id32(33));

        assertEquals(floor, RecoveryFloor.decode(floor.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> RecoveryFloor.decode(
                        java.util.Arrays.copyOf(floor.canonicalBytes(), floor.canonicalBytes().length - 1)));
        final byte[] trailing = Bytes.concat(floor.canonicalBytes(), new byte[] {0});
        assertThrows(IllegalArgumentException.class, () -> RecoveryFloor.decode(trailing));
        final byte[] badDigest = floor.canonicalBytes();
        badDigest[badDigest.length - 1] ^= 0x01;
        assertThrows(IllegalArgumentException.class, () -> RecoveryFloor.decode(badDigest));
        assertTrue(floor.canonicalBytes().length > 0);
    }

    private static CheckpointManifest manifest(
            final ShardId shard,
            final UUID topic,
            final byte[] lineage,
            final byte[] checkpointId,
            final long lineageGeneration,
            final long offset,
            final long mutationSequence,
            final CheckpointManifest.ParentCheckpoint parent) {
        final KafkaSourcePosition position =
                new KafkaSourcePosition(shard, "cluster", topic, offset, null, 1_000 + offset);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry(
                "CURRENT", 1, id32(40), Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(
                checkpointId,
                lineage,
                lineageGeneration,
                parent,
                null,
                new CheckpointManifest.CreatedBy(id32(41), id32(42), 1),
                new CheckpointManifest.CreatedAt(
                        1_000, 1_001, "CERTIFIED_HOST_CLOCK", id32(43), 1, offset, offset, id32(44), 0, null),
                shard,
                id32(45),
                UUID.randomUUID(),
                1,
                mutationSequence,
                position,
                id32(46),
                id32(47),
                List.of(),
                List.of(file));
    }

    private static byte[] id16(final int value) {
        final byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return bytes;
    }

    private static byte[] id32(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return bytes;
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                id32(57),
                1,
                2,
                3,
                id32(58),
                0,
                null);
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static void assertManifestEquals(final CheckpointManifest expected, final CheckpointManifest actual) {
        assertArrayEquals(expected.canonicalJsonBytes(), actual.canonicalJsonBytes());
    }
}
