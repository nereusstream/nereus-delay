package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RecoveryCatalogTest {
    @Test
    void publishesAncestryAdvancesFloorAndSelectsOnlyDescendants() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(1);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(2), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        assertEquals(1, catalog.publish(genesis, 0).catalogGeneration());

        final CheckpointManifest child = manifest(shard, topic, lineage, id16(3), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        assertEquals(2, catalog.publish(child, 1).catalogGeneration());
        final RecoveryFloor firstFloor = catalog.advanceFloor(genesis.checkpointId(), 2, id32(4));
        assertArrayEquals(genesis.checkpointId(), firstFloor.checkpointId());
        assertEquals(firstFloor, catalog.advanceFloor(genesis.checkpointId(), 2, id32(4)));
        assertEquals(child, catalog.publish(child, 1).manifest());
        assertEquals(List.of(genesis, child), catalog.recoverySet(child.checkpointId()));

        final RecoveryFloor secondFloor = catalog.advanceFloor(child.checkpointId(), 3, id32(5));
        assertArrayEquals(child.checkpointId(), secondFloor.checkpointId());
        assertEquals(List.of(child), catalog.recoverySet(child.checkpointId()));
        catalog.validatePublishedRestoreCandidate(child);
        assertEquals(child, catalog.selectRecoveryCandidate(child.checkpointId()));
        assertEquals(4, catalog.publish(child, 4).catalogGeneration());
    }

    @Test
    void floorCoverageRequiresExactAncestryAndFloorCounters() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(40);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(41), 0, 10, 10, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        final CheckpointManifest child = manifest(shard, topic, lineage, id16(42), 1, 11, 11,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, 1);
        catalog.advanceFloor(genesis.checkpointId(), 2, id32(43));

        assertTrue(catalog.proveFloorCoverage(child.checkpointId(), 10, genesis.appliedShardLogPosition()).isPresent());
        assertFalse(catalog.proveFloorCoverage(child.checkpointId(), 11, genesis.appliedShardLogPosition()).isPresent());
        assertFalse(catalog.proveFloorCoverage(genesis.checkpointId(), 10,
                new KafkaSourcePosition(shard, "cluster", topic, 11, null, 1_011)).isPresent());

        catalog.advanceFloor(child.checkpointId(), 3, id32(44));
        final RecoveryCatalog.FloorCoverage coverage = catalog.proveFloorCoverage(child.checkpointId(), 11,
                child.appliedShardLogPosition()).orElseThrow();
        assertArrayEquals(child.checkpointId(), coverage.floor().checkpointId());
        assertEquals(List.of(child), coverage.ancestry());
    }

    @Test
    void floorCannotMoveAcrossAPublishedSibling() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final byte[] lineage = id16(30);
        final CheckpointManifest genesis = manifest(shard, UUID.randomUUID(), lineage, id16(31), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        final CheckpointManifest first = manifest(shard, ((KafkaSourcePosition) genesis.appliedShardLogPosition())
                .nativeTopicUuid(), lineage, id16(32), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final CheckpointManifest sibling = manifest(shard, ((KafkaSourcePosition) genesis.appliedShardLogPosition())
                .nativeTopicUuid(), lineage, id16(33), 1, 3, 3,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(first, 1);
        catalog.publish(sibling, 2);
        catalog.advanceFloor(first.checkpointId(), 3, id32(34));
        assertThrows(IllegalStateException.class,
                () -> catalog.advanceFloor(sibling.checkpointId(), 4, id32(35)));
    }

    @Test
    void localRecoveryPinBindsCurrentFloorCandidateAndCatalogGeneration() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(60);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(61), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        final RecoveryFloor floor = catalog.advanceFloor(genesis.checkpointId(), 1, id32(62));
        final RecoveryFloorRefV1 floorRef = new RecoveryFloorRefV1(floor.recoveryLineageId(), floor.checkpointId(),
                floor.manifestSha256(), floor.catalogGeneration(), floor.appliedSourcePosition(),
                floor.includedMutationSequence(), List.of());
        final RecoveryCandidateRefV1 candidate = new RecoveryCandidateRefV1(
                RecoveryCandidateKindV1.CATALOG_CHECKPOINT, lineage, genesis.checkpointId(),
                genesis.manifestSha256(), null);
        final RecoveryPinV1 pin = new RecoveryPinV1(id16(63), new ShardSubjectV1(shard),
                new OwnerIdentityV1(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(64)),
                candidate, floorRef, floor.catalogGeneration(), id32(65));

        assertEquals(pin, catalog.createRecoveryPin(pin));
        assertEquals(java.util.Optional.of(pin), catalog.activeRecoveryPin());
        assertThrows(IllegalStateException.class, () -> catalog.createRecoveryPin(new RecoveryPinV1(
                id16(66), new ShardSubjectV1(shard), pin.owner(), candidate, floorRef,
                floor.catalogGeneration(), id32(67))));

        catalog.releaseRecoveryPin(pin);
        assertTrue(catalog.activeRecoveryPin().isEmpty());
        final CheckpointManifest child = manifest(shard, topic, lineage, id16(68), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, floor.catalogGeneration());
        assertThrows(IllegalStateException.class, () -> catalog.createRecoveryPin(pin));
    }

    @Test
    void catalogPublicationRequiresExactPublishedUploadIntentIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final byte[] lineage = id16(70);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), lineage, id16(71), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(manifest, 0);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(72),
                ProfileKindV1.OBJECT_STORE);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(), manifest.createdBy().ownerEpoch(), id32(73));
        final CheckpointUploadIntentV1 published = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard), lineage, manifest.checkpointId(), owner,
                uuidBytes(manifest.sourceStoreIncarnation()), id32(74), 1,
                null, null, profile, evidence(1_000), 5_000,
                CheckpointUploadStateV1.PUBLISHED, 2,
                new CheckpointResourceV1(lineage, manifest.checkpointId(), profile, Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/71/manifest"), Bytes.utf8("version-1"),
                        manifest.canonicalJsonBytes().length, manifest.manifestSha256()), null);

        assertEquals(1, catalog.publishUploadedCheckpoint(published, manifest, 1).catalogGeneration());
        assertEquals(1, new OxiaRecoveryCatalog(catalog)
                .publishUploadedCheckpoint(published, manifest, 1).catalogGeneration());
        assertThrows(IllegalArgumentException.class, () -> catalog.publishUploadedCheckpoint(
                new CheckpointUploadIntentV1(new ShardSubjectV1(shard), lineage, manifest.checkpointId(), owner,
                        uuidBytes(manifest.sourceStoreIncarnation()), id32(75), 1, null, null, profile,
                        evidence(1_000), 5_000, CheckpointUploadStateV1.PENDING_UPLOAD, 1, null, null),
                manifest, 2));
    }

    @Test
    void uploadedCheckpointPublicationRereadsExactManifestAfterResponseLoss() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 13);
        final byte[] lineage = id16(90);
        final UUID topic = UUID.randomUUID();
        final CheckpointManifest parent = manifest(shard, topic, lineage, id16(89), 0, 0, 0, null);
        final CheckpointManifest manifest = manifest(shard, topic, lineage, id16(91), 1, 1, 1,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())));
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(92),
                ProfileKindV1.OBJECT_STORE);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(), manifest.createdBy().ownerEpoch(), id32(93));
        final CheckpointUploadIntentV1 published = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard), lineage, manifest.checkpointId(), owner,
                uuidBytes(manifest.sourceStoreIncarnation()), id32(94), 1,
                parent.checkpointId(), parent.manifestSha256(), profile, evidence(2_000), 6_000,
                CheckpointUploadStateV1.PUBLISHED, 2,
                new CheckpointResourceV1(lineage, manifest.checkpointId(), profile, Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/91/manifest"), Bytes.utf8("version-1"),
                        manifest.canonicalJsonBytes().length, manifest.manifestSha256()), null);

        final RecoveryCatalog catalog = new RecoveryCatalog();
        assertEquals(1, catalog.publish(parent, 0).catalogGeneration());
        final RecoveryCatalog.Publication first = catalog.publishUploadedCheckpoint(published, manifest, 1);
        assertEquals(2, first.catalogGeneration());

        // Advance the catalog after the original publication, then retry with
        // the same base generation as the lost response's original CAS.
        catalog.advanceFloor(manifest.checkpointId(), 2, id32(95));
        final RecoveryCatalog.Publication reread = catalog.publishUploadedCheckpoint(published, manifest, 1);
        assertEquals(3, reread.catalogGeneration());
        assertEquals(manifest, reread.manifest());
    }

    @Test
    void uploadedCheckpointPublicationRejectsSameManifestWithDifferentObjectIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 14);
        final byte[] lineage = id16(100);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), lineage, id16(101), 0,
                1, 1, null);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("checkpoint-store"), 1, id32(102),
                ProfileKindV1.OBJECT_STORE);
        final OwnerIdentityV1 owner = new OwnerIdentityV1(manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(), manifest.createdBy().ownerEpoch(), id32(103));
        final CheckpointResourceV1 resource = new CheckpointResourceV1(lineage, manifest.checkpointId(), profile,
                Bytes.utf8("bucket"), Bytes.utf8("checkpoint/101/manifest"), Bytes.utf8("version-1"),
                manifest.canonicalJsonBytes().length, manifest.manifestSha256());
        final CheckpointUploadIntentV1 published = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard), lineage, manifest.checkpointId(), owner,
                uuidBytes(manifest.sourceStoreIncarnation()), id32(104), 1, null, null, profile,
                evidence(3_000), 7_000, CheckpointUploadStateV1.PUBLISHED, 2, resource, null);
        final CheckpointUploadIntentV1 conflicting = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard), lineage, manifest.checkpointId(), owner,
                uuidBytes(manifest.sourceStoreIncarnation()), id32(105), 1, null, null, profile,
                evidence(3_000), 7_000, CheckpointUploadStateV1.PUBLISHED, 2,
                new CheckpointResourceV1(lineage, manifest.checkpointId(), profile, Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/101/manifest"), Bytes.utf8("version-2"),
                        manifest.canonicalJsonBytes().length, manifest.manifestSha256()), null);

        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(manifest, 0);
        catalog.publishUploadedCheckpoint(published, manifest, 1);
        assertThrows(IllegalStateException.class,
                () -> catalog.publishUploadedCheckpoint(conflicting, manifest, 1));
    }

    @Test
    void typedFloorRequiresSameGenerationCursorDominance() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final byte[] lineage = id16(80);
        final EvidenceCursorV1 older = EvidenceCursorV1.kafka(id32(82), id16(83), id16(84),
                1, 4, 100, 11, 10);
        final CheckpointManifest genesis = manifest(shard, UUID.randomUUID(), lineage, id16(81), 0, 1, 1, null,
                List.of(older));
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        assertThrows(IllegalArgumentException.class,
                () -> catalog.advanceFloor(genesis.checkpointId(), 1, List.of()));
        final RecoveryFloorRefV1 first = catalog.advanceFloor(genesis.checkpointId(), 1, List.of(older));
        assertEquals(first, catalog.currentFloorRef().orElseThrow());
        assertEquals(first, catalog.advanceFloor(genesis.checkpointId(), 1, List.of(older)));

        final EvidenceCursorV1 newer = EvidenceCursorV1.kafka(id32(82), id16(83), id16(84),
                1, 4, 101, 12, 11);
        final CheckpointManifest child = manifest(shard, ((KafkaSourcePosition) genesis.appliedShardLogPosition())
                .nativeTopicUuid(), lineage, id16(85), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())),
                List.of(newer));
        catalog.publish(child, 2);
        final RecoveryFloorRefV1 second = catalog.advanceFloor(child.checkpointId(), 3, List.of(newer));
        assertEquals(second, catalog.currentFloorRef().orElseThrow());

        final EvidenceCursorV1 regressed = EvidenceCursorV1.kafka(id32(82), id16(83), id16(84),
                1, 4, 102, 10, 11);
        assertThrows(IllegalArgumentException.class,
                () -> catalog.advanceFloor(child.checkpointId(), 4, List.of(regressed)));
        assertThrows(IllegalStateException.class,
                () -> catalog.advanceFloor(child.checkpointId(), 4, id32(86)));
    }

    @Test
    void rejectsParentHashLineageAndSourceIdentityViolations() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] lineage = id16(10);
        final CheckpointManifest parent = manifest(shard, UUID.randomUUID(), lineage, id16(11), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(parent, 0);
        final CheckpointManifest wrongHash = manifest(shard, parent.appliedShardLogPosition() instanceof KafkaSourcePosition p
                ? p.nativeTopicUuid() : UUID.randomUUID(), lineage, id16(12), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(id32(13))));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(wrongHash, 1));

        final CheckpointManifest wrongSource = manifest(shard, UUID.randomUUID(), lineage, id16(14), 1, 2, 2,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(wrongSource, 1));
    }

    @Test
    void OxiaBoundaryDelegatesCasAndRejectsIdentityDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(50), id16(51), 0, 1, 1, null);
        final CheckpointManifest wrongPublication = manifest(shard, UUID.randomUUID(), id16(52), id16(53), 0,
                1, 1, null);
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(new RecoveryCatalog());
        assertEquals(1, authority.publish(manifest, 0).catalogGeneration());
        assertEquals(manifest.canonicalJson(), authority.manifest(manifest.checkpointId()).orElseThrow()
                .canonicalJson());
        authority.validatePublishedRestoreCandidate(manifest);

        final OxiaRecoveryCatalog.CasBackend malformed = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                return new RecoveryCatalog.Publication(wrongPublication, expected + 1, null);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                return null;
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] ignored) {
                return java.util.Optional.empty();
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {
            }

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] ignored,
                                                                                           final long sequence,
                                                                                           final io.nereusstream.delay.protocol.SourcePosition... positions) {
                return java.util.Optional.empty();
            }
        };
        assertThrows(IllegalStateException.class,
                () -> new OxiaRecoveryCatalog(malformed).publish(manifest, 0));
    }

    private static CheckpointManifest manifest(final ShardId shard, final UUID topic, final byte[] lineage,
                                               final byte[] checkpointId, final long lineageGeneration,
                                               final long offset, final long mutationSequence,
                                               final CheckpointManifest.ParentCheckpoint parent) {
        return manifest(shard, topic, lineage, checkpointId, lineageGeneration, offset, mutationSequence, parent,
                List.of());
    }

    private static CheckpointManifest manifest(final ShardId shard, final UUID topic, final byte[] lineage,
                                               final byte[] checkpointId, final long lineageGeneration,
                                               final long offset, final long mutationSequence,
                                               final CheckpointManifest.ParentCheckpoint parent,
                                               final List<EvidenceCursorV1> evidenceCursors) {
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", topic, offset, null,
                1_000 + offset);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry("CURRENT", 1, id32(20),
                Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(checkpointId, lineage, lineageGeneration, parent, null,
                new CheckpointManifest.CreatedBy(id32(21), id32(22), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_001, "TEST", id32(23), 1, offset, offset,
                        id32(24), 0, null), shard, id32(25), UUID.randomUUID(), 1, mutationSequence, position,
                id32(26), id32(27), evidenceCursors, List.of(file));
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
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, id32(76), 1, 2, 3,
                id32(77), 0, null);
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }
}
