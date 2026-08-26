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
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryInstallPhase;
import com.nereusstream.delay.protocol.RecoveryInstallState;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RecoveryCatalogTest {
    @Test
    void snapshotRejectsPublishedStateAtCatalogGenerationZero() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 31);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(310), id16(311), 0, 1, 1, null);

        assertThrows(
                IllegalArgumentException.class,
                () -> RecoveryCatalog.fromSnapshot(
                        new RecoveryCatalog.Snapshot(0, shard, List.of(manifest), Map.of(), null, null, null)));
    }

    @Test
    void snapshotRejectsCurrentFloorOnAnotherBranchOfPinnedObservedFloor() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 32);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(320);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(321), 0, 1, 1, null);
        final CheckpointManifest first = manifest(
                shard,
                topic,
                lineage,
                id16(322),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final CheckpointManifest sibling = manifest(
                shard,
                topic,
                lineage,
                id16(323),
                1,
                3,
                3,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final RecoveryFloorRef currentTypedFloor = new RecoveryFloorRef(
                sibling.recoveryLineageId(),
                sibling.checkpointId(),
                sibling.manifestSha256(),
                4,
                sibling.appliedShardLogPosition(),
                sibling.shardMutationSequence(),
                List.of());
        final RecoveryFloor currentFloor = RecoveryFloor.create(
                sibling.recoveryLineageId(),
                sibling.checkpointId(),
                sibling.manifestSha256(),
                4,
                sibling.appliedShardLogPosition(),
                sibling.shardMutationSequence(),
                currentTypedFloor.floorDigest());
        final RecoveryFloorRef observedGenesisFloor = new RecoveryFloorRef(
                genesis.recoveryLineageId(),
                genesis.checkpointId(),
                genesis.manifestSha256(),
                4,
                genesis.appliedShardLogPosition(),
                genesis.shardMutationSequence(),
                List.of());
        final RecoveryPin pin = new RecoveryPin(
                id16(324),
                new ShardSubject(shard),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(325)),
                new RecoveryCandidateRef(
                        RecoveryCandidateKind.CATALOG_CHECKPOINT,
                        lineage,
                        first.checkpointId(),
                        first.manifestSha256(),
                        null),
                observedGenesisFloor,
                4,
                id32(326));

        assertThrows(
                IllegalArgumentException.class,
                () -> RecoveryCatalog.fromSnapshot(new RecoveryCatalog.Snapshot(
                        4, shard, List.of(genesis, first, sibling), Map.of(), currentFloor, currentTypedFloor, pin)));
    }

    @Test
    void publishesAncestryAdvancesFloorAndSelectsOnlyDescendants() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(1);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(2), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        assertEquals(1, catalog.publish(genesis, 0).catalogGeneration());

        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(3),
                1,
                2,
                2,
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
        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(42),
                1,
                11,
                11,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, 1);
        catalog.advanceFloor(genesis.checkpointId(), 2, id32(43));

        assertTrue(catalog.proveFloorCoverage(child.checkpointId(), 10, genesis.appliedShardLogPosition())
                .isPresent());
        final KafkaSourcePosition conflictingSameOffset = (KafkaSourcePosition) genesis.appliedShardLogPosition();
        assertFalse(catalog.proveFloorCoverage(
                        child.checkpointId(),
                        10,
                        new KafkaSourcePosition(
                                shard,
                                conflictingSameOffset.authenticatedClusterId(),
                                conflictingSameOffset.nativeTopicUuid(),
                                conflictingSameOffset.offset(),
                                7,
                                conflictingSameOffset.brokerLogAppendTimeEpochMs() + 1))
                .isPresent());
        assertFalse(catalog.proveFloorCoverage(child.checkpointId(), 11, genesis.appliedShardLogPosition())
                .isPresent());
        assertFalse(catalog.proveFloorCoverage(
                        genesis.checkpointId(), 10, new KafkaSourcePosition(shard, "cluster", topic, 11, null, 1_011))
                .isPresent());

        catalog.advanceFloor(child.checkpointId(), 3, id32(44));
        final RecoveryCatalog.FloorCoverage coverage = catalog.proveFloorCoverage(
                        child.checkpointId(), 11, child.appliedShardLogPosition())
                .orElseThrow();
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
        final CheckpointManifest first = manifest(
                shard,
                ((KafkaSourcePosition) genesis.appliedShardLogPosition()).nativeTopicUuid(),
                lineage,
                id16(32),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final CheckpointManifest sibling = manifest(
                shard,
                ((KafkaSourcePosition) genesis.appliedShardLogPosition()).nativeTopicUuid(),
                lineage,
                id16(33),
                1,
                3,
                3,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(first, 1);
        catalog.publish(sibling, 2);
        catalog.advanceFloor(first.checkpointId(), 3, id32(34));
        assertThrows(IllegalStateException.class, () -> catalog.advanceFloor(sibling.checkpointId(), 4, id32(35)));
    }

    @Test
    void catalogComparesManifestMutationSequenceAsUnsigned() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 29);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(290);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(291), 0, 1, Long.MAX_VALUE, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);

        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(292),
                1,
                2,
                Long.MIN_VALUE,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, 1);
        final RecoveryFloor floor = catalog.advanceFloor(child.checkpointId(), 2, id32(293));

        assertEquals(Long.MIN_VALUE, floor.includedMutationSequence());
        assertTrue(catalog.proveFloorCoverage(child.checkpointId(), Long.MAX_VALUE, child.appliedShardLogPosition())
                .isPresent());
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
        final RecoveryFloorRef floorRef = new RecoveryFloorRef(
                floor.recoveryLineageId(),
                floor.checkpointId(),
                floor.manifestSha256(),
                floor.catalogGeneration(),
                floor.appliedSourcePosition(),
                floor.includedMutationSequence(),
                List.of());
        final RecoveryCandidateRef candidate = new RecoveryCandidateRef(
                RecoveryCandidateKind.CATALOG_CHECKPOINT,
                lineage,
                genesis.checkpointId(),
                genesis.manifestSha256(),
                null);
        final RecoveryPin pin = new RecoveryPin(
                id16(63),
                new ShardSubject(shard),
                new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1, id32(64)),
                candidate,
                floorRef,
                floor.catalogGeneration(),
                id32(65));
        final EvidenceCursor unbound = EvidenceCursor.kafka(id32(67), id16(68), id16(69), 1, 1, 10, 2, 1);
        final RecoveryFloorRef mismatchedFloorRef = new RecoveryFloorRef(
                floor.recoveryLineageId(),
                floor.checkpointId(),
                floor.manifestSha256(),
                floor.catalogGeneration(),
                floor.appliedSourcePosition(),
                floor.includedMutationSequence(),
                List.of(unbound));
        assertThrows(
                IllegalStateException.class,
                () -> catalog.createRecoveryPin(new RecoveryPin(
                        id16(70),
                        new ShardSubject(shard),
                        pin.owner(),
                        candidate,
                        mismatchedFloorRef,
                        floor.catalogGeneration(),
                        id32(71))));

        assertEquals(pin, catalog.createRecoveryPin(pin));
        assertEquals(java.util.Optional.of(pin), catalog.activeRecoveryPin());
        assertThrows(
                IllegalStateException.class,
                () -> catalog.createRecoveryPin(new RecoveryPin(
                        id16(66),
                        new ShardSubject(shard),
                        pin.owner(),
                        candidate,
                        floorRef,
                        floor.catalogGeneration(),
                        id32(67))));

        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(68),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, floor.catalogGeneration());
        catalog.advanceFloor(child.checkpointId(), floor.catalogGeneration() + 1, id32(72));
        assertThrows(IllegalStateException.class, () -> catalog.validatePublishedRestoreCandidate(genesis));

        catalog.releaseRecoveryPin(pin);
        assertTrue(catalog.activeRecoveryPin().isEmpty());
        assertThrows(IllegalStateException.class, () -> catalog.createRecoveryPin(pin));
    }

    @Test
    void validatesLocalStoreRecoveryAgainstTheTypedFloorAndAncestry() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 14);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(140);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(141), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        final RecoveryFloorRef floor = catalog.advanceFloor(genesis.checkpointId(), 1, List.of());
        final byte[] storeIncarnation = id16(142);
        final StoreRecoveryMetadata local = new StoreRecoveryMetadata(
                new RecoveryCandidateRef(
                        RecoveryCandidateKind.LOCAL_STORE,
                        lineage,
                        genesis.checkpointId(),
                        genesis.manifestSha256(),
                        storeIncarnation),
                floor,
                floor.catalogGeneration(),
                new RecoveryInstallState(RecoveryInstallPhase.OPEN, storeIncarnation, genesis.checkpointId()));

        catalog.validateLocalStoreRecovery(shard, local);

        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(143),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        catalog.publish(child, floor.catalogGeneration());
        catalog.advanceFloor(child.checkpointId(), floor.catalogGeneration() + 1, List.of());
        assertThrows(IllegalStateException.class, () -> catalog.validateLocalStoreRecovery(shard, local));
    }

    @Test
    void OxiaBoundaryForwardsLocalStoreRecoveryValidation() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 15);
        final byte[] lineage = id16(150);
        final CheckpointManifest genesis = manifest(shard, UUID.randomUUID(), lineage, id16(151), 0, 1, 1, null);
        final RecoveryCatalog delegate = new RecoveryCatalog();
        delegate.publish(genesis, 0);
        final RecoveryFloorRef floor = delegate.advanceFloor(genesis.checkpointId(), 1, List.of());
        final byte[] storeIncarnation = id16(152);
        final StoreRecoveryMetadata local = new StoreRecoveryMetadata(
                new RecoveryCandidateRef(
                        RecoveryCandidateKind.LOCAL_STORE,
                        lineage,
                        genesis.checkpointId(),
                        genesis.manifestSha256(),
                        storeIncarnation),
                floor,
                floor.catalogGeneration(),
                new RecoveryInstallState(RecoveryInstallPhase.OPEN, storeIncarnation, genesis.checkpointId()));
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(delegate);

        authority.validateLocalStoreRecovery(shard, local);

        final StoreRecoveryMetadata incomplete = StoreRecoveryMetadata.empty();
        assertThrows(IllegalArgumentException.class, () -> authority.validateLocalStoreRecovery(shard, incomplete));
        final StoreRecoveryMetadata wrongShard = local;
        assertThrows(
                IllegalArgumentException.class,
                () -> authority.validateLocalStoreRecovery(new ShardId(RouteIncarnation.random(), 15), wrongShard));
    }

    @Test
    void catalogPublicationRequiresExactPublishedUploadIntentIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final byte[] lineage = id16(70);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), lineage, id16(71), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(manifest, 0);
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(72), ProfileKind.OBJECT_STORE);
        final OwnerIdentity owner = new OwnerIdentity(
                manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(),
                manifest.createdBy().ownerEpoch(),
                id32(73));
        final CheckpointUploadIntent published = new CheckpointUploadIntent(
                new ShardSubject(shard),
                lineage,
                manifest.checkpointId(),
                owner,
                uuidBytes(manifest.sourceStoreIncarnation()),
                id32(74),
                1,
                null,
                null,
                profile,
                evidence(1_000),
                5_000,
                CheckpointUploadState.PUBLISHED,
                2,
                new CheckpointResource(
                        lineage,
                        manifest.checkpointId(),
                        profile,
                        Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/71/manifest"),
                        Bytes.utf8("version-1"),
                        manifest.canonicalJsonBytes().length,
                        manifest.manifestSha256()),
                null);

        assertEquals(
                1, catalog.publishUploadedCheckpoint(published, manifest, 1).catalogGeneration());
        assertEquals(
                1,
                new OxiaRecoveryCatalog(catalog)
                        .publishUploadedCheckpoint(published, manifest, 1)
                        .catalogGeneration());
        final OxiaRecoveryCatalog.CasBackend mustNotReceiveInvalidRequest = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                throw new AssertionError("invalid upload intent reached Oxia publish");
            }

            @Override
            public RecoveryCatalog.Publication publishUploadedCheckpoint(
                    final CheckpointUploadIntent ignoredIntent,
                    final CheckpointManifest ignoredManifest,
                    final long expected) {
                throw new AssertionError("invalid upload intent reached Oxia upload publication");
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                throw new AssertionError("invalid upload intent reached Oxia floor CAS");
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] ignored) {
                throw new AssertionError("invalid upload intent reached Oxia manifest read");
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                throw new AssertionError("invalid upload intent reached Oxia floor read");
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {
                throw new AssertionError("invalid upload intent reached Oxia restore validation");
            }

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                throw new AssertionError("invalid upload intent reached Oxia coverage proof");
            }
        };
        assertThrows(IllegalStateException.class, () -> new OxiaRecoveryCatalog(mustNotReceiveInvalidRequest)
                .publishUploadedCheckpoint(published, manifest, 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.publishUploadedCheckpoint(
                        new CheckpointUploadIntent(
                                new ShardSubject(shard),
                                lineage,
                                manifest.checkpointId(),
                                owner,
                                uuidBytes(manifest.sourceStoreIncarnation()),
                                id32(75),
                                1,
                                null,
                                null,
                                profile,
                                evidence(1_000),
                                5_000,
                                CheckpointUploadState.PENDING_UPLOAD,
                                1,
                                null,
                                null),
                        manifest,
                        2));
    }

    @Test
    void uploadedCheckpointPublicationRereadsExactManifestAfterResponseLoss() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 13);
        final byte[] lineage = id16(90);
        final UUID topic = UUID.randomUUID();
        final CheckpointManifest parent = manifest(shard, topic, lineage, id16(89), 0, 0, 0, null);
        final CheckpointManifest manifest = manifest(
                shard,
                topic,
                lineage,
                id16(91),
                1,
                1,
                1,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())));
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(92), ProfileKind.OBJECT_STORE);
        final OwnerIdentity owner = new OwnerIdentity(
                manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(),
                manifest.createdBy().ownerEpoch(),
                id32(93));
        final CheckpointUploadIntent published = new CheckpointUploadIntent(
                new ShardSubject(shard),
                lineage,
                manifest.checkpointId(),
                owner,
                uuidBytes(manifest.sourceStoreIncarnation()),
                id32(94),
                1,
                parent.checkpointId(),
                parent.manifestSha256(),
                profile,
                evidence(2_000),
                6_000,
                CheckpointUploadState.PUBLISHED,
                2,
                new CheckpointResource(
                        lineage,
                        manifest.checkpointId(),
                        profile,
                        Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/91/manifest"),
                        Bytes.utf8("version-1"),
                        manifest.canonicalJsonBytes().length,
                        manifest.manifestSha256()),
                null);

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
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), lineage, id16(101), 0, 1, 1, null);
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("checkpoint-store"), 1, id32(102), ProfileKind.OBJECT_STORE);
        final OwnerIdentity owner = new OwnerIdentity(
                manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(),
                manifest.createdBy().ownerEpoch(),
                id32(103));
        final CheckpointResource resource = new CheckpointResource(
                lineage,
                manifest.checkpointId(),
                profile,
                Bytes.utf8("bucket"),
                Bytes.utf8("checkpoint/101/manifest"),
                Bytes.utf8("version-1"),
                manifest.canonicalJsonBytes().length,
                manifest.manifestSha256());
        final CheckpointUploadIntent published = new CheckpointUploadIntent(
                new ShardSubject(shard),
                lineage,
                manifest.checkpointId(),
                owner,
                uuidBytes(manifest.sourceStoreIncarnation()),
                id32(104),
                1,
                null,
                null,
                profile,
                evidence(3_000),
                7_000,
                CheckpointUploadState.PUBLISHED,
                2,
                resource,
                null);
        final CheckpointUploadIntent conflicting = new CheckpointUploadIntent(
                new ShardSubject(shard),
                lineage,
                manifest.checkpointId(),
                owner,
                uuidBytes(manifest.sourceStoreIncarnation()),
                id32(105),
                1,
                null,
                null,
                profile,
                evidence(3_000),
                7_000,
                CheckpointUploadState.PUBLISHED,
                2,
                new CheckpointResource(
                        lineage,
                        manifest.checkpointId(),
                        profile,
                        Bytes.utf8("bucket"),
                        Bytes.utf8("checkpoint/101/manifest"),
                        Bytes.utf8("version-2"),
                        manifest.canonicalJsonBytes().length,
                        manifest.manifestSha256()),
                null);

        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(manifest, 0);
        catalog.publishUploadedCheckpoint(published, manifest, 1);
        assertThrows(IllegalStateException.class, () -> catalog.publishUploadedCheckpoint(conflicting, manifest, 1));
    }

    @Test
    void typedFloorRequiresSameGenerationCursorDominance() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 12);
        final byte[] lineage = id16(80);
        final EvidenceCursor older = EvidenceCursor.kafka(id32(82), id16(83), id16(84), 1, 4, 100, 11, 10);
        final CheckpointManifest genesis =
                manifest(shard, UUID.randomUUID(), lineage, id16(81), 0, 1, 1, null, List.of(older));
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(genesis, 0);
        assertThrows(IllegalArgumentException.class, () -> catalog.advanceFloor(genesis.checkpointId(), 1, List.of()));
        final EvidenceCursor regressedChildCursor = EvidenceCursor.kafka(id32(82), id16(83), id16(84), 1, 4, 99, 10, 9);
        final CheckpointManifest regressedChild = manifest(
                shard,
                ((KafkaSourcePosition) genesis.appliedShardLogPosition()).nativeTopicUuid(),
                lineage,
                id16(86),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())),
                List.of(regressedChildCursor));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(regressedChild, 1));
        final RecoveryFloorRef first = catalog.advanceFloor(genesis.checkpointId(), 1, List.of(older));
        assertEquals(first, catalog.currentFloorRef().orElseThrow());
        assertEquals(first, catalog.advanceFloor(genesis.checkpointId(), 1, List.of(older)));

        final EvidenceCursor newer = EvidenceCursor.kafka(id32(82), id16(83), id16(84), 1, 4, 101, 12, 11);
        final CheckpointManifest child = manifest(
                shard,
                ((KafkaSourcePosition) genesis.appliedShardLogPosition()).nativeTopicUuid(),
                lineage,
                id16(85),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())),
                List.of(newer));
        catalog.publish(child, 2);
        final RecoveryFloorRef second = catalog.advanceFloor(child.checkpointId(), 3, List.of(newer));
        assertEquals(second, catalog.currentFloorRef().orElseThrow());

        final EvidenceCursor regressed = EvidenceCursor.kafka(id32(82), id16(83), id16(84), 1, 4, 102, 10, 11);
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.advanceFloor(child.checkpointId(), 4, List.of(regressed)));
        assertThrows(IllegalStateException.class, () -> catalog.advanceFloor(child.checkpointId(), 4, id32(86)));
    }

    @Test
    void rejectsParentHashLineageAndSourceIdentityViolations() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final byte[] lineage = id16(10);
        final CheckpointManifest parent = manifest(shard, UUID.randomUUID(), lineage, id16(11), 0, 1, 1, null);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        catalog.publish(parent, 0);
        final CheckpointManifest wrongHash = manifest(
                shard,
                parent.appliedShardLogPosition() instanceof KafkaSourcePosition p
                        ? p.nativeTopicUuid()
                        : UUID.randomUUID(),
                lineage,
                id16(12),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(id32(13))));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(wrongHash, 1));

        final CheckpointManifest wrongSource = manifest(
                shard,
                UUID.randomUUID(),
                lineage,
                id16(14),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(parent.checkpointId(), Bytes.hex(parent.manifestSha256())));
        assertThrows(IllegalArgumentException.class, () -> catalog.publish(wrongSource, 1));
    }

    @Test
    void OxiaBoundaryDelegatesCasAndRejectsIdentityDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(50), id16(51), 0, 1, 1, null);
        final CheckpointManifest wrongPublication =
                manifest(shard, UUID.randomUUID(), id16(52), id16(53), 0, 1, 1, null);
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(new RecoveryCatalog());
        assertEquals(1, authority.publish(manifest, 0).catalogGeneration());
        assertEquals(
                manifest.canonicalJson(),
                authority.manifest(manifest.checkpointId()).orElseThrow().canonicalJson());
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
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return java.util.Optional.empty();
            }
        };
        assertThrows(IllegalStateException.class, () -> new OxiaRecoveryCatalog(malformed).publish(manifest, 0));
    }

    @Test
    void OxiaBoundaryComparesCatalogGenerationsAsUnsignedValues() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 28);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(280), id16(281), 0, 1, 1, null);
        final byte[] evidenceDigest = id32(282);
        final RecoveryFloor highBitFloor = RecoveryFloor.create(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                Long.MIN_VALUE + 1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                evidenceDigest);
        final OxiaRecoveryCatalog.CasBackend backend = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                return new RecoveryCatalog.Publication(manifest, Long.MIN_VALUE, null);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                return highBitFloor;
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] ignored) {
                return Optional.of(manifest);
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                return Optional.of(highBitFloor);
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return Optional.empty();
            }
        };

        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(backend);
        assertEquals(Long.MIN_VALUE, authority.publish(manifest, Long.MAX_VALUE).catalogGeneration());
        assertEquals(highBitFloor, authority.advanceFloor(manifest.checkpointId(), Long.MIN_VALUE, evidenceDigest));
        assertEquals(highBitFloor, authority.currentFloor().orElseThrow());
    }

    @Test
    void OxiaBoundaryRejectsRestoreCandidateDriftBeforeBackendValidation() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 27);
        final CheckpointManifest published = manifest(shard, UUID.randomUUID(), id16(270), id16(271), 0, 1, 1, null);
        final CheckpointManifest drifted =
                manifest(shard, UUID.randomUUID(), id16(270), published.checkpointId(), 0, 1, 1, null);
        final RecoveryCatalog delegate = new RecoveryCatalog();
        delegate.publish(published, 0);
        final OxiaRecoveryCatalog.CasBackend backend = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                return delegate.manifest(checkpointId);
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                return Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {
                throw new AssertionError("drifted restore candidate reached backend");
            }

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return Optional.empty();
            }
        };
        assertThrows(IllegalStateException.class, () -> new OxiaRecoveryCatalog(backend)
                .validatePublishedRestoreCandidate(drifted));
    }

    @Test
    void OxiaBoundaryRejectsPublicationFloorDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 24);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(240), id16(241), 0, 1, 1, null);
        final RecoveryCatalog delegate = new RecoveryCatalog();
        delegate.publish(manifest, 0);
        final RecoveryFloor wrongLineage = RecoveryFloor.create(
                id16(242),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                id32(243));
        final RecoveryFloor newerThanPublication = RecoveryFloor.create(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                2,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                id32(244));
        final RecoveryFloor[] returned = {wrongLineage};
        final OxiaRecoveryCatalog.CasBackend backend = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                return new RecoveryCatalog.Publication(manifest, expected + 1, returned[0]);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                return null;
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                return delegate.manifest(checkpointId);
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return java.util.Optional.empty();
            }
        };
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(backend);
        assertThrows(IllegalStateException.class, () -> authority.publish(manifest, 0));

        returned[0] = newerThanPublication;
        assertThrows(IllegalStateException.class, () -> authority.publish(manifest, 0));
    }

    @Test
    void OxiaBoundaryBindsReadFloorAndCoverageResponsesToPublishedManifests() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(73), id16(74), 0, 1, 1, null);
        final RecoveryFloor validFloor = RecoveryFloor.create(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                id32(75));
        final RecoveryFloor wrongFloor = RecoveryFloor.create(
                id16(76),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                id32(77));
        final RecoveryFloorRef validTypedFloor = new RecoveryFloorRef(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                List.of());
        final RecoveryFloorRef wrongTypedFloor = new RecoveryFloorRef(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                id32(78),
                1,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                List.of());
        final CheckpointManifest wrongCandidate =
                manifest(shard, UUID.randomUUID(), id16(79), manifest.checkpointId(), 0, 1, 1, null);

        final class Backend implements OxiaRecoveryCatalog.CasBackend {
            private RecoveryFloor scalarFloor = wrongFloor;
            private RecoveryFloorRef typedFloor = wrongTypedFloor;
            private java.util.Optional<RecoveryCatalog.FloorCoverage> coverage = java.util.Optional.of(
                    new RecoveryCatalog.FloorCoverage(validFloor, wrongCandidate, List.of(wrongCandidate)));

            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                return new RecoveryCatalog.Publication(manifest, expected + 1, validFloor);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                return validFloor;
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] ignored) {
                return java.util.Optional.of(manifest);
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.of(scalarFloor);
            }

            @Override
            public java.util.Optional<RecoveryFloorRef> currentFloorRef() {
                return java.util.Optional.of(typedFloor);
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return coverage;
            }
        }

        final Backend backend = new Backend();
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(backend);
        assertThrows(IllegalStateException.class, authority::currentFloor);
        backend.scalarFloor = validFloor;
        assertEquals(validFloor, authority.currentFloor().orElseThrow());
        assertThrows(IllegalStateException.class, authority::currentFloorRef);
        backend.typedFloor = validTypedFloor;
        assertEquals(validTypedFloor, authority.currentFloorRef().orElseThrow());
        assertThrows(
                IllegalStateException.class,
                () -> authority.proveFloorCoverage(manifest.checkpointId(), 0, manifest.appliedShardLogPosition()));
        backend.coverage =
                java.util.Optional.of(new RecoveryCatalog.FloorCoverage(validFloor, manifest, List.of(manifest)));
        final KafkaSourcePosition actualPosition = (KafkaSourcePosition) manifest.appliedShardLogPosition();
        final KafkaSourcePosition conflictingPosition = new KafkaSourcePosition(
                shard,
                actualPosition.authenticatedClusterId(),
                actualPosition.nativeTopicUuid(),
                actualPosition.offset(),
                7,
                actualPosition.brokerLogAppendTimeEpochMs() + 1);
        assertThrows(
                IllegalStateException.class,
                () -> authority.proveFloorCoverage(manifest.checkpointId(), 0, conflictingPosition));
        assertTrue(authority
                .proveFloorCoverage(manifest.checkpointId(), 0, manifest.appliedShardLogPosition())
                .isPresent());
    }

    @Test
    void OxiaBoundaryRejectsBrokenFloorCoverageParentChain() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 25);
        final UUID topic = UUID.randomUUID();
        final byte[] lineage = id16(250);
        final CheckpointManifest genesis = manifest(shard, topic, lineage, id16(251), 0, 1, 1, null);
        final CheckpointManifest child = manifest(
                shard,
                topic,
                lineage,
                id16(252),
                1,
                2,
                2,
                new CheckpointManifest.ParentCheckpoint(genesis.checkpointId(), Bytes.hex(genesis.manifestSha256())));
        final CheckpointManifest candidate = manifest(
                shard,
                topic,
                lineage,
                id16(253),
                2,
                3,
                3,
                new CheckpointManifest.ParentCheckpoint(child.checkpointId(), Bytes.hex(child.manifestSha256())));
        final RecoveryCatalog delegate = new RecoveryCatalog();
        delegate.publish(genesis, 0);
        delegate.publish(child, 1);
        delegate.publish(candidate, 2);
        final RecoveryFloor floor = delegate.advanceFloor(genesis.checkpointId(), 3, id32(254));

        final class Backend implements OxiaRecoveryCatalog.CasBackend {
            private java.util.Optional<RecoveryCatalog.FloorCoverage> coverage = java.util.Optional.of(
                    new RecoveryCatalog.FloorCoverage(floor, candidate, List.of(genesis, candidate)));

            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
                return delegate.manifest(checkpointId);
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return coverage;
            }
        }

        final Backend backend = new Backend();
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(backend);
        assertThrows(
                IllegalStateException.class,
                () -> authority.proveFloorCoverage(candidate.checkpointId(), 1, genesis.appliedShardLogPosition()));
        backend.coverage = java.util.Optional.of(
                new RecoveryCatalog.FloorCoverage(floor, candidate, List.of(genesis, child, candidate)));
        assertTrue(authority
                .proveFloorCoverage(candidate.checkpointId(), 1, genesis.appliedShardLogPosition())
                .isPresent());
    }

    @Test
    void OxiaBoundaryRejectsTypedFloorBoundaryAndCursorDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final EvidenceCursor cursor = EvidenceCursor.kafka(id32(56), id16(57), id16(58), 1, 1, 1_001, 2, 1);
        final CheckpointManifest manifest =
                manifest(shard, UUID.randomUUID(), id16(53), id16(54), 0, 1, 1, null, List.of(cursor));
        final RecoveryFloorRef wrongManifest = new RecoveryFloorRef(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                id32(59),
                2,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                List.of(cursor));
        final RecoveryFloorRef wrongCursors = new RecoveryFloorRef(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                manifest.manifestSha256(),
                2,
                manifest.appliedShardLogPosition(),
                manifest.shardMutationSequence(),
                List.of());

        final class Backend implements OxiaRecoveryCatalog.CasBackend {
            private RecoveryFloorRef typedResult = wrongManifest;

            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                return new RecoveryCatalog.Publication(manifest, expected + 1, null);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] ignored, final long expected, final byte[] digest) {
                return RecoveryFloor.create(
                        manifest.recoveryLineageId(),
                        manifest.checkpointId(),
                        manifest.manifestSha256(),
                        expected + 1,
                        manifest.appliedShardLogPosition(),
                        manifest.shardMutationSequence(),
                        digest);
            }

            @Override
            public RecoveryFloorRef advanceFloor(
                    final byte[] ignored, final long expected, final List<EvidenceCursor> ignoredCursors) {
                return typedResult;
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(final byte[] ignored) {
                return java.util.Optional.of(manifest);
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return java.util.Optional.empty();
            }
        }

        final Backend backend = new Backend();
        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(backend);
        assertThrows(
                IllegalStateException.class, () -> authority.advanceFloor(manifest.checkpointId(), 1, List.of(cursor)));
        backend.typedResult = wrongCursors;
        assertThrows(
                IllegalStateException.class, () -> authority.advanceFloor(manifest.checkpointId(), 1, List.of(cursor)));
    }

    @Test
    void OxiaBoundaryKeepsValidationSnapshotWhenBackendMutatesRequestBuffers() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 26);
        final CheckpointManifest manifest = manifest(shard, UUID.randomUUID(), id16(260), id16(261), 0, 1, 1, null);
        final RecoveryCatalog delegate = new RecoveryCatalog();
        delegate.publish(manifest, 0);
        final byte[] checkpointId = manifest.checkpointId();
        final byte[] evidenceDigest = id32(262);
        final RecoveryFloor validFloor = delegate.advanceFloor(checkpointId, 1, evidenceDigest);

        final OxiaRecoveryCatalog.CasBackend mutatingBackend = new OxiaRecoveryCatalog.CasBackend() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest ignored, final long expected) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RecoveryFloor advanceFloor(
                    final byte[] requestedCheckpointId, final long expected, final byte[] requestedEvidenceDigest) {
                requestedCheckpointId[0] ^= 0x55;
                requestedEvidenceDigest[0] ^= 0x33;
                return validFloor;
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] requestedCheckpointId) {
                requestedCheckpointId[0] ^= 0x11;
                return Optional.of(manifest);
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                return Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest ignored) {}

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] ignored,
                    final long sequence,
                    final com.nereusstream.delay.protocol.SourcePosition... positions) {
                return Optional.empty();
            }
        };

        final OxiaRecoveryCatalog authority = new OxiaRecoveryCatalog(mutatingBackend);
        assertEquals(validFloor, authority.advanceFloor(checkpointId, 1, evidenceDigest));
        assertArrayEquals(manifest.checkpointId(), checkpointId);
        assertArrayEquals(id32(262), evidenceDigest);
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
        return manifest(
                shard, topic, lineage, checkpointId, lineageGeneration, offset, mutationSequence, parent, List.of());
    }

    private static CheckpointManifest manifest(
            final ShardId shard,
            final UUID topic,
            final byte[] lineage,
            final byte[] checkpointId,
            final long lineageGeneration,
            final long offset,
            final long mutationSequence,
            final CheckpointManifest.ParentCheckpoint parent,
            final List<EvidenceCursor> evidenceCursors) {
        final KafkaSourcePosition position =
                new KafkaSourcePosition(shard, "cluster", topic, offset, null, 1_000 + offset);
        final CheckpointManifest.FileEntry file = new CheckpointManifest.FileEntry(
                "CURRENT", 1, id32(20), Bytes.utf8("object/current"), Bytes.utf8("version"), null);
        return new CheckpointManifest(
                checkpointId,
                lineage,
                lineageGeneration,
                parent,
                null,
                new CheckpointManifest.CreatedBy(id32(21), id32(22), 1),
                new CheckpointManifest.CreatedAt(
                        1_000, 1_001, "CERTIFIED_HOST_CLOCK", id32(23), 1, offset, offset, id32(24), 0, null),
                shard,
                id32(25),
                UUID.randomUUID(),
                1,
                mutationSequence,
                position,
                id32(26),
                id32(27),
                evidenceCursors,
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
                id32(76),
                1,
                2,
                3,
                id32(77),
                0,
                null);
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }
}
