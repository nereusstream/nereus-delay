package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RecoveryCandidateKindV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryInstallPhaseV1;
import io.nereusstream.delay.protocol.RecoveryInstallStateV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SloFinalOutcomeV1;
import io.nereusstream.delay.protocol.SloObjectiveNameV1;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloPathV1;
import io.nereusstream.delay.protocol.SloPopulationV1;
import io.nereusstream.delay.protocol.SloSampleEventIdentityV1;
import io.nereusstream.delay.protocol.SloSampleFinalV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;
import io.nereusstream.delay.protocol.SloThresholdUnitV1;
import io.nereusstream.delay.protocol.SloTimeEndpointKindV1;
import io.nereusstream.delay.protocol.SloTimeEndpointV1;
import io.nereusstream.delay.protocol.SourcePosition;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void physicalCheckpointPrimitivesAreNotPublicProductionApi() {
        for (var method : ShardStore.class.getDeclaredMethods()) {
            if (method.getName().equals("createCheckpoint")
                    || method.getName().equals("restoreFromCheckpoint")) {
                assertFalse(Modifier.isPublic(method.getModifiers()), method::toGenericString);
            }
        }
    }

    @Test
    void oneShardUsesIndependentDbAndAtomicBatchSurvivesReopen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final byte[] key = scratchMetaQuotaKey(16);
        final byte[] payload = Bytes.utf8("source-position");
        final Path checkpoint;
        final byte[] dbIdentity;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key, payload));
            assertArrayEquals(payload, store.getValue(ColumnFamily.META, key, 3).payload());
            checkpoint = tempDir.resolve("checkpoint");
            store.createCheckpoint(checkpoint);
            assertNotNull(store.latestSequenceNumber());
        }
        assertTrueFile(checkpoint.resolve("CURRENT"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertArrayEquals(dbIdentity, reopened.metadata().dbIdentity());
            assertArrayEquals(payload, reopened.getValue(ColumnFamily.META, key, 3).payload());
            final List<Path> dbs;
            try (var stream = Files.walk(tempDir.resolve("shards"))) {
                dbs = stream.filter(path -> path.getFileName().toString().equals("CURRENT")).toList();
            }
            assertEquals(1, dbs.size());
        }
    }

    @Test
    void localRecoveryReuseOpensOnlyCatalogValidatedActiveStore() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("local-reuse"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 43);
        final byte[] lineage = bytes(44);
        final byte[] checkpointId = bytes(45);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        final RecoveryFloorRefV1 observedFloor;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                    shardId, "cluster", UUID.randomUUID(), 0, null, 1_000);
            final CheckpointManifest manifest = new CheckpointManifest(checkpointId, lineage, 0, null, null,
                    new CheckpointManifest.CreatedBy(bytes(46), bytes(47), 1),
                    new CheckpointManifest.CreatedAt(1_000, 1_001, "CERTIFIED_HOST_CLOCK", bytes(48), 1,
                            0, 0, bytes(32, 49), 0, null), shardId, store.metadata().dbIdentity(),
                    store.metadata().storeIncarnationUuid(), 1, 0, appliedPosition, bytes(32, 50),
                    bytes(32, 51), List.of(new CheckpointManifest.FileEntry("CURRENT", 1, bytes(32, 52),
                            Bytes.utf8("object/current"), Bytes.utf8("version"), null)));
            catalog.publish(manifest, 0);
            observedFloor = catalog.advanceFloor(checkpointId, 1, List.of());
            store.recordRecoveryMetadata(new RecoveryCandidateRefV1(RecoveryCandidateKindV1.LOCAL_STORE,
                    lineage, checkpointId, manifest.manifestSha256(), store.metadata().storeIncarnation()),
                    observedFloor);
        }

        // The catalog must see the persisted recovery projection, not an
        // OPEN marker written speculatively by the native open path.  A
        // previously cleanly closed store therefore reaches validation as
        // CLOSED_CLEAN and is only published OPEN after this callback returns.
        final RecoveryCatalogAuthority proofBeforeOpen = new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest candidate,
                                                       final long expectedCatalogGeneration) {
                return catalog.publish(candidate, expectedCatalogGeneration);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] candidateCheckpointId,
                                               final long expectedCatalogGeneration,
                                               final byte[] evidenceCursorDigest) {
                return catalog.advanceFloor(candidateCheckpointId, expectedCatalogGeneration, evidenceCursorDigest);
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] candidateCheckpointId) {
                return catalog.manifest(candidateCheckpointId);
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                return catalog.currentFloor();
            }

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                                final long requiredMutationSequence,
                                                                                final SourcePosition... requiredPositions) {
                return catalog.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions);
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
                catalog.validatePublishedRestoreCandidate(candidate);
            }

            @Override
            public void validateLocalStoreRecovery(final ShardId candidateShard,
                                                   final StoreRecoveryMetadata localMetadata) {
                assertNotNull(localMetadata.installState());
                assertEquals(RecoveryInstallPhaseV1.CLOSED_CLEAN, localMetadata.installState().phase());
                catalog.validateLocalStoreRecovery(candidateShard, localMetadata);
            }
        };

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reused = ShardStore.openForLocalRecoveryReuse(config, shardId, resources,
                     proofBeforeOpen)) {
            assertEquals(observedFloor, reused.recoveryMetadata().lastObservedFloor());
            assertEquals(RecoveryInstallPhaseV1.OPEN, reused.recoveryMetadata().installState().phase());
        }

        final RecoveryCatalog unavailable = new RecoveryCatalog();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalArgumentException.class,
                    () -> ShardStore.openForLocalRecoveryReuse(config, shardId, resources, unavailable));
            assertEquals(0, resources.registeredPhysicalUsageSources());
        }
        // Rejection must close the native DB and release its Worker slots so a
        // later restore/open attempt can retry the same ACTIVE incarnation.
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(shardId, reopened.shardId());
        }
    }

    @Test
    void localRecoveryReuseDoesNotCreateAFreshDbWithoutActiveIncarnation() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("local-reuse-empty"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 44);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalStateException.class,
                    () -> ShardStore.openForLocalRecoveryReuse(config, shardId, resources,
                            new RecoveryCatalog()));
        }
        final Path shardRoot = config.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toUnsignedString(shardId.partition()));
        assertTrue(Files.isDirectory(shardRoot));
        try (var paths = Files.walk(shardRoot)) {
            assertTrue(paths.noneMatch(path -> path.getFileName().toString().equals("CURRENT")));
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("cannot inspect empty recovery root", failure);
        }
    }

    @Test
    void closedShardStoreFailsClosedForAllRocksDbOperations() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("closed-lifecycle"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final ShardStore store;
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        try {
            store = ShardStore.open(config, shardId, resources);
            store.close();

            assertThrows(IllegalStateException.class,
                    () -> store.get(ColumnFamily.META, KeyCodec.metaFixed(1)));
            assertThrows(IllegalStateException.class,
                    () -> store.getValue(ColumnFamily.META, KeyCodec.metaFixed(1), 1));
            assertThrows(IllegalStateException.class,
                    () -> store.scan(ColumnFamily.META, null, null, 1));
            assertThrows(IllegalStateException.class, () -> store.write(batch -> {
                // The callback must never run after the store has closed.
                throw new AssertionError("closed store accepted a write callback");
            }));
            assertThrows(IllegalStateException.class, store::flushAndSync);
            assertThrows(IllegalStateException.class, store::latestSequenceNumber);
        } finally {
            resources.close();
        }
    }

    @Test
    void nativeWriteFailureHasATypeDistinctFromSemanticStaleness() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("write-failure-type"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final ShardStore.RocksDbWriteFailure failure = assertThrows(ShardStore.RocksDbWriteFailure.class,
                    () -> store.write(batch -> {
                        throw new RocksDBException("synthetic native write failure");
                    }));
            assertEquals("RocksDB write failed", failure.getMessage());
            assertTrue(failure.getCause() instanceof RocksDBException);
        }
    }

    @Test
    void postWriteVerificationFailureFencesStoreUntilReopen() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("post-write-verification"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shardId, resources);
        try {
            final ShardStore.RocksDbWriteFailure failure = assertThrows(ShardStore.RocksDbWriteFailure.class,
                    () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(4),
                            Bytes.utf8("malformed-ingress-fence"))));
            assertEquals("cannot verify ingress fence state after write", failure.getMessage());
            assertTrue(failure.getCause() instanceof RuntimeException);
            assertTrue(store.isWriteOutcomeUncertain());
            assertThrows(IllegalStateException.class,
                    () -> store.get(ColumnFamily.META, KeyCodec.metaFixed(1)));
            assertThrows(IllegalStateException.class,
                    () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(15),
                            Bytes.utf8("must-not-follow-uncertain-write"))));
            assertDoesNotThrow(store::close);
        } finally {
            resources.close();
        }
    }

    @Test
    void fixedFormatAndIdentityValuesUseRegisteredValueEnvelope() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fixed-envelope"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 27);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final byte[] format = store.get(ColumnFamily.META, KeyCodec.metaFixed(1));
            final byte[] identity = store.get(ColumnFamily.META, KeyCodec.metaFixed(2));
            assertEquals(1, ValueEnvelope.decode(format, 1).valueType());
            assertArrayEquals(Bytes.u32be(1), ValueEnvelope.decode(format, 1).payload());
            assertEquals(1, ValueEnvelope.decode(identity, 1).valueType());
            assertArrayEquals(store.metadata().encode(), ValueEnvelope.decode(identity, 1).payload());
        }
    }

    @Test
    void reopenRejectsExistingDbMissingShardIdentity() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("missing-identity"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.delete(ColumnFamily.META, KeyCodec.metaFixed(2)));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.open(config, shardId, resources));
        }
    }

    @Test
    void reopenRejectsStoreIncarnationMetadataNotMatchingPath() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("incarnation-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 29);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final StoreMetadata original = store.metadata();
            final byte[] otherIncarnation = java.util.Arrays.copyOf(
                    Bytes.sha256(Bytes.utf8("other-store-incarnation")), 16);
            store.write(batch -> batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(2),
                    new StoreMetadata(1, shardId, otherIncarnation, original.dbIdentity()).encode()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.open(config, shardId, resources));
        }
    }

    @Test
    void malformedExistingMetadataDoesNotLeaveRocksDbOpen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-metadata"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 30);
        final Path dbPath;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
            store.write(batch -> batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(2),
                    Bytes.utf8("malformed")));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void malformedRuntimeMetadataDoesNotLeaveRocksDbOpen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-runtime-metadata"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 33);
        final Path dbPath;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
        }
        overwriteRawColumnFamilyValue(dbPath, "meta_cf", KeyCodec.metaFixed(6),
                ValueEnvelope.encode(1, Bytes.utf8("not-canonical-evidence-cursors")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void foreignRecoveryFloorDoesNotLeaveRocksDbOpen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("foreign-recovery-floor"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 36);
        final Path dbPath;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
        }
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 36);
        final RecoveryFloorRefV1 foreignFloor = new RecoveryFloorRefV1(bytes(37), bytes(38), bytes(32, 39), 1,
                new KafkaSourcePosition(foreignShard, "cluster", UUID.randomUUID(), 1, null, 1), 1, List.of());
        overwriteRawColumnFamilyValue(dbPath, "meta_cf", KeyCodec.metaRecovery(2),
                ValueEnvelope.encode(1, foreignFloor.canonicalBytes()));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void recoveryInstallStateDriftDoesNotLeaveRocksDbOpen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("recovery-install-drift"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 38);
        final Path dbPath;
        final byte[] checkpointId = bytes(57);
        final byte[] storeIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
            storeIncarnation = store.metadata().storeIncarnation();
            store.recordRecoveryMetadata(new RecoveryCandidateRefV1(RecoveryCandidateKindV1.LOCAL_STORE,
                    bytes(58), checkpointId, bytes(32, 59), storeIncarnation), null);
        }
        overwriteRawColumnFamilyValue(dbPath, "meta_cf", KeyCodec.metaRecovery(4), ValueEnvelope.encode(1,
                new RecoveryInstallStateV1(RecoveryInstallPhaseV1.CLOSED_CLEAN,
                        storeIncarnation, bytes(60)).canonicalBytes()));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void danglingRecoveryCatalogGenerationDoesNotLeaveRocksDbOpen() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("dangling-recovery-generation"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 40);
        final Path dbPath;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
        }
        overwriteRawColumnFamilyValue(dbPath, "meta_cf", KeyCodec.metaRecovery(3),
                ValueEnvelope.encode(1, Bytes.u64beBits(1)));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void fixedControlMetadataIsValidatedBeforeShardActivation() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fixed-control-metadata"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final Path dbPath;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
        }
        overwriteRawColumnFamilyValue(dbPath, "meta_cf", KeyCodec.metaFixed(12),
                ValueEnvelope.encode(1, Bytes.utf8("wrong-control-type")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void compatibleControlSnapshotIsPersistedAndRevalidatedForItsShard() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("control-snapshot"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 39);
        final CompatibleControlSnapshotV1 snapshot = new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 61), 1, bytes(32, 62), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 63), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
        final Path dbPath;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            dbPath = store.dbPath();
            assertNull(store.controlSnapshot());
            store.recordControlSnapshot(snapshot);
            assertEquals(snapshot, store.controlSnapshot());
            assertArrayEquals(snapshot.canonicalBytes(), store.getValue(ColumnFamily.META,
                    KeyCodec.metaFixed(10), 1).payload());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(snapshot, reopened.controlSnapshot());
        }
        final ShardId foreignShard = new ShardId(RouteIncarnation.random(), 40);
        final CompatibleControlSnapshotV1 foreign = new CompatibleControlSnapshotV1(
                new ShardSubjectV1(foreignShard), snapshot.protocolTuples(), snapshot.profiles(),
                snapshot.initialQuotaGrant());
        overwriteRawColumnFamilyValue(dbPath, "meta_cf", KeyCodec.metaFixed(10),
                ValueEnvelope.encode(1, foreign.canonicalBytes()));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.open(config, shardId, resources));
        }
        assertRawRocksDbCanBeOpened(dbPath);
    }

    @Test
    void checkpointUsesTemporaryNamespaceAndRejectsExistingTarget() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-atomic"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 23);
        final Path checkpoint = tempDir.resolve("checkpoint-atomic-output");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            assertEquals(checkpoint, store.createCheckpoint(checkpoint));
            assertTrueFile(checkpoint.resolve("CURRENT"));
            final Path stagingRoot = checkpoint.getParent().resolve("checkpoint-tmp");
            try (var paths = Files.list(stagingRoot)) {
                assertEquals(List.of(), paths.toList());
            }
            assertThrows(IllegalStateException.class, () -> store.createCheckpoint(checkpoint));
            assertTrueFile(checkpoint.resolve("CURRENT"));
        }
    }

    @Test
    void checkpointIdentityIsCopiedWithTheDbAndFailedAttemptRollsBackProjection() {
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-identity"));
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-identity-restored"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 34);
        final byte[] checkpointId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("checkpoint-id")), 16);
        final Path checkpoint = tempDir.resolve("checkpoint-with-identity");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.createCheckpoint(checkpoint, checkpointId);
            assertArrayEquals(checkpointId, source.runtimeMetadata().lastCheckpointId());
            assertThrows(IllegalStateException.class, () -> source.createCheckpoint(checkpoint, checkpointId));
            assertArrayEquals(checkpointId, source.runtimeMetadata().lastCheckpointId());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint)) {
            assertArrayEquals(checkpointId, restored.runtimeMetadata().lastCheckpointId());
        }
    }

    @Test
    void convenienceCheckpointAllocatesIdentityBeforeSnapshot() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 36);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-auto-id"));
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-auto-id-restored"));
        final Path checkpoint = tempDir.resolve("checkpoint-auto-id-output");
        final byte[] checkpointId;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            assertEquals(checkpoint, source.createCheckpoint(checkpoint));
            checkpointId = source.runtimeMetadata().lastCheckpointId();
            assertNotNull(checkpointId);
            assertEquals(16, checkpointId.length);
            assertFalse(java.util.Arrays.equals(new byte[16], checkpointId));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint)) {
            assertArrayEquals(checkpointId, restored.runtimeMetadata().lastCheckpointId());
        }
    }

    @Test
    void checkpointCreateSlotRejectionDoesNotMutateCheckpointProjection() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-slot-before-projection"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final byte[] checkpointId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("slot-before-projection")), 16);
        final Path checkpoint = tempDir.resolve("checkpoint-slot-before-projection-output");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            resources.acquireCheckpointCreateSlot();
            try {
                assertThrows(IllegalStateException.class, () -> store.createCheckpoint(checkpoint, checkpointId));
                assertNull(store.runtimeMetadata().lastCheckpointId());
                assertNull(store.getValue(ColumnFamily.META, KeyCodec.metaFixed(7), 1));
            } finally {
                resources.releaseCheckpointCreateSlot();
            }
            store.createCheckpoint(checkpoint, checkpointId);
            assertArrayEquals(checkpointId, store.runtimeMetadata().lastCheckpointId());
        }
    }

    @Test
    void flushAndSyncMakesTheShardBoundaryExplicit() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("flush-sync"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 32);
        final byte[] key = scratchMetaQuotaKey(15);
        final byte[] payload = Bytes.utf8("flush-sync");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key, payload));
            store.flushAndSync();
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertArrayEquals(payload, reopened.getValue(ColumnFamily.META, key, 3).payload());
        }
    }

    @Test
    void flushAndSyncFailureFencesStoreUntilReopen() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("flush-sync-failure"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 37);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final ShardStore.FlushSyncOperation failure = () -> {
                throw new RocksDBException("synthetic flush failure");
            };
            final IllegalStateException exception = assertThrows(IllegalStateException.class,
                    () -> store.flushAndSync(failure));
            assertEquals("RocksDB flush/sync failed", exception.getMessage());
            assertTrue(store.isWriteOutcomeUncertain());
            assertThrows(IllegalStateException.class,
                    () -> store.get(ColumnFamily.META, KeyCodec.metaFixed(1)));
        }
    }

    @Test
    void openRejectsSymbolicShardPathAncestors() throws Exception {
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final ShardId shardId = new ShardId(routeIncarnation, 24);
        for (String component : List.of("shards", "route", "partition")) {
            final Path root = tempDir.resolve("symbolic-path-" + component);
            final Path outside = tempDir.resolve("symbolic-path-target-" + component);
            Files.createDirectories(root);
            Files.createDirectories(outside);
            final Path shards = root.resolve("shards");
            final Path route = shards.resolve(routeIncarnation.uuid().toString());
            final Path partition = route.resolve(Integer.toString(shardId.partition()));
            final Path link;
            if (component.equals("shards")) {
                link = shards;
            } else if (component.equals("route")) {
                Files.createDirectories(shards);
                link = route;
            } else {
                Files.createDirectories(route);
                link = partition;
            }
            try {
                Files.createSymbolicLink(link, outside);
            } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
                return;
            }

            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
                final IllegalStateException failure = assertThrows(IllegalStateException.class,
                        () -> ShardStore.open(config, shardId, resources));
                assertTrue(failure.getCause() instanceof java.io.IOException);
            }
            try (var paths = Files.walk(outside)) {
                assertTrue(paths.noneMatch(path -> path.getFileName().toString().equals("CURRENT")));
            }
        }
    }

    @Test
    void openRejectsSymbolicActivePointer() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("symbolic-active"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 24);
        final Path shardRoot = config.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        Files.createDirectories(shardRoot);
        final Path target = tempDir.resolve("active-target");
        Files.write(target, Bytes.utf8("not-an-active-pointer"));
        try {
            Files.createSymbolicLink(shardRoot.resolve("ACTIVE"), target);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.open(config, shardId, resources));
            assertTrue(failure.getCause() instanceof java.io.IOException);
        }
    }

    @Test
    void openRejectsSymbolicStoreIncarnation() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("symbolic-incarnation"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 26);
        final Path shardRoot = config.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        final Path incarnations = shardRoot.resolve("incarnations");
        Files.createDirectories(incarnations);
        final Path target = tempDir.resolve("incarnation-target");
        Files.createDirectories(target.resolve("db"));
        Files.write(target.resolve("db").resolve("CURRENT"), Bytes.utf8("MANIFEST-1\n"));
        try {
            Files.createSymbolicLink(incarnations.resolve(UUID.randomUUID().toString()), target);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.open(config, shardId, resources));
            assertTrue(failure.getCause() instanceof java.io.IOException);
        }
    }

    @Test
    void sloOutboxStartAndMergedFinalSurviveReopen() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 22);
        final SloSampleStartV1 start = sloStart();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outbox = new SloObservationOutboxStore(store);
            assertEquals(start, outbox.ensureStart(start).start());
            assertEquals(1, outbox.scan(10).size());
            final SloSampleFinalV1 finalObservation = new SloSampleFinalV1(start.sampleId(), start.startDigest(),
                    SloFinalOutcomeV1.BAD_TIMEOUT, SloThresholdUnitV1.MILLISECONDS, 10, 12, null,
                    endpoint(200), bytes(32, 9), 1);
            outbox.mergeFinal(finalObservation, SloThresholdDirectionV1.AT_MOST);
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxV1 value = new SloObservationOutboxStore(reopened).get(start.sampleId());
            assertEquals(SloFinalOutcomeV1.BAD_TIMEOUT, value.finalObservation().outcome());
            assertArrayEquals(value.canonicalBytes(),
                    SloObservationOutboxV1.decode(value.canonicalBytes()).canonicalBytes());
            assertThrows(IllegalStateException.class,
                    () -> new SloObservationOutboxStore(reopened).deleteAfterCollectorAck(start.sampleId(),
                            Bytes.sha256(Bytes.utf8("wrong-digest"))));
            assertEquals(1, new SloObservationOutboxStore(reopened).scan(10).size());
            assertEquals(true, new SloObservationOutboxStore(reopened).deleteAfterCollectorAck(start.sampleId(),
                    value.recordDigest()));
            assertEquals(0, new SloObservationOutboxStore(reopened).scan(10).size());
        }
    }

    @Test
    void completeCheckpointRestoresIntoFreshStoreIncarnation() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("source"));
        final Path checkpoint = tempDir.resolve("checkpoint-for-restore");
        final byte[] key = scratchMetaQuotaKey(14);
        final byte[] payload = Bytes.utf8("checkpoint-value");
        final byte[] originalStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            originalStoreIncarnation = store.metadata().storeIncarnation();
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key, payload));
            store.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("restored"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 3).payload());
            org.junit.jupiter.api.Assertions.assertFalse(
                    java.util.Arrays.equals(originalStoreIncarnation, restored.metadata().storeIncarnation()));
            assertNotEquals(sourceConfig.rootPath(), restoreConfig.rootPath());

            // restoreFromCheckpoint must release the worker-wide download slot
            // before returning the opened active DB, not only after the caller
            // closes that DB.  This exercises the real restore path rather
            // than only testing the semaphore API in isolation.
            resources.acquireCheckpointDownloadSlot();
            resources.releaseCheckpointDownloadSlot();
        }
    }

    @Test
    void restoreCanReplaceAnOrphanIncarnationWhenActivePointerWasNotInstalled() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 24);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("orphan-source"));
        final Path checkpoint = tempDir.resolve("orphan-checkpoint");
        final byte[] key = scratchMetaQuotaKey(13);
        final byte[] payload = Bytes.utf8("orphan-recovery");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.write(batch -> batch.putValue(ColumnFamily.META, 3, key, payload));
            source.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("orphan-restore"));
        final Path shardRoot = restoreConfig.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        final Path orphanDb;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore orphan = ShardStore.open(restoreConfig, shardId, resources)) {
            orphanDb = orphan.dbPath();
        }
        Files.delete(shardRoot.resolve("ACTIVE"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 3).payload());
            assertNotEquals(orphanDb, restored.dbPath());
            assertTrueFile(shardRoot.resolve("ACTIVE"));
            assertTrueFile(orphanDb.resolve("CURRENT"));
        }
    }

    @Test
    void restoreRejectsAnActivePointerWhoseDbIsMissing() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("active-corrupt-source"));
        final Path checkpoint = tempDir.resolve("active-corrupt-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig targetConfig = ShardStoreConfig.defaults(tempDir.resolve("active-corrupt-target"));
        final Path activeDb;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(targetConfig);
             ShardStore target = ShardStore.open(targetConfig, shardId, resources)) {
            activeDb = target.dbPath();
        }
        try (var paths = Files.walk(activeDb)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(targetConfig)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(targetConfig, shardId, resources, checkpoint));
            assertTrue(failure.getCause() instanceof java.io.IOException);
        }
    }

    @Test
    void restoreRejectsSymbolicActiveIncarnation() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 27);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("symbolic-restore-source"));
        final Path checkpoint = tempDir.resolve("symbolic-restore-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("symbolic-restore-target"));
        final Path shardRoot = restoreConfig.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        final Path activeIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore active = ShardStore.open(restoreConfig, shardId, resources)) {
            activeIncarnation = active.dbPath().getParent();
        }
        final Path external = tempDir.resolve("symbolic-active-incarnation-target");
        Files.move(activeIncarnation, external);
        try {
            Files.createSymbolicLink(activeIncarnation, external);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            Files.move(external, activeIncarnation);
            return;
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint));
            assertTrue(failure.getCause() instanceof java.io.IOException);
        }
    }

    @Test
    void failedStagedRestoreCleansRuntimeValidationTree() throws Exception {
        final ShardId sourceShard = new ShardId(RouteIncarnation.random(), 25);
        final ShardId targetShard = new ShardId(RouteIncarnation.random(), 25);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("failed-restore-source"));
        final Path checkpoint = tempDir.resolve("failed-restore-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, sourceShard, resources)) {
            source.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig targetConfig = ShardStoreConfig.defaults(tempDir.resolve("failed-restore-target"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(targetConfig)) {
            assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(targetConfig, targetShard, resources, checkpoint));
        }
        final Path restoreTmp = targetConfig.rootPath().resolve("shards")
                .resolve(targetShard.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(targetShard.partition())).resolve("restore-tmp");
        if (Files.exists(restoreTmp)) {
            try (var paths = Files.list(restoreTmp)) {
                assertTrue(paths.toList().isEmpty());
            }
        }
    }

    @Test
    void failedActivePointerInstallRemovesUnpublishedDb() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 26);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("pointer-failure-source"));
        final Path checkpoint = tempDir.resolve("pointer-failure-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig targetConfig = ShardStoreConfig.defaults(tempDir.resolve("pointer-failure-target"));
        final Path shardRoot = targetConfig.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        Files.createDirectories(shardRoot);
        Files.createDirectory(shardRoot.resolve("ACTIVE.tmp"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(targetConfig)) {
            assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(targetConfig, shardId, resources, checkpoint));
        }
        final Path incarnations = shardRoot.resolve("incarnations");
        if (Files.exists(incarnations)) {
            try (var paths = Files.list(incarnations)) {
                assertTrue(paths.toList().isEmpty());
            }
        }
        assertTrue(Files.isDirectory(shardRoot.resolve("ACTIVE.tmp")));
    }

    @Test
    void checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 52);
        final ShardStoreConfig checkpointConfig = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-tmp-link"));
        final Path checkpointParent = tempDir.resolve("checkpoint-tmp-link-parent");
        final Path checkpoint = checkpointParent.resolve("checkpoint");
        final Path checkpointOutside = tempDir.resolve("checkpoint-tmp-link-target");
        Files.createDirectories(checkpointParent);
        Files.createDirectories(checkpointOutside);
        try {
            Files.createSymbolicLink(checkpointParent.resolve("checkpoint-tmp"), checkpointOutside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(checkpointConfig);
             ShardStore store = ShardStore.open(checkpointConfig, shardId, resources)) {
            assertThrows(IllegalStateException.class, () -> store.createCheckpoint(checkpoint));
        }
        try (var paths = Files.walk(checkpointOutside)) {
            assertTrue(paths.noneMatch(path -> path.getFileName().toString().equals("CURRENT")));
        }

        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("active-tmp-source"));
        final Path sourceCheckpoint = tempDir.resolve("active-tmp-source-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.createCheckpoint(sourceCheckpoint);
        }
        final ShardStoreConfig targetConfig = ShardStoreConfig.defaults(tempDir.resolve("active-tmp-target"));
        final Path targetShardRoot = targetConfig.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        Files.createDirectories(targetShardRoot);
        final Path pointerOutside = tempDir.resolve("active-tmp-pointer-target");
        final byte[] pointerBytes = Bytes.utf8("preserve-this-pointer-target");
        Files.write(pointerOutside, pointerBytes);
        Files.createSymbolicLink(targetShardRoot.resolve("ACTIVE.tmp"), pointerOutside);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(targetConfig)) {
            assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(targetConfig, shardId, resources, sourceCheckpoint));
        }
        assertArrayEquals(pointerBytes, Files.readAllBytes(pointerOutside));
        assertTrue(Files.isSymbolicLink(targetShardRoot.resolve("ACTIVE.tmp")));
    }

    @Test
    void checkpointRejectsSymbolicParentComponentBeforeCreatingOutsideFiles() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 53);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-parent-link"));
        final Path parentRoot = tempDir.resolve("checkpoint-parent-link-root");
        final Path outside = tempDir.resolve("checkpoint-parent-link-outside");
        Files.createDirectories(parentRoot);
        Files.createDirectories(outside);
        final Path linkedParent = parentRoot.resolve("nested");
        try {
            Files.createSymbolicLink(linkedParent, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        final Path checkpoint = linkedParent.resolve("checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            assertThrows(IllegalStateException.class, () -> store.createCheckpoint(checkpoint));
        }
        try (var paths = Files.walk(outside)) {
            assertTrue(paths.noneMatch(path -> path.getFileName().toString().equals("CURRENT")));
        }
    }

    @Test
    void catalogBoundRestoreRejectsPinDriftBeforeActivePublication() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("catalog-source"));
        final Path checkpoint = tempDir.resolve("catalog-checkpoint");
        final byte[] key = scratchMetaQuotaKey(12);
        final byte[] payload = Bytes.utf8("catalog-value");
        final byte[] checkpointId = bytes(30);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), 0, null, 1_000);
        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshotFor(shardId);
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.recordControlSnapshot(controlSnapshot);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64be(0));
                batch.putValue(ColumnFamily.META, 3, key, payload);
            });
            store.createCheckpoint(checkpoint, checkpointId);
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, bytes(31), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(32), bytes(33), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(34), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 0, appliedPosition,
                controlSnapshot.snapshotDigest(), new byte[32], files);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        final ShardStoreConfig unpublishedConfig = ShardStoreConfig.defaults(tempDir.resolve("unpublished-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(unpublishedConfig)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.restoreFromCheckpoint(
                    unpublishedConfig, shardId, resources, checkpoint, manifest, catalog));
        }
        catalog.publish(manifest, 0);
        final RecoveryFloor floor = catalog.advanceFloor(manifest.checkpointId(), 1,
                Bytes.sha256(Bytes.utf8("catalog-floor")));
        final RecoveryFloorRefV1 floorRef = new RecoveryFloorRefV1(floor.recoveryLineageId(), floor.checkpointId(),
                floor.manifestSha256(), floor.catalogGeneration(), floor.appliedSourcePosition(),
                floor.includedMutationSequence(), List.of());
        final RecoveryPinV1 pin = new RecoveryPinV1(java.util.Arrays.copyOf(
                Bytes.sha256(Bytes.utf8("catalog-pin"), Bytes.utf8("id")), 16),
                new ShardSubjectV1(shardId), new OwnerIdentityV1(manifest.createdBy().deploymentId(),
                manifest.createdBy().workerRunId(), manifest.createdBy().ownerEpoch(),
                Bytes.sha256(Bytes.utf8("catalog-lease"))),
                new RecoveryCandidateRefV1(RecoveryCandidateKindV1.CATALOG_CHECKPOINT,
                        manifest.recoveryLineageId(), manifest.checkpointId(), manifest.manifestSha256(), null),
                floorRef, floor.catalogGeneration(), Bytes.sha256(Bytes.utf8("oxia-session")));
        catalog.createRecoveryPin(pin);
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("catalog-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources,
                     checkpoint, manifest, catalog, pin)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 3).payload());
            assertEquals(floorRef, restored.recoveryMetadata().lastObservedFloor());
            assertEquals(floorRef.catalogGeneration(), restored.recoveryMetadata().catalogGeneration());
            assertEquals(RecoveryCandidateKindV1.LOCAL_STORE, restored.recoveryMetadata().lineageBase().kind());
            assertArrayEquals(restored.metadata().storeIncarnation(),
                    restored.recoveryMetadata().lineageBase().storeIncarnation());
            assertTrue(restored.hasReusableRecoveryProof());
        }
        final RecoveryCatalogAuthority pinDriftingAuthority = new RecoveryCatalogAuthority() {
            private int pinReads;

            @Override
            public RecoveryCatalog.Publication publish(final CheckpointManifest candidate,
                                                       final long expectedCatalogGeneration) {
                return catalog.publish(candidate, expectedCatalogGeneration);
            }

            @Override
            public RecoveryFloor advanceFloor(final byte[] candidateCheckpointId,
                                               final long expectedCatalogGeneration,
                                               final byte[] evidenceCursorDigest) {
                return catalog.advanceFloor(candidateCheckpointId, expectedCatalogGeneration, evidenceCursorDigest);
            }

            @Override
            public Optional<CheckpointManifest> manifest(final byte[] candidateCheckpointId) {
                return catalog.manifest(candidateCheckpointId);
            }

            @Override
            public Optional<RecoveryFloor> currentFloor() {
                return catalog.currentFloor();
            }

            @Override
            public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
                catalog.validatePublishedRestoreCandidate(candidate);
            }

            @Override
            public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    final byte[] candidateCheckpointId, final long requiredMutationSequence,
                    final io.nereusstream.delay.protocol.SourcePosition... requiredPositions) {
                return catalog.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions);
            }

            @Override
            public Optional<RecoveryPinV1> activeRecoveryPin() {
                // The first three reads cover admission, staged validation and
                // the pre-rename install fence.  The fourth read simulates a
                // session/pin change while the formally opened incarnation is
                // being prepared for ACTIVE publication.
                if (++pinReads == 4) {
                    catalog.releaseRecoveryPin(pin);
                    return Optional.empty();
                }
                return catalog.activeRecoveryPin();
            }
        };
        final ShardStoreConfig driftingConfig = ShardStoreConfig.defaults(tempDir.resolve("pin-drift-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(driftingConfig)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.restoreFromCheckpoint(
                    driftingConfig, shardId, resources, checkpoint, manifest, pinDriftingAuthority, pin));
        }
        final Path driftingShardRoot = driftingConfig.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toUnsignedString(shardId.partition()));
        assertFalse(Files.exists(driftingShardRoot.resolve("ACTIVE"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
        assertTrue(catalog.activeRecoveryPin().isEmpty());
        final ShardStoreConfig missingPinConfig = ShardStoreConfig.defaults(tempDir.resolve("missing-pin-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(missingPinConfig)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.restoreFromCheckpoint(
                    missingPinConfig, shardId, resources, checkpoint, manifest, catalog, pin));
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

    @Test
    void workerAcquireSlotIsReleasedAfterOpenAndFailsBeforeOpeningWhenHeld() {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("acquire-bounded"), 1, 2, 32, 64,
                1, 1024 * 1024, 1024 * 1024, 1, 1, 1, 1024, 1, 1024 * 1024, 1);
        final ShardId first = new ShardId(RouteIncarnation.random(), 5);
        final ShardId second = new ShardId(RouteIncarnation.random(), 6);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore firstStore = ShardStore.open(config, first, resources)) {
            assertEquals(first, firstStore.shardId());
            // A successful open must release the short-lived acquisition slot
            // even though the DB and owned-shard slots remain held.
            resources.acquireShardAcquireSlot();
            resources.releaseShardAcquireSlot();

            resources.acquireShardAcquireSlot();
            try {
                final IllegalStateException rejected = assertThrows(IllegalStateException.class,
                        () -> ShardStore.open(config, second, resources));
                assertEquals("worker concurrent shard acquire limit reached", rejected.getMessage());
            } finally {
                resources.releaseShardAcquireSlot();
            }
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore secondStore = ShardStore.open(config, second, resources)) {
            assertEquals(second, secondStore.shardId());
        }
    }

    @Test
    void perDbWriteBufferCeilingMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new ShardStoreConfig(tempDir.resolve("invalid-wbm"),
                1, 1, 32, 32, 1, 1024 * 1024, 1024 * 1024, 1, 1, 1, 1024, 1, 0));
    }

    @Test
    void perDbWriteBufferCeilingIsBoundAtRocksDbDbLevel() throws Exception {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("db-wbm"), 1, 1, 32, 32,
                1, 1024 * 1024, 1024 * 1024, 1, 1, 1, 1024, 1, 4096);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 22);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final java.lang.reflect.Field field = ShardStore.class.getDeclaredField("dbOptions");
            field.setAccessible(true);
            final DBOptions options = (DBOptions) field.get(store);
            assertEquals(config.maxWriteBufferBytesPerDb(), options.dbWriteBufferSize());
        }
    }

    @Test
    void backgroundJobSplitMustFitPerDbCeiling() {
        assertThrows(IllegalArgumentException.class, () -> new ShardStoreConfig(
                tempDir.resolve("invalid-background-split"), 1, 1, 32, 32, 2,
                1024 * 1024, 1024 * 1024, 1, 1, 1, 1024, 1,
                1024 * 1024, 1, 2, 2));
    }

    @Test
    void workerOwnedShardLimitIsIndependentFromTransientDbSlots() {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("owned-bounded"), 1, 2, 32, 64,
                1, 1024 * 1024, 1024 * 1024, 1, 1, 1024);
        final ShardId first = new ShardId(RouteIncarnation.random(), 3);
        final ShardId second = new ShardId(RouteIncarnation.random(), 4);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            try (ShardStore firstStore = ShardStore.open(config, first, resources)) {
                assertEquals(first, firstStore.shardId());
                final IllegalStateException rejected = assertThrows(IllegalStateException.class,
                        () -> ShardStore.open(config, second, resources));
                assertEquals("worker maxOwnedShards limit reached", rejected.getMessage());
            }
            try (ShardStore secondStore = ShardStore.open(config, second, resources)) {
                assertEquals(second, secondStore.shardId());
            }
        }
    }

    @Test
    void duplicateOwnedShardOpenIsRejectedBeforeCreatingAnotherDb() throws Exception {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("duplicate-owned-shard"), 2, 2, 32,
                64, 1, 1024 * 1024, 1024 * 1024, 1, 1, 1, 1024, 1, 1024 * 1024, 2);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore first = ShardStore.open(config, shardId, resources);
            try {
                final IllegalStateException rejected = assertThrows(IllegalStateException.class,
                        () -> ShardStore.open(config, shardId, resources));
                assertEquals("worker already owns shard " + shardId, rejected.getMessage());
                try (var paths = Files.walk(tempDir.resolve("duplicate-owned-shard").resolve("shards"))) {
                    assertEquals(1, paths.filter(path -> path.getFileName().toString().equals("CURRENT")).count());
                }
            } finally {
                first.close();
            }
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(shardId, reopened.shardId());
        }
    }

    @Test
    void sharedResourcesCannotCloseBeforeTheShardDb() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resource-lifecycle"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 21);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        final ShardStore store = ShardStore.open(config, shardId, resources);
        try {
            assertThrows(IllegalStateException.class, resources::close);
        } finally {
            store.close();
            resources.close();
        }
    }

    @Test
    void physicalUsageProbeAndGuardObserveOneShardDb() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("physical-usage"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 53);
        final RocksDbUsageLimits limits = new RocksDbUsageLimits(
                Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Long.MAX_VALUE, 1, Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final RocksDbUsageSnapshot usage = store.physicalUsage();
            org.junit.jupiter.api.Assertions.assertEquals(shardId, usage.shardId());
            org.junit.jupiter.api.Assertions.assertTrue(usage.localBytes() > 0);
            org.junit.jupiter.api.Assertions.assertTrue(usage.localFiles() > 0);
            store.requirePhysicalUsageWithin(limits);
        }
    }

    @Test
    void physicalUsageFailsClosedOnADeceptiveSymbolicFile() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("physical-usage-symlink"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 54);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final Path outside = tempDir.resolve("physical-usage-outside");
            Files.write(outside, new byte[]{1, 2, 3});
            Files.createSymbolicLink(store.dbPath().resolve("external-file"), outside);

            assertThrows(IllegalStateException.class, store::physicalUsage);
        }
    }

    @Test
    void checkpointDownloadSlotIsWorkerBoundedAndReleased() {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("restore-slot"), 1, 2, 32, 64,
                1, 1024 * 1024, 1024 * 1024, 1, 1, 1, 1024);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        try {
            resources.acquireCheckpointDownloadSlot();
            assertThrows(IllegalStateException.class, resources::acquireCheckpointDownloadSlot);
            resources.releaseCheckpointDownloadSlot();
            resources.acquireCheckpointDownloadSlot();
            resources.releaseCheckpointDownloadSlot();
        } finally {
            resources.close();
        }
    }

    @Test
    void drainSlotIsWorkerBoundedAndCloseProtected() {
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("drain-slot"), 1, 1, 32, 32,
                1, 1024 * 1024, 1024 * 1024, 1, 1, 1, 1024, 1);
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        try {
            resources.acquireDrainSlot();
            assertThrows(IllegalStateException.class, resources::acquireDrainSlot);
            assertThrows(IllegalStateException.class, resources::close);
            resources.releaseDrainSlot();
            resources.acquireDrainSlot();
            resources.releaseDrainSlot();
        } finally {
            resources.close();
        }
    }

    @Test
    void sharedResourcesCannotCloseWhileCheckpointOperationHoldsWorkerSlot() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("checkpoint-resource-lifecycle"));
        final SharedRocksDbResources resources = new SharedRocksDbResources(config);
        try {
            resources.acquireCheckpointCreateSlot();
            assertThrows(IllegalStateException.class, resources::close);
            resources.releaseCheckpointCreateSlot();

            resources.acquireCheckpointUploadSlot();
            assertThrows(IllegalStateException.class, resources::close);
            resources.releaseCheckpointUploadSlot();
        } finally {
            resources.close();
        }
    }

    @Test
    void restoreWithManifestRejectsFileIdentityDrift() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("manifest-source"));
        final Path checkpoint = tempDir.resolve("manifest-checkpoint");
        final byte[] key = scratchMetaQuotaKey(11);
        final byte[] payload = Bytes.utf8("manifest-value");
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key, payload));
            store.createCheckpoint(checkpoint);
        }
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpoint);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(bytes(10), bytes(11), 1, null, null,
                new CheckpointManifest.CreatedBy(bytes(12), bytes(13), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(14), 1, 1, 1,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 1, new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 0, null, 1_000),
                new byte[32], new byte[32], files);

        final Path firstFile = checkpoint.resolve(inventory.get(0).name());
        Files.writeString(firstFile, "tampered");
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("manifest-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint, manifest));
        }
    }

    @Test
    void restoreWithManifestRejectsRuntimeStateDrift() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("runtime-manifest-source"));
        final Path checkpoint = tempDir.resolve("runtime-manifest-checkpoint");
        final byte[] checkpointId = bytes(40);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), 3, null, 1_003);
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64be(7));
            });
            store.createCheckpoint(checkpoint, checkpointId);
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, bytes(41), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(42), bytes(43), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(44), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 8, appliedPosition, new byte[32], new byte[32], files);

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("runtime-manifest-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.restoreFromCheckpoint(
                    restoreConfig, shardId, resources, checkpoint, manifest));
        }
    }

    @Test
    void restoreWithManifestRejectsControlStateDigestDrift() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 41);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("control-manifest-source"));
        final Path checkpoint = tempDir.resolve("control-manifest-checkpoint");
        final byte[] checkpointId = bytes(90);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), 6, null, 1_006);
        final CompatibleControlSnapshotV1 snapshot = new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 91), 1, bytes(32, 92), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 93), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        final long mutationSequence;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.recordControlSnapshot(snapshot);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64be(7));
            });
            store.createCheckpoint(checkpoint, checkpointId);
            mutationSequence = store.shardMutationSequence();
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, bytes(94), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(95), bytes(96), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(97), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, mutationSequence, appliedPosition, new byte[32], new byte[32], files);

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("control-manifest-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint, manifest));
            assertNotNull(failure.getCause());
            assertTrue(failure.getCause().getMessage().contains("control snapshot"));
        }
    }

    @Test
    void restoreWithManifestRejectsMissingControlSnapshot() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 42);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("missing-control-source"));
        final Path checkpoint = tempDir.resolve("missing-control-checkpoint");
        final byte[] checkpointId = bytes(100);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), 7, null, 1_007);
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        final long mutationSequence;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3),
                    appliedPosition.canonicalBytes()));
            store.createCheckpoint(checkpoint, checkpointId);
            mutationSequence = store.shardMutationSequence();
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, bytes(101), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(102), bytes(103), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(104), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, mutationSequence, appliedPosition, bytes(32, 105), bytes(32, 106), files);

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("missing-control-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint, manifest));
            assertNotNull(failure.getCause());
            assertTrue(failure.getCause().getMessage().contains("control snapshot is missing"));
        }
    }

    @Test
    void restoreWithManifestRejectsRecoveryProjectionLineageDrift() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 37);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("recovery-manifest-source"));
        final Path checkpoint = tempDir.resolve("recovery-manifest-checkpoint");
        final byte[] checkpointId = bytes(50);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), 4, null, 1_004);
        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshotFor(shardId);
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.recordControlSnapshot(controlSnapshot);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64be(9));
            });
            store.recordRecoveryMetadata(new RecoveryCandidateRefV1(RecoveryCandidateKindV1.LOCAL_STORE,
                    bytes(51), checkpointId, bytes(32, 52), store.metadata().storeIncarnation()), null);
            store.createCheckpoint(checkpoint, checkpointId);
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, bytes(53), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(54), bytes(55), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(56), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 9, appliedPosition, controlSnapshot.snapshotDigest(), new byte[32], files);

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("recovery-manifest-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            assertThrows(IllegalStateException.class, () -> ShardStore.restoreFromCheckpoint(
                    restoreConfig, shardId, resources, checkpoint, manifest));
        }
    }

    @Test
    void restoreWithManifestRejectsRecoveryProjectionCheckpointDrift() throws Exception {
        assertRecoveryProjectionRestoreRejected("recovery-checkpoint-drift", 38, bytes(60), bytes(61),
                bytes(32, 62), "candidate checkpoint");
    }

    @Test
    void restoreWithManifestRejectsRecoveryProjectionManifestHashDrift() throws Exception {
        assertRecoveryProjectionRestoreRejected("recovery-manifest-hash-drift", 39, bytes(63), bytes(63),
                bytes(32, 64), "manifest hash");
    }

    private void assertRecoveryProjectionRestoreRejected(final String namespace, final int partition,
                                                         final byte[] checkpointId,
                                                         final byte[] candidateCheckpointId,
                                                         final byte[] candidateManifestHash,
                                                         final String expectedCause) throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), partition);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve(namespace + "-source"));
        final Path checkpoint = tempDir.resolve(namespace + "-checkpoint");
        final byte[] lineage = bytes(70 + partition);
        final KafkaSourcePosition appliedPosition = new KafkaSourcePosition(
                shardId, "cluster", UUID.randomUUID(), partition, null, 1_000L + partition);
        final CompatibleControlSnapshotV1 controlSnapshot = controlSnapshotFor(shardId);
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.recordControlSnapshot(controlSnapshot);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3), appliedPosition.canonicalBytes());
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5), Bytes.u64be(11));
            });
            store.recordRecoveryMetadata(new RecoveryCandidateRefV1(RecoveryCandidateKindV1.LOCAL_STORE,
                    lineage, candidateCheckpointId, candidateManifestHash, store.metadata().storeIncarnation()), null);
            store.createCheckpoint(checkpoint, checkpointId);
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(checkpointId, lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(80), bytes(81), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "CERTIFIED_HOST_CLOCK", bytes(82), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 11, appliedPosition, controlSnapshot.snapshotDigest(), new byte[32], files);

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve(namespace + "-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig)) {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint, manifest));
            assertNotNull(failure.getCause());
            assertTrue(failure.getCause().getMessage().contains(expectedCause));
        }
    }

    private static byte[] bytes(final int last) {
        final byte[] value = new byte[16];
        value[15] = (byte) last;
        return value;
    }

    /** Raw unregistered key used only by generic RocksDB persistence fixtures. */
    private static byte[] scratchMetaQuotaKey(final int subtype) {
        if (subtype <= 5 || subtype > 0xff) {
            throw new IllegalArgumentException("scratch subtype must be outside the registered range");
        }
        return new byte[]{3, 1, (byte) subtype};
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }

    private static CompatibleControlSnapshotV1 controlSnapshotFor(final ShardId shardId) {
        return new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 107), 1, bytes(32, 108), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 109), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static SloSampleStartV1 sloStart() {
        final byte[] identityPayload = io.nereusstream.delay.protocol.CanonicalProtobuf.message(output ->
                io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, 1, bytes(16, 8)));
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.QUERY_LATENCY, identityPayload);
        return new SloSampleStartV1(Bytes.sha256(Bytes.utf8("slo-objective")),
                SloObjectiveNameV1.QUERY_LATENCY, SloPopulationV1.ALL_ACCEPTED, SloPathV1.NOT_APPLICABLE,
                identity, endpoint(100), 200L);
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static void assertRawRocksDbCanBeOpened(final Path dbPath) throws Exception {
        final List<byte[]> names;
        try (Options options = new Options()) {
            names = RocksDB.listColumnFamilies(options, dbPath.toString());
        }
        final List<ColumnFamilyOptions> options = names.stream()
                .map(ignored -> new ColumnFamilyOptions()).toList();
        final List<ColumnFamilyDescriptor> descriptors = new java.util.ArrayList<>();
        for (int index = 0; index < names.size(); index++) {
            descriptors.add(new ColumnFamilyDescriptor(names.get(index), options.get(index)));
        }
        final List<ColumnFamilyHandle> handles = new java.util.ArrayList<>();
        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(false)
                .setCreateMissingColumnFamilies(false);
             RocksDB db = RocksDB.open(dbOptions, dbPath.toString(), descriptors, handles)) {
            assertEquals(names.size(), handles.size());
            assertTrue(db.getLatestSequenceNumber() >= 0);
        } finally {
            handles.forEach(ColumnFamilyHandle::close);
            options.forEach(ColumnFamilyOptions::close);
        }
    }

    private static void overwriteRawColumnFamilyValue(final Path dbPath, final String columnFamilyName,
                                                      final byte[] key, final byte[] value) throws Exception {
        final List<byte[]> names;
        try (Options options = new Options()) {
            names = RocksDB.listColumnFamilies(options, dbPath.toString());
        }
        final List<ColumnFamilyOptions> options = names.stream()
                .map(ignored -> new ColumnFamilyOptions()).toList();
        final List<ColumnFamilyDescriptor> descriptors = new java.util.ArrayList<>();
        for (int index = 0; index < names.size(); index++) {
            descriptors.add(new ColumnFamilyDescriptor(names.get(index), options.get(index)));
        }
        final List<ColumnFamilyHandle> handles = new java.util.ArrayList<>();
        try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(false)
                .setCreateMissingColumnFamilies(false);
             RocksDB db = RocksDB.open(dbOptions, dbPath.toString(), descriptors, handles)) {
            final int index = names.stream().map(name -> new String(name, java.nio.charset.StandardCharsets.UTF_8))
                    .toList().indexOf(columnFamilyName);
            if (index < 0) {
                throw new AssertionError("missing column family: " + columnFamilyName);
            }
            db.put(handles.get(index), key, value);
        } finally {
            handles.forEach(ColumnFamilyHandle::close);
            options.forEach(ColumnFamilyOptions::close);
        }
    }

    private static void assertTrueFile(final Path path) {
        if (!Files.isRegularFile(path)) {
            throw new AssertionError("expected regular file: " + path);
        }
    }
}
