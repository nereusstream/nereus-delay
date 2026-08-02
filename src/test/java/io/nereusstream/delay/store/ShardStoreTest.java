package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        final byte[] key = KeyCodec.metaFixed(7);
        final byte[] payload = Bytes.utf8("checkpoint-value");
        final byte[] originalStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            originalStoreIncarnation = store.metadata().storeIncarnation();
            store.write(batch -> batch.putValue(ColumnFamily.META, 7, key, payload));
            store.createCheckpoint(checkpoint);
        }

        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("restored"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources, checkpoint)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 7).payload());
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
        final byte[] key = KeyCodec.metaFixed(9);
        final byte[] payload = Bytes.utf8("orphan-recovery");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore source = ShardStore.open(sourceConfig, shardId, resources)) {
            source.write(batch -> batch.putValue(ColumnFamily.META, 9, key, payload));
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
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 9).payload());
            assertNotEquals(orphanDb, restored.dbPath());
            assertTrueFile(shardRoot.resolve("ACTIVE"));
            assertTrueFile(orphanDb.resolve("CURRENT"));
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
    void catalogBoundRestoreRequiresPublishedFloorEligibleManifest() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final ShardStoreConfig sourceConfig = ShardStoreConfig.defaults(tempDir.resolve("catalog-source"));
        final Path checkpoint = tempDir.resolve("catalog-checkpoint");
        final byte[] key = KeyCodec.metaFixed(7);
        final byte[] payload = Bytes.utf8("catalog-value");
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> batch.putValue(ColumnFamily.META, 7, key, payload));
            store.createCheckpoint(checkpoint);
        }
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(checkpoint).stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(bytes(30), bytes(31), 0, null, null,
                new CheckpointManifest.CreatedBy(bytes(32), bytes(33), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "TEST_CLOCK", bytes(34), 1, 0, 0,
                        Bytes.sha256(Bytes.utf8("evidence")), 0, null), shardId, dbIdentity, sourceStoreIncarnation,
                1, 0, new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(), 0, null, 1_000),
                new byte[32], new byte[32], files);
        final RecoveryCatalog catalog = new RecoveryCatalog();
        final ShardStoreConfig unpublishedConfig = ShardStoreConfig.defaults(tempDir.resolve("unpublished-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(unpublishedConfig)) {
            assertThrows(IllegalArgumentException.class, () -> ShardStore.restoreFromCheckpoint(
                    unpublishedConfig, shardId, resources, checkpoint, manifest, catalog));
        }
        catalog.publish(manifest, 0);
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(tempDir.resolve("catalog-restore"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, shardId, resources,
                     checkpoint, manifest.canonicalJsonBytes(), catalog)) {
            assertArrayEquals(payload, restored.getValue(ColumnFamily.META, key, 7).payload());
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
        final byte[] key = KeyCodec.metaFixed(8);
        final byte[] payload = Bytes.utf8("manifest-value");
        final byte[] dbIdentity;
        final UUID sourceStoreIncarnation;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(sourceConfig);
             ShardStore store = ShardStore.open(sourceConfig, shardId, resources)) {
            dbIdentity = store.metadata().dbIdentity();
            sourceStoreIncarnation = store.metadata().storeIncarnationUuid();
            store.write(batch -> batch.putValue(ColumnFamily.META, 8, key, payload));
            store.createCheckpoint(checkpoint);
        }
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpoint);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version"), null))
                .toList();
        final CheckpointManifest manifest = new CheckpointManifest(bytes(10), bytes(11), 1, null, null,
                new CheckpointManifest.CreatedBy(bytes(12), bytes(13), 1),
                new CheckpointManifest.CreatedAt(1_000, 1_000, "TEST_CLOCK", bytes(14), 1, 1, 1,
                        Bytes.sha256(Bytes.utf8("evidence")), 1, null), shardId, dbIdentity, sourceStoreIncarnation,
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

    private static byte[] bytes(final int last) {
        final byte[] value = new byte[16];
        value[15] = (byte) last;
        return value;
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
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

    private static void assertTrueFile(final Path path) {
        if (!Files.isRegularFile(path)) {
            throw new AssertionError("expected regular file: " + path);
        }
    }
}
