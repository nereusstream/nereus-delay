package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.LegacyCheckpointTailReplayer;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishAdmissionBodyTest;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.GenerationRuntimeIndex;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.runtime.RetiredMessageIdentityRecord;
import com.nereusstream.delay.runtime.TerminalGenerationRecord;
import com.nereusstream.delay.runtime.TimelineEntry;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.TimelineWorkRef;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

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
        final DelayMessageId retiredMessageId = DelayMessageId.random(shard);
        final RetiredMessageIdentityRecord retired = new RetiredMessageIdentityRecord(
                retiredMessageId,
                retiredMessageId.routingId().logicalTimestampEpochMs(),
                17,
                source.canonicalBytes());
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
                final ScheduledFixture scheduled = scheduledFixture(shard, source, "legacy-inventory-lane");
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(retiredMessageId),
                        retired.encode());
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
        assertRecoveryInstallIsOnlyNonRuntimeBlocker(state, source);
        assertEquals(1, state.retiredMessageIdentities());
        assertEquals(
                1,
                state.messageDispositions().get(
                        LegacyCheckpointStateInventory.MessageDisposition.CANDIDATE_PENDING_FULL_AUDIT));
        assertTrue(state.recordsByFamily().get(ColumnFamily.ID) > 0);
        assertEquals(
                state.scannedRecords(),
                state.recordsByFamily().values().stream().mapToLong(Long::longValue).sum());
        assertTrue(state.scannedBytes() > 0);
        assertTrue(state.chargedBytes() >= state.scannedBytes());
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
    void opensWritableReplayOnlyOnAnIsolatedCopyAndRemovesItAfterNativeClose() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 14);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("replay-copy-store"));
        final Path image = tempDir.resolve("replay-copy-checkpoint");
        final byte[] checkpointId = bytes(16, 16);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final CheckpointManifest manifest;
        final Path activePointer = config.rootPath()
                .resolve("shards")
                .resolve(shard.routeIncarnation().uuid().toString())
                .resolve(Integer.toUnsignedString(shard.partition()))
                .resolve("ACTIVE");
        final List<CheckpointFileInventory> sourceFiles;
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
                        Bytes.u64beBits(3));
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
            sourceFiles = CheckpointFileInventory.collect(image, finiteLimits());
            final byte[] activeBefore = Files.readAllBytes(activePointer);
            final int registeredStores = resources.registeredPhysicalUsageSources();
            Path workspace;
            try (ShardStore.LegacyCheckpointReplayCopy replayCopy = ShardStore.openLegacyCheckpointReplayCopy(
                    config, shard, resources, image, manifest, finiteLimits())) {
                final ShardStore copy = replayCopy.store();
                workspace = copy.dbPath().getParent();
                assertEquals(shard, copy.shardId());
                assertArrayEquals(store.metadata().dbIdentity(), copy.metadata().dbIdentity());
                assertEquals(store.metadata().storeIncarnationUuid(), copy.metadata().storeIncarnationUuid());
                assertEquals(manifest.sourceStoreIncarnation(), copy.metadata().storeIncarnationUuid());
                assertEquals(source, copy.appliedShardLogPosition());
                assertFalse(Files.exists(workspace.resolve("ACTIVE")));
                assertEquals(registeredStores, resources.registeredPhysicalUsageSources());

                final byte[] scratchKey = KeyCodec.idMessage(DelayMessageId.random(shard));
                final byte[] scratchPayload = Bytes.utf8("private replay mutation");
                copy.write(batch -> batch.putValue(ColumnFamily.ID, 1, scratchKey, scratchPayload));
                assertArrayEquals(scratchPayload, copy.getValue(ColumnFamily.ID, scratchKey, 1).payload());
                assertArrayEquals(activeBefore, Files.readAllBytes(activePointer));
            }
            assertFalse(Files.exists(workspace));
            assertEquals(registeredStores, resources.registeredPhysicalUsageSources());
            assertFileInventoriesEqual(sourceFiles, CheckpointFileInventory.collect(image, finiteLimits()));
        }
    }

    @Test
    void replaysLegacyTailOnlyThroughTheProtectedCutAndBlocksWhenBudgetStopsEarly() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 15);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("tail-replay-store"));
        final Path image = tempDir.resolve("tail-replay-checkpoint");
        final byte[] checkpointId = bytes(16, 17);
        final UUID topicId = UUID.randomUUID();
        final KafkaSourcePosition checkpointPosition =
                new KafkaSourcePosition(shard, "legacy-tail-cluster", topicId, 31, null, 4_000);
        final KafkaSourcePosition firstTail =
                new KafkaSourcePosition(shard, "legacy-tail-cluster", topicId, 32, null, 4_100);
        final KafkaSourcePosition cutPosition =
                new KafkaSourcePosition(shard, "legacy-tail-cluster", topicId, 33, null, 4_200);
        final KafkaSourcePosition afterCut =
                new KafkaSourcePosition(shard, "legacy-tail-cluster", topicId, 34, null, 4_300);
        final CheckpointManifest manifest;
        final Path activePointer = config.rootPath()
                .resolve("shards")
                .resolve(shard.routeIncarnation().uuid().toString())
                .resolve(Integer.toUnsignedString(shard.partition()))
                .resolve("ACTIVE");
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                bytes(32, 61),
                1,
                new KafkaActivationBarrier(shard, "legacy-tail-cluster", topicId, 0));
        final SourceRecordConsumer.CheckpointCut protectedCut = new SourceRecordConsumer.CheckpointCut() {
            @Override
            public com.nereusstream.delay.protocol.SourcePosition position() {
                return cutPosition;
            }

            @Override
            public void requireCurrent() {
                // This fixture represents a still-valid source-side cut capability.
            }
        };
        final List<SourceReplayEntry> sourceEntries = List.of(
                new SourceReplayRecord(PreparedCommand.cancel(shard, DelayMessageId.random(shard), 0, 10_000),
                        firstTail, null, null),
                new SourceReplayRecord(PreparedCommand.cancel(shard, DelayMessageId.random(shard), 0, 10_000),
                        cutPosition, null, null),
                new SourceReplayRecord(PreparedCommand.cancel(shard, DelayMessageId.random(shard), 0, 10_000),
                        afterCut, null, null));
        final var verificationKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final List<CheckpointFileInventory> filesBefore;
        final byte[] activeBefore;

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore original = ShardStore.open(config, shard, resources)) {
            original.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                        checkpointPosition.canonicalBytes());
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(0));
            });
            original.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, original, checkpointId, checkpointPosition);
            filesBefore = CheckpointFileInventory.collect(image, finiteLimits());
            activeBefore = Files.readAllBytes(activePointer);

            final AtomicInteger exactIteratorReads = new AtomicInteger();
            final SourceReplayCursor<SourceReplayEntry> exactCursor = trackedCursor(sourceEntries, exactIteratorReads);
            try (ShardStore.LegacyCheckpointReplayCopy replayCopy = ShardStore.openLegacyCheckpointReplayCopy(
                    config, shard, resources, image, manifest, finiteLimits())) {
                final LegacyCheckpointTailReplayer.Result result = LegacyCheckpointTailReplayer.replay(
                        replayCopy,
                        assignment,
                        SourceReplaySuccessor.strictKafka(),
                        protectedCut,
                        exactCursor,
                        store -> new DelayShard(store, DelayShardConfig.defaults()),
                        verificationKeys.getPublic(),
                        () -> 5_000,
                        new ReplayTurnBudget(8, 1 << 20, 60_000_000_000L));
                assertEquals(LegacyCheckpointTailReplayer.Status.EXACT_CUT_REACHED, result.status());
                assertEquals(LegacyCheckpointTailReplayer.BlockReason.NONE, result.blockReason());
                assertEquals(2, result.recordsApplied());
                assertEquals(2, result.commandsApplied());
                assertEquals(0, result.systemMutationsApplied());
                assertEquals(cutPosition, result.appliedThrough());
                assertEquals(2, exactIteratorReads.get());
                assertEquals(checkpointPosition, original.appliedShardLogPosition());
            }

            final AtomicInteger boundedIteratorReads = new AtomicInteger();
            final SourceReplayCursor<SourceReplayEntry> boundedCursor =
                    trackedCursor(sourceEntries, boundedIteratorReads);
            try (ShardStore.LegacyCheckpointReplayCopy replayCopy = ShardStore.openLegacyCheckpointReplayCopy(
                    config, shard, resources, image, manifest, finiteLimits())) {
                final LegacyCheckpointTailReplayer.Result result = LegacyCheckpointTailReplayer.replay(
                        replayCopy,
                        assignment,
                        SourceReplaySuccessor.strictKafka(),
                        protectedCut,
                        boundedCursor,
                        store -> new DelayShard(store, DelayShardConfig.defaults()),
                        verificationKeys.getPublic(),
                        () -> 5_000,
                        new ReplayTurnBudget(1, 1 << 20, 60_000_000_000L));
                assertEquals(LegacyCheckpointTailReplayer.Status.BLOCKED, result.status());
                assertEquals(
                        LegacyCheckpointTailReplayer.BlockReason.REPLAY_BUDGET_EXHAUSTED, result.blockReason());
                assertEquals(1, result.recordsApplied());
                assertEquals(firstTail, result.appliedThrough());
                assertTrue(boundedIteratorReads.get() <= 2);
            }

            assertEquals(checkpointPosition, original.appliedShardLogPosition());
            assertArrayEquals(activeBefore, Files.readAllBytes(activePointer));
            assertFileInventoriesEqual(filesBefore, CheckpointFileInventory.collect(image, finiteLimits()));
        }
    }

    private static SourceReplayCursor<SourceReplayEntry> trackedCursor(
            final List<SourceReplayEntry> entries, final AtomicInteger nextCalls) {
        final Iterator<SourceReplayEntry> iterator = new Iterator<>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < entries.size();
            }

            @Override
            public SourceReplayEntry next() {
                nextCalls.incrementAndGet();
                return entries.get(index++);
            }
        };
        return SourceReplayCursor.of(iterator);
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
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(1));
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
        final IllegalArgumentException malformed = assertThrows(
                IllegalArgumentException.class,
                () -> LegacyCheckpointStateInventory.inspect(
                        image,
                        shard,
                        manifest,
                        finiteLimits(),
                        new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L)));
        assertTrue(malformed.getMessage().contains("unsupported message record version"));
        final List<CheckpointFileInventory> after = CheckpointFileInventory.collect(image, finiteLimits());
        assertFileInventoriesEqual(before, after);
    }

    @Test
    void reportsMissingScheduledExpiryIndexAsAStableSourceBoundConflict() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("missing-index-store"));
        final Path image = tempDir.resolve("missing-index-checkpoint");
        final byte[] checkpointId = bytes(16, 4);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "missing-index-lane");
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertConflictCountExcludingRuntimeMetadata(inventory, 2);
        final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().stream()
                .filter(item -> item.reason() == LegacyCheckpointStateInventory.ConflictReason.EXPIRY_INDEX_MISSING)
                .findFirst()
                .orElseThrow();
        assertEquals(LegacyCheckpointStateInventory.ConflictReason.EXPIRY_INDEX_MISSING, conflict.reason());
        assertEquals("MESSAGE", conflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(scheduled.messageId())), conflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
    }

    @Test
    void reportsMalformedTerminalGenerationKeyAsAStableConflict() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-terminal-key-store"));
        final Path image = tempDir.resolve("malformed-terminal-key-checkpoint");
        final byte[] checkpointId = bytes(16, 24);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "malformed-terminal-key-lane");
        final byte[] malformedKey = new byte[] {1, 1, 1};
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.put(ColumnFamily.TERMINAL, malformedKey, Bytes.utf8("malformed terminal key"));
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertConflictCountExcludingRuntimeMetadata(inventory, 2);
        final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().stream()
                .filter(item -> item.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.TERMINAL_GENERATION_KEY_MALFORMED)
                .findFirst()
                .orElseThrow();
        assertEquals(
                LegacyCheckpointStateInventory.ConflictReason.TERMINAL_GENERATION_KEY_MALFORMED,
                conflict.reason());
        assertEquals("TERMINAL", conflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(Bytes.sha256(malformedKey), conflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
    }

    @Test
    void reportsMalformedMessageKeysAsStableConflictsAndContinuesInventory() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-message-key-store"));
        final Path image = tempDir.resolve("malformed-message-key-checkpoint");
        final byte[] checkpointId = bytes(16, 25);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "malformed-message-key-lane");
        final byte[] malformedLengthKey = new byte[] {1, 1, 1};
        final byte[] malformedIdentityKey = new byte[2 + DelayMessageId.LENGTH];
        malformedIdentityKey[0] = 1;
        malformedIdentityKey[1] = 1;
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.put(ColumnFamily.ID, malformedLengthKey, Bytes.utf8("malformed message key length"));
                batch.put(ColumnFamily.ID, malformedIdentityKey, Bytes.utf8("malformed message identity"));
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertEquals(1, inventory.messageStatuses().get(MessageStatus.SCHEDULED));
        assertConflictCountExcludingRuntimeMetadata(inventory, 3);
        for (byte[] malformedKey : List.of(malformedLengthKey, malformedIdentityKey)) {
            final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().stream()
                    .filter(item -> Bytes.constantTimeEquals(Bytes.sha256(malformedKey), item.oldKeyDigest()))
                    .findFirst()
                    .orElseThrow();
            assertEquals(
                    LegacyCheckpointStateInventory.ConflictReason.MESSAGE_KEY_MALFORMED,
                    conflict.reason());
            assertEquals("MESSAGE", conflict.recordKind());
            assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
        }
    }

    @Test
    void reportsMalformedLaneKeysAndValuesAsStableConflictsAndContinuesInventory() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-lane-store"));
        final Path image = tempDir.resolve("malformed-lane-checkpoint");
        final byte[] checkpointId = bytes(16, 26);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "malformed-lane-value");
        final byte[] malformedKey = new byte[] {2, 1, 1};
        final byte[] malformedRecordKey = KeyCodec.metaLane(scheduled.message().laneId());
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.put(ColumnFamily.META, malformedKey, Bytes.utf8("malformed Lane key"));
                batch.put(ColumnFamily.META, malformedRecordKey, Bytes.utf8("malformed Lane envelope"));
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertEquals(1, inventory.messageStatuses().get(MessageStatus.SCHEDULED));
        assertConflictCountExcludingRuntimeMetadata(inventory, 3);
        final LegacyCheckpointStateInventory.Conflict malformedKeyConflict = inventory.conflicts().stream()
                .filter(item -> Bytes.constantTimeEquals(Bytes.sha256(malformedKey), item.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals(
                LegacyCheckpointStateInventory.ConflictReason.LANE_KEY_MALFORMED,
                malformedKeyConflict.reason());
        assertEquals("LANE", malformedKeyConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), malformedKeyConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict malformedRecordConflict = inventory.conflicts().stream()
                .filter(item -> Bytes.constantTimeEquals(Bytes.sha256(malformedRecordKey), item.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals(
                LegacyCheckpointStateInventory.ConflictReason.LANE_RECORD_MALFORMED,
                malformedRecordConflict.reason());
        assertEquals("LANE", malformedRecordConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), malformedRecordConflict.sourcePosition()));
    }

    @Test
    void reportsMalformedClaimSequenceMetadataAndContinuesInventory() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("malformed-claim-sequence-store"));
        final Path malformedEnvelopeImage = tempDir.resolve("malformed-claim-sequence-envelope-checkpoint");
        final Path malformedPayloadImage = tempDir.resolve("malformed-claim-sequence-payload-checkpoint");
        final byte[] checkpointId = bytes(16, 28);
        final byte[] secondCheckpointId = bytes(16, 29);
        final byte[] claimSequenceKey = KeyCodec.metaFixed(ShardStore.META_CLAIM_SEQUENCE);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "malformed-claim-sequence-lane");
        final CheckpointManifest malformedEnvelopeManifest;
        final CheckpointManifest malformedPayloadManifest;
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.put(ColumnFamily.META, claimSequenceKey, Bytes.utf8("malformed sequence envelope"));
            });
            store.createCheckpoint(malformedEnvelopeImage, checkpointId);
            malformedEnvelopeManifest =
                    manifestFor(malformedEnvelopeImage, shard, store, checkpointId, source);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, claimSequenceKey, Bytes.u32be(7));
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(2));
            });
            store.createCheckpoint(malformedPayloadImage, secondCheckpointId);
            malformedPayloadManifest =
                    manifestFor(malformedPayloadImage, shard, store, secondCheckpointId, source);
        }

        for (ExpectedCheckpoint checkpoint : List.of(
                new ExpectedCheckpoint(malformedEnvelopeImage, malformedEnvelopeManifest),
                new ExpectedCheckpoint(malformedPayloadImage, malformedPayloadManifest))) {
            final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                    checkpoint.image(),
                    shard,
                    checkpoint.manifest(),
                    finiteLimits(),
                    new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
            assertEquals(1, inventory.messageStatuses().get(MessageStatus.SCHEDULED));
            assertConflictCountExcludingRuntimeMetadata(inventory, 2);
            final LegacyCheckpointStateInventory.Conflict malformedSequence = inventory.conflicts().stream()
                    .filter(item -> Bytes.constantTimeEquals(Bytes.sha256(claimSequenceKey), item.oldKeyDigest()))
                    .findFirst()
                    .orElseThrow();
            assertEquals("CLAIM_SEQUENCE", malformedSequence.recordKind());
            assertEquals(
                    LegacyCheckpointStateInventory.ConflictReason.CLAIM_SEQUENCE_METADATA_MALFORMED,
                    malformedSequence.reason());
            assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), malformedSequence.sourcePosition()));
            assertTrue(inventory.conflicts().stream().anyMatch(item -> item.reason()
                    == LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED));
            assertFalse(inventory.conflicts().stream().anyMatch(item -> item.reason()
                    == LegacyCheckpointStateInventory.ConflictReason.CLAIM_SEQUENCE_AFTER_CHECKPOINT_HIGH_WATER));
        }
    }

    @Test
    void reportsUnauditedLegacyNamespacesAndUnknownKeysAndContinuesInventory() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("unaudited-namespace-store"));
        final Path image = tempDir.resolve("unaudited-namespace-checkpoint");
        final byte[] checkpointId = bytes(16, 27);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "unaudited-namespace-lane");
        final byte[] reservationKey = KeyCodec.idReservation(bytes(32, 31));
        final byte[] bindingKey = KeyCodec.idScheduleBinding(scheduled.messageId());
        final byte[] readyKey = KeyCodec.timelineReady(5_000, scheduled.message().laneId(), 1);
        final byte[] dedupeKey = KeyCodec.dedupeSystemMutation(bytes(32, 32));
        final byte[] unknownDedupeKey = new byte[] {(byte) 0x7f, 1};
        final byte[] dlqExportKey = KeyCodec.terminalDlqExport(bytes(32, 33));
        final byte[] gcKey = KeyCodec.gcTask(6_000, (byte) 1, bytes(32, 34), 1);
        final byte[] quotaKey = KeyCodec.metaQuota(1);
        final byte[] schedulerKey = KeyCodec.metaScheduler(1);
        final byte[] controlMetadataKey = KeyCodec.metaFixed(10);
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(
                        ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.put(ColumnFamily.ID, reservationKey, Bytes.utf8("opaque reservation"));
                batch.put(ColumnFamily.ID, bindingKey, Bytes.utf8("opaque binding"));
                batch.put(ColumnFamily.TIMELINE, readyKey, Bytes.utf8("opaque READY projection"));
                batch.put(ColumnFamily.DEDUPE, dedupeKey, Bytes.utf8("opaque dedupe record"));
                batch.put(ColumnFamily.DEDUPE, unknownDedupeKey, Bytes.utf8("unknown dedupe key"));
                batch.put(ColumnFamily.TERMINAL, dlqExportKey, Bytes.utf8("opaque DLQ export"));
                batch.put(ColumnFamily.GC, gcKey, Bytes.utf8("opaque GC task"));
                batch.put(ColumnFamily.META, quotaKey, Bytes.utf8("opaque quota projection"));
                batch.put(ColumnFamily.META, schedulerKey, Bytes.utf8("opaque scheduler state"));
                batch.putValue(ColumnFamily.META, 1, controlMetadataKey, Bytes.utf8("opaque control snapshot"));
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertEquals(1, inventory.messageStatuses().get(MessageStatus.SCHEDULED));
        final List<ExpectedLegacyBlocker> expected = List.of(
                new ExpectedLegacyBlocker(
                        reservationKey,
                        "RESERVATION",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        bindingKey,
                        "SCHEDULE_BINDING",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        readyKey, "READY", LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        dedupeKey, "DEDUPE", LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        unknownDedupeKey,
                        "LEGACY_DEDUPE_KEY",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_KEY_KIND_UNRECOGNIZED),
                new ExpectedLegacyBlocker(
                        dlqExportKey,
                        "DLQ_EXPORT",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        gcKey, "GC", LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        quotaKey, "QUOTA", LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        schedulerKey,
                        "SCHEDULER",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        controlMetadataKey,
                        "CONTROL_METADATA",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_CONTROL_METADATA_UNAUDITED),
                new ExpectedLegacyBlocker(
                        KeyCodec.metaRecovery(4),
                        "RECOVERY_METADATA",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        KeyCodec.metaFixed(6),
                        "RUNTIME_METADATA",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        KeyCodec.metaFixed(8),
                        "RUNTIME_METADATA",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED),
                new ExpectedLegacyBlocker(
                        KeyCodec.metaFixed(9),
                        "RUNTIME_METADATA",
                        LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED));
        assertEquals(expected.size(), inventory.conflicts().size());
        for (ExpectedLegacyBlocker blocker : expected) {
            final LegacyCheckpointStateInventory.Conflict actual = inventory.conflicts().stream()
                    .filter(item -> Bytes.constantTimeEquals(Bytes.sha256(blocker.key()), item.oldKeyDigest()))
                    .findFirst()
                    .orElseThrow();
            assertEquals(blocker.recordKind(), actual.recordKind());
            assertEquals(blocker.reason(), actual.reason());
            assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), actual.sourcePosition()));
        }
    }

    @Test
    void acceptsLegacyTimelinePointersForScheduledCheckpointMessages() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("legacy-timeline-store"));
        final Path image = tempDir.resolve("legacy-timeline-checkpoint");
        final byte[] checkpointId = bytes(16, 6);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "legacy-timeline-lane");
        final byte[] legacyPointer = new TimelineEntry(scheduled.messageId(), scheduled.message().generation())
                .encode();
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(scheduled.messageId()),
                        scheduled.message().encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), legacyPointer);
                batch.putValue(ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), legacyPointer);
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertRecoveryInstallIsOnlyNonRuntimeBlocker(inventory, source);
        assertEquals(1, inventory.messageStatuses().get(MessageStatus.SCHEDULED));
    }

    @Test
    void reportsOrphanedTimelineIndexForCurrentScheduledMessage() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("orphan-timeline-store"));
        final Path image = tempDir.resolve("orphan-timeline-checkpoint");
        final byte[] checkpointId = bytes(16, 21);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final ScheduledFixture scheduled = scheduledFixture(shard, source, "orphan-timeline-lane");
        final byte[] orphanKey = KeyCodec.timelineDue(
                scheduled.message().laneId(),
                5_001,
                source.sourceOrderToken(),
                scheduled.messageId(),
                scheduled.message().generation());
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
                        Bytes.u64beBits(1));
                batch.putValue(
                        ColumnFamily.ID, 1, KeyCodec.idMessage(scheduled.messageId()), scheduled.message().encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, scheduled.timelineKey(), scheduled.work().canonicalBytes());
                batch.putValue(ColumnFamily.TIMELINE, 1, scheduled.expiryKey(), scheduled.work().canonicalBytes());
                batch.putValue(ColumnFamily.TIMELINE, 1, orphanKey, scheduled.work().canonicalBytes());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertConflictCountExcludingRuntimeMetadata(inventory, 2);
        final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().stream()
                .filter(item -> item.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.TIMELINE_INDEX_ORPHANED_OR_STALE)
                .findFirst()
                .orElseThrow();
        assertEquals(
                LegacyCheckpointStateInventory.ConflictReason.TIMELINE_INDEX_ORPHANED_OR_STALE,
                conflict.reason());
        assertEquals("TIMELINE", conflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(Bytes.sha256(orphanKey), conflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
    }

    @Test
    void checksAttemptObligationsAndFindsOnlyUnreferencedInflightLedgers() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 8);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("uncertain-retry-store"));
        final Path image = tempDir.resolve("uncertain-retry-checkpoint");
        final byte[] checkpointId = bytes(16, 5);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 31, null, 4_000);
        final UncertainRetryFixture present = uncertainRetryFixture(
                shard, source, "uncertain-retry-present", 40);
        final KafkaSourcePosition missingSource = new KafkaSourcePosition(
                shard, "legacy-cluster", source.nativeTopicUuid(), 32, null, 4_001);
        final UncertainRetryFixture missing = uncertainRetryFixture(
                shard, missingSource, "uncertain-retry-missing", 60);
        final KafkaSourcePosition terminalSource = new KafkaSourcePosition(
                shard, "legacy-cluster", source.nativeTopicUuid(), 33, null, 4_002);
        final UncertainRetryFixture terminal = uncertainRetryFixture(
                shard, terminalSource, "uncertain-retry-terminal", 80);
        final KafkaSourcePosition orphanSource = new KafkaSourcePosition(
                shard, "legacy-cluster", source.nativeTopicUuid(), 37, null, 4_006);
        final UncertainRetryFixture orphan = uncertainRetryFixture(
                shard, orphanSource, "uncertain-retry-orphan", 100);
        final KafkaSourcePosition missingTerminalSource = new KafkaSourcePosition(
                shard, "legacy-cluster", source.nativeTopicUuid(), 35, null, 4_004);
        final ScheduledFixture missingTerminal =
                scheduledFixture(shard, missingTerminalSource, "terminal-without-summary");
        final GenerationRuntimeIndex publishedRuntime = GenerationRuntimeIndex.none(
                GenerationAggregateState.PUBLISHED,
                List.of(terminal.attempt().obligationRef()),
                1,
                0,
                false,
                2);
        final MessageRecord terminalMessage = MessageRecord.current(
                MessageStatus.PUBLISHED,
                1,
                2,
                terminal.scheduled().message().deliverAtEpochMs(),
                terminal.scheduled().message().expireAtEpochMs(),
                terminal.scheduled().message().retryEligibilityAtEpochMs(),
                terminal.scheduled().message().laneId(),
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.FORBID,
                Bytes.utf8("terminal-payload"),
                terminalSource.canonicalBytes(),
                null,
                terminal.scheduled().message().earliestNativeCandidateAtEpochMs(),
                publishedRuntime);
        final GenerationRuntimeIndex canceledRuntime = GenerationRuntimeIndex.none(
                GenerationAggregateState.CANCELED, List.of(), 1, 0, false, 2);
        final MessageRecord canceledWithoutSummary = MessageRecord.current(
                MessageStatus.CANCELED,
                1,
                2,
                missingTerminal.message().deliverAtEpochMs(),
                missingTerminal.message().expireAtEpochMs(),
                missingTerminal.message().retryEligibilityAtEpochMs(),
                missingTerminal.message().laneId(),
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.FORBID,
                Bytes.utf8("canceled-payload"),
                missingTerminalSource.canonicalBytes(),
                null,
                missingTerminal.message().earliestNativeCandidateAtEpochMs(),
                canceledRuntime);
        final KafkaSourcePosition mismatchedTerminalSource = new KafkaSourcePosition(
                shard, "legacy-cluster", source.nativeTopicUuid(), 36, null, 4_005);
        final KafkaSourcePosition futureRetiredSource = orphanSource;
        final DelayMessageId futureRetiredMessageId = DelayMessageId.random(shard);
        final RetiredMessageIdentityRecord futureRetired = new RetiredMessageIdentityRecord(
                futureRetiredMessageId,
                futureRetiredMessageId.routingId().logicalTimestampEpochMs(),
                3,
                futureRetiredSource.canonicalBytes());
        final DelayMessageId terminalBeyondCutMessageId = DelayMessageId.random(shard);
        final TerminalGenerationRecord terminalBeyondCut = new TerminalGenerationRecord(
                terminalBeyondCutMessageId,
                0,
                MessageStatus.CANCELED,
                StableCode.CANCELED,
                1,
                futureRetiredSource.canonicalBytes(),
                false,
                List.of());
        final ScheduledFixture mismatchedTerminal =
                scheduledFixture(shard, mismatchedTerminalSource, "terminal-summary-mismatch");
        final GenerationRuntimeIndex mismatchedPublishedRuntime = GenerationRuntimeIndex.none(
                GenerationAggregateState.PUBLISHED, List.of(), 1, 0, false, 2);
        final MessageRecord mismatchedPublishedMessage = MessageRecord.current(
                MessageStatus.PUBLISHED,
                1,
                2,
                mismatchedTerminal.message().deliverAtEpochMs(),
                mismatchedTerminal.message().expireAtEpochMs(),
                mismatchedTerminal.message().retryEligibilityAtEpochMs(),
                mismatchedTerminal.message().laneId(),
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.FORBID,
                Bytes.utf8("mismatched-terminal-payload"),
                mismatchedTerminalSource.canonicalBytes(),
                null,
                mismatchedTerminal.message().earliestNativeCandidateAtEpochMs(),
                mismatchedPublishedRuntime);
        final TerminalGenerationRecord mismatchedTerminalSummary = new TerminalGenerationRecord(
                mismatchedTerminal.messageId(),
                1,
                MessageStatus.PUBLISHED,
                StableCode.ALREADY_PUBLISHED,
                2,
                mismatchedTerminalSource.canonicalBytes(),
                true,
                List.of());
        final TerminalGenerationRecord terminalSummary = new TerminalGenerationRecord(
                terminal.scheduled().messageId(),
                1,
                MessageStatus.PUBLISHED,
                StableCode.ALREADY_PUBLISHED,
                2,
                terminalSource.canonicalBytes(),
                false,
                List.of(terminal.attempt().obligationRef()));
        final CheckpointManifest manifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                        mismatchedTerminalSource.canonicalBytes());
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(2));
                putUncertainRetry(batch, present);
                putUncertainRetry(batch, missing);
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(terminal.scheduled().messageId()),
                        terminalMessage.encode());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(missingTerminal.messageId()),
                        canceledWithoutSummary.encode());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(mismatchedTerminal.messageId()),
                        mismatchedPublishedMessage.encode());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(futureRetiredMessageId),
                        futureRetired.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        1,
                        KeyCodec.terminalGeneration(terminal.scheduled().messageId(), 1),
                        terminalSummary.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        1,
                        KeyCodec.terminalGeneration(mismatchedTerminal.messageId(), 1),
                        mismatchedTerminalSummary.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        1,
                        KeyCodec.terminalGeneration(terminalBeyondCutMessageId, 0),
                        terminalBeyondCut.encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        present.attempt().encodedKey(),
                        present.attempt().encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        terminal.attempt().encodedKey(),
                        terminal.attempt().encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        orphan.attempt().encodedKey(),
                        orphan.attempt().encode());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, mismatchedTerminalSource);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));

        assertEquals(
                2,
                inventory.messageDispositions().get(
                        LegacyCheckpointStateInventory.MessageDisposition.BLOCKED_PENDING_OLD_SEND_RECOVERY));
        assertEquals(
                2,
                inventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason.SCHEDULED_REQUIRES_OLD_SEND_RECOVERY)
                        .count());
        final LegacyCheckpointStateInventory.Conflict missingLedger = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_OBLIGATION_MISSING)
                .findFirst()
                .orElseThrow();
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(missing.scheduled().messageId())), missingLedger.oldKeyDigest()));
        assertEquals("MESSAGE", missingLedger.recordKind());
        assertTrue(Bytes.constantTimeEquals(missingSource.canonicalBytes(), missingLedger.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict orphanLedger = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_LEDGER_UNREFERENCED)
                .findFirst()
                .orElseThrow();
        assertEquals("ATTEMPT", orphanLedger.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(orphan.attempt().encodedKey()), orphanLedger.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(orphanSource.canonicalBytes(), orphanLedger.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict futureAttemptConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_SOURCE_AFTER_CHECKPOINT)
                .findFirst()
                .orElseThrow();
        assertEquals("ATTEMPT", futureAttemptConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(orphan.attempt().encodedKey()), futureAttemptConflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(orphanSource.canonicalBytes(), futureAttemptConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict missingTerminalSummary = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.TERMINAL_SUMMARY_MISSING)
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", missingTerminalSummary.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(missingTerminal.messageId())),
                missingTerminalSummary.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(
                missingTerminalSource.canonicalBytes(), missingTerminalSummary.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict terminalMismatch = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.TERMINAL_SUMMARY_VALUE_MISMATCH)
                .findFirst()
                .orElseThrow();
        assertEquals("TERMINAL", terminalMismatch.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.terminalGeneration(mismatchedTerminal.messageId(), 1)),
                terminalMismatch.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(
                mismatchedTerminalSource.canonicalBytes(), terminalMismatch.sourcePosition()));
        assertEquals(1, inventory.retiredMessageIdentities());
        final LegacyCheckpointStateInventory.Conflict futureRetiredSourceConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.RETIRED_IDENTITY_SOURCE_AFTER_CHECKPOINT)
                .findFirst()
                .orElseThrow();
        final LegacyCheckpointStateInventory.Conflict futureRetiredSequenceConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.RETIRED_IDENTITY_SEQUENCE_AFTER_CHECKPOINT)
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", futureRetiredSourceConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(futureRetiredMessageId)),
                futureRetiredSourceConflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(
                futureRetiredSource.canonicalBytes(), futureRetiredSourceConflict.sourcePosition()));
        assertEquals("MESSAGE", futureRetiredSequenceConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(futureRetiredMessageId)),
                futureRetiredSequenceConflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(
                futureRetiredSource.canonicalBytes(), futureRetiredSequenceConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict terminalSourceConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.TERMINAL_SOURCE_AFTER_CHECKPOINT)
                .findFirst()
                .orElseThrow();
        assertEquals("TERMINAL", terminalSourceConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.terminalGeneration(terminalBeyondCutMessageId, 0)),
                terminalSourceConflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(
                futureRetiredSource.canonicalBytes(), terminalSourceConflict.sourcePosition()));
        assertConflictCountExcludingRuntimeMetadata(inventory, 14);
    }

    @Test
    void messageDispositionsKeepUnresolvedAndTerminalObligationsInTheLegacyDomain() {
        assertEquals(
                LegacyCheckpointStateInventory.MessageDisposition.BLOCKED_PENDING_CLAIM_RECONCILIATION,
                LegacyCheckpointStateInventory.dispositionForStatus(MessageStatus.CLAIMED));
        assertEquals(
                LegacyCheckpointStateInventory.MessageDisposition.PRESERVE_OLD_SEND_RECOVERY,
                LegacyCheckpointStateInventory.dispositionForStatus(MessageStatus.UNCERTAIN));
        assertEquals(
                LegacyCheckpointStateInventory.MessageDisposition.PRESERVE_BROKER_RESPONSIBILITY,
                LegacyCheckpointStateInventory.dispositionForStatus(MessageStatus.HANDED_OFF));
        assertEquals(
                LegacyCheckpointStateInventory.MessageDisposition.PRESERVE_TERMINAL_AND_REFERENCES,
                LegacyCheckpointStateInventory.dispositionForStatus(MessageStatus.PUBLISHED));
    }

    @Test
    void reportsCanonicalAdmissionAttemptIdentityMismatch() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("admission-join-store"));
        final Path matchingImage = tempDir.resolve("admission-join-matching-checkpoint");
        final Path mismatchedImage = tempDir.resolve("admission-join-mismatched-checkpoint");
        final byte[] matchingCheckpointId = bytes(16, 6);
        final byte[] mismatchedCheckpointId = bytes(16, 7);
        final UUID sourceTopic = UUID.randomUUID();
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", sourceTopic, 41, null, 4_010);
        final KafkaSourcePosition malformedSource = new KafkaSourcePosition(
                shard, "legacy-cluster", sourceTopic, 42, null, 4_011);
        final UncertainRetryFixture matching = canonicalUncertainRetryFixture(
                shard, source, "canonical-admission-join", 120);
        final UncertainRetryFixture opaqueMalformedBase = uncertainRetryFixture(
                shard, malformedSource, "malformed-admission-join", 124);
        final PublishAttemptLedger malformedAttempt = PublishAttemptLedger.publishing(
                        opaqueMalformedBase.attempt().delayMessageId(),
                        opaqueMalformedBase.attempt().generation(),
                        opaqueMalformedBase.attempt().publishAttemptId(),
                        opaqueMalformedBase.attempt().claimId(),
                        opaqueMalformedBase.attempt().ownerEpoch(),
                        opaqueMalformedBase.attempt().attemptNo(),
                        opaqueMalformedBase.attempt().laneId(),
                        opaqueMalformedBase.attempt().laneIncarnation(),
                        opaqueMalformedBase.attempt().ownerIdentity(),
                        opaqueMalformedBase.attempt().storeIncarnation(),
                        opaqueMalformedBase.attempt().preparedPublishHash(),
                        new byte[] {0x0a},
                        opaqueMalformedBase.attempt().sourcePosition())
                .withUnknownOutcome(
                        opaqueMalformedBase.attempt().outcomeBytes(),
                        opaqueMalformedBase.attempt().evidenceBytes(),
                        opaqueMalformedBase.attempt().sourcePosition());
        final UncertainRetryFixture malformed = new UncertainRetryFixture(
                opaqueMalformedBase.scheduled(), opaqueMalformedBase.message(), opaqueMalformedBase.work(),
                malformedAttempt);
        final PublishAttemptLedger original = matching.attempt();
        final PublishAttemptLedger mismatchedAttempt = PublishAttemptLedger.publishing(
                        original.delayMessageId(),
                        original.generation(),
                        original.publishAttemptId(),
                        bytes(32, 240),
                        original.ownerEpoch(),
                        original.attemptNo(),
                        original.laneId(),
                        original.laneIncarnation(),
                        original.ownerIdentity(),
                        original.storeIncarnation(),
                        original.preparedPublishHash(),
                        original.admissionBytes(),
                        original.sourcePosition())
                .withUnknownOutcome(original.outcomeBytes(), original.evidenceBytes(), original.sourcePosition());
        assertTrue(Bytes.constantTimeEquals(
                original.obligationRef().canonicalBytes(), mismatchedAttempt.obligationRef().canonicalBytes()));
        final UncertainRetryFixture mismatched = new UncertainRetryFixture(
                matching.scheduled(), matching.message(), matching.work(), mismatchedAttempt);
        final CheckpointManifest matchingManifest;
        final CheckpointManifest mismatchedManifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                        malformedSource.canonicalBytes());
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(1));
                putUncertainRetry(batch, mismatched);
                putUncertainRetry(batch, malformed);
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        original.encodedKey(),
                        original.encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        malformedAttempt.encodedKey(),
                        malformedAttempt.encode());
            });
            store.createCheckpoint(matchingImage, matchingCheckpointId);
            matchingManifest = manifestFor(matchingImage, shard, store, matchingCheckpointId, malformedSource);
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(2));
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        mismatchedAttempt.encodedKey(),
                        mismatchedAttempt.encode());
            });
            store.createCheckpoint(mismatchedImage, mismatchedCheckpointId);
            mismatchedManifest = manifestFor(mismatchedImage, shard, store, mismatchedCheckpointId, malformedSource);
        }

        final LegacyCheckpointStateInventory.Inventory matchingInventory = LegacyCheckpointStateInventory.inspect(
                matchingImage,
                shard,
                matchingManifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        assertEquals(
                0,
                matchingInventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_ADMISSION_LEDGER_MISMATCH)
                        .count());
        assertEquals(
                1,
                matchingInventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_ADMISSION_MALFORMED)
                        .count());

        final LegacyCheckpointStateInventory.Inventory mismatchedInventory = LegacyCheckpointStateInventory.inspect(
                mismatchedImage,
                shard,
                mismatchedManifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));

        final LegacyCheckpointStateInventory.Conflict mismatch = mismatchedInventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_ADMISSION_LEDGER_MISMATCH)
                .findFirst()
                .orElseThrow();
        assertEquals("ATTEMPT", mismatch.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(mismatchedAttempt.encodedKey()), mismatch.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), mismatch.sourcePosition()));
        assertEquals(
                0,
                mismatchedInventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_OBLIGATION_VALUE_MISMATCH)
                        .count());
        assertEquals(
                1,
                mismatchedInventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason.ATTEMPT_ADMISSION_MALFORMED)
                        .count());
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

    private static ScheduledFixture scheduledFixture(
            final ShardId shard, final KafkaSourcePosition source, final String laneTuple) {
        return scheduledFixture(shard, source, laneTuple, 1);
    }

    private static ScheduledFixture scheduledFixture(
            final ShardId shard,
            final KafkaSourcePosition source,
            final String laneTuple,
            final int generation) {
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8(laneTuple));
        final byte[] timelineKey = KeyCodec.timelineDue(
                lane, 5_000, source.sourceOrderToken(), messageId, generation);
        final TimelineWorkRef work = TimelineWorkRef.initial(timelineKey, 5_000, 1);
        final GenerationRuntimeIndex runtime = GenerationRuntimeIndex.timeline(
                GenerationAggregateState.SCHEDULED, work, 1);
        final MessageRecord message = MessageRecord.current(
                MessageStatus.SCHEDULED,
                generation,
                1,
                5_000,
                8_000,
                5_000,
                lane,
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.FORBID,
                Bytes.utf8("payload"),
                source.canonicalBytes(),
                null,
                5_000,
                runtime);
        final byte[] expiryKey = KeyCodec.timelineExpiry(8_000, lane, messageId, generation);
        return new ScheduledFixture(messageId, message, work, timelineKey, expiryKey);
    }

    private static UncertainRetryFixture uncertainRetryFixture(
            final ShardId shard,
            final KafkaSourcePosition source,
            final String laneTuple,
            final int attemptSeed) {
        final ScheduledFixture scheduled = scheduledFixture(shard, source, laneTuple);
        final byte[] attemptId = bytes(32, attemptSeed);
        final PublishAttemptLedger attempt = PublishAttemptLedger.publishing(
                        scheduled.messageId(),
                        1,
                        attemptId,
                        bytes(32, attemptSeed + 1),
                        9,
                        1,
                        scheduled.message().laneId(),
                        bytes(16, attemptSeed + 2),
                        new byte[] {(byte) attemptSeed},
                        bytes(16, attemptSeed + 3),
                        bytes(32, attemptSeed + 4),
                        new byte[] {(byte) (attemptSeed + 5)},
                        source.canonicalBytes())
                .withUnknownOutcome(Bytes.utf8("unknown"), Bytes.utf8("evidence"), source.canonicalBytes());
        return uncertainRetryFixture(scheduled, attempt, source);
    }

    private static UncertainRetryFixture canonicalUncertainRetryFixture(
            final ShardId shard,
            final KafkaSourcePosition source,
            final String laneTuple,
            final int attemptSeed) {
        final ScheduledFixture scheduled = scheduledFixture(shard, source, laneTuple, 0);
        final byte[] laneIncarnation = bytes(16, attemptSeed + 2);
        final PublishAdmissionBodyTest.Fixture bodyFixture = PublishAdmissionBodyTest.Fixture.createForSourceWithLane(
                shard,
                scheduled.messageId(),
                laneIncarnation,
                scheduled.timelineKey(),
                TimelineWorkKind.UNCERTAIN_RETRY.wireValue(),
                2,
                2,
                Bytes.sha256(Bytes.utf8("canonical-admission-obligations")),
                Bytes.sha256(Bytes.utf8("canonical-admission-semantics")),
                scheduled.message().laneId().bytes());
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(bodyFixture.body());
        final PublishAttemptLedger attempt = PublishAttemptLedger.publishing(
                        scheduled.messageId(),
                        admission.generation(),
                        admission.publishAttemptId(),
                        admission.claimId(),
                        OwnerIdentity.decode(admission.ownerIdentity()).ownerEpoch(),
                        admission.descriptor().attemptNo(),
                        new DestinationLaneId(admission.laneId()),
                        admission.laneIncarnation(),
                        admission.ownerIdentity(),
                        admission.storeIncarnation(),
                        admission.preparedPublishHash(),
                        bodyFixture.body(),
                        source.canonicalBytes())
                .withUnknownOutcome(
                        Bytes.utf8("canonical-unknown"),
                        Bytes.utf8("canonical-evidence"),
                        source.canonicalBytes());
        return uncertainRetryFixture(scheduled, attempt, source);
    }

    private static UncertainRetryFixture uncertainRetryFixture(
            final ScheduledFixture scheduled,
            final PublishAttemptLedger attempt,
            final KafkaSourcePosition source) {
        final TimelineWorkRef retryWork = new TimelineWorkRef(
                TimelineWorkKind.UNCERTAIN_RETRY,
                scheduled.timelineKey(),
                5_000,
                5_000,
                2,
                2,
                false,
                UncertainRetryAuthority.PINNED_POLICY,
                null,
                null);
        final GenerationRuntimeIndex runtime = GenerationRuntimeIndex.timeline(
                GenerationAggregateState.UNCERTAIN, retryWork, List.of(attempt.obligationRef()), 1, 0, false, 2);
        final MessageRecord message = MessageRecord.current(
                MessageStatus.SCHEDULED,
                scheduled.message().generation(),
                2,
                5_000,
                8_000,
                scheduled.message().laneId(),
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload"),
                source.canonicalBytes(),
                null,
                5_000,
                runtime);
        return new UncertainRetryFixture(scheduled, message, retryWork, attempt);
    }

    private static void putUncertainRetry(
            final ShardStore.Batch batch, final UncertainRetryFixture fixture) throws RocksDBException {
        batch.putValue(
                ColumnFamily.ID,
                1,
                KeyCodec.idMessage(fixture.scheduled().messageId()),
                fixture.message().encode());
        batch.putValue(
                ColumnFamily.TIMELINE,
                1,
                fixture.scheduled().timelineKey(),
                fixture.work().canonicalBytes());
        batch.putValue(
                ColumnFamily.TIMELINE,
                1,
                fixture.scheduled().expiryKey(),
                fixture.work().canonicalBytes());
    }

    private record ScheduledFixture(
            DelayMessageId messageId,
            MessageRecord message,
            TimelineWorkRef work,
            byte[] timelineKey,
            byte[] expiryKey) {}

    private record ExpectedLegacyBlocker(
            byte[] key, String recordKind, LegacyCheckpointStateInventory.ConflictReason reason) {}

    private record ExpectedCheckpoint(Path image, CheckpointManifest manifest) {}

    private record UncertainRetryFixture(
            ScheduledFixture scheduled, MessageRecord message, TimelineWorkRef work, PublishAttemptLedger attempt) {}

    private static void assertFileInventoriesEqual(
            final List<CheckpointFileInventory> expected, final List<CheckpointFileInventory> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).name(), actual.get(index).name());
            assertEquals(expected.get(index).length(), actual.get(index).length());
            assertTrue(Bytes.constantTimeEquals(expected.get(index).checksum(), actual.get(index).checksum()));
        }
    }

    private static void assertRecoveryInstallIsOnlyNonRuntimeBlocker(
            final LegacyCheckpointStateInventory.Inventory inventory, final KafkaSourcePosition source) {
        assertConflictCountExcludingRuntimeMetadata(inventory, 1);
        final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().stream()
                .filter(item -> item.recordKind().equals("RECOVERY_METADATA"))
                .findFirst()
                .orElseThrow();
        assertEquals("RECOVERY_METADATA", conflict.recordKind());
        assertEquals(
                LegacyCheckpointStateInventory.ConflictReason.LEGACY_NAMESPACE_UNAUDITED, conflict.reason());
        assertTrue(Bytes.constantTimeEquals(Bytes.sha256(KeyCodec.metaRecovery(4)), conflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
    }

    private static void assertConflictCountExcludingRuntimeMetadata(
            final LegacyCheckpointStateInventory.Inventory inventory, final long expected) {
        assertEquals(
                expected,
                inventory.conflicts().stream()
                        .filter(conflict -> !conflict.recordKind().equals("RUNTIME_METADATA"))
                        .count());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
