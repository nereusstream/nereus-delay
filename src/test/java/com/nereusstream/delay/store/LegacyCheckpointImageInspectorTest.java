package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.GenerationRuntimeIndex;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.TimelineWorkRef;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
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
        assertTrue(state.conflicts().isEmpty());
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
        assertEquals(1, inventory.conflicts().size());
        final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().get(0);
        assertEquals(LegacyCheckpointStateInventory.ConflictReason.EXPIRY_INDEX_MISSING, conflict.reason());
        assertEquals("MESSAGE", conflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(scheduled.messageId())), conflict.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
    }

    @Test
    void checksOpenScheduledAttemptObligationsAgainstTheirInflightLedgers() throws Exception {
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
        final CheckpointManifest manifest;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            store.write(batch -> {
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_APPLIED_SOURCE_POSITION),
                        missingSource.canonicalBytes());
                batch.putValue(
                        ColumnFamily.META,
                        ShardStore.META_FIXED_VALUE_TYPE,
                        KeyCodec.metaFixed(ShardStore.META_MUTATION_SEQUENCE),
                        Bytes.u64beBits(2));
                putUncertainRetry(batch, present);
                putUncertainRetry(batch, missing);
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        PublishAttemptLedger.VALUE_TYPE,
                        present.attempt().encodedKey(),
                        present.attempt().encode());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, missingSource);
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
        assertTrue(Bytes.constantTimeEquals(missingSource.canonicalBytes(), missingLedger.sourcePosition()));
        assertEquals(3, inventory.conflicts().size());
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
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8(laneTuple));
        final byte[] timelineKey = KeyCodec.timelineDue(
                lane, 5_000, source.sourceOrderToken(), messageId, 1);
        final TimelineWorkRef work = TimelineWorkRef.initial(timelineKey, 5_000, 1);
        final GenerationRuntimeIndex runtime = GenerationRuntimeIndex.timeline(
                GenerationAggregateState.SCHEDULED, work, 1);
        final MessageRecord message = MessageRecord.current(
                MessageStatus.SCHEDULED,
                1,
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
        final byte[] expiryKey = KeyCodec.timelineExpiry(8_000, lane, messageId, 1);
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
                1,
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

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
