package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.ClaimRecordTestSupport;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LegacyCheckpointClaimInventoryTest {
    @TempDir
    Path tempDir;

    @Test
    void auditsClaimRecordsAgainstCurrentMessageOwnership() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-audit-store"));
        final Path image = tempDir.resolve("claim-audit-checkpoint");
        final byte[] checkpointId = bytes(16, 41);
        final DestinationLaneId validLane = DestinationLaneId.derive(Bytes.utf8("claim-audit-valid"));
        final DestinationLaneId missingLane = DestinationLaneId.derive(Bytes.utf8("claim-audit-missing"));
        final DestinationLaneId orphanLane = DestinationLaneId.derive(Bytes.utf8("claim-audit-orphan"));
        final PreparedCommand validSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        validLane, 2_000, 8_000, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-audit-valid")),
                9_000);
        final PreparedCommand missingSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        missingLane, 2_100, 8_100, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-audit-missing")),
                9_000);
        final PreparedCommand orphanSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        orphanLane, 2_200, 8_200, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-audit-orphan")),
                9_000);
        final KafkaSourcePosition validSource = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 40, null, 4_000);
        final KafkaSourcePosition missingSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 41, null, 4_001);
        final KafkaSourcePosition orphanSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 42, null, 4_002);
        final AuthorIdentity owner = AuthorIdentity.owner(
                Bytes.utf8("claim-audit-deployment"),
                Bytes.utf8("claim-audit-worker"),
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("claim-audit-fence")));
        final CheckpointManifest manifest;
        final ClaimRecord orphanClaim;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            final ClaimRecord validClaim = ClaimRecordTestSupport.claimScheduled(
                    store, validSchedule, validSource, validLane, owner, 2_500, chargeVector());
            final ClaimRecord missingClaim = ClaimRecordTestSupport.claimScheduled(
                    store, missingSchedule, missingSource, missingLane, owner, 2_500, chargeVector());
            orphanClaim = ClaimRecordTestSupport.claimScheduled(
                    store, orphanSchedule, orphanSource, orphanLane, owner, 2_500, chargeVector());
            final byte[] mismatchedOrphanClaimKey =
                    KeyCodec.inflight((byte) 1, orphanClaim.ownerEpoch(), bytes(ClaimRecord.HASH_LENGTH, 93));
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, missingClaim.encodedKey());
                batch.delete(ColumnFamily.ID, KeyCodec.idMessage(orphanSchedule.delayMessageId()));
                batch.delete(ColumnFamily.INFLIGHT, orphanClaim.encodedKey());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        mismatchedOrphanClaimKey,
                        orphanClaim.encode());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, orphanSource);
            assertTrue(store.getValue(ColumnFamily.INFLIGHT, validClaim.encodedKey(), ClaimRecord.VALUE_TYPE) != null);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        final LegacyCheckpointStateInventory.Conflict missingRecord = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", missingRecord.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(missingSchedule.delayMessageId())), missingRecord.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(missingSource.canonicalBytes(), missingRecord.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict orphanRecord = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", orphanRecord.recordKind());
        final byte[] mismatchedOrphanClaimKey =
                KeyCodec.inflight((byte) 1, orphanClaim.ownerEpoch(), bytes(ClaimRecord.HASH_LENGTH, 93));
        assertTrue(Bytes.constantTimeEquals(Bytes.sha256(mismatchedOrphanClaimKey), orphanRecord.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(orphanSource.canonicalBytes(), orphanRecord.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict keyValueMismatch = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_KEY_VALUE_MISMATCH)
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", keyValueMismatch.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(mismatchedOrphanClaimKey), keyValueMismatch.oldKeyDigest()));
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

    private static byte[] chargeVector() {
        return com.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
