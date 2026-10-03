package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ClaimResultBody;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.ClaimRecordTestSupport;
import com.nereusstream.delay.runtime.GenerationRuntimeIndex;
import com.nereusstream.delay.runtime.MessageRecord;
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
        final DestinationLaneId staleLane = DestinationLaneId.derive(Bytes.utf8("claim-audit-stale"));
        final DestinationLaneId staleObligationLane =
                DestinationLaneId.derive(Bytes.utf8("claim-audit-stale-obligation"));
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
        final PreparedCommand staleSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        staleLane, 2_300, 8_300, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-audit-stale")),
                9_000);
        final PreparedCommand staleObligationSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        staleObligationLane,
                        2_400,
                        8_400,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-audit-stale-obligation")),
                9_000);
        final KafkaSourcePosition validSource = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 40, null, 4_000);
        final KafkaSourcePosition missingSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 41, null, 4_001);
        final KafkaSourcePosition orphanSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 42, null, 4_002);
        final KafkaSourcePosition staleSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 43, null, 4_003);
        final KafkaSourcePosition staleObligationSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 44, null, 4_004);
        final AuthorIdentity owner = AuthorIdentity.owner(
                Bytes.utf8("claim-audit-deployment"),
                Bytes.utf8("claim-audit-worker"),
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("claim-audit-fence")));
        final CheckpointManifest manifest;
        final ClaimRecord orphanClaim;
        final ClaimRecord staleClaim;
        final ClaimRecord stalePreconditionClaim;
        final ClaimRecord staleObligationClaim;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            final ClaimRecord validClaim = ClaimRecordTestSupport.claimScheduled(
                    store, validSchedule, validSource, validLane, owner, 2_500, chargeVector());
            final ClaimRecord missingClaim = ClaimRecordTestSupport.claimScheduled(
                    store, missingSchedule, missingSource, missingLane, owner, 2_500, chargeVector());
            orphanClaim = ClaimRecordTestSupport.claimScheduled(
                    store, orphanSchedule, orphanSource, orphanLane, owner, 2_500, chargeVector());
            staleClaim = ClaimRecordTestSupport.claimScheduled(
                    store, staleSchedule, staleSource, staleLane, owner, 2_500, chargeVector());
            final ClaimRecord staleObligationSourceClaim = ClaimRecordTestSupport.claimScheduled(
                    store,
                    staleObligationSchedule,
                    staleObligationSource,
                    staleObligationLane,
                    owner,
                    2_500,
                    chargeVector());
            final ClaimResultBody.ClaimPrecondition validPrecondition =
                    ClaimResultBody.decodePrecondition(validClaim.preconditionBytes());
            stalePreconditionClaim = withPrecondition(
                    validClaim,
                    preconditionWithAdmissionsUsed(
                            validPrecondition, Math.addExact(validPrecondition.expectedAdmissionsUsed(), 1)));
            final ClaimResultBody.ClaimPrecondition obligationPrecondition =
                    ClaimResultBody.decodePrecondition(staleObligationSourceClaim.preconditionBytes());
            staleObligationClaim = withPrecondition(
                    staleObligationSourceClaim,
                    preconditionWithObligationSetDigest(obligationPrecondition, bytes(ClaimRecord.HASH_LENGTH, 119)));
            final MessageRecord currentStaleMessage = MessageRecord.decode(store.getValue(
                            ColumnFamily.ID, KeyCodec.idMessage(staleSchedule.delayMessageId()), 1)
                    .payload());
            final long staleStateVersion = Math.addExact(currentStaleMessage.stateVersion(), 1);
            final GenerationRuntimeIndex staleRuntime = GenerationRuntimeIndex.claimed(
                    staleClaim.claimId(),
                    currentStaleMessage.runtimeIndex().attemptObligations(),
                    currentStaleMessage.runtimeIndex().admissionsUsed(),
                    currentStaleMessage.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    currentStaleMessage.runtimeIndex().possibleDestinationDuplicate(),
                    staleStateVersion);
            final MessageRecord staleMessage = new MessageRecord(
                    currentStaleMessage.status(),
                    currentStaleMessage.generation(),
                    staleStateVersion,
                    currentStaleMessage.deliverAtEpochMs(),
                    currentStaleMessage.expireAtEpochMs(),
                    currentStaleMessage.earliestNativeCandidateAtEpochMs(),
                    currentStaleMessage.laneId(),
                    currentStaleMessage.orderingMode(),
                    currentStaleMessage.nativeDeliveryPolicy(),
                    currentStaleMessage.payload(),
                    currentStaleMessage.scheduleSourcePosition(),
                    currentStaleMessage.payloadReference(),
                    currentStaleMessage.retryEligibilityAtEpochMs(),
                    staleRuntime,
                    currentStaleMessage.legacyEncoding());
            final byte[] mismatchedOrphanClaimKey =
                    KeyCodec.inflight((byte) 1, orphanClaim.ownerEpoch(), bytes(ClaimRecord.HASH_LENGTH, 93));
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, missingClaim.encodedKey());
                batch.delete(ColumnFamily.ID, KeyCodec.idMessage(orphanSchedule.delayMessageId()));
                batch.delete(ColumnFamily.INFLIGHT, orphanClaim.encodedKey());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        stalePreconditionClaim.encodedKey(),
                        stalePreconditionClaim.encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        staleObligationClaim.encodedKey(),
                        staleObligationClaim.encode());
                batch.putValue(
                        ColumnFamily.ID,
                        1,
                        KeyCodec.idMessage(staleSchedule.delayMessageId()),
                        staleMessage.encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        mismatchedOrphanClaimKey,
                        orphanClaim.encode());
            });
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, staleObligationSource);
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
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.idMessage(missingSchedule.delayMessageId())),
                        conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", missingRecord.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                Bytes.sha256(KeyCodec.idMessage(missingSchedule.delayMessageId())), missingRecord.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(missingSource.canonicalBytes(), missingRecord.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict stalePreconditionConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(stalePreconditionClaim.encodedKey()), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", stalePreconditionConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(validSource.canonicalBytes(), stalePreconditionConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict stalePreconditionMessageMissing = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.idMessage(validSchedule.delayMessageId())), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", stalePreconditionMessageMissing.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                validSource.canonicalBytes(), stalePreconditionMessageMissing.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict staleObligationConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(staleObligationClaim.encodedKey()), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", staleObligationConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                staleObligationSource.canonicalBytes(), staleObligationConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict staleObligationMessageMissing = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.idMessage(staleObligationSchedule.delayMessageId())),
                        conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", staleObligationMessageMissing.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                staleObligationSource.canonicalBytes(), staleObligationMessageMissing.sourcePosition()));
        final byte[] mismatchedOrphanClaimKey =
                KeyCodec.inflight((byte) 1, orphanClaim.ownerEpoch(), bytes(ClaimRecord.HASH_LENGTH, 93));
        final LegacyCheckpointStateInventory.Conflict orphanRecord = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(mismatchedOrphanClaimKey), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", orphanRecord.recordKind());
        assertTrue(Bytes.constantTimeEquals(Bytes.sha256(mismatchedOrphanClaimKey), orphanRecord.oldKeyDigest()));
        assertTrue(Bytes.constantTimeEquals(staleObligationSource.canonicalBytes(), orphanRecord.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict staleClaimConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(staleClaim.encodedKey()), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", staleClaimConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(staleSource.canonicalBytes(), staleClaimConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict staleMessageClaimMissing = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.idMessage(staleSchedule.delayMessageId())), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", staleMessageClaimMissing.recordKind());
        assertTrue(Bytes.constantTimeEquals(staleSource.canonicalBytes(), staleMessageClaimMissing.sourcePosition()));
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

    private static byte[] preconditionWithAdmissionsUsed(
            final ClaimResultBody.ClaimPrecondition precondition, final int admissionsUsed) {
        return encodePrecondition(precondition, admissionsUsed, precondition.expectedObligationSetDigest());
    }

    private static ClaimRecord withPrecondition(final ClaimRecord claim, final byte[] preconditionBytes) {
        return ClaimRecord.claimed(
                claim.delayMessageId(),
                claim.generation(),
                claim.claimId(),
                claim.ownerEpoch(),
                claim.claimSequence(),
                claim.laneId(),
                claim.laneIncarnation(),
                claim.laneControlVersion(),
                claim.runtimeLaneVersion(),
                claim.ownerIdentity(),
                claim.storeIncarnation(),
                preconditionBytes,
                claim.timelineKey(),
                claim.runtimeRevision(),
                claim.sourceTimelineWork());
    }

    private static byte[] preconditionWithObligationSetDigest(
            final ClaimResultBody.ClaimPrecondition precondition, final byte[] obligationSetDigest) {
        return encodePrecondition(precondition, precondition.expectedAdmissionsUsed(), obligationSetDigest);
    }

    private static byte[] encodePrecondition(
            final ClaimResultBody.ClaimPrecondition precondition,
            final int admissionsUsed,
            final byte[] obligationSetDigest) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, precondition.claimId());
            CanonicalProtobuf.bytes(output, 2, precondition.messageId());
            CanonicalProtobuf.uint32Bits(output, 3, precondition.generation());
            CanonicalProtobuf.int64(output, 4, precondition.stateVersion());
            CanonicalProtobuf.bytes(output, 5, precondition.destinationLaneId());
            CanonicalProtobuf.bytes(output, 6, precondition.laneIncarnation());
            CanonicalProtobuf.int64(output, 7, precondition.laneControlVersion());
            CanonicalProtobuf.int64(output, 8, precondition.runtimeLaneVersion());
            CanonicalProtobuf.bytes(output, 9, precondition.originalTimelineKeySha256());
            if (precondition.hasMaterialization()) {
                CanonicalProtobuf.bytes(output, 10, precondition.materialization());
                CanonicalProtobuf.bytes(
                        output, 11, precondition.materializationValue().materializationDigest());
            }
            CanonicalProtobuf.bytes(output, 12, precondition.claimedCharge());
            CanonicalProtobuf.int64(output, 13, precondition.claimDeadline());
            CanonicalProtobuf.bytes(output, 14, precondition.ownerIdentity());
            CanonicalProtobuf.bytes(output, 15, precondition.storeIncarnation());
            CanonicalProtobuf.uint32(output, 16, precondition.sourceWorkKind());
            CanonicalProtobuf.uint32(output, 17, admissionsUsed);
            CanonicalProtobuf.uint32(output, 18, precondition.expectedUncertainRetryAdmissionsUsed());
            CanonicalProtobuf.bytes(output, 19, obligationSetDigest);
            CanonicalProtobuf.bytes(output, 20, precondition.sourceTimelineSemanticDigest());
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
