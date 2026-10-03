package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ClaimResultBody;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.LaneRecordEnvelope;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.ClaimRecordTestSupport;
import com.nereusstream.delay.runtime.GenerationRuntimeIndex;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.TimelineWorkRef;
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
        final DestinationLaneId stalePreconditionVersionLane =
                DestinationLaneId.derive(Bytes.utf8("claim-audit-stale-precondition-version"));
        final DestinationLaneId laneIdentityLane =
                DestinationLaneId.derive(Bytes.utf8("claim-audit-lane-identity"));
        final DestinationLaneId laneStateLane = DestinationLaneId.derive(Bytes.utf8("claim-audit-lane-state"));
        final DestinationLaneId sourceTimelineLane =
                DestinationLaneId.derive(Bytes.utf8("claim-audit-source-timeline"));
        final DestinationLaneId mismatchedLaneKey =
                DestinationLaneId.derive(Bytes.utf8("claim-audit-mismatched-lane-key"));
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
        final PreparedCommand stalePreconditionVersionSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        stalePreconditionVersionLane,
                        2_500,
                        8_500,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-audit-stale-precondition-version")),
                9_000);
        final PreparedCommand laneIdentitySchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        laneIdentityLane,
                        2_600,
                        8_600,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-audit-lane-identity")),
                9_000);
        final PreparedCommand laneStateSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        laneStateLane,
                        2_700,
                        8_700,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-audit-lane-state")),
                9_000);
        final PreparedCommand sourceTimelineSchedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        sourceTimelineLane,
                        2_800,
                        8_800,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-audit-source-timeline")),
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
        final KafkaSourcePosition stalePreconditionVersionSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 45, null, 4_005);
        final KafkaSourcePosition laneIdentitySource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 46, null, 4_006);
        final KafkaSourcePosition laneStateSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 47, null, 4_007);
        final KafkaSourcePosition sourceTimelineSource = new KafkaSourcePosition(
                shard, "legacy-cluster", validSource.nativeTopicUuid(), 48, null, 4_008);
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
        final ClaimRecord stalePreconditionVersionClaim;
        final ClaimRecord mismatchedLaneClaim;
        final ClaimRecord staleLaneStateClaim;
        final ClaimRecord mismatchedSourceTimelineClaim;
        final LaneRecord mismatchedLaneState;
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
            final ClaimRecord stalePreconditionVersionSourceClaim = ClaimRecordTestSupport.claimScheduled(
                    store,
                    stalePreconditionVersionSchedule,
                    stalePreconditionVersionSource,
                    stalePreconditionVersionLane,
                    owner,
                    2_500,
                    chargeVector());
            final ClaimResultBody.ClaimPrecondition versionPrecondition =
                    ClaimResultBody.decodePrecondition(stalePreconditionVersionSourceClaim.preconditionBytes());
            stalePreconditionVersionClaim = withPrecondition(
                    stalePreconditionVersionSourceClaim,
                    preconditionWithStateVersion(
                            versionPrecondition, Math.addExact(versionPrecondition.stateVersion(), 1)));
            final ClaimRecord laneIdentitySourceClaim = ClaimRecordTestSupport.claimScheduled(
                    store, laneIdentitySchedule, laneIdentitySource, laneIdentityLane, owner, 2_500, chargeVector());
            final DestinationLaneId mismatchedClaimLane =
                    DestinationLaneId.derive(Bytes.utf8("claim-audit-other-claim-lane"));
            final byte[] mismatchedTimelineKey = timelineKeyWithLane(laneIdentitySourceClaim, mismatchedClaimLane);
            final ClaimResultBody.ClaimPrecondition lanePrecondition =
                    ClaimResultBody.decodePrecondition(laneIdentitySourceClaim.preconditionBytes());
            mismatchedLaneClaim = withPrecondition(
                    laneIdentitySourceClaim,
                    preconditionWithLaneIdentity(lanePrecondition, mismatchedClaimLane, mismatchedTimelineKey),
                    mismatchedClaimLane,
                    mismatchedTimelineKey,
                    null);
            staleLaneStateClaim = ClaimRecordTestSupport.claimScheduled(
                    store, laneStateSchedule, laneStateSource, laneStateLane, owner, 2_500, chargeVector());
            final ClaimRecord sourceTimelineSourceClaim = ClaimRecordTestSupport.claimScheduled(
                    store,
                    sourceTimelineSchedule,
                    sourceTimelineSource,
                    sourceTimelineLane,
                    owner,
                    2_900,
                    chargeVector());
            final TimelineWorkRef sourceTimelineWork =
                    TimelineWorkRef.decode(sourceTimelineSourceClaim.sourceTimelineWork());
            final byte[] wrongSourceOrderToken = sourceTimelineSource.sourceOrderToken();
            wrongSourceOrderToken[wrongSourceOrderToken.length - 1] ^= 1;
            final byte[] wrongSourceTimelineKey = KeyCodec.timelineDue(
                    sourceTimelineLane,
                    Math.max(
                            sourceTimelineWork.actionAtEpochMs(), sourceTimelineWork.retryEligibilityAtEpochMs()),
                    wrongSourceOrderToken,
                    sourceTimelineSourceClaim.delayMessageId(),
                    sourceTimelineSourceClaim.generation());
            final TimelineWorkRef wrongSourceTimelineWork = new TimelineWorkRef(
                    sourceTimelineWork.workKind(),
                    wrongSourceTimelineKey,
                    sourceTimelineWork.actionAtEpochMs(),
                    sourceTimelineWork.retryEligibilityAtEpochMs(),
                    sourceTimelineWork.candidateAttemptNo(),
                    sourceTimelineWork.runtimeRevision(),
                    sourceTimelineWork.orderedHeadBlocking(),
                    sourceTimelineWork.uncertainRetryAuthority(),
                    sourceTimelineWork.uncertainRetryControl(),
                    sourceTimelineWork.uncertainRetryControlPosition());
            mismatchedSourceTimelineClaim = withSourceTimeline(
                    sourceTimelineSourceClaim,
                    wrongSourceTimelineKey,
                    wrongSourceTimelineWork.canonicalBytes());
            final LaneRecordEnvelope currentLaneEnvelope = LaneRecordEnvelope.decode(store.getValue(
                            ColumnFamily.META, KeyCodec.metaLane(laneStateLane), 2)
                    .payload());
            final LaneRecord currentLane = LaneRecord.decode(currentLaneEnvelope.activeStateBytes());
            mismatchedLaneState = new LaneRecord(
                    currentLane.laneId(),
                    bytes(16, 149),
                    Math.addExact(currentLane.laneControlVersion(), 1),
                    currentLane.laneVersion(),
                    currentLane.admissionGate(),
                    currentLane.runtimeReadiness(),
                    currentLane.weight(),
                    currentLane.nextEligibleAtEpochMs());
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
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        stalePreconditionVersionClaim.encodedKey(),
                        stalePreconditionVersionClaim.encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        mismatchedSourceTimelineClaim.encodedKey(),
                        mismatchedSourceTimelineClaim.encode());
                batch.putValue(
                        ColumnFamily.INFLIGHT,
                        ClaimRecord.VALUE_TYPE,
                        mismatchedLaneClaim.encodedKey(),
                        mismatchedLaneClaim.encode());
                batch.putValue(
                        ColumnFamily.META,
                        2,
                        KeyCodec.metaLane(laneStateLane),
                        LaneRecordEnvelope.active(mismatchedLaneState.encode()).canonicalBytes());
                batch.putValue(
                        ColumnFamily.META,
                        2,
                        KeyCodec.metaLane(mismatchedLaneKey),
                        LaneRecordEnvelope.active(mismatchedLaneState.encode()).canonicalBytes());
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
            manifest = manifestFor(image, shard, store, checkpointId, sourceTimelineSource);
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
        final LegacyCheckpointStateInventory.Conflict stalePreconditionVersionConflict =
                inventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason
                                        .CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                        .filter(conflict -> Bytes.constantTimeEquals(
                                Bytes.sha256(stalePreconditionVersionClaim.encodedKey()), conflict.oldKeyDigest()))
                        .findFirst()
                        .orElseThrow();
        assertEquals("CLAIM", stalePreconditionVersionConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                stalePreconditionVersionSource.canonicalBytes(), stalePreconditionVersionConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict stalePreconditionVersionMessageMissing =
                inventory.conflicts().stream()
                        .filter(conflict -> conflict.reason()
                                == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                        .filter(conflict -> Bytes.constantTimeEquals(
                                Bytes.sha256(KeyCodec.idMessage(stalePreconditionVersionSchedule.delayMessageId())),
                                conflict.oldKeyDigest()))
                        .findFirst()
                        .orElseThrow();
        assertEquals("MESSAGE", stalePreconditionVersionMessageMissing.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                stalePreconditionVersionSource.canonicalBytes(),
                stalePreconditionVersionMessageMissing.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict mismatchedLaneConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_NOT_REPRESENTED_BY_CURRENT_MESSAGE)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(mismatchedLaneClaim.encodedKey()), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", mismatchedLaneConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                laneIdentitySource.canonicalBytes(), mismatchedLaneConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict mismatchedLaneMessageMissing = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.idMessage(laneIdentitySchedule.delayMessageId())),
                        conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", mismatchedLaneMessageMissing.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                laneIdentitySource.canonicalBytes(), mismatchedLaneMessageMissing.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict staleLaneStateConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_LANE_STATE_MISMATCH)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(staleLaneStateClaim.encodedKey()), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", staleLaneStateConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(laneStateSource.canonicalBytes(), staleLaneStateConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict staleLaneStateMessageMissing = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_RECORD_MISSING)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.idMessage(laneStateSchedule.delayMessageId())), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("MESSAGE", staleLaneStateMessageMissing.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                laneStateSource.canonicalBytes(), staleLaneStateMessageMissing.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict sourceTimelineConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_SOURCE_TIMELINE_MISMATCH)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(mismatchedSourceTimelineClaim.encodedKey()), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", sourceTimelineConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                sourceTimelineSource.canonicalBytes(), sourceTimelineConflict.sourcePosition()));
        final LegacyCheckpointStateInventory.Conflict laneKeyValueConflict = inventory.conflicts().stream()
                .filter(conflict -> conflict.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.LANE_RECORD_KEY_VALUE_MISMATCH)
                .filter(conflict -> Bytes.constantTimeEquals(
                        Bytes.sha256(KeyCodec.metaLane(mismatchedLaneKey)), conflict.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("LANE", laneKeyValueConflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(
                sourceTimelineSource.canonicalBytes(), laneKeyValueConflict.sourcePosition()));
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
        assertTrue(Bytes.constantTimeEquals(sourceTimelineSource.canonicalBytes(), orphanRecord.sourcePosition()));
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

    @Test
    void rejectsClaimLaneRuntimeVersionAheadOfCheckpointLane() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 11);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-lane-version-store"));
        final Path image = tempDir.resolve("claim-lane-version-checkpoint");
        final byte[] checkpointId = bytes(16, 51);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-lane-version"));
        final PreparedCommand schedule = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(lane, 3_000, 9_000, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-lane-version")),
                10_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(
                shard, "legacy-cluster", UUID.randomUUID(), 51, null, 4_100);
        final AuthorIdentity owner = AuthorIdentity.owner(
                Bytes.utf8("claim-lane-version-deployment"),
                Bytes.utf8("claim-lane-version-worker"),
                Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("claim-lane-version-fence")));
        final CheckpointManifest manifest;
        final ClaimRecord futureLaneVersionClaim;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            final ClaimRecord claim = ClaimRecordTestSupport.claimScheduled(
                    store, schedule, source, lane, owner, 4_000, chargeVector());
            final long futureLaneVersion = Math.addExact(claim.runtimeLaneVersion(), 2);
            final ClaimResultBody.ClaimPrecondition precondition =
                    ClaimResultBody.decodePrecondition(claim.preconditionBytes());
            futureLaneVersionClaim = ClaimRecord.claimed(
                    claim.delayMessageId(),
                    claim.generation(),
                    claim.claimId(),
                    claim.ownerEpoch(),
                    claim.claimSequence(),
                    claim.laneId(),
                    claim.laneIncarnation(),
                    claim.laneControlVersion(),
                    futureLaneVersion,
                    claim.ownerIdentity(),
                    claim.storeIncarnation(),
                    preconditionWithRuntimeLaneVersion(precondition, futureLaneVersion),
                    claim.timelineKey(),
                    claim.runtimeRevision(),
                    claim.sourceTimelineWork());
            store.write(batch -> batch.putValue(
                    ColumnFamily.INFLIGHT,
                    ClaimRecord.VALUE_TYPE,
                    futureLaneVersionClaim.encodedKey(),
                    futureLaneVersionClaim.encode()));
            store.createCheckpoint(image, checkpointId);
            manifest = manifestFor(image, shard, store, checkpointId, source);
        }

        final LegacyCheckpointStateInventory.Inventory inventory = LegacyCheckpointStateInventory.inspect(
                image,
                shard,
                manifest,
                finiteLimits(),
                new LegacyCheckpointStateInventory.ReadLimits(1_000, 1 << 20, 60_000_000_000L));
        final LegacyCheckpointStateInventory.Conflict conflict = inventory.conflicts().stream()
                .filter(item -> item.reason()
                        == LegacyCheckpointStateInventory.ConflictReason.CLAIM_LANE_RUNTIME_VERSION_AFTER_CURRENT)
                .filter(item -> Bytes.constantTimeEquals(
                        Bytes.sha256(futureLaneVersionClaim.encodedKey()), item.oldKeyDigest()))
                .findFirst()
                .orElseThrow();
        assertEquals("CLAIM", conflict.recordKind());
        assertTrue(Bytes.constantTimeEquals(source.canonicalBytes(), conflict.sourcePosition()));
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
        return encodePrecondition(
                precondition,
                admissionsUsed,
                precondition.expectedObligationSetDigest(),
                precondition.stateVersion(),
                precondition.runtimeLaneVersion(),
                precondition.destinationLaneId(),
                precondition.originalTimelineKeySha256(),
                precondition.sourceTimelineSemanticDigest());
    }

    private static ClaimRecord withPrecondition(final ClaimRecord claim, final byte[] preconditionBytes) {
        return withPrecondition(
                claim, preconditionBytes, claim.laneId(), claim.timelineKey(), claim.sourceTimelineWork());
    }

    private static ClaimRecord withPrecondition(
            final ClaimRecord claim,
            final byte[] preconditionBytes,
            final DestinationLaneId laneId,
            final byte[] timelineKey,
            final byte[] sourceTimelineWork) {
        return ClaimRecord.claimed(
                claim.delayMessageId(),
                claim.generation(),
                claim.claimId(),
                claim.ownerEpoch(),
                claim.claimSequence(),
                laneId,
                claim.laneIncarnation(),
                claim.laneControlVersion(),
                claim.runtimeLaneVersion(),
                claim.ownerIdentity(),
                claim.storeIncarnation(),
                preconditionBytes,
                timelineKey,
                claim.runtimeRevision(),
                sourceTimelineWork);
    }

    private static ClaimRecord withSourceTimeline(
            final ClaimRecord claim, final byte[] timelineKey, final byte[] sourceTimelineWork) {
        final ClaimResultBody.ClaimPrecondition precondition =
                ClaimResultBody.decodePrecondition(claim.preconditionBytes());
        final TimelineWorkRef work = TimelineWorkRef.decode(sourceTimelineWork);
        final byte[] preconditionBytes = encodePrecondition(
                precondition,
                precondition.expectedAdmissionsUsed(),
                precondition.expectedObligationSetDigest(),
                precondition.stateVersion(),
                precondition.runtimeLaneVersion(),
                precondition.destinationLaneId(),
                Bytes.sha256(timelineKey),
                work.semanticWorkDigest());
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
                timelineKey,
                claim.runtimeRevision(),
                sourceTimelineWork);
    }

    private static byte[] timelineKeyWithLane(final ClaimRecord claim, final DestinationLaneId laneId) {
        final byte[] timelineKey = claim.timelineKey();
        System.arraycopy(laneId.bytes(), 0, timelineKey, 2, laneId.bytes().length);
        return timelineKey;
    }

    private static byte[] preconditionWithObligationSetDigest(
            final ClaimResultBody.ClaimPrecondition precondition, final byte[] obligationSetDigest) {
        return encodePrecondition(
                precondition,
                precondition.expectedAdmissionsUsed(),
                obligationSetDigest,
                precondition.stateVersion(),
                precondition.runtimeLaneVersion(),
                precondition.destinationLaneId(),
                precondition.originalTimelineKeySha256(),
                precondition.sourceTimelineSemanticDigest());
    }

    private static byte[] preconditionWithStateVersion(
            final ClaimResultBody.ClaimPrecondition precondition, final long stateVersion) {
        return encodePrecondition(
                precondition,
                precondition.expectedAdmissionsUsed(),
                precondition.expectedObligationSetDigest(),
                stateVersion,
                precondition.runtimeLaneVersion(),
                precondition.destinationLaneId(),
                precondition.originalTimelineKeySha256(),
                precondition.sourceTimelineSemanticDigest());
    }

    private static byte[] preconditionWithRuntimeLaneVersion(
            final ClaimResultBody.ClaimPrecondition precondition, final long runtimeLaneVersion) {
        return encodePrecondition(
                precondition,
                precondition.expectedAdmissionsUsed(),
                precondition.expectedObligationSetDigest(),
                precondition.stateVersion(),
                runtimeLaneVersion,
                precondition.destinationLaneId(),
                precondition.originalTimelineKeySha256(),
                precondition.sourceTimelineSemanticDigest());
    }

    private static byte[] preconditionWithLaneIdentity(
            final ClaimResultBody.ClaimPrecondition precondition,
            final DestinationLaneId laneId,
            final byte[] timelineKey) {
        return encodePrecondition(
                precondition,
                precondition.expectedAdmissionsUsed(),
                precondition.expectedObligationSetDigest(),
                precondition.stateVersion(),
                precondition.runtimeLaneVersion(),
                laneId.bytes(),
                Bytes.sha256(timelineKey),
                precondition.sourceTimelineSemanticDigest());
    }

    private static byte[] encodePrecondition(
            final ClaimResultBody.ClaimPrecondition precondition,
            final int admissionsUsed,
            final byte[] obligationSetDigest,
            final long stateVersion,
            final long runtimeLaneVersion,
            final byte[] destinationLaneId,
            final byte[] originalTimelineKeySha256,
            final byte[] sourceTimelineSemanticDigest) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, precondition.claimId());
            CanonicalProtobuf.bytes(output, 2, precondition.messageId());
            CanonicalProtobuf.uint32Bits(output, 3, precondition.generation());
            CanonicalProtobuf.int64(output, 4, stateVersion);
            CanonicalProtobuf.bytes(output, 5, destinationLaneId);
            CanonicalProtobuf.bytes(output, 6, precondition.laneIncarnation());
            CanonicalProtobuf.int64(output, 7, precondition.laneControlVersion());
            CanonicalProtobuf.int64(output, 8, runtimeLaneVersion);
            CanonicalProtobuf.bytes(output, 9, originalTimelineKeySha256);
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
            CanonicalProtobuf.bytes(output, 20, sourceTimelineSemanticDigest);
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
