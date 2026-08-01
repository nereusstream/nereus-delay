package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ClaimResultBody;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProof;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishAdmissionBodyTest.Fixture;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelayShardTest {
    @TempDir
    Path tempDir;

    @Test
    void admissionBudgetConfigRequiresAPositiveTotalAndSmallerUncertainBudget() {
        assertThrows(IllegalArgumentException.class,
                () -> new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                        3, 100, 10_000, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                        3, 100, 10_000, 1, 1));
    }

    @Test
    void cancelAndRescheduleRemainTooLateWhenUncertainObligationSurvivesCurrentWorkProjection() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("uncertain-control-guard"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 27);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("uncertain-control-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("uncertain-control")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] attemptId = Bytes.sha256(Bytes.utf8("uncertain-control-attempt"));
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("uncertain-control-claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("uncertain-control-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("uncertain-control-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            shard.applyUnknownPublishOutcome(attemptId, 42, Bytes.utf8("unknown-outcome"),
                    Bytes.utf8("unknown-evidence"), unknownPosition);

            final MessageRecord uncertain = shard.getMessage(schedule.delayMessageId());
            final MessageRecord scheduledProjection = new MessageRecord(MessageStatus.SCHEDULED,
                    uncertain.generation(), uncertain.stateVersion() + 1, uncertain.deliverAtEpochMs(),
                    uncertain.expireAtEpochMs(), uncertain.laneId(), uncertain.orderingMode(), uncertain.payload(),
                    uncertain.scheduleSourcePosition(), uncertain.payloadReference(), uncertain.retryEligibilityAtEpochMs())
                    .withRuntimeIndex(GenerationRuntimeIndex.none(GenerationAggregateState.UNCERTAIN,
                            uncertain.runtimeIndex().attemptObligations(), uncertain.runtimeIndex().admissionsUsed(),
                            uncertain.runtimeIndex().uncertainRetryAdmissionsUsed(),
                            uncertain.runtimeIndex().possibleDestinationDuplicate(),
                            uncertain.runtimeIndex().runtimeRevision() + 1));
            store.write(batch -> batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()),
                    scheduledProjection.encode()));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.TOO_LATE,
                    shard.apply(cancel, position(shardId, 3, 1_003)).stableCode());
            final PreparedCommand reschedule = PreparedCommand.reschedule(shardId, schedule.delayMessageId(), 0,
                    3_000, 6_000, 9_000);
            assertEquals(StableCode.TOO_LATE,
                    shard.apply(reschedule, position(shardId, 4, 1_004)).stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).runtimeIndex().attemptObligations().size());
        }
    }

    @Test
    void appliesScheduleCancelAndRescheduleAtomicallyAndReplaysIdempotently() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-a"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("hello")), 9_000);
        final KafkaSourcePosition position0 = position(shardId, 0, 1_000);
        final KafkaSourcePosition position1 = position(shardId, 1, 1_100);
        final KafkaSourcePosition position2 = position(shardId, 2, 1_200);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final CommandResult scheduled = shard.apply(schedule, position0);
            assertEquals(StableCode.SCHEDULED, scheduled.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(GenerationAggregateState.SCHEDULED,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.INITIAL_SCHEDULE,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().timeline().workKind());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, position0.sourceOrderToken(), schedule.delayMessageId(), 0), 1));
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(5_000, lane, schedule.delayMessageId(), 0), 1));
            assertEquals(0, shard.discoverDue(1_999, 10).size());
            assertEquals(1, shard.discoverDue(2_000, 10).size());
            assertEquals(1, shard.discoverExpiry(5_000, 10).size());

            assertEquals(scheduled, shard.apply(schedule, position0));

            final PreparedCommand reschedule = PreparedCommand.reschedule(shardId, schedule.delayMessageId(), 0,
                    3_000, 6_000, 9_000);
            final CommandResult superseded = shard.apply(reschedule, position1);
            assertEquals(StableCode.SUPERSEDED, superseded.stableCode());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).generation());
            assertEquals(MessageStatus.SUPERSEDED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).status());

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 1, 9_000);
            final CommandResult canceled = shard.apply(cancel, position2);
            assertEquals(StableCode.CANCELED, canceled.stableCode());
            assertEquals(MessageStatus.CANCELED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(MessageStatus.CANCELED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 1).status());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 3_000, position1.sourceOrderToken(), schedule.delayMessageId(), 1), 1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(6_000, lane, schedule.delayMessageId(), 1), 1));
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertEquals(0, shard.discoverExpiry(10_000, 10).size());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(position2, reopened.lastAppliedSourcePosition());
            assertEquals(MessageStatus.CANCELED, reopened.getMessage(schedule.delayMessageId()).status());
        }
    }

    @Test
    void rejectsWindowAndCommandIdentityConflictWithoutChangingMessageState() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition position0 = position(shardId, 0, 10_000);
        final PreparedCommand invalid = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("lane-b")), 1, 2, OrderingMode.BEST_EFFORT, new byte[0]),
                20_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.INVALID_DELIVERY_WINDOW, shard.apply(invalid, position0).stableCode());
            assertNull(shard.getMessage(invalid.delayMessageId()));

            final PreparedCommand conflicting = PreparedCommand.create(shardId, invalid.commandId(),
                    invalid.delayMessageId(), invalid.type(), invalid.retryUntilEpochMs(),
                    io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("different")), 11_000, 12_000,
                            OrderingMode.BEST_EFFORT, new byte[0])));
            final CommandResult conflict = shard.apply(conflicting, position(shardId, 1, 10_001));
            assertEquals(StableCode.COMMAND_ID_CONFLICT, conflict.stableCode());
        }
    }

    @Test
    void fifoScheduleUsesOrderedTimelineNamespace() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fifo"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("fifo-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.DELIVERY_TIME_FIFO, Bytes.utf8("fifo")), 9_000);
        final KafkaSourcePosition position = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(command, position).stableCode());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineOrdered(lane, 2_000, position.sourceOrderToken(), command.delayMessageId(), 0),
                    1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, position.sourceOrderToken(), command.delayMessageId(), 0), 1));
        }
    }

    @Test
    void readyIndexTracksLaneVersionAndCanBeDeterministicallyRebuilt() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 5);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("ready-lane"));
        final PreparedCommand later = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 3_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("later")), 9_000);
        final PreparedCommand earlier = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("earlier")), 9_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(later, position(shardId, 0, 1_000)).stableCode());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(earlier, position(shardId, 1, 1_001)).stableCode());
            assertEquals(0, shard.discoverReady(10_000, 10).size());

            final LaneRecord readyLane = shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(2_000, readyLane.nextEligibleAtEpochMs());
            assertEquals(1, shard.discoverReady(10_000, 10).size());
            assertEquals(earlier.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(2_000, lane, readyLane.laneVersion()), 3));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, earlier.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.CANCELED,
                    shard.apply(cancel, position(shardId, 2, 1_002)).stableCode());
            final LaneRecord afterCancel = shard.getLane(lane);
            assertEquals(3_000, afterCancel.nextEligibleAtEpochMs());
            assertEquals(later.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(2_000, lane, readyLane.laneVersion()), 3));

            store.write(batch -> batch.delete(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(3_000, lane, afterCancel.laneVersion())));
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertEquals(1, shard.rebuildReadyIndexes());
            assertEquals(later.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());

            final LaneRecord paused = shard.updateLaneGate(lane, afterCancel.laneControlVersion(),
                    AdmissionGate.ADMIN_PAUSED);
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertThrows(IllegalStateException.class,
                    () -> shard.updateLaneGate(lane, afterCancel.laneControlVersion(), AdmissionGate.OPEN));
            final LaneRecord resumed = shard.updateLaneGate(lane, paused.laneControlVersion(), AdmissionGate.OPEN);
            assertEquals(RuntimeReadiness.BLOCKED, resumed.runtimeReadiness());
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(1, shard.discoverReady(10_000, 10).size());

            shard.updateLaneReadiness(lane, RuntimeReadiness.BLOCKED);
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            shard.updateLaneReadiness(lane, RuntimeReadiness.RECOVERING_EVIDENCE);
            assertEquals(0, shard.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void hardQuotaRejectsNewScheduleAndReleasesOnCancel() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("quota"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 1, 3, 1,
                1, 1_000, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("quota-lane"));
        final PreparedCommand first = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("abc")), 9_000);
        final PreparedCommand second = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_100, 5_100,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("d")), 9_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(first, position(shardId, 0, 1_000)).stableCode());
            assertEquals(1, shard.quota().pendingMessages());
            assertEquals(3, shard.quota().pendingBytes());
            assertEquals(StableCode.HARD_QUOTA_EXCEEDED,
                    shard.apply(second, position(shardId, 1, 1_001)).stableCode());
            final PreparedCommand cancel = PreparedCommand.cancel(shardId, first.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.CANCELED, shard.apply(cancel, position(shardId, 2, 1_002)).stableCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.quota().pendingBytes());
            final PreparedCommand retry = PreparedCommand.schedule(shardId,
                    new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_100, 5_100,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("d")), 9_000);
            assertEquals(StableCode.SCHEDULED, shard.apply(retry, position(shardId, 3, 1_003)).stableCode());
        }
    }

    @Test
    void largePayloadPrepareCommitUsesReservationQuotaAndObjectReference() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("large"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 4);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("large-lane"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("large")), 4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final PayloadProofTrustSet trustSet = new PayloadProofTrustSet(9, Map.of(2, keyPair.getPublic()));
        final KafkaSourcePosition preparePosition = position(shardId, 0, 1_000);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig, trustSet);
            assertEquals(StableCode.OK, shard.apply(prepare, preparePosition).stableCode());
            assertEquals(PayloadReservationStatus.RESERVED, shard.getReservation(reservationId).status());
            assertEquals(1, shard.quota().reservationMessages());
            assertEquals(8, shard.quota().reservationBytes());

            final PreparedCommand abandonedPrepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
            assertEquals(StableCode.OK,
                    shard.apply(abandonedPrepare, position(shardId, 1, 1_001)).stableCode());
            final PreparedCommand abandon = PreparedCommand.cancel(shardId, abandonedPrepare.delayMessageId(), 0,
                    9_000);
            assertEquals(StableCode.PAYLOAD_RESERVATION_ABANDONED,
                    shard.apply(abandon, position(shardId, 2, 1_002)).stableCode());
            assertEquals(1, shard.quota().reservationMessages());

            final PayloadCommitProof proof = PayloadCommitProof.signed(9, 2, shardId.routeIncarnation().bytes(),
                    shardId.partition(), prepare.delayMessageId(), reservationId, Bytes.sha256(Bytes.utf8("profile")),
                    Bytes.utf8("bucket"), Bytes.utf8("key"), Bytes.utf8("v1"), new byte[0],
                    intent.expectedPayloadLength(), intent.payloadSha256(), 5_000, keyPair.getPrivate());
            final PreparedCommand commit = PreparedCommand.commitLarge(shardId, prepare.delayMessageId(), proof,
                    9_000);
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(commit, position(shardId, 3, 1_003)).stableCode());
            final MessageRecord message = shard.getMessage(prepare.delayMessageId());
            assertNotNull(message.payloadReference());
            assertEquals(8, message.payloadLength());
            assertEquals(PayloadReservationStatus.COMMITTED, shard.getReservation(reservationId).status());
            assertEquals(1, shard.quota().pendingMessages());
            assertEquals(0, shard.quota().reservationMessages());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig, trustSet);
            assertEquals(8, reopened.getMessage(prepare.delayMessageId()).payloadLength());
            assertEquals(0, reopened.quota().reservationMessages());
        }
    }

    @Test
    void publishAdmissionAndUnknownOutcomeMoveOneAttemptAcrossInflightKeysAtomically() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("attempt-ledger"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("attempt-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("attempt")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("attempt-id"));
        final byte[] claimId = Bytes.sha256(Bytes.utf8("claim-id"));
        final byte[] ownerIdentity = Bytes.sha256(Bytes.utf8("owner"));
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-publish"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(command, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(command.delayMessageId(), 0,
                    attemptId, claimId, 42, 1, lane, new byte[16], ownerIdentity,
                    store.metadata().storeIncarnation(), preparedHash, Bytes.utf8("admission-body"),
                    admissionPosition.canonicalBytes());

            assertEquals(admission, shard.admitPublishAttempt(admission, admissionPosition));
            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(command.delayMessageId()).status());
            assertEquals(CurrentSendWorkKind.PUBLISHING,
                    shard.getMessage(command.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(1, shard.getMessage(command.delayMessageId()).runtimeIndex().attemptObligations().size());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(), command.delayMessageId(), 0),
                    1));
            assertNotNull(store.getValue(ColumnFamily.INFLIGHT, admission.encodedKey(), PublishAttemptLedger.VALUE_TYPE));
            assertEquals(admission.obligationRef(),
                    AttemptObligationRef.decode(admission.obligationRef().canonicalBytes()));
            assertEquals(admission, PublishAttemptLedger.decode(admission.encode()));

            final PublishAttemptLedger uncertain = shard.applyUnknownPublishOutcome(attemptId, 42,
                    Bytes.utf8("unknown-outcome"), Bytes.utf8("timeout-evidence"), outcomePosition);
            assertEquals(AttemptLedgerState.UNCERTAIN, uncertain.state());
            assertEquals(uncertain, PublishAttemptLedger.decode(uncertain.encode()));
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(uncertain, shard.findOpenPublishAttempt(attemptId));
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(command.delayMessageId()).status());
            assertEquals(GenerationAggregateState.UNCERTAIN,
                    shard.getMessage(command.delayMessageId()).runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.NONE,
                    shard.getMessage(command.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(1, shard.getMessage(command.delayMessageId()).runtimeIndex().attemptObligations().size());
            assertNull(store.getValue(ColumnFamily.INFLIGHT, admission.encodedKey(), PublishAttemptLedger.VALUE_TYPE));
            assertNotNull(store.getValue(ColumnFamily.INFLIGHT, uncertain.encodedKey(), PublishAttemptLedger.VALUE_TYPE));
            assertArrayEquals(outcomePosition.canonicalBytes(), uncertain.sourcePosition());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(AttemptLedgerState.UNCERTAIN, reopened.findOpenPublishAttempt(attemptId).state());
            assertEquals(outcomePosition, reopened.lastAppliedSourcePosition());
        }
    }

    @Test
    void verifiedPublishSuccessClosesPublishingLedgerAndRetainsTerminalHistory() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("attempt-success"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 10);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("success-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("success")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("success-attempt"));
        final PublishAttemptLedger[] holder = new PublishAttemptLedger[1];
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(command, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            holder[0] = PublishAttemptLedger.publishing(command.delayMessageId(), 0, attemptId,
                    Bytes.sha256(Bytes.utf8("success-claim")), 7, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("success-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("success-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(holder[0], admissionPosition);
            assertEquals(MessageStatus.PUBLISHED,
                    shard.applyPublishedPublishOutcome(attemptId, 7, outcomePosition).status());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(command.delayMessageId()).status());
            assertEquals(MessageStatus.PUBLISHED,
                    shard.getTerminalGeneration(command.delayMessageId(), 0).status());
        }
    }

    @Test
    void sourceOrderedExpireMutationAtomicallyClosesScheduledGenerationAndDedupes() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-expiry"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 12);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("expiry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("expiry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition expiryPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition duplicatePosition = position(shardId, 2, 1_002);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(5_000, 5_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("expiry-proof")), 0, null);
        final byte[] body = expiryBody(shardId, schedule.delayMessageId(), 0, 5_000, proof.canonicalBytes());
        final byte[] author = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.EXPIRE_GENERATION, 9_000,
                Bytes.sha256(Bytes.utf8("expiry-operation")), body, author, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(mutation, expiryPosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.EXPIRED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.ALREADY_EXPIRED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverExpiry(10_000, 10).size());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(mutation, duplicatePosition, keyPair.getPublic()).stableCode());
            assertEquals(duplicatePosition, shard.lastAppliedSourcePosition());
            assertArrayEquals(expiryPosition.canonicalBytes(), shard.getSystemMutationResult(mutation.systemMutationId())
                    .appliedSourcePosition());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.EXPIRED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.OK,
                    reopened.getSystemMutationResult(mutation.systemMutationId()).stableCode());
        }
    }

    @Test
    void sourceOrderedPublishOutcomeAtomicallyMovesAttemptToUncertainAndRetainsDedupe() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-unknown"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 13);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("outcome-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("outcome")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-outcome-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 2,
                Bytes.sha256(Bytes.utf8("outcome-proof")), 0, null);
        final byte[] body = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-outcome-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(mutation, outcomePosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.findOpenPublishAttempt(attemptId).state());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.getSystemMutationResult(mutation.systemMutationId()).stableCode());
        }
    }

    @Test
    void sourceOrderedUnknownOutcomeMaterializesPinnedPolicyUncertainRetry() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-uncertain-retry"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("uncertain-retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("uncertain-retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("policy-uncertain-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 5, 5,
                Bytes.sha256(Bytes.utf8("uncertain-retry-proof")), 0, null);
        final byte[] body = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes(),
                unknownRetryDecision(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, 3_000));
        final PublishOutcomeBody parsed = PublishOutcomeBody.decode(body);
        assertEquals(2, parsed.retryDecision().kind());
        assertEquals(3_000, parsed.retryDecision().nextRetryAt());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("policy-uncertain-retry-operation")), body, owner, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("policy-uncertain-retry-claim")), 42, 1, lane,
                    new byte[16], Bytes.sha256(Bytes.utf8("policy-uncertain-retry-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("policy-uncertain-retry-prepared")),
                    Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, result.stableCode());
            final MessageRecord retry = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, retry.status());
            assertEquals(GenerationAggregateState.UNCERTAIN, retry.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE, retry.runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.PINNED_POLICY,
                    retry.runtimeIndex().timeline().uncertainRetryAuthority());
            assertEquals(2, retry.runtimeIndex().timeline().candidateAttemptNo());
            assertEquals(3_000, retry.retryEligibilityAtEpochMs());
            assertEquals(1, retry.runtimeIndex().admissionsUsed());
            assertEquals(0, retry.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(0, shard.discoverDue(2_999, 10).size());
            assertEquals(1, shard.discoverDue(3_000, 10).size());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord retry = reopened.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, retry.status());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.PINNED_POLICY,
                    retry.runtimeIndex().timeline().uncertainRetryAuthority());
            assertEquals(AttemptLedgerState.UNCERTAIN, reopened.getPublishAttempt(attemptId, 42).state());
            assertEquals(1, reopened.discoverDue(3_000, 10).size());
        }
    }

    @Test
    void sourceOrderedAdmissionConsumesUncertainRetryBudgetOnlyForTheNewAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-uncertain-admission"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 29);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("uncertain-admission")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition retryAdmissionPosition = position(shardId, 3, 3_001);
        final byte[] firstAttemptId = Bytes.sha256(Bytes.utf8("uncertain-admission-first"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 6, 6,
                Bytes.sha256(Bytes.utf8("uncertain-admission-proof")), 0, null);
        final byte[] outcomeBody = publishOutcomeBody(shardId, firstAttemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes(),
                unknownRetryDecision(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, 3_000));
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("uncertain-admission-unknown")), outcomeBody, owner, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger firstAdmission = PublishAttemptLedger.publishing(schedule.delayMessageId(),
                    0, firstAttemptId, Bytes.sha256(Bytes.utf8("uncertain-admission-claim")), 42, 1, lane,
                    LaneRecord.initial(lane, schedulePosition).laneIncarnation(),
                    Bytes.sha256(Bytes.utf8("uncertain-admission-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("uncertain-admission-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(firstAdmission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final MessageRecord uncertainRetry = shard.getMessage(schedule.delayMessageId());
            final byte[] retryTimelineKey = uncertainRetry.runtimeIndex().timeline().encodedTimelineKey();
            final TimelineWorkRef retryWork = uncertainRetry.runtimeIndex().timeline();
            final Fixture fixture = Fixture.createForSource(shardId,
                    schedule.delayMessageId(), LaneRecord.initial(lane, schedulePosition).laneIncarnation(),
                    retryTimelineKey, 3, 1, 0,
                    GenerationRuntimeIndex.obligationSetDigest(uncertainRetry.runtimeIndex().attemptObligations()),
                    retryWork.semanticWorkDigest(), 2, uncertainRetry.stateVersion());
            final SystemMutation retryAdmission = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_ADMISSION, 9_000,
                    Bytes.sha256(Bytes.utf8("uncertain-admission-retry")), fixture.body(), fixture.owner(), 1,
                    keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(retryAdmission, retryAdmissionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            final MessageRecord publishing = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.PUBLISHING, publishing.status());
            assertEquals(GenerationAggregateState.UNCERTAIN, publishing.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.PUBLISHING, publishing.runtimeIndex().currentWorkKind());
            assertEquals(2, publishing.runtimeIndex().admissionsUsed());
            assertEquals(1, publishing.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(2, publishing.runtimeIndex().attemptObligations().size());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(firstAttemptId, 42).state());
            final PublishAdmissionBody admitted = PublishAdmissionBody.decode(fixture.body());
            assertEquals(AttemptLedgerState.PUBLISHING,
                    shard.getPublishAttempt(admitted.publishAttemptId(), 7).state());
            assertEquals(result, shard.getSystemMutationResult(retryAdmission.systemMutationId()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord publishing = reopened.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.PUBLISHING, publishing.status());
            assertEquals(2, publishing.runtimeIndex().admissionsUsed());
            assertEquals(1, publishing.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(2, publishing.runtimeIndex().attemptObligations().size());
        }
    }

    @Test
    void sourceOrderedEvidenceResolutionClosesUncertainAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-evidence-resolution"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("evidence-resolution-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("evidence")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolutionPosition = position(shardId, 3, 1_003);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-resolution-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence unknownObserved = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 2,
                Bytes.sha256(Bytes.utf8("unknown-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], unknownObserved.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("resolution-unknown-operation")), unknownBody, owner, 1,
                keyPair.getPrivate());
        final byte[] resolutionBody = evidenceResolutionBody(shardId, attemptId, StableCode.OK, 1, 0,
                new TrustedUtcIntervalEvidence(1_003, 1_003,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 3, 3,
                        Bytes.sha256(Bytes.utf8("resolution-proof")), 0, null).canonicalBytes());
        final byte[] service = AuthorIdentity.service(Bytes.utf8("evidence-service"), Bytes.utf8("run"), 1)
                .canonicalBytes();
        final SystemMutation resolution = SystemMutation.signed(shardId, SystemMutationType.EVIDENCE_RESOLUTION,
                9_000, Bytes.sha256(Bytes.utf8("resolution-operation")), resolutionBody, service, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(schedule.delayMessageId()).status());

            final SystemMutationResult result = shard.applySystemMutation(resolution, resolutionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(MessageStatus.PUBLISHED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).status());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(result, shard.getSystemMutationResult(resolution.systemMutationId()));
        }
    }

    @Test
    void sourceOrderedEvidenceResolutionRequeuesVerifiedNotPublishedAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-evidence-retry"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("evidence-retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("evidence-retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolutionPosition = position(shardId, 3, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-resolution-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence unknownObserved = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 2,
                Bytes.sha256(Bytes.utf8("unknown-retry-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], unknownObserved.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("resolution-retry-unknown-operation")), unknownBody, owner, 1,
                keyPair.getPrivate());
        final byte[] resolutionBody = evidenceResolutionBody(shardId, attemptId,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2, 1,
                new TrustedUtcIntervalEvidence(2_002, 2_002,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 3, 3,
                        Bytes.sha256(Bytes.utf8("resolution-retry-proof")), 0, null).canonicalBytes());
        final byte[] service = AuthorIdentity.service(Bytes.utf8("evidence-service"), Bytes.utf8("run"), 1)
                .canonicalBytes();
        final SystemMutation resolution = SystemMutation.signed(shardId, SystemMutationType.EVIDENCE_RESOLUTION,
                9_000, Bytes.sha256(Bytes.utf8("resolution-retry-operation")), resolutionBody, service, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final SystemMutationResult result = shard.applySystemMutation(resolution, resolutionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(1, shard.discoverDue(2_002, 10).size());
        }
    }

    @Test
    void sourceOrderedPublishSuccessUsesEvidenceAndClosesAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-published"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 14);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("published-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("published")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-published-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 3, 3,
                Bytes.sha256(Bytes.utf8("published-proof")), 0, null);
        final byte[] body = publishOutcomeBody(shardId, attemptId, 1, 0, StableCode.OK,
                nestedPlaceholder(), observedAt.canonicalBytes());
        final PublishOutcomeBody parsedOutcome = PublishOutcomeBody.decode(body);
        assertEquals(1, parsedOutcome.retryDecision().kind());
        assertEquals(1, parsedOutcome.retryDecision().completedAttemptNo());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-published-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 7, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            assertEquals(StableCode.OK,
                    shard.applySystemMutation(mutation, outcomePosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.findOpenPublishAttempt(attemptId));
        }
    }

    @Test
    void sourceOrderedNotPublishedRetriableClosesAttemptAndRequeuesBestEffortWork() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-retry"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 16);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] body = publishNotPublishedBody(shardId, attemptId, 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2_002);
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-retry-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).runtimeIndex().admissionsUsed());

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).runtimeIndex().admissionsUsed());
            assertEquals(2, shard.getMessage(schedule.delayMessageId()).runtimeIndex().timeline()
                    .candidateAttemptNo());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(0, shard.discoverDue(2_001, 10).size());
            assertEquals(1, shard.discoverDue(2_002, 10).size());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
        }
    }

    @Test
    void sourceOrderedNotPublishedPermanentClosesGenerationAndReleasesQuota() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-permanent"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("permanent-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("permanent")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-permanent-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] body = publishNotPublishedBody(shardId, attemptId, 2,
                StableCode.DESTINATION_DEFINITIVE_PERMANENT, -1);
        final PublishOutcomeBody parsed = PublishOutcomeBody.decode(body);
        assertEquals(3, parsed.retryDecision().kind());
        assertEquals(1, parsed.retryDecision().completedAttemptNo());
        assertEquals(5_000, parsed.retryDecision().retryDeadline());
        assertFalse(parsed.retryDecision().hasNextRetryAt());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-permanent-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_PERMANENT, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.DESTINATION_DEFINITIVE_PERMANENT,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
        }
    }

    @Test
    void sourceOrderedNotPublishedLaneUnavailableBlocksLaneAndKeepsRetryTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-lane"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-unavailable"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("lane")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-lane-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] body = publishNotPublishedBody(shardId, attemptId, 3,
                StableCode.CAPABILITY_UNAVAILABLE, 2_002);
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-lane-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.CAPABILITY_UNAVAILABLE, result.stableCode());
            assertEquals(RuntimeReadiness.BLOCKED, shard.getLane(lane).runtimeReadiness());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(1, shard.discoverDue(2_002, 10).size());
        }
    }

    @Test
    void sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-admission"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 15);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId, io.nereusstream.delay.protocol.CommandId.random(shardId),
                messageId, io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("hello"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final Fixture fixture = Fixture.createForSource(shardId, messageId,
                LaneRecord.initial(lane, schedulePosition).laneIncarnation(), sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(java.util.List.of()), sourceWork.semanticWorkDigest());
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final java.security.KeyPairGenerator keyPairGenerator = java.security.KeyPairGenerator.getInstance("Ed25519");
        final java.security.KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("admission-operation")), fixture.body(), fixture.owner(), 1,
                keyPair.getPrivate());
        final PublishAdmissionBody parsed = PublishAdmissionBody.decode(fixture.body());

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertNull(shard.getClaim(parsed.claimId(), 1));

            final SystemMutationResult result = shard.applySystemMutation(mutation, admissionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(messageId).status());
            final PublishAttemptLedger ledger = shard.findOpenPublishAttempt(parsed.publishAttemptId());
            assertNotNull(ledger);
            assertEquals(AttemptLedgerState.PUBLISHING, ledger.state());
            assertEquals(parsed.descriptor().attemptNo(), ledger.attemptNo());
            assertArrayEquals(fixture.body(), ledger.admissionBytes());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
            assertEquals(result, shard.applySystemMutation(mutation, admissionPosition, keyPair.getPublic()));
        }
    }

    @Test
    void sourceOrderedPublishAdmissionFailsClosedWhenGenerationBudgetIsExhausted() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-admission-budget"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 1, 0);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 25);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(
                        new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                                OrderingMode.BEST_EFFORT, Bytes.utf8("budget"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000,
                schedulePosition.sourceOrderToken(), messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final byte[] laneIncarnation = LaneRecord.initial(lane, schedulePosition).laneIncarnation();
        final Fixture firstFixture = Fixture.createForSource(shardId, messageId, laneIncarnation,
                sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(java.util.List.of()), sourceWork.semanticWorkDigest());
        final PublishAdmissionBody firstBody = PublishAdmissionBody.decode(firstFixture.body());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final SystemMutation firstAdmission = SystemMutation.signed(shardId,
                SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("admission-budget-first")), firstFixture.body(), firstFixture.owner(), 7,
                keyPair.getPrivate());
        final KafkaSourcePosition firstAdmissionPosition = position(shardId, 1, 1_001);
        final byte[] outcomeBody = publishNotPublishedBody(shardId, firstBody.publishAttemptId(), 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2_002);
        final SystemMutation outcome = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("admission-budget-outcome")), outcomeBody, firstFixture.owner(), 7,
                keyPair.getPrivate());
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(firstAdmission, firstAdmissionPosition, keyPair.getPublic()).stableCode());
            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE,
                    shard.applySystemMutation(outcome, outcomePosition, keyPair.getPublic()).stableCode());
            final MessageRecord retry = shard.getMessage(messageId);
            assertEquals(1, retry.runtimeIndex().admissionsUsed());
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, retry.runtimeIndex().timeline().workKind());

            final Fixture secondFixture = Fixture.createForSource(shardId, messageId, laneIncarnation,
                    retry.runtimeIndex().timeline().encodedTimelineKey(), 2, 1, 0,
                    GenerationRuntimeIndex.obligationSetDigest(java.util.List.of()),
                    retry.runtimeIndex().timeline().semanticWorkDigest(), 2, retry.stateVersion());
            final SystemMutation secondAdmission = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_ADMISSION, 9_000,
                    Bytes.sha256(Bytes.utf8("admission-budget-second")), secondFixture.body(), secondFixture.owner(),
                    7, keyPair.getPrivate());
            final SystemMutationResult rejected = shard.applySystemMutation(secondAdmission,
                    position(shardId, 3, 2_003), keyPair.getPublic());

            assertEquals(StableCode.STALE_SYSTEM_MUTATION, rejected.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(messageId).status());
            assertEquals(1, shard.getMessage(messageId).runtimeIndex().admissionsUsed());
            assertNull(shard.findOpenPublishAttempt(firstBody.publishAttemptId()));
        }
    }

    @Test
    void localClaimIsDurableAndRevokeRestoresTimelineAtomically() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-lifecycle"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-lifecycle-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final byte[] timelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                schedule.delayMessageId(), 0);
        final ClaimRecord claim;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);

            claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());

            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(CurrentSendWorkKind.CLAIMED,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertArrayEquals(claim.claimId(), shard.getMessage(schedule.delayMessageId()).runtimeIndex().claimId());
            assertEquals(1, shard.claimSequence());
            assertEquals(claim, shard.getClaim(claim.claimId(), owner.generation()));
            assertNull(store.getValue(ColumnFamily.TIMELINE, timelineKey, 1));
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(5_000, lane, schedule.delayMessageId(), 0), 1));
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertEquals(1, shard.discoverExpiry(10_000, 10).size());
        }
        try (SharedRocksDbResources reopenedResources = new SharedRocksDbResources(config);
             ShardStore reopenedStore = ShardStore.open(config, shardId, reopenedResources)) {
            final DelayShard reopened = new DelayShard(reopenedStore, DelayShardConfig.defaults());
            assertEquals(1, reopened.claimSequence());
            assertEquals(MessageStatus.CLAIMED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(claim, reopened.getClaim(claim.claimId(), owner.generation()));
            final MessageRecord restored = reopened.revokeClaim(claim.claimId(), owner.generation());
            assertEquals(MessageStatus.SCHEDULED, restored.status());
            assertEquals(CurrentSendWorkKind.TIMELINE, restored.runtimeIndex().currentWorkKind());
            assertNull(reopened.getClaim(claim.claimId(), owner.generation()));
            assertNotNull(reopenedStore.getValue(ColumnFamily.TIMELINE, timelineKey, 1));
            assertEquals(1, reopened.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void shardActivationFailsClosedForOrphanedPublishLedger() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("orphaned-ledger"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 21);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("orphaned-ledger-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("orphaned")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("orphaned-ledger-attempt"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.delete(ColumnFamily.ID, KeyCodec.idMessage(schedule.delayMessageId())));
            assertThrows(IllegalStateException.class, () -> new DelayShard(store, DelayShardConfig.defaults()));
        }
    }

    @Test
    void terminalSummaryRetainsASecondOpenObligationAndReopensSafely() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("terminal-obligation-summary"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 26);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("terminal-summary-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("terminal-summary")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition firstAdmissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition terminalPosition = position(shardId, 2, 1_002);
        final byte[] firstAttemptId = Bytes.sha256(Bytes.utf8("terminal-summary-first"));
        final byte[] secondAttemptId = Bytes.sha256(Bytes.utf8("terminal-summary-second"));

        TerminalGenerationRecord summary;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger first = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    firstAttemptId, Bytes.sha256(Bytes.utf8("terminal-summary-first-claim")), 42, 1, lane,
                    new byte[16], Bytes.sha256(Bytes.utf8("terminal-summary-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("terminal-summary-prepared")),
                    Bytes.utf8("terminal-summary-first-admission"), firstAdmissionPosition.canonicalBytes());
            shard.admitPublishAttempt(first, firstAdmissionPosition);

            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final PublishAttemptLedger second = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    secondAttemptId, Bytes.sha256(Bytes.utf8("terminal-summary-second-claim")), 43, 2, lane,
                    shard.getLane(lane).laneIncarnation(), Bytes.sha256(Bytes.utf8("terminal-summary-owner-2")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("terminal-summary-prepared-2")),
                    Bytes.utf8("terminal-summary-second-admission"), firstAdmissionPosition.canonicalBytes());
            final List<AttemptObligationRef> obligations = new ArrayList<>(List.of(
                    first.obligationRef(), second.obligationRef()));
            obligations.sort(DelayShardTest::compareObligations);
            final MessageRecord withSecond = new MessageRecord(MessageStatus.PUBLISHING, current.generation(),
                    current.stateVersion(), current.deliverAtEpochMs(), current.expireAtEpochMs(), current.laneId(),
                    current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(
                            GenerationRuntimeIndex.publishing(firstAttemptId, obligations, 2, 0, false,
                                    current.stateVersion()));
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()), withSecond.encode());
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, second.encodedKey(),
                        second.encode());
            });

            final MessageRecord published = shard.applyPublishedPublishOutcome(firstAttemptId, 42,
                    terminalPosition);
            assertEquals(MessageStatus.PUBLISHED, published.status());
            summary = shard.getTerminalGeneration(schedule.delayMessageId(), 0);
            assertNotNull(summary);
            assertEquals(List.of(second.obligationRef()), summary.openObligations());
            assertEquals(summary.openObligations(), published.runtimeIndex().attemptObligations());
            final TerminalGenerationRecord retained = summary;

            final MessageRecord latePublished = shard.applyPublishedPublishOutcome(secondAttemptId, 43,
                    position(shardId, 3, 1_003));
            assertEquals(MessageStatus.PUBLISHED, latePublished.status());
            assertTrue(latePublished.runtimeIndex().possibleDestinationDuplicate());
            assertEquals(List.of(), latePublished.runtimeIndex().attemptObligations());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(secondAttemptId, 43));
            assertEquals(List.of(second.obligationRef()), retained.openObligations());
            summary = shard.getTerminalGeneration(schedule.delayMessageId(), 0);
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(summary.openObligations(),
                    reopened.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertTrue(reopened.getMessage(schedule.delayMessageId()).runtimeIndex().possibleDestinationDuplicate());
            assertNull(reopened.getPublishAttempt(secondAttemptId, 43));
        }
    }

    @Test
    void lateOutcomeSettlesOlderGenerationThroughTerminalSummary() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("historical-terminal-outcome"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 30);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("historical-terminal-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("historical-terminal")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition generationOnePosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition outcomePosition = position(shardId, 3, 1_003);
        final KafkaSourcePosition secondOutcomePosition = position(shardId, 4, 1_004);
        final KafkaSourcePosition thirdOutcomePosition = position(shardId, 5, 1_005);
        final KafkaSourcePosition resolutionPosition = position(shardId, 6, 1_006);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("historical-terminal-attempt"));
        final byte[] secondAttemptId = Bytes.sha256(Bytes.utf8("historical-terminal-second-attempt"));
        final byte[] thirdAttemptId = Bytes.sha256(Bytes.utf8("historical-terminal-third-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final AuthorIdentity secondOwner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 43,
                Bytes.sha256(Bytes.utf8("lease-2")));
        final AuthorIdentity thirdOwner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 44,
                Bytes.sha256(Bytes.utf8("lease-3")));
        TerminalGenerationRecord oldSummary;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger oldAttempt = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("historical-terminal-claim")), owner.generation(), 1, lane,
                    shard.getLane(lane).laneIncarnation(), owner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("historical-terminal-prepared")), Bytes.utf8("historical-admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(oldAttempt, admissionPosition);

            final MessageRecord prior = shard.getMessage(schedule.delayMessageId());
            final byte[] nextTimelineKey = KeyCodec.timelineDue(lane, 3_000,
                    generationOnePosition.sourceOrderToken(), schedule.delayMessageId(), 1);
            final MessageRecord next = new MessageRecord(MessageStatus.SCHEDULED, 1,
                    prior.stateVersion() + 1, 3_000, 6_000, lane, OrderingMode.BEST_EFFORT, prior.payload(),
                    generationOnePosition.canonicalBytes(), prior.payloadReference(), 3_000).withRuntimeIndex(
                    GenerationRuntimeIndex.timeline(GenerationAggregateState.SCHEDULED,
                            TimelineWorkRef.initial(nextTimelineKey, 3_000, prior.stateVersion() + 1),
                            List.of(), 0, 0, false, prior.stateVersion() + 1));
            oldSummary = new TerminalGenerationRecord(schedule.delayMessageId(), 0, MessageStatus.SUPERSEDED,
                    StableCode.SUPERSEDED, prior.stateVersion(), generationOnePosition.canonicalBytes(), false,
                    List.of(oldAttempt.obligationRef()));
            final TerminalGenerationRecord initialSummary = oldSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()), next.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, nextTimelineKey,
                        new TimelineEntry(schedule.delayMessageId(), 1).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineExpiry(6_000, lane, schedule.delayMessageId(), 1),
                        new TimelineEntry(schedule.delayMessageId(), 1).encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), initialSummary.encode());
            });

            final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_003, 1_003,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 7, 7,
                    Bytes.sha256(Bytes.utf8("historical-outcome-proof")), 0, null);
            final byte[] outcomeBody = publishNotPublishedBody(shardId, attemptId, 1,
                    StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 3_000);
            final SystemMutation outcome = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-outcome-operation")), outcomeBody,
                    owner.canonicalBytes(), 1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(outcome, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertFalse(shard.getMessage(schedule.delayMessageId()).runtimeIndex().possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(attemptId, owner.generation()));
            assertEquals(result, shard.getSystemMutationResult(outcome.systemMutationId()));

            final PublishAttemptLedger second = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    secondAttemptId, Bytes.sha256(Bytes.utf8("historical-terminal-second-claim")),
                    secondOwner.generation(), 2, lane, shard.getLane(lane).laneIncarnation(),
                    secondOwner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("historical-terminal-second-prepared")),
                    Bytes.utf8("historical-second-admission"), secondOutcomePosition.canonicalBytes());
            final TerminalGenerationRecord secondSummary = new TerminalGenerationRecord(schedule.delayMessageId(), 0,
                    MessageStatus.SUPERSEDED,
                    StableCode.SUPERSEDED, prior.stateVersion(), generationOnePosition.canonicalBytes(), false,
                    List.of(second.obligationRef()));
            oldSummary = secondSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, second.encodedKey(),
                        second.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), secondSummary.encode());
            });
            final byte[] secondOutcomeBody = publishOutcomeBody(shardId, secondAttemptId, 1, 0, StableCode.OK,
                    nestedPlaceholder(), observedAt.canonicalBytes());
            final SystemMutation secondOutcome = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-second-outcome-operation")), secondOutcomeBody,
                    secondOwner.canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult secondResult = shard.applySystemMutation(secondOutcome,
                    secondOutcomePosition, keyPair.getPublic());
            assertEquals(StableCode.OK, secondResult.stableCode());
            assertTrue(shard.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(secondAttemptId, secondOwner.generation()));

            final PublishAttemptLedger third = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    thirdAttemptId, Bytes.sha256(Bytes.utf8("historical-terminal-third-claim")),
                    thirdOwner.generation(), 3, lane, shard.getLane(lane).laneIncarnation(),
                    thirdOwner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("historical-terminal-third-prepared")),
                    Bytes.utf8("historical-third-admission"), thirdOutcomePosition.canonicalBytes());
            final TerminalGenerationRecord thirdSummary = new TerminalGenerationRecord(
                    schedule.delayMessageId(), 0, MessageStatus.SUPERSEDED, StableCode.SUPERSEDED,
                    prior.stateVersion(), generationOnePosition.canonicalBytes(), true,
                    List.of(third.obligationRef()));
            oldSummary = thirdSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, third.encodedKey(),
                        third.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), thirdSummary.encode());
            });
            final byte[] unknownBody = publishOutcomeBody(shardId, thirdAttemptId, 3, 4,
                    StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
            final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-third-unknown-operation")), unknownBody,
                    thirdOwner.canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult unknownResult = shard.applySystemMutation(unknown, thirdOutcomePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, unknownResult.stableCode());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(thirdAttemptId,
                    thirdOwner.generation()).state());
            assertEquals(AttemptLedgerState.UNCERTAIN,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations().get(0).ledgerState());

            final byte[] resolutionBody = evidenceResolutionBody(shardId, thirdAttemptId, StableCode.OK, 1, 0,
                    new TrustedUtcIntervalEvidence(1_006, 1_006,
                            TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 8, 8,
                            Bytes.sha256(Bytes.utf8("historical-resolution-proof")), 0, null).canonicalBytes());
            final AuthorIdentity service = AuthorIdentity.service(Bytes.utf8("evidence-service"),
                    Bytes.utf8("historical-run"), 2);
            final SystemMutation resolution = SystemMutation.signed(shardId,
                    SystemMutationType.EVIDENCE_RESOLUTION, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-resolution-operation")), resolutionBody,
                    service.canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult resolutionResult = shard.applySystemMutation(resolution, resolutionPosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, resolutionResult.stableCode());
            assertTrue(shard.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(thirdAttemptId, thirdOwner.generation()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.SCHEDULED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(oldSummary.generation(), reopened.getTerminalGeneration(schedule.delayMessageId(), 0)
                    .generation());
            assertTrue(reopened.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertNull(reopened.getPublishAttempt(attemptId, owner.generation()));
            assertNull(reopened.getPublishAttempt(secondAttemptId, secondOwner.generation()));
            assertNull(reopened.getPublishAttempt(thirdAttemptId, thirdOwner.generation()));
        }
    }

    @Test
    void claimResultConsumesExactClaimAndTerminalizesCurrentGeneration() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-result-claimed"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 23);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-result-claimed-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-result")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition resultPosition = position(shardId, 1, 2_100);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());
            final byte[] body = claimResultBody(shardId, claim.claimId(), schedule.delayMessageId(), 0, lane,
                    claim.laneIncarnation(), claim.laneControlVersion(), claim.runtimeLaneVersion(),
                    claim.timelineKey(), owner.canonicalBytes(), store.metadata().storeIncarnation(), 3_000, 1,
                    2_000, 2_000);
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT, 9_000,
                    Bytes.sha256(Bytes.utf8("claimed-result-operation")), body, owner.canonicalBytes(), 1,
                    keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(mutation, resultPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
        }
    }

    @Test
    void publishAdmissionConsumesExactLocalClaimBeforeCreatingAttemptLedger() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-admission"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 24);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-admission-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-admission")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());
            final byte[] attemptId = Bytes.sha256(Bytes.utf8("claim-admission-attempt"));
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, claim.claimId(), owner.generation(), 1, lane, claim.laneIncarnation(),
                    owner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());

            shard.admitPublishAttempt(admission, admissionPosition);

            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertEquals(admission, shard.getPublishAttempt(attemptId, owner.generation()));
            assertNull(store.getValue(ColumnFamily.TIMELINE, claim.timelineKey(), 1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(5_000, lane, schedule.delayMessageId(), 0), 1));
        }
    }

    @Test
    void sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-claim-result"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 21);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-result-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-result")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition claimResultPosition = position(shardId, 1, 1_100);
        final KafkaSourcePosition duplicatePosition = position(shardId, 2, 1_101);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] claimId = Bytes.sha256(Bytes.utf8("claim-result-claim"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            final LaneRecord laneRecord = shard.getLane(lane);
            final byte[] timelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                    schedule.delayMessageId(), 0);
            final byte[] body = claimResultBody(shardId, claimId, schedule.delayMessageId(), 0, lane,
                    laneRecord.laneIncarnation(), 1, laneRecord.laneVersion(), timelineKey, owner,
                    store.metadata().storeIncarnation(), 3_000, 1, 1_500, 1_500);
            final ClaimResultBody parsed = ClaimResultBody.decode(body);
            assertEquals(1, parsed.resultKind());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE, parsed.stableCode());
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT, 9_000,
                    claimId, body, owner, 1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(mutation, claimResultPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertEquals(0, shard.discoverExpiry(10_000, 10).size());
            assertEquals(result, shard.applySystemMutation(mutation, duplicatePosition, keyPair.getPublic()));
            assertEquals(duplicatePosition, shard.lastAppliedSourcePosition());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
        }
    }

    @Test
    void staleClaimResultDoesNotTerminalizeChangedTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-claim-result-stale"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 22);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-result-stale-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-result-stale")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition claimResultPosition = position(shardId, 1, 1_100);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] claimId = Bytes.sha256(Bytes.utf8("claim-result-stale-claim"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            final LaneRecord laneRecord = shard.getLane(lane);
            final byte[] timelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                    schedule.delayMessageId(), 0);
            final byte[] body = claimResultBody(shardId, claimId, schedule.delayMessageId(), 0, lane,
                    laneRecord.laneIncarnation(), 1, laneRecord.laneVersion(), Bytes.sha256(timelineKey), owner,
                    store.metadata().storeIncarnation(), 3_000, 1, 1_500, 1_500);
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT, 9_000,
                    Bytes.sha256(Bytes.utf8("claim-result-stale-operation")), body, owner, 1,
                    keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(mutation, claimResultPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.STALE_SYSTEM_MUTATION, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, shard.quota().pendingMessages());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE, timelineKey, 1));
        }
    }

    private static byte[] expiryBody(final ShardId shard, final io.nereusstream.delay.protocol.DelayMessageId messageId,
                                     final int generation, final long expireAt, final byte[] proof) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.EXPIRE_GENERATION.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, messageId.bytes());
            CanonicalProtobuf.uint32(output, 11, generation);
            CanonicalProtobuf.int64(output, 12, expireAt);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }

    private static byte[] publishOutcomeBody(final ShardId shard, final byte[] attemptId, final int sideEffect,
                                              final int disposition, final StableCode stableCode,
                                              final byte[] evidence, final byte[] observedAt) {
        final byte[] retry = sideEffect == 3 ? nestedPlaceholder() : verifiedRetryDecision(stableCode);
        return publishOutcomeBody(shard, attemptId, sideEffect, disposition, stableCode, evidence, observedAt,
                retry);
    }

    private static byte[] publishOutcomeBody(final ShardId shard, final byte[] attemptId, final int sideEffect,
                                              final int disposition, final StableCode stableCode,
                                              final byte[] evidence, final byte[] observedAt, final byte[] retry) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, attemptId);
            CanonicalProtobuf.uint32(output, 11, sideEffect);
            CanonicalProtobuf.uint32(output, 12, disposition);
            CanonicalProtobuf.uint32(output, 13, stableCode.wireValue());
            if (evidence.length != 0) {
                CanonicalProtobuf.bytes(output, 14, evidence);
            }
            CanonicalProtobuf.bytes(output, 15, sideEffect == 3 ? nestedPlaceholder() : chargeVector());
            CanonicalProtobuf.bytes(output, 16, observedAt);
            CanonicalProtobuf.bytes(output, 17, retry);
        });
    }

    private static byte[] unknownRetryDecision(final StableCode cause, final long nextRetryAt) {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown-retry-policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("unknown-retry-policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 2);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            CanonicalProtobuf.int64(output, 6, nextRetryAt);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] verifiedRetryDecision(final StableCode stableCode) {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] publishNotPublishedBody(final ShardId shard, final byte[] attemptId,
                                                  final int disposition, final StableCode stableCode,
                                                  final long nextRetryAt) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, switch (disposition) {
                case 1 -> 2;
                case 2 -> 3;
                case 3 -> 4;
                default -> throw new IllegalArgumentException("invalid test disposition");
            });
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            if (disposition != 2) {
                CanonicalProtobuf.int64(output, 6, nextRetryAt);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, attemptId);
            CanonicalProtobuf.uint32(output, 11, 2);
            CanonicalProtobuf.uint32(output, 12, disposition);
            CanonicalProtobuf.uint32(output, 13, stableCode.wireValue());
            CanonicalProtobuf.bytes(output, 14, nestedPlaceholder());
            CanonicalProtobuf.bytes(output, 15, chargeVector());
            CanonicalProtobuf.bytes(output, 16, new TrustedUtcIntervalEvidence(2_002, 2_002,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 4, 4,
                    Bytes.sha256(Bytes.utf8("retry-proof")), 0, null).canonicalBytes());
            CanonicalProtobuf.bytes(output, 17, retry);
        });
    }

    private static byte[] evidenceResolutionBody(final ShardId shard, final byte[] attemptId,
                                                 final StableCode stableCode, final int sideEffect,
                                                 final int disposition, final byte[] observedAt) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, sideEffect == 1 ? 1 : disposition == 3 ? 4 : 2);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            if (sideEffect == 2 && disposition != 2) {
                CanonicalProtobuf.int64(output, 6, 2_002);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.EVIDENCE_RESOLUTION.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, attemptId);
            CanonicalProtobuf.bytes(output, 11, nestedPlaceholder());
            CanonicalProtobuf.bytes(output, 12, nestedPlaceholder());
            CanonicalProtobuf.uint32(output, 13, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 14, sideEffect);
            CanonicalProtobuf.uint32(output, 15, disposition);
            CanonicalProtobuf.bytes(output, 16, chargeVector());
            CanonicalProtobuf.bytes(output, 17, observedAt);
            CanonicalProtobuf.bytes(output, 18, retry);
        });
    }

    private static byte[] claimResultBody(final ShardId shard, final byte[] claimId,
                                          final DelayMessageId messageId, final int generation,
                                          final DestinationLaneId lane, final byte[] laneIncarnation,
                                          final long laneControlVersion, final long runtimeLaneVersion,
                                          final byte[] timelineKey, final byte[] owner,
                                          final byte[] storeIncarnation, final long claimDeadline,
                                          final int sourceWorkKind, final long observedAtEarliest,
                                          final long observedAtLatest) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] precondition = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, claimId);
            CanonicalProtobuf.bytes(output, 2, messageId.bytes());
            CanonicalProtobuf.uint32(output, 3, generation);
            CanonicalProtobuf.int64(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, lane.bytes());
            CanonicalProtobuf.bytes(output, 6, laneIncarnation);
            CanonicalProtobuf.int64(output, 7, laneControlVersion);
            CanonicalProtobuf.int64(output, 8, runtimeLaneVersion);
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(timelineKey));
            CanonicalProtobuf.bytes(output, 12, chargeVector());
            CanonicalProtobuf.int64(output, 13, claimDeadline);
            CanonicalProtobuf.bytes(output, 14, owner);
            CanonicalProtobuf.bytes(output, 15, storeIncarnation);
            CanonicalProtobuf.uint32(output, 16, sourceWorkKind);
            CanonicalProtobuf.uint32(output, 17, 0);
            CanonicalProtobuf.uint32(output, 18, 0);
            CanonicalProtobuf.bytes(output, 19,
                    Bytes.sha256(Bytes.utf8("nereus-delay-attempt-obligation-set-v1\0")));
            CanonicalProtobuf.bytes(output, 20, claimTimelineSemanticDigest(messageId, lane, timelineKey,
                    sourceWorkKind));
        });
        final byte[] observedAt = new TrustedUtcIntervalEvidence(observedAtEarliest, observedAtLatest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("claim-clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("claim-result-proof")), 0, null).canonicalBytes();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.CLAIM_RESULT.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, claimId);
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, generation);
            CanonicalProtobuf.bytes(output, 13, lane.bytes());
            CanonicalProtobuf.bytes(output, 14, laneIncarnation);
            CanonicalProtobuf.bytes(output, 15, precondition);
            CanonicalProtobuf.uint32(output, 16, 1);
            CanonicalProtobuf.uint32(output, 17, StableCode.CLAIM_PERMANENT_FAILURE.wireValue());
            CanonicalProtobuf.bytes(output, 18, observedAt);
            CanonicalProtobuf.bytes(output, 20, chargeVector());
        });
    }

    private static byte[] claimTimelineSemanticDigest(final DelayMessageId messageId,
                                                       final DestinationLaneId lane, final byte[] timelineKey,
                                                       final int sourceWorkKind) {
        final byte[] semanticFields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, sourceWorkKind);
            CanonicalProtobuf.bytes(output, 2, timelineKey);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(timelineKey));
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-timeline-work-semantic-v1\0"), semanticFields);
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }

    private static byte[] nestedPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, new byte[]{1}));
    }

    private static int compareObligations(final AttemptObligationRef left, final AttemptObligationRef right) {
        final int id = compareUnsigned(left.publishAttemptId(), right.publishAttemptId());
        return id != 0 ? id : compareUnsigned(left.encodedInflightKey(), right.encodedInflightKey());
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic")), offset,
                1, timestamp);
    }
}
