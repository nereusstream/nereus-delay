package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DelayShardTest {
    @TempDir
    Path tempDir;

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

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
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
        final Fixture fixture = Fixture.create(shardId, messageId);
        final DestinationLaneId lane = new DestinationLaneId(fixture.lane());
        final PreparedCommand schedule = PreparedCommand.create(shardId, io.nereusstream.delay.protocol.CommandId.random(shardId),
                messageId, io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("hello"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
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
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] retry = sideEffect == 3 ? nestedPlaceholder() : verifiedRetryDecision(stableCode);
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

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic")), offset,
                1, timestamp);
    }
}
