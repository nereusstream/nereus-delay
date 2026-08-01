package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProof;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
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

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic")), offset,
                1, timestamp);
    }
}
