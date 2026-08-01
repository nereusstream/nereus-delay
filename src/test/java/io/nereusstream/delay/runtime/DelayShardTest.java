package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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

            assertEquals(scheduled, shard.apply(schedule, position0));

            final PreparedCommand reschedule = PreparedCommand.reschedule(shardId, schedule.delayMessageId(), 0,
                    3_000, 6_000, 9_000);
            final CommandResult superseded = shard.apply(reschedule, position1);
            assertEquals(StableCode.SUPERSEDED, superseded.stableCode());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).generation());

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 1, 9_000);
            final CommandResult canceled = shard.apply(cancel, position2);
            assertEquals(StableCode.CANCELED, canceled.stableCode());
            assertEquals(MessageStatus.CANCELED, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 3_000, position1.sourceOrderToken(), schedule.delayMessageId(), 1), 1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(6_000, lane, schedule.delayMessageId(), 1), 1));
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

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic")), offset,
                1, timestamp);
    }
}
