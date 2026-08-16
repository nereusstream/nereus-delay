package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandBodies;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.CommandType;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
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

class CommandProtocolDedupeApplyTest {
    @TempDir
    Path tempDir;

    @Test
    void sameCommandIdWithADifferentProtocolTupleIsAConflictNotADuplicate() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("protocol-tuple-conflict"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final long brokerTime = commandId.routingId().logicalTimestampEpochMs();
        final long retryUntil = brokerTime + 30_000;
        final byte[] body = CommandBodies.schedule(new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("protocol-tuple-conflict-lane")), brokerTime + 1_000,
                brokerTime + 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")));
        final PreparedCommand managed = PreparedCommand.create(shardId, commandId, messageId, CommandType.SCHEDULE,
                ProtocolTupleV1.managedCommandV1(), retryUntil, body);
        final PreparedCommand next = PreparedCommand.create(shardId, commandId, messageId, CommandType.SCHEDULE,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 2), retryUntil, body);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(managed, position(shardId, 0, brokerTime)).stableCode());
            assertEquals(StableCode.COMMAND_ID_CONFLICT,
                    shard.apply(next, position(shardId, 1, brokerTime + 1)).stableCode());
            assertEquals(StableCode.SCHEDULED, shard.getCommandResult(commandId).stableCode());

            final byte[] encoded = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeCommand(commandId), 1).payload();
            assertEquals(ProtocolTupleV1.managedCommandV1(), CommandDedupeRecord.decode(encoded).protocolTuple());
        }
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("protocol-tuple-topic")),
                offset, 1, timestamp);
    }
}
