package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class CommandProtocolTupleTest {
    @Test
    void commandHashBindsTheProtocolTupleWithoutChangingTheManagedVector() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final CommandId commandId = CommandId.random(shard);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final long retryUntil = commandId.routingId().logicalTimestampEpochMs() + 10_000;
        final byte[] body = CommandBodies.schedule(new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("protocol-tuple-hash")),
                retryUntil - 9_000,
                retryUntil - 5_000,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("payload")));
        final PreparedCommand managed = PreparedCommand.create(
                shard, commandId, messageId, CommandType.SCHEDULE, ProtocolTuple.managedCommand(), retryUntil, body);
        final ProtocolTuple nextTuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 2);
        final PreparedCommand next =
                PreparedCommand.create(shard, commandId, messageId, CommandType.SCHEDULE, nextTuple, retryUntil, body);

        assertEquals(ProtocolTuple.managedCommand(), managed.protocolTuple());
        assertArrayEquals(
                managed.commandHash(),
                CommandHash.compute(
                        managed.type(),
                        managed.commandId(),
                        managed.delayMessageId(),
                        managed.retryUntilEpochMs(),
                        managed.canonicalBody()));
        assertArrayEquals(
                managed.commandHash(),
                CommandHash.compute(
                        managed.protocolTuple(),
                        managed.type(),
                        managed.commandId(),
                        managed.delayMessageId(),
                        managed.retryUntilEpochMs(),
                        managed.canonicalBody()));
        assertFalse(Arrays.equals(managed.commandHash(), next.commandHash()));
        assertThrows(IllegalArgumentException.class, () -> CommandCodec.encodeFrame(next));
        assertThrows(IllegalArgumentException.class, () -> CanonicalCommandQueuedReceipt.PreparedCommandRef.from(next));
    }

    @Test
    void preparedCommandsRejectSystemMutationTuples() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final CommandId commandId = CommandId.random(shard);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final long retryUntil = commandId.routingId().logicalTimestampEpochMs() + 10_000;
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedCommand.create(
                        shard,
                        commandId,
                        messageId,
                        CommandType.SCHEDULE,
                        new ProtocolTuple(1, 1, ProtocolTuple.SYSTEM_MUTATION, 1, 1),
                        retryUntil,
                        new byte[0]));
    }
}
