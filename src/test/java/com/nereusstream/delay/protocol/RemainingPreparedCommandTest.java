package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class RemainingPreparedCommandTest {
    @Test
    void cancelAndRescheduleCommandsRoundTripThroughTheStrictFrameSeam() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final PreparedCommand cancel = PreparedCommand.cancel(shard, messageId, new MessagePrecondition(1L, 2L), 500);
        assertEquals(cancel, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(cancel)));
        final PreparedCommand reschedule =
                PreparedCommand.reschedule(shard, messageId, new MessagePrecondition(null, null), 10, 100, 500);
        assertEquals(reschedule, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(reschedule)));
    }
}
