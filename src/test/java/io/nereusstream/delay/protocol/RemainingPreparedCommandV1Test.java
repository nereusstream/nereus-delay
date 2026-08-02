package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RemainingPreparedCommandV1Test {
    @Test
    void cancelAndRescheduleV1CommandsRoundTripThroughTheStrictFrameSeam() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final PreparedCommand cancel = PreparedCommand.cancelV1(shard, messageId,
                new MessagePreconditionV1(1L, 2L), 500);
        assertEquals(cancel, CommandCodec.decodeFrameV1(CommandCodec.encodeFrameV1(cancel)));
        final PreparedCommand reschedule = PreparedCommand.rescheduleV1(shard, messageId,
                new MessagePreconditionV1(null, null), 10, 100, 500);
        assertEquals(reschedule, CommandCodec.decodeFrameV1(CommandCodec.encodeFrameV1(reschedule)));
    }
}
