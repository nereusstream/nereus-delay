package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class RemainingClientCommandBodyTest {
    @Test
    void preconditionPresenceIsIndependentAndBodiesRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final MessagePrecondition empty = new MessagePrecondition(null, null);
        final MessagePrecondition both = new MessagePrecondition(3L, 11L);
        assertEquals(empty, MessagePrecondition.decode(empty.canonicalBytes()));
        assertEquals(both, MessagePrecondition.decode(both.canonicalBytes()));

        final CancelCommandBody cancel = new CancelCommandBody(messageId, 100, empty);
        assertEquals(cancel, CancelCommandBody.decode(cancel.canonicalBytes()));
        final RescheduleCommandBody reschedule = new RescheduleCommandBody(messageId, 100, both, 20, 40);
        assertEquals(reschedule, RescheduleCommandBody.decode(reschedule.canonicalBytes()));
        assertEquals(cancel, CommandBodies.decodeCancel(CommandBodies.cancel(messageId, 100, empty)));
        assertEquals(
                reschedule, CommandBodies.decodeReschedule(CommandBodies.reschedule(messageId, 100, both, 20, 40)));
    }

    @Test
    void rejectsInvalidPresenceTimingAndCanonicalBytes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MessagePrecondition.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint64(output, 2, 1);
                    CanonicalProtobuf.uint32(output, 1, 1);
                })));
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RescheduleCommandBody(messageId, 100, new MessagePrecondition(null, null), 40, 20));
        final byte[] badCancel =
                new CancelCommandBody(messageId, 100, new MessagePrecondition(null, null)).canonicalBytes();
        badCancel[2] = 3;
        assertThrows(IllegalArgumentException.class, () -> CancelCommandBody.decode(badCancel));
    }
}
