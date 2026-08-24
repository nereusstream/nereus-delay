package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class RemainingClientCommandBodyV1Test {
    @Test
    void preconditionPresenceIsIndependentAndBodiesRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final MessagePreconditionV1 empty = new MessagePreconditionV1(null, null);
        final MessagePreconditionV1 both = new MessagePreconditionV1(3L, 11L);
        assertEquals(empty, MessagePreconditionV1.decode(empty.canonicalBytes()));
        assertEquals(both, MessagePreconditionV1.decode(both.canonicalBytes()));

        final CancelCommandBodyV1 cancel = new CancelCommandBodyV1(messageId, 100, empty);
        assertEquals(cancel, CancelCommandBodyV1.decode(cancel.canonicalBytes()));
        final RescheduleCommandBodyV1 reschedule = new RescheduleCommandBodyV1(messageId, 100, both, 20, 40);
        assertEquals(reschedule, RescheduleCommandBodyV1.decode(reschedule.canonicalBytes()));
        assertEquals(cancel, CommandBodies.decodeCancelV1(CommandBodies.cancelV1(messageId, 100, empty)));
        assertEquals(
                reschedule, CommandBodies.decodeRescheduleV1(CommandBodies.rescheduleV1(messageId, 100, both, 20, 40)));
    }

    @Test
    void rejectsInvalidPresenceTimingAndCanonicalBytes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MessagePreconditionV1.decode(CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.uint64(output, 2, 1);
                    CanonicalProtobuf.uint32(output, 1, 1);
                })));
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RescheduleCommandBodyV1(messageId, 100, new MessagePreconditionV1(null, null), 40, 20));
        final byte[] badCancel =
                new CancelCommandBodyV1(messageId, 100, new MessagePreconditionV1(null, null)).canonicalBytes();
        badCancel[2] = 3;
        assertThrows(IllegalArgumentException.class, () -> CancelCommandBodyV1.decode(badCancel));
    }
}
