package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ReplayDeadLetterBodyTest {
    @Test
    void canonicalEncoderRoundTripsTheExplicitReplayMutation() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final ControlRef controlRef = new ControlRef(bytes(1), bytes(2), 7);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(bytes(3), 4, bytes(4));
        final byte[] acknowledgement = bytes(5);

        final byte[] encoded = ReplayDeadLetterBody.encode(
                shard, 9_000, controlRef, messageId, 2, 11, 3_000, 8_000, retryPolicy, true, acknowledgement);
        final ReplayDeadLetterBody decoded = ReplayDeadLetterBody.decode(encoded);

        assertEquals(controlRef, decoded.controlRef());
        assertEquals(messageId, decoded.messageId());
        assertEquals(2, decoded.expectedGeneration());
        assertEquals(11, decoded.expectedStateVersion());
        assertEquals(retryPolicy, decoded.retryPolicyRef());
        assertEquals(3_000, decoded.deliverAtEpochMs());
        assertEquals(8_000, decoded.expireAtEpochMs());
        assertArrayEquals(retryPolicy.canonicalBytes(), decoded.retryPolicy());
        assertArrayEquals(acknowledgement, decoded.acknowledgementHash());
        assertArrayEquals(
                encoded,
                ReplayDeadLetterBody.encode(
                        shard,
                        9_000,
                        decoded.controlRef(),
                        decoded.messageId(),
                        decoded.expectedGeneration(),
                        decoded.expectedStateVersion(),
                        decoded.deliverAtEpochMs(),
                        decoded.expireAtEpochMs(),
                        retryPolicy,
                        decoded.allowPossibleDuplicate(),
                        decoded.acknowledgementHash()));
    }

    @Test
    void encoderRejectsAReservedAcknowledgementAndCrossShardMessage() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(bytes(6), 1, bytes(7));
        final ControlRef controlRef = new ControlRef(bytes(8), bytes(9), 0);

        assertThrows(
                IllegalArgumentException.class,
                () -> ReplayDeadLetterBody.encode(
                        shard, 1, controlRef, messageId, 0, 0, 1, 2, retryPolicy, false, bytes(10)));
        assertThrows(
                IllegalArgumentException.class,
                () -> ReplayDeadLetterBody.encode(
                        shard,
                        1,
                        controlRef,
                        DelayMessageId.random(new ShardId(RouteIncarnation.random(), 0)),
                        0,
                        0,
                        1,
                        2,
                        retryPolicy,
                        false,
                        null));
    }

    @Test
    void decoderRejectsAReplayBodyWhoseMessageRoutesToAnotherShard() {
        final ShardId bodyShard = new ShardId(RouteIncarnation.random(), 1);
        final ShardId messageShard = new ShardId(RouteIncarnation.random(), 1);
        final DelayMessageId messageId = DelayMessageId.random(messageShard);
        final ControlRef controlRef = new ControlRef(bytes(11), bytes(12), 0);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(bytes(13), 1, bytes(14));
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(bodyShard).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, 1);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 0);
            CanonicalProtobuf.int64(output, 14, 1);
            CanonicalProtobuf.int64(output, 15, 2);
            CanonicalProtobuf.bytes(output, 16, retryPolicy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 17, 0);
        });

        assertThrows(IllegalArgumentException.class, () -> ReplayDeadLetterBody.decode(encoded));
    }

    @Test
    void rejectsOpaqueRetryPolicyReference() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final ControlRef controlRef = new ControlRef(bytes(15), bytes(16), 0);
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shard).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 0);
            CanonicalProtobuf.int64(output, 14, 1);
            CanonicalProtobuf.int64(output, 15, 2);
            CanonicalProtobuf.bytes(
                    output,
                    16,
                    CanonicalProtobuf.message(nested -> CanonicalProtobuf.bytes(nested, 1, new byte[] {1})));
            CanonicalProtobuf.uint32(output, 17, 0);
        });

        assertThrows(IllegalArgumentException.class, () -> ReplayDeadLetterBody.decode(body));
    }

    private static byte[] bytes(final int seed) {
        final byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
