package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.store.KeyCodec;
import org.junit.jupiter.api.Test;

class ReadyIndexValueTest {
    @Test
    void dualReadyRoundTripsBothHeadsAndTheMinimumPersistentWake() {
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("ready-lane")));
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(1L, 2L)), 3);
        final DelayMessageId ordinaryMessage = DelayMessageId.random(shard);
        final DelayMessageId nativeMessage = DelayMessageId.random(shard);
        final byte[] sourceOrderToken = new byte[21];
        sourceOrderToken[0] = 2;
        final byte[] ordinaryKey = KeyCodec.timelineDue(lane, 2_000, sourceOrderToken, ordinaryMessage, 4);
        final byte[] nativeKey = KeyCodec.timelineNativeCandidate(lane, 1_000, sourceOrderToken, nativeMessage, 5);
        final HandoffPolicyHeadRef headRef = new HandoffPolicyHeadRef(
                Bytes.sha256(Bytes.utf8("scope")), 9, Bytes.sha256(Bytes.utf8("snapshot")), 17);
        final ReadyIndexValue ordinary =
                new ReadyIndexValue(lane, 2_000, 11, ordinaryMessage, 4, Bytes.sha256(ordinaryKey));
        final ReadyIndexValue nativeHead =
                ReadyIndexValue.nativeCandidate(lane, 1_000, 11, nativeMessage, 5, Bytes.sha256(nativeKey), headRef);
        final ReadyIndexValue value = ordinary.withNativeHead(nativeHead);

        final ReadyIndexValue decoded = ReadyIndexValue.decode(value.encode());

        assertEquals(1_000, decoded.persistentWakeAtEpochMs());
        assertEquals(2_000, decoded.nextEligibleAtEpochMs());
        assertEquals(nativeMessage, decoded.nativeHead().messageId());
        assertEquals(headRef, decoded.nativeHead().policyHeadRef());
        assertArrayEquals(value.stateDigest(), decoded.stateDigest());
        assertArrayEquals(value.encode(), decoded.encode());
        assertNotNull(decoded.nativeHead());
    }

    @Test
    void dualReadyDigestAndNestedIdentityAreFailClosed() {
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("ready-lane-2")));
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(3L, 4L)), 5);
        final DelayMessageId ordinaryMessage = DelayMessageId.random(shard);
        final DelayMessageId nativeMessage = DelayMessageId.random(shard);
        final byte[] token = new byte[21];
        token[0] = 2;
        final HandoffPolicyHeadRef ref = new HandoffPolicyHeadRef(
                Bytes.sha256(Bytes.utf8("scope-2")), 1, Bytes.sha256(Bytes.utf8("snapshot-2")), 2);
        final ReadyIndexValue value = new ReadyIndexValue(
                        lane,
                        4_000,
                        2,
                        ordinaryMessage,
                        1,
                        Bytes.sha256(KeyCodec.timelineDue(lane, 4_000, token, ordinaryMessage, 1)))
                .withNativeHead(ReadyIndexValue.nativeCandidate(
                        lane,
                        3_000,
                        2,
                        nativeMessage,
                        1,
                        Bytes.sha256(KeyCodec.timelineNativeCandidate(lane, 3_000, token, nativeMessage, 1)),
                        ref));

        final byte[] tampered = value.encode();
        tampered[tampered.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> ReadyIndexValue.decode(tampered));
        assertThrows(IllegalStateException.class, () -> ReadyIndexValue.nativeCandidate(
                        lane,
                        3_000,
                        3,
                        nativeMessage,
                        1,
                        Bytes.sha256(KeyCodec.timelineNativeCandidate(lane, 3_000, token, nativeMessage, 1)),
                        ref)
                .withNativeHead(ReadyIndexValue.nativeCandidate(
                        lane,
                        3_001,
                        2,
                        nativeMessage,
                        1,
                        Bytes.sha256(KeyCodec.timelineNativeCandidate(lane, 3_001, token, nativeMessage, 1)),
                        ref)));
    }
}
