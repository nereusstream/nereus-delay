package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PayloadProofTrustSetControlStateTest {
    @Test
    void markersRoundTripAndCloseKeepsHistoricalVerification() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final PayloadProofTrustSetRefV1 first = ref(1, 1);
        final PayloadProofTrustSetRefV1 second = ref(2, 2);
        final KafkaSourcePosition activation = position(shard, 10, 100);
        final KafkaSourcePosition close = position(shard, 20, 200);
        final ControlReasonV1 reason =
                new ControlReasonV1(ControlReasonKindV1.POLICY_CHANGE, Bytes.sha256(Bytes.utf8("ticket")), null);
        final PayloadProofIssuanceClosePayloadV1 closePayload =
                new PayloadProofIssuanceClosePayloadV1(first, 7, reason);

        PayloadProofTrustSetControlState state =
                PayloadProofTrustSetControlState.empty().activate(first, activation);
        assertTrue(state.firstSeenIssuanceOpen(first, 7, position(shard, 15, 150)));
        state = state.close(closePayload, close);
        assertFalse(state.firstSeenIssuanceOpen(first, 7, position(shard, 21, 210)));
        assertTrue(state.historicalVerificationAllowed(first, 7, position(shard, 21, 210)));
        state = state.activate(second, position(shard, 30, 300));
        assertEquals(second, state.activeTrustSet().orElseThrow());
        assertEquals(state, PayloadProofTrustSetControlState.decode(state.canonicalBytes()));
        assertEquals(2, state.activations().size());
        assertEquals(1, state.closures().size());
    }

    @Test
    void exactMarkerReplayIsIdempotentButRegressionIsRejected() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final PayloadProofTrustSetRefV1 first = ref(4, 4);
        final KafkaSourcePosition activation = position(shard, 10, 100);
        final PayloadProofTrustSetControlState state =
                PayloadProofTrustSetControlState.empty().activate(first, activation);
        assertEquals(state, state.activate(first, activation));
        assertThrows(
                IllegalArgumentException.class,
                () -> state.activate(
                        first,
                        new KafkaSourcePosition(
                                shard,
                                "cluster-a",
                                UUID.fromString("00000000-0000-0000-0000-000000000007"),
                                10,
                                7,
                                101)));
        assertThrows(IllegalArgumentException.class, () -> state.activate(first, position(shard, 11, 101)));
        assertThrows(IllegalArgumentException.class, () -> state.activate(ref(3, 3), position(shard, 12, 102)));
        assertThrows(
                IllegalArgumentException.class,
                () -> state.close(
                        new PayloadProofIssuanceClosePayloadV1(
                                ref(99, 9), 1, new ControlReasonV1(ControlReasonKindV1.INCIDENT, null, null)),
                        position(shard, 20, 200)));
    }

    @Test
    void activationVersionsUseUnsignedOrdering() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final PayloadProofTrustSetControlState state = PayloadProofTrustSetControlState.empty()
                .activate(ref(Long.MIN_VALUE, 1), position(shard, 10, 100))
                .activate(ref(-1L, 2), position(shard, 20, 200));

        assertEquals(-1L, state.activeTrustSet().orElseThrow().version());
        assertEquals(state, PayloadProofTrustSetControlState.decode(state.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class, () -> state.activate(ref(Long.MIN_VALUE, 3), position(shard, 30, 300)));
    }

    @Test
    void canonicalOrderAndSourceIdentityAreFenced() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final PayloadProofTrustSetRefV1 first = ref(1, 5);
        final PayloadProofTrustSetRefV1 second = ref(2, 6);
        final PayloadProofTrustSetControlState state = PayloadProofTrustSetControlState.empty()
                .activate(first, position(shard, 10, 100))
                .activate(second, position(shard, 20, 200));
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, state.activations().get(1).canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, state.activations().get(0).canonicalBytes());
        });
        assertThrows(IllegalArgumentException.class, () -> PayloadProofTrustSetControlState.decode(encoded));

        final ShardId otherShard = new ShardId(RouteIncarnation.random(), 5);
        assertThrows(IllegalArgumentException.class, () -> state.activate(ref(3, 7), position(otherShard, 30, 300)));

        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.INCIDENT, null, null);
        final PayloadProofIssuanceClosePayloadV1 close = new PayloadProofIssuanceClosePayloadV1(first, 1, reason);
        assertThrows(IllegalArgumentException.class, () -> state.close(close, position(shard, 5, 50)));
    }

    private static PayloadProofTrustSetRefV1 ref(final long version, final int seed) {
        return new PayloadProofTrustSetRefV1(version, bytes(32, seed));
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long time) {
        return new KafkaSourcePosition(
                shard, "cluster-a", UUID.fromString("00000000-0000-0000-0000-000000000007"), offset, null, time);
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        result[0] = (byte) value;
        return result;
    }
}
