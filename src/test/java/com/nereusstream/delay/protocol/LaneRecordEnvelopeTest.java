package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LaneRecordEnvelopeTest {
    @Test
    void activeAndTerminalBranchesAreCanonicalAndClosed() {
        final LaneRecordEnvelope active = LaneRecordEnvelope.active(Bytes.utf8("legacy-active-state"));
        assertEquals(active, LaneRecordEnvelope.decode(active.canonicalBytes()));
        assertEquals(java.util.Optional.empty(), active.typedActiveState());
        assertThrows(IllegalArgumentException.class, active::activeState);

        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 7, 1, 100);
        final ProfileRef destination = new ProfileRef(bytes(4, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final LaneTerminalGuard guard =
                new LaneTerminalGuard(bytes(16, 5), 3, source, destination, capability, tuple, bytes(32, 6), 9);
        final LaneRecordEnvelope terminal = LaneRecordEnvelope.terminal(guard);
        assertEquals(terminal, LaneRecordEnvelope.decode(terminal.canonicalBytes()));
        assertArrayEquals(guard.canonicalBytes(), terminal.terminalGuard().canonicalBytes());
    }

    @Test
    void typedActiveBranchRoundTripsWithoutAcceptingLegacyBytesAsFullState() {
        final ProfileRef destination = new ProfileRef(bytes(4, 7), 1, bytes(32, 8), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 9), 1, bytes(32, 10), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final ActiveLaneState state = new ActiveLaneState(
                DestinationLaneId.derive(tuple),
                bytes(16, 11),
                com.nereusstream.delay.runtime.AdmissionGate.OPEN,
                com.nereusstream.delay.runtime.RuntimeReadiness.BLOCKED,
                LaneRuntimeBlockReason.CAPABILITY,
                1,
                1,
                destination,
                capability,
                tuple,
                3,
                new PublishAdmissionBody.ChargeVector(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                null,
                null,
                LaneCircuitState.CLOSED,
                0,
                0,
                0,
                0,
                null,
                null,
                null);

        final LaneRecordEnvelope envelope = LaneRecordEnvelope.active(state);
        final LaneRecordEnvelope decoded = LaneRecordEnvelope.decode(envelope.canonicalBytes());
        assertEquals(state, decoded.activeState());
        assertEquals(java.util.Optional.of(state), decoded.typedActiveState());
        final var outer = QueryCodecSupport.read(envelope.canonicalBytes(), "LaneRecordEnvelope");
        final var typed = QueryCodecSupport.read(QueryCodecSupport.nested(outer.get(1), 10), "ActiveLaneState");
        assertEquals(0, typed.get(0).wireType());
    }

    @Test
    void malformedTypedStateIsNotDowngradedToLegacyAdapter() {
        final byte[] malformedState = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, bytes(32, 12));
        });
        final byte[] envelope = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 10, malformedState);
        });
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelope.decode(envelope));
    }

    @Test
    void branchAndPayloadMismatchIsRejected() {
        final LaneRecordEnvelope active = LaneRecordEnvelope.active(Bytes.utf8("active"));
        final byte[] encoded = active.canonicalBytes();
        encoded[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelope.decode(encoded));

        final byte[] tamperedState = active.canonicalBytes();
        tamperedState[tamperedState.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelope.decode(tamperedState));

        final byte[] duplicate = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 10, Bytes.utf8("active"));
            CanonicalProtobuf.bytes(output, 11, Bytes.utf8("terminal"));
        });
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelope.decode(duplicate));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
