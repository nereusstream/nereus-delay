package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneRecordEnvelopeV1Test {
    @Test
    void activeAndTerminalBranchesAreCanonicalAndClosed() {
        final LaneRecordEnvelopeV1 active = LaneRecordEnvelopeV1.active(Bytes.utf8("legacy-active-state"));
        assertEquals(active, LaneRecordEnvelopeV1.decode(active.canonicalBytes()));

        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 7,
                1, 100);
        final ProfileRefV1 destination = new ProfileRefV1(bytes(4, 1), 1, bytes(32, 2),
                ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(bytes(4, 3), 1, bytes(32, 4),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final LaneTerminalGuardV1 guard = new LaneTerminalGuardV1(bytes(16, 5), 3, source, destination,
                capability, Bytes.utf8("tuple"), bytes(32, 6), 9);
        final LaneRecordEnvelopeV1 terminal = LaneRecordEnvelopeV1.terminal(guard);
        assertEquals(terminal, LaneRecordEnvelopeV1.decode(terminal.canonicalBytes()));
        assertArrayEquals(guard.canonicalBytes(), terminal.terminalGuard().canonicalBytes());
    }

    @Test
    void branchAndPayloadMismatchIsRejected() {
        final LaneRecordEnvelopeV1 active = LaneRecordEnvelopeV1.active(Bytes.utf8("active"));
        final byte[] encoded = active.canonicalBytes();
        encoded[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelopeV1.decode(encoded));

        final byte[] tamperedState = active.canonicalBytes();
        tamperedState[tamperedState.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelopeV1.decode(tamperedState));

        final byte[] duplicate = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 10, Bytes.utf8("active"));
            CanonicalProtobuf.bytes(output, 11, Bytes.utf8("terminal"));
        });
        assertThrows(IllegalArgumentException.class, () -> LaneRecordEnvelopeV1.decode(duplicate));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
