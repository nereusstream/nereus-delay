package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneTerminalGuardV1Test {
    @Test
    void terminalGuardAndRetirementProgressRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9,
                2, 100);
        final ProfileRefV1 destination = new ProfileRefV1(bytes(4, 1), 2, bytes(32, 2),
                ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(bytes(4, 3), 1, bytes(32, 4),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final LaneTerminalGuardV1 guard = new LaneTerminalGuardV1(bytes(16, 5), 7, source, destination,
                capability, Bytes.utf8("canonical-lane-tuple"), bytes(32, 6), 8);
        final LaneRetirementProgressV1 progress = new LaneRetirementProgressV1(bytes(32, 7), 9, source);

        assertArrayEquals(guard.canonicalBytes(), LaneTerminalGuardV1.decode(guard.canonicalBytes()).canonicalBytes());
        assertArrayEquals(progress.canonicalBytes(),
                LaneRetirementProgressV1.decode(progress.canonicalBytes()).canonicalBytes());
    }

    @Test
    void guardRejectsIdentityOrDigestDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1,
                null, 10);
        final ProfileRefV1 profile = new ProfileRefV1(bytes(4, 1), 1, bytes(32, 2), ProfileKindV1.DESTINATION);
        final LaneTerminalGuardV1 guard = new LaneTerminalGuardV1(bytes(16, 3), 1, source, profile, profile,
                Bytes.utf8("tuple"), bytes(32, 4), 1);
        final byte[] tampered = guard.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneTerminalGuardV1.decode(tampered));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
