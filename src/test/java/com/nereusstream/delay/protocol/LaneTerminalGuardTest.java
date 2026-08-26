package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LaneTerminalGuardTest {
    @Test
    void terminalGuardAndRetirementProgressRoundTrip() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, 2, 100);
        final ProfileRef destination = new ProfileRef(bytes(4, 1), 2, bytes(32, 2), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final LaneTerminalGuard guard =
                new LaneTerminalGuard(bytes(16, 5), 7, source, destination, capability, tuple, bytes(32, 6), 8);
        final LaneRetirementProgress progress = new LaneRetirementProgress(bytes(32, 7), 9, source);

        assertArrayEquals(
                guard.canonicalBytes(),
                LaneTerminalGuard.decode(guard.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                progress.canonicalBytes(),
                LaneRetirementProgress.decode(progress.canonicalBytes()).canonicalBytes());
    }

    @Test
    void retirementMutationSequencesPreserveCompleteUnsigned64BitPatterns() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9, 2, 100);
        final ProfileRef destination = new ProfileRef(bytes(4, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final long highBit = Long.MIN_VALUE;
        final LaneTerminalGuard guard =
                new LaneTerminalGuard(bytes(16, 5), 7, source, destination, capability, tuple, bytes(32, 6), highBit);
        final LaneRetirementProgress progress = new LaneRetirementProgress(bytes(32, 7), highBit, source);

        assertEquals(highBit, LaneTerminalGuard.decode(guard.canonicalBytes()).retirementMutationSequence());
        assertEquals(
                highBit,
                LaneRetirementProgress.decode(progress.canonicalBytes()).appliedShardMutationSequence());
    }

    @Test
    void guardRejectsIdentityOrDigestDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1, null, 10);
        final ProfileRef destination = new ProfileRef(bytes(4, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final LaneTerminalGuard guard =
                new LaneTerminalGuard(bytes(16, 3), 1, source, destination, capability, tuple, bytes(32, 4), 1);
        final byte[] tampered = guard.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneTerminalGuard.decode(tampered));
    }

    @Test
    void guardRequiresDestinationAndCapabilityProfileSlots() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1, null, 10);
        final ProfileRef destination = new ProfileRef(bytes(4, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);

        assertThrows(
                IllegalArgumentException.class,
                () -> new LaneTerminalGuard(bytes(16, 5), 1, source, capability, capability, tuple, bytes(32, 6), 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LaneTerminalGuard(bytes(16, 5), 1, source, destination, destination, tuple, bytes(32, 6), 1));
    }

    @Test
    void guardRejectsProfileProjectionDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 1, null, 10);
        final ProfileRef destination = new ProfileRef(bytes(4, 1), 1, bytes(32, 2), ProfileKind.DESTINATION);
        final ProfileRef capability = new ProfileRef(bytes(4, 3), 1, bytes(32, 4), ProfileKind.DELIVERY_CAPABILITY);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final ProfileRef driftedDestination =
                new ProfileRef(bytes(4, 9), destination.version(), destination.semanticHash(), ProfileKind.DESTINATION);

        assertThrows(
                IllegalArgumentException.class,
                () -> new LaneTerminalGuard(
                        bytes(16, 5), 1, source, driftedDestination, capability, tuple, bytes(32, 6), 1));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
