package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoveryFloorRefV1Test {
    @Test
    void roundTripsTypedFloorWithSortedEvidenceCursors() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final UUID topic = UUID.randomUUID();
        final EvidenceCursorV1 older = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), uuidBytes(topic),
                2, 1, 100, 11, 10);
        final EvidenceCursorV1 newer = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), uuidBytes(topic),
                2, 2, 100, 12, 11);
        final RecoveryFloorRefV1 floor = new RecoveryFloorRefV1(bytes(16, 3), bytes(16, 4), bytes(32, 5),
                7, new KafkaSourcePosition(shard, "cluster-a", topic, 90, 3, 1_000), 42,
                List.of(older, newer));

        assertEquals(floor, RecoveryFloorRefV1.decode(floor.canonicalBytes()));
        assertEquals(List.of(older, newer), floor.evidenceCursors());
    }

    @Test
    void catalogGenerationPreservesCompleteUnsigned64BitPattern() {
        final RecoveryFloorRefV1 floor = new RecoveryFloorRefV1(bytes(16, 3), bytes(16, 4), bytes(32, 5),
                Long.MIN_VALUE, source(UUID.randomUUID()), 42, List.of());

        final RecoveryFloorRefV1 decoded = RecoveryFloorRefV1.decode(floor.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.catalogGeneration());
        assertEquals(floor, decoded);
    }

    @Test
    void rejectsUnsortedCursorsAndTamperedDigest() {
        final UUID topic = UUID.randomUUID();
        final EvidenceCursorV1 older = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), uuidBytes(topic),
                2, 1, 100, 11, 10);
        final EvidenceCursorV1 newer = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), uuidBytes(topic),
                2, 2, 100, 12, 11);
        assertThrows(IllegalArgumentException.class, () -> new RecoveryFloorRefV1(bytes(16, 3), bytes(16, 4),
                bytes(32, 5), 7, source(topic), 42, List.of(newer, older)));

        final RecoveryFloorRefV1 floor = new RecoveryFloorRefV1(bytes(16, 3), bytes(16, 4), bytes(32, 5),
                7, source(topic), 42, List.of(older));
        final byte[] tampered = floor.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> RecoveryFloorRefV1.decode(tampered));
    }

    private static KafkaSourcePosition source(final UUID topic) {
        return new KafkaSourcePosition(new ShardId(RouteIncarnation.random(), 4), "cluster-a", topic, 90, 3,
                1_000);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }
}
