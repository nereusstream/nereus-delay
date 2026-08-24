package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.SourcePosition;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LaneCloseMaterializationCursorTest {
    @Test
    void canonicalRoundTripPreservesPhaseAndResumeKey() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("close-cursor-lane"));
        final SourcePosition position = new KafkaSourcePosition(
                new com.nereusstream.delay.protocol.ShardId(new RouteIncarnation(bytes(16, 1)), 3),
                "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000002"),
                17,
                null,
                1_000);
        final LaneCloseMaterializationCursor cursor = new LaneCloseMaterializationCursor(
                lane,
                bytes(16, 7),
                42,
                position.canonicalBytes(),
                LaneCloseMaterializationCursor.Phase.MESSAGES,
                Bytes.concat(new byte[] {1, 1}, bytes(41, 9)),
                3,
                12,
                2,
                8);

        final LaneCloseMaterializationCursor decoded = LaneCloseMaterializationCursor.decode(cursor.canonicalBytes());
        assertEquals(cursor.laneId(), decoded.laneId());
        assertArrayEquals(cursor.laneIncarnation(), decoded.laneIncarnation());
        assertEquals(cursor.closeVersion(), decoded.closeVersion());
        assertEquals(cursor.phase(), decoded.phase());
        assertArrayEquals(cursor.lastKey(), decoded.lastKey());
        assertArrayEquals(cursor.digest(), decoded.digest());
        assertArrayEquals(cursor.canonicalBytes(), decoded.canonicalBytes());
        assertEquals(
                LaneCloseMaterializationCursor.Phase.RESERVATIONS,
                cursor.nextPhase().phase());
    }

    @Test
    void rejectsDigestAndSourceIdentityDrift() {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("close-cursor-invalid"));
        final SourcePosition position = new KafkaSourcePosition(
                new com.nereusstream.delay.protocol.ShardId(new RouteIncarnation(bytes(16, 3)), 4),
                "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000004"),
                19,
                null,
                1_001);
        final LaneCloseMaterializationCursor cursor = new LaneCloseMaterializationCursor(
                lane,
                bytes(16, 8),
                1,
                position.canonicalBytes(),
                LaneCloseMaterializationCursor.Phase.RESERVATIONS,
                null,
                0,
                0,
                0,
                0);
        final byte[] encoded = cursor.canonicalBytes();
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> LaneCloseMaterializationCursor.decode(encoded));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LaneCloseMaterializationCursor(
                        lane,
                        bytes(16, 8),
                        1,
                        Bytes.utf8("not-a-source-position"),
                        LaneCloseMaterializationCursor.Phase.MESSAGES,
                        null,
                        0,
                        0,
                        0,
                        0));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }
}
