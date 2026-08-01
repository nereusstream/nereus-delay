package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EvidenceCursorV1Test {
    @Test
    void roundTripsKafkaAndPulsarCursors() {
        final EvidenceCursorV1 kafka = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 4, 100, 11, 10);
        assertEquals(kafka, EvidenceCursorV1.decode(kafka.canonicalBytes()));

        final EvidenceCursorV1 pulsar = EvidenceCursorV1.pulsar(bytes(32, 4), bytes(16, 5), bytes(32, 6),
                2, 7, 200, "persistent://tenant/ns/topic", 8, 9, 10, 1, 2);
        assertEquals(pulsar, EvidenceCursorV1.decode(pulsar.canonicalBytes()));
    }

    @Test
    void rejectsResourceBranchAndBatchShapeMismatch() {
        assertThrows(IllegalArgumentException.class,
                () -> EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(32, 3), 0, 1, 1, 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> EvidenceCursorV1.pulsar(bytes(32, 1), bytes(16, 2), bytes(32, 3), 0, 1, 1,
                        "topic", 1, 1, 1, 2, 2));
    }

    @Test
    void orderingIncludesEvidenceGenerationInTheIdentityKey() {
        final EvidenceCursorV1 older = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 4, 100, 11, 10);
        final EvidenceCursorV1 newer = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 5, 100, 11, 10);
        assertTrue(older.compareTo(newer) < 0);
        assertTrue(newer.compareTo(older) > 0);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }
}
