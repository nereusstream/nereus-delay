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
    void preservesUnsignedPartitionAndBatchFields() {
        final EvidenceCursorV1 kafka = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                -1, Long.MIN_VALUE, 100, Long.MIN_VALUE, -1L);
        assertEquals(kafka, EvidenceCursorV1.decode(kafka.canonicalBytes()));

        final EvidenceCursorV1 pulsar = EvidenceCursorV1.pulsar(bytes(32, 4), bytes(16, 5), bytes(32, 6),
                -1, Long.MIN_VALUE, 200, "persistent://tenant/ns/topic", 8, Long.MIN_VALUE, -1L,
                Integer.MIN_VALUE, Integer.MIN_VALUE + 1);
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

    @Test
    void dominanceIsMonotonicWithinOneGenerationAndRejectsGapsOrGenerationChanges() {
        final EvidenceCursorV1 olderKafka = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 4, 100, 11, 10);
        final EvidenceCursorV1 newerKafka = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 4, 101, 12, 11);
        final EvidenceCursorV1 regressedLso = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 4, 101, 12, 9);
        final EvidenceCursorV1 otherGeneration = EvidenceCursorV1.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3),
                1, 5, 101, 12, 11);
        assertTrue(newerKafka.sameIdentity(olderKafka));
        assertTrue(newerKafka.dominates(olderKafka));
        assertTrue(newerKafka.strictlyDominates(olderKafka));
        assertTrue(!regressedLso.dominates(olderKafka));
        assertTrue(!newerKafka.sameIdentity(otherGeneration));
        assertTrue(!newerKafka.dominates(otherGeneration));
    }

    @Test
    void pulsarDominanceUsesInclusiveLedgerEntryAndBatchMember() {
        final EvidenceCursorV1 older = EvidenceCursorV1.pulsar(bytes(32, 8), bytes(16, 9), bytes(32, 10),
                2, 7, 200, "persistent://tenant/ns/topic", 8, 9, 10, 1, 4);
        final EvidenceCursorV1 newer = EvidenceCursorV1.pulsar(bytes(32, 8), bytes(16, 9), bytes(32, 10),
                2, 7, 201, "persistent://tenant/ns/topic", 8, 9, 10, 3, 4);
        final EvidenceCursorV1 regressed = EvidenceCursorV1.pulsar(bytes(32, 8), bytes(16, 9), bytes(32, 10),
                2, 7, 201, "persistent://tenant/ns/topic", 8, 9, 9, 0, 4);
        assertTrue(newer.dominates(older));
        assertTrue(!regressed.dominates(older));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }
}
