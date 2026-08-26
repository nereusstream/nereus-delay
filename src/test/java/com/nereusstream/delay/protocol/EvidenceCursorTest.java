package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class EvidenceCursorTest {
    @Test
    void roundTripsKafkaAndPulsarCursors() {
        final EvidenceCursor kafka = EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 4, 100, 11, 10);
        assertEquals(kafka, EvidenceCursor.decode(kafka.canonicalBytes()));

        final EvidenceCursor pulsar = EvidenceCursor.pulsar(
                bytes(32, 4), bytes(16, 5), bytes(32, 6), 2, 7, 200, "persistent://tenant/ns/topic", 8, 9, 10, 1, 2);
        assertEquals(pulsar, EvidenceCursor.decode(pulsar.canonicalBytes()));
    }

    @Test
    void preservesUnsignedPartitionAndBatchFields() {
        final EvidenceCursor kafka = EvidenceCursor.kafka(
                bytes(32, 1), bytes(16, 2), bytes(16, 3), -1, Long.MIN_VALUE, 100, Long.MIN_VALUE, -1L);
        assertEquals(kafka, EvidenceCursor.decode(kafka.canonicalBytes()));

        final EvidenceCursor pulsar = EvidenceCursor.pulsar(
                bytes(32, 4),
                bytes(16, 5),
                bytes(32, 6),
                -1,
                Long.MIN_VALUE,
                200,
                "persistent://tenant/ns/topic",
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                -1L,
                Integer.MIN_VALUE,
                Integer.MIN_VALUE + 1);
        assertEquals(pulsar, EvidenceCursor.decode(pulsar.canonicalBytes()));
        assertEquals(
                Long.MIN_VALUE, EvidenceCursor.decode(pulsar.canonicalBytes()).physicalTopicCreationTimestamp());
    }

    @Test
    void rejectsResourceBranchAndBatchShapeMismatch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(32, 3), 0, 1, 1, 1, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> EvidenceCursor.pulsar(bytes(32, 1), bytes(16, 2), bytes(32, 3), 0, 1, 1, "topic", 1, 1, 1, 2, 2));
    }

    @Test
    void orderingIncludesEvidenceGenerationInTheIdentityKey() {
        final EvidenceCursor older = EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 4, 100, 11, 10);
        final EvidenceCursor newer = EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 5, 100, 11, 10);
        assertTrue(older.compareTo(newer) < 0);
        assertTrue(newer.compareTo(older) > 0);
    }

    @Test
    void dominanceIsMonotonicWithinOneGenerationAndRejectsGapsOrGenerationChanges() {
        final EvidenceCursor olderKafka =
                EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 4, 100, 11, 10);
        final EvidenceCursor newerKafka =
                EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 4, 101, 12, 11);
        final EvidenceCursor regressedLso =
                EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 4, 101, 12, 9);
        final EvidenceCursor otherGeneration =
                EvidenceCursor.kafka(bytes(32, 1), bytes(16, 2), bytes(16, 3), 1, 5, 101, 12, 11);
        assertTrue(newerKafka.sameIdentity(olderKafka));
        assertTrue(newerKafka.dominates(olderKafka));
        assertTrue(newerKafka.strictlyDominates(olderKafka));
        assertTrue(!regressedLso.dominates(olderKafka));
        assertTrue(!newerKafka.sameIdentity(otherGeneration));
        assertTrue(!newerKafka.dominates(otherGeneration));
    }

    @Test
    void pulsarDominanceUsesInclusiveLedgerEntryAndBatchMember() {
        final EvidenceCursor older = EvidenceCursor.pulsar(
                bytes(32, 8), bytes(16, 9), bytes(32, 10), 2, 7, 200, "persistent://tenant/ns/topic", 8, 9, 10, 1, 4);
        final EvidenceCursor newer = EvidenceCursor.pulsar(
                bytes(32, 8), bytes(16, 9), bytes(32, 10), 2, 7, 201, "persistent://tenant/ns/topic", 8, 9, 10, 3, 4);
        final EvidenceCursor regressed = EvidenceCursor.pulsar(
                bytes(32, 8), bytes(16, 9), bytes(32, 10), 2, 7, 201, "persistent://tenant/ns/topic", 8, 9, 9, 0, 4);
        assertTrue(newer.dominates(older));
        assertTrue(!regressed.dominates(older));
    }

    @Test
    void pulsarCursorIdentityIncludesPhysicalTopicCreationIdentity() {
        final EvidenceCursor original = EvidenceCursor.pulsar(
                bytes(32, 11), bytes(16, 12), bytes(32, 13), 2, 7, 200, "persistent://tenant/ns/topic", 8, 9, 10, 1, 4);
        final EvidenceCursor replacementTopic = EvidenceCursor.pulsar(
                bytes(32, 11),
                bytes(16, 12),
                bytes(32, 13),
                2,
                7,
                201,
                "persistent://tenant/ns/replacement",
                8,
                9,
                10,
                2,
                4);
        final EvidenceCursor replacementCreation = EvidenceCursor.pulsar(
                bytes(32, 11), bytes(16, 12), bytes(32, 13), 2, 7, 201, "persistent://tenant/ns/topic", 9, 9, 10, 2, 4);

        assertTrue(!original.sameIdentity(replacementTopic));
        assertTrue(!original.dominates(replacementTopic));
        assertTrue(!original.sameIdentity(replacementCreation));
        assertTrue(!original.dominates(replacementCreation));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }
}
