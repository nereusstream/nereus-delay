package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExternalResourceBranchTest {
    @Test
    void kafkaReceiptSlotPreservesTypedIdentityAndRawUnsignedFields() {
        final KafkaReceiptSlotResourceV1 resource = new KafkaReceiptSlotResourceV1("cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000004"),
                new RouteIncarnation(bytes(16, 2)), 0x8000_0003, 0x8000_0001, Long.MIN_VALUE);

        final KafkaReceiptSlotResourceV1 decoded = KafkaReceiptSlotResourceV1.decode(resource.canonicalBytes());
        assertEquals(resource, decoded);
        assertArrayEquals(resource.canonicalBytes(), decoded.canonicalBytes());
        assertEquals(0x8000_0003, decoded.shardPartition());
        assertEquals(0x8000_0001, decoded.receiptLaneSlot());
        assertEquals(Long.MIN_VALUE, decoded.slotGeneration());

        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.KAFKA_RECEIPT_SLOT,
                        resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void pulsarJournalGenerationRequiresPulsarResourceAndRoundTrips() {
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 3), "journal", Long.MIN_VALUE));
        final PulsarJournalGenerationResourceV1 resource = new PulsarJournalGenerationResourceV1(
                broker, 0x8000_0005, Long.MIN_VALUE);

        assertEquals(resource, PulsarJournalGenerationResourceV1.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.PULSAR_JOURNAL_GENERATION,
                        resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
        assertThrows(IllegalArgumentException.class, () -> new PulsarJournalGenerationResourceV1(
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                0, 1));
    }

    private static byte[] bytes(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
