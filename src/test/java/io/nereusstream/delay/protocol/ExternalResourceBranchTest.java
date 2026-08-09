package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @Test
    void payloadObjectRoundTripsOptionalEtagAndRejectsWrongProfileKind() {
        final PayloadObjectResourceV1 resource = new PayloadObjectResourceV1(objectStoreProfile(),
                Bytes.utf8("container"), Bytes.utf8("payload/key"), Bytes.utf8("version-7"), null,
                123, bytes(32, 11));

        final PayloadObjectResourceV1 decoded = PayloadObjectResourceV1.decode(resource.canonicalBytes());
        assertEquals(resource, decoded);
        assertNull(decoded.etag());
        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.PAYLOAD_OBJECT,
                        resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
        assertThrows(IllegalArgumentException.class, () -> new PayloadObjectResourceV1(
                new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 12), ProfileKindV1.DESTINATION),
                Bytes.utf8("container"), Bytes.utf8("key"), Bytes.utf8("version"), Bytes.utf8("etag"),
                1, bytes(32, 13)));
    }

    @Test
    void dlqExportRoundTripsWithTypedBrokerTarget() {
        final DlqExportResourceV1 resource = new DlqExportResourceV1(bytes(32, 14),
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                Bytes.utf8("object-or-message-id"), bytes(32, 15));

        assertEquals(resource, DlqExportResourceV1.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.DLQ_EXPORT_OBJECT,
                        resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void laneChannelRoundTripsThroughTypedChannelIdentity() {
        final LaneChannelResourceV1 resource = new LaneChannelResourceV1(
                ChannelResourceIdentityV1.decode(ProtocolTestFixtures.baselineKafkaChannel()));

        assertEquals(resource, LaneChannelResourceV1.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.LANE_CHANNEL,
                        resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void localStoreRoundTripsShardIdentityAndDbIdentity() {
        final LocalStoreResourceV1 resource = new LocalStoreResourceV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 16)), 0x8000_0007), bytes(16, 17),
                bytes(32, 18), bytes(32, 19));

        final LocalStoreResourceV1 decoded = LocalStoreResourceV1.decode(resource.canonicalBytes());
        assertEquals(resource, decoded);
        assertEquals(0x8000_0007, decoded.shard().partition());
        final ResourceRetireIntentBody.ExactResourceIdentity identity =
                ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.LOCAL_STORE,
                        resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    private static ProfileRefV1 objectStoreProfile() {
        return new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 10), ProfileKindV1.OBJECT_STORE);
    }

    private static byte[] bytes(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
