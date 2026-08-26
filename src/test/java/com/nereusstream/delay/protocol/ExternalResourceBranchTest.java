package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ExternalResourceBranchTest {
    @Test
    void kafkaReceiptSlotPreservesTypedIdentityAndRawUnsignedFields() {
        final KafkaReceiptSlotResource resource = new KafkaReceiptSlotResource(
                "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000004"),
                new RouteIncarnation(bytes(16, 2)),
                0x8000_0003,
                0x8000_0001,
                Long.MIN_VALUE);

        final KafkaReceiptSlotResource decoded = KafkaReceiptSlotResource.decode(resource.canonicalBytes());
        assertEquals(resource, decoded);
        assertArrayEquals(resource.canonicalBytes(), decoded.canonicalBytes());
        assertEquals(0x8000_0003, decoded.shardPartition());
        assertEquals(0x8000_0001, decoded.receiptLaneSlot());
        assertEquals(Long.MIN_VALUE, decoded.slotGeneration());

        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.KAFKA_RECEIPT_SLOT, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void pulsarJournalGenerationRequiresPulsarResourceAndRoundTrips() {
        final BrokerResourceIdentity broker = BrokerResourceIdentity.pulsar(
                new PulsarBrokerResourceIdentity("cluster", bytes(32, 3), "journal", Long.MIN_VALUE));
        final PulsarJournalGenerationResource resource =
                new PulsarJournalGenerationResource(broker, 0x8000_0005, Long.MIN_VALUE);

        assertEquals(resource, PulsarJournalGenerationResource.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.PULSAR_JOURNAL_GENERATION, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarJournalGenerationResource(
                        BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID())),
                        0,
                        1));
    }

    @Test
    void payloadObjectRoundTripsOptionalEtagAndRejectsWrongProfileKind() {
        final PayloadObjectResource resource = new PayloadObjectResource(
                objectStoreProfile(),
                Bytes.utf8("container"),
                Bytes.utf8("payload/key"),
                Bytes.utf8("version-7"),
                null,
                123,
                bytes(32, 11));

        final PayloadObjectResource decoded = PayloadObjectResource.decode(resource.canonicalBytes());
        assertEquals(resource, decoded);
        assertNull(decoded.etag());
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.PAYLOAD_OBJECT, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> new PayloadObjectResource(
                        new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 12), ProfileKind.DESTINATION),
                        Bytes.utf8("container"),
                        Bytes.utf8("key"),
                        Bytes.utf8("version"),
                        Bytes.utf8("etag"),
                        1,
                        bytes(32, 13)));
    }

    @Test
    void dlqExportRoundTripsWithTypedBrokerTarget() {
        final DlqExportResource resource = new DlqExportResource(
                bytes(32, 14),
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID())),
                Bytes.utf8("object-or-message-id"),
                bytes(32, 15));

        assertEquals(resource, DlqExportResource.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.DLQ_EXPORT_OBJECT, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void laneChannelRoundTripsThroughTypedChannelIdentity() {
        final LaneChannelResource resource =
                new LaneChannelResource(ChannelResourceIdentity.decode(ProtocolTestFixtures.baselineKafkaChannel()));

        assertEquals(resource, LaneChannelResource.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.LANE_CHANNEL, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void localStoreRoundTripsShardIdentityAndDbIdentity() {
        final LocalStoreResource resource = new LocalStoreResource(
                new ShardSubject(new RouteIncarnation(bytes(16, 16)), 0x8000_0007),
                bytes(16, 17),
                bytes(32, 18),
                bytes(32, 19));

        final LocalStoreResource decoded = LocalStoreResource.decode(resource.canonicalBytes());
        assertEquals(resource, decoded);
        assertEquals(0x8000_0007, decoded.shard().partition());
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.LOCAL_STORE, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    private static ProfileRef objectStoreProfile() {
        return new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 10), ProfileKind.OBJECT_STORE);
    }

    private static byte[] bytes(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
