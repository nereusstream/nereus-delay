package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DeliveryCapabilitySemanticTest {
    @Test
    void roundTripsBaselineKafkaCapability() {
        final DeliveryCapabilitySemantic capability = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 1),
                bytes(32, 2),
                0,
                0);

        assertEquals(capability, DeliveryCapabilitySemantic.decode(capability.canonicalBytes()));
        assertTrue(!capability.requiresEvidenceResource());
        assertTrue(TimingCapability.includes(capability.timingCapabilityBits(), TimingCapability.ORDINARY_MANAGED));
    }

    @Test
    void roundTripsStrongKafkaAndPulsarCapabilities() {
        final BrokerResourceIdentity kafkaEvidence =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("evidence-cluster", UUID.randomUUID()));
        final DeliveryCapabilitySemantic kafka = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.KAFKA_TRANSACTIONAL_RECEIPT,
                TimingCapability.ORDINARY_MANAGED,
                kafkaEvidence,
                3,
                10_000,
                20_000,
                4,
                bytes(32, 3),
                bytes(32, 4),
                1,
                2);
        assertEquals(kafka, DeliveryCapabilitySemantic.decode(kafka.canonicalBytes()));

        final BrokerResourceIdentity pulsarEvidence = BrokerResourceIdentity.pulsar(
                new PulsarBrokerResourceIdentity("evidence-cluster", bytes(32, 5), "journal", 7));
        final DeliveryCapabilitySemantic pulsar = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.PULSAR_BROKER_DEDUP,
                TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_GUARDED_HANDOFF,
                pulsarEvidence,
                2,
                10_000,
                20_000,
                4,
                bytes(32, 6),
                bytes(32, 7),
                1,
                1);
        assertEquals(pulsar, DeliveryCapabilitySemantic.decode(pulsar.canonicalBytes()));
        assertTrue(pulsar.requiresEvidenceResource());
    }

    @Test
    void rejectsCapabilityBranchAndTimingMismatches() {
        final BrokerResourceIdentity kafkaEvidence =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("evidence-cluster", UUID.randomUUID()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeliveryCapabilitySemantic(
                        AdapterKind.KAFKA,
                        OutcomeCapability.AT_LEAST_ONCE,
                        TimingCapability.ORDINARY_MANAGED,
                        kafkaEvidence,
                        1,
                        1,
                        1,
                        1,
                        bytes(32, 1),
                        bytes(32, 2),
                        1,
                        1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeliveryCapabilitySemantic(
                        AdapterKind.KAFKA,
                        OutcomeCapability.KAFKA_TRANSACTIONAL_RECEIPT,
                        TimingCapability.ORDINARY_MANAGED,
                        BrokerResourceIdentity.pulsar(
                                new PulsarBrokerResourceIdentity("evidence-cluster", bytes(32, 3), "journal", 1)),
                        1,
                        1,
                        1,
                        1,
                        bytes(32, 4),
                        bytes(32, 5),
                        1,
                        1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeliveryCapabilitySemantic(
                        AdapterKind.KAFKA,
                        OutcomeCapability.AT_LEAST_ONCE,
                        TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_AUTO_FAST,
                        null,
                        0,
                        0,
                        0,
                        0,
                        bytes(32, 6),
                        bytes(32, 7),
                        0,
                        0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeliveryCapabilitySemantic(
                        AdapterKind.KAFKA,
                        OutcomeCapability.AT_LEAST_ONCE,
                        0,
                        null,
                        0,
                        0,
                        0,
                        0,
                        bytes(32, 8),
                        bytes(32, 9),
                        0,
                        0));
    }

    @Test
    void rejectsTamperedCanonicalBytes() {
        final DeliveryCapabilitySemantic capability = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 1),
                bytes(32, 2),
                0,
                0);
        final byte[] tampered = capability.canonicalBytes();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> DeliveryCapabilitySemantic.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
