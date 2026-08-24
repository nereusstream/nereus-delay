package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DeliveryCapabilitySemanticV1Test {
    @Test
    void roundTripsBaselineKafkaCapability() {
        final DeliveryCapabilitySemanticV1 capability = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA,
                OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 1),
                bytes(32, 2),
                0,
                0);

        assertEquals(capability, DeliveryCapabilitySemanticV1.decode(capability.canonicalBytes()));
        assertTrue(!capability.requiresEvidenceResource());
        assertTrue(TimingCapabilityV1.includes(capability.timingCapabilityBits(), TimingCapabilityV1.ORDINARY_MANAGED));
    }

    @Test
    void roundTripsStrongKafkaAndPulsarCapabilities() {
        final BrokerResourceIdentityV1 kafkaEvidence = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("evidence-cluster", UUID.randomUUID()));
        final DeliveryCapabilitySemanticV1 kafka = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA,
                OutcomeCapabilityV1.KAFKA_TRANSACTIONAL_RECEIPT,
                TimingCapabilityV1.ORDINARY_MANAGED,
                kafkaEvidence,
                3,
                10_000,
                20_000,
                4,
                bytes(32, 3),
                bytes(32, 4),
                1,
                2);
        assertEquals(kafka, DeliveryCapabilitySemanticV1.decode(kafka.canonicalBytes()));

        final BrokerResourceIdentityV1 pulsarEvidence = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("evidence-cluster", bytes(32, 5), "journal", 7));
        final DeliveryCapabilitySemanticV1 pulsar = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.PULSAR,
                OutcomeCapabilityV1.PULSAR_BROKER_DEDUP,
                TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_GUARDED_HANDOFF,
                pulsarEvidence,
                2,
                10_000,
                20_000,
                4,
                bytes(32, 6),
                bytes(32, 7),
                1,
                1);
        assertEquals(pulsar, DeliveryCapabilitySemanticV1.decode(pulsar.canonicalBytes()));
        assertTrue(pulsar.requiresEvidenceResource());
    }

    @Test
    void rejectsCapabilityBranchAndTimingMismatches() {
        final BrokerResourceIdentityV1 kafkaEvidence = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("evidence-cluster", UUID.randomUUID()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeliveryCapabilitySemanticV1(
                        AdapterKindV1.KAFKA,
                        OutcomeCapabilityV1.AT_LEAST_ONCE,
                        TimingCapabilityV1.ORDINARY_MANAGED,
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
                () -> new DeliveryCapabilitySemanticV1(
                        AdapterKindV1.KAFKA,
                        OutcomeCapabilityV1.KAFKA_TRANSACTIONAL_RECEIPT,
                        TimingCapabilityV1.ORDINARY_MANAGED,
                        BrokerResourceIdentityV1.pulsar(
                                new PulsarBrokerResourceIdentityV1("evidence-cluster", bytes(32, 3), "journal", 1)),
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
                () -> new DeliveryCapabilitySemanticV1(
                        AdapterKindV1.KAFKA,
                        OutcomeCapabilityV1.AT_LEAST_ONCE,
                        TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_AUTO_FAST,
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
                () -> new DeliveryCapabilitySemanticV1(
                        AdapterKindV1.KAFKA,
                        OutcomeCapabilityV1.AT_LEAST_ONCE,
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
        final DeliveryCapabilitySemanticV1 capability = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA,
                OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED,
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
        assertThrows(IllegalArgumentException.class, () -> DeliveryCapabilitySemanticV1.decode(tampered));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
