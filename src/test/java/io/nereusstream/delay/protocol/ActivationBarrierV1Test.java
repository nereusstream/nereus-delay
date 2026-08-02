package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActivationBarrierV1Test {
    @Test
    void roundTripsKafkaAndPulsarBranches() {
        final BrokerResourceIdentityV1 kafka = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID()));
        final ActivationBarrierV1 kafkaBarrier = ActivationBarrierV1.kafka(kafka, 3, 11, 10);
        assertEquals(kafkaBarrier, ActivationBarrierV1.decode(kafkaBarrier.canonicalBytes()));

        final BrokerResourceIdentityV1 pulsar = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 1), "persistent://tenant/ns/topic", 7));
        final ActivationBarrierV1 pulsarBarrier = ActivationBarrierV1.pulsar(
                pulsar, 4, 5, 6, 1, 3, 9, bytes(32, 2));
        assertEquals(pulsarBarrier, ActivationBarrierV1.decode(pulsarBarrier.canonicalBytes()));
    }

    @Test
    void enforcesResourceBranchAndGuardPairing() {
        final BrokerResourceIdentityV1 kafka = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID()));
        assertThrows(IllegalArgumentException.class,
                () -> ActivationBarrierV1.pulsar(kafka, 0, 1, 1, 0, 1, 1, bytes(32, 3)));
        assertThrows(IllegalArgumentException.class,
                () -> ActivationBarrierV1.empty(kafka, 0, 1L, null));
        final BrokerResourceIdentityV1 pulsar = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 5), "topic", 1));
        assertThrows(IllegalArgumentException.class,
                () -> ActivationBarrierV1.empty(pulsar, 0, null, null));
        assertThrows(IllegalArgumentException.class,
                () -> ActivationBarrierV1.pulsar(BrokerResourceIdentityV1.pulsar(
                        new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 4), "topic", 1)),
                        0, 1, 1, 0, 1, 1, new byte[32]));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }
}
