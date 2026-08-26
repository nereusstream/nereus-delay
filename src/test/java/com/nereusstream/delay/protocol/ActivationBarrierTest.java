package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ActivationBarrierTest {
    @Test
    void roundTripsKafkaAndPulsarBranches() {
        final BrokerResourceIdentity kafka =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID()));
        final ActivationBarrier kafkaBarrier = ActivationBarrier.kafka(kafka, 3, 11, 10);
        assertEquals(kafkaBarrier, ActivationBarrier.decode(kafkaBarrier.canonicalBytes()));

        final BrokerResourceIdentity pulsar = BrokerResourceIdentity.pulsar(
                new PulsarBrokerResourceIdentity("cluster", bytes(32, 1), "persistent://tenant/ns/topic", 7));
        final ActivationBarrier pulsarBarrier = ActivationBarrier.pulsar(pulsar, 4, 5, 6, 1, 3, 9, bytes(32, 2));
        assertEquals(pulsarBarrier, ActivationBarrier.decode(pulsarBarrier.canonicalBytes()));
    }

    @Test
    void preservesUnsignedPartitionAndBatchFields() {
        final BrokerResourceIdentity kafka =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID()));
        final ActivationBarrier kafkaBarrier = ActivationBarrier.kafka(kafka, -1, Long.MIN_VALUE, -1L);
        assertEquals(kafkaBarrier, ActivationBarrier.decode(kafkaBarrier.canonicalBytes()));

        final BrokerResourceIdentity pulsar =
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity("cluster", bytes(32, 1), "topic", 7));
        final ActivationBarrier pulsarBarrier = ActivationBarrier.pulsar(
                pulsar,
                -1,
                Long.MIN_VALUE,
                -1L,
                Integer.MIN_VALUE,
                Integer.MIN_VALUE + 1,
                Long.MIN_VALUE,
                bytes(32, 2));
        assertEquals(pulsarBarrier, ActivationBarrier.decode(pulsarBarrier.canonicalBytes()));
        final ActivationBarrier emptyPulsar = ActivationBarrier.empty(pulsar, -1, Long.MIN_VALUE, bytes(32, 3));
        assertEquals(emptyPulsar, ActivationBarrier.decode(emptyPulsar.canonicalBytes()));
    }

    @Test
    void enforcesResourceBranchAndGuardPairing() {
        final BrokerResourceIdentity kafka =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID()));
        assertThrows(
                IllegalArgumentException.class, () -> ActivationBarrier.pulsar(kafka, 0, 1, 1, 0, 1, 1, bytes(32, 3)));
        assertThrows(IllegalArgumentException.class, () -> ActivationBarrier.empty(kafka, 0, 1L, null));
        final BrokerResourceIdentity pulsar =
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity("cluster", bytes(32, 5), "topic", 1));
        assertThrows(IllegalArgumentException.class, () -> ActivationBarrier.empty(pulsar, 0, null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> ActivationBarrier.pulsar(
                        BrokerResourceIdentity.pulsar(
                                new PulsarBrokerResourceIdentity("cluster", bytes(32, 4), "topic", 1)),
                        0,
                        1,
                        1,
                        0,
                        1,
                        1,
                        new byte[32]));
    }

    @Test
    void projectsSignedRouteBarrierToExactSourceIdentity() {
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 10));
        final KafkaBrokerResourceIdentity kafkaResource = new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID());
        final ActivationBarrier kafka = ActivationBarrier.kafka(BrokerResourceIdentity.kafka(kafkaResource), 3, 11, 12);
        final SourceActivationBarrier kafkaSource = kafka.toSourceBarrier(incarnation);
        assertTrue(kafkaSource instanceof KafkaActivationBarrier);
        assertEquals(new ShardId(incarnation, 3), kafkaSource.shardId());
        assertEquals(11, ((KafkaActivationBarrier) kafkaSource).exclusiveOffset());

        final PulsarBrokerResourceIdentity pulsarResource =
                new PulsarBrokerResourceIdentity("cluster", bytes(32, 20), "persistent://tenant/ns/topic", 21);
        final ActivationBarrier pulsar = ActivationBarrier.pulsar(
                BrokerResourceIdentity.pulsar(pulsarResource), 4, 5, 6, 1, 3, 9, bytes(32, 22));
        final SourceActivationBarrier pulsarSource = pulsar.toSourceBarrier(incarnation);
        assertTrue(pulsarSource instanceof PulsarActivationBarrier);
        final PulsarActivationBarrier exact = (PulsarActivationBarrier) pulsarSource;
        assertEquals(new ShardId(incarnation, 4), exact.shardId());
        assertEquals(5, exact.ledgerId());
        assertEquals(6, exact.entryId());
        assertEquals(3, exact.batchSize());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) (seed + i);
        }
        return value;
    }
}
