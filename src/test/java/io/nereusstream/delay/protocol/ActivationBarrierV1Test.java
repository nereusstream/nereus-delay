package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void preservesUnsignedPartitionAndBatchFields() {
        final BrokerResourceIdentityV1 kafka = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID()));
        final ActivationBarrierV1 kafkaBarrier = ActivationBarrierV1.kafka(kafka, -1, Long.MIN_VALUE, -1L);
        assertEquals(kafkaBarrier, ActivationBarrierV1.decode(kafkaBarrier.canonicalBytes()));

        final BrokerResourceIdentityV1 pulsar = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 1), "topic", 7));
        final ActivationBarrierV1 pulsarBarrier = ActivationBarrierV1.pulsar(
                pulsar, -1, Long.MIN_VALUE, -1L, Integer.MIN_VALUE, Integer.MIN_VALUE + 1,
                Long.MIN_VALUE, bytes(32, 2));
        assertEquals(pulsarBarrier, ActivationBarrierV1.decode(pulsarBarrier.canonicalBytes()));
        final ActivationBarrierV1 emptyPulsar = ActivationBarrierV1.empty(pulsar, -1, Long.MIN_VALUE,
                bytes(32, 3));
        assertEquals(emptyPulsar, ActivationBarrierV1.decode(emptyPulsar.canonicalBytes()));
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

    @Test
    void projectsSignedRouteBarrierToExactSourceIdentity() {
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 10));
        final KafkaBrokerResourceIdentityV1 kafkaResource = new KafkaBrokerResourceIdentityV1(
                "cluster", UUID.randomUUID());
        final ActivationBarrierV1 kafka = ActivationBarrierV1.kafka(
                BrokerResourceIdentityV1.kafka(kafkaResource), 3, 11, 12);
        final SourceActivationBarrier kafkaSource = kafka.toSourceBarrier(incarnation);
        assertTrue(kafkaSource instanceof KafkaActivationBarrier);
        assertEquals(new ShardId(incarnation, 3), kafkaSource.shardId());
        assertEquals(11, ((KafkaActivationBarrier) kafkaSource).exclusiveOffset());

        final PulsarBrokerResourceIdentityV1 pulsarResource = new PulsarBrokerResourceIdentityV1(
                "cluster", bytes(32, 20), "persistent://tenant/ns/topic", 21);
        final ActivationBarrierV1 pulsar = ActivationBarrierV1.pulsar(
                BrokerResourceIdentityV1.pulsar(pulsarResource), 4, 5, 6, 1, 3, 9, bytes(32, 22));
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
