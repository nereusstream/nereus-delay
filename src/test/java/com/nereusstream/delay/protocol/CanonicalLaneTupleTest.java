package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CanonicalLaneTupleTest {
    @Test
    void claimProjectionReconstructsExactKafkaAndPulsarLaneIdentity() {
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "capability");
        final byte[] payload = Bytes.utf8("payload");
        final BrokerResourceIdentity kafkaTarget = BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                "fixture-target-cluster", UUID.nameUUIDFromBytes(Bytes.utf8("fixture-lane-topic"))));
        final ClaimMaterialization kafkaClaim = materialization(
                destination,
                capability,
                kafkaTarget,
                3,
                PayloadForPublish.inline(payload),
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())));
        assertDoesNotThrow(() -> CanonicalLaneTuple.requireClaimProjection(
                ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability), kafkaClaim));

        final byte[] resourceIncarnation = Bytes.sha256(Bytes.utf8("pulsar-resource"));
        final BrokerResourceIdentity pulsarTarget = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "pulsar-cluster", resourceIncarnation, "persistent://tenant/ns/topic-partition-5", 17));
        final ClaimMaterialization pulsarClaim = materialization(
                destination,
                capability,
                pulsarTarget,
                5,
                PayloadForPublish.inline(payload),
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())));
        assertDoesNotThrow(() -> CanonicalLaneTuple.requireClaimProjection(
                pulsarTuple(destination, capability, resourceIncarnation), pulsarClaim));
    }

    @Test
    void rejectsKafkaTupleWhoseRepeatedTopicUuidProjectionDiffers() {
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "capability");
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final int secondTopicUuidOffset = 32 + 1 + 4 + Bytes.utf8("fixture-target-cluster").length + 1 + 16 + 4;
        tuple[secondTopicUuidOffset] ^= 1;

        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalLaneTuple.requireProfileProjection(tuple, destination, capability));
    }

    private static ClaimMaterialization materialization(
            final ProfileRef destination,
            final ProfileRef capability,
            final BrokerResourceIdentity target,
            final long partition,
            final PayloadForPublish payload,
            final AdapterMetadata metadata) {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        return new ClaimMaterialization(
                destination,
                capability,
                target,
                partition,
                DelayMessageId.random(shardId),
                0,
                payload,
                metadata,
                100,
                200,
                100);
    }

    private static byte[] pulsarTuple(
            final ProfileRef destination, final ProfileRef capability, final byte[] resourceIncarnation) {
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("pulsar-tenant-routing-scope")),
                Bytes.u8(AdapterKind.PULSAR.wireValue()),
                Bytes.lp32(Bytes.utf8("pulsar-cluster")),
                Bytes.u8(2),
                resourceIncarnation,
                Bytes.u64be(17),
                Bytes.lp32(Bytes.utf8("persistent://tenant/ns/topic-partition-5")),
                Bytes.u32be(5),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(2),
                Bytes.u32be(9));
    }

    private static ProfileRef profile(final ProfileKind kind, final String id) {
        return new ProfileRef(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-semantic")), kind);
    }
}
