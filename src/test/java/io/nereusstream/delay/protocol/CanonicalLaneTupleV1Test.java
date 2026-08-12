package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CanonicalLaneTupleV1Test {
    @Test
    void claimProjectionReconstructsExactKafkaAndPulsarLaneIdentity() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability");
        final byte[] payload = Bytes.utf8("payload");
        final BrokerResourceIdentityV1 kafkaTarget = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("fixture-target-cluster",
                        UUID.nameUUIDFromBytes(Bytes.utf8("fixture-lane-topic"))));
        final ClaimMaterializationV1 kafkaClaim = materialization(destination, capability, kafkaTarget, 3,
                PayloadForPublishV1.inline(payload),
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())));
        assertDoesNotThrow(() -> CanonicalLaneTupleV1.requireClaimProjection(
                ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability), kafkaClaim));

        final byte[] resourceIncarnation = Bytes.sha256(Bytes.utf8("pulsar-resource"));
        final BrokerResourceIdentityV1 pulsarTarget = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("pulsar-cluster", resourceIncarnation,
                        "persistent://tenant/ns/topic-partition-5", 17));
        final ClaimMaterializationV1 pulsarClaim = materialization(destination, capability, pulsarTarget, 5,
                PayloadForPublishV1.inline(payload),
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())));
        assertDoesNotThrow(() -> CanonicalLaneTupleV1.requireClaimProjection(
                pulsarTuple(destination, capability, resourceIncarnation), pulsarClaim));
    }

    @Test
    void rejectsKafkaTupleWhoseRepeatedTopicUuidProjectionDiffers() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability");
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final int secondTopicUuidOffset = 32 + 1 + 4 + Bytes.utf8("fixture-target-cluster").length + 1 + 16 + 4;
        tuple[secondTopicUuidOffset] ^= 1;

        assertThrows(IllegalArgumentException.class,
                () -> CanonicalLaneTupleV1.requireProfileProjection(tuple, destination, capability));
    }

    private static ClaimMaterializationV1 materialization(final ProfileRefV1 destination,
                                                           final ProfileRefV1 capability,
                                                           final BrokerResourceIdentityV1 target,
                                                           final long partition,
                                                           final PayloadForPublishV1 payload,
                                                           final AdapterMetadataV1 metadata) {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        return new ClaimMaterializationV1(destination, capability, target, partition,
                DelayMessageId.random(shardId), 0, payload, metadata, 100, 200, 100);
    }

    private static byte[] pulsarTuple(final ProfileRefV1 destination, final ProfileRefV1 capability,
                                      final byte[] resourceIncarnation) {
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("pulsar-tenant-routing-scope")),
                Bytes.u8(AdapterKindV1.PULSAR.wireValue()),
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

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final String id) {
        return new ProfileRefV1(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-semantic")), kind);
    }
}
