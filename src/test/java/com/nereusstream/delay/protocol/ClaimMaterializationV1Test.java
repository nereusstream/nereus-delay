package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ClaimMaterializationV1Test {
    @Test
    void canonicalProjectionRoundTripsCompleteUint32PartitionAndGeneration() {
        final ClaimMaterializationV1 materialization = materialization(
                new ProfileRefV1(
                        Bytes.utf8("destination"),
                        Long.MIN_VALUE,
                        Bytes.sha256(Bytes.utf8("destination")),
                        ProfileKindV1.DESTINATION),
                new ProfileRefV1(
                        Bytes.utf8("capability"),
                        -1L,
                        Bytes.sha256(Bytes.utf8("capability")),
                        ProfileKindV1.DELIVERY_CAPABILITY),
                PayloadForPublishV1.inline(Bytes.utf8("opaque")),
                0xffff_ffffL,
                0xffff_ffffL);

        final ClaimMaterializationV1 decoded = ClaimMaterializationV1.decode(materialization.canonicalBytes());

        assertEquals(materialization, decoded);
        assertEquals(0xffff_ffffL, decoded.physicalPartition());
        assertEquals(0xffff_ffffL, decoded.generation());
        assertArrayEquals(materialization.materializationDigest(), decoded.materializationDigest());
    }

    @Test
    void committedPayloadBranchIsPartOfTheTypedProjection() {
        final byte[] bytes = Bytes.utf8("large-payload");
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(
                new ProfileRefV1(
                        Bytes.utf8("object-store"),
                        1,
                        Bytes.sha256(Bytes.utf8("object-store")),
                        ProfileKindV1.OBJECT_STORE),
                Bytes.utf8("bucket"),
                Bytes.utf8("key"),
                Bytes.utf8("version"),
                Bytes.utf8("etag"),
                bytes.length,
                Bytes.sha256(bytes),
                nonZero(32, 5),
                nonZero(32, 6));
        final ClaimMaterializationV1 materialization = materialization(
                profile(ProfileKindV1.DESTINATION, "destination"),
                profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability"),
                PayloadForPublishV1.object(descriptor),
                3,
                4);

        final ClaimMaterializationV1 decoded = ClaimMaterializationV1.decode(materialization.canonicalBytes());

        assertEquals(descriptor, decoded.payload().object());
        assertArrayEquals(materialization.canonicalBytes(), decoded.canonicalBytes());
    }

    @Test
    void constructorRejectsProfileSlotMetadataAndTimingDrift() {
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability");
        final BrokerResourceIdentityV1 target = target();
        final DelayMessageId messageId = messageId();
        final PayloadForPublishV1 payload = PayloadForPublishV1.inline(Bytes.utf8("payload"));
        final AdapterMetadataV1 pulsarMetadata =
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of()));

        assertThrows(
                IllegalArgumentException.class,
                () -> new ClaimMaterializationV1(
                        profile(ProfileKindV1.OBJECT_STORE, "wrong"),
                        capability,
                        target,
                        0,
                        messageId,
                        0,
                        payload,
                        AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                        1_000,
                        2_000,
                        1_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClaimMaterializationV1(
                        destination,
                        capability,
                        target,
                        0,
                        messageId,
                        0,
                        payload,
                        pulsarMetadata,
                        1_000,
                        2_000,
                        1_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClaimMaterializationV1(
                        destination,
                        capability,
                        target,
                        0,
                        messageId,
                        0,
                        payload,
                        AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                        2_000,
                        1_000,
                        2_000));
    }

    private static ClaimMaterializationV1 materialization(
            final ProfileRefV1 destination,
            final ProfileRefV1 capability,
            final PayloadForPublishV1 payload,
            final long partition,
            final long generation) {
        return new ClaimMaterializationV1(
                destination,
                capability,
                target(),
                partition,
                messageId(),
                generation,
                payload,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                1_000,
                2_000,
                1_000);
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final String id) {
        return new ProfileRefV1(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-hash")), kind);
    }

    private static BrokerResourceIdentityV1 target() {
        return BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", UUID.nameUUIDFromBytes(Bytes.utf8("topic"))));
    }

    private static DelayMessageId messageId() {
        return DelayMessageId.random(new ShardId(RouteIncarnation.random(), 7));
    }

    private static byte[] nonZero(final int length, final int seed) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) seed);
        return result;
    }
}
