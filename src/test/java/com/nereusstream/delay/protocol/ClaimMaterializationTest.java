package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ClaimMaterializationTest {
    @Test
    void canonicalProjectionRoundTripsCompleteUint32PartitionAndGeneration() {
        final ClaimMaterialization materialization = materialization(
                new ProfileRef(
                        Bytes.utf8("destination"),
                        Long.MIN_VALUE,
                        Bytes.sha256(Bytes.utf8("destination")),
                        ProfileKind.DESTINATION),
                new ProfileRef(
                        Bytes.utf8("capability"),
                        -1L,
                        Bytes.sha256(Bytes.utf8("capability")),
                        ProfileKind.DELIVERY_CAPABILITY),
                PayloadForPublish.inline(Bytes.utf8("opaque")),
                0xffff_ffffL,
                0xffff_ffffL);

        final ClaimMaterialization decoded = ClaimMaterialization.decode(materialization.canonicalBytes());

        assertEquals(materialization, decoded);
        assertEquals(0xffff_ffffL, decoded.physicalPartition());
        assertEquals(0xffff_ffffL, decoded.generation());
        assertArrayEquals(materialization.materializationDigest(), decoded.materializationDigest());
    }

    @Test
    void committedPayloadBranchIsPartOfTheTypedProjection() {
        final byte[] bytes = Bytes.utf8("large-payload");
        final CommittedPayloadDescriptor descriptor = new CommittedPayloadDescriptor(
                new ProfileRef(
                        Bytes.utf8("object-store"),
                        1,
                        Bytes.sha256(Bytes.utf8("object-store")),
                        ProfileKind.OBJECT_STORE),
                Bytes.utf8("bucket"),
                Bytes.utf8("key"),
                Bytes.utf8("version"),
                Bytes.utf8("etag"),
                bytes.length,
                Bytes.sha256(bytes),
                nonZero(32, 5),
                nonZero(32, 6));
        final ClaimMaterialization materialization = materialization(
                profile(ProfileKind.DESTINATION, "destination"),
                profile(ProfileKind.DELIVERY_CAPABILITY, "capability"),
                PayloadForPublish.object(descriptor),
                3,
                4);

        final ClaimMaterialization decoded = ClaimMaterialization.decode(materialization.canonicalBytes());

        assertEquals(descriptor, decoded.payload().object());
        assertArrayEquals(materialization.canonicalBytes(), decoded.canonicalBytes());
    }

    @Test
    void constructorRejectsProfileSlotMetadataAndTimingDrift() {
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "capability");
        final BrokerResourceIdentity target = target();
        final DelayMessageId messageId = messageId();
        final PayloadForPublish payload = PayloadForPublish.inline(Bytes.utf8("payload"));
        final AdapterMetadata pulsarMetadata = AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of()));

        assertThrows(
                IllegalArgumentException.class,
                () -> new ClaimMaterialization(
                        profile(ProfileKind.OBJECT_STORE, "wrong"),
                        capability,
                        target,
                        0,
                        messageId,
                        0,
                        payload,
                        AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                        1_000,
                        2_000,
                        1_000));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClaimMaterialization(
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
                () -> new ClaimMaterialization(
                        destination,
                        capability,
                        target,
                        0,
                        messageId,
                        0,
                        payload,
                        AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                        2_000,
                        1_000,
                        2_000));
    }

    private static ClaimMaterialization materialization(
            final ProfileRef destination,
            final ProfileRef capability,
            final PayloadForPublish payload,
            final long partition,
            final long generation) {
        return new ClaimMaterialization(
                destination,
                capability,
                target(),
                partition,
                messageId(),
                generation,
                payload,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                1_000,
                2_000,
                1_000);
    }

    private static ProfileRef profile(final ProfileKind kind, final String id) {
        return new ProfileRef(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-hash")), kind);
    }

    private static BrokerResourceIdentity target() {
        return BrokerResourceIdentity.kafka(
                new KafkaBrokerResourceIdentity("cluster", UUID.nameUUIDFromBytes(Bytes.utf8("topic"))));
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
