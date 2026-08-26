package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import org.junit.jupiter.api.Test;

class CanonicalScheduleIntentTest {
    @Test
    void inlineScheduleRoundTripsTheClosedFieldsAndMetadataUnion() {
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                1_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("ordering"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(
                        Bytes.utf8("key"), List.of(new KafkaMetadata.Header("x-name", Bytes.utf8("value"))))),
                Bytes.utf8("business"),
                900L);

        final CanonicalScheduleIntent decoded = CanonicalScheduleIntent.decode(intent.canonicalBytes());
        assertEquals(intent, decoded);
        assertTrue(decoded.hasPayloadBranch());
        assertTrue(decoded.hasInlinePayload());
        assertArrayEquals(Bytes.utf8("payload"), decoded.inlinePayload());
        assertEquals(AdapterMetadata.Kind.KAFKA, decoded.adapterMetadata().kind());
    }

    @Test
    void prepareFormAllowsNeitherPayloadBranchAndCommittedDescriptorRoundTrips() {
        final CanonicalScheduleIntent prepare = CanonicalScheduleIntent.forPrepare(
                destination(),
                retryPolicy(),
                1_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.DELIVERY_TIME_FIFO,
                new byte[0],
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);
        final CanonicalScheduleIntent decodedPrepare = CanonicalScheduleIntent.decode(prepare.canonicalBytes());
        assertEquals(prepare, decodedPrepare);
        assertFalse(decodedPrepare.hasPayloadBranch());

        final CommittedPayloadDescriptor descriptor = new CommittedPayloadDescriptor(
                objectStore(),
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                null,
                7,
                Bytes.sha256(Bytes.utf8("payload")),
                bytes(32, 4),
                bytes(32, 5));
        final CanonicalScheduleIntent committed = CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                1_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                null,
                descriptor,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        assertEquals(committed, CanonicalScheduleIntent.decode(committed.canonicalBytes()));
        assertEquals(
                descriptor,
                CanonicalScheduleIntent.decode(committed.canonicalBytes()).committedPayload());
    }

    @Test
    void rejectsWrongProfileBranchPayloadAmbiguityAndMetadataUnion() {
        final ProfileRef objectStore = objectStore();
        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalScheduleIntent.create(
                        objectStore,
                        retryPolicy(),
                        1,
                        2,
                        DeliveryMode.MANAGED,
                        OrderingMode.BEST_EFFORT,
                        new byte[0],
                        Bytes.utf8("p"),
                        null,
                        AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                        null,
                        null));
        assertThrows(
                IllegalArgumentException.class,
                () -> CanonicalScheduleIntent.create(
                        destination(),
                        retryPolicy(),
                        1,
                        2,
                        DeliveryMode.MANAGED,
                        OrderingMode.BEST_EFFORT,
                        new byte[0],
                        Bytes.utf8("p"),
                        new CommittedPayloadDescriptor(
                                objectStore,
                                Bytes.utf8("b"),
                                Bytes.utf8("k"),
                                Bytes.utf8("v"),
                                null,
                                1,
                                bytes(32, 1),
                                bytes(32, 2),
                                bytes(32, 3)),
                        AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                        null,
                        null));
        final byte[] duplicateMetadata = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new byte[0]);
            CanonicalProtobuf.bytes(output, 2, new byte[0]);
        });
        assertThrows(IllegalArgumentException.class, () -> AdapterMetadata.decode(duplicateMetadata));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CommittedPayloadDescriptor(
                        destination(),
                        Bytes.utf8("b"),
                        Bytes.utf8("k"),
                        Bytes.utf8("v"),
                        null,
                        1,
                        bytes(32, 1),
                        bytes(32, 2),
                        bytes(32, 3)));
    }

    @Test
    void rejectsNonCanonicalOrWrongQuotaVersion() {
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                1,
                2,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("p"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final byte[] nonCanonical = intent.canonicalBytes();
        nonCanonical[nonCanonical.length - 1] = 2;
        assertThrows(IllegalArgumentException.class, () -> CanonicalScheduleIntent.decode(nonCanonical));
    }

    @Test
    void rejectsReservedCallerMetadataNames() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaMetadata.Header("nereus.delay.internal", Bytes.utf8("x")));
        assertThrows(IllegalArgumentException.class, () -> new PulsarMetadata.Property("nereus.delay.internal", "x"));
    }

    private static ProfileRef destination() {
        return new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 1), ProfileKind.DESTINATION);
    }

    private static ProfileRef objectStore() {
        return new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 2), ProfileKind.OBJECT_STORE);
    }

    private static RetryPolicyRef retryPolicy() {
        return new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 3));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
