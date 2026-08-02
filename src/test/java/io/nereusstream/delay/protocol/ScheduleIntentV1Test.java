package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScheduleIntentV1Test {
    @Test
    void inlineScheduleRoundTripsTheClosedFieldsAndMetadataUnion() {
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination(), retryPolicy(), 1_000, 5_000,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("ordering"), Bytes.utf8("payload"),
                null, AdapterMetadataV1.kafka(new KafkaMetadataV1(Bytes.utf8("key"), List.of(
                        new KafkaMetadataV1.Header("x-name", Bytes.utf8("value"))))), Bytes.utf8("business"),
                900L);

        final ScheduleIntentV1 decoded = ScheduleIntentV1.decode(intent.canonicalBytes());
        assertEquals(intent, decoded);
        assertTrue(decoded.hasPayloadBranch());
        assertTrue(decoded.hasInlinePayload());
        assertArrayEquals(Bytes.utf8("payload"), decoded.inlinePayload());
        assertEquals(AdapterMetadataV1.Kind.KAFKA, decoded.adapterMetadata().kind());
    }

    @Test
    void prepareFormAllowsNeitherPayloadBranchAndCommittedDescriptorRoundTrips() {
        final ScheduleIntentV1 prepare = ScheduleIntentV1.forPrepare(destination(), retryPolicy(), 1_000, 5_000,
                DeliveryMode.MANAGED, OrderingMode.DELIVERY_TIME_FIFO, new byte[0],
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
        final ScheduleIntentV1 decodedPrepare = ScheduleIntentV1.decode(prepare.canonicalBytes());
        assertEquals(prepare, decodedPrepare);
        assertFalse(decodedPrepare.hasPayloadBranch());

        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(objectStore(),
                Bytes.utf8("bucket"), Bytes.utf8("object"), Bytes.utf8("version"), null, 7,
                Bytes.sha256(Bytes.utf8("payload")), bytes(32, 4), bytes(32, 5));
        final ScheduleIntentV1 committed = ScheduleIntentV1.create(destination(), retryPolicy(), 1_000, 5_000,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], null, descriptor,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        assertEquals(committed, ScheduleIntentV1.decode(committed.canonicalBytes()));
        assertEquals(descriptor, ScheduleIntentV1.decode(committed.canonicalBytes()).committedPayload());
    }

    @Test
    void rejectsWrongProfileBranchPayloadAmbiguityAndMetadataUnion() {
        final ProfileRefV1 objectStore = objectStore();
        assertThrows(IllegalArgumentException.class, () -> ScheduleIntentV1.create(objectStore, retryPolicy(),
                1, 2, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("p"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null));
        assertThrows(IllegalArgumentException.class, () -> ScheduleIntentV1.create(destination(), retryPolicy(),
                1, 2, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("p"),
                new CommittedPayloadDescriptorV1(objectStore, Bytes.utf8("b"), Bytes.utf8("k"), Bytes.utf8("v"),
                        null, 1, bytes(32, 1), bytes(32, 2), bytes(32, 3)),
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null));
        final byte[] duplicateMetadata = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new byte[0]);
            CanonicalProtobuf.bytes(output, 2, new byte[0]);
        });
        assertThrows(IllegalArgumentException.class, () -> AdapterMetadataV1.decode(duplicateMetadata));
        assertThrows(IllegalArgumentException.class, () -> new CommittedPayloadDescriptorV1(destination(),
                Bytes.utf8("b"), Bytes.utf8("k"), Bytes.utf8("v"), null, 1, bytes(32, 1), bytes(32, 2),
                bytes(32, 3)));
    }

    @Test
    void rejectsNonCanonicalOrWrongQuotaVersion() {
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination(), retryPolicy(), 1, 2,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("p"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final byte[] nonCanonical = intent.canonicalBytes();
        nonCanonical[nonCanonical.length - 1] = 2;
        assertThrows(IllegalArgumentException.class, () -> ScheduleIntentV1.decode(nonCanonical));
    }

    @Test
    void rejectsReservedCallerMetadataNames() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaMetadataV1.Header("nereus.delay.internal", Bytes.utf8("x")));
        assertThrows(IllegalArgumentException.class,
                () -> new PulsarMetadataV1.Property("nereus.delay.internal", "x"));
    }

    private static ProfileRefV1 destination() {
        return new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 1), ProfileKindV1.DESTINATION);
    }

    private static ProfileRefV1 objectStore() {
        return new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 2), ProfileKindV1.OBJECT_STORE);
    }

    private static RetryPolicyRefV1 retryPolicy() {
        return new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 3));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
