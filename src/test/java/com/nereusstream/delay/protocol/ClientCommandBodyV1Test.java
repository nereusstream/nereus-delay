package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClientCommandBodyV1Test {
    @Test
    void scheduleBodyRoundTripsWithCommonFieldsAndPayloadBranch() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 3));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(
                destination(),
                retryPolicy(),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                null,
                null);
        final ScheduleCommandBodyV1 body = new ScheduleCommandBodyV1(messageId, 500, intent);

        assertEquals(body, ScheduleCommandBodyV1.decode(body.canonicalBytes()));
        assertEquals(body, CommandBodies.decodeScheduleV1(CommandBodies.scheduleV1(messageId, 500, intent)));
    }

    @Test
    void prepareBodyRoundTripsAndRejectsWrongPayloadPresence() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 4));
        final ScheduleIntentV1 intent = ScheduleIntentV1.forPrepare(
                destination(),
                retryPolicy(),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.DELIVERY_TIME_FIFO,
                new byte[0],
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())),
                null,
                null);
        final PrepareLargeScheduleBodyV1 body = new PrepareLargeScheduleBodyV1(
                messageId,
                500,
                intent,
                9,
                Bytes.sha256(Bytes.utf8("payload")),
                1000,
                new PayloadProofTrustSetRefV1(1, bytes(32, 4)),
                objectStore());

        assertEquals(body, PrepareLargeScheduleBodyV1.decode(body.canonicalBytes()));
        assertEquals(
                body,
                CommandBodies.decodePrepareLargeV1(CommandBodies.prepareLargeV1(
                        messageId,
                        500,
                        intent,
                        9,
                        Bytes.sha256(Bytes.utf8("payload")),
                        1000,
                        new PayloadProofTrustSetRefV1(1, bytes(32, 4)),
                        objectStore())));
        final byte[] wrongType = body.canonicalBytes();
        wrongType[2] = 3;
        assertThrows(IllegalArgumentException.class, () -> PrepareLargeScheduleBodyV1.decode(wrongType));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrepareLargeScheduleBodyV1(
                        messageId,
                        500,
                        intent,
                        9,
                        Bytes.sha256(Bytes.utf8("payload")),
                        1000,
                        new PayloadProofTrustSetRefV1(1, bytes(32, 4)),
                        destination()));
    }

    @Test
    void ordinaryScheduleCannotUsePrepareWithoutPayload() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 5));
        final ScheduleIntentV1 prepare = ScheduleIntentV1.forPrepare(
                destination(),
                retryPolicy(),
                1,
                2,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                null,
                null);
        assertThrows(IllegalArgumentException.class, () -> new ScheduleCommandBodyV1(messageId, 10, prepare));
    }

    private static ProfileRefV1 destination() {
        return new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 1), ProfileKindV1.DESTINATION);
    }

    private static RetryPolicyRefV1 retryPolicy() {
        return new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 2));
    }

    private static ProfileRefV1 objectStore() {
        return new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 3), ProfileKindV1.OBJECT_STORE);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
