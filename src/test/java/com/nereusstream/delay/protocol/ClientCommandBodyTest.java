package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClientCommandBodyTest {
    @Test
    void scheduleBodyRoundTripsWithCommonFieldsAndPayloadBranch() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 3));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final ScheduleCommandBody body = new ScheduleCommandBody(messageId, 500, intent);

        assertEquals(body, ScheduleCommandBody.decode(body.canonicalBytes()));
        assertEquals(body, CommandBodies.decodeSchedule(CommandBodies.schedule(messageId, 500, intent)));
    }

    @Test
    void prepareBodyRoundTripsAndRejectsWrongPayloadPresence() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 4));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.forPrepare(
                destination(),
                retryPolicy(),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.DELIVERY_TIME_FIFO,
                new byte[0],
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);
        final PrepareLargeScheduleBody body = new PrepareLargeScheduleBody(
                messageId,
                500,
                intent,
                9,
                Bytes.sha256(Bytes.utf8("payload")),
                1000,
                new PayloadProofTrustSetRef(1, bytes(32, 4)),
                objectStore());

        assertEquals(body, PrepareLargeScheduleBody.decode(body.canonicalBytes()));
        assertEquals(
                body,
                CommandBodies.decodePrepareLarge(CommandBodies.prepareLarge(
                        messageId,
                        500,
                        intent,
                        9,
                        Bytes.sha256(Bytes.utf8("payload")),
                        1000,
                        new PayloadProofTrustSetRef(1, bytes(32, 4)),
                        objectStore())));
        final byte[] wrongType = body.canonicalBytes();
        wrongType[2] = 3;
        assertThrows(IllegalArgumentException.class, () -> PrepareLargeScheduleBody.decode(wrongType));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PrepareLargeScheduleBody(
                        messageId,
                        500,
                        intent,
                        9,
                        Bytes.sha256(Bytes.utf8("payload")),
                        1000,
                        new PayloadProofTrustSetRef(1, bytes(32, 4)),
                        destination()));
    }

    @Test
    void ordinaryScheduleCannotUsePrepareWithoutPayload() {
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 5));
        final CanonicalScheduleIntent prepare = CanonicalScheduleIntent.forPrepare(
                destination(),
                retryPolicy(),
                1,
                2,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        assertThrows(IllegalArgumentException.class, () -> new ScheduleCommandBody(messageId, 10, prepare));
    }

    private static ProfileRef destination() {
        return new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 1), ProfileKind.DESTINATION);
    }

    private static RetryPolicyRef retryPolicy() {
        return new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 2));
    }

    private static ProfileRef objectStore() {
        return new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 3), ProfileKind.OBJECT_STORE);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
