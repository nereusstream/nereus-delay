package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class V1ScheduleBindingTest {
    @Test
    void scheduleBindingRoundTripsTheExactBodyAndDerivedLaneTuple() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
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
        final PreparedCommand command = PreparedCommand.scheduleV1(shardId, intent, 500);
        final byte[] tuple = Bytes.utf8("canonical-lane-tuple");
        final V1ScheduleBinding binding =
                V1ScheduleBinding.fromCommand(command, DestinationLaneId.derive(tuple), tuple);

        assertEquals(binding, V1ScheduleBinding.decode(binding.encode()));
        assertEquals(command.delayMessageId(), binding.delayMessageId());
        assertEquals(command.canonicalBody().length, binding.canonicalBody().length);
    }

    @Test
    void bindingRejectsWrongTupleOrLegacyBody() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final byte[] tuple = Bytes.utf8("tuple");
        assertThrows(
                IllegalArgumentException.class,
                () -> new V1ScheduleBinding(
                        CommandType.SCHEDULE,
                        messageId,
                        DestinationLaneId.derive(Bytes.utf8("different")),
                        tuple,
                        new byte[] {0x01, 0x02}));
        assertThrows(
                IllegalArgumentException.class,
                () -> new V1ScheduleBinding(
                        CommandType.SCHEDULE,
                        messageId,
                        DestinationLaneId.derive(tuple),
                        tuple,
                        CommandBodies.schedule(new ScheduleIntent(
                                DestinationLaneId.derive(tuple),
                                10,
                                100,
                                OrderingMode.BEST_EFFORT,
                                Bytes.utf8("legacy")))));
    }

    private static ProfileRefV1 destination() {
        return new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 1), ProfileKindV1.DESTINATION);
    }

    private static RetryPolicyRefV1 retryPolicy() {
        return new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 2));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
