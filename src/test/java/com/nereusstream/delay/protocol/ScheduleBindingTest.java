package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class ScheduleBindingTest {
    @Test
    void scheduleBindingRoundTripsTheExactBodyAndDerivedLaneTuple() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
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
        final PreparedCommand command = PreparedCommand.schedule(shardId, intent, 500);
        final byte[] tuple = Bytes.utf8("canonical-lane-tuple");
        final ScheduleBinding binding = ScheduleBinding.fromCommand(command, DestinationLaneId.derive(tuple), tuple);

        assertEquals(binding, ScheduleBinding.decode(binding.encode()));
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
                () -> new ScheduleBinding(
                        CommandType.SCHEDULE,
                        messageId,
                        DestinationLaneId.derive(Bytes.utf8("different")),
                        tuple,
                        new byte[] {0x01, 0x02}));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ScheduleBinding(
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

    private static ProfileRef destination() {
        return new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 1), ProfileKind.DESTINATION);
    }

    private static RetryPolicyRef retryPolicy() {
        return new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 2));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
