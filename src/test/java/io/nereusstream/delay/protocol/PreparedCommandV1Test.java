package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PreparedCommandV1Test {
    @Test
    void scheduleAndPrepareV1CommandsKeepOuterAndBodyIdentityAligned() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ScheduleIntentV1 scheduleIntent = ScheduleIntentV1.create(destination(), retryPolicy(), 10, 100,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("payload"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand schedule = PreparedCommand.scheduleV1(shard, scheduleIntent, 500);
        assertEquals(schedule, CommandCodec.decodeFrameV1(CommandCodec.encodeFrameV1(schedule)));

        final ScheduleIntentV1 prepareIntent = ScheduleIntentV1.forPrepare(destination(), retryPolicy(), 10, 100,
                DeliveryMode.MANAGED, OrderingMode.DELIVERY_TIME_FIFO, new byte[0],
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);
        final PreparedCommand prepare = PreparedCommand.prepareLargeV1(shard, prepareIntent, 9,
                Bytes.sha256(Bytes.utf8("payload")), 1000, new PayloadProofTrustSetRefV1(1, bytes(32, 5)),
                objectStore(), 500);
        assertEquals(prepare, CommandCodec.decodeFrameV1(CommandCodec.encodeFrameV1(prepare)));
    }

    @Test
    void v1DecoderRejectsACommonFieldThatDoesNotMatchTheOuterCommand() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination(), retryPolicy(), 10, 100,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("payload"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand valid = PreparedCommand.scheduleV1(shard, intent, 500);
        final byte[] body = valid.canonicalBody();
        final ScheduleCommandBodyV1 decoded = ScheduleCommandBodyV1.decode(body);
        final byte[] mismatch = new ScheduleCommandBodyV1(DelayMessageId.random(shard), decoded.retryUntilEpochMs(),
                decoded.intent()).canonicalBytes();
        final PreparedCommand forged = PreparedCommand.create(shard, valid.commandId(), valid.delayMessageId(),
                valid.type(), valid.retryUntilEpochMs(), mismatch);
        assertThrows(IllegalArgumentException.class, () -> CommandCodec.decodeEnvelopeV1(
                CommandCodec.encodeEnvelope(forged)));
    }

    @Test
    void managedPreparedSubmissionRejectsACompatibilityBody() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final PreparedCommand legacy = PreparedCommand.schedule(shard,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("legacy-managed")), 10, 100,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 500);
        assertThrows(IllegalArgumentException.class,
                () -> PreparedSubmissionV1.managed(CommandCodec.encodeFrame(legacy)));
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
