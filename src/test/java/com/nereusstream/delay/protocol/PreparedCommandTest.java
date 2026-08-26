package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class PreparedCommandTest {
    @Test
    void scheduleAndPrepareCommandsKeepOuterAndBodyIdentityAligned() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final CanonicalScheduleIntent scheduleIntent = CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand schedule = PreparedCommand.schedule(shard, scheduleIntent, 500);
        assertEquals(schedule, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(schedule)));

        final CanonicalScheduleIntent prepareIntent = CanonicalScheduleIntent.forPrepare(
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
        final PreparedCommand prepare = PreparedCommand.prepareLarge(
                shard,
                prepareIntent,
                9,
                Bytes.sha256(Bytes.utf8("payload")),
                1000,
                new PayloadProofTrustSetRef(1, bytes(32, 5)),
                objectStore(),
                500);
        assertEquals(prepare, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(prepare)));
    }

    @Test
    void decoderRejectsACommonFieldThatDoesNotMatchTheOuterCommand() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand valid = PreparedCommand.schedule(shard, intent, 500);
        final byte[] body = valid.canonicalBody();
        final ScheduleCommandBody decoded = ScheduleCommandBody.decode(body);
        final byte[] mismatch = new ScheduleCommandBody(
                        DelayMessageId.random(shard), decoded.retryUntilEpochMs(), decoded.intent())
                .canonicalBytes();
        final PreparedCommand forged = PreparedCommand.create(
                shard, valid.commandId(), valid.delayMessageId(), valid.type(), valid.retryUntilEpochMs(), mismatch);
        assertThrows(
                IllegalArgumentException.class,
                () -> CommandCodec.decodeManagedEnvelope(CommandCodec.encodeManagedEnvelope(forged)));
    }

    @Test
    void managedPreparedSubmissionRejectsACompatibilityBody() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final PreparedCommand legacy = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("legacy-managed")),
                        10,
                        100,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload")),
                500);
        assertThrows(
                IllegalArgumentException.class,
                () -> PreparedSubmission.managed(CommandCodec.encodeManagedFrame(legacy)));
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
