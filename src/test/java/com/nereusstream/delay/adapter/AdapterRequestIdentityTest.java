package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AdapterRequestIdentityTest {
    @Test
    void ingressAndDestinationRequestsRejectNonCanonicalIdentityText() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final CommandId commandId = CommandId.random(shard);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final DestinationLaneId laneId = DestinationLaneId.derive(Bytes.utf8("request-lane"));
        final byte[] resource = Bytes.sha256(Bytes.utf8("request-resource"));
        final String decomposed = "cluster" + '\u0301';
        final String topic = "persistent://tenant/ns/topic";
        final byte[] frame = Bytes.utf8("frame");

        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaProduceRequest(
                        decomposed, "command-topic", UUID.randomUUID(), shard.partition(), commandId, frame));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarSendRequest(
                        "cluster", resource, topic + '\u0301', 1, shard.partition(), commandId, frame));
        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaDestinationRequest(
                        decomposed,
                        UUID.randomUUID(),
                        shard.partition(),
                        laneId,
                        new byte[16],
                        messageId,
                        0,
                        new byte[32],
                        1,
                        1,
                        frame,
                        new byte[0]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarDestinationRequest(
                        "cluster",
                        resource,
                        topic + '\u0301',
                        1,
                        shard.partition(),
                        laneId,
                        new byte[16],
                        messageId,
                        0,
                        new byte[32],
                        1,
                        1,
                        frame,
                        new byte[0]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarNativeSendRequest(
                        "cluster", resource, topic + '\u0301', 1, shard.partition(), nonZero(32), new byte[32], frame));
    }

    @Test
    void nativeRequestRejectsZeroDeliveryIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarNativeSendRequest(
                        "cluster",
                        Bytes.sha256(Bytes.utf8("resource")),
                        "persistent://tenant/ns/topic",
                        1,
                        shard.partition(),
                        new byte[32],
                        new byte[32],
                        Bytes.utf8("prepared")));
    }

    @Test
    void kafkaDestinationRejectsCertifiedHandoffTiming() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final DestinationLaneId laneId = DestinationLaneId.derive(Bytes.utf8("kafka-timing-lane"));
        final DelayMessageId messageId = DelayMessageId.random(shard);

        assertThrows(
                IllegalArgumentException.class,
                () -> new KafkaDestinationRequest(
                        "cluster",
                        UUID.randomUUID(),
                        shard.partition(),
                        laneId,
                        new byte[16],
                        messageId,
                        0,
                        new byte[32],
                        99,
                        100,
                        Bytes.utf8("payload"),
                        new byte[0]));
    }

    @Test
    void pulsarTimingPolicyRejectsNonExactHandoffLead() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final DestinationPublishRequest request = new DestinationPublishRequest(
                DestinationLaneId.derive(Bytes.utf8("pulsar-timing-policy")),
                new byte[16],
                DelayMessageId.random(shard),
                0,
                new byte[32],
                99,
                200,
                Bytes.utf8("payload"),
                new byte[0]);
        assertThrows(IllegalArgumentException.class, () -> PulsarDestinationTimingPolicy.certifiedHandoff(100)
                .validate(request));
        assertThrows(IllegalArgumentException.class, () -> PulsarDestinationTimingPolicy.ordinaryManaged()
                .validate(request));
    }

    private static byte[] nonZero(final int length) {
        final byte[] value = new byte[length];
        value[0] = 1;
        return value;
    }
}
