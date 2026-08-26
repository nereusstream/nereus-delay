package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SourceAssignmentTest {
    @Test
    void assignmentBindsShardAndBarrierAndCopiesIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0);
        final byte[] identity = Bytes.sha256(Bytes.utf8("assignment"));
        final SourceAssignment assignment = new SourceAssignment(shard, identity, 4, barrier);
        identity[0] ^= 1;
        assertArrayEquals(Bytes.sha256(Bytes.utf8("assignment")), assignment.assignmentId());
    }

    @Test
    void assignmentRejectsWrongShardAndZeroIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0);
        assertThrows(IllegalArgumentException.class, () -> new SourceAssignment(shard, new byte[32], 1, barrier));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SourceAssignment(
                        new ShardId(RouteIncarnation.random(), 2), Bytes.sha256(Bytes.utf8("assignment")), 1, barrier));
    }

    @Test
    void routeFactoryUsesTheSignedPartitionBarrier() throws Exception {
        final UUID topic = UUID.randomUUID();
        final KafkaIngressRouteResource ingress =
                new KafkaIngressRouteResource("cluster", "persistent://tenant/ns/commands", topic, 1);
        final BrokerResourceIdentity resource =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", topic));
        final QuotaGrantRef quota = new QuotaGrantRef(
                Bytes.sha256(Bytes.utf8("quota")),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 20));
        final RouteSnapshot route = RouteSnapshot.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                900,
                ingress,
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                List.of(new RoutePartitionPolicy(
                        0, ActivationBarrier.kafka(resource, 0, 17, 18), quota, 1, bytes(32, 3))),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                100,
                1000,
                new com.nereusstream.delay.protocol.IngressCredentialBindingRef(
                        bytes(32, 4), 1, bytes(32, 5), bytes(32, 6), bytes(32, 7)),
                bytes(32, 8),
                new TrustedUtcIntervalEvidence(
                        200,
                        201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        bytes(8, 9),
                        1,
                        2,
                        3,
                        bytes(32, 10),
                        0,
                        null),
                1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());

        final SourceAssignment assignment =
                RouteSourceAssignmentFactory.fromRoute(route, 0, Bytes.sha256(Bytes.utf8("assignment")), 4);
        assertEquals(new ShardId(incarnation, 0), assignment.shardId());
        assertEquals(17, ((KafkaActivationBarrier) assignment.activationBarrier()).exclusiveOffset());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
