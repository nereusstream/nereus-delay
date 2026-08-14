package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        assertThrows(IllegalArgumentException.class,
                () -> new SourceAssignment(shard, new byte[32], 1, barrier));
        assertThrows(IllegalArgumentException.class,
                () -> new SourceAssignment(new ShardId(RouteIncarnation.random(), 2),
                        Bytes.sha256(Bytes.utf8("assignment")), 1, barrier));
    }

    @Test
    void routeFactoryUsesTheSignedPartitionBarrier() throws Exception {
        final UUID topic = UUID.randomUUID();
        final KafkaIngressRouteResourceV1 ingress = new KafkaIngressRouteResourceV1(
                "cluster", "persistent://tenant/ns/commands", topic, 1);
        final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", topic));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("quota")), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 20));
        final RouteSnapshotV1 route = RouteSnapshotV1.create(incarnation, bytes(32, 1), bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW, 900, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1,
                List.of(new RoutePartitionPolicyV1(0, ActivationBarrierV1.kafka(resource, 0, 17, 18), quota, 1,
                        bytes(32, 3))), 100, 200, 1024, 4096, 10, 8192, 500, 100, 1000,
                new io.nereusstream.delay.protocol.IngressCredentialBindingRefV1(bytes(32, 4), 1,
                        bytes(32, 5), bytes(32, 6), bytes(32, 7)), bytes(32, 8),
                new TrustedUtcIntervalEvidence(200, 201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 9), 1, 2, 3,
                        bytes(32, 10), 0, null), 1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());

        final SourceAssignment assignment = RouteSourceAssignmentFactory.fromRoute(route, 0,
                Bytes.sha256(Bytes.utf8("assignment")), 4);
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
