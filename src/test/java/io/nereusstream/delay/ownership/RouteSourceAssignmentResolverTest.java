package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.route.RouteSnapshotProvider;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RouteSourceAssignmentResolverTest {
    @Test
    void activeAndHistoricalLookupsUseTheAuthorizedRouteProvider() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSnapshotV1 route = route(keys, RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSnapshotProvider provider = new RouteSnapshotProvider() {
            @Override
            public RouteSnapshotV1 activeForNewSchedule(final AuthenticatedTenantContext context,
                                                         final RouteSelectionHint selected) {
                assertEquals(tenant, context);
                assertEquals(hint, selected);
                return route;
            }

            @Override
            public RouteSnapshotV1 exact(final RouteIncarnation incarnation,
                                          final AuthenticatedTenantContext context) {
                assertEquals(tenant, context);
                return route.routeIncarnation().equals(incarnation) ? route : null;
            }

            @Override
            public long publishedRevision() {
                return 1;
            }
        };
        final RouteSourceAssignmentResolver resolver = new RouteSourceAssignmentResolver(provider);
        final byte[] assignmentId = Bytes.sha256(Bytes.utf8("route-assignment"));

        final SourceAssignment active = resolver.active(tenant, hint, 0, assignmentId, 4);
        final SourceAssignment historical = resolver.exact(tenant, route.routeIncarnation(), 0,
                assignmentId, 5);

        assertEquals(active.shardId(), historical.shardId());
        assertEquals(4, active.assignmentEpoch());
        assertEquals(5, historical.assignmentEpoch());
        assertEquals(active.activationBarrier(), historical.activationBarrier());
    }

    @Test
    void missingOrUnauthorizedHistoricalRouteFailsClosed() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSnapshotV1 route = route(keys, RouteLifecycleV1.CONTROL_ONLY, 2);
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSnapshotProvider provider = new RouteSnapshotProvider() {
            @Override
            public RouteSnapshotV1 activeForNewSchedule(final AuthenticatedTenantContext context,
                                                         final RouteSelectionHint hint) {
                throw new IllegalStateException("not used");
            }

            @Override
            public RouteSnapshotV1 exact(final RouteIncarnation incarnation,
                                          final AuthenticatedTenantContext context) {
                return null;
            }

            @Override
            public long publishedRevision() {
                return 0;
            }
        };
        final RouteSourceAssignmentResolver resolver = new RouteSourceAssignmentResolver(provider);

        assertThrows(IllegalArgumentException.class, () -> resolver.exact(tenant, route.routeIncarnation(), 0,
                Bytes.sha256(Bytes.utf8("missing-assignment")), 1));
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static RouteSnapshotV1 route(final KeyPair keys, final RouteLifecycleV1 lifecycle,
                                         final long controlVersion) {
        final UUID topic = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResourceV1 ingress = new KafkaIngressRouteResourceV1("cluster",
                "persistent://tenant/ns/delay", topic, 1);
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", topic));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("quota")), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        return RouteSnapshotV1.create(new RouteIncarnation(bytes(16, 30)), bytes(32, 1), bytes(32, 2), lifecycle,
                900, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), controlVersion,
                List.of(new RoutePartitionPolicyV1(0, ActivationBarrierV1.kafka(broker, 0, 17, 18), quota, 1,
                        bytes(32, 3))), 100, 200, 1024, 4096, 10, 8192, 500, 100, 1000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                bytes(32, 44), new TrustedUtcIntervalEvidence(200, 201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 45), 1, 2, 3,
                        bytes(32, 46), 0, null), 1, keys.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
