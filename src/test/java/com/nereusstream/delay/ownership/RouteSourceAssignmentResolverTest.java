package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
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
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RouteSourceAssignmentResolverTest {
    @Test
    void activeAndHistoricalLookupsUseTheAuthorizedRouteProvider() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSnapshot route = route(keys, RouteLifecycle.ACTIVE_FOR_NEW, 1);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSnapshotProvider provider = new RouteSnapshotProvider() {
            @Override
            public RouteSnapshot activeForNewSchedule(
                    final AuthenticatedTenantContext context, final RouteSelectionHint selected) {
                assertEquals(tenant, context);
                assertEquals(hint, selected);
                return route;
            }

            @Override
            public RouteSnapshot exact(final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
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
        final SourceAssignment historical = resolver.exact(tenant, route.routeIncarnation(), 0, assignmentId, 5);

        assertEquals(active.shardId(), historical.shardId());
        assertEquals(4, active.assignmentEpoch());
        assertEquals(5, historical.assignmentEpoch());
        assertEquals(active.activationBarrier(), historical.activationBarrier());
    }

    @Test
    void missingOrUnauthorizedHistoricalRouteFailsClosed() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSnapshot route = route(keys, RouteLifecycle.CONTROL_ONLY, 2);
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSnapshotProvider provider = new RouteSnapshotProvider() {
            @Override
            public RouteSnapshot activeForNewSchedule(
                    final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
                throw new IllegalStateException("not used");
            }

            @Override
            public RouteSnapshot exact(final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
                return null;
            }

            @Override
            public long publishedRevision() {
                return 0;
            }
        };
        final RouteSourceAssignmentResolver resolver = new RouteSourceAssignmentResolver(provider);

        assertThrows(
                IllegalArgumentException.class,
                () -> resolver.exact(
                        tenant, route.routeIncarnation(), 0, Bytes.sha256(Bytes.utf8("missing-assignment")), 1));
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static RouteSnapshot route(final KeyPair keys, final RouteLifecycle lifecycle, final long controlVersion) {
        final UUID topic = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResource ingress =
                new KafkaIngressRouteResource("cluster", "persistent://tenant/ns/delay", topic, 1);
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", topic));
        final QuotaGrantRef quota = new QuotaGrantRef(
                Bytes.sha256(Bytes.utf8("quota")),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        return RouteSnapshot.create(
                new RouteIncarnation(bytes(16, 30)),
                bytes(32, 1),
                bytes(32, 2),
                lifecycle,
                900,
                ingress,
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                controlVersion,
                List.of(new RoutePartitionPolicy(
                        0, ActivationBarrier.kafka(broker, 0, 17, 18), quota, 1, bytes(32, 3))),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                100,
                1000,
                new IngressCredentialBindingRef(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                bytes(32, 44),
                new TrustedUtcIntervalEvidence(
                        200,
                        201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        bytes(8, 45),
                        1,
                        2,
                        3,
                        bytes(32, 46),
                        0,
                        null),
                1,
                keys.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
