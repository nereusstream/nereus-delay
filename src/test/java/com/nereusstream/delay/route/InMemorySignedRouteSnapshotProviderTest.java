package com.nereusstream.delay.route;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ActivationBarrierV1;
import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycleV1;
import com.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.RoutingHashVersionV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InMemorySignedRouteSnapshotProviderTest {
    @Test
    void contiguousSignedWatchPublishesAliasAndHistoricalRoute() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSnapshotV1 snapshot = snapshot(keys);
        final InMemorySignedRouteSnapshotProvider provider =
                new InMemorySignedRouteSnapshotProvider(keys.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final AuthenticatedTenantContext tenant =
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));

        provider.accept(1, 0, hint, snapshot);

        assertEquals(RouteCacheHealth.HEALTHY, provider.health());
        assertEquals(1, provider.publishedRevision());
        assertEquals(snapshot, provider.activeForNewSchedule(tenant, hint));
        assertEquals(snapshot, provider.exact(snapshot.routeIncarnation(), tenant));
        assertNull(provider.exact(
                snapshot.routeIncarnation(), new AuthenticatedTenantContext(bytes(32, 4), bytes(32, 5), bytes(32, 6))));
    }

    @Test
    void watchGapFreezesAllReads() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemorySignedRouteSnapshotProvider provider =
                new InMemorySignedRouteSnapshotProvider(keys.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        provider.accept(1, 0, hint, snapshot(keys));

        assertThrows(IllegalArgumentException.class, () -> provider.accept(3, 1, hint, snapshot(keys)));
        assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());
        assertThrows(
                IllegalStateException.class,
                () -> provider.activeForNewSchedule(
                        new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3)), hint));
    }

    @Test
    void wrongVerificationKeyFreezesSignatureHealth() throws Exception {
        final KeyPair signer = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final KeyPair wrong = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemorySignedRouteSnapshotProvider provider =
                new InMemorySignedRouteSnapshotProvider(wrong.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));

        assertThrows(IllegalArgumentException.class, () -> provider.accept(1, 0, hint, snapshot(signer)));
        assertEquals(RouteCacheHealth.SIGNATURE_INVALID, provider.health());
    }

    @Test
    void compatibleSameIncarnationSuccessorUpdatesTheHistoricalView() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemorySignedRouteSnapshotProvider provider =
                new InMemorySignedRouteSnapshotProvider(keys.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final RouteSnapshotV1 first = snapshot(keys);
        final RouteSnapshotV1 successor = snapshot(keys, RouteLifecycleV1.CONTROL_ONLY, 2);

        provider.accept(1, 0, hint, first);
        provider.accept(2, 1, hint, successor);

        assertEquals(RouteCacheHealth.HEALTHY, provider.health());
        assertEquals(
                RouteLifecycleV1.CONTROL_ONLY,
                provider.exact(
                                first.routeIncarnation(),
                                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3)))
                        .lifecycle());
    }

    private static RouteSnapshotV1 snapshot(final KeyPair keys) {
        return snapshot(keys, RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
    }

    private static RouteSnapshotV1 snapshot(
            final KeyPair keys, final RouteLifecycleV1 lifecycle, final long controlVersion) {
        final UUID topic = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResourceV1 ingress =
                new KafkaIngressRouteResourceV1("cluster", "persistent://tenant/ns/delay", topic, 2);
        final BrokerResourceIdentityV1 broker =
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", topic));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(
                bytes(32, 20),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        return RouteSnapshotV1.create(
                new RouteIncarnation(bytes(16, 30)),
                bytes(32, 1),
                bytes(32, 2),
                lifecycle,
                900,
                ingress,
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1),
                controlVersion,
                List.of(policy(0, broker, quota), policy(1, broker, quota)),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                100,
                1000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
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

    private static RoutePartitionPolicyV1 policy(
            final int number, final BrokerResourceIdentityV1 broker, final QuotaGrantRefV1 quota) {
        return new RoutePartitionPolicyV1(
                number, ActivationBarrierV1.kafka(broker, number, 0, 0), quota, 1, bytes(32, 50 + number));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
