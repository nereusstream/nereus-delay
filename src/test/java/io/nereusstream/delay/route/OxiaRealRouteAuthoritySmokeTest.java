package io.nereusstream.delay.route;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in real Oxia coverage for signed Route event/head publication and refresh. */
@Tag("real-service")
class OxiaRealRouteAuthoritySmokeTest {
    @Test
    void signedRoutePublicationHeadCasAndRefreshWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-route/" + UUID.randomUUID();
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSelectionHintWithTenant route = route();

        try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                     endpoint, namespace, "nereus-delay-real-route-publisher-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix);
             OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                     endpoint, namespace, "nereus-delay-real-route-provider-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix)) {
            final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                    publisherSession, prefix, keys.getPublic());
            final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                    providerSession, prefix, keys.getPublic(), () -> 200);
            final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 30));
            final RouteSnapshotV1 active = OxiaSignedRouteSnapshotProviderTest.snapshot(
                    keys, incarnation, RouteLifecycleV1.ACTIVE_FOR_NEW, 1);

            assertEquals(1, publisher.publish(route.hint(), active, 0).revision());
            provider.refresh().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(1, provider.publishedRevision());
            assertArrayEquals(active.canonicalBytes(),
                    provider.activeForNewSchedule(route.tenant(), route.hint()).canonicalBytes());

            final RouteSnapshotV1 retired = OxiaSignedRouteSnapshotProviderTest.snapshot(
                    keys, incarnation, RouteLifecycleV1.RETIRED, 2);
            assertEquals(2, publisher.publish(route.hint(), retired, 1).revision());
            provider.refresh().toCompletableFuture().join();
            assertEquals(2, provider.publishedRevision());
            assertEquals(RouteLifecycleV1.RETIRED,
                    provider.exact(incarnation, route.tenant()).lifecycle());
            assertThrows(IllegalArgumentException.class,
                    () -> provider.activeForNewSchedule(route.tenant(), route.hint()));
            provider.close();
        }
    }

    @Test
    void signedRouteNotificationsRefreshAStartedProviderAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-route-notification/" + UUID.randomUUID();
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSelectionHintWithTenant route = route();

        try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                     endpoint, namespace, "nereus-delay-route-notification-publisher-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix);
             OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                     endpoint, namespace, "nereus-delay-route-notification-provider-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), prefix)) {
            final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                    publisherSession, prefix, keys.getPublic());
            final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                    providerSession, prefix, keys.getPublic(), () -> 200);
            try {
                provider.start().toCompletableFuture().join();
                final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 30));
                final RouteSnapshotV1 active = OxiaSignedRouteSnapshotProviderTest.snapshot(
                        keys, incarnation, RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
                publisher.publish(route.hint(), active, 0);

                awaitRevision(provider, 1);
                assertEquals(RouteCacheHealth.HEALTHY, provider.health());
                assertArrayEquals(active.canonicalBytes(),
                        provider.activeForNewSchedule(route.tenant(), route.hint()).canonicalBytes());

                final RouteSnapshotV1 retired = OxiaSignedRouteSnapshotProviderTest.snapshot(
                        keys, incarnation, RouteLifecycleV1.RETIRED, 2);
                publisher.publish(route.hint(), retired, 1);
                awaitRevision(provider, 2);
                assertEquals(RouteLifecycleV1.RETIRED,
                        provider.exact(incarnation, route.tenant()).lifecycle());
                assertTrue(provider.health() == RouteCacheHealth.HEALTHY);
            } finally {
                provider.close();
            }
        }
    }

    private static void awaitRevision(final OxiaSignedRouteSnapshotProvider provider, final long revision)
            throws InterruptedException {
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (provider.publishedRevision() < revision && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertEquals(revision, provider.publishedRevision());
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static RouteSelectionHintWithTenant route() {
        return new RouteSelectionHintWithTenant(OxiaSignedRouteSnapshotProviderTest.hint(),
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record RouteSelectionHintWithTenant(io.nereusstream.delay.semantic.RouteSelectionHint hint,
                                                AuthenticatedTenantContext tenant) {
    }
}
