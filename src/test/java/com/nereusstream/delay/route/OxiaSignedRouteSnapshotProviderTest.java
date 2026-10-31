package com.nereusstream.delay.route;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import com.nereusstream.delay.semantic.RouteSelectionHint;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationSubscription;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class OxiaSignedRouteSnapshotProviderTest {
    @Test
    void sessionFenceRequiresExplicitReopenAfterEphemeralMarkerExpires() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        final OxiaSignedRouteSnapshotPublisher publisher =
                new OxiaSignedRouteSnapshotPublisher(session, "/nereus/route", keys.getPublic());
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(session, "/nereus/route", keys.getPublic(), () -> 200);
        try {
            final RouteSnapshot active =
                    snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1);
            client.failNextEphemeralPutAfterCommit();
            publisher.publish(hint(), active, 0);
            provider.start().toCompletableFuture().join();
            assertEquals(1, provider.publishedRevision());

            client.expireEphemeralRecords();

            assertThrows(IllegalStateException.class, () -> publisher.publish(hint(), active, 1));
            provider.refresh().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(1, provider.publishedRevision());
        } finally {
            session.close();
        }
    }

    @Test
    void startRetriesNotificationRegistrationAfterACommittedRegistrationIsFenced() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient authority = new FakeRouteClient();
        final FakeRouteClient previousNotificationClient = new FakeRouteClient();
        final FakeRouteClient replacementNotificationClient = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(
                authority, previousNotificationClient, () -> replacementNotificationClient, "/nereus/route");
        final RouteSnapshot active =
                snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1);
        final OxiaSignedRouteSnapshotPublisher publisher =
                new OxiaSignedRouteSnapshotPublisher(authority, "/nereus/route", keys.getPublic());
        publisher.publish(hint(), active, 0);
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(session, "/nereus/route", keys.getPublic(), () -> 200);
        try {
            previousNotificationClient.afterNextNotifications(authority::expireEphemeralRecords);
            assertThrows(
                    RuntimeException.class,
                    () -> provider.start().toCompletableFuture().join());
            assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());

            provider.start().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(1, provider.publishedRevision());
        } finally {
            provider.close();
        }
    }

    @Test
    void refreshAfterAnInitialRouteGapRestoresTheNotificationStream() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient authority = new FakeRouteClient();
        final FakeRouteClient notificationClient = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(
                authority, notificationClient, () -> new FakeRouteClient(), "/nereus/route");
        final RouteSnapshot first =
                snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1);
        final OxiaRouteSnapshotRecord firstEvent = OxiaRouteSnapshotRecord.create(1, 0, hint(), first);
        authority.seed("/nereus/route/events/00000000000000000001", firstEvent.canonicalBytes());
        authority.seed("/nereus/route/head", new OxiaRouteSnapshotHead(2, firstEvent.recordDigest()).canonicalBytes());
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(session, "/nereus/route", keys.getPublic(), () -> 200);
        try {
            assertThrows(
                    RuntimeException.class,
                    () -> provider.start().toCompletableFuture().join());
            assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());

            final RouteSnapshot second = snapshot(keys, first.routeIncarnation(), RouteLifecycle.CONTROL_ONLY, 2);
            final OxiaRouteSnapshotRecord secondEvent = OxiaRouteSnapshotRecord.create(2, 1, hint(), second);
            authority.seed("/nereus/route/events/00000000000000000002", secondEvent.canonicalBytes());
            authority.seed(
                    "/nereus/route/head", new OxiaRouteSnapshotHead(2, secondEvent.recordDigest()).canonicalBytes());

            provider.refresh().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(2, provider.publishedRevision());
        } finally {
            provider.close();
        }
    }

    @Test
    void explicitSessionReconnectRotatesMarkerAfterFenceAndRestoresReads() {
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        try {
            session.startSession();
            final byte[] firstIdentity = session.sessionIdentity();
            client.expireEphemeralRecords();
            assertThrows(IllegalStateException.class, () -> session.get("/nereus/route/missing"));

            session.reconnectSession();

            assertFalse(java.util.Arrays.equals(firstIdentity, session.sessionIdentity()));
            assertNull(session.get("/nereus/route/missing"));
        } finally {
            session.close();
        }
    }

    @Test
    void explicitSessionReconnectRotatesMarkerWhenTheOldMarkerIsStillReadable() {
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        try {
            session.startSession();
            final byte[] firstIdentity = session.sessionIdentity();

            session.reconnectSession();

            assertFalse(java.util.Arrays.equals(firstIdentity, session.sessionIdentity()));
            assertNull(session.get("/nereus/route/missing"));
        } finally {
            session.close();
        }
    }

    @Test
    void explicitSessionReconnectReplacesAFactoryManagedAuthorityClient() throws Exception {
        final FakeRouteClient previousAuthority = new FakeRouteClient();
        final FakeRouteClient replacementAuthority = new FakeRouteClient();
        replacementAuthority.put("/replacement-session-prime", bytes(32, 90), Set.of(PutOption.AsEphemeralRecord));
        final FakeRouteClient notification = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(
                previousAuthority, () -> replacementAuthority, notification, null, "/nereus/route");
        try {
            session.startSession();
            final byte[] firstIdentity = session.sessionIdentity();

            session.reconnectSession();

            assertEquals(1, previousAuthority.closeCount());
            assertFalse(java.util.Arrays.equals(firstIdentity, session.sessionIdentity()));
            assertNull(session.get("/nereus/route/missing"));
        } finally {
            session.close();
        }
        assertEquals(1, replacementAuthority.closeCount());
        assertEquals(1, notification.closeCount());
    }

    @Test
    void sessionCloseAttemptsTheIndependentWatchClientAfterAuthorityCloseFails() {
        final FakeRouteClient authority = new FakeRouteClient();
        final FakeRouteClient notification = new FakeRouteClient();
        final OxiaRouteAuthoritySession session =
                new OxiaRouteAuthoritySession(authority, notification, () -> new FakeRouteClient(), "/nereus/route");
        authority.failNextClose();

        final IllegalStateException failure = assertThrows(IllegalStateException.class, session::close);

        assertEquals("simulated Route client close failure", failure.getMessage());
        assertEquals(1, authority.closeCount());
        assertEquals(1, notification.closeCount());

        session.close();

        assertEquals(2, authority.closeCount());
        assertEquals(2, notification.closeCount());
    }

    @Test
    void routeProviderRetriesClientCloseAfterAReleaseFailure() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        client.failNextClose();

        final IllegalStateException failure = assertThrows(IllegalStateException.class, provider::close);

        assertEquals("simulated Route client close failure", failure.getMessage());
        assertEquals(RouteCacheHealth.CLOSED, provider.health());
        assertEquals(1, client.closeCount());

        provider.close();

        assertEquals(2, client.closeCount());
    }

    @Test
    void routeProviderCloseAllowsItsNativeNotificationCallbackToExit() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        provider.start().toCompletableFuture().join();
        final Consumer<Notification> notification = client.watchers.getFirst();
        try (var callbacks = Executors.newSingleThreadExecutor()) {
            client.afterNextClose(() -> assertDoesNotThrow(() -> callbacks
                    .submit(() -> notification.accept(new Notification.KeyModified("/nereus/route/head", 1)))
                    .get(5, TimeUnit.SECONDS)));

            provider.close();

            assertEquals(RouteCacheHealth.CLOSED, provider.health());
            assertEquals(1, client.closeCount());
        }
    }

    @Test
    void sessionCloseAllowsItsNativeCallbackToObserveTheClosedSession() {
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        session.startSession();
        try (var callbacks = Executors.newSingleThreadExecutor()) {
            client.afterNextClose(() -> assertDoesNotThrow(() -> callbacks
                    .submit(() -> assertThrows(IllegalStateException.class, () -> session.get("/nereus/route/head")))
                    .get(5, TimeUnit.SECONDS)));

            session.close();

            assertEquals(1, client.closeCount());
        }
    }

    @Test
    void notificationReconnectRequiresTheCurrentSessionBeforeRegistration() {
        final FakeRouteClient authority = new FakeRouteClient();
        final FakeRouteClient previousNotificationClient = new FakeRouteClient();
        final FakeRouteClient replacementNotificationClient = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(
                authority, previousNotificationClient, () -> replacementNotificationClient, "/nereus/route");
        try {
            session.startSession();
            authority.expireEphemeralRecords();

            assertThrows(IllegalStateException.class, () -> session.reconnectNotifications(notification -> {}));
            assertEquals(0, replacementNotificationClient.notificationRegistrationCount());
        } finally {
            session.close();
        }
    }

    @Test
    void notificationReconnectRejectsACommittedRegistrationAfterTheMarkerChanges() {
        final FakeRouteClient authority = new FakeRouteClient();
        final FakeRouteClient previousNotificationClient = new FakeRouteClient();
        final FakeRouteClient replacementNotificationClient = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(
                authority, previousNotificationClient, () -> replacementNotificationClient, "/nereus/route");
        try {
            session.startSession();
            replacementNotificationClient.afterNextNotifications(authority::expireEphemeralRecords);

            assertThrows(IllegalStateException.class, () -> session.reconnectNotifications(notification -> {}));
            assertEquals(1, replacementNotificationClient.notificationRegistrationCount());
        } finally {
            session.close();
        }
    }

    @Test
    void sessionFenceRejectsACommittedRouteHeadAfterTheMarkerChanges() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        final OxiaSignedRouteSnapshotPublisher publisher =
                new OxiaSignedRouteSnapshotPublisher(session, "/nereus/route", keys.getPublic());
        try {
            client.expireAfterNextHeadPut();
            assertThrows(
                    IllegalStateException.class,
                    () -> publisher.publish(
                            hint(),
                            snapshot(keys, new RouteIncarnation(bytes(16, 31)), RouteLifecycle.ACTIVE_FOR_NEW, 1),
                            0));
            assertEquals(
                    1,
                    OxiaRouteSnapshotHead.decode(
                                    client.get("/nereus/route/head").value())
                            .publishedRevision());
        } finally {
            session.close();
        }
    }

    @Test
    void publisherHeadCasAndNotificationRebuildAnExactRouteCache() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final RouteSelectionHint hint = hint();
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 30));
        final OxiaSignedRouteSnapshotPublisher publisher =
                new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);

        final RouteSnapshot active = snapshot(keys, incarnation, RouteLifecycle.ACTIVE_FOR_NEW, 1);
        publisher.publish(hint, active, 0);
        provider.start().toCompletableFuture().join();

        assertEquals(RouteCacheHealth.HEALTHY, provider.health());
        assertEquals(1, provider.publishedRevision());
        assertArrayEquals(
                active.canonicalBytes(),
                provider.activeForNewSchedule(tenant(), hint).canonicalBytes());

        final RouteSnapshot retired = snapshot(keys, incarnation, RouteLifecycle.RETIRED, 2);
        publisher.publish(hint, retired, 1);

        assertEquals(2, provider.publishedRevision());
        assertEquals(
                RouteLifecycle.RETIRED, provider.exact(incarnation, tenant()).lifecycle());
        assertThrows(IllegalArgumentException.class, () -> provider.activeForNewSchedule(tenant(), hint));
    }

    @Test
    void aFreshWatchBecomesReadyWithoutWritesAndThenObservesTheFirstRoute() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var publisher = new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        try {
            provider.start().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.UNAVAILABLE, provider.health());
            publisher.publish(
                    hint(), snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1), 0);
            assertEquals(1, provider.publishedRevision());
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
        } finally {
            provider.close();
        }
    }

    @Test
    void aPublicationBetweenWatchReadinessAndSnapshotIsIncluded() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var publisher = new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        final var active = snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1);
        client.afterNextReadiness = () -> publisher.publish(hint(), active, 0);
        try {
            provider.start().toCompletableFuture().join();
            assertEquals(1, provider.publishedRevision());
            assertArrayEquals(
                    active.canonicalBytes(),
                    provider.activeForNewSchedule(tenant(), hint()).canonicalBytes());
        } finally {
            provider.close();
        }
    }

    @Test
    void aPublicationDuringSnapshotKeepsItsDirtyNotification() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var publisher = new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        final var incarnation = new RouteIncarnation(bytes(16, 30));
        publisher.publish(hint(), snapshot(keys, incarnation, RouteLifecycle.ACTIVE_FOR_NEW, 1), 0);
        client.afterNextHeadGet =
                () -> publisher.publish(hint(), snapshot(keys, incarnation, RouteLifecycle.RETIRED, 2), 1);
        try {
            provider.start().toCompletableFuture().join();
            assertEquals(2, provider.publishedRevision());
            assertEquals(
                    RouteLifecycle.RETIRED,
                    provider.exact(incarnation, tenant()).lifecycle());
        } finally {
            provider.close();
        }
    }

    @Test
    void anInvalidatedQuietWatchCannotKeepTheCacheHealthy() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var publisher = new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        publisher.publish(
                hint(), snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1), 0);
        try {
            provider.start().toCompletableFuture().join();
            client.subscription.invalidated.completeExceptionally(new IllegalStateException("stream disconnected"));
            assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());
            assertThrows(IllegalStateException.class, () -> provider.activeForNewSchedule(tenant(), hint()));
            provider.refresh().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(1, provider.publishedRevision());
        } finally {
            provider.close();
        }
    }

    @Test
    void oldSnapshotAndCallbacksCannotReplaceANewerWatchGeneration() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var publisher = new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        final var incarnation = new RouteIncarnation(bytes(16, 30));
        publisher.publish(hint(), snapshot(keys, incarnation, RouteLifecycle.ACTIVE_FOR_NEW, 1), 0);
        final var oldCallback = new java.util.concurrent.atomic.AtomicReference<Consumer<Notification>>();
        client.afterNextHeadGet = () -> {
            oldCallback.set(client.watchers.getFirst());
            publisher.publish(hint(), snapshot(keys, incarnation, RouteLifecycle.RETIRED, 2), 1);
            provider.refresh().toCompletableFuture().join();
        };
        try {
            assertThrows(
                    RuntimeException.class,
                    () -> provider.start().toCompletableFuture().join());
            client.seed("/nereus/route/head", new byte[] {0});
            oldCallback.get().accept(new Notification.KeyModified("/nereus/route/head", 3));
            assertEquals(2, provider.publishedRevision());
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(
                    RouteLifecycle.RETIRED,
                    provider.exact(incarnation, tenant()).lifecycle());
        } finally {
            provider.close();
        }
    }

    @Test
    void anErrorBeforeTheFirstBatchDoesNotMakeAWatchReady() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        client.afterNextNotifications(() -> {
            final var failure = new IllegalStateException("stream failed before its first batch");
            client.subscription.ready.completeExceptionally(failure);
            client.subscription.invalidated.completeExceptionally(failure);
        });
        try {
            assertThrows(
                    RuntimeException.class,
                    () -> provider.start().toCompletableFuture().join());
            assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());
            assertEquals(0, provider.publishedRevision());
        } finally {
            provider.close();
        }
    }

    @Test
    void aFreshWatchWaitsForTheOriginalStreamToActuallyExit() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var publisher = new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        publisher.publish(
                hint(), snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1), 0);
        client.holdNextTermination = true;
        provider.start().toCompletableFuture().join();
        final var original = client.subscription;
        try {
            assertThrows(
                    RuntimeException.class,
                    () -> provider.refresh().toCompletableFuture().join());
            assertEquals(1, client.notificationRegistrationCount());
            assertEquals(RouteCacheHealth.UNAVAILABLE, provider.health());
            original.terminated.complete(null);
            provider.refresh().toCompletableFuture().join();
            assertEquals(RouteCacheHealth.HEALTHY, provider.health());
            assertEquals(1, provider.publishedRevision());
        } finally {
            original.terminated.complete(null);
            provider.close();
        }
    }

    @Test
    void closeCanCancelAnOpeningWatchWithoutHoldingTheCacheMonitor() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final var provider = new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);
        final var registered = new java.util.concurrent.CountDownLatch(1);
        client.holdNextReadiness = true;
        client.afterNextNotifications(registered::countDown);
        try (var executor = Executors.newSingleThreadExecutor()) {
            final var opening = executor.submit(provider::start);
            org.junit.jupiter.api.Assertions.assertTrue(registered.await(5, TimeUnit.SECONDS));
            assertEquals(RouteCacheHealth.UNAVAILABLE, provider.health());
            provider.close();
            assertThrows(
                    RuntimeException.class,
                    () -> opening.get(5, TimeUnit.SECONDS).toCompletableFuture().join());
            assertEquals(RouteCacheHealth.CLOSED, provider.health());
        }
    }

    @Test
    void missingHeadEventFailsClosedAndRefreshCanRecoverAfterRepair() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final RouteSelectionHint hint = hint();
        final RouteSnapshot snapshot =
                snapshot(keys, new RouteIncarnation(bytes(16, 30)), RouteLifecycle.ACTIVE_FOR_NEW, 1);
        final OxiaRouteSnapshotRecord event = OxiaRouteSnapshotRecord.create(1, 0, hint, snapshot);
        client.seed("/nereus/route/events/00000000000000000001", event.canonicalBytes());
        client.seed("/nereus/route/head", new OxiaRouteSnapshotHead(2, event.recordDigest()).canonicalBytes());
        final OxiaSignedRouteSnapshotProvider provider =
                new OxiaSignedRouteSnapshotProvider(client, "/nereus/route", keys.getPublic(), () -> 200);

        assertThrows(
                RuntimeException.class,
                () -> provider.start().toCompletableFuture().join());
        assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());

        client.seed(
                "/nereus/route/events/00000000000000000002",
                OxiaRouteSnapshotRecord.create(
                                2,
                                1,
                                hint(),
                                snapshot(keys, snapshot.routeIncarnation(), RouteLifecycle.CONTROL_ONLY, 2))
                        .canonicalBytes());
        client.seed(
                "/nereus/route/head",
                new OxiaRouteSnapshotHead(
                                2,
                                OxiaRouteSnapshotRecord.create(
                                                2,
                                                1,
                                                hint(),
                                                snapshot(
                                                        keys,
                                                        snapshot.routeIncarnation(),
                                                        RouteLifecycle.CONTROL_ONLY,
                                                        2))
                                        .recordDigest())
                        .canonicalBytes());

        provider.refresh().toCompletableFuture().join();
        assertEquals(RouteCacheHealth.HEALTHY, provider.health());
        assertEquals(2, provider.publishedRevision());
    }

    @Test
    void publisherRejectsSameIncarnationResourceDriftBeforeHeadCas() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final RouteSelectionHint hint = hint();
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 30));
        final OxiaSignedRouteSnapshotPublisher publisher =
                new OxiaSignedRouteSnapshotPublisher(client, "/nereus/route", keys.getPublic());
        final RouteSnapshot first = snapshot(keys, incarnation, RouteLifecycle.ACTIVE_FOR_NEW, 1);
        publisher.publish(hint, first, 0);
        final RouteSnapshot changedResource = snapshotWithTopic(
                keys,
                incarnation,
                RouteLifecycle.CONTROL_ONLY,
                2,
                UUID.fromString("12345678-1234-7abc-8def-1234567890ac"));

        assertThrows(IllegalArgumentException.class, () -> publisher.publish(hint, changedResource, 1));
        assertEquals(
                1,
                OxiaRouteSnapshotHead.decode(client.get("/nereus/route/head").value())
                        .publishedRevision());
    }

    static RouteSelectionHint hint() {
        return new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
    }

    private static com.nereusstream.delay.semantic.AuthenticatedTenantContext tenant() {
        return new com.nereusstream.delay.semantic.AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    static RouteSnapshot snapshot(
            final KeyPair keys,
            final RouteIncarnation incarnation,
            final RouteLifecycle lifecycle,
            final long controlVersion) {
        return snapshotWithTopic(
                keys, incarnation, lifecycle, controlVersion, UUID.fromString("12345678-1234-7abc-8def-1234567890ab"));
    }

    private static RouteSnapshot snapshotWithTopic(
            final KeyPair keys,
            final RouteIncarnation incarnation,
            final RouteLifecycle lifecycle,
            final long controlVersion,
            final UUID topic) {
        final KafkaIngressRouteResource ingress =
                new KafkaIngressRouteResource("cluster", "persistent://tenant/ns/delay", topic, 2);
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", topic));
        final QuotaGrantRef quota = new QuotaGrantRef(
                bytes(32, 20),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        return RouteSnapshot.create(
                incarnation,
                bytes(32, 1),
                bytes(32, 2),
                lifecycle,
                900,
                ingress,
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
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

    private static RoutePartitionPolicy policy(
            final int partition, final BrokerResourceIdentity broker, final QuotaGrantRef quota) {
        return new RoutePartitionPolicy(
                partition, ActivationBarrier.kafka(broker, partition, 0, 0), quota, 1, bytes(32, 50 + partition));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    static final class FakeRouteClient implements OxiaRouteRecordClient {
        private final Map<String, Stored> records = new TreeMap<>();
        private final Set<String> ephemeralKeys = new HashSet<>();
        private final List<Consumer<Notification>> watchers = new ArrayList<>();
        private long nextVersion = 1;
        private long nextSessionId = 7;
        private boolean failNextEphemeralPutAfterCommit;
        private Runnable afterNextHeadPut;
        private Runnable afterNextNotifications;
        private int notificationRegistrations;
        private int closeCalls;
        private int failNextCloseCalls;
        private Runnable afterNextClose;
        private Runnable afterNextHeadGet;
        private Runnable afterNextReadiness;
        private boolean holdNextReadiness;
        private boolean holdNextTermination;
        private FakeSubscription subscription;

        @Override
        public GetResult get(final String key) {
            final Stored stored = records.get(key);
            final GetResult result =
                    stored == null ? null : new GetResult(key, Bytes.copy(stored.value()), stored.version());
            if (key.endsWith("/head") && afterNextHeadGet != null) {
                final Runnable callback = afterNextHeadGet;
                afterNextHeadGet = null;
                callback.run();
            }
            return result;
        }

        @Override
        public CloseableIterable<GetResult> rangeScan(final String startKeyInclusive, final String endKeyExclusive) {
            final List<GetResult> values = records.entrySet().stream()
                    .filter(entry -> entry.getKey().compareTo(startKeyInclusive) >= 0
                            && entry.getKey().compareTo(endKeyExclusive) < 0)
                    .map(entry -> new GetResult(
                            entry.getKey(),
                            Bytes.copy(entry.getValue().value()),
                            entry.getValue().version()))
                    .toList();
            return new CloseableIterable<>() {
                @Override
                public Iterator<GetResult> iterator() {
                    return values.iterator();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void notifications(final Consumer<Notification> consumer) {
            watchers.add(consumer);
            notificationRegistrations++;
            if (afterNextNotifications != null) {
                final Runnable callback = afterNextNotifications;
                afterNextNotifications = null;
                callback.run();
            }
        }

        @Override
        public NotificationSubscription notificationsReady(final Consumer<Notification> consumer) {
            if (subscription != null && !subscription.terminated.isDone()) {
                throw new IllegalStateException("original fake watch stream has not exited");
            }
            final var opened = new FakeSubscription(consumer, holdNextTermination);
            subscription = opened;
            final boolean hold = holdNextReadiness;
            holdNextReadiness = false;
            holdNextTermination = false;
            notifications(consumer);
            if (!hold) {
                opened.ready.complete(null);
            }
            if (afterNextReadiness != null) {
                final Runnable callback = afterNextReadiness;
                afterNextReadiness = null;
                callback.run();
            }
            return opened;
        }

        private final class FakeSubscription implements NotificationSubscription {
            private final Consumer<Notification> consumer;
            private final CompletableFuture<Void> ready = new CompletableFuture<>();
            private final CompletableFuture<Void> invalidated = new CompletableFuture<>();
            private final CompletableFuture<Void> terminated = new CompletableFuture<>();
            private final boolean holdTermination;

            private FakeSubscription(Consumer<Notification> consumer, boolean holdTermination) {
                this.consumer = consumer;
                this.holdTermination = holdTermination;
            }

            @Override
            public CompletableFuture<Void> ready() {
                return ready.copy();
            }

            @Override
            public CompletableFuture<Void> invalidated() {
                return invalidated.copy();
            }

            @Override
            public CompletableFuture<Void> termination() {
                return terminated.copy();
            }

            @Override
            public Duration requestTimeout() {
                return Duration.ofSeconds(2);
            }

            @Override
            public void requireCurrent() {
                if (!ready.isDone() || ready.isCompletedExceptionally() || invalidated.isDone()) {
                    throw new IllegalStateException("fake notification stream is not current");
                }
            }

            @Override
            public void close() {
                invalidated.complete(null);
                ready.completeExceptionally(new IllegalStateException("fake watch closed before readiness"));
                watchers.remove(consumer);
                if (!holdTermination) {
                    terminated.complete(null);
                }
            }
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final Stored existing = records.get(key);
            final OptionVersionId expected = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (expected != null && expected.versionId() == OptionVersionId.KEY_NOT_EXISTS && existing != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (expected != null
                    && expected.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (existing == null || existing.version().versionId() != expected.versionId())) {
                throw new UnexpectedVersionIdException(key, expected.versionId());
            }
            final boolean ephemeral = options.contains(PutOption.AsEphemeralRecord);
            final Version version = new Version(
                    nextVersion++,
                    0,
                    0,
                    0,
                    ephemeral ? Optional.of(nextSessionId++) : Optional.empty(),
                    ephemeral ? Optional.of("fake-route-client") : Optional.empty());
            records.put(key, new Stored(Bytes.copy(value), version));
            if (ephemeral) {
                ephemeralKeys.add(key);
            }
            final Notification notification = existing == null
                    ? new Notification.KeyCreated(key, version.versionId())
                    : new Notification.KeyModified(key, version.versionId());
            for (Consumer<Notification> watcher : List.copyOf(watchers)) {
                watcher.accept(notification);
            }
            if (ephemeral && failNextEphemeralPutAfterCommit) {
                failNextEphemeralPutAfterCommit = false;
                throw new IllegalStateException("simulated Route session put response loss");
            }
            if (!ephemeral && key.endsWith("/head") && afterNextHeadPut != null) {
                final Runnable callback = afterNextHeadPut;
                afterNextHeadPut = null;
                callback.run();
            }
            return new PutResult(key, version);
        }

        private void seed(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 0, Optional.empty(), Optional.empty());
            records.put(key, new Stored(Bytes.copy(value), version));
        }

        void expireEphemeralRecords() {
            for (String key : Set.copyOf(ephemeralKeys)) {
                records.remove(key);
            }
            ephemeralKeys.clear();
        }

        void failNextEphemeralPutAfterCommit() {
            failNextEphemeralPutAfterCommit = true;
        }

        void expireAfterNextHeadPut() {
            afterNextHeadPut = this::expireEphemeralRecords;
        }

        void afterNextNotifications(final Runnable callback) {
            afterNextNotifications = callback;
        }

        int notificationRegistrationCount() {
            return notificationRegistrations;
        }

        void failNextClose() {
            failNextCloseCalls++;
        }

        void afterNextClose(final Runnable callback) {
            afterNextClose = callback;
        }

        int closeCount() {
            return closeCalls;
        }

        @Override
        public void close() {
            closeCalls++;
            if (afterNextClose != null) {
                final Runnable callback = afterNextClose;
                afterNextClose = null;
                callback.run();
            }
            if (subscription != null) {
                subscription.close();
                if (!subscription.terminated.isDone()) {
                    throw new IllegalStateException("original fake watch stream cleanup remains pending");
                }
            }
            records.clear();
            ephemeralKeys.clear();
            watchers.clear();
            if (failNextCloseCalls > 0) {
                failNextCloseCalls--;
                throw new IllegalStateException("simulated Route client close failure");
            }
        }

        private record Stored(byte[] value, Version version) {}
    }
}
