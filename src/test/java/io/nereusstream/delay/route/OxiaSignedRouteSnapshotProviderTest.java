package io.nereusstream.delay.route;

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
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaSignedRouteSnapshotProviderTest {
    @Test
    void sessionFenceRequiresExplicitReopenAfterEphemeralMarkerExpires() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(session,
                "/nereus/route", keys.getPublic());
        final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(session,
                "/nereus/route", keys.getPublic(), () -> 200);
        try {
            final RouteSnapshotV1 active = snapshot(keys, new RouteIncarnation(bytes(16, 30)),
                    RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
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
    void sessionFenceRejectsACommittedRouteHeadAfterTheMarkerChanges() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final OxiaRouteAuthoritySession session = new OxiaRouteAuthoritySession(client, "/nereus/route");
        final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(session,
                "/nereus/route", keys.getPublic());
        try {
            client.expireAfterNextHeadPut();
            assertThrows(IllegalStateException.class, () -> publisher.publish(hint(),
                    snapshot(keys, new RouteIncarnation(bytes(16, 31)), RouteLifecycleV1.ACTIVE_FOR_NEW, 1), 0));
            assertEquals(1, OxiaRouteSnapshotHeadV1.decode(client.get("/nereus/route/head").value())
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
        final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(client,
                "/nereus/route", keys.getPublic());
        final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(client,
                "/nereus/route", keys.getPublic(), () -> 200);

        final RouteSnapshotV1 active = snapshot(keys, incarnation, RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
        publisher.publish(hint, active, 0);
        provider.start().toCompletableFuture().join();

        assertEquals(RouteCacheHealth.HEALTHY, provider.health());
        assertEquals(1, provider.publishedRevision());
        assertArrayEquals(active.canonicalBytes(), provider.activeForNewSchedule(tenant(), hint).canonicalBytes());

        final RouteSnapshotV1 retired = snapshot(keys, incarnation, RouteLifecycleV1.RETIRED, 2);
        publisher.publish(hint, retired, 1);

        assertEquals(2, provider.publishedRevision());
        assertEquals(RouteLifecycleV1.RETIRED,
                provider.exact(incarnation, tenant()).lifecycle());
        assertThrows(IllegalArgumentException.class, () -> provider.activeForNewSchedule(tenant(), hint));
    }

    @Test
    void missingHeadEventFailsClosedAndRefreshCanRecoverAfterRepair() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final FakeRouteClient client = new FakeRouteClient();
        final RouteSelectionHint hint = hint();
        final RouteSnapshotV1 snapshot = snapshot(keys, new RouteIncarnation(bytes(16, 30)),
                RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
        final OxiaRouteSnapshotRecordV1 event = OxiaRouteSnapshotRecordV1.create(1, 0, hint, snapshot);
        client.seed("/nereus/route/events/00000000000000000001", event.canonicalBytes());
        client.seed("/nereus/route/head", new OxiaRouteSnapshotHeadV1(2, event.recordDigest()).canonicalBytes());
        final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(client,
                "/nereus/route", keys.getPublic(), () -> 200);

        assertThrows(RuntimeException.class, () -> provider.start().toCompletableFuture().join());
        assertEquals(RouteCacheHealth.WATCH_GAP, provider.health());

        client.seed("/nereus/route/events/00000000000000000002",
                OxiaRouteSnapshotRecordV1.create(2, 1, hint(), snapshot(keys, snapshot.routeIncarnation(),
                        RouteLifecycleV1.CONTROL_ONLY, 2)).canonicalBytes());
        client.seed("/nereus/route/head", new OxiaRouteSnapshotHeadV1(2,
                OxiaRouteSnapshotRecordV1.create(2, 1, hint(), snapshot(keys, snapshot.routeIncarnation(),
                        RouteLifecycleV1.CONTROL_ONLY, 2)).recordDigest()).canonicalBytes());

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
        final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(client,
                "/nereus/route", keys.getPublic());
        final RouteSnapshotV1 first = snapshot(keys, incarnation, RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
        publisher.publish(hint, first, 0);
        final RouteSnapshotV1 changedResource = snapshotWithTopic(keys, incarnation,
                RouteLifecycleV1.CONTROL_ONLY, 2, UUID.fromString("12345678-1234-7abc-8def-1234567890ac"));

        assertThrows(IllegalArgumentException.class, () -> publisher.publish(hint, changedResource, 1));
        assertEquals(1, OxiaRouteSnapshotHeadV1.decode(client.get("/nereus/route/head").value())
                .publishedRevision());
    }

    static RouteSelectionHint hint() {
        return new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
    }

    private static io.nereusstream.delay.semantic.AuthenticatedTenantContext tenant() {
        return new io.nereusstream.delay.semantic.AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2),
                bytes(32, 3));
    }

    static RouteSnapshotV1 snapshot(final KeyPair keys, final RouteIncarnation incarnation,
                                    final RouteLifecycleV1 lifecycle, final long controlVersion) {
        return snapshotWithTopic(keys, incarnation, lifecycle, controlVersion,
                UUID.fromString("12345678-1234-7abc-8def-1234567890ab"));
    }

    private static RouteSnapshotV1 snapshotWithTopic(final KeyPair keys, final RouteIncarnation incarnation,
                                                     final RouteLifecycleV1 lifecycle, final long controlVersion,
                                                     final UUID topic) {
        final KafkaIngressRouteResourceV1 ingress = new KafkaIngressRouteResourceV1("cluster",
                "persistent://tenant/ns/delay", topic, 2);
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", topic));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(bytes(32, 20), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0));
        return RouteSnapshotV1.create(incarnation, bytes(32, 1), bytes(32, 2), lifecycle, 900, ingress,
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), controlVersion,
                List.of(policy(0, broker, quota), policy(1, broker, quota)), 100, 200, 1024, 4096, 10,
                8192, 500, 100, 1000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                bytes(32, 44), new TrustedUtcIntervalEvidence(200, 201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 45), 1, 2, 3,
                        bytes(32, 46), 0, null), 1, keys.getPrivate());
    }

    private static RoutePartitionPolicyV1 policy(final int partition, final BrokerResourceIdentityV1 broker,
                                                  final QuotaGrantRefV1 quota) {
        return new RoutePartitionPolicyV1(partition, ActivationBarrierV1.kafka(broker, partition, 0, 0), quota, 1,
                bytes(32, 50 + partition));
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

        @Override
        public GetResult get(final String key) {
            final Stored stored = records.get(key);
            return stored == null ? null : new GetResult(key, Bytes.copy(stored.value()), stored.version());
        }

        @Override
        public CloseableIterable<GetResult> rangeScan(final String startKeyInclusive,
                                                       final String endKeyExclusive) {
            final List<GetResult> values = records.entrySet().stream()
                    .filter(entry -> entry.getKey().compareTo(startKeyInclusive) >= 0
                            && entry.getKey().compareTo(endKeyExclusive) < 0)
                    .map(entry -> new GetResult(entry.getKey(), Bytes.copy(entry.getValue().value()),
                            entry.getValue().version()))
                    .toList();
            return new CloseableIterable<>() {
                @Override
                public Iterator<GetResult> iterator() {
                    return values.iterator();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public void notifications(final Consumer<Notification> consumer) {
            watchers.add(consumer);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final Stored existing = records.get(key);
            final OptionVersionId expected = options.stream().filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast).findFirst().orElse(null);
            if (expected != null && expected.versionId() == OptionVersionId.KEY_NOT_EXISTS && existing != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (expected != null && expected.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (existing == null || existing.version().versionId() != expected.versionId())) {
                throw new UnexpectedVersionIdException(key, expected.versionId());
            }
            final boolean ephemeral = options.contains(PutOption.AsEphemeralRecord);
            final Version version = new Version(nextVersion++, 0, 0, 0,
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

        @Override
        public void close() {
            records.clear();
            ephemeralKeys.clear();
            watchers.clear();
        }

        private record Stored(byte[] value, Version version) {
        }
    }
}
