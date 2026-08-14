package io.nereusstream.delay.route;

import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.TrustedClock;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.text.Normalizer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Oxia-backed Route cache. The cache is rebuilt from the CAS head and the
 * immutable event stream; preparation only reads the synchronized maps.
 */
public final class OxiaSignedRouteSnapshotProvider implements RouteSnapshotProvider, RouteSnapshotRefresher {
    private static final String EVENTS_SUFFIX = "/events/";
    private static final String HEAD_SUFFIX = "/head";

    private final OxiaRouteRecordClient client;
    private final PublicKey verificationKey;
    private final TrustedClock trustedClock;
    private final String eventPrefix;
    private final String headKey;
    private final String eventScanEnd;
    private final Map<RouteKey, RouteSnapshotV1> active = new HashMap<>();
    private final Map<RouteIncarnation, RouteSnapshotV1> history = new HashMap<>();
    private final Map<RouteIncarnation, RouteKey> activeKeyByIncarnation = new HashMap<>();
    private long publishedRevision;
    private RouteCacheHealth health = RouteCacheHealth.UNAVAILABLE;
    private boolean started;

    /** Creates a provider over an owned/configured SyncOxiaClient. */
    public OxiaSignedRouteSnapshotProvider(final SyncOxiaClient client, final String keyPrefix,
                                           final PublicKey verificationKey, final TrustedClock trustedClock) {
        this(new SyncRecordClient(client), keyPrefix, verificationKey, trustedClock);
    }

    OxiaSignedRouteSnapshotProvider(final OxiaRouteRecordClient client, final String keyPrefix,
                                    final PublicKey verificationKey, final TrustedClock trustedClock) {
        this.client = Objects.requireNonNull(client, "client");
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        final String prefix = canonicalKeyPrefix(keyPrefix);
        this.eventPrefix = prefix + EVENTS_SUFFIX;
        this.headKey = prefix + HEAD_SUFFIX;
        this.eventScanEnd = eventPrefix + "\uffff";
    }

    @Override
    public synchronized CompletionStage<Void> start() {
        requireOpen();
        if (started) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            refreshFromAuthority();
            started = true;
            client.notifications(this::onNotification);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException failure) {
            health = classify(failure);
            return failed(failure);
        }
    }

    /** Rebuilds from the authoritative head after a gap or reconnect. */
    public synchronized CompletionStage<Void> refresh() {
        requireOpen();
        try {
            refreshFromAuthority();
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException failure) {
            health = classify(failure);
            return failed(failure);
        }
    }

    @Override
    public synchronized RouteSnapshotV1 activeForNewSchedule(final AuthenticatedTenantContext context,
                                                               final RouteSelectionHint hint) {
        requireHealthy();
        final RouteSnapshotV1 snapshot = active.get(new RouteKey(hint.adapterKind(), hint.routeAliasUtf8Nfc()));
        if (snapshot == null) {
            throw new IllegalArgumentException("Route alias is unavailable");
        }
        snapshot.requireUsableForNewSchedule(context.authenticatedTenantScopeHash(), context.tenantRoutingScope(),
                trustedClock.nowEpochMs());
        return snapshot;
    }

    @Override
    public synchronized RouteSnapshotV1 exact(final RouteIncarnation incarnation,
                                               final AuthenticatedTenantContext context) {
        requireHealthy();
        final RouteSnapshotV1 snapshot = history.get(Objects.requireNonNull(incarnation, "incarnation"));
        if (snapshot == null) {
            return null;
        }
        try {
            snapshot.requireTenantScope(context.authenticatedTenantScopeHash(), context.tenantRoutingScope());
        } catch (RuntimeException unauthorized) {
            return null;
        }
        return snapshot;
    }

    @Override
    public synchronized long publishedRevision() {
        return publishedRevision;
    }

    @Override
    public synchronized RouteCacheHealth health() {
        return health;
    }

    @Override
    public synchronized void close() {
        if (health == RouteCacheHealth.CLOSED) {
            return;
        }
        active.clear();
        history.clear();
        activeKeyByIncarnation.clear();
        health = RouteCacheHealth.CLOSED;
        client.close();
    }

    private void refreshFromAuthority() {
        final HeadEntry head = readHead();
        final Map<Long, OxiaRouteSnapshotRecordV1> events = new TreeMap<>();
        if (head != null) {
            final CloseableIterable<GetResult> scan = client.rangeScan(eventPrefix, eventScanEnd);
            try {
                for (GetResult result : scan) {
                    if (result == null || result.key() == null || result.value() == null
                            || result.version() == null || !result.key().startsWith(eventPrefix)) {
                        throw new IllegalStateException("Oxia Route scan returned an invalid record");
                    }
                    final OxiaRouteSnapshotRecordV1 event = OxiaRouteSnapshotRecordV1.decode(result.value(),
                            verificationKey);
                    if (!eventKey(event.revision()).equals(result.key())) {
                        throw new IllegalStateException("Oxia Route event key is not bound to its revision");
                    }
                    if (event.revision() <= head.head().publishedRevision()) {
                        final OxiaRouteSnapshotRecordV1 previous = events.put(event.revision(), event);
                        if (previous != null
                                && !Bytes.constantTimeEquals(previous.canonicalBytes(), event.canonicalBytes())) {
                            throw new IllegalStateException("Oxia Route stream has conflicting revisions");
                        }
                    }
                }
            } finally {
                scan.close();
            }
        }

        final Map<RouteKey, RouteSnapshotV1> nextActive = new HashMap<>();
        final Map<RouteIncarnation, RouteSnapshotV1> nextHistory = new HashMap<>();
        final Map<RouteIncarnation, RouteKey> nextActiveKeys = new HashMap<>();
        if (head == null) {
            replaceCache(nextActive, nextHistory, nextActiveKeys, 0, RouteCacheHealth.UNAVAILABLE);
            return;
        }
        for (long revision = 1; revision <= head.head().publishedRevision(); revision++) {
            final OxiaRouteSnapshotRecordV1 event = events.get(revision);
            if (event == null || event.previousRevision() != revision - 1) {
                throw new RouteGapException("Oxia Route stream has a missing or non-contiguous event");
            }
            if (revision == head.head().publishedRevision()
                    && !Bytes.constantTimeEquals(head.head().eventDigest(), event.recordDigest())) {
                throw new RouteGapException("Oxia Route head does not match its final event");
            }
            apply(nextActive, nextHistory, nextActiveKeys, event);
        }
        replaceCache(nextActive, nextHistory, nextActiveKeys, head.head().publishedRevision(),
                RouteCacheHealth.HEALTHY);
    }

    private void apply(final Map<RouteKey, RouteSnapshotV1> nextActive,
                       final Map<RouteIncarnation, RouteSnapshotV1> nextHistory,
                       final Map<RouteIncarnation, RouteKey> nextActiveKeys,
                       final OxiaRouteSnapshotRecordV1 event) {
        final RouteSnapshotV1 previous = nextHistory.get(event.snapshot().routeIncarnation());
        if (previous != null) {
            try {
                RouteSnapshotCompatibilityV1.requireCompatibleSuccessor(previous, event.snapshot());
            } catch (IllegalArgumentException incompatible) {
                throw new RouteQuarantineException(incompatible.getMessage(), incompatible);
            }
            final RouteKey oldKey = nextActiveKeys.get(event.snapshot().routeIncarnation());
            final RouteKey newKey = new RouteKey(event.route().adapterKind(), event.route().routeAliasUtf8Nfc());
            if (oldKey != null && !oldKey.equals(newKey)) {
                nextActive.remove(oldKey);
            }
        }
        final RouteKey key = new RouteKey(event.route().adapterKind(), event.route().routeAliasUtf8Nfc());
        nextActive.put(key, event.snapshot());
        nextActiveKeys.put(event.snapshot().routeIncarnation(), key);
        nextHistory.put(event.snapshot().routeIncarnation(), event.snapshot());
    }

    private void replaceCache(final Map<RouteKey, RouteSnapshotV1> nextActive,
                              final Map<RouteIncarnation, RouteSnapshotV1> nextHistory,
                              final Map<RouteIncarnation, RouteKey> nextActiveKeys,
                              final long revision, final RouteCacheHealth nextHealth) {
        active.clear();
        active.putAll(nextActive);
        history.clear();
        history.putAll(nextHistory);
        activeKeyByIncarnation.clear();
        activeKeyByIncarnation.putAll(nextActiveKeys);
        publishedRevision = revision;
        health = nextHealth;
    }

    private void onNotification(final Notification notification) {
        if (notification == null || notification.key() == null) {
            return;
        }
        synchronized (this) {
            if (health == RouteCacheHealth.CLOSED
                    || (!headKey.equals(notification.key()) && !notification.key().startsWith(eventPrefix))) {
                return;
            }
            try {
                refreshFromAuthority();
            } catch (RuntimeException failure) {
                health = classify(failure);
            }
        }
    }

    private HeadEntry readHead() {
        final GetResult result = client.get(headKey);
        if (result == null) {
            return null;
        }
        if (!headKey.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia Route head response is not exact");
        }
        return new HeadEntry(OxiaRouteSnapshotHeadV1.decode(result.value()), result.version().versionId());
    }

    private void requireHealthy() {
        if (health != RouteCacheHealth.HEALTHY) {
            throw new IllegalStateException("Route cache is not healthy: " + health);
        }
    }

    private void requireOpen() {
        if (health == RouteCacheHealth.CLOSED) {
            throw new IllegalStateException("Route cache is closed");
        }
    }

    private static RouteCacheHealth classify(final RuntimeException failure) {
        if (failure instanceof RouteQuarantineException) {
            return RouteCacheHealth.QUARANTINED;
        }
        if (failure.getCause() instanceof RouteQuarantineException) {
            return RouteCacheHealth.QUARANTINED;
        }
        final String message = failure.getMessage();
        return message != null && message.toLowerCase(java.util.Locale.ROOT).contains("signature")
                ? RouteCacheHealth.SIGNATURE_INVALID : RouteCacheHealth.WATCH_GAP;
    }

    private String eventKey(final long revision) {
        return eventPrefix + String.format(java.util.Locale.ROOT, "%020d", revision);
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be a nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    private static <T> CompletionStage<T> failed(final RuntimeException failure) {
        return CompletableFuture.failedFuture(failure);
    }

    private record RouteKey(AdapterKindV1 adapterKind, String routeAlias) {
        private RouteKey(final AdapterKindV1 adapterKind, final byte[] routeAlias) {
            this(adapterKind, Base64.getUrlEncoder().withoutPadding().encodeToString(Bytes.copy(routeAlias)));
        }
    }

    private record HeadEntry(OxiaRouteSnapshotHeadV1 head, long versionId) {
    }

    private static final class RouteGapException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private RouteGapException(final String message) {
            super(message);
        }
    }

    private static final class RouteQuarantineException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private RouteQuarantineException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    private static final class SyncRecordClient implements OxiaRouteRecordClient {
        private final SyncOxiaClient delegate;

        private SyncRecordClient(final SyncOxiaClient delegate) {
            this.delegate = Objects.requireNonNull(delegate, "client");
        }

        @Override
        public GetResult get(final String key) {
            return delegate.get(key);
        }

        @Override
        public CloseableIterable<GetResult> rangeScan(final String startKeyInclusive,
                                                       final String endKeyExclusive) {
            return delegate.rangeScan(startKeyInclusive, endKeyExclusive);
        }

        @Override
        public void notifications(final Consumer<Notification> consumer) {
            delegate.notifications(consumer);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            return delegate.put(key, value, options);
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (Exception failure) {
                throw new IllegalStateException("failed to close Oxia Route client", failure);
            }
        }
    }
}
