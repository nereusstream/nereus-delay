package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.TrustedClock;
import java.security.PublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Deterministic local Route authority used by composition tests and embedded
 * deployments. It models the immutable signed snapshot and contiguous watch
 * revision rules; an Oxia-backed refresher must provide the same contract.
 */
public final class InMemorySignedRouteSnapshotProvider implements RouteSnapshotProvider, RouteSnapshotRefresher {
    private final PublicKey verificationKey;
    private final TrustedClock trustedClock;
    private final Map<RouteKey, RouteSnapshotV1> active = new HashMap<>();
    private final Map<RouteIncarnation, RouteSnapshotV1> history = new HashMap<>();
    private final Map<RouteIncarnation, RouteKey> activeKeyByIncarnation = new HashMap<>();
    private long publishedRevision;
    private RouteCacheHealth health = RouteCacheHealth.UNAVAILABLE;

    public InMemorySignedRouteSnapshotProvider(final PublicKey verificationKey, final TrustedClock trustedClock) {
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    /**
     * Applies one authenticated watch item. Revisions must be contiguous; a
     * gap freezes both new-route and historical-route reads until the caller
     * constructs a fresh provider from a complete snapshot stream.
     */
    public synchronized void accept(
            final long revision,
            final long previousRevision,
            final RouteSelectionHint route,
            final RouteSnapshotV1 snapshot) {
        requireOpen();
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(snapshot, "snapshot");
        if (revision <= 0
                || previousRevision < 0
                || revision != publishedRevision + 1
                || previousRevision != publishedRevision) {
            health = RouteCacheHealth.WATCH_GAP;
            throw new IllegalArgumentException("Route watch revision is not contiguous");
        }
        final RouteSnapshotV1 verified;
        try {
            verified = RouteSnapshotV1.decode(snapshot.canonicalBytes(), verificationKey);
        } catch (RuntimeException invalidSignatureOrBytes) {
            health = RouteCacheHealth.SIGNATURE_INVALID;
            throw new IllegalArgumentException("Route snapshot signature/digest is invalid", invalidSignatureOrBytes);
        }
        final RouteSnapshotV1 previous = history.get(verified.routeIncarnation());
        if (previous != null) {
            try {
                RouteSnapshotCompatibilityV1.requireCompatibleSuccessor(previous, verified);
            } catch (IllegalArgumentException incompatible) {
                health = RouteCacheHealth.QUARANTINED;
                throw new IllegalArgumentException("Route incarnation changed immutable fields", incompatible);
            }
            final RouteKey previousKey = activeKeyByIncarnation.get(verified.routeIncarnation());
            final RouteKey nextKey = new RouteKey(route.adapterKind(), route.routeAliasUtf8Nfc());
            if (previousKey != null && !previousKey.equals(nextKey)) {
                active.remove(previousKey);
            }
        }
        final RouteKey nextKey = new RouteKey(route.adapterKind(), route.routeAliasUtf8Nfc());
        active.put(nextKey, verified);
        activeKeyByIncarnation.put(verified.routeIncarnation(), nextKey);
        history.put(verified.routeIncarnation(), verified);
        publishedRevision = revision;
        health = RouteCacheHealth.HEALTHY;
    }

    @Override
    public synchronized RouteSnapshotV1 activeForNewSchedule(
            final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
        requireHealthy();
        final RouteSnapshotV1 snapshot = active.get(new RouteKey(hint.adapterKind(), hint.routeAliasUtf8Nfc()));
        if (snapshot == null) {
            throw new IllegalArgumentException("Route alias is unavailable");
        }
        snapshot.requireUsableForNewSchedule(
                context.authenticatedTenantScopeHash(), context.tenantRoutingScope(), trustedClock.nowEpochMs());
        return snapshot;
    }

    @Override
    public synchronized RouteSnapshotV1 exact(
            final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
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
    public CompletionStage<Void> start() {
        return CompletableFuture.completedFuture(null);
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

    private record RouteKey(AdapterKindV1 adapterKind, String routeAlias) {
        private RouteKey(final AdapterKindV1 adapterKind, final byte[] routeAlias) {
            this(adapterKind, Base64.getUrlEncoder().withoutPadding().encodeToString(Bytes.copy(routeAlias)));
        }
    }
}
