package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.TrustedClock;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.NotificationSubscription;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final Executor notificationExecutor;
    private final ExecutorService ownedNotificationExecutor;
    private final Map<RouteKey, RouteSnapshot> active = new HashMap<>();
    private final Map<RouteIncarnation, RouteSnapshot> history = new HashMap<>();
    private final Map<RouteIncarnation, RouteKey> activeKeyByIncarnation = new HashMap<>();
    private long publishedRevision;
    private volatile RouteCacheHealth health = RouteCacheHealth.UNAVAILABLE;
    private boolean started;
    private boolean closeCompleted;
    private final Object closeLock = new Object();
    private final Object watchOpeningLock = new Object();
    private java.util.concurrent.ThreadPoolExecutor sharedNotifications;
    private RefreshLimits refreshLimits;
    private java.util.function.LongSupplier refreshClock;
    private volatile boolean refreshStopping;
    private com.nereusstream.delay.ownership.OwnedCheckpointIo<Boolean> notificationIo;
    private boolean notificationScheduling;
    private WatchGeneration watch;
    private NotificationSubscription retiringWatch;
    private long watchGeneration;

    private static final class RefreshDeadline {
        private final long started = System.nanoTime();
        private long budget;

        private void use(java.time.Duration timeout) {
            final long nanos = Objects.requireNonNull(timeout).toNanos();
            if (nanos <= 0) {
                throw new IllegalArgumentException("Route watch requires its original finite request timeout");
            }
            budget = budget == 0 ? nanos : Math.min(budget, nanos);
            remaining();
        }

        private long remaining() {
            final long elapsed = System.nanoTime() - started;
            if (budget == 0 || elapsed < 0 || elapsed >= budget) {
                throw new RouteWatchUnavailable("Route watch refresh exceeded its original request deadline", null);
            }
            return budget - elapsed;
        }

        private void await(CompletableFuture<Void> observation) {
            try {
                observation.get(remaining(), java.util.concurrent.TimeUnit.NANOSECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RouteWatchUnavailable("Route watch refresh interrupted", interrupted);
            } catch (java.util.concurrent.ExecutionException failure) {
                throw new java.util.concurrent.CompletionException(failure.getCause());
            } catch (java.util.concurrent.TimeoutException expired) {
                throw new RouteWatchUnavailable("Route watch refresh exceeded its original request deadline", expired);
            }
        }
    }

    private static final class WatchGeneration {
        private final long generation;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private NotificationSubscription subscription;
        private boolean initializing = true;
        private boolean admitted;
        private boolean refreshing;
        private boolean dirty;

        private WatchGeneration(long generation) {
            this.generation = generation;
        }
    }

    public record RefreshLimits(int records, long bytes, long elapsedNanos) {
        public RefreshLimits {
            if (records < 1
                    || records == Integer.MAX_VALUE
                    || bytes < 1
                    || bytes == Long.MAX_VALUE
                    || elapsedNanos < 1
                    || elapsedNanos == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Route refresh requires finite records/bytes/time");
            }
        }
    }

    /** Configured root borrows its existing finite pool; notification work remains on the original tracked task. */
    public OxiaSignedRouteSnapshotProvider(
            OxiaRouteAuthoritySession session,
            String prefix,
            PublicKey key,
            TrustedClock clock,
            java.util.concurrent.ThreadPoolExecutor pool,
            RefreshLimits limits,
            java.util.function.LongSupplier nanos) {
        this(session, prefix, key, clock, false);
        if (pool.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) pool.getQueue().size() + pool.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || pool.getRejectedExecutionHandler().getClass()
                        != java.util.concurrent.ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("Route refresh requires a finite pool and AbortPolicy");
        }
        sharedNotifications = pool;
        refreshLimits = Objects.requireNonNull(limits);
        refreshClock = Objects.requireNonNull(nanos);
    }

    /** Stops future refreshes without closing the shared authority that Host drain still needs. */
    public void stopNotificationIo() {
        synchronized (this) {
            refreshStopping = true;
            if (notificationIo != null) {
                notificationIo.requestStop();
            }
        }
    }

    public boolean notificationIoExited() {
        synchronized (this) {
            return !notificationScheduling
                    && (watch == null || !watch.initializing)
                    && (notificationIo == null || notificationIo.ioExited());
        }
    }

    /** Creates a provider over an owned/configured SyncOxiaClient. */
    public OxiaSignedRouteSnapshotProvider(
            final SyncOxiaClient client,
            final String keyPrefix,
            final PublicKey verificationKey,
            final TrustedClock trustedClock) {
        this(new SyncRecordClient(client), keyPrefix, verificationKey, trustedClock, true);
    }

    /** Creates a provider whose Oxia reads are fenced by the supplied ephemeral session. */
    public OxiaSignedRouteSnapshotProvider(
            final OxiaRouteAuthoritySession session,
            final String keyPrefix,
            final PublicKey verificationKey,
            final TrustedClock trustedClock) {
        this(
                (OxiaRouteRecordClient) Objects.requireNonNull(session, "session"),
                keyPrefix,
                verificationKey,
                trustedClock,
                true);
    }

    OxiaSignedRouteSnapshotProvider(
            final OxiaRouteRecordClient client,
            final String keyPrefix,
            final PublicKey verificationKey,
            final TrustedClock trustedClock) {
        this(client, keyPrefix, verificationKey, trustedClock, false);
    }

    private OxiaSignedRouteSnapshotProvider(
            final OxiaRouteRecordClient client,
            final String keyPrefix,
            final PublicKey verificationKey,
            final TrustedClock trustedClock,
            final boolean asynchronousNotifications) {
        this.client = Objects.requireNonNull(client, "client");
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        final String prefix = canonicalKeyPrefix(keyPrefix);
        this.eventPrefix = prefix + EVENTS_SUFFIX;
        this.headKey = prefix + HEAD_SUFFIX;
        this.eventScanEnd = eventPrefix + "\uffff";
        if (asynchronousNotifications) {
            final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
                final Thread thread = new Thread(runnable, "nereus-delay-route-refresh");
                thread.setDaemon(true);
                return thread;
            });
            notificationExecutor = executor;
            ownedNotificationExecutor = executor;
        } else {
            notificationExecutor = Runnable::run;
            ownedNotificationExecutor = null;
        }
    }

    @Override
    public CompletionStage<Void> start() {
        synchronized (this) {
            requireOpen();
            if (watch != null && watch.initializing) {
                return watch.completion.copy();
            }
            if (started && currentWatchHealthy()) {
                return CompletableFuture.completedFuture(null);
            }
        }
        return openWatch(false);
    }

    /** Rebuilds from the authoritative head after a gap or reconnect. */
    public CompletionStage<Void> refresh() {
        return openWatch(true);
    }

    private CompletionStage<Void> openWatch(boolean reconnect) {
        synchronized (watchOpeningLock) {
            return openWatchSerially(reconnect);
        }
    }

    private CompletionStage<Void> openWatchSerially(boolean reconnect) {
        final RefreshDeadline deadline = new RefreshDeadline();
        final WatchGeneration selected;
        final WatchGeneration previous;
        final boolean reopen;
        synchronized (this) {
            requireOpen();
            previous = watch;
            if (previous != null && previous.subscription != null) {
                if (retiringWatch != null && retiringWatch != previous.subscription) {
                    throw new IllegalStateException("original Route watch cleanup remains pending");
                }
                retiringWatch = previous.subscription;
            }
            selected = new WatchGeneration(++watchGeneration);
            watch = selected;
            reopen = reconnect || started;
            started = true;
            health = RouteCacheHealth.UNAVAILABLE;
        }
        try {
            final NotificationSubscription retiring;
            synchronized (this) {
                retiring = retiringWatch;
            }
            if (retiring != null) {
                deadline.use(retiring.requestTimeout());
                retiring.close();
                // Observation is bounded by the original SDK request timeout; its native fact stays owned on timeout.
                deadline.await(retiring.termination());
                synchronized (this) {
                    if (retiringWatch == retiring) {
                        retiringWatch = null;
                    }
                }
            }
            if (reopen) {
                client.reconnectSession();
            } else {
                client.startSession();
            }
            final NotificationSubscription opened =
                    client.notificationsReady(notification -> onNotification(selected, notification));
            synchronized (this) {
                selected.subscription = opened;
            }
            deadline.use(opened.requestTimeout());
            opened.invalidated().whenComplete((ignored, failure) -> {
                synchronized (this) {
                    if (watch == selected && health != RouteCacheHealth.CLOSED) {
                        health = RouteCacheHealth.WATCH_GAP;
                    }
                }
            });
            deadline.await(opened.ready());
            refreshFromAuthority(selected, deadline);
            selected.completion.complete(null);
        } catch (RuntimeException failure) {
            synchronized (this) {
                if (watch == selected && health != RouteCacheHealth.CLOSED) {
                    health = classify(failure);
                }
            }
            selected.completion.completeExceptionally(failure);
        } finally {
            synchronized (this) {
                selected.initializing = false;
            }
            submitNotificationIo(selected);
        }
        return selected.completion.copy();
    }

    @Override
    public synchronized RouteSnapshot activeForNewSchedule(
            final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
        requireHealthy();
        final RouteSnapshot snapshot = active.get(new RouteKey(hint.adapterKind(), hint.routeAliasUtf8Nfc()));
        if (snapshot == null) {
            throw new IllegalArgumentException("Route alias is unavailable");
        }
        snapshot.requireUsableForNewSchedule(
                context.authenticatedTenantScopeHash(), context.tenantRoutingScope(), trustedClock.nowEpochMs());
        return snapshot;
    }

    @Override
    public synchronized RouteSnapshot exact(
            final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
        requireHealthy();
        final RouteSnapshot snapshot = history.get(Objects.requireNonNull(incarnation, "incarnation"));
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
        currentWatchHealthy();
        return health;
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            final RefreshDeadline deadline = new RefreshDeadline();
            final NotificationSubscription current;
            final NotificationSubscription retiring;
            synchronized (this) {
                if (closeCompleted) {
                    return;
                }
                if (sharedNotifications != null) {
                    stopNotificationIo();
                }
                active.clear();
                history.clear();
                activeKeyByIncarnation.clear();
                health = RouteCacheHealth.CLOSED;
                current = watch == null ? null : watch.subscription;
                retiring = retiringWatch;
            }
            // SDK termination waits for notification callbacks, which must observe CLOSED under the cache monitor.
            Throwable closeFailure = null;
            if (current != null) {
                try {
                    deadline.use(current.requestTimeout());
                    current.close();
                    deadline.await(current.termination());
                } catch (RuntimeException | Error failure) {
                    closeFailure = appendCloseFailure(closeFailure, failure);
                }
            }
            if (retiring != null && retiring != current) {
                try {
                    deadline.use(retiring.requestTimeout());
                    retiring.close();
                    deadline.await(retiring.termination());
                } catch (RuntimeException | Error failure) {
                    closeFailure = appendCloseFailure(closeFailure, failure);
                }
            }
            if (sharedNotifications != null && !notificationIoExited()) {
                closeFailure = appendCloseFailure(
                        closeFailure, new IllegalStateException("original Route refresh I/O remains pending"));
            }
            if (ownedNotificationExecutor != null) {
                try {
                    ownedNotificationExecutor.shutdownNow();
                } catch (RuntimeException | Error failure) {
                    closeFailure = appendCloseFailure(closeFailure, failure);
                }
            }
            if (sharedNotifications == null) {
                try {
                    client.close();
                } catch (RuntimeException | Error failure) {
                    closeFailure = appendCloseFailure(closeFailure, failure);
                }
            }
            if (closeFailure != null) {
                throwUnchecked(closeFailure);
            }
            synchronized (this) {
                closeCompleted = true;
            }
        }
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    private void refreshFromAuthority(WatchGeneration selected, RefreshDeadline deadline) {
        synchronized (this) {
            requireSelected(selected);
            selected.dirty = false;
        }
        selected.subscription.requireCurrent();
        final CacheSnapshot snapshot = readFromAuthority(deadline);
        client.requireNotificationCurrent(selected.subscription);
        deadline.remaining();
        synchronized (this) {
            requireSelected(selected);
            selected.subscription.requireCurrent();
            replaceCache(
                    snapshot.active(),
                    snapshot.history(),
                    snapshot.activeKeys(),
                    snapshot.revision(),
                    snapshot.health());
            selected.admitted = true;
        }
    }

    private record CacheSnapshot(
            Map<RouteKey, RouteSnapshot> active,
            Map<RouteIncarnation, RouteSnapshot> history,
            Map<RouteIncarnation, RouteKey> activeKeys,
            long revision,
            RouteCacheHealth health) {}

    private CacheSnapshot readFromAuthority(RefreshDeadline deadline) {
        final var budget = refreshLimits == null
                ? null
                : new com.nereusstream.delay.store.BoundedReadBudget(
                        refreshLimits.records(), refreshLimits.bytes(), refreshLimits.elapsedNanos(), refreshClock);
        requireRefreshRead(budget, deadline);
        final HeadEntry head = readHead();
        if (budget != null && head != null) {
            requireRefreshCharge(budget, deadline, headKey, head.head().canonicalBytes().length);
            if (head.head().publishedRevision() >= refreshLimits.records()) {
                throw new IllegalStateException("Route history exceeds its configured record allowance");
            }
        }
        final Map<Long, OxiaRouteSnapshotRecord> events = new TreeMap<>();
        if (head != null) {
            if (budget != null) {
                for (long revision = 1; revision <= head.head().publishedRevision(); revision++) {
                    requireRefreshRead(budget, deadline);
                    final String key = eventKey(revision);
                    final GetResult result = client.get(key);
                    if (result == null
                            || !key.equals(result.key())
                            || result.value() == null
                            || result.version() == null) {
                        throw new RouteGapException(
                                "Oxia Route head references a missing or invalid immutable revision");
                    }
                    requireRefreshCharge(budget, deadline, key, result.value().length);
                    final var event = OxiaRouteSnapshotRecord.decode(result.value(), verificationKey);
                    if (event.revision() != revision) {
                        throw new RouteGapException("Oxia Route revision/key mismatch");
                    }
                    events.put(revision, event);
                }
            } else {
                final CloseableIterable<GetResult> scan = client.rangeScan(eventPrefix, eventScanEnd);
                try {
                    final var iterator = scan.iterator();
                    while (true) {
                        requireRefreshRead(budget, deadline);
                        if (!iterator.hasNext()) {
                            break;
                        }
                        final GetResult result = iterator.next();
                        if (result == null
                                || result.key() == null
                                || result.value() == null
                                || result.version() == null
                                || !result.key().startsWith(eventPrefix)) {
                            throw new IllegalStateException("Oxia Route scan returned an invalid record");
                        }
                        requireRefreshCharge(budget, deadline, result.key(), result.value().length);
                        final OxiaRouteSnapshotRecord event =
                                OxiaRouteSnapshotRecord.decode(result.value(), verificationKey);
                        if (!eventKey(event.revision()).equals(result.key())) {
                            throw new IllegalStateException("Oxia Route event key is not bound to its revision");
                        }
                        if (event.revision() <= head.head().publishedRevision()) {
                            final OxiaRouteSnapshotRecord previous = events.put(event.revision(), event);
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
        }

        final Map<RouteKey, RouteSnapshot> nextActive = new HashMap<>();
        final Map<RouteIncarnation, RouteSnapshot> nextHistory = new HashMap<>();
        final Map<RouteIncarnation, RouteKey> nextActiveKeys = new HashMap<>();
        if (head == null) {
            return new CacheSnapshot(nextActive, nextHistory, nextActiveKeys, 0, RouteCacheHealth.UNAVAILABLE);
        }
        for (long revision = 1; revision <= head.head().publishedRevision(); revision++) {
            if (budget != null && !budget.beforeTimedWork()) {
                throw new IllegalStateException("Route refresh decode deadline exceeded");
            }
            final OxiaRouteSnapshotRecord event = events.get(revision);
            if (event == null || event.previousRevision() != revision - 1) {
                throw new RouteGapException("Oxia Route stream has a missing or non-contiguous event");
            }
            if (revision == head.head().publishedRevision()
                    && !Bytes.constantTimeEquals(head.head().eventDigest(), event.recordDigest())) {
                throw new RouteGapException("Oxia Route head does not match its final event");
            }
            apply(nextActive, nextHistory, nextActiveKeys, event);
        }
        if (refreshStopping || budget != null && !budget.beforeTimedWork()) {
            throw new IllegalStateException("Route refresh stopped or exceeded final deadline");
        }
        return new CacheSnapshot(
                nextActive, nextHistory, nextActiveKeys, head.head().publishedRevision(), RouteCacheHealth.HEALTHY);
    }

    private void requireRefreshRead(com.nereusstream.delay.store.BoundedReadBudget budget, RefreshDeadline deadline) {
        deadline.remaining();
        if (refreshStopping || budget != null && !budget.beforeRead()) {
            throw new IllegalStateException("Route refresh is stopped or exhausted");
        }
    }

    private void requireRefreshCharge(
            com.nereusstream.delay.store.BoundedReadBudget budget, RefreshDeadline deadline, String key, int bytes) {
        deadline.remaining();
        if (budget != null && !budget.tryCharge(key.getBytes(StandardCharsets.UTF_8).length, bytes)) {
            throw new IllegalStateException("Route refresh byte budget exhausted");
        }
    }

    private void apply(
            final Map<RouteKey, RouteSnapshot> nextActive,
            final Map<RouteIncarnation, RouteSnapshot> nextHistory,
            final Map<RouteIncarnation, RouteKey> nextActiveKeys,
            final OxiaRouteSnapshotRecord event) {
        final RouteSnapshot previous = nextHistory.get(event.snapshot().routeIncarnation());
        if (previous != null) {
            try {
                RouteSnapshotCompatibility.requireCompatibleSuccessor(previous, event.snapshot());
            } catch (IllegalArgumentException incompatible) {
                throw new RouteQuarantineException(incompatible.getMessage(), incompatible);
            }
            final RouteKey oldKey = nextActiveKeys.get(event.snapshot().routeIncarnation());
            final RouteKey newKey =
                    new RouteKey(event.route().adapterKind(), event.route().routeAliasUtf8Nfc());
            if (oldKey != null && !oldKey.equals(newKey)) {
                nextActive.remove(oldKey);
            }
        }
        final RouteKey key =
                new RouteKey(event.route().adapterKind(), event.route().routeAliasUtf8Nfc());
        nextActive.put(key, event.snapshot());
        nextActiveKeys.put(event.snapshot().routeIncarnation(), key);
        nextHistory.put(event.snapshot().routeIncarnation(), event.snapshot());
    }

    private void replaceCache(
            final Map<RouteKey, RouteSnapshot> nextActive,
            final Map<RouteIncarnation, RouteSnapshot> nextHistory,
            final Map<RouteIncarnation, RouteKey> nextActiveKeys,
            final long revision,
            final RouteCacheHealth nextHealth) {
        active.clear();
        active.putAll(nextActive);
        history.clear();
        history.putAll(nextHistory);
        activeKeyByIncarnation.clear();
        activeKeyByIncarnation.putAll(nextActiveKeys);
        publishedRevision = revision;
        health = nextHealth;
    }

    private void onNotification(WatchGeneration selected, final Notification notification) {
        synchronized (this) {
            if (watch != selected
                    || refreshStopping
                    || health == RouteCacheHealth.CLOSED
                    || notification == null
                    || notification.key() == null
                    || (!headKey.equals(notification.key())
                            && !notification.key().startsWith(eventPrefix))) {
                return;
            }
            selected.dirty = true;
        }
        submitNotificationIo(selected);
    }

    private void submitNotificationIo(WatchGeneration selected) {
        synchronized (this) {
            if (watch != selected
                    || refreshStopping
                    || health == RouteCacheHealth.CLOSED
                    || !selected.dirty
                    || selected.initializing
                    || !selected.admitted
                    || selected.refreshing
                    || notificationScheduling
                    || sharedNotifications != null && notificationIo != null && !notificationIo.ioExited()) {
                return;
            }
            selected.refreshing = true;
            notificationScheduling = true;
        }
        try {
            if (sharedNotifications != null) {
                final var original = com.nereusstream.delay.ownership.OwnedCheckpointIo.submit(
                        guard -> {
                            refreshFromAuthority(selected, notificationDeadline(selected));
                            return true;
                        },
                        () -> {
                            synchronized (this) {
                                requireSelected(selected);
                            }
                        },
                        sharedNotifications);
                synchronized (this) {
                    notificationIo = original;
                    notificationScheduling = false;
                    if (refreshStopping) {
                        original.requestStop();
                    }
                }
                original.receipt().whenComplete((value, error) -> notificationRefreshExited(selected, error));
            } else {
                notificationExecutor.execute(() -> {
                    Throwable failure = null;
                    try {
                        refreshFromAuthority(selected, notificationDeadline(selected));
                    } catch (RuntimeException | Error stopped) {
                        failure = stopped;
                    } finally {
                        notificationRefreshExited(selected, failure);
                    }
                });
                synchronized (this) {
                    notificationScheduling = false;
                }
                submitNotificationIo(selected);
            }
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            // Keep the dirty bit. A later notification or explicit refresh retries the original authority read.
            synchronized (this) {
                selected.refreshing = false;
                notificationScheduling = false;
                if (watch == selected && health != RouteCacheHealth.CLOSED) {
                    health = RouteCacheHealth.UNAVAILABLE;
                }
            }
        }
    }

    private void notificationRefreshExited(WatchGeneration selected, Throwable failure) {
        final WatchGeneration current;
        synchronized (this) {
            selected.refreshing = false;
            if (watch == selected && failure != null && health != RouteCacheHealth.CLOSED) {
                final Throwable cause =
                        failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
                                ? failure.getCause()
                                : failure;
                health = cause instanceof RuntimeException runtime ? classify(runtime) : RouteCacheHealth.UNAVAILABLE;
            }
            current = watch;
        }
        if (current != null) {
            submitNotificationIo(current);
        }
    }

    private RefreshDeadline notificationDeadline(WatchGeneration selected) {
        final var deadline = new RefreshDeadline();
        deadline.use(selected.subscription.requestTimeout());
        return deadline;
    }

    private void requireSelected(WatchGeneration selected) {
        requireOpen();
        if (watch != selected) {
            throw new IllegalStateException("Route refresh belongs to old watch generation " + selected.generation);
        }
    }

    private boolean currentWatchHealthy() {
        if (health != RouteCacheHealth.HEALTHY) {
            return false;
        }
        try {
            if (watch == null || watch.subscription == null) {
                throw new IllegalStateException("Route watch is not ready");
            }
            watch.subscription.requireCurrent();
            return true;
        } catch (RuntimeException unavailable) {
            health = RouteCacheHealth.WATCH_GAP;
            return false;
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
        return new HeadEntry(
                OxiaRouteSnapshotHead.decode(result.value()), result.version().versionId());
    }

    private void requireHealthy() {
        if (refreshStopping || !currentWatchHealthy()) {
            throw new IllegalStateException("Route cache is not healthy: " + health);
        }
    }

    private void requireOpen() {
        if (refreshStopping || health == RouteCacheHealth.CLOSED) {
            throw new IllegalStateException("Route cache is closed");
        }
    }

    private static RouteCacheHealth classify(final RuntimeException failure) {
        if (failure instanceof RouteWatchUnavailable
                || failure.getCause() instanceof java.util.concurrent.TimeoutException) {
            return RouteCacheHealth.UNAVAILABLE;
        }
        if (failure instanceof RouteQuarantineException) {
            return RouteCacheHealth.QUARANTINED;
        }
        if (failure.getCause() instanceof RouteQuarantineException) {
            return RouteCacheHealth.QUARANTINED;
        }
        final String message = failure.getMessage();
        return message != null && message.toLowerCase(java.util.Locale.ROOT).contains("signature")
                ? RouteCacheHealth.SIGNATURE_INVALID
                : RouteCacheHealth.WATCH_GAP;
    }

    private String eventKey(final long revision) {
        return eventPrefix + String.format(java.util.Locale.ROOT, "%020d", revision);
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be a nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    private record RouteKey(AdapterKind adapterKind, String routeAlias) {
        private RouteKey(final AdapterKind adapterKind, final byte[] routeAlias) {
            this(adapterKind, Base64.getUrlEncoder().withoutPadding().encodeToString(Bytes.copy(routeAlias)));
        }
    }

    private record HeadEntry(OxiaRouteSnapshotHead head, long versionId) {}

    private static final class RouteWatchUnavailable extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private RouteWatchUnavailable(String message, Throwable cause) {
            super(message, cause);
        }
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
        public CloseableIterable<GetResult> rangeScan(final String startKeyInclusive, final String endKeyExclusive) {
            return delegate.rangeScan(startKeyInclusive, endKeyExclusive);
        }

        @Override
        public void notifications(final Consumer<Notification> consumer) {
            delegate.notifications(consumer);
        }

        @Override
        public NotificationSubscription notificationsReady(final Consumer<Notification> consumer) {
            return delegate.notificationsReady(consumer);
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
