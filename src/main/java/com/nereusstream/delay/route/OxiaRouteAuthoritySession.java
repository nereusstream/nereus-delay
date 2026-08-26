package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.Bytes;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.text.Normalizer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Session-fenced Oxia record surface for Route publication and cache reads.
 *
 * <p>The marker is an Oxia ephemeral record bound to this client's session.
 * Every delegated operation first rereads the exact marker, value, version and
 * session metadata. A lost session therefore fences an old Route authority
 * before it can publish a head or refresh a cache.</p>
 */
public final class OxiaRouteAuthoritySession implements OxiaRouteRecordClient {
    private static final byte[] SESSION_DOMAIN = Bytes.utf8("nereus-delay-route-session\0");
    private static final SecureRandom RANDOM = new SecureRandom();

    private final OxiaRouteRecordClient delegate;
    private final java.util.function.Supplier<OxiaRouteRecordClient> notificationDelegateFactory;
    private OxiaRouteRecordClient notificationDelegate;
    private final String sessionPrefix;
    private String sessionKey;
    private byte[] challenge;
    private Version sessionVersion;
    private byte[] sessionIdentity;
    private boolean started;
    private boolean closed;
    private boolean closeCompleted;

    /** Creates a session that owns and closes the supplied Oxia client. */
    public OxiaRouteAuthoritySession(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Package-private deterministic constructor for Route authority tests. */
    OxiaRouteAuthoritySession(final OxiaRouteRecordClient delegate, final String keyPrefix) {
        this(delegate, delegate, null, keyPrefix);
    }

    /** Package-private composition constructor for deterministic session-fence tests. */
    OxiaRouteAuthoritySession(
            final OxiaRouteRecordClient delegate,
            final OxiaRouteRecordClient notificationDelegate,
            final java.util.function.Supplier<OxiaRouteRecordClient> notificationDelegateFactory,
            final String keyPrefix) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.notificationDelegate = Objects.requireNonNull(notificationDelegate, "notificationDelegate");
        this.notificationDelegateFactory = notificationDelegateFactory;
        final String prefix = canonicalKeyPrefix(keyPrefix);
        sessionPrefix = prefix + "/sessions/";
        rotateMarker();
    }

    /** Connects a session-fenced Route authority client with explicit Oxia session settings. */
    public static OxiaRouteAuthoritySession connect(
            final String serviceAddress,
            final String namespace,
            final String clientIdentifier,
            final Duration sessionTimeout,
            final String keyPrefix)
            throws OxiaException {
        Objects.requireNonNull(serviceAddress, "serviceAddress");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(clientIdentifier, "clientIdentifier");
        Objects.requireNonNull(sessionTimeout, "sessionTimeout");
        if (sessionTimeout.isZero() || sessionTimeout.isNegative()) {
            throw new IllegalArgumentException("sessionTimeout must be positive");
        }
        final String canonicalNamespace = canonicalText(namespace, "namespace");
        final String canonicalClientIdentifier = canonicalText(clientIdentifier, "clientIdentifier");
        final String canonicalPrefix = canonicalKeyPrefix(keyPrefix);
        final SyncOxiaClient sessionClient = OxiaClientBuilder.create(serviceAddress)
                .namespace(canonicalNamespace)
                .clientIdentifier(canonicalClientIdentifier)
                .sessionTimeout(sessionTimeout)
                .syncClient();
        try {
            final SyncOxiaClient notificationClient = OxiaClientBuilder.create(serviceAddress)
                    .namespace(canonicalNamespace)
                    .clientIdentifier(
                            canonicalText(canonicalClientIdentifier + "-route-watch", "notificationClientIdentifier"))
                    .sessionTimeout(sessionTimeout)
                    .syncClient();
            return new OxiaRouteAuthoritySession(
                    new SyncRecordClient(sessionClient),
                    new SyncRecordClient(notificationClient),
                    () -> createNotificationClient(
                            serviceAddress, canonicalNamespace, canonicalClientIdentifier, sessionTimeout),
                    canonicalPrefix);
        } catch (RuntimeException failure) {
            try {
                sessionClient.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static OxiaRouteRecordClient createNotificationClient(
            final String serviceAddress,
            final String namespace,
            final String clientIdentifier,
            final Duration sessionTimeout) {
        try {
            return new SyncRecordClient(OxiaClientBuilder.create(serviceAddress)
                    .namespace(namespace)
                    .clientIdentifier(canonicalText(clientIdentifier + "-route-watch", "notificationClientIdentifier"))
                    .sessionTimeout(sessionTimeout)
                    .syncClient());
        } catch (OxiaException failure) {
            throw new IllegalStateException("failed to create Oxia Route notification client", failure);
        }
    }

    @Override
    public synchronized void startSession() {
        requireNotClosed();
        if (started) {
            requireSession();
            return;
        }
        try {
            final PutResult result;
            try {
                result = delegate.put(
                        sessionKey, challenge, Set.of(PutOption.IfRecordDoesNotExist, PutOption.AsEphemeralRecord));
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException failure) {
                throw new IllegalStateException("Oxia Route session marker CAS failed", failure);
            }
            establish(result == null ? null : result.version());
        } catch (RuntimeException failure) {
            final GetResult observed = exactMarker();
            if (observed != null) {
                try {
                    establish(observed.version());
                    return;
                } catch (RuntimeException rereadMismatch) {
                    failure.addSuppressed(rereadMismatch);
                }
            }
            throw failure;
        }
    }

    /**
     * Reopens the ephemeral marker after the caller explicitly requests
     * recovery. A service restart can leave the old marker readable while the
     * underlying Oxia session is already gone, so a successful marker reread
     * is not sufficient to make this explicit recovery call a no-op.
     */
    @Override
    public synchronized void reconnectSession() {
        requireNotClosed();
        if (started) {
            try {
                requireSession();
            } catch (SessionFenceException fenced) {
                // The old marker may still be visible after the session was
                // lost. The explicit reconnect below must use a fresh marker
                // in either case.
            }
            clearSession();
        }
        rotateMarker();
        startSession();
    }

    /** Replaces the notification client after a service restart and starts a fresh offset-tracked stream. */
    @Override
    public synchronized void reconnectNotifications(final Consumer<Notification> consumer) {
        requireNotClosed();
        Objects.requireNonNull(consumer, "consumer");
        if (notificationDelegateFactory == null || notificationDelegate == delegate) {
            return;
        }
        requireSession();
        final OxiaRouteRecordClient replacement = notificationDelegateFactory.get();
        final OxiaRouteRecordClient previous = notificationDelegate;
        notificationDelegate = replacement;
        try {
            previous.close();
        } catch (RuntimeException failure) {
            try {
                replacement.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            notificationDelegate = previous;
            throw failure;
        }
        try {
            requireSession();
            notificationDelegate.notifications(consumer);
            requireSession();
        } catch (RuntimeException failure) {
            try {
                replacement.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            notificationDelegate = previous;
            throw failure;
        }
    }

    /** Returns the Oxia-session-derived identity after a successful exact check. */
    public synchronized byte[] sessionIdentity() {
        requireSession();
        return Bytes.copy(sessionIdentity);
    }

    @Override
    public synchronized GetResult get(final String key) {
        requireSession();
        try {
            final GetResult result = delegate.get(key);
            requireSession();
            return result;
        } catch (RuntimeException failure) {
            requireSession();
            throw failure;
        }
    }

    @Override
    public synchronized CloseableIterable<GetResult> rangeScan(
            final String startKeyInclusive, final String endKeyExclusive) {
        requireSession();
        try {
            final CloseableIterable<GetResult> result = delegate.rangeScan(startKeyInclusive, endKeyExclusive);
            requireSession();
            return new SessionBoundIterable<>(result, this::requireSession);
        } catch (RuntimeException failure) {
            requireSession();
            throw failure;
        }
    }

    @Override
    public synchronized void notifications(final Consumer<Notification> consumer) {
        requireSession();
        try {
            notificationDelegate.notifications(consumer);
            requireSession();
        } catch (RuntimeException failure) {
            requireSession();
            throw failure;
        }
    }

    @Override
    public synchronized PutResult put(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        requireSession();
        try {
            final PutResult result = delegate.put(key, value, options);
            requireSession();
            return result;
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException expectedCasRace) {
            requireSession();
            throw expectedCasRace;
        } catch (RuntimeException failure) {
            requireSession();
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (closeCompleted) {
            return;
        }
        closed = true;
        started = false;
        sessionVersion = null;
        sessionIdentity = null;
        Throwable closeFailure = null;
        try {
            delegate.close();
        } catch (RuntimeException | Error failure) {
            // The watch client is an independent Oxia session. Its close
            // must still be attempted when the authority client reports a
            // teardown failure, otherwise the watch session can be stranded.
            closeFailure = appendCloseFailure(closeFailure, failure);
        }
        if (notificationDelegate != delegate) {
            try {
                notificationDelegate.close();
            } catch (RuntimeException | Error failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (closeFailure != null) {
            throwUnchecked(closeFailure);
        }
        closeCompleted = true;
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

    private synchronized void requireSession() {
        requireNotClosed();
        if (!started) {
            throw new IllegalStateException("Oxia Route authority session is not started");
        }
        final GetResult observed = exactMarker();
        if (observed == null) {
            throw new SessionFenceException("Oxia Route authority session marker is absent");
        }
        if (observed.version().versionId() != sessionVersion.versionId()
                || !sameSessionMetadata(observed.version(), sessionVersion)) {
            throw new SessionFenceException("Oxia Route authority session identity changed");
        }
    }

    private void clearSession() {
        started = false;
        sessionVersion = null;
        sessionIdentity = null;
    }

    private void rotateMarker() {
        challenge = new byte[32];
        RANDOM.nextBytes(challenge);
        sessionKey = sessionPrefix + Bytes.hex(challenge);
    }

    private void establish(final Version version) {
        if (version == null) {
            throw new IllegalStateException("Oxia Route session marker has no version");
        }
        if (version.sessionId().isEmpty() || version.clientIdentifier().isEmpty()) {
            throw new IllegalStateException("Oxia Route session marker is not ephemeral");
        }
        sessionVersion = version;
        sessionIdentity = deriveSessionIdentity(version);
        started = true;
        final GetResult observed = exactMarker();
        if (observed == null || observed.version().versionId() != version.versionId()) {
            started = false;
            sessionVersion = null;
            sessionIdentity = null;
            throw new IllegalStateException("Oxia Route session marker disappeared during start");
        }
    }

    private GetResult exactMarker() {
        final GetResult result = delegate.get(sessionKey);
        if (result == null) {
            return null;
        }
        if (!sessionKey.equals(result.key())
                || result.value() == null
                || result.version() == null
                || !Arrays.equals(challenge, result.value())) {
            throw new IllegalStateException("Oxia Route session marker is not exact");
        }
        return result;
    }

    private static boolean sameSessionMetadata(final Version left, final Version right) {
        return left.sessionId().equals(right.sessionId())
                && left.clientIdentifier().equals(right.clientIdentifier());
    }

    private static byte[] deriveSessionIdentity(final Version version) {
        final long sessionId = version.sessionId()
                .orElseThrow(() -> new IllegalArgumentException("Oxia Route session has no session id"));
        final String clientIdentifier = canonicalText(
                version.clientIdentifier()
                        .orElseThrow(() -> new IllegalArgumentException("Oxia Route session has no client identifier")),
                "clientIdentifier");
        if (sessionId < 0) {
            throw new IllegalArgumentException("Oxia Route session id must be non-negative");
        }
        return Bytes.sha256(
                SESSION_DOMAIN, Bytes.u64be(sessionId), Bytes.lp32(clientIdentifier.getBytes(StandardCharsets.UTF_8)));
    }

    private void requireNotClosed() {
        if (closed) {
            throw new IllegalStateException("Oxia Route authority session is closed");
        }
    }

    private static String canonicalKeyPrefix(final String value) {
        final String canonical = canonicalText(value, "keyPrefix");
        if (canonical.endsWith("/") || canonical.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("keyPrefix must not end with '/'");
        }
        return canonical;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (!value.equals(new String(encoded, StandardCharsets.UTF_8))
                || value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static final class SessionFenceException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private SessionFenceException(final String message) {
            super(message);
        }
    }

    /** Keeps a range scan fenced while its lazy iterator consumes Oxia data. */
    private static final class SessionBoundIterable<T> implements CloseableIterable<T> {
        private final CloseableIterable<T> delegate;
        private final Runnable sessionCheck;

        private SessionBoundIterable(final CloseableIterable<T> delegate, final Runnable sessionCheck) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.sessionCheck = Objects.requireNonNull(sessionCheck, "sessionCheck");
        }

        @Override
        public Iterator<T> iterator() {
            sessionCheck.run();
            try {
                final Iterator<T> iterator = delegate.iterator();
                sessionCheck.run();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return around(iterator::hasNext);
                    }

                    @Override
                    public T next() {
                        return around(iterator::next);
                    }

                    @Override
                    public void remove() {
                        sessionCheck.run();
                        try {
                            iterator.remove();
                            sessionCheck.run();
                        } catch (RuntimeException failure) {
                            sessionCheck.run();
                            throw failure;
                        }
                    }
                };
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public void close() {
            delegate.close();
        }

        private <R> R around(final Supplier<R> action) {
            sessionCheck.run();
            try {
                final R result = action.get();
                sessionCheck.run();
                return result;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
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
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            return delegate.put(key, value, options);
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (Exception failure) {
                throw new IllegalStateException("failed to close Oxia Route authority session", failure);
            }
        }
    }
}
