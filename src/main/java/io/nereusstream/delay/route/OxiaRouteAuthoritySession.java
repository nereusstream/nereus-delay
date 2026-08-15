package io.nereusstream.delay.route;

import io.nereusstream.delay.protocol.Bytes;
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
import java.text.Normalizer;
import java.time.Duration;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Session-fenced Oxia record surface for Route publication and cache reads.
 *
 * <p>The marker is an Oxia ephemeral record bound to this client's session.
 * Every delegated operation first rereads the exact marker, value, version and
 * session metadata. A lost session therefore fences an old Route authority
 * before it can publish a head or refresh a cache.</p>
 */
public final class OxiaRouteAuthoritySession implements OxiaRouteRecordClient {
    private static final byte[] SESSION_DOMAIN = Bytes.utf8("nereus-delay-route-session-v1\0");
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

    /** Creates a session that owns and closes the supplied Oxia client. */
    public OxiaRouteAuthoritySession(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Package-private deterministic constructor for Route authority tests. */
    OxiaRouteAuthoritySession(final OxiaRouteRecordClient delegate, final String keyPrefix) {
        this(delegate, delegate, null, keyPrefix);
    }

    private OxiaRouteAuthoritySession(final OxiaRouteRecordClient delegate,
                                      final OxiaRouteRecordClient notificationDelegate,
                                      final java.util.function.Supplier<OxiaRouteRecordClient>
                                              notificationDelegateFactory,
                                      final String keyPrefix) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.notificationDelegate = Objects.requireNonNull(notificationDelegate, "notificationDelegate");
        this.notificationDelegateFactory = notificationDelegateFactory;
        final String prefix = canonicalKeyPrefix(keyPrefix);
        sessionPrefix = prefix + "/sessions/";
        rotateMarker();
    }

    /** Connects a session-fenced Route authority client with explicit Oxia session settings. */
    public static OxiaRouteAuthoritySession connect(final String serviceAddress, final String namespace,
                                                    final String clientIdentifier, final Duration sessionTimeout,
                                                    final String keyPrefix) throws OxiaException {
        Objects.requireNonNull(serviceAddress, "serviceAddress");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(clientIdentifier, "clientIdentifier");
        Objects.requireNonNull(sessionTimeout, "sessionTimeout");
        if (sessionTimeout.isZero() || sessionTimeout.isNegative()) {
            throw new IllegalArgumentException("sessionTimeout must be positive");
        }
        final String canonicalNamespace = canonicalText(namespace, "namespace");
        final String canonicalClientIdentifier = canonicalText(clientIdentifier, "clientIdentifier");
        final SyncOxiaClient sessionClient = OxiaClientBuilder.create(serviceAddress)
                .namespace(canonicalNamespace)
                .clientIdentifier(canonicalClientIdentifier)
                .sessionTimeout(sessionTimeout)
                .syncClient();
        try {
            final SyncOxiaClient notificationClient = OxiaClientBuilder.create(serviceAddress)
                    .namespace(canonicalNamespace)
                    .clientIdentifier(canonicalText(canonicalClientIdentifier + "-route-watch",
                            "notificationClientIdentifier"))
                    .sessionTimeout(sessionTimeout)
                    .syncClient();
            return new OxiaRouteAuthoritySession(new SyncRecordClient(sessionClient),
                    new SyncRecordClient(notificationClient),
                    () -> createNotificationClient(serviceAddress, canonicalNamespace,
                            canonicalClientIdentifier, sessionTimeout),
                    keyPrefix);
        } catch (RuntimeException failure) {
            try {
                sessionClient.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static OxiaRouteRecordClient createNotificationClient(final String serviceAddress,
                                                                   final String namespace,
                                                                   final String clientIdentifier,
                                                                   final Duration sessionTimeout) {
        try {
            return new SyncRecordClient(OxiaClientBuilder.create(serviceAddress)
                    .namespace(namespace)
                    .clientIdentifier(canonicalText(clientIdentifier + "-route-watch",
                            "notificationClientIdentifier"))
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
                result = delegate.put(sessionKey, challenge,
                        Set.of(PutOption.IfRecordDoesNotExist, PutOption.AsEphemeralRecord));
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

    /** Reopens the ephemeral marker only after the caller explicitly requests recovery. */
    @Override
    public synchronized void reconnectSession() {
        requireNotClosed();
        if (started) {
            try {
                requireSession();
                return;
            } catch (SessionFenceException fenced) {
                clearSession();
                rotateMarker();
            }
        }
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
            notificationDelegate.notifications(consumer);
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
        return delegate.get(key);
    }

    @Override
    public synchronized CloseableIterable<GetResult> rangeScan(final String startKeyInclusive,
                                                                final String endKeyExclusive) {
        requireSession();
        return delegate.rangeScan(startKeyInclusive, endKeyExclusive);
    }

    @Override
    public synchronized void notifications(final Consumer<Notification> consumer) {
        requireSession();
        notificationDelegate.notifications(consumer);
    }

    @Override
    public synchronized PutResult put(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        requireSession();
        return delegate.put(key, value, options);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        started = false;
        sessionVersion = null;
        sessionIdentity = null;
        delegate.close();
        if (notificationDelegate != delegate) {
            notificationDelegate.close();
        }
    }

    private void requireSession() {
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
        if (!sessionKey.equals(result.key()) || result.value() == null || result.version() == null
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
        final long sessionId = version.sessionId().orElseThrow(
                () -> new IllegalArgumentException("Oxia Route session has no session id"));
        final String clientIdentifier = canonicalText(version.clientIdentifier().orElseThrow(
                () -> new IllegalArgumentException("Oxia Route session has no client identifier")),
                "clientIdentifier");
        if (sessionId < 0) {
            throw new IllegalArgumentException("Oxia Route session id must be non-negative");
        }
        return Bytes.sha256(SESSION_DOMAIN, Bytes.u64be(sessionId),
                Bytes.lp32(clientIdentifier.getBytes(StandardCharsets.UTF_8)));
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
        if (!value.equals(new String(encoded, StandardCharsets.UTF_8)) || value.isBlank()
                || value.indexOf('\0') >= 0 || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
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
                throw new IllegalStateException("failed to close Oxia Route authority session", failure);
            }
        }
    }
}
