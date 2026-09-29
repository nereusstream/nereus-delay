package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetNativePolicyHead;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Persistent current-head service; only the authenticated publish API can reach its low-level Oxia CAS. */
public final class OxiaSyncTargetNativePolicyAuthority {
    private static final String HEAD_SEGMENT = "/target-native-policy/head/";

    private final TargetNativePolicyTrust trust;
    private final TargetNativePolicyTrust.PublisherScopeProof scopeProof;
    private final HeadStore store;
    private final SyncOxiaClient notificationClient;
    private final String headKeyPrefix;
    private final AtomicReference<Runnable> currentHeadChangeListener = new AtomicReference<>();
    private final Object notificationRegistrationLock = new Object();
    private boolean notificationRegistrationStarted;
    private final TargetNativePolicyAuthority readAuthority = new ReadAuthority();

    public OxiaSyncTargetNativePolicyAuthority(
            final SyncOxiaClient client,
            final String keyPrefix,
            final TargetNativePolicyTrust trust,
            final TargetNativePolicyTrust.PublisherScopeProof scopeProof) {
        this(new SyncRecordClient(client), keyPrefix, trust, scopeProof, client);
    }

    OxiaSyncTargetNativePolicyAuthority(
            final RecordClient client,
            final String keyPrefix,
            final TargetNativePolicyTrust trust,
            final TargetNativePolicyTrust.PublisherScopeProof scopeProof) {
        this(client, keyPrefix, trust, scopeProof, null);
    }

    private OxiaSyncTargetNativePolicyAuthority(
            final RecordClient client,
            final String keyPrefix,
            final TargetNativePolicyTrust trust,
            final TargetNativePolicyTrust.PublisherScopeProof scopeProof,
            final SyncOxiaClient notificationClient) {
        store = new HeadStore(client, keyPrefix);
        this.trust = Objects.requireNonNull(trust, "trust");
        this.scopeProof = Objects.requireNonNull(scopeProof, "scopeProof");
        this.notificationClient = notificationClient;
        headKeyPrefix = canonicalKeyPrefix(keyPrefix) + HEAD_SEGMENT;
    }

    /** Opens an owned bounded Oxia client for source-authorized policy reads and publication. */
    public static ClientHandle connect(
            final String serviceAddress,
            final String namespace,
            final String clientIdentifier,
            final Duration requestTimeout,
            final String keyPrefix,
            final TargetNativePolicyTrust trust,
            final TargetNativePolicyTrust.PublisherScopeProof scopeProof)
            throws OxiaException {
        Objects.requireNonNull(serviceAddress, "serviceAddress");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(clientIdentifier, "clientIdentifier");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        canonicalKeyPrefix(keyPrefix);
        Objects.requireNonNull(trust, "trust");
        Objects.requireNonNull(scopeProof, "scopeProof");
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        final SyncOxiaClient client = OxiaClientBuilder.create(serviceAddress)
                .namespace(namespace)
                .clientIdentifier(clientIdentifier)
                .requestTimeout(requestTimeout)
                .syncClient();
        return new ClientHandle(client, new OxiaSyncTargetNativePolicyAuthority(client, keyPrefix, trust, scopeProof));
    }

    /** Read-only interface for Claim/Admission checks. Its CAS method always rejects. */
    public TargetNativePolicyAuthority readAuthority() {
        return readAuthority;
    }

    /** Resolves the persistent current head by exact scope digest. */
    public Optional<TargetNativePolicyAuthority.Publication> current(final byte[] scopeDigest) {
        return store.current(scopeDigest);
    }

    /**
     * Subscribes one active Worker loop to this client's current-head notifications. The Oxia notification stream is
     * client-scoped and cannot be individually detached; closing the returned handle clears the Worker callback.
     */
    public Closeable subscribeCurrentHeadChanges(final Runnable listener) {
        final Runnable exactListener = Objects.requireNonNull(listener, "listener");
        if (notificationClient == null) {
            throw new IllegalStateException("Oxia current-head notifications require the owned Oxia client");
        }
        if (!currentHeadChangeListener.compareAndSet(null, exactListener)) {
            throw new IllegalStateException("Oxia current-head notifications already have an active Worker listener");
        }
        try {
            synchronized (notificationRegistrationLock) {
                if (!notificationRegistrationStarted) {
                    notificationClient.notifications(this::onNotification);
                    notificationRegistrationStarted = true;
                }
            }
        } catch (RuntimeException failure) {
            currentHeadChangeListener.compareAndSet(exactListener, null);
            throw failure;
        }
        return () -> currentHeadChangeListener.compareAndSet(exactListener, null);
    }

    private void onNotification(final Notification notification) {
        if (affectsCurrentHeadNamespace(Objects.requireNonNull(notification, "notification"), headKeyPrefix)) {
            final Runnable listener = currentHeadChangeListener.get();
            if (listener != null) {
                listener.run();
            }
        }
    }

    static boolean affectsCurrentHeadNamespace(final Notification notification, final String headKeyPrefix) {
        final String exactPrefix = Objects.requireNonNull(headKeyPrefix, "headKeyPrefix");
        if (notification instanceof Notification.KeyRangeDelete rangeDelete) {
            final String upperBound = exactPrefix.substring(0, exactPrefix.length() - 1) + '0';
            return rangeDelete.startKeyInclusive().compareTo(upperBound) < 0
                    && rangeDelete.endKeyExclusive().compareTo(exactPrefix) > 0;
        }
        final String key = notification.key();
        if (key == null || !key.startsWith(exactPrefix) || key.length() != exactPrefix.length() + 64) {
            return false;
        }
        for (int index = exactPrefix.length(); index < key.length(); index++) {
            final char value = key.charAt(index);
            if (!(value >= '0' && value <= '9') && !(value >= 'a' && value <= 'f')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Publishes through the source-backed trust and scope-proof providers retained by this service. Callers cannot
     * supply request-local trust lambdas or invoke the underlying CAS directly.
     */
    public TargetNativePolicyAuthority.Publication publish(
            final TargetNativePolicyTrust.PublisherPermission permission,
            final ControlAuthorizationContext actor,
            final SourcePosition source,
            final long expectedRevision,
            final TargetNativePolicySnapshot snapshot) {
        return store.publish(permission, trust, actor, scopeProof, source, expectedRevision, snapshot);
    }

    private final class ReadAuthority implements TargetNativePolicyAuthority {
        @Override
        public Optional<Publication> current(final byte[] scopeDigest) {
            return store.current(scopeDigest);
        }

        @Override
        public Closeable subscribeCurrentHeadChanges(final Runnable listener) {
            return OxiaSyncTargetNativePolicyAuthority.this.subscribeCurrentHeadChanges(listener);
        }

        @Override
        public Publication compareAndSet(
                final byte[] scopeDigest, final long expectedRevision, final TargetNativePolicyHead next) {
            throw new UnsupportedOperationException("Target Native current-head readers cannot publish");
        }
    }

    private static final class HeadStore implements TargetNativePolicyAuthority {
        private final RecordClient client;
        private final String keyPrefix;

        private HeadStore(final RecordClient client, final String keyPrefix) {
            this.client = Objects.requireNonNull(client, "client");
            this.keyPrefix = canonicalKeyPrefix(keyPrefix);
        }

        @Override
        public Optional<Publication> current(final byte[] scopeDigest) {
            final byte[] scope = scope(scopeDigest);
            return Optional.ofNullable(decode(client.get(headKey(scope)), scope));
        }

        @Override
        public Publication compareAndSet(
                final byte[] scopeDigest, final long expectedRevision, final TargetNativePolicyHead next) {
            final byte[] scope = scope(scopeDigest);
            if (expectedRevision < 0) {
                throw new IllegalArgumentException("expectedRevision must be non-negative");
            }
            final TargetNativePolicyHead exactNext = Objects.requireNonNull(next, "next");
            if (!Arrays.equals(scope, exactNext.snapshot().policyScopeDigest())) {
                throw new IllegalArgumentException("Target Native head scope mismatch");
            }
            final String key = headKey(scope);
            final GetResult currentResult = client.get(key);
            final Publication current = decode(currentResult, scope);
            final long actualRevision = current == null ? 0 : current.revision();
            if (actualRevision != expectedRevision) {
                throw new IllegalStateException("Target Native publication revision conflict");
            }
            final TargetNativePolicyHead expectedNext =
                    TargetNativePolicyHead.next(current == null ? null : current.head(), exactNext.snapshot());
            if (!expectedNext.equals(exactNext)) {
                throw new IllegalArgumentException("Target Native publication loses its lease high-water mark");
            }
            final Set<PutOption> options = currentResult == null
                    ? Set.of(PutOption.IfRecordDoesNotExist)
                    : Set.of(PutOption.IfVersionIdEquals(currentResult.version().versionId()));
            try {
                return publicationAfterPut(key, scope, exactNext, client.put(key, exactNext.canonicalBytes(), options));
            } catch (UnexpectedVersionIdException | KeyAlreadyExistsException conflict) {
                throw new IllegalStateException("Target Native publication revision conflict", conflict);
            } catch (RuntimeException responseFailure) {
                final Publication observed = decode(client.get(key), scope);
                if (observed != null && exactNext.equals(observed.head()) && observed.revision() > expectedRevision) {
                    return observed;
                }
                throw responseFailure;
            }
        }

        private Publication publicationAfterPut(
                final String key, final byte[] scope, final TargetNativePolicyHead head, final PutResult result) {
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Target Native put returned no exact version");
            }
            final Publication publication = new Publication(externalVersion(result.version().versionId()), head);
            final Publication observed = decode(client.get(key), scope);
            if (observed == null || !publication.sameHead(observed)) {
                throw new IllegalStateException("Oxia Target Native put did not reread as the exact current head");
            }
            return publication;
        }

        private Publication decode(final GetResult result, final byte[] expectedScope) {
            if (result == null) {
                return null;
            }
            final String expectedKey = headKey(expectedScope);
            if (!expectedKey.equals(result.key()) || result.value() == null || result.version() == null) {
                throw new IllegalStateException("Oxia Target Native response has an invalid record identity");
            }
            final TargetNativePolicyHead head;
            try {
                head = TargetNativePolicyHead.decode(result.value());
            } catch (RuntimeException failure) {
                throw new IllegalStateException("Oxia Target Native current head is non-canonical", failure);
            }
            if (!Arrays.equals(expectedScope, head.snapshot().policyScopeDigest())) {
                throw new IllegalStateException("Oxia Target Native current head scope mismatch");
            }
            return new Publication(externalVersion(result.version().versionId()), head);
        }

        private String headKey(final byte[] scope) {
            return keyPrefix + HEAD_SEGMENT + Bytes.hex(scope);
        }
    }

    private static long externalVersion(final long versionId) {
        if (versionId < 0 || versionId == Long.MAX_VALUE) {
            throw new IllegalStateException("Oxia Target Native version is outside the supported range");
        }
        return versionId + 1;
    }

    private static byte[] scope(final byte[] value) {
        Bytes.requireLength(value, TargetNativePolicySnapshot.HASH_LENGTH, "policyScopeDigest");
        return Bytes.copy(value);
    }

    private static String canonicalKeyPrefix(final String value) {
        final String prefix = Objects.requireNonNull(value, "keyPrefix").trim();
        if (prefix.isEmpty() || prefix.endsWith("/") || prefix.contains("//")) {
            throw new IllegalArgumentException("keyPrefix is not canonical");
        }
        return prefix.startsWith("/") ? prefix : "/" + prefix;
    }

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
    }

    private record SyncRecordClient(SyncOxiaClient delegate) implements RecordClient {
        private SyncRecordClient {
            Objects.requireNonNull(delegate, "client");
        }

        @Override
        public GetResult get(final String key) {
            return delegate.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            return delegate.put(key, value, options);
        }
    }

    public record ClientHandle(SyncOxiaClient client, OxiaSyncTargetNativePolicyAuthority authority)
            implements Closeable {
        public ClientHandle {
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(authority, "authority");
        }

        @Override
        public void close() throws IOException {
            try {
                client.close();
            } catch (Exception failure) {
                throw new IOException("cannot close Oxia Target Native policy client", failure);
            }
        }
    }
}
