package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.text.Normalizer;
import java.util.Objects;
import java.util.Set;

/**
 * Publishes one signed Route event and advances the Oxia head with a version
 * CAS. A failed head CAS leaves only an invisible orphan event.
 */
public final class OxiaSignedRouteSnapshotPublisher {
    private static final String EVENTS_SUFFIX = "/events/";
    private static final String HEAD_SUFFIX = "/head";

    private final OxiaRouteRecordClient client;
    private final PublicKey verificationKey;
    private final String eventPrefix;
    private final String headKey;

    public OxiaSignedRouteSnapshotPublisher(
            final SyncOxiaClient client, final String keyPrefix, final PublicKey verificationKey) {
        this(new SyncRecordClient(client), keyPrefix, verificationKey);
    }

    /** Creates a publisher whose event/head CAS is fenced by the supplied ephemeral session. */
    public OxiaSignedRouteSnapshotPublisher(
            final OxiaRouteAuthoritySession session, final String keyPrefix, final PublicKey verificationKey) {
        this((OxiaRouteRecordClient) Objects.requireNonNull(session, "session"), keyPrefix, verificationKey);
    }

    OxiaSignedRouteSnapshotPublisher(
            final OxiaRouteRecordClient client, final String keyPrefix, final PublicKey verificationKey) {
        this.client = Objects.requireNonNull(client, "client");
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        final String prefix = canonicalKeyPrefix(keyPrefix);
        this.eventPrefix = prefix + EVENTS_SUFFIX;
        this.headKey = prefix + HEAD_SUFFIX;
    }

    public Publication publish(
            final RouteSelectionHint route, final RouteSnapshotV1 snapshot, final long expectedPreviousRevision) {
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(snapshot, "snapshot");
        client.startSession();
        if (expectedPreviousRevision < 0) {
            throw new IllegalArgumentException("expectedPreviousRevision must be non-negative");
        }
        final RouteSnapshotV1 verified = RouteSnapshotV1.decode(snapshot.canonicalBytes(), verificationKey);
        final HeadEntry currentHead = readHead();
        final long observedRevision =
                currentHead == null ? 0 : currentHead.head().publishedRevision();
        if (observedRevision != expectedPreviousRevision) {
            throw new IllegalStateException("Route head revision changed before publication");
        }
        final long nextRevision;
        try {
            nextRevision = Math.addExact(expectedPreviousRevision, 1);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Route revision exhausted", overflow);
        }
        final OxiaRouteSnapshotRecordV1 event =
                OxiaRouteSnapshotRecordV1.create(nextRevision, expectedPreviousRevision, route, verified);
        if (expectedPreviousRevision > 0) {
            final GetResult previousResult = client.get(eventKey(expectedPreviousRevision));
            if (previousResult == null || previousResult.value() == null) {
                throw new IllegalStateException("previous Route event is unavailable");
            }
            final OxiaRouteSnapshotRecordV1 previous =
                    OxiaRouteSnapshotRecordV1.decode(previousResult.value(), verificationKey);
            if (previous.snapshot().routeIncarnation().equals(verified.routeIncarnation())) {
                RouteSnapshotCompatibilityV1.requireCompatibleSuccessor(previous.snapshot(), verified);
            }
        }
        putEventExactly(event);
        final OxiaRouteSnapshotHeadV1 nextHead = new OxiaRouteSnapshotHeadV1(nextRevision, event.recordDigest());
        try {
            putHeadExactly(nextHead, currentHead);
        } catch (RuntimeException failure) {
            final HeadEntry observed = readHead();
            if (observed != null
                    && Bytes.constantTimeEquals(observed.head().canonicalBytes(), nextHead.canonicalBytes())) {
                return new Publication(nextRevision, event.recordDigest());
            }
            throw failure;
        }
        return new Publication(nextRevision, event.recordDigest());
    }

    private void putEventExactly(final OxiaRouteSnapshotRecordV1 event) {
        final String key = eventKey(event.revision());
        final GetResult existing = client.get(key);
        if (existing != null) {
            if (existing.value() == null
                    || !key.equals(existing.key())
                    || !Bytes.constantTimeEquals(existing.value(), event.canonicalBytes())) {
                throw new IllegalStateException("Route event key already contains different bytes");
            }
            return;
        }
        try {
            final PutResult result = client.put(key, event.canonicalBytes(), Set.of(PutOption.IfRecordDoesNotExist));
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Route event put returned no exact version");
            }
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final GetResult observed = client.get(key);
            if (observed == null
                    || observed.value() == null
                    || !Bytes.constantTimeEquals(observed.value(), event.canonicalBytes())) {
                throw new IllegalStateException("Route event CAS lost to different bytes", race);
            }
        } catch (RuntimeException responseFailure) {
            final GetResult observed = client.get(key);
            if (observed == null
                    || observed.value() == null
                    || !Bytes.constantTimeEquals(observed.value(), event.canonicalBytes())) {
                throw responseFailure;
            }
        }
    }

    private void putHeadExactly(final OxiaRouteSnapshotHeadV1 nextHead, final HeadEntry currentHead) {
        final Set<PutOption> options = currentHead == null
                ? Set.of(PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.IfVersionIdEquals(currentHead.versionId()));
        try {
            final PutResult result = client.put(headKey, nextHead.canonicalBytes(), options);
            if (result == null || !headKey.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Route head put returned no exact version");
            }
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            throw new IllegalStateException("Oxia Route head CAS lost", race);
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
                OxiaRouteSnapshotHeadV1.decode(result.value()), result.version().versionId());
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

    public record Publication(long revision, byte[] eventDigest) {
        public Publication {
            if (revision <= 0) {
                throw new IllegalArgumentException("publication revision must be positive");
            }
            Bytes.requireLength(eventDigest, 32, "eventDigest");
            eventDigest = Bytes.copy(eventDigest);
        }

        @Override
        public byte[] eventDigest() {
            return Bytes.copy(eventDigest);
        }
    }

    private record HeadEntry(OxiaRouteSnapshotHeadV1 head, long versionId) {}

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
        public io.oxia.client.api.CloseableIterable<GetResult> rangeScan(
                final String startKeyInclusive, final String endKeyExclusive) {
            return delegate.rangeScan(startKeyInclusive, endKeyExclusive);
        }

        @Override
        public void notifications(final java.util.function.Consumer<io.oxia.client.api.Notification> consumer) {
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
                throw new IllegalStateException("failed to close Oxia Route publisher client", failure);
            }
        }
    }
}
