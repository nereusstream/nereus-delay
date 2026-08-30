package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import io.oxia.client.api.GetResult;
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

/** Canonical single-record Oxia current-head authority for each handoff policy scope. */
public final class OxiaSyncHandoffPolicyAuthority implements HandoffPolicyAuthority {
    private static final String HEAD_SEGMENT = "/handoff-policy/head/";

    private final RecordClient client;
    private final String keyPrefix;

    public OxiaSyncHandoffPolicyAuthority(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    OxiaSyncHandoffPolicyAuthority(final RecordClient client, final String keyPrefix) {
        this.client = Objects.requireNonNull(client, "client");
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
    }

    /** Opens an owned bounded Oxia client for persistent policy reads and publications. */
    public static ClientHandle connect(
            final String serviceAddress,
            final String namespace,
            final String clientIdentifier,
            final Duration requestTimeout,
            final String keyPrefix)
            throws OxiaException {
        Objects.requireNonNull(serviceAddress, "serviceAddress");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(clientIdentifier, "clientIdentifier");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        final SyncOxiaClient client = OxiaClientBuilder.create(serviceAddress)
                .namespace(namespace)
                .clientIdentifier(clientIdentifier)
                .requestTimeout(requestTimeout)
                .syncClient();
        return new ClientHandle(client, new OxiaSyncHandoffPolicyAuthority(client, keyPrefix));
    }

    @Override
    public Optional<Publication> current(final byte[] policyScopeDigest) {
        final byte[] scope = scope(policyScopeDigest);
        return Optional.ofNullable(decode(client.get(headKey(scope)), scope));
    }

    @Override
    public Publication compareAndSet(
            final byte[] policyScopeDigest, final long expectedOxiaVersion, final HandoffPolicyHead next) {
        final byte[] scope = scope(policyScopeDigest);
        if (expectedOxiaVersion < 0) {
            throw new IllegalArgumentException("expectedOxiaVersion must be non-negative");
        }
        final HandoffPolicyHead exactNext = Objects.requireNonNull(next, "next");
        if (!Arrays.equals(scope, exactNext.scopeDigest())) {
            throw new IllegalArgumentException("policy head scope mismatch");
        }
        final String key = headKey(scope);
        final GetResult currentResult = client.get(key);
        final Publication current = decode(currentResult, scope);
        final long actualVersion = current == null ? 0 : current.oxiaVersion();
        if (actualVersion != expectedOxiaVersion) {
            throw new IllegalStateException("policy head compare-and-set revision conflict");
        }
        final byte[] bytes = exactNext.canonicalBytes();
        final Set<PutOption> options = currentResult == null
                ? Set.of(PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.IfVersionIdEquals(currentResult.version().versionId()));
        try {
            return publicationAfterPut(key, scope, exactNext, client.put(key, bytes, options));
        } catch (UnexpectedVersionIdException | KeyAlreadyExistsException conflict) {
            throw new IllegalStateException("policy head compare-and-set revision conflict", conflict);
        } catch (RuntimeException responseFailure) {
            final Publication observed = decode(client.get(key), scope);
            if (observed != null && Arrays.equals(bytes, observed.head().canonicalBytes())) {
                return observed;
            }
            throw responseFailure;
        }
    }

    private Publication publicationAfterPut(
            final String key, final byte[] scope, final HandoffPolicyHead head, final PutResult result) {
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia handoff policy put returned no exact version");
        }
        final Publication publication =
                new Publication(externalVersion(result.version().versionId()), head);
        final Publication observed = decode(client.get(key), scope);
        if (observed == null || !publication.sameHead(observed)) {
            throw new IllegalStateException("Oxia handoff policy put did not reread as the exact current head");
        }
        return publication;
    }

    private Publication decode(final GetResult result, final byte[] expectedScope) {
        if (result == null) {
            return null;
        }
        final String expectedKey = headKey(expectedScope);
        if (!expectedKey.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia handoff policy response has an invalid record identity");
        }
        final HandoffPolicyHead head;
        try {
            head = HandoffPolicyHead.decode(result.value());
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Oxia handoff policy current head is non-canonical", failure);
        }
        if (!Arrays.equals(expectedScope, head.scopeDigest())) {
            throw new IllegalStateException("Oxia handoff policy current head scope mismatch");
        }
        return new Publication(externalVersion(result.version().versionId()), head);
    }

    private String headKey(final byte[] scope) {
        return keyPrefix + HEAD_SEGMENT + Bytes.hex(scope);
    }

    private static long externalVersion(final long versionId) {
        if (versionId < 0 || versionId == Long.MAX_VALUE) {
            throw new IllegalStateException("Oxia handoff policy version is outside the supported range");
        }
        return versionId + 1;
    }

    private static byte[] scope(final byte[] value) {
        Bytes.requireLength(value, HandoffPolicyHead.HASH_LENGTH, "policyScopeDigest");
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

    public record ClientHandle(SyncOxiaClient client, OxiaSyncHandoffPolicyAuthority authority) implements Closeable {
        public ClientHandle {
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(authority, "authority");
        }

        @Override
        public void close() throws IOException {
            try {
                client.close();
            } catch (Exception failure) {
                throw new IOException("cannot close Oxia handoff policy client", failure);
            }
        }
    }
}
