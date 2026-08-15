package io.nereusstream.delay.gateway;

import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.protocol.Bytes;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.Set;

/**
 * Oxia-backed append-only Gateway audit sink.
 *
 * <p>Each digest-only event has its own immutable record key.  Recording the
 * same event again is therefore idempotent, while a response lost after the
 * write is accepted only after an exact value reread.  The sink deliberately
 * does not expose request or prepared-submission bytes.</p>
 */
public final class OxiaGatewayAuditSink implements GatewayAuditSink, AutoCloseable {
    private static final int MAX_EVENT_BYTES = 64 * 1024;
    private static final String AUDIT_SUFFIX = "/audit/";
    private static final byte[] KEY_DOMAIN = Bytes.utf8("nereus-delay-gateway-audit-record-v1\0");

    private final OxiaGatewayRecordClient client;
    private final String recordPrefix;

    /** Creates a sink over an already configured Oxia client. */
    public OxiaGatewayAuditSink(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Creates a sink fenced to the exact ephemeral session of a handle. */
    public OxiaGatewayAuditSink(final OxiaSyncOwnerLeaseBackend.ClientHandle handle, final String keyPrefix) {
        this(new SessionBoundOxiaGatewayRecordClient(handle), keyPrefix);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaGatewayAuditSink(final RecordClient client, final String keyPrefix) {
        this((OxiaGatewayRecordClient) client, keyPrefix);
    }

    OxiaGatewayAuditSink(final OxiaGatewayRecordClient client, final String keyPrefix) {
        this.client = Objects.requireNonNull(client, "client");
        this.recordPrefix = canonicalKeyPrefix(keyPrefix) + AUDIT_SUFFIX;
    }

    @Override
    public synchronized void record(final GatewayAuditEventV1 event) {
        final GatewayAuditEventV1 requested = Objects.requireNonNull(event, "event");
        final byte[] value = requested.canonicalBytes();
        if (value.length > MAX_EVENT_BYTES) {
            throw new IllegalArgumentException("Gateway audit event exceeds the size bound");
        }
        final String key = recordKey(value);
        final GetResult existing = client.get(key);
        if (existing != null) {
            acceptExact(existing, key, value);
            return;
        }
        try {
            final PutResult result = client.put(key, value, Set.of(PutOption.IfRecordDoesNotExist));
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Gateway audit put returned no exact version");
            }
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            acceptAfterRace(key, value, race);
        } catch (RuntimeException responseFailure) {
            acceptAfterResponseLoss(key, value, responseFailure);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private void acceptAfterRace(final String key, final byte[] value, final Throwable race) {
        final GetResult observed = client.get(key);
        if (observed == null) {
            throw new IllegalStateException("Gateway audit CAS lost without an observable record", race);
        }
        try {
            acceptExact(observed, key, value);
        } catch (RuntimeException mismatch) {
            mismatch.addSuppressed(race);
            throw mismatch;
        }
    }

    private void acceptAfterResponseLoss(final String key, final byte[] value,
                                         final RuntimeException responseFailure) {
        try {
            final GetResult observed = client.get(key);
            if (observed != null) {
                acceptExact(observed, key, value);
                return;
            }
        } catch (RuntimeException rereadFailure) {
            responseFailure.addSuppressed(rereadFailure);
        }
        throw responseFailure;
    }

    private static void acceptExact(final GetResult result, final String key, final byte[] expected) {
        if (!key.equals(result.key()) || result.value() == null || result.version() == null
                || !Bytes.constantTimeEquals(expected, result.value())) {
            throw new IllegalStateException("Oxia Gateway audit record is not the exact immutable event");
        }
    }

    private String recordKey(final byte[] eventBytes) {
        return recordPrefix + Bytes.hex(Bytes.sha256(KEY_DOMAIN, eventBytes));
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be nonblank NFC UTF-8 without trailing '/'");
        }
        return value;
    }

    interface RecordClient extends OxiaGatewayRecordClient {
    }

    private static final class SyncRecordClient implements RecordClient {
        private final SyncOxiaClient delegate;

        private SyncRecordClient(final SyncOxiaClient delegate) {
            this.delegate = Objects.requireNonNull(delegate, "client");
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

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (Exception failure) {
                throw new IllegalStateException("failed to close Oxia Gateway audit client", failure);
            }
        }
    }
}
