package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

/** Exact immutable public verifier bytes; Source activation/issuance and retention authority remain external. */
public final class OxiaSyncPayloadProofTrustSetCatalog implements PayloadProofTrustSetControlCatalog {
    private final RecordClient client;
    private final String prefix;
    private final int maximumValueBytes;
    private final Runnable session;

    public OxiaSyncPayloadProofTrustSetCatalog(OxiaSyncOwnerLeaseBackend.ClientHandle handle, String keyPrefix,
            int maximumValueBytes) {
        this(new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()), keyPrefix,
                maximumValueBytes, handle.backend()::assertConnectedSession);
    }

    OxiaSyncPayloadProofTrustSetCatalog(RecordClient client, String keyPrefix, int maximumValueBytes,
            Runnable session) {
        this.client = Objects.requireNonNull(client, "client");
        this.session = Objects.requireNonNull(session, "session");
        prefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
        if (prefix.isBlank() || prefix.endsWith("/") || prefix.indexOf('\0') >= 0
                || !prefix.equals(new String(prefix.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))
                || !Normalizer.isNormalized(prefix, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException("Trust Set catalog requires a canonical nonempty key prefix");
        }
        if (maximumValueBytes <= 0 || maximumValueBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Trust Set catalog requires a finite positive byte bound");
        }
        this.maximumValueBytes = maximumValueBytes;
    }

    /** Publishes already-authorized immutable bytes under held publication/retention protection. */
    public void publish(PayloadProofTrustSetSemantic semantic, Runnable protection) {
        Objects.requireNonNull(semantic, "semantic");
        Objects.requireNonNull(protection, "protection").run();
        final byte[] value = semantic.canonicalBytes();
        requireBounded(value);
        final String key = key(semantic.ref());
        final byte[] existing = get(key);
        if (existing != null) {
            requireExact(existing, value);
            protection.run();
            return;
        }
        try {
            protection.run();
            session.run();
            client.putIfAbsent(key, value);
            session.run();
            protection.run();
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            requireObservedExact(key, value, race);
            protection.run();
        } catch (RuntimeException uncertain) {
            final byte[] observed = get(key);
            if (observed == null) { throw uncertain; }
            requireExact(observed, value);
            protection.run();
        }
    }

    @Override
    public PayloadProofTrustSetSemantic resolve(PayloadProofTrustSetRef reference) {
        final byte[] value = get(key(Objects.requireNonNull(reference, "reference")));
        if (value == null) { return null; }
        requireBounded(value);
        final var semantic = PayloadProofTrustSetSemantic.decode(value);
        if (!reference.equals(semantic.ref())) {
            throw new IllegalStateException("Trust Set catalog key differs from immutable semantic bytes");
        }
        return semantic;
    }

    /** Caller owns finite executor, client request timeout and protection until actual I/O ends. */
    public CompletableFuture<PayloadProofTrustSetSemantic> resolveAsync(PayloadProofTrustSetRef reference,
            Duration timeout, Runnable protection, ThreadPoolExecutor executor) {
        Objects.requireNonNull(reference, "reference");
        return CheckpointRegistryLookup.submit(() -> resolve(reference), timeout, protection, executor,
                System::nanoTime);
    }

    private void requireObservedExact(String key, byte[] value, Exception failure) {
        final byte[] observed = get(key);
        if (observed == null) { throw new IllegalStateException("Trust Set vanished after CAS", failure); }
        requireExact(observed, value);
    }

    private void requireExact(byte[] existing, byte[] expected) {
        requireBounded(existing);
        PayloadProofTrustSetSemantic.decode(existing);
        if (!Arrays.equals(existing, expected)) {
            throw new IllegalStateException("Trust Set reference changed immutable semantic bytes");
        }
    }

    private void requireBounded(byte[] value) {
        if (value.length == 0 || value.length > maximumValueBytes) {
            throw new IllegalArgumentException("Trust Set catalog value exceeds its finite byte bound");
        }
    }

    private byte[] get(String key) {
        session.run();
        final byte[] value = client.get(key);
        session.run();
        return value;
    }

    private String key(PayloadProofTrustSetRef reference) {
        return prefix + "/payload-trust-set/" + Bytes.hex(reference.canonicalBytes());
    }

    interface RecordClient {
        byte[] get(String key);
        void putIfAbsent(String key, byte[] value) throws KeyAlreadyExistsException, UnexpectedVersionIdException;
    }

    private record SyncRecordClient(SyncOxiaClient client) implements RecordClient {
        @Override
        public byte[] get(String key) {
            final var result = client.get(key);
            if (result == null) { return null; }
            if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
                throw new IllegalStateException("Oxia returned an incomplete or mismatched Trust Set record");
            }
            return result.value();
        }

        @Override
        public void putIfAbsent(String key, byte[] value)
                throws KeyAlreadyExistsException, UnexpectedVersionIdException {
            final var result = client.put(key, value, Set.of(PutOption.IfRecordDoesNotExist));
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Trust Set put returned no exact version");
            }
        }
    }
}
