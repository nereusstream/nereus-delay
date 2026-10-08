package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/** Session-fenced immutable Retry Policy storage; caller control authorization and Source publication are required. */
public final class OxiaSyncRetryPolicyCatalog implements RetryPolicyCatalog {
    private static final int MAX_RECORD_BYTES = 1 << 20;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-oxia-retry-policy\0");
    private final RecordClient client;
    private final String prefix;
    private final Runnable session;

    public OxiaSyncRetryPolicyCatalog(final OxiaSyncOwnerLeaseBackend.ClientHandle handle, final String keyPrefix) {
        this(new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()), keyPrefix,
                handle.backend()::assertConnectedSession);
    }

    OxiaSyncRetryPolicyCatalog(RecordClient client, String keyPrefix, Runnable session) {
        this.client = Objects.requireNonNull(client, "client");
        this.session = Objects.requireNonNull(session, "session");
        prefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
        if (prefix.isBlank() || prefix.endsWith("/") || prefix.indexOf('\0') >= 0
                || !prefix.equals(new String(prefix.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))
                || !Normalizer.isNormalized(prefix, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException("Retry Policy catalog requires a canonical nonempty key prefix");
        }
    }

    /** Stores an already-authorized publication; exact reread is required after a race or uncertain response. */
    public void publish(final RetryPolicySemantic semantic, final SourcePosition visibleAt) {
        final var proposed = new Publication(semantic, visibleAt);
        final byte[] value = encode(proposed);
        final String key = key(semantic.ref(), visibleAt);
        final byte[] existing = get(key);
        if (existing != null) {
            requireExact(existing, value, key);
            return;
        }
        try {
            session.run();
            client.putIfAbsent(key, value);
            session.run();
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            requireObservedExact(key, value, race);
        } catch (RuntimeException uncertain) {
            final byte[] observed = get(key);
            if (observed == null) { throw uncertain; }
            requireExact(observed, value, key);
        }
    }

    @Override
    public RetryPolicySemantic resolve(final RetryPolicyRef reference, final SourcePosition sourcePosition) {
        Objects.requireNonNull(reference, "reference");
        TargetSourcePosition.requireBounded(sourcePosition);
        final String key = key(reference, sourcePosition);
        final byte[] value = get(key);
        if (value == null) { return null; }
        final var publication = decode(value);
        if (!key.equals(key(publication.semantic().ref(), publication.visibleAt()))) {
            throw new IllegalStateException("Retry Policy catalog key disagrees with its immutable value");
        }
        if (!reference.matches(publication.semantic())
                || !publication.visibleAt().sameSourceIdentity(sourcePosition)
                || publication.visibleAt().kind() != sourcePosition.kind()) { return null; }
        final int order = publication.visibleAt().compareTo(sourcePosition);
        if (order > 0 || order == 0
                && !Arrays.equals(publication.visibleAt().canonicalBytes(), sourcePosition.canonicalBytes())) {
            return null;
        }
        return publication.semantic();
    }

    private void requireObservedExact(String key, byte[] value, Exception failure) {
        final byte[] observed = get(key);
        if (observed == null) {
            throw new IllegalStateException("Retry Policy publication vanished after CAS", failure);
        }
        requireExact(observed, value, key);
    }

    private static void requireExact(byte[] existing, byte[] expected, String key) {
        decode(existing);
        if (!Arrays.equals(existing, expected)) {
            throw new IllegalStateException("Retry Policy version or first Source visibility changed: " + key);
        }
    }

    private byte[] get(String key) {
        session.run();
        final byte[] value = client.get(key);
        session.run();
        return value;
    }

    private String key(RetryPolicyRef reference, SourcePosition source) {
        return prefix + "/retry-policy/" + Bytes.hex(new ShardSubject(source.shardId()).canonicalBytes()) + "/"
                + Bytes.hex(reference.policyId()) + "/" + Long.toUnsignedString(reference.version());
    }

    private record Publication(RetryPolicySemantic semantic, SourcePosition visibleAt) {
        Publication {
            Objects.requireNonNull(semantic, "semantic");
            TargetSourcePosition.requireBounded(visibleAt);
        }
    }

    private static byte[] fields(Publication publication) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, publication.semantic().canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, publication.visibleAt().canonicalBytes());
        });
    }

    private static byte[] encode(Publication publication) {
        final byte[] fields = fields(publication);
        final byte[] value = CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(out, 4, Bytes.sha256(DOMAIN, fields));
        });
        if (value.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("Retry Policy catalog value too large");
        }
        return value;
    }

    private static Publication decode(byte[] value) {
        if (value.length == 0 || value.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("invalid Retry Policy catalog value length");
        }
        final var fields = QueryCodecSupport.read(value, "Oxia Retry Policy publication");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "Oxia Retry Policy publication");
        if (QueryCodecSupport.uint(fields.get(0), 1) != 1) {
            throw new IllegalArgumentException("unsupported Retry Policy catalog version");
        }
        final var publication = new Publication(RetryPolicySemantic.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                SourcePositionCodec.decode(QueryCodecSupport.bytes(fields.get(2), 3)));
        if (!Arrays.equals(QueryCodecSupport.fixed(fields.get(3), 4, 32), Bytes.sha256(DOMAIN, fields(publication)))) {
            throw new IllegalArgumentException("Retry Policy catalog digest mismatch");
        }
        QueryCodecSupport.requireCanonical(value, encode(publication), "Oxia Retry Policy publication");
        return publication;
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
                throw new IllegalStateException("Oxia returned an incomplete or mismatched Retry Policy record");
            }
            return result.value();
        }

        @Override
        public void putIfAbsent(String key, byte[] value)
                throws KeyAlreadyExistsException, UnexpectedVersionIdException {
            final var result = client.put(key, value, Set.of(PutOption.IfRecordDoesNotExist));
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Retry Policy put returned no exact version");
            }
        }
    }
}
