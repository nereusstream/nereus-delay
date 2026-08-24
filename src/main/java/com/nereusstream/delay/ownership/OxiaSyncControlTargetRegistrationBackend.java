package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ControlTargetMutationBindingV1;
import com.nereusstream.delay.protocol.ControlTargetRefV1;
import com.nereusstream.delay.protocol.PreparedControlOperationV1;
import com.nereusstream.delay.protocol.SystemMutation;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Durable Oxia backend for immutable Prepared Control target registration.
 *
 * <p>Each operation ID is one Oxia record. Registration is an
 * {@code IfRecordDoesNotExist} CAS and all later reads return the exact
 * canonical Prepared bytes. A response-loss retry is classified as
 * idempotent only after that exact reread.</p>
 *
 * <p>This backend deliberately does not add actor authorization or source
 * ordering; those remain required by the surrounding Control authority. The
 * handle-bound constructor additionally fences every record I/O to the exact
 * ephemeral Oxia session.</p>
 */
public final class OxiaSyncControlTargetRegistrationBackend
        implements OxiaControlTargetRegistrationAuthority.CasBackend {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_PREPARED_BYTES = 8 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-control-target-v1\0");

    private final RecordClient client;
    private final String keyPrefix;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncControlTargetRegistrationBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Creates a backend fenced to the exact ephemeral session of a handle. */
    public OxiaSyncControlTargetRegistrationBackend(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle, final String keyPrefix) {
        this(
                new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()),
                keyPrefix,
                handle.backend()::assertConnectedSession);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncControlTargetRegistrationBackend(final RecordClient client, final String keyPrefix) {
        this(client, keyPrefix, () -> {});
    }

    /** Package-private constructor used to exercise the session fence. */
    OxiaSyncControlTargetRegistrationBackend(
            final RecordClient client, final String keyPrefix, final Runnable sessionCheck) {
        this.client = new SessionBoundRecordClient(
                Objects.requireNonNull(client, "client"), Objects.requireNonNull(sessionCheck, "sessionCheck"));
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
    }

    @Override
    public ControlTargetRegistrationAuthority.RegistrationResult register(final PreparedControlOperationV1 prepared) {
        Objects.requireNonNull(prepared, "prepared");
        final String key = operationKey(prepared.operationId());
        final Entry existing = read(key, prepared.operationId());
        if (existing != null) {
            return classify(existing, prepared);
        }
        final byte[] value = encode(prepared);
        try {
            putExact(key, value, Set.of(PutOption.IfRecordDoesNotExist));
            return ControlTargetRegistrationAuthority.RegistrationResult.RECORDED;
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final Entry observed = read(key, prepared.operationId());
            if (observed == null) {
                throw new IllegalStateException("Oxia target registration disappeared after CAS", race);
            }
            return classify(observed, prepared);
        } catch (RuntimeException responseFailure) {
            final Entry observed = read(key, prepared.operationId());
            if (observed != null && exact(observed.prepared(), prepared)) {
                return ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED;
            }
            throw responseFailure;
        }
    }

    @Override
    public Optional<PreparedControlOperationV1> find(final byte[] operationId) {
        return Optional.ofNullable(read(operationKey(operationId), operationId)).map(Entry::prepared);
    }

    @Override
    public void validateMutation(
            final PreparedControlOperationV1 prepared, final ControlTargetRefV1 target, final SystemMutation mutation) {
        Objects.requireNonNull(prepared, "prepared");
        final Entry registered = read(operationKey(prepared.operationId()), prepared.operationId());
        if (registered == null || !exact(registered.prepared(), prepared)) {
            throw new IllegalArgumentException("Control operation has not been registered exactly");
        }
        ControlTargetMutationBindingV1.validate(registered.prepared(), target, mutation);
    }

    private static ControlTargetRegistrationAuthority.RegistrationResult classify(
            final Entry existing, final PreparedControlOperationV1 requested) {
        if (!exact(existing.prepared(), requested)) {
            throw new IllegalArgumentException("Control operation ID is already registered with different bytes");
        }
        return ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED;
    }

    private Entry read(final String key, final byte[] operationId) {
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia target registration response has an invalid record identity");
        }
        return decode(result.value(), operationId, result.version().versionId());
    }

    private void putExact(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final PutResult result = client.put(key, value, options);
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia target registration put returned no exact version");
        }
    }

    private String operationKey(final byte[] operationId) {
        Bytes.requireLength(operationId, 32, "operationId");
        boolean nonZero = false;
        for (byte value : operationId) {
            nonZero |= value != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException("operationId must be non-zero");
        }
        return keyPrefix + "/operation/" + Bytes.hex(operationId);
    }

    private static boolean exact(final PreparedControlOperationV1 left, final PreparedControlOperationV1 right) {
        return Bytes.constantTimeEquals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static byte[] encode(final PreparedControlOperationV1 prepared) {
        final byte[] preparedBytes = prepared.canonicalBytes();
        if (preparedBytes.length == 0 || preparedBytes.length > MAX_PREPARED_BYTES) {
            throw new IllegalArgumentException("Prepared Control bytes exceed Oxia target limit");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, preparedBytes);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.bytes(output, 2, preparedBytes);
            CanonicalProtobuf.bytes(output, 3, digest);
        });
    }

    private static Entry decode(final byte[] encoded, final byte[] operationId, final long versionId) {
        if (encoded == null || encoded.length > MAX_PREPARED_BYTES + 256) {
            throw new IllegalStateException("Oxia target registration record exceeds bounded size");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        if (uint(version, 1) != RECORD_VERSION) {
            throw new IllegalStateException("unsupported Oxia target registration record version");
        }
        final byte[] preparedBytes = bytes(next(reader, 2), 2);
        final byte[] digest = bytes(next(reader, 3), 3);
        if (reader.hasRemaining() || !Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, preparedBytes))) {
            throw new IllegalStateException("Oxia target registration record is non-canonical or corrupt");
        }
        try {
            final PreparedControlOperationV1 prepared = PreparedControlOperationV1.decode(preparedBytes);
            if (!Bytes.constantTimeEquals(operationId, prepared.operationId())
                    || !Arrays.equals(encoded, encode(prepared))) {
                throw new IllegalStateException("Oxia target registration record identity mismatch");
            }
            return new Entry(prepared, versionId);
        } catch (RuntimeException malformed) {
            if (malformed instanceof IllegalStateException stateFailure
                    && "Oxia target registration record identity mismatch".equals(stateFailure.getMessage())) {
                throw stateFailure;
            }
            throw new IllegalStateException("Oxia target registration record contains malformed values", malformed);
        }
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalStateException("missing Oxia target registration field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalStateException("unexpected Oxia target registration field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalStateException("invalid Oxia target registration bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalStateException("invalid Oxia target registration varint field " + number);
        }
        return field.unsignedValue();
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

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
    }

    private record Entry(PreparedControlOperationV1 prepared, long versionId) {
        private Entry {
            Objects.requireNonNull(prepared, "prepared");
        }
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
    }

    /**
     * Checks the caller's Oxia session around every record operation. A
     * successful registration whose response is lost after the marker
     * disappears is therefore never reported as recorded.
     */
    private static final class SessionBoundRecordClient implements RecordClient {
        private final RecordClient delegate;
        private final Runnable sessionCheck;

        private SessionBoundRecordClient(final RecordClient delegate, final Runnable sessionCheck) {
            this.delegate = delegate;
            this.sessionCheck = sessionCheck;
        }

        @Override
        public GetResult get(final String key) {
            sessionCheck.run();
            try {
                final GetResult result = delegate.get(key);
                sessionCheck.run();
                return result;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            sessionCheck.run();
            try {
                final PutResult result = delegate.put(key, value, options);
                sessionCheck.run();
                return result;
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }
    }
}
