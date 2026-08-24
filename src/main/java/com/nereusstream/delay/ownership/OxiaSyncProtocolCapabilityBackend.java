package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclarationV1;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Revision-CAS Oxia authority for session-bound Worker protocol capabilities.
 *
 * <p>A handle-backed instance writes each declaration as an ephemeral record
 * and checks the exact Oxia session around every request.  The un-sessioned
 * constructor is retained for deterministic authority tests and controllers
 * that provide their own liveness fence.</p>
 */
public final class OxiaSyncProtocolCapabilityBackend implements ProtocolCapabilityAuthority {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_RECORD_BYTES = 256 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-protocol-capability-record-v1\0");

    private final RecordClient client;
    private final String keyPrefix;
    private final Runnable sessionCheck;
    private final java.util.function.Supplier<byte[]> sessionIdentity;
    private final boolean ephemeral;

    /** Creates an unsessioned backend for deterministic tests or an external controller. */
    public OxiaSyncProtocolCapabilityBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix, () -> {}, () -> null, false);
    }

    /** Creates a capability authority fenced and ephemerally bound to one Oxia session. */
    public OxiaSyncProtocolCapabilityBackend(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle, final String keyPrefix) {
        this(
                new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()),
                keyPrefix,
                handle.backend()::assertConnectedSession,
                handle.backend()::connectedSessionIdentity,
                true);
    }

    OxiaSyncProtocolCapabilityBackend(final RecordClient client, final String keyPrefix) {
        this(client, keyPrefix, () -> {}, () -> null, false);
    }

    OxiaSyncProtocolCapabilityBackend(
            final RecordClient client,
            final String keyPrefix,
            final Runnable sessionCheck,
            final java.util.function.Supplier<byte[]> sessionIdentity,
            final boolean ephemeral) {
        this.client = new SessionBoundRecordClient(
                Objects.requireNonNull(client, "client"), Objects.requireNonNull(sessionCheck, "sessionCheck"));
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
        this.sessionCheck = sessionCheck;
        this.sessionIdentity = Objects.requireNonNull(sessionIdentity, "sessionIdentity");
        this.ephemeral = ephemeral;
    }

    @Override
    public Publication publish(final ProtocolCapabilityDeclarationV1 declaration, final long expectedRevision) {
        Objects.requireNonNull(declaration, "declaration");
        if (expectedRevision < 0) {
            throw new IllegalArgumentException("expected capability revision must be non-negative");
        }
        requireDeclarationSession(declaration);
        final String key = workerKey(declaration.workerId());
        requireSession();
        final Stored current = read(key, declaration.workerId());
        final long observedRevision =
                current == null ? 0 : current.publication().revision();
        if (observedRevision != expectedRevision) {
            throw new IllegalStateException("protocol capability revision changed");
        }
        if (current != null
                && Arrays.equals(current.publication().declaration().canonicalBytes(), declaration.canonicalBytes())) {
            return current.publication();
        }
        final long nextRevision;
        try {
            nextRevision = Math.addExact(expectedRevision, 1);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("protocol capability revision exhausted", overflow);
        }
        final byte[] value = encode(nextRevision, declaration);
        final Set<PutOption> options;
        if (current == null) {
            options = ephemeral
                    ? Set.of(PutOption.IfRecordDoesNotExist, PutOption.AsEphemeralRecord)
                    : Set.of(PutOption.IfRecordDoesNotExist);
        } else {
            options = ephemeral
                    ? Set.of(PutOption.IfVersionIdEquals(current.versionId()), PutOption.AsEphemeralRecord)
                    : Set.of(PutOption.IfVersionIdEquals(current.versionId()));
        }
        try {
            putExact(key, value, options);
            return new Publication(nextRevision, declaration);
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
            final Stored observed = read(key, declaration.workerId());
            if (observed != null && exact(observed, nextRevision, declaration, value)) {
                return observed.publication();
            }
            throw new IllegalStateException("protocol capability CAS lost", conflict);
        } catch (RuntimeException responseFailure) {
            final Stored observed = read(key, declaration.workerId());
            if (observed != null && exact(observed, nextRevision, declaration, value)) {
                return observed.publication();
            }
            throw responseFailure;
        }
    }

    @Override
    public Optional<Publication> current(final String workerId) {
        final String canonicalWorkerId = canonicalWorkerId(workerId);
        requireSession();
        final Stored current = read(workerKey(canonicalWorkerId), canonicalWorkerId);
        return current == null ? Optional.empty() : Optional.of(current.publication());
    }

    @Override
    public boolean withdraw(final Publication expected) {
        Objects.requireNonNull(expected, "expected");
        requireDeclarationSession(expected.declaration());
        final String workerId = expected.declaration().workerId();
        final String key = workerKey(workerId);
        requireSession();
        final Stored current = read(key, workerId);
        if (current == null || !current.publication().sameIdentity(expected)) {
            return false;
        }
        try {
            return client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(current.versionId())));
        } catch (UnexpectedVersionIdException conflict) {
            return false;
        } catch (RuntimeException responseFailure) {
            final Stored observed = read(key, workerId);
            if (observed == null) {
                return true;
            }
            if (!observed.publication().sameIdentity(expected)) {
                return false;
            }
            throw responseFailure;
        }
    }

    private Stored read(final String key, final String expectedWorkerId) {
        requireSession();
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia protocol capability response has an invalid record identity");
        }
        try {
            final Decoded decoded = decode(result.value());
            if (!expectedWorkerId.equals(decoded.publication().declaration().workerId())) {
                throw new IllegalStateException("Oxia protocol capability belongs to another Worker");
            }
            return new Stored(decoded.publication(), result.version().versionId(), result.value());
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Oxia protocol capability record is malformed", malformed);
        }
    }

    private void putExact(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final PutResult result = client.put(key, value, options);
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia protocol capability put returned no exact version");
        }
    }

    private void requireDeclarationSession(final ProtocolCapabilityDeclarationV1 declaration) {
        if (!ephemeral) {
            return;
        }
        final byte[] currentSession = sessionIdentity.get();
        if (currentSession == null || !Bytes.constantTimeEquals(currentSession, declaration.sessionIdentity())) {
            throw new IllegalStateException("protocol capability declaration is not bound to this Oxia session");
        }
    }

    private String workerKey(final String workerId) {
        return keyPrefix + "/worker/" + Bytes.hex(Bytes.sha256(Bytes.utf8(workerId)));
    }

    private void requireSession() {
        sessionCheck.run();
    }

    private static boolean exact(
            final Stored observed,
            final long revision,
            final ProtocolCapabilityDeclarationV1 declaration,
            final byte[] value) {
        return observed.publication().revision() == revision
                && Arrays.equals(observed.publication().declaration().canonicalBytes(), declaration.canonicalBytes())
                && Arrays.equals(observed.value(), value);
    }

    private static byte[] encode(final long revision, final ProtocolCapabilityDeclarationV1 declaration) {
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, revision);
            CanonicalProtobuf.bytes(output, 3, declaration.canonicalBytes());
        });
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(body);
            CanonicalProtobuf.bytes(output, 4, Bytes.sha256(DIGEST_DOMAIN, body));
        });
    }

    private static Decoded decode(final byte[] encoded) {
        if (encoded.length == 0 || encoded.length > MAX_RECORD_BYTES) {
            throw new IllegalStateException("Oxia protocol capability record exceeds its bound");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = fields(encoded);
        requireNumbers(fields, 1, 2, 3, 4);
        if (uint(fields.get(0), 1) != RECORD_VERSION) {
            throw new IllegalStateException("unsupported Oxia protocol capability record version");
        }
        final long revision = uint(fields.get(1), 2);
        if (revision == 0) {
            throw new IllegalStateException("Oxia protocol capability revision is zero");
        }
        final byte[] declarationBytes = bytes(fields.get(2), 3);
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, revision);
            CanonicalProtobuf.bytes(output, 3, declarationBytes);
        });
        if (!Bytes.constantTimeEquals(fixed(fields.get(3), 4, 32), Bytes.sha256(DIGEST_DOMAIN, body))) {
            throw new IllegalStateException("Oxia protocol capability record digest mismatch");
        }
        final ProtocolCapabilityDeclarationV1 declaration = ProtocolCapabilityDeclarationV1.decode(declarationBytes);
        if (!Arrays.equals(encoded, encode(revision, declaration))) {
            throw new IllegalStateException("Oxia protocol capability record is not canonical");
        }
        return new Decoded(new Publication(revision, declaration));
    }

    private static List<CanonicalProtobuf.Reader.Field> fields(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> result = new ArrayList<>();
        while (reader.hasRemaining()) {
            result.add(reader.next());
        }
        return result;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int... numbers) {
        if (fields.size() != numbers.length) {
            throw new IllegalArgumentException("Oxia protocol capability record field count mismatch");
        }
        for (int index = 0; index < numbers.length; index++) {
            if (fields.get(index).number() != numbers[index]) {
                throw new IllegalArgumentException("Oxia protocol capability record field order mismatch");
            }
        }
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Oxia protocol capability bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "Oxia protocol capability field " + number);
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Oxia protocol capability uint field " + number);
        }
        return field.unsignedValue();
    }

    private static String canonicalWorkerId(final String value) {
        Objects.requireNonNull(value, "workerId");
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("workerId must be nonblank NFC UTF-8");
        }
        return value;
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

        boolean delete(String key, Set<DeleteOption> options) throws UnexpectedVersionIdException;
    }

    private record Stored(Publication publication, long versionId, byte[] value) {
        private Stored {
            value = Bytes.copy(value);
        }

        @Override
        public byte[] value() {
            return Bytes.copy(value);
        }
    }

    private record Decoded(Publication publication) {}

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
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            return delegate.delete(key, options);
        }
    }

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

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            sessionCheck.run();
            try {
                final boolean deleted = delegate.delete(key, options);
                sessionCheck.run();
                return deleted;
            } catch (UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }
    }
}
