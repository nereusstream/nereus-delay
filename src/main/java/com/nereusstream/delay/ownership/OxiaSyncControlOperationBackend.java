package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ControlOperationQueryResponse;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationStateTransition;
import com.nereusstream.delay.protocol.CurrentControlOperation;
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
import java.util.Set;

/**
 * Durable Oxia CAS backend for one Control Operation state record.
 *
 * <p>The complete receipt and CURRENT projection are stored in the same
 * canonical value and advanced with one Oxia version CAS. A response-loss
 * retry is accepted only after an exact reread of the requested successor;
 * this backend never reconstructs a target set from a partial response.</p>
 *
 * <p>Authenticated actor/scope authorization and source-ordered registration
 * remain above this record surface. The handle-bound constructor additionally
 * fences every record I/O to the exact ephemeral Oxia session. Cross-record
 * transactions remain above this record surface. The
 * {@link OxiaControlOperationAuthority} adapter validates the response
 * projection before exposing it to callers.</p>
 */
public final class OxiaSyncControlOperationBackend implements OxiaControlOperationAuthority.CasBackend {
    private static final int SNAPSHOT_VERSION = 1;
    private static final int DIGEST_LENGTH = 32;
    private static final int MAX_COMPONENT_BYTES = 8 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-control-operation\0");

    private final RecordClient client;
    private final String keyPrefix;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncControlOperationBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Creates a backend fenced to the exact ephemeral session of a handle. */
    public OxiaSyncControlOperationBackend(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle, final String keyPrefix) {
        this(
                new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()),
                keyPrefix,
                handle.backend()::assertConnectedSession);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncControlOperationBackend(final RecordClient client, final String keyPrefix) {
        this(client, keyPrefix, () -> {});
    }

    /** Package-private constructor used to exercise the session fence. */
    OxiaSyncControlOperationBackend(final RecordClient client, final String keyPrefix, final Runnable sessionCheck) {
        this.client = new SessionBoundRecordClient(
                Objects.requireNonNull(client, "client"), Objects.requireNonNull(sessionCheck, "sessionCheck"));
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
    }

    @Override
    public ControlOperationQueryResponse register(
            final ControlOperationReceipt receipt, final CurrentControlOperation initial) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(initial, "initial");
        if (!matchesIdentity(receipt, initial) || initial.operationRevision() != receipt.operationRevision()) {
            return ControlOperationQueryResponse.integrityError();
        }
        final String key = operationKey(receipt.operationId());
        final Entry existing = read(key, receipt.operationId());
        if (existing != null) {
            return classifyRegistration(existing, receipt, initial);
        }
        final byte[] value = encode(receipt, initial);
        try {
            putExact(key, value, Set.of(PutOption.IfRecordDoesNotExist));
            return ControlOperationQueryResponse.current(initial);
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final Entry observed = read(key, receipt.operationId());
            return observed == null
                    ? ControlOperationQueryResponse.integrityError()
                    : classifyRegistration(observed, receipt, initial);
        } catch (RuntimeException responseFailure) {
            final Entry observed = read(key, receipt.operationId());
            if (observed != null && exact(observed, receipt, initial)) {
                return ControlOperationQueryResponse.current(observed.current());
            }
            throw responseFailure;
        }
    }

    @Override
    public ControlOperationQueryResponse advance(
            final ControlOperationReceipt receipt, final long expectedRevision, final CurrentControlOperation next) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(next, "next");
        if (expectedRevision <= 0 || expectedRevision == Long.MAX_VALUE) {
            return expectedRevision <= 0
                    ? ControlOperationQueryResponse.invalidReceipt()
                    : ControlOperationQueryResponse.integrityError();
        }
        if (!matchesIdentity(receipt, next) || !isExactSuccessor(expectedRevision, next.operationRevision())) {
            return ControlOperationQueryResponse.integrityError();
        }
        final String key = operationKey(receipt.operationId());
        final Entry existing = read(key, receipt.operationId());
        if (existing == null || !existing.receipt().equals(receipt)) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        try {
            validateTransition(existing.current(), next);
        } catch (IllegalArgumentException invalidTransition) {
            return ControlOperationQueryResponse.integrityError();
        }
        if (existing.current().equals(next)) {
            return ControlOperationQueryResponse.current(existing.current());
        }
        if (existing.current().operationRevision() != expectedRevision) {
            return ControlOperationQueryResponse.integrityError();
        }
        final byte[] value = encode(receipt, next);
        try {
            putExact(key, value, Set.of(PutOption.IfVersionIdEquals(existing.versionId())));
            return ControlOperationQueryResponse.current(next);
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final Entry observed = read(key, receipt.operationId());
            if (observed != null && exact(observed, receipt, next)) {
                return ControlOperationQueryResponse.current(observed.current());
            }
            return ControlOperationQueryResponse.integrityError();
        } catch (RuntimeException responseFailure) {
            final Entry observed = read(key, receipt.operationId());
            if (observed != null && exact(observed, receipt, next)) {
                return ControlOperationQueryResponse.current(observed.current());
            }
            throw responseFailure;
        }
    }

    @Override
    public ControlOperationQueryResponse query(final ControlOperationReceipt receipt, final long nowEpochMs) {
        if (receipt == null || nowEpochMs < 0) {
            return ControlOperationQueryResponse.invalidReceipt();
        }
        final Entry existing = read(operationKey(receipt.operationId()), receipt.operationId());
        if (existing == null || !existing.receipt().equals(receipt) || nowEpochMs > receipt.queryUntilEpochMs()) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        return ControlOperationQueryResponse.current(existing.current());
    }

    private static ControlOperationQueryResponse classifyRegistration(
            final Entry existing, final ControlOperationReceipt receipt, final CurrentControlOperation initial) {
        if (!existing.receipt().equals(receipt)) {
            return ControlOperationQueryResponse.notFoundOrNotAuthorized();
        }
        if (existing.current().equals(initial)) {
            return ControlOperationQueryResponse.current(existing.current());
        }
        return ControlOperationQueryResponse.integrityError();
    }

    private static void validateTransition(final CurrentControlOperation current, final CurrentControlOperation next) {
        try {
            ControlOperationStateTransition.validate(current.state(), next.state());
            ControlOperationStateTransition.validateTargets(current.targetStates(), next.targetStates());
        } catch (IllegalArgumentException invalidTransition) {
            throw new IllegalArgumentException("invalid Control Operation state transition", invalidTransition);
        }
    }

    private Entry read(final String key, final byte[] operationId) {
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia control operation response has an invalid record identity");
        }
        return decode(result.value(), operationId, result.version().versionId());
    }

    private void putExact(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final PutResult result = client.put(key, value, options);
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia control operation put returned no exact version");
        }
    }

    private String operationKey(final byte[] operationId) {
        Bytes.requireLength(operationId, 32, "operationId");
        return keyPrefix + "/operation/" + Bytes.hex(operationId);
    }

    private static boolean exact(
            final Entry entry, final ControlOperationReceipt receipt, final CurrentControlOperation current) {
        return entry.receipt().equals(receipt) && entry.current().equals(current);
    }

    private static boolean matchesIdentity(
            final ControlOperationReceipt receipt, final CurrentControlOperation current) {
        return Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                && Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                && Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash());
    }

    private static boolean isExactSuccessor(final long expectedRevision, final long nextRevision) {
        return expectedRevision > 0 && expectedRevision < Long.MAX_VALUE && nextRevision == expectedRevision + 1;
    }

    private static byte[] encode(final ControlOperationReceipt receipt, final CurrentControlOperation current) {
        final byte[] receiptBytes = receipt.frame();
        final byte[] currentBytes = current.canonicalBytes();
        checkComponentLength(receiptBytes, "receipt");
        checkComponentLength(currentBytes, "current");
        final byte[] digest = digest(receiptBytes, currentBytes);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SNAPSHOT_VERSION);
            CanonicalProtobuf.bytes(output, 2, receiptBytes);
            CanonicalProtobuf.bytes(output, 3, currentBytes);
            CanonicalProtobuf.bytes(output, 4, digest);
        });
    }

    private static Entry decode(final byte[] encoded, final byte[] operationId, final long versionId) {
        if (encoded == null || encoded.length > MAX_COMPONENT_BYTES * 2L + 256) {
            throw new IllegalStateException("Oxia control operation record exceeds bounded size");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        if (uint(version, 1) != SNAPSHOT_VERSION) {
            throw new IllegalStateException("unsupported Oxia control operation record version");
        }
        final byte[] receiptBytes = bytes(next(reader, 2), 2);
        final byte[] currentBytes = bytes(next(reader, 3), 3);
        final byte[] digest = bytes(next(reader, 4), 4);
        if (reader.hasRemaining() || !Bytes.constantTimeEquals(digest, digest(receiptBytes, currentBytes))) {
            throw new IllegalStateException("Oxia control operation record is non-canonical or corrupt");
        }
        try {
            final ControlOperationReceipt receipt = ControlOperationReceipt.decodeFrame(receiptBytes);
            final CurrentControlOperation current = CurrentControlOperation.decode(currentBytes);
            if (!Bytes.constantTimeEquals(operationId, receipt.operationId())
                    || !matchesIdentity(receipt, current)
                    || !Arrays.equals(encoded, encode(receipt, current))) {
                throw new IllegalStateException("Oxia control operation record identity mismatch");
            }
            return new Entry(receipt, current, versionId);
        } catch (RuntimeException malformed) {
            if (malformed instanceof IllegalStateException stateFailure
                    && "Oxia control operation record identity mismatch".equals(stateFailure.getMessage())) {
                throw stateFailure;
            }
            throw new IllegalStateException("Oxia control operation record contains malformed values", malformed);
        }
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalStateException("missing Oxia control operation record field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalStateException("unexpected Oxia control operation record field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalStateException("invalid Oxia control operation bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalStateException("invalid Oxia control operation varint field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] digest(final byte[] receiptBytes, final byte[] currentBytes) {
        return Bytes.sha256(DIGEST_DOMAIN, receiptBytes, currentBytes);
    }

    private static void checkComponentLength(final byte[] value, final String name) {
        if (value == null || value.length == 0 || value.length > MAX_COMPONENT_BYTES) {
            throw new IllegalArgumentException(name + " exceeds Oxia control operation limit");
        }
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

    private record Entry(ControlOperationReceipt receipt, CurrentControlOperation current, long versionId) {
        private Entry {
            Objects.requireNonNull(receipt, "receipt");
            Objects.requireNonNull(current, "current");
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
     * successful CAS whose response is lost after the marker disappears is
     * therefore never converted into a guessed operation result.
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
