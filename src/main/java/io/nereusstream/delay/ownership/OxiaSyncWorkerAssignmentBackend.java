package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ShardId;
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
 * Oxia revision-CAS backend for the desired Worker assignment of one shard.
 *
 * <p>The assignment record is durable, while the optional session gate makes
 * a placement controller stop publishing as soon as its Oxia session is
 * fenced.  Owner Lease acquisition remains a separate ephemeral CAS.</p>
 */
public final class OxiaSyncWorkerAssignmentBackend implements WorkerAssignmentAuthority {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final int MAX_RECORD_BYTES = 256 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8(
            "nereus-delay-oxia-worker-assignment-record-v1\0");

    private final RecordClient client;
    private final String keyPrefix;
    private final Runnable sessionCheck;

    /** Creates an unsessioned backend for deterministic tests or an external controller. */
    public OxiaSyncWorkerAssignmentBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix, () -> {
        });
    }

    /** Creates a backend fenced by an existing session-bound Oxia client handle. */
    public OxiaSyncWorkerAssignmentBackend(final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
                                           final String keyPrefix) {
        this(new SyncRecordClient(Objects.requireNonNull(handle, "handle").client()), keyPrefix,
                () -> handle.sessionIdentity());
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncWorkerAssignmentBackend(final RecordClient client, final String keyPrefix) {
        this(client, keyPrefix, () -> {
        });
    }

    private OxiaSyncWorkerAssignmentBackend(final RecordClient client, final String keyPrefix,
                                            final Runnable sessionCheck) {
        this.client = Objects.requireNonNull(client, "client");
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
        this.sessionCheck = Objects.requireNonNull(sessionCheck, "sessionCheck");
    }

    @Override
    public Publication publish(final WorkerAssignment assignment, final long expectedRevision) {
        Objects.requireNonNull(assignment, "assignment");
        requireExpectedRevision(expectedRevision);
        final String key = assignmentKey(assignment.sourceAssignment().shardId());
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            requireSession();
            final Stored current = read(key, assignment.sourceAssignment().shardId());
            final long observedRevision = current == null ? 0 : current.publication().revision();
            if (observedRevision != expectedRevision) {
                throw new IllegalStateException("worker assignment revision changed");
            }
            if (current != null && current.publication().assignment().sameIdentity(assignment)) {
                return current.publication();
            }
            validateEpochSuccessor(current == null ? null : current.publication(), assignment);
            final long nextRevision;
            try {
                nextRevision = Math.addExact(expectedRevision, 1);
            } catch (ArithmeticException overflow) {
                throw new IllegalStateException("worker assignment publication revision exhausted", overflow);
            }
            final byte[] value = encode(nextRevision, assignment);
            final Set<PutOption> options = current == null ? Set.of(PutOption.IfRecordDoesNotExist)
                    : Set.of(PutOption.IfVersionIdEquals(current.versionId()));
            try {
                putExact(key, value, options);
                return new Publication(nextRevision, assignment);
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
                final Stored observed = read(key, assignment.sourceAssignment().shardId());
                if (observed != null && exact(observed, nextRevision, assignment, value)) {
                    return observed.publication();
                }
                throw new IllegalStateException("worker assignment CAS lost", conflict);
            } catch (RuntimeException responseFailure) {
                final Stored observed = read(key, assignment.sourceAssignment().shardId());
                if (observed != null && exact(observed, nextRevision, assignment, value)) {
                    return observed.publication();
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("worker assignment CAS did not converge");
    }

    @Override
    public Optional<Publication> current(final ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId");
        requireSession();
        final Stored current = read(assignmentKey(shardId), shardId);
        return current == null ? Optional.empty() : Optional.of(current.publication());
    }

    @Override
    public boolean withdraw(final Publication expected) {
        Objects.requireNonNull(expected, "expected");
        final ShardId shardId = expected.assignment().sourceAssignment().shardId();
        final String key = assignmentKey(shardId);
        requireSession();
        final Stored current = read(key, shardId);
        if (current == null || current.publication().revision() != expected.revision()
                || !current.publication().assignment().sameIdentity(expected.assignment())) {
            return false;
        }
        try {
            if (client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(current.versionId())))) {
                return true;
            }
            return false;
        } catch (UnexpectedVersionIdException conflict) {
            return false;
        } catch (RuntimeException responseFailure) {
            final Stored observed = read(key, shardId);
            if (observed == null) {
                return true;
            }
            if (!observed.publication().assignment().sameIdentity(expected.assignment())
                    || observed.publication().revision() != expected.revision()) {
                return false;
            }
            throw responseFailure;
        }
    }

    private Stored read(final String key, final ShardId shardId) {
        requireSession();
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia Worker assignment response has an invalid record identity");
        }
        final Decoded decoded;
        try {
            decoded = decode(result.value());
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Oxia Worker assignment record contains malformed values", malformed);
        }
        if (!shardId.equals(decoded.publication().assignment().sourceAssignment().shardId())) {
            throw new IllegalStateException("Oxia Worker assignment belongs to another shard");
        }
        return new Stored(decoded.publication(), result.version().versionId(), result.value());
    }

    private void putExact(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final PutResult result = client.put(key, value, options);
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia Worker assignment put returned no exact version");
        }
    }

    private static boolean exact(final Stored observed, final long revision, final WorkerAssignment assignment,
                                 final byte[] value) {
        return observed.publication().revision() == revision
                && observed.publication().assignment().sameIdentity(assignment)
                && Arrays.equals(observed.value(), value);
    }

    private static byte[] encode(final long revision, final WorkerAssignment assignment) {
        if (revision <= 0) {
            throw new IllegalArgumentException("assignment record revision must be positive");
        }
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, revision);
            CanonicalProtobuf.bytes(output, 3, assignment.canonicalBytes());
        });
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(body);
            CanonicalProtobuf.bytes(output, 4, Bytes.sha256(DIGEST_DOMAIN, body));
        });
    }

    private static Decoded decode(final byte[] encoded) {
        if (encoded.length == 0 || encoded.length > MAX_RECORD_BYTES) {
            throw new IllegalStateException("Oxia Worker assignment record exceeds its bound");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = fields(encoded);
        requireNumbers(fields, 1, 2, 3, 4);
        if (uint(fields.get(0), 1) != RECORD_VERSION) {
            throw new IllegalStateException("unsupported Oxia Worker assignment record version");
        }
        final long revision = uint(fields.get(1), 2);
        if (revision == 0) {
            throw new IllegalStateException("Oxia Worker assignment record revision is zero");
        }
        final byte[] assignmentBytes = bytes(fields.get(2), 3);
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, revision);
            CanonicalProtobuf.bytes(output, 3, assignmentBytes);
        });
        if (!Bytes.constantTimeEquals(fixed(fields.get(3), 4, 32), Bytes.sha256(DIGEST_DOMAIN, body))) {
            throw new IllegalStateException("Oxia Worker assignment record digest mismatch");
        }
        final WorkerAssignment assignment = WorkerAssignment.decode(assignmentBytes);
        if (!Arrays.equals(encoded, encode(revision, assignment))) {
            throw new IllegalStateException("Oxia Worker assignment record is not canonical");
        }
        return new Decoded(new Publication(revision, assignment));
    }

    private String assignmentKey(final ShardId shardId) {
        return keyPrefix + "/assignment/" + Bytes.hex(Bytes.concat(shardId.routeIncarnation().bytes(),
                Bytes.u32beBits(shardId.partition())));
    }

    private void requireSession() {
        sessionCheck.run();
    }

    private static void validateEpochSuccessor(final Publication current, final WorkerAssignment next) {
        if (current != null && Long.compareUnsigned(next.placementEpoch(),
                current.assignment().placementEpoch()) <= 0) {
            throw new IllegalArgumentException("replacement assignment epoch is not newer");
        }
    }

    private static void requireExpectedRevision(final long expectedRevision) {
        if (expectedRevision < 0) {
            throw new IllegalArgumentException("expected assignment revision must be non-negative");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> fields(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> result = new ArrayList<>();
        while (reader.hasRemaining()) {
            result.add(reader.next());
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Oxia Worker assignment record is empty");
        }
        return result;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int... numbers) {
        if (fields.size() != numbers.length) {
            throw new IllegalArgumentException("Oxia Worker assignment record field count mismatch");
        }
        for (int index = 0; index < numbers.length; index++) {
            if (fields.get(index).number() != numbers[index]) {
                throw new IllegalArgumentException("Oxia Worker assignment record field order mismatch");
            }
        }
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Oxia Worker assignment bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "Oxia Worker assignment field " + number);
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Oxia Worker assignment uint field " + number);
        }
        return field.unsignedValue();
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
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

    private record Decoded(Publication publication) {
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
        public boolean delete(final String key, final Set<DeleteOption> options)
                throws UnexpectedVersionIdException {
            return delegate.delete(key, options);
        }
    }
}
