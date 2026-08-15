package io.nereusstream.delay.ownership;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Oxia implementation of the owner-lease CAS surface.
 *
 * <p>The lease record is an Oxia ephemeral record.  A separate durable epoch
 * record is incremented with version CAS before the ephemeral record is
 * created; losing a race may consume an epoch, but it can never reuse one.
 * The public client constructor does not own or close its client. The
 * {@link #connect} factory creates an ephemeral session marker and returns a
 * {@link ClientHandle} that owns the connected client.</p>
 *
 * <p>The backend is deliberately below {@link OxiaOwnerLeaseStore}: the
 * latter remains responsible for validating the response against the V1
 * fencing contract.  A response lost after a successful Oxia write is
 * propagated as an exception rather than guessed as a successful CAS.</p>
 */
public final class OxiaSyncOwnerLeaseBackend implements OxiaOwnerLeaseStore.LeaseCasBackend {
    private static final int MAX_EPOCH_CAS_ATTEMPTS = 32;
    private static final byte[] SESSION_DOMAIN = Bytes.utf8(
            "nereus-delay-oxia-session-identity-v1\0");

    private final RecordClient client;
    private final String keyPrefix;
    private final SessionMarker sessionMarker;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncOwnerLeaseBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix, false);
    }

    /**
     * Convenience factory that creates a client with Oxia's ephemeral-session
     * support enabled. The returned handle owns the client and exposes the
     * session-derived identity for context-bound owner leases.
     */
    public static ClientHandle connect(final String serviceAddress, final String namespace,
                                       final String clientIdentifier, final String keyPrefix)
            throws OxiaException {
        return connect(serviceAddress, namespace, clientIdentifier, Duration.ofSeconds(15), keyPrefix);
    }

    /** Runtime-friendly wrapper for opt-in integration launchers. */
    public static ClientHandle connectUnchecked(final String serviceAddress, final String namespace,
                                                final String clientIdentifier, final Duration sessionTimeout,
                                                final String keyPrefix) {
        try {
            return connect(serviceAddress, namespace, clientIdentifier, sessionTimeout, keyPrefix);
        } catch (OxiaException failure) {
            throw new IllegalStateException("cannot connect to Oxia owner authority", failure);
        }
    }

    /** Creates a client with an explicit Oxia ephemeral-session timeout. */
    public static ClientHandle connect(final String serviceAddress, final String namespace,
                                       final String clientIdentifier, final Duration sessionTimeout,
                                       final String keyPrefix) throws OxiaException {
        Objects.requireNonNull(serviceAddress, "serviceAddress");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(clientIdentifier, "clientIdentifier");
        Objects.requireNonNull(sessionTimeout, "sessionTimeout");
        if (sessionTimeout.isZero() || sessionTimeout.isNegative()) {
            throw new IllegalArgumentException("sessionTimeout must be positive");
        }
        final SyncOxiaClient client = OxiaClientBuilder.create(serviceAddress)
                .namespace(canonicalText(namespace, "namespace"))
                .clientIdentifier(canonicalText(clientIdentifier, "clientIdentifier"))
                .sessionTimeout(sessionTimeout)
                .syncClient();
        try {
            return new ClientHandle(client, new OxiaSyncOwnerLeaseBackend(client, keyPrefix, true));
        } catch (RuntimeException failure) {
            try {
                client.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncOwnerLeaseBackend(final RecordClient client, final String keyPrefix) {
        this(client, keyPrefix, false);
    }

    private OxiaSyncOwnerLeaseBackend(final SyncOxiaClient client, final String keyPrefix,
                                      final boolean establishSession) {
        this(new SyncRecordClient(client), keyPrefix, establishSession);
    }

    private OxiaSyncOwnerLeaseBackend(final RecordClient client, final String keyPrefix,
                                      final boolean establishSession) {
        this.client = Objects.requireNonNull(client, "client");
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
        this.sessionMarker = establishSession ? establishSessionMarker() : null;
    }

    /** Returns the exact session-derived identity for a client created by {@link #connect}. */
    public byte[] connectedSessionIdentity() {
        if (sessionMarker == null) {
            throw new IllegalStateException("backend was not connected with an Oxia session marker");
        }
        requireConnectedSession();
        return Bytes.copy(sessionMarker.identity());
    }

    /**
     * Verifies that the connected client still owns the exact ephemeral
     * session marker established by {@link #connect}. Callers that use the
     * durable Gateway records must fail closed when this check does not pass;
     * a new client/session has to be composed explicitly for recovery.
     */
    public void assertConnectedSession() {
        if (sessionMarker == null) {
            throw new IllegalStateException("backend was not connected with an Oxia session marker");
        }
        requireConnectedSession();
    }

    /**
     * Derives the 32-byte V1 session identity from the metadata attached to an
     * Oxia ephemeral record.  Callers of context-bound acquisition should pass
     * this value, not a process-local random value.
     */
    public static byte[] sessionIdentity(final Version version) {
        Objects.requireNonNull(version, "version");
        final long sessionId = version.sessionId().orElseThrow(
                () -> new IllegalArgumentException("Oxia lease is not bound to a session"));
        final String clientIdentifier = canonicalText(version.clientIdentifier().orElseThrow(
                () -> new IllegalArgumentException("Oxia lease has no client identity")),
                "clientIdentifier");
        if (sessionId < 0) {
            throw new IllegalArgumentException("Oxia session id must be non-negative");
        }
        return Bytes.sha256(SESSION_DOMAIN, Bytes.u64be(sessionId),
                Bytes.lp32(Bytes.utf8(clientIdentifier)));
    }

    @Override
    public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                        final long nowEpochMs, final long leaseDurationMs) {
        validateRequest(shardId, ownerId, nowEpochMs, leaseDurationMs);
        return acquireInternal(shardId, ownerId, nowEpochMs, leaseDurationMs, null);
    }

    @Override
    public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                        final byte[] sessionIdentity, final long nowEpochMs,
                                        final long leaseDurationMs) {
        Objects.requireNonNull(assignment, "assignment");
        requireConnectedSession(sessionIdentity);
        final OwnerLeaseContext context = new OwnerLeaseContext(assignment.assignmentId(),
                assignment.assignmentEpoch(), sessionIdentity);
        validateRequest(assignment.shardId(), ownerId, nowEpochMs, leaseDurationMs);
        return acquireInternal(assignment.shardId(), ownerId, nowEpochMs, leaseDurationMs, context);
    }

    @Override
    public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                      final long leaseDurationMs) {
        Objects.requireNonNull(expected, "expected");
        requireConnectedSession(expected.sessionIdentity());
        validateRequest(expected.shardId(), expected.ownerId(), nowEpochMs, leaseDurationMs);
        final StoredLease current = readLease(expected.shardId());
        if (current == null || !expected.sameIdentity(current.lease)
                || !expected.validAt(nowEpochMs)) {
            return Optional.empty();
        }
        final long expiresAt;
        try {
            expiresAt = Math.addExact(nowEpochMs, leaseDurationMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("lease expiry overflows epoch milliseconds", overflow);
        }
        final OwnerLease renewed = new OwnerLease(expected.shardId(), expected.ownerId(), expected.ownerEpoch(),
                expected.leaseToken(), expiresAt, expected.context(), expected.state());
        return putLease(renewed, current.versionId);
    }

    @Override
    public boolean release(final OwnerLease expected) {
        Objects.requireNonNull(expected, "expected");
        requireConnectedSession(expected.sessionIdentity());
        final StoredLease current = readLease(expected.shardId());
        if (current == null || !expected.sameIdentity(current.lease)) {
            return false;
        }
        try {
            return client.delete(leaseKey(expected.shardId()),
                    Set.of(DeleteOption.IfVersionIdEquals(current.versionId)));
        } catch (UnexpectedVersionIdException lostRace) {
            return false;
        } catch (RuntimeException failure) {
            // A successful delete can lose its response. The only definitive
            // success is an exact reread proving that the lease record is
            // absent; a replacement owner or a still-present identity keeps
            // the release outcome unknown/fenced.
            try {
                final StoredLease observed = readLease(expected.shardId());
                if (observed == null) {
                    return true;
                }
                if (!expected.sameIdentity(observed.lease)) {
                    return false;
                }
            } catch (RuntimeException rereadFailure) {
                failure.addSuppressed(rereadFailure);
            }
            throw failure;
        }
    }

    @Override
    public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
        Objects.requireNonNull(expected, "expected");
        requireConnectedSession(expected.sessionIdentity());
        Objects.requireNonNull(nextState, "nextState");
        if (!expected.state().canTransitionTo(nextState)) {
            return Optional.empty();
        }
        final StoredLease current = readLease(expected.shardId());
        if (current == null || !expected.sameIdentity(current.lease)) {
            return Optional.empty();
        }
        final OwnerLease transitioned = new OwnerLease(expected.shardId(), expected.ownerId(), expected.ownerEpoch(),
                expected.leaseToken(), expected.expiresAtEpochMs(), expected.context(), nextState);
        return putLease(transitioned, current.versionId);
    }

    @Override
    public Optional<OwnerLease> current(final ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId");
        final StoredLease current = readLease(shardId);
        return current == null ? Optional.empty() : Optional.of(current.lease);
    }

    private SessionMarker establishSessionMarker() {
        final byte[] challenge = randomToken();
        final String key = keyPrefix + "/session/" + Bytes.hex(challenge);
        final PutResult result;
        try {
            result = client.put(key, challenge,
                    Set.of(PutOption.IfRecordDoesNotExist, PutOption.AsEphemeralRecord));
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException failure) {
            throw new IllegalStateException("Oxia owner session marker CAS failed", failure);
        }
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia owner session marker put returned an invalid identity");
        }
        final GetResult observed = client.get(key);
        if (observed == null || !key.equals(observed.key()) || observed.value() == null
                || !Arrays.equals(challenge, observed.value()) || observed.version() == null
                || observed.version().versionId() != result.version().versionId()) {
            throw new IllegalStateException("Oxia owner session marker reread is not exact");
        }
        validateEphemeralVersion(observed.version(), null);
        return new SessionMarker(key, challenge, observed.version(), sessionIdentity(observed.version()));
    }

    private void requireConnectedSession(final byte[] requestedIdentity) {
        if (sessionMarker == null || requestedIdentity == null) {
            return;
        }
        requireConnectedSession();
        if (!Bytes.constantTimeEquals(requestedIdentity, sessionMarker.identity())) {
            throw new IllegalStateException("owner lease request is bound to another Oxia session");
        }
    }

    private void requireConnectedSession() {
        if (sessionMarker == null) {
            return;
        }
        final GetResult observed = client.get(sessionMarker.key());
        if (observed == null || !sessionMarker.key().equals(observed.key()) || observed.value() == null
                || !Arrays.equals(sessionMarker.challenge(), observed.value()) || observed.version() == null
                || observed.version().versionId() != sessionMarker.version().versionId()
                || !sameSessionMetadata(observed.version(), sessionMarker.version())) {
            throw new IllegalStateException("Oxia owner session marker is absent or changed");
        }
    }

    private static boolean sameSessionMetadata(final Version left, final Version right) {
        return left.sessionId().equals(right.sessionId())
                && left.clientIdentifier().equals(right.clientIdentifier());
    }

    private Optional<OwnerLease> acquireInternal(final ShardId shardId, final String ownerId,
                                                 final long nowEpochMs, final long leaseDurationMs,
                                                 final OwnerLeaseContext context) {
        if (readLease(shardId) != null) {
            return Optional.empty();
        }
        final long expiresAt;
        try {
            expiresAt = Math.addExact(nowEpochMs, leaseDurationMs);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("lease expiry overflows epoch milliseconds", overflow);
        }
        final long ownerEpoch = allocateEpoch(shardId);
        final OwnerLease candidate = new OwnerLease(shardId, ownerId, ownerEpoch, randomToken(), expiresAt,
                context, ShardLifecycleState.ACQUIRING);
        PutResult result = null;
        try {
            result = client.put(leaseKey(shardId), encodeLease(candidate),
                    Set.of(PutOption.IfRecordDoesNotExist, PutOption.AsEphemeralRecord));
            validateLeasePutResult(result, leaseKey(shardId), context);
            final OwnerLease stored = decodeLease(resultValue(result, candidate));
            if (!candidate.sameIdentity(stored) || stored.state() != ShardLifecycleState.ACQUIRING) {
                throw new IllegalStateException("Oxia lease create response changed its identity");
            }
            return Optional.of(stored);
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException lostRace) {
            return Optional.empty();
        } catch (RuntimeException failure) {
            // A response can be lost after the ephemeral record committed.
            // Reread the exact candidate before deciding that acquisition is
            // unknown; this is the only safe way to avoid stranding a lease
            // that this caller actually owns.
            try {
                final StoredLease observed = readLease(shardId);
                if (observed != null && candidate.sameIdentity(observed.lease)
                        && observed.lease.state() == ShardLifecycleState.ACQUIRING
                        && observed.lease.expiresAtEpochMs() == candidate.expiresAtEpochMs()) {
                    return Optional.of(observed.lease);
                }
            } catch (RuntimeException rereadFailure) {
                failure.addSuppressed(rereadFailure);
            }
            // A malformed response must not strand an ephemeral record that
            // would block the next owner.  Cleanup is best effort; the
            // original integrity/transport failure remains authoritative.
            if (result != null && result.version() != null) {
                try {
                    client.delete(leaseKey(shardId),
                            Set.of(DeleteOption.IfVersionIdEquals(result.version().versionId())));
                } catch (RuntimeException | UnexpectedVersionIdException ignored) {
                    // The session may already have expired or another owner
                    // may have won after a malformed response.
                }
            }
            throw failure;
        }
    }

    private Optional<OwnerLease> putLease(final OwnerLease lease, final long versionId) {
        try {
            final PutResult result = client.put(leaseKey(lease.shardId()), encodeLease(lease),
                    Set.of(PutOption.IfVersionIdEquals(versionId), PutOption.AsEphemeralRecord));
            validateLeasePutResult(result, leaseKey(lease.shardId()), lease.context());
            final OwnerLease stored = decodeLease(resultValue(result, lease));
            if (!lease.sameIdentity(stored) || lease.state() != stored.state()
                    || lease.expiresAtEpochMs() != stored.expiresAtEpochMs()) {
                throw new IllegalStateException("Oxia lease CAS response changed its identity or state");
            }
            return Optional.of(stored);
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException lostRace) {
            return Optional.empty();
        } catch (RuntimeException failure) {
            // Renew/transition have the same response-loss rule as acquire:
            // only the exact candidate value observed after the failed write
            // may be reported as success. A stale value, a new owner, or a
            // reread failure leaves the original operation unknown.
            try {
                final StoredLease observed = readLease(lease.shardId());
                if (observed != null && lease.sameIdentity(observed.lease)
                        && observed.lease.state() == lease.state()
                        && observed.lease.expiresAtEpochMs() == lease.expiresAtEpochMs()) {
                    return Optional.of(observed.lease);
                }
            } catch (RuntimeException rereadFailure) {
                failure.addSuppressed(rereadFailure);
            }
            throw failure;
        }
    }

    private long allocateEpoch(final ShardId shardId) {
        final String key = epochKey(shardId);
        for (int attempt = 0; attempt < MAX_EPOCH_CAS_ATTEMPTS; attempt++) {
            final GetResult current = client.get(key);
            if (current == null) {
                try {
                    final byte[] expected = Bytes.u64be(1);
                    final PutResult created = client.put(key, expected, Set.of(PutOption.IfRecordDoesNotExist));
                    if (created == null || !key.equals(created.key()) || created.version() == null) {
                        throw new IllegalStateException("Oxia epoch create returned no version");
                    }
                    return 1;
                } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
                    continue;
                } catch (RuntimeException responseFailure) {
                    if (epochValueWasCommitted(key, Bytes.u64be(1))) {
                        return 1;
                    }
                    throw responseFailure;
                }
            }
            if (!key.equals(current.key()) || current.value() == null || current.version() == null) {
                throw new IllegalStateException("Oxia owner epoch response has an invalid record identity");
            }
            final long previous = decodeEpoch(current.value());
            if (previous == 0 || previous == -1L) {
                throw new IllegalStateException("Oxia owner epoch is exhausted or malformed");
            }
            final long next = previous + 1;
            final byte[] expected = Bytes.u64beBits(next);
            try {
                final PutResult updated = client.put(key, expected,
                        Set.of(PutOption.IfVersionIdEquals(current.version().versionId())));
                if (updated == null || !key.equals(updated.key()) || updated.version() == null) {
                    throw new IllegalStateException("Oxia epoch CAS returned no version");
                }
                return next;
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
                // Another worker won the version CAS.  Re-read and retry.
            } catch (RuntimeException responseFailure) {
                if (epochValueWasCommitted(key, expected)) {
                    return next;
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("Oxia owner epoch CAS did not converge");
    }

    private boolean epochValueWasCommitted(final String key, final byte[] expectedValue) {
        final GetResult observed = client.get(key);
        if (observed == null) {
            return false;
        }
        if (!key.equals(observed.key()) || observed.value() == null || observed.version() == null) {
            throw new IllegalStateException("Oxia owner epoch response has an invalid record identity");
        }
        return Arrays.equals(expectedValue, observed.value());
    }

    private StoredLease readLease(final ShardId shardId) {
        final String key = leaseKey(shardId);
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia owner lease response has an invalid record identity");
        }
        final OwnerLease lease = decodeLease(result.value());
        if (!shardId.equals(lease.shardId())) {
            throw new IllegalStateException("Oxia lease belongs to another shard");
        }
        validateEphemeralVersion(result.version(), lease.context());
        return new StoredLease(lease, result.version().versionId());
    }

    private void validateEphemeralVersion(final Version version, final OwnerLeaseContext context) {
        Objects.requireNonNull(version, "Oxia lease version");
        if (version.sessionId().isEmpty() || version.clientIdentifier().isEmpty()) {
            throw new IllegalStateException("Oxia owner lease is not an ephemeral session record");
        }
        if (context != null && !Bytes.constantTimeEquals(context.sessionIdentity(), sessionIdentity(version))) {
            throw new IllegalStateException("Oxia lease session identity does not match the request");
        }
    }

    private void validateLeasePutResult(final PutResult result, final String expectedKey,
                                        final OwnerLeaseContext context) {
        if (result == null || !expectedKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia lease put returned an invalid record identity");
        }
        validateEphemeralVersion(result.version(), context);
    }

    private static byte[] resultValue(final PutResult result, final OwnerLease fallback) {
        if (result == null || result.version() == null) {
            throw new IllegalStateException("Oxia lease put returned no version");
        }
        // The Oxia PutResult intentionally does not echo the value.  The
        // request bytes are canonical and have already passed the CAS, so the
        // response projection is the exact value that was submitted.
        return encodeLease(fallback);
    }

    private String leaseKey(final ShardId shardId) {
        return keyPrefix + "/lease/" + shardToken(shardId);
    }

    private String epochKey(final ShardId shardId) {
        return keyPrefix + "/epoch/" + shardToken(shardId);
    }

    private static String shardToken(final ShardId shardId) {
        return Bytes.hex(Bytes.concat(shardId.routeIncarnation().bytes(), Bytes.u32beBits(shardId.partition())));
    }

    private static byte[] randomToken() {
        final byte[] token = new byte[32];
        ThreadLocalRandom.current().nextBytes(token);
        return token;
    }

    private static byte[] encodeLease(final OwnerLease lease) {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, lease.shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, lease.shardId().partition());
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8(canonicalText(lease.ownerId(), "ownerId")));
            CanonicalProtobuf.uint64Bits(output, 4, lease.ownerEpoch());
            CanonicalProtobuf.bytes(output, 5, lease.leaseToken());
            CanonicalProtobuf.uint64Bits(output, 6, lease.expiresAtEpochMs());
            CanonicalProtobuf.uint32(output, 7, lease.state().wireValue());
            if (lease.context() != null) {
                CanonicalProtobuf.bytes(output, 8, lease.context().sourceAssignmentId());
                CanonicalProtobuf.uint64(output, 9, lease.context().assignmentEpoch());
                CanonicalProtobuf.bytes(output, 10, lease.context().sessionIdentity());
            }
        });
        return encoded;
    }

    private static OwnerLease decodeLease(final byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        final Map<Integer, CanonicalProtobuf.Reader.Field> fields = new HashMap<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(bytes);
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (fields.put(field.number(), field) != null) {
                throw new IllegalArgumentException("duplicate Oxia lease field");
            }
        }
        for (int number = 1; number <= 7; number++) {
            if (!fields.containsKey(number)) {
                throw new IllegalArgumentException("missing Oxia lease field " + number);
            }
        }
        final byte[] route = bytesField(fields, 1, 16);
        final long partition = uintField(fields, 2);
        if (partition > 0xffff_ffffL) {
            throw new IllegalArgumentException("Oxia lease partition is outside uint32");
        }
        final String ownerId = canonicalText(new String(bytesField(fields, 3, Integer.MAX_VALUE),
                StandardCharsets.UTF_8), "ownerId");
        final long ownerEpoch = uintField(fields, 4);
        if (ownerEpoch == 0) {
            throw new IllegalArgumentException("Oxia owner epoch must be non-zero");
        }
        final byte[] token = bytesField(fields, 5, 32);
        final long expiresAt = uintField(fields, 6);
        final ShardLifecycleState state = state(uintField(fields, 7));
        final boolean hasContext = fields.containsKey(8) || fields.containsKey(9) || fields.containsKey(10);
        final OwnerLeaseContext context;
        if (hasContext) {
            if (!fields.keySet().containsAll(Set.of(8, 9, 10))) {
                throw new IllegalArgumentException("Oxia lease context is incomplete");
            }
            context = new OwnerLeaseContext(bytesField(fields, 8, 32), uintField(fields, 9),
                    bytesField(fields, 10, 32));
        } else {
            context = null;
        }
        final OwnerLease lease = new OwnerLease(new ShardId(new RouteIncarnation(route), (int) partition), ownerId,
                ownerEpoch, token, expiresAt, context, state);
        if (!Arrays.equals(bytes, encodeLease(lease))) {
            throw new IllegalArgumentException("Oxia lease bytes are not canonical");
        }
        return lease;
    }

    private static long decodeEpoch(final byte[] bytes) {
        Bytes.requireLength(bytes, Long.BYTES, "owner epoch");
        return java.nio.ByteBuffer.wrap(bytes).getLong();
    }

    private static byte[] bytesField(final Map<Integer, CanonicalProtobuf.Reader.Field> fields,
                                     final int number, final int maxLength) {
        final CanonicalProtobuf.Reader.Field field = fields.get(number);
        if (field == null || field.wireType() != 2 || field.rawValue().length > maxLength) {
            throw new IllegalArgumentException("invalid Oxia lease bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uintField(final Map<Integer, CanonicalProtobuf.Reader.Field> fields, final int number) {
        final CanonicalProtobuf.Reader.Field field = fields.get(number);
        if (field == null || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Oxia lease varint field " + number);
        }
        return field.unsignedValue();
    }

    private static ShardLifecycleState state(final long value) {
        for (ShardLifecycleState candidate : ShardLifecycleState.values()) {
            if (candidate.wireValue() == value) {
                return candidate;
            }
        }
        throw new IllegalArgumentException("unknown Oxia lease lifecycle state");
    }

    private static void validateRequest(final ShardId shardId, final String ownerId, final long nowEpochMs,
                                        final long leaseDurationMs) {
        Objects.requireNonNull(shardId, "shardId");
        canonicalText(ownerId, "ownerId");
        if (nowEpochMs < 0 || leaseDurationMs <= 0) {
            throw new IllegalArgumentException("invalid Oxia owner lease request");
        }
    }

    private static String canonicalKeyPrefix(final String value) {
        final String canonical = canonicalText(value, "keyPrefix");
        if (canonical.endsWith("/") || canonical.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("keyPrefix must not end with '/'");
        }
        return canonical;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (!value.equals(new String(encoded, StandardCharsets.UTF_8)) || value.isBlank()
                || value.indexOf('\0') >= 0 || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    /** A connected client and its non-owning lease backend. */
    public record ClientHandle(SyncOxiaClient client, OxiaSyncOwnerLeaseBackend backend)
            implements Closeable {
        public ClientHandle {
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(backend, "backend");
        }

        /** Returns the exact session-derived identity for context-bound owner leases. */
        public byte[] sessionIdentity() {
            return backend.connectedSessionIdentity();
        }

        @Override
        public void close() throws IOException {
            try {
                client.close();
            } catch (Exception failure) {
                throw new IOException("cannot close Oxia client", failure);
            }
        }
    }

    /** Narrow record surface to keep deterministic tests independent of gRPC. */
    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;

        boolean delete(String key, Set<DeleteOption> options) throws UnexpectedVersionIdException;
    }

    private record StoredLease(OwnerLease lease, long versionId) {
    }

    private record SessionMarker(String key, byte[] challenge, Version version, byte[] identity) {
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
