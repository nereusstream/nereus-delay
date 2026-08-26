package com.nereusstream.delay.gateway;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Oxia single-record CAS implementation of the Gateway idempotency contract.
 *
 * <p>Every state transition rereads and version-CASes one canonical value. If
 * a transition response is lost, the exact successor may be reread, but an
 * ownership permit is never reconstructed from that reread.</p>
 */
public final class OxiaGatewayIdempotencyStore implements GatewayIdempotencyStore, AutoCloseable {
    private static final int MAX_RECORD_BYTES = 32 * 1024 * 1024;
    private static final String RECORD_SUFFIX = "/idempotency/";

    private final OxiaGatewayRecordClient client;
    private final TrustedClock trustedClock;
    private final long ownershipMaxAgeMs;
    private final long outcomeWaitMs;
    private final String recordPrefix;

    public OxiaGatewayIdempotencyStore(
            final SyncOxiaClient client,
            final String keyPrefix,
            final TrustedClock trustedClock,
            final long ownershipMaxAgeMs,
            final long outcomeWaitMs) {
        this(new SyncRecordClient(client), keyPrefix, trustedClock, ownershipMaxAgeMs, outcomeWaitMs);
    }

    /** Creates a store fenced to the exact ephemeral session of a handle. */
    public OxiaGatewayIdempotencyStore(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
            final String keyPrefix,
            final TrustedClock trustedClock,
            final long ownershipMaxAgeMs,
            final long outcomeWaitMs) {
        this(
                new SessionBoundOxiaGatewayRecordClient(handle),
                keyPrefix,
                trustedClock,
                ownershipMaxAgeMs,
                outcomeWaitMs);
    }

    OxiaGatewayIdempotencyStore(
            final OxiaGatewayRecordClient client,
            final String keyPrefix,
            final TrustedClock trustedClock,
            final long ownershipMaxAgeMs,
            final long outcomeWaitMs) {
        this.client = Objects.requireNonNull(client, "client");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        GatewayIdempotencyStore.requireTimeBounds(trustedClock, ownershipMaxAgeMs, outcomeWaitMs);
        this.ownershipMaxAgeMs = ownershipMaxAgeMs;
        this.outcomeWaitMs = outcomeWaitMs;
        this.recordPrefix = canonicalKeyPrefix(keyPrefix) + RECORD_SUFFIX;
    }

    @Override
    public synchronized GatewayIdempotencyStore.PrepareResult prepareIfAbsent(
            final Digest32 keyHash,
            final GatewayOperationKind operation,
            final Digest32 bodyHash,
            final PreparedSubmission submission,
            final long retainUntilEpochMs) {
        Objects.requireNonNull(keyHash, "keyHash");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(bodyHash, "bodyHash");
        Objects.requireNonNull(submission, "submission");
        final String key = recordKey(keyHash);
        final Entry existing = read(key, keyHash);
        if (existing != null) {
            return classifyPrepare(existing.record(), operation, bodyHash);
        }
        final long now = now();
        final GatewayIdempotencyRecord created = new GatewayIdempotencyRecord(
                keyHash,
                operation,
                bodyHash,
                submission.canonicalBytes(),
                GatewayIdempotencyPhase.PREPARED,
                List.of(),
                null,
                now,
                Math.max(now, retainUntilEpochMs),
                1);
        try {
            put(key, created, Set.of(PutOption.IfRecordDoesNotExist));
            return new GatewayIdempotencyStore.PrepareResult(GatewayIdempotencyStore.PrepareState.CREATED, created);
        } catch (RuntimeException raceOrResponseLoss) {
            final Entry observed = read(key, keyHash);
            if (observed == null) {
                throw raceOrResponseLoss;
            }
            return classifyPrepare(observed.record(), operation, bodyHash);
        }
    }

    @Override
    public synchronized GatewayIdempotencyStore.AttemptStart startAttempt(final Digest32 keyHash) {
        final Entry current = require(keyHash);
        if (current.record().phase() != GatewayIdempotencyPhase.PREPARED
                || current.record().aggregateOutcomeBytes() != null) {
            final Entry recovered = recoverExpiredStartedAttempt(keyHash, current);
            return new GatewayIdempotencyStore.AttemptStart(recovered.record(), null);
        }
        if (now() >= current.record().retainUntilEpochMs()) {
            return new GatewayIdempotencyStore.AttemptStart(current.record(), null);
        }
        final long started = now();
        final long uncertaintyAt = checkedAdd(started, outcomeWaitMs);
        final long ownershipNotAfter = checkedAdd(started, ownershipMaxAgeMs);
        final PhysicalEnqueueAttemptId attemptId = PhysicalEnqueueAttemptId.random();
        final GatewayPhysicalAttempt attempt = new GatewayPhysicalAttempt(
                1,
                attemptId,
                GatewayPhysicalAttemptState.STARTED,
                null,
                started,
                uncertaintyAt,
                checkedIncrement(current.record().revision()),
                ownershipNotAfter);
        final GatewayIdempotencyRecord next = current.record().withAttempt(attempt);
        try {
            put(current.key(), next, Set.of(PutOption.IfVersionIdEquals(current.versionId())));
            return new GatewayIdempotencyStore.AttemptStart(
                    next,
                    new GatewayAttemptOwnershipPermit(attemptId, next.revision(), ownershipNotAfter, trustedClock));
        } catch (RuntimeException raceOrResponseLoss) {
            final Entry observed = require(keyHash);
            if (observed.record().canonicalBytes().length == next.canonicalBytes().length
                    && Bytes.constantTimeEquals(observed.record().canonicalBytes(), next.canonicalBytes())) {
                return new GatewayIdempotencyStore.AttemptStart(observed.record(), null);
            }
            return new GatewayIdempotencyStore.AttemptStart(observed.record(), null);
        }
    }

    private Entry recoverExpiredStartedAttempt(final Digest32 keyHash, final Entry current) {
        if (current.record().phase() != GatewayIdempotencyPhase.ACTIVE
                || current.record().attempts().isEmpty()) {
            return current;
        }
        final GatewayPhysicalAttempt started =
                current.record().attempts().get(current.record().attempts().size() - 1);
        if (started.state() != GatewayPhysicalAttemptState.STARTED || now() < started.uncertaintyAtEpochMs()) {
            return current;
        }
        final PreparedSubmission prepared =
                PreparedSubmission.decode(current.record().preparedSubmissionBytes());
        final SubmissionOutcomeMessage uncertain =
                GatewayOutcomeSupport.uncertain(prepared, started.physicalAttemptId());
        final GatewayIdempotencyRecord recovered = current.record()
                .withOutcome(
                        new GatewayIdempotencyRecord.PhysicalEnqueueAttemptIdMatch(started.physicalAttemptId()),
                        uncertain,
                        GatewayPhysicalAttemptState.UNCERTAIN);
        try {
            put(current.key(), recovered, Set.of(PutOption.IfVersionIdEquals(current.versionId())));
        } catch (RuntimeException raceOrResponseLoss) {
            return require(keyHash);
        }
        return require(keyHash);
    }

    @Override
    public synchronized GatewayIdempotencyStore.RetryStart startRetry(
            final Digest32 keyHash,
            final PhysicalEnqueueAttemptId expectedPrior,
            final PhysicalEnqueueAttemptId retryRequestId) {
        Objects.requireNonNull(expectedPrior, "expectedPrior");
        Objects.requireNonNull(retryRequestId, "retryRequestId");
        final Entry current = recoverExpiredStartedAttempt(keyHash, require(keyHash));
        final Digest32 retryHash = GatewayIdempotencyHash.retryRequestHash(keyHash, expectedPrior, retryRequestId);
        for (GatewayPhysicalAttempt attempt : current.record().attempts()) {
            if (retryRequestId.equals(attempt.retryRequestId())) {
                return new GatewayIdempotencyStore.RetryStart(
                        current.record(),
                        null,
                        retryHash.equals(attempt.retryRequestHash())
                                ? GatewayIdempotencyStore.RetryState.EXISTING_RETRY
                                : GatewayIdempotencyStore.RetryState.CONFLICT);
            }
        }
        if (current.record().aggregateOutcomeBytes() == null
                || current.record().phase() != GatewayIdempotencyPhase.QUIESCENT) {
            return new GatewayIdempotencyStore.RetryStart(
                    current.record(), null, GatewayIdempotencyStore.RetryState.NOT_RETRYABLE);
        }
        final SubmissionOutcomeMessage aggregate;
        try {
            aggregate = SubmissionOutcomeMessage.decode(current.record().aggregateOutcomeBytes());
        } catch (RuntimeException malformed) {
            return new GatewayIdempotencyStore.RetryStart(
                    current.record(), null, GatewayIdempotencyStore.RetryState.NOT_RETRYABLE);
        }
        if (!isUncertain(aggregate) || current.record().attempts().isEmpty()) {
            return new GatewayIdempotencyStore.RetryStart(
                    current.record(), null, GatewayIdempotencyStore.RetryState.NOT_RETRYABLE);
        }
        GatewayPhysicalAttempt prior = null;
        for (int index = current.record().attempts().size() - 1; index >= 0; index--) {
            if (current.record().attempts().get(index).state() == GatewayPhysicalAttemptState.UNCERTAIN) {
                prior = current.record().attempts().get(index);
                break;
            }
        }
        if (prior == null || !prior.physicalAttemptId().equals(expectedPrior)) {
            return new GatewayIdempotencyStore.RetryStart(
                    current.record(), null, GatewayIdempotencyStore.RetryState.STALE_PRECONDITION);
        }
        final long started = now();
        final long uncertaintyAt = checkedAdd(started, outcomeWaitMs);
        final long ownershipNotAfter = checkedAdd(started, ownershipMaxAgeMs);
        final PhysicalEnqueueAttemptId attemptId = PhysicalEnqueueAttemptId.random();
        final GatewayPhysicalAttempt attempt = new GatewayPhysicalAttempt(
                current.record().attempts().size() + 1,
                attemptId,
                GatewayPhysicalAttemptState.STARTED,
                null,
                started,
                uncertaintyAt,
                retryRequestId,
                retryHash,
                checkedIncrement(current.record().revision()),
                ownershipNotAfter);
        final GatewayIdempotencyRecord next = current.record().withAttempt(attempt);
        try {
            put(current.key(), next, Set.of(PutOption.IfVersionIdEquals(current.versionId())));
            return new GatewayIdempotencyStore.RetryStart(
                    next,
                    new GatewayAttemptOwnershipPermit(attemptId, next.revision(), ownershipNotAfter, trustedClock),
                    GatewayIdempotencyStore.RetryState.STARTED);
        } catch (RuntimeException raceOrResponseLoss) {
            final Entry observed = require(keyHash);
            final GatewayIdempotencyStore.RetryState state = observed.record().attempts().stream()
                    .filter(item -> retryRequestId.equals(item.retryRequestId()))
                    .map(item -> retryHash.equals(item.retryRequestHash())
                            ? GatewayIdempotencyStore.RetryState.EXISTING_RETRY
                            : GatewayIdempotencyStore.RetryState.CONFLICT)
                    .findFirst()
                    .orElse(GatewayIdempotencyStore.RetryState.STALE_PRECONDITION);
            return new GatewayIdempotencyStore.RetryStart(observed.record(), null, state);
        }
    }

    @Override
    public synchronized GatewayIdempotencyRecord finish(
            final Digest32 keyHash, final PhysicalEnqueueAttemptId attemptId, final SubmissionOutcomeMessage outcome) {
        Objects.requireNonNull(attemptId, "attemptId");
        Objects.requireNonNull(outcome, "outcome");
        final Entry current = require(keyHash);
        current.record().attempts().stream()
                .filter(item -> item.physicalAttemptId().equals(attemptId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Gateway attempt does not belong to record"));
        final GatewayPhysicalAttemptState state = stateFor(outcome);
        final GatewayIdempotencyRecord next = current.record()
                .withOutcome(new GatewayIdempotencyRecord.PhysicalEnqueueAttemptIdMatch(attemptId), outcome, state);
        if (next == current.record()) {
            return next;
        }
        try {
            put(current.key(), next, Set.of(PutOption.IfVersionIdEquals(current.versionId())));
            return next;
        } catch (RuntimeException raceOrResponseLoss) {
            final Entry observed = require(keyHash);
            if (Bytes.constantTimeEquals(observed.record().canonicalBytes(), next.canonicalBytes())) {
                return observed.record();
            }
            throw raceOrResponseLoss;
        }
    }

    @Override
    public synchronized GatewayIdempotencyRecord exact(final Digest32 keyHash) {
        final Entry entry = read(recordKey(Objects.requireNonNull(keyHash, "keyHash")), keyHash);
        return entry == null ? null : entry.record();
    }

    @Override
    public void close() {
        client.close();
    }

    private GatewayIdempotencyStore.PrepareResult classifyPrepare(
            final GatewayIdempotencyRecord record, final GatewayOperationKind operation, final Digest32 bodyHash) {
        if (record.operation() != operation || !record.requestBodyHash().equals(bodyHash)) {
            return new GatewayIdempotencyStore.PrepareResult(GatewayIdempotencyStore.PrepareState.CONFLICT, record);
        }
        return new GatewayIdempotencyStore.PrepareResult(GatewayIdempotencyStore.PrepareState.EXISTING_MATCH, record);
    }

    private Entry require(final Digest32 keyHash) {
        final Entry entry = read(recordKey(Objects.requireNonNull(keyHash, "keyHash")), keyHash);
        if (entry == null) {
            throw new IllegalArgumentException("Gateway idempotency record not found");
        }
        return entry;
    }

    private Entry read(final String key, final Digest32 keyHash) {
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key())
                || result.value() == null
                || result.version() == null
                || result.value().length > MAX_RECORD_BYTES) {
            throw new IllegalStateException("Oxia Gateway record response is not exact");
        }
        final GatewayIdempotencyRecord record = GatewayIdempotencyRecord.decode(result.value());
        if (!record.gatewayKeyHash().equals(keyHash)) {
            throw new IllegalStateException("Oxia Gateway record key/hash mismatch");
        }
        return new Entry(key, record, result.version().versionId());
    }

    private void put(final String key, final GatewayIdempotencyRecord record, final Set<PutOption> options) {
        final byte[] value = record.canonicalBytes();
        if (value.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("Gateway idempotency record exceeds bounded size");
        }
        try {
            final PutResult result = client.put(key, value, options);
            if (result == null || !key.equals(result.key()) || result.version() == null) {
                throw new IllegalStateException("Oxia Gateway put returned no exact version");
            }
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            throw new IllegalStateException("Oxia Gateway CAS lost", race);
        }
    }

    private String recordKey(final Digest32 keyHash) {
        return recordPrefix + Bytes.hex(keyHash.bytes());
    }

    private long now() {
        final long now = trustedClock.nowEpochMs();
        if (now < 0) {
            throw new IllegalStateException("trusted Gateway clock returned a negative epoch");
        }
        return now;
    }

    private static long checkedAdd(final long left, final long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Gateway time bound overflows", overflow);
        }
    }

    private static long checkedIncrement(final long value) {
        if (value <= 0 || value == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Gateway record revision is exhausted");
        }
        return value + 1;
    }

    private static boolean isUncertain(final SubmissionOutcomeMessage outcome) {
        return outcome.kind() == SubmissionOutcomeKind.NATIVE_ENQUEUE_UNCERTAIN
                || outcome.kind() == SubmissionOutcomeKind.MANAGED
                        && outcome.managed().kind()
                                == com.nereusstream.delay.protocol.EnqueueOutcomeKind.ENQUEUE_UNCERTAIN;
    }

    private static GatewayPhysicalAttemptState stateFor(final SubmissionOutcomeMessage outcome) {
        return switch (outcome.kind()) {
            case MANAGED ->
                switch (outcome.managed().kind()) {
                    case QUEUED -> GatewayPhysicalAttemptState.QUEUED;
                    case DEFINITELY_NOT_QUEUED -> GatewayPhysicalAttemptState.DEFINITELY_NOT_QUEUED;
                    case ENQUEUE_UNCERTAIN -> GatewayPhysicalAttemptState.UNCERTAIN;
                };
            case NATIVE_RECEIPT -> GatewayPhysicalAttemptState.QUEUED;
            case NATIVE_DEFINITELY_NOT_QUEUED -> GatewayPhysicalAttemptState.DEFINITELY_NOT_QUEUED;
            case NATIVE_ENQUEUE_UNCERTAIN -> GatewayPhysicalAttemptState.UNCERTAIN;
        };
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be nonblank NFC UTF-8 without trailing '/'");
        }
        return value;
    }

    private record Entry(String key, GatewayIdempotencyRecord record, long versionId) {}

    private static final class SyncRecordClient implements OxiaGatewayRecordClient {
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
                throw new IllegalStateException("failed to close Oxia Gateway client", failure);
            }
        }
    }
}
