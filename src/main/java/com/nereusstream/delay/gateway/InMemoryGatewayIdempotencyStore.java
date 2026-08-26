package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Single-process conformance store modelling the Gateway one-record CAS rules. */
public final class InMemoryGatewayIdempotencyStore implements GatewayIdempotencyStore {
    private final Map<Digest32, GatewayIdempotencyRecord> records = new HashMap<>();
    private final TrustedClock trustedClock;
    private final long ownershipMaxAgeMs;
    private final long outcomeWaitMs;

    public InMemoryGatewayIdempotencyStore(
            final TrustedClock trustedClock, final long ownershipMaxAgeMs, final long outcomeWaitMs) {
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        GatewayIdempotencyStore.requireTimeBounds(trustedClock, ownershipMaxAgeMs, outcomeWaitMs);
        this.ownershipMaxAgeMs = ownershipMaxAgeMs;
        this.outcomeWaitMs = outcomeWaitMs;
    }

    public synchronized PrepareResult prepareIfAbsent(
            final Digest32 keyHash,
            final GatewayOperationKind operation,
            final Digest32 bodyHash,
            final PreparedSubmission submission,
            final long retainUntilEpochMs) {
        Objects.requireNonNull(keyHash, "keyHash");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(bodyHash, "bodyHash");
        Objects.requireNonNull(submission, "submission");
        final GatewayIdempotencyRecord existing = records.get(keyHash);
        if (existing != null) {
            return new PrepareResult(
                    existing.requestBodyHash().equals(bodyHash) ? PrepareState.EXISTING_MATCH : PrepareState.CONFLICT,
                    existing);
        }
        final long now = now();
        final GatewayIdempotencyRecord created = new GatewayIdempotencyRecord(
                keyHash,
                operation,
                bodyHash,
                submission.canonicalBytes(),
                GatewayIdempotencyPhase.PREPARED,
                java.util.List.of(),
                null,
                now,
                Math.max(now, retainUntilEpochMs),
                1);
        records.put(keyHash, created);
        return new PrepareResult(PrepareState.CREATED, created);
    }

    public synchronized AttemptStart startAttempt(final Digest32 keyHash) {
        final GatewayIdempotencyRecord current = require(keyHash);
        if (current.phase() != GatewayIdempotencyPhase.PREPARED || current.aggregateOutcomeBytes() != null) {
            if (current.phase() == GatewayIdempotencyPhase.ACTIVE
                    && !current.attempts().isEmpty()) {
                final GatewayPhysicalAttempt started =
                        current.attempts().get(current.attempts().size() - 1);
                if (started.state() == GatewayPhysicalAttemptState.STARTED && now() >= started.uncertaintyAtEpochMs()) {
                    final PreparedSubmission prepared = PreparedSubmission.decode(current.preparedSubmissionBytes());
                    final SubmissionOutcomeMessage uncertain =
                            GatewayOutcomeSupport.uncertain(prepared, started.physicalAttemptId());
                    final GatewayIdempotencyRecord recovered = current.withOutcome(
                            new GatewayIdempotencyRecord.PhysicalEnqueueAttemptIdMatch(started.physicalAttemptId()),
                            uncertain,
                            GatewayPhysicalAttemptState.UNCERTAIN);
                    records.put(keyHash, recovered);
                    return new AttemptStart(recovered, null);
                }
            }
            return new AttemptStart(current, null);
        }
        if (now() >= current.retainUntilEpochMs()) {
            return new AttemptStart(current, null);
        }
        final long started = now();
        final long uncertaintyAt = checkedAdd(started, outcomeWaitMs);
        final long ownershipNotAfter = checkedAdd(started, ownershipMaxAgeMs);
        final PhysicalEnqueueAttemptId id = PhysicalEnqueueAttemptId.random();
        final GatewayPhysicalAttempt attempt = new GatewayPhysicalAttempt(
                1,
                id,
                GatewayPhysicalAttemptState.STARTED,
                null,
                started,
                uncertaintyAt,
                checkedIncrement(current.revision()),
                ownershipNotAfter);
        final GatewayIdempotencyRecord next = current.withAttempt(attempt);
        records.put(keyHash, next);
        return new AttemptStart(
                next, new GatewayAttemptOwnershipPermit(id, next.revision(), ownershipNotAfter, trustedClock));
    }

    /** Starts exactly one explicit retry from the current uncertain aggregate. */
    public synchronized RetryStart startRetry(
            final Digest32 keyHash,
            final PhysicalEnqueueAttemptId expectedPriorAttemptId,
            final PhysicalEnqueueAttemptId retryRequestId) {
        GatewayIdempotencyRecord current = require(keyHash);
        current = recoverExpiredStartedAttempt(keyHash, current);
        final Digest32 retryHash =
                GatewayIdempotencyHash.retryRequestHash(keyHash, expectedPriorAttemptId, retryRequestId);
        for (GatewayPhysicalAttempt attempt : current.attempts()) {
            if (retryRequestId.equals(attempt.retryRequestId())) {
                return new RetryStart(
                        current,
                        null,
                        retryHash.equals(attempt.retryRequestHash()) ? RetryState.EXISTING_RETRY : RetryState.CONFLICT);
            }
        }
        if (current.aggregateOutcomeBytes() == null || current.phase() != GatewayIdempotencyPhase.QUIESCENT) {
            return new RetryStart(current, null, RetryState.NOT_RETRYABLE);
        }
        final SubmissionOutcomeMessage aggregate;
        try {
            aggregate = SubmissionOutcomeMessage.decode(current.aggregateOutcomeBytes());
        } catch (RuntimeException malformed) {
            return new RetryStart(current, null, RetryState.NOT_RETRYABLE);
        }
        final boolean uncertain = aggregate.kind() == SubmissionOutcomeKind.NATIVE_ENQUEUE_UNCERTAIN
                || (aggregate.kind() == SubmissionOutcomeKind.MANAGED
                        && aggregate.managed().kind()
                                == com.nereusstream.delay.protocol.EnqueueOutcomeKind.ENQUEUE_UNCERTAIN);
        if (!uncertain || current.attempts().isEmpty()) {
            return new RetryStart(current, null, RetryState.NOT_RETRYABLE);
        }
        GatewayPhysicalAttempt prior = null;
        for (int index = current.attempts().size() - 1; index >= 0; index--) {
            if (current.attempts().get(index).state() == GatewayPhysicalAttemptState.UNCERTAIN) {
                prior = current.attempts().get(index);
                break;
            }
        }
        if (prior == null || !prior.physicalAttemptId().equals(expectedPriorAttemptId)) {
            return new RetryStart(current, null, RetryState.STALE_PRECONDITION);
        }
        final long started = now();
        final long uncertaintyAt = checkedAdd(started, outcomeWaitMs);
        final long ownershipNotAfter = checkedAdd(started, ownershipMaxAgeMs);
        final PhysicalEnqueueAttemptId id = PhysicalEnqueueAttemptId.random();
        final GatewayPhysicalAttempt attempt = new GatewayPhysicalAttempt(
                current.attempts().size() + 1,
                id,
                GatewayPhysicalAttemptState.STARTED,
                null,
                started,
                uncertaintyAt,
                retryRequestId,
                retryHash,
                checkedIncrement(current.revision()),
                ownershipNotAfter);
        final GatewayIdempotencyRecord next = current.withAttempt(attempt);
        records.put(keyHash, next);
        return new RetryStart(
                next,
                new GatewayAttemptOwnershipPermit(id, next.revision(), ownershipNotAfter, trustedClock),
                RetryState.STARTED);
    }

    private GatewayIdempotencyRecord recoverExpiredStartedAttempt(
            final Digest32 keyHash, final GatewayIdempotencyRecord current) {
        if (current.phase() != GatewayIdempotencyPhase.ACTIVE
                || current.attempts().isEmpty()) {
            return current;
        }
        final GatewayPhysicalAttempt started =
                current.attempts().get(current.attempts().size() - 1);
        if (started.state() != GatewayPhysicalAttemptState.STARTED || now() < started.uncertaintyAtEpochMs()) {
            return current;
        }
        final PreparedSubmission prepared = PreparedSubmission.decode(current.preparedSubmissionBytes());
        final SubmissionOutcomeMessage uncertain =
                GatewayOutcomeSupport.uncertain(prepared, started.physicalAttemptId());
        final GatewayIdempotencyRecord recovered = current.withOutcome(
                new GatewayIdempotencyRecord.PhysicalEnqueueAttemptIdMatch(started.physicalAttemptId()),
                uncertain,
                GatewayPhysicalAttemptState.UNCERTAIN);
        records.put(keyHash, recovered);
        return recovered;
    }

    public synchronized GatewayIdempotencyRecord finish(
            final Digest32 keyHash, final PhysicalEnqueueAttemptId attemptId, final SubmissionOutcomeMessage outcome) {
        final GatewayIdempotencyRecord current = require(keyHash);
        final GatewayPhysicalAttempt attempt = current.attempts().stream()
                .filter(value -> value.physicalAttemptId().equals(attemptId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Gateway attempt does not belong to record"));
        final GatewayPhysicalAttemptState state =
                switch (outcome.kind()) {
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
        final GatewayIdempotencyRecord next = current.withOutcome(
                new GatewayIdempotencyRecord.PhysicalEnqueueAttemptIdMatch(attemptId), outcome, state);
        records.put(keyHash, next);
        return next;
    }

    public synchronized GatewayIdempotencyRecord exact(final Digest32 keyHash) {
        return records.get(keyHash);
    }

    private GatewayIdempotencyRecord require(final Digest32 keyHash) {
        return Objects.requireNonNull(records.get(keyHash), "Gateway idempotency record not found");
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
}
