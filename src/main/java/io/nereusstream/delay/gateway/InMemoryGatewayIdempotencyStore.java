package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeKindV1;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;
import io.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Single-process conformance store modelling the Gateway one-record CAS rules. */
public final class InMemoryGatewayIdempotencyStore implements GatewayIdempotencyStore {
    private final Map<Digest32, GatewayIdempotencyRecordV1> records = new HashMap<>();
    private final TrustedClock trustedClock;
    private final long ownershipMaxAgeMs;
    private final long outcomeWaitMs;

    public InMemoryGatewayIdempotencyStore(final TrustedClock trustedClock, final long ownershipMaxAgeMs,
                                           final long outcomeWaitMs) {
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        GatewayIdempotencyStore.requireTimeBounds(trustedClock, ownershipMaxAgeMs, outcomeWaitMs);
        this.ownershipMaxAgeMs = ownershipMaxAgeMs;
        this.outcomeWaitMs = outcomeWaitMs;
    }

    public synchronized PrepareResult prepareIfAbsent(final Digest32 keyHash, final GatewayOperationKindV1 operation,
                                                       final Digest32 bodyHash, final PreparedSubmissionV1 submission,
                                                       final long retainUntilEpochMs) {
        Objects.requireNonNull(keyHash, "keyHash");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(bodyHash, "bodyHash");
        Objects.requireNonNull(submission, "submission");
        final GatewayIdempotencyRecordV1 existing = records.get(keyHash);
        if (existing != null) {
            return new PrepareResult(existing.requestBodyHash().equals(bodyHash)
                    ? PrepareState.EXISTING_MATCH : PrepareState.CONFLICT, existing);
        }
        final long now = now();
        final GatewayIdempotencyRecordV1 created = new GatewayIdempotencyRecordV1(keyHash, operation, bodyHash,
                submission.canonicalBytes(), GatewayIdempotencyPhaseV1.PREPARED, java.util.List.of(), null, now,
                Math.max(now, retainUntilEpochMs), 1);
        records.put(keyHash, created);
        return new PrepareResult(PrepareState.CREATED, created);
    }

    public synchronized AttemptStart startAttempt(final Digest32 keyHash) {
        final GatewayIdempotencyRecordV1 current = require(keyHash);
        if (current.phase() != GatewayIdempotencyPhaseV1.PREPARED || current.aggregateOutcomeBytes() != null) {
            if (current.phase() == GatewayIdempotencyPhaseV1.ACTIVE
                    && !current.attempts().isEmpty()) {
                final GatewayPhysicalAttemptV1 started = current.attempts().get(current.attempts().size() - 1);
                if (started.state() == GatewayPhysicalAttemptStateV1.STARTED
                        && now() >= started.uncertaintyAtEpochMs()) {
                    final PreparedSubmissionV1 prepared = PreparedSubmissionV1.decode(
                            current.preparedSubmissionBytes());
                    final SubmissionOutcomeMessageV1 uncertain = GatewayOutcomeSupport.uncertain(prepared,
                            started.physicalAttemptId());
                    final GatewayIdempotencyRecordV1 recovered = current.withOutcome(
                            new GatewayIdempotencyRecordV1.PhysicalEnqueueAttemptIdMatch(
                                    started.physicalAttemptId()), uncertain.canonicalBytes(),
                            GatewayPhysicalAttemptStateV1.UNCERTAIN);
                    records.put(keyHash, recovered);
                    return new AttemptStart(recovered, null);
                }
            }
            return new AttemptStart(current, null);
        }
        final long started = now();
        final long uncertaintyAt = checkedAdd(started, outcomeWaitMs);
        final long ownershipNotAfter = checkedAdd(started, ownershipMaxAgeMs);
        final PhysicalEnqueueAttemptId id = PhysicalEnqueueAttemptId.random();
        final GatewayPhysicalAttemptV1 attempt = new GatewayPhysicalAttemptV1(1, id,
                GatewayPhysicalAttemptStateV1.STARTED, null, started, uncertaintyAt, current.revision() + 1,
                ownershipNotAfter);
        final GatewayIdempotencyRecordV1 next = current.withAttempt(attempt);
        records.put(keyHash, next);
        return new AttemptStart(next, new GatewayAttemptOwnershipPermit(id, next.revision(), ownershipNotAfter,
                trustedClock));
    }

    /** Starts exactly one explicit retry from the current uncertain aggregate. */
    public synchronized RetryStart startRetry(final Digest32 keyHash,
                                               final PhysicalEnqueueAttemptId expectedPriorAttemptId,
                                               final PhysicalEnqueueAttemptId retryRequestId) {
        GatewayIdempotencyRecordV1 current = require(keyHash);
        current = recoverExpiredStartedAttempt(keyHash, current);
        final Digest32 retryHash = GatewayIdempotencyHashV1.retryRequestHash(keyHash, expectedPriorAttemptId,
                retryRequestId);
        for (GatewayPhysicalAttemptV1 attempt : current.attempts()) {
            if (retryRequestId.equals(attempt.retryRequestId())) {
                return new RetryStart(current, null,
                        retryHash.equals(attempt.retryRequestHash()) ? RetryState.EXISTING_RETRY
                                : RetryState.CONFLICT);
            }
        }
        if (current.aggregateOutcomeBytes() == null || current.phase() != GatewayIdempotencyPhaseV1.QUIESCENT) {
            return new RetryStart(current, null, RetryState.NOT_RETRYABLE);
        }
        final SubmissionOutcomeMessageV1 aggregate;
        try {
            aggregate = SubmissionOutcomeMessageV1.decode(current.aggregateOutcomeBytes());
        } catch (RuntimeException malformed) {
            return new RetryStart(current, null, RetryState.NOT_RETRYABLE);
        }
        final boolean uncertain = aggregate.kind() == SubmissionOutcomeKindV1.NATIVE_ENQUEUE_UNCERTAIN
                || (aggregate.kind() == SubmissionOutcomeKindV1.MANAGED
                && aggregate.managed().kind() == io.nereusstream.delay.protocol.EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN);
        if (!uncertain || current.attempts().isEmpty()) {
            return new RetryStart(current, null, RetryState.NOT_RETRYABLE);
        }
        final GatewayPhysicalAttemptV1 prior = current.attempts().get(current.attempts().size() - 1);
        if (prior.state() != GatewayPhysicalAttemptStateV1.UNCERTAIN
                || !prior.physicalAttemptId().equals(expectedPriorAttemptId)) {
            return new RetryStart(current, null, RetryState.STALE_PRECONDITION);
        }
        final long started = now();
        final long uncertaintyAt = checkedAdd(started, outcomeWaitMs);
        final long ownershipNotAfter = checkedAdd(started, ownershipMaxAgeMs);
        final PhysicalEnqueueAttemptId id = PhysicalEnqueueAttemptId.random();
        final GatewayPhysicalAttemptV1 attempt = new GatewayPhysicalAttemptV1(current.attempts().size() + 1, id,
                GatewayPhysicalAttemptStateV1.STARTED, null, started, uncertaintyAt, retryRequestId, retryHash,
                current.revision() + 1, ownershipNotAfter);
        final GatewayIdempotencyRecordV1 next = current.withAttempt(attempt);
        records.put(keyHash, next);
        return new RetryStart(next, new GatewayAttemptOwnershipPermit(id, next.revision(), ownershipNotAfter,
                trustedClock), RetryState.STARTED);
    }

    private GatewayIdempotencyRecordV1 recoverExpiredStartedAttempt(final Digest32 keyHash,
                                                                     final GatewayIdempotencyRecordV1 current) {
        if (current.phase() != GatewayIdempotencyPhaseV1.ACTIVE || current.attempts().isEmpty()) {
            return current;
        }
        final GatewayPhysicalAttemptV1 started = current.attempts().get(current.attempts().size() - 1);
        if (started.state() != GatewayPhysicalAttemptStateV1.STARTED
                || now() < started.uncertaintyAtEpochMs()) {
            return current;
        }
        final PreparedSubmissionV1 prepared = PreparedSubmissionV1.decode(current.preparedSubmissionBytes());
        final SubmissionOutcomeMessageV1 uncertain = GatewayOutcomeSupport.uncertain(prepared,
                started.physicalAttemptId());
        final GatewayIdempotencyRecordV1 recovered = current.withOutcome(
                new GatewayIdempotencyRecordV1.PhysicalEnqueueAttemptIdMatch(started.physicalAttemptId()),
                uncertain.canonicalBytes(), GatewayPhysicalAttemptStateV1.UNCERTAIN);
        records.put(keyHash, recovered);
        return recovered;
    }

    public synchronized GatewayIdempotencyRecordV1 finish(final Digest32 keyHash,
                                                           final PhysicalEnqueueAttemptId attemptId,
                                                           final SubmissionOutcomeMessageV1 outcome) {
        final GatewayIdempotencyRecordV1 current = require(keyHash);
        final GatewayPhysicalAttemptV1 attempt = current.attempts().stream()
                .filter(value -> value.physicalAttemptId().equals(attemptId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Gateway attempt does not belong to record"));
        final GatewayPhysicalAttemptStateV1 state = switch (outcome.kind()) {
            case MANAGED -> switch (outcome.managed().kind()) {
                case QUEUED -> GatewayPhysicalAttemptStateV1.QUEUED;
                case DEFINITELY_NOT_QUEUED -> GatewayPhysicalAttemptStateV1.DEFINITELY_NOT_QUEUED;
                case ENQUEUE_UNCERTAIN -> GatewayPhysicalAttemptStateV1.UNCERTAIN;
            };
            case NATIVE_RECEIPT -> GatewayPhysicalAttemptStateV1.QUEUED;
            case NATIVE_DEFINITELY_NOT_QUEUED -> GatewayPhysicalAttemptStateV1.DEFINITELY_NOT_QUEUED;
            case NATIVE_ENQUEUE_UNCERTAIN -> GatewayPhysicalAttemptStateV1.UNCERTAIN;
        };
        final GatewayIdempotencyRecordV1 next = current.withOutcome(
                new GatewayIdempotencyRecordV1.PhysicalEnqueueAttemptIdMatch(attemptId), outcome.canonicalBytes(),
                state);
        records.put(keyHash, next);
        return next;
    }

    public synchronized GatewayIdempotencyRecordV1 exact(final Digest32 keyHash) {
        return records.get(keyHash);
    }

    private GatewayIdempotencyRecordV1 require(final Digest32 keyHash) {
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

}
