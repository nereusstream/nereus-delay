package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;
import io.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Single-process conformance store modelling the Gateway one-record CAS rules. */
public final class InMemoryGatewayIdempotencyStore {
    private final Map<Digest32, GatewayIdempotencyRecordV1> records = new HashMap<>();
    private final TrustedClock trustedClock;
    private final long ownershipMaxAgeMs;
    private final long outcomeWaitMs;

    public InMemoryGatewayIdempotencyStore(final TrustedClock trustedClock, final long ownershipMaxAgeMs,
                                           final long outcomeWaitMs) {
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        if (ownershipMaxAgeMs <= 0 || outcomeWaitMs <= 0 || ownershipMaxAgeMs > outcomeWaitMs) {
            throw new IllegalArgumentException("Gateway ownership/outcome bounds are invalid");
        }
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

    public enum PrepareState {
        CREATED,
        EXISTING_MATCH,
        CONFLICT
    }

    public record PrepareResult(PrepareState state, GatewayIdempotencyRecordV1 record) {
        public PrepareResult {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(record, "record");
        }
    }

    public record AttemptStart(GatewayIdempotencyRecordV1 record, GatewayAttemptOwnershipPermit permit) {
        public AttemptStart {
            Objects.requireNonNull(record, "record");
        }
    }
}
