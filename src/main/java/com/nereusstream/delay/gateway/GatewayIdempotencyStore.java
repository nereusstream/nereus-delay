package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import java.util.Objects;

/** Durable single-record Gateway CAS surface shared by local and Oxia stores. */
public interface GatewayIdempotencyStore {
    PrepareResult prepareIfAbsent(
            Digest32 keyHash,
            GatewayOperationKind operation,
            Digest32 bodyHash,
            PreparedSubmission submission,
            long retainUntilEpochMs);

    AttemptStart startAttempt(Digest32 keyHash);

    RetryStart startRetry(
            Digest32 keyHash, PhysicalEnqueueAttemptId expectedPriorAttemptId, PhysicalEnqueueAttemptId retryRequestId);

    GatewayIdempotencyRecord finish(
            Digest32 keyHash, PhysicalEnqueueAttemptId attemptId, SubmissionOutcomeMessage outcome);

    GatewayIdempotencyRecord exact(Digest32 keyHash);

    enum PrepareState {
        CREATED,
        EXISTING_MATCH,
        CONFLICT
    }

    record PrepareResult(PrepareState state, GatewayIdempotencyRecord record) {
        public PrepareResult {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(record, "record");
        }
    }

    record AttemptStart(GatewayIdempotencyRecord record, GatewayAttemptOwnershipPermit permit) {
        public AttemptStart {
            Objects.requireNonNull(record, "record");
        }
    }

    enum RetryState {
        STARTED,
        EXISTING_RETRY,
        CONFLICT,
        STALE_PRECONDITION,
        NOT_RETRYABLE
    }

    record RetryStart(GatewayIdempotencyRecord record, GatewayAttemptOwnershipPermit permit, RetryState state) {
        public RetryStart {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(state, "state");
        }
    }

    /** Validates the bounded time parameters shared by all store implementations. */
    static void requireTimeBounds(
            final TrustedClock trustedClock, final long ownershipMaxAgeMs, final long outcomeWaitMs) {
        Objects.requireNonNull(trustedClock, "trustedClock");
        if (ownershipMaxAgeMs <= 0 || outcomeWaitMs <= 0 || ownershipMaxAgeMs > outcomeWaitMs) {
            throw new IllegalArgumentException("Gateway ownership/outcome bounds are invalid");
        }
    }
}
