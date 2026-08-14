package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;
import io.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

import java.util.Objects;

/** Durable single-record Gateway CAS surface shared by local and Oxia stores. */
public interface GatewayIdempotencyStore {
    PrepareResult prepareIfAbsent(Digest32 keyHash, GatewayOperationKindV1 operation, Digest32 bodyHash,
                                   PreparedSubmissionV1 submission, long retainUntilEpochMs);

    AttemptStart startAttempt(Digest32 keyHash);

    RetryStart startRetry(Digest32 keyHash, PhysicalEnqueueAttemptId expectedPriorAttemptId,
                          PhysicalEnqueueAttemptId retryRequestId);

    GatewayIdempotencyRecordV1 finish(Digest32 keyHash, PhysicalEnqueueAttemptId attemptId,
                                      SubmissionOutcomeMessageV1 outcome);

    GatewayIdempotencyRecordV1 exact(Digest32 keyHash);

    enum PrepareState {
        CREATED,
        EXISTING_MATCH,
        CONFLICT
    }

    record PrepareResult(PrepareState state, GatewayIdempotencyRecordV1 record) {
        public PrepareResult {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(record, "record");
        }
    }

    record AttemptStart(GatewayIdempotencyRecordV1 record, GatewayAttemptOwnershipPermit permit) {
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

    record RetryStart(GatewayIdempotencyRecordV1 record, GatewayAttemptOwnershipPermit permit,
                      RetryState state) {
        public RetryStart {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(state, "state");
        }
    }

    /** Validates the bounded time parameters shared by all store implementations. */
    static void requireTimeBounds(final TrustedClock trustedClock, final long ownershipMaxAgeMs,
                                  final long outcomeWaitMs) {
        Objects.requireNonNull(trustedClock, "trustedClock");
        if (ownershipMaxAgeMs <= 0 || outcomeWaitMs <= 0 || ownershipMaxAgeMs > outcomeWaitMs) {
            throw new IllegalArgumentException("Gateway ownership/outcome bounds are invalid");
        }
    }
}
