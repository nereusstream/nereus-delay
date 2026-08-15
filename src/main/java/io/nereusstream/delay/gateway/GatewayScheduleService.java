package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DelaySemanticCore;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.SemanticPreparationException;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.submission.SubmissionCoordinator;
import io.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import io.nereusstream.delay.transport.TransportOwnershipState;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Single-process Gateway Schedule conformance composition.
 *
 * <p>The durable idempotency store is written before the coordinator receives
 * an ownership permit. A reread of a stored record can return its public
 * outcome, but cannot recreate the one-shot permit for an attempt that was
 * won by another process.</p>
 */
public final class GatewayScheduleService {
    private final DelaySemanticCore semanticCore;
    private final GatewayIdempotencyStore idempotency;
    private final SubmissionCoordinator submissions;
    private final TrustedClock trustedClock;

    public GatewayScheduleService(final DelaySemanticCore semanticCore,
                                  final GatewayIdempotencyStore idempotency,
                                  final SubmissionCoordinator submissions,
                                  final TrustedClock trustedClock) {
        this.semanticCore = Objects.requireNonNull(semanticCore, "semanticCore");
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency");
        this.submissions = Objects.requireNonNull(submissions, "submissions");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    public CompletionStage<GatewaySubmissionOutcomeV1> schedule(final AuthenticatedTenantContext tenant,
                                                                 final GatewayScheduleRequestV1 request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        final io.nereusstream.delay.transport.Digest32 keyHash;
        final io.nereusstream.delay.transport.Digest32 bodyHash;
        try {
            keyHash = GatewayIdempotencyHashV1.keyHash(tenant.authenticatedTenantScopeHash(),
                    request.idempotencyKey());
            bodyHash = GatewayIdempotencyHashV1.bodyHash(GatewayOperationKindV1.SCHEDULE,
                    request.canonicalBodyBytes());
        } catch (RuntimeException invalidRequest) {
            return completed(preparationError(StableCode.INVALID_METADATA));
        }

        final GatewayIdempotencyStore.PrepareResult prepared;
        final PreparedSubmissionV1 submission;
        try {
            final GatewayIdempotencyRecordV1 existing = idempotency.exact(keyHash);
            if (existing != null && !existing.requestBodyHash().equals(bodyHash)) {
                return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
            }
            if (existing != null) {
                submission = PreparedSubmissionV1.decode(existing.preparedSubmissionBytes());
                prepared = new GatewayIdempotencyStore.PrepareResult(
                        GatewayIdempotencyStore.PrepareState.EXISTING_MATCH, existing);
            } else {
                submission = semanticCore.prepareSchedule(tenant, request.route(), request.scheduleIntent(),
                        request.retryUntilEpochMs(), request.submissionMode());
                prepared = idempotency.prepareIfAbsent(keyHash, GatewayOperationKindV1.SCHEDULE, bodyHash,
                        submission, request.retryUntilEpochMs());
            }
        } catch (SemanticPreparationException failure) {
            return completed(GatewaySubmissionOutcomeV1.preparationError(failure.error()));
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return completed(preparationError(StableCode.INVALID_PREPARED_COMMAND));
        }
        if (prepared.state() == GatewayIdempotencyStore.PrepareState.CONFLICT) {
            return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        return continueAttempt(tenant, keyHash, submission, request.retryUntilEpochMs());
    }

    /** Cancel path using the same durable prepared-bytes/attempt protocol as Schedule. */
    public CompletionStage<GatewaySubmissionOutcomeV1> cancel(final AuthenticatedTenantContext tenant,
                                                               final GatewayCancelRequestV1 request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(tenant, request.idempotencyKey(), GatewayOperationKindV1.CANCEL,
                request.canonicalBodyBytes(), request.retryUntilEpochMs(),
                () -> semanticCore.prepareCancel(tenant, request.delayMessageId(), request.messagePrecondition(),
                        request.retryUntilEpochMs()));
    }

    /** Reschedule path using the same durable prepared-bytes/attempt protocol as Schedule. */
    public CompletionStage<GatewaySubmissionOutcomeV1> reschedule(final AuthenticatedTenantContext tenant,
                                                                   final GatewayRescheduleRequestV1 request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(tenant, request.idempotencyKey(), GatewayOperationKindV1.RESCHEDULE,
                request.canonicalBodyBytes(), request.retryUntilEpochMs(),
                () -> semanticCore.prepareReschedule(tenant, request.delayMessageId(), request.messagePrecondition(),
                        request.deliverAtEpochMs(), request.expireAtEpochMs(), request.retryUntilEpochMs()));
    }

    /** PrepareLargeSchedule path using the shared durable prepared-bytes protocol. */
    public CompletionStage<GatewaySubmissionOutcomeV1> prepareLargeSchedule(
            final AuthenticatedTenantContext tenant, final GatewayPrepareLargeScheduleRequestV1 request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(tenant, request.idempotencyKey(), GatewayOperationKindV1.PREPARE_LARGE_SCHEDULE,
                request.canonicalBodyBytes(), request.retryUntilEpochMs(),
                () -> semanticCore.prepareLargeSchedule(tenant, request.route(),
                        new LargeSchedulePreparationV1(request.scheduleIntent(), request.expectedPayloadLength(),
                                request.payloadSha256(), request.reservationTtlMs(), request.trustSet(),
                                request.objectStoreProfile()), request.retryUntilEpochMs()));
    }

    /** CommitLargeSchedule path using the shared durable prepared-bytes protocol. */
    public CompletionStage<GatewaySubmissionOutcomeV1> commitLargeSchedule(
            final AuthenticatedTenantContext tenant, final GatewayCommitLargeScheduleRequestV1 request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(tenant, request.idempotencyKey(), GatewayOperationKindV1.COMMIT_LARGE_SCHEDULE,
                request.canonicalBodyBytes(), request.retryUntilEpochMs(),
                () -> semanticCore.preparePayloadCommit(tenant, request.reservation(), request.proof(),
                        request.retryUntilEpochMs()));
    }

    private CompletionStage<GatewaySubmissionOutcomeV1> continueAttempt(
            final AuthenticatedTenantContext tenant,
            final io.nereusstream.delay.transport.Digest32 keyHash,
            final PreparedSubmissionV1 submission,
            final long retryUntilEpochMs) {
        try {
            if (trustedClock.nowEpochMs() >= retryUntilEpochMs) {
                return completed(GatewaySubmissionOutcomeV1.submission(GatewayOutcomeSupport.localDefinite(submission,
                        StableCode.PREPARED_COMMAND_EXPIRED)));
            }
        } catch (RuntimeException unavailable) {
            return completed(GatewaySubmissionOutcomeV1.submission(GatewayOutcomeSupport.localDefinite(submission,
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE)));
        }
        return continueStartedAttempt(tenant, keyHash, submission, idempotency.startAttempt(keyHash));
    }

    private CompletionStage<GatewaySubmissionOutcomeV1> prepareCommand(
            final AuthenticatedTenantContext tenant, final byte[] idempotencyKey,
            final GatewayOperationKindV1 operation, final byte[] canonicalBody, final long retryUntilEpochMs,
            final CommandPreparation preparation) {
        final io.nereusstream.delay.transport.Digest32 keyHash;
        final io.nereusstream.delay.transport.Digest32 bodyHash;
        try {
            keyHash = GatewayIdempotencyHashV1.keyHash(tenant.authenticatedTenantScopeHash(), idempotencyKey);
            bodyHash = GatewayIdempotencyHashV1.bodyHash(operation, canonicalBody);
        } catch (RuntimeException invalidRequest) {
            return completed(preparationError(StableCode.INVALID_METADATA));
        }
        final GatewayIdempotencyStore.PrepareResult prepared;
        final PreparedSubmissionV1 submission;
        try {
            final GatewayIdempotencyRecordV1 existing = idempotency.exact(keyHash);
            if (existing != null && !existing.requestBodyHash().equals(bodyHash)) {
                return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
            }
            if (existing != null) {
                submission = PreparedSubmissionV1.decode(existing.preparedSubmissionBytes());
                prepared = new GatewayIdempotencyStore.PrepareResult(
                        GatewayIdempotencyStore.PrepareState.EXISTING_MATCH, existing);
            } else {
                submission = semanticCore.prepareManaged(tenant, preparation.prepare());
                prepared = idempotency.prepareIfAbsent(keyHash, operation, bodyHash, submission, retryUntilEpochMs);
            }
        } catch (SemanticPreparationException failure) {
            return completed(GatewaySubmissionOutcomeV1.preparationError(failure.error()));
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return completed(preparationError(StableCode.INVALID_PREPARED_COMMAND));
        }
        if (prepared.state() == GatewayIdempotencyStore.PrepareState.CONFLICT) {
            return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        return continueAttempt(tenant, keyHash, submission, retryUntilEpochMs);
    }

    /** Retries only the stored exact prepared bytes after an uncertain aggregate. */
    public CompletionStage<GatewaySubmissionOutcomeV1> retryUncertain(final AuthenticatedTenantContext tenant,
                                                                       final GatewayRetryUncertainRequestV1 request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        final io.nereusstream.delay.transport.Digest32 keyHash;
        try {
            keyHash = GatewayIdempotencyHashV1.keyHash(tenant.authenticatedTenantScopeHash(),
                    request.originalIdempotencyKey());
        } catch (RuntimeException invalidRequest) {
            return completed(preparationError(StableCode.INVALID_METADATA));
        }
        final GatewayIdempotencyRecordV1 record = idempotency.exact(keyHash);
        if (record == null) {
            return completed(preparationError(StableCode.NOT_FOUND_OR_NOT_AUTHORIZED));
        }
        final PreparedSubmissionV1 submission;
        try {
            submission = PreparedSubmissionV1.decode(record.preparedSubmissionBytes());
        } catch (RuntimeException malformed) {
            return completed(preparationError(StableCode.INTEGRITY_ERROR));
        }
        final GatewayIdempotencyStore.RetryStart started;
        try {
            started = idempotency.startRetry(keyHash, request.expectedPriorPhysicalAttemptId(),
                    request.retryRequestId());
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return completed(preparationError(StableCode.INTEGRITY_ERROR));
        }
        if (started.state() == GatewayIdempotencyStore.RetryState.CONFLICT) {
            return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        return continueStartedAttempt(tenant, keyHash, submission,
                new GatewayIdempotencyStore.AttemptStart(started.record(), started.permit()));
    }

    private CompletionStage<GatewaySubmissionOutcomeV1> continueStartedAttempt(
            final AuthenticatedTenantContext tenant,
            final io.nereusstream.delay.transport.Digest32 keyHash,
            final PreparedSubmissionV1 submission,
            final GatewayIdempotencyStore.AttemptStart started) {
        if (started.permit() == null) {
            final byte[] aggregate = started.record().aggregateOutcomeBytes();
            if (aggregate != null) {
                try {
                    return completed(GatewaySubmissionOutcomeV1.submission(
                            SubmissionOutcomeMessageV1.decode(aggregate)));
                } catch (RuntimeException malformed) {
                    return completed(preparationError(StableCode.INTEGRITY_ERROR));
                }
            }
            if (started.record().attempts().isEmpty()) {
                return completed(GatewaySubmissionOutcomeV1.submission(
                        GatewayOutcomeSupport.localDefinite(submission, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
            }
            final GatewayPhysicalAttemptV1 last = started.record().attempts()
                    .get(started.record().attempts().size() - 1);
            return completed(GatewaySubmissionOutcomeV1.submission(GatewayOutcomeSupport.uncertain(submission,
                    last.physicalAttemptId())));
        }
        final GatewayAttemptOwnershipPermit permit = started.permit();
        final CompletionStage<SubmissionOutcomeMessageV1> stage;
        try {
            stage = submissions.submit(tenant, submission, permit);
        } catch (RuntimeException failure) {
            return finishAfterFailure(keyHash, submission, permit);
        }
        if (stage == null) {
            return finishAfterFailure(keyHash, submission, permit);
        }
        try {
            return stage.handle((outcome, failure) -> {
                final SubmissionOutcomeMessageV1 resolved;
                if (failure != null || outcome == null) {
                    resolved = GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId());
                } else {
                    resolved = outcome;
                }
                try {
                    idempotency.finish(keyHash, permit.physicalAttemptId(), resolved);
                } catch (OxiaGatewaySessionUnavailableException sessionFailure) {
                    throw sessionFailure;
                } catch (RuntimeException ignored) {
                    return GatewaySubmissionOutcomeV1.submission(
                            GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId()));
                }
                return GatewaySubmissionOutcomeV1.submission(resolved);
            });
        } catch (RuntimeException failure) {
            return finishAfterFailure(keyHash, submission, permit);
        }
    }

    private CompletionStage<GatewaySubmissionOutcomeV1> finishAfterFailure(
            final io.nereusstream.delay.transport.Digest32 keyHash,
            final PreparedSubmissionV1 submission,
            final GatewayAttemptOwnershipPermit permit) {
        final SubmissionOutcomeMessageV1 outcome = permit.state() == TransportOwnershipState.LIBRARY_OWNED
                ? GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId())
                : GatewayOutcomeSupport.localDefinite(submission, StableCode.BROKER_RESOURCE_UNCERTIFIED);
        try {
            idempotency.finish(keyHash, permit.physicalAttemptId(), outcome);
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException ignored) {
            return completed(GatewaySubmissionOutcomeV1.submission(
                    GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId())));
        }
        return completed(GatewaySubmissionOutcomeV1.submission(outcome));
    }

    private static GatewaySubmissionOutcomeV1 preparationError(final StableCode code) {
        return GatewaySubmissionOutcomeV1.preparationError(
                StableErrorV1.of(FailureStageV1.PREPARATION, code, null, null, null, null));
    }

    private static <T> CompletionStage<T> completed(final T value) {
        return CompletableFuture.completedFuture(value);
    }

    @FunctionalInterface
    private interface CommandPreparation {
        io.nereusstream.delay.protocol.PreparedCommand prepare();
    }
}
