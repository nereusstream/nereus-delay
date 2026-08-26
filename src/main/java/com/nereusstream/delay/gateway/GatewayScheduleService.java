package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparation;
import com.nereusstream.delay.semantic.SemanticPreparationException;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.submission.SubmissionCoordinator;
import com.nereusstream.delay.transport.GatewayAttemptOwnershipPermit;
import com.nereusstream.delay.transport.TransportOwnershipState;
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

    public GatewayScheduleService(
            final DelaySemanticCore semanticCore,
            final GatewayIdempotencyStore idempotency,
            final SubmissionCoordinator submissions,
            final TrustedClock trustedClock) {
        this.semanticCore = Objects.requireNonNull(semanticCore, "semanticCore");
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency");
        this.submissions = Objects.requireNonNull(submissions, "submissions");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    public CompletionStage<GatewaySubmissionOutcome> schedule(
            final AuthenticatedTenantContext tenant, final GatewayScheduleRequest request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        final com.nereusstream.delay.transport.Digest32 keyHash;
        final com.nereusstream.delay.transport.Digest32 bodyHash;
        try {
            keyHash = GatewayIdempotencyHash.keyHash(tenant.authenticatedTenantScopeHash(), request.idempotencyKey());
            bodyHash = GatewayIdempotencyHash.bodyHash(GatewayOperationKind.SCHEDULE, request.canonicalBodyBytes());
        } catch (RuntimeException invalidRequest) {
            return completed(preparationError(StableCode.INVALID_METADATA));
        }

        final GatewayIdempotencyStore.PrepareResult prepared;
        final PreparedSubmission submission;
        try {
            final GatewayIdempotencyRecord existing = idempotency.exact(keyHash);
            if (existing != null && !existing.requestBodyHash().equals(bodyHash)) {
                return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
            }
            if (existing != null) {
                submission = PreparedSubmission.decode(existing.preparedSubmissionBytes());
                prepared = new GatewayIdempotencyStore.PrepareResult(
                        GatewayIdempotencyStore.PrepareState.EXISTING_MATCH, existing);
            } else {
                submission = semanticCore.prepareSchedule(
                        tenant,
                        request.route(),
                        request.scheduleIntent(),
                        request.retryUntilEpochMs(),
                        request.submissionMode());
                prepared = idempotency.prepareIfAbsent(
                        keyHash, GatewayOperationKind.SCHEDULE, bodyHash, submission, request.retryUntilEpochMs());
            }
        } catch (SemanticPreparationException failure) {
            return completed(GatewaySubmissionOutcome.preparationError(failure.error()));
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return completed(preparationError(StableCode.INVALID_PREPARED_COMMAND));
        }
        if (prepared.state() == GatewayIdempotencyStore.PrepareState.CONFLICT) {
            return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        return continueAttempt(tenant, keyHash, submission);
    }

    /** Cancel path using the same durable prepared-bytes/attempt protocol as Schedule. */
    public CompletionStage<GatewaySubmissionOutcome> cancel(
            final AuthenticatedTenantContext tenant, final GatewayCancelRequest request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(
                tenant,
                request.idempotencyKey(),
                GatewayOperationKind.CANCEL,
                request.canonicalBodyBytes(),
                request.retryUntilEpochMs(),
                () -> semanticCore.prepareCancel(
                        tenant, request.delayMessageId(), request.messagePrecondition(), request.retryUntilEpochMs()));
    }

    /** Reschedule path using the same durable prepared-bytes/attempt protocol as Schedule. */
    public CompletionStage<GatewaySubmissionOutcome> reschedule(
            final AuthenticatedTenantContext tenant, final GatewayRescheduleRequest request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(
                tenant,
                request.idempotencyKey(),
                GatewayOperationKind.RESCHEDULE,
                request.canonicalBodyBytes(),
                request.retryUntilEpochMs(),
                () -> semanticCore.prepareReschedule(
                        tenant,
                        request.delayMessageId(),
                        request.messagePrecondition(),
                        request.deliverAtEpochMs(),
                        request.expireAtEpochMs(),
                        request.retryUntilEpochMs()));
    }

    /** PrepareLargeSchedule path using the shared durable prepared-bytes protocol. */
    public CompletionStage<GatewaySubmissionOutcome> prepareLargeSchedule(
            final AuthenticatedTenantContext tenant, final GatewayPrepareLargeScheduleRequest request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(
                tenant,
                request.idempotencyKey(),
                GatewayOperationKind.PREPARE_LARGE_SCHEDULE,
                request.canonicalBodyBytes(),
                request.retryUntilEpochMs(),
                () -> semanticCore.prepareLargeSchedule(
                        tenant,
                        request.route(),
                        new LargeSchedulePreparation(
                                request.scheduleIntent(),
                                request.expectedPayloadLength(),
                                request.payloadSha256(),
                                request.reservationTtlMs(),
                                request.trustSet(),
                                request.objectStoreProfile()),
                        request.retryUntilEpochMs()));
    }

    /** CommitLargeSchedule path using the shared durable prepared-bytes protocol. */
    public CompletionStage<GatewaySubmissionOutcome> commitLargeSchedule(
            final AuthenticatedTenantContext tenant, final GatewayCommitLargeScheduleRequest request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        return prepareCommand(
                tenant,
                request.idempotencyKey(),
                GatewayOperationKind.COMMIT_LARGE_SCHEDULE,
                request.canonicalBodyBytes(),
                request.retryUntilEpochMs(),
                () -> semanticCore.preparePayloadCommit(
                        tenant, request.reservation(), request.proof(), request.retryUntilEpochMs()));
    }

    private CompletionStage<GatewaySubmissionOutcome> continueAttempt(
            final AuthenticatedTenantContext tenant,
            final com.nereusstream.delay.transport.Digest32 keyHash,
            final PreparedSubmission submission) {
        return continueStartedAttempt(tenant, keyHash, submission, idempotency.startAttempt(keyHash));
    }

    private CompletionStage<GatewaySubmissionOutcome> prepareCommand(
            final AuthenticatedTenantContext tenant,
            final byte[] idempotencyKey,
            final GatewayOperationKind operation,
            final byte[] canonicalBody,
            final long retryUntilEpochMs,
            final CommandPreparation preparation) {
        final com.nereusstream.delay.transport.Digest32 keyHash;
        final com.nereusstream.delay.transport.Digest32 bodyHash;
        try {
            keyHash = GatewayIdempotencyHash.keyHash(tenant.authenticatedTenantScopeHash(), idempotencyKey);
            bodyHash = GatewayIdempotencyHash.bodyHash(operation, canonicalBody);
        } catch (RuntimeException invalidRequest) {
            return completed(preparationError(StableCode.INVALID_METADATA));
        }
        final GatewayIdempotencyStore.PrepareResult prepared;
        final PreparedSubmission submission;
        try {
            final GatewayIdempotencyRecord existing = idempotency.exact(keyHash);
            if (existing != null && !existing.requestBodyHash().equals(bodyHash)) {
                return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
            }
            if (existing != null) {
                submission = PreparedSubmission.decode(existing.preparedSubmissionBytes());
                prepared = new GatewayIdempotencyStore.PrepareResult(
                        GatewayIdempotencyStore.PrepareState.EXISTING_MATCH, existing);
            } else {
                submission = semanticCore.prepareManaged(tenant, preparation.prepare());
                prepared = idempotency.prepareIfAbsent(keyHash, operation, bodyHash, submission, retryUntilEpochMs);
            }
        } catch (SemanticPreparationException failure) {
            return completed(GatewaySubmissionOutcome.preparationError(failure.error()));
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return completed(preparationError(StableCode.INVALID_PREPARED_COMMAND));
        }
        if (prepared.state() == GatewayIdempotencyStore.PrepareState.CONFLICT) {
            return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        return continueAttempt(tenant, keyHash, submission);
    }

    /** Retries only the stored exact prepared bytes after an uncertain aggregate. */
    public CompletionStage<GatewaySubmissionOutcome> retryUncertain(
            final AuthenticatedTenantContext tenant, final GatewayRetryUncertainRequest request) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(request, "request");
        final com.nereusstream.delay.transport.Digest32 keyHash;
        try {
            keyHash = GatewayIdempotencyHash.keyHash(
                    tenant.authenticatedTenantScopeHash(), request.originalIdempotencyKey());
        } catch (RuntimeException invalidRequest) {
            return completed(preparationError(StableCode.INVALID_METADATA));
        }
        final GatewayIdempotencyRecord record = idempotency.exact(keyHash);
        if (record == null) {
            return completed(preparationError(StableCode.NOT_FOUND_OR_NOT_AUTHORIZED));
        }
        final PreparedSubmission submission;
        try {
            submission = PreparedSubmission.decode(record.preparedSubmissionBytes());
        } catch (RuntimeException malformed) {
            return completed(preparationError(StableCode.INTEGRITY_ERROR));
        }
        final GatewayIdempotencyStore.RetryStart started;
        try {
            started =
                    idempotency.startRetry(keyHash, request.expectedPriorPhysicalAttemptId(), request.retryRequestId());
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            return completed(preparationError(StableCode.INTEGRITY_ERROR));
        }
        if (started.state() == GatewayIdempotencyStore.RetryState.CONFLICT) {
            return completed(preparationError(StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        return continueStartedAttempt(
                tenant,
                keyHash,
                submission,
                new GatewayIdempotencyStore.AttemptStart(started.record(), started.permit()));
    }

    private CompletionStage<GatewaySubmissionOutcome> continueStartedAttempt(
            final AuthenticatedTenantContext tenant,
            final com.nereusstream.delay.transport.Digest32 keyHash,
            final PreparedSubmission submission,
            final GatewayIdempotencyStore.AttemptStart started) {
        if (started.permit() == null) {
            final byte[] aggregate = started.record().aggregateOutcomeBytes();
            if (aggregate != null) {
                try {
                    return completed(GatewaySubmissionOutcome.submission(SubmissionOutcomeMessage.decode(aggregate)));
                } catch (RuntimeException malformed) {
                    return completed(preparationError(StableCode.INTEGRITY_ERROR));
                }
            }
            if (started.record().attempts().isEmpty()) {
                try {
                    final StableCode code =
                            trustedClock.nowEpochMs() >= started.record().retainUntilEpochMs()
                                    ? StableCode.PREPARED_COMMAND_EXPIRED
                                    : StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED;
                    return completed(
                            GatewaySubmissionOutcome.submission(GatewayOutcomeSupport.localDefinite(submission, code)));
                } catch (RuntimeException unavailable) {
                    return completed(GatewaySubmissionOutcome.submission(
                            GatewayOutcomeSupport.localDefinite(submission, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE)));
                }
            }
            final GatewayPhysicalAttempt last =
                    started.record().attempts().get(started.record().attempts().size() - 1);
            return completed(GatewaySubmissionOutcome.submission(
                    GatewayOutcomeSupport.uncertain(submission, last.physicalAttemptId())));
        }
        final GatewayAttemptOwnershipPermit permit = started.permit();
        final CompletionStage<SubmissionOutcomeMessage> stage;
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
                final SubmissionOutcomeMessage resolved;
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
                    return GatewaySubmissionOutcome.submission(
                            GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId()));
                }
                return GatewaySubmissionOutcome.submission(resolved);
            });
        } catch (RuntimeException failure) {
            return finishAfterFailure(keyHash, submission, permit);
        }
    }

    private CompletionStage<GatewaySubmissionOutcome> finishAfterFailure(
            final com.nereusstream.delay.transport.Digest32 keyHash,
            final PreparedSubmission submission,
            final GatewayAttemptOwnershipPermit permit) {
        final SubmissionOutcomeMessage outcome = permit.state() == TransportOwnershipState.LIBRARY_OWNED
                ? GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId())
                : GatewayOutcomeSupport.localDefinite(submission, StableCode.BROKER_RESOURCE_UNCERTIFIED);
        try {
            idempotency.finish(keyHash, permit.physicalAttemptId(), outcome);
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (RuntimeException ignored) {
            return completed(GatewaySubmissionOutcome.submission(
                    GatewayOutcomeSupport.uncertain(submission, permit.physicalAttemptId())));
        }
        return completed(GatewaySubmissionOutcome.submission(outcome));
    }

    private static GatewaySubmissionOutcome preparationError(final StableCode code) {
        return GatewaySubmissionOutcome.preparationError(
                StableError.of(FailureStage.PREPARATION, code, null, null, null, null));
    }

    private static <T> CompletionStage<T> completed(final T value) {
        return CompletableFuture.completedFuture(value);
    }

    @FunctionalInterface
    private interface CommandPreparation {
        com.nereusstream.delay.protocol.PreparedCommand prepare();
    }
}
