package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessage;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Dispatches an already prepared, immutable branch without reselecting a
 * managed/native path after I/O or uncertainty.
 */
public final class PreparedSubmissionAdapter implements AutoCloseable {
    private final WireCommandIngressAdapter managedIngress;
    private final PinnedPulsarNativeSubmissionAdapter nativeSubmission;
    private final CloseGuard closeGuard = new CloseGuard();

    public PreparedSubmissionAdapter(
            final WireCommandIngressAdapter managedIngress,
            final PinnedPulsarNativeSubmissionAdapter nativeSubmission) {
        this.managedIngress = Objects.requireNonNull(managedIngress, "managedIngress");
        this.nativeSubmission = Objects.requireNonNull(nativeSubmission, "nativeSubmission");
    }

    /**
     * Submits the exact prepared branch. The managed receipt query boundary
     * is supplied by the route policy; native outcomes ignore it because a
     * native receipt has no managed query authority.
     */
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final PreparedSubmission submission,
            final long receiptQueryUntilEpochMs,
            final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(submission, "submission");
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeManagedFrame(submission.managedFrame());
            return closeGuard.invokeIfOpen(
                    () -> submitManaged(command, receiptQueryUntilEpochMs, physicalEnqueueAttemptId),
                    () -> CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                            WireIngressOutcomeSupport.localDefinite(command, StableCode.CLIENT_CLOSED))));
        }
        // The native adapter has its own pinned close gate and therefore
        // returns the exact native-branch local-definite outcome when this
        // wrapper has already been fenced. Keep the prepared branch fixed.
        return nativeSubmission.submitPreparedSubmission(submission, physicalEnqueueAttemptId);
    }

    /**
     * Strict managed submission path. The policy is bound by the ingress
     * adapter and checked again here so a caller cannot replace it with an
     * absolute SDK timestamp or a different Route snapshot.
     */
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final PreparedSubmission submission,
            final QueuedReceiptQueryPolicy routePolicy,
            final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(submission, "submission");
        Objects.requireNonNull(routePolicy, "routePolicy");
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeManagedFrame(submission.managedFrame());
            return closeGuard.invokeIfOpen(
                    () -> submitManaged(command, routePolicy, physicalEnqueueAttemptId),
                    () -> CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                            WireIngressOutcomeSupport.localDefinite(command, StableCode.CLIENT_CLOSED))));
        }
        // Native receipts do not carry managed query authority; retain the
        // already prepared native branch and ignore the managed policy.
        return nativeSubmission.submitPreparedSubmission(submission, physicalEnqueueAttemptId);
    }

    /**
     * Sends the exact H4/H5 native record projection. The branch and record
     * are passed separately so callers cannot silently fall back to the
     * envelope-only compatibility path after preparation.
     */
    public CompletionStage<SubmissionOutcomeMessage> submitNativePreparedRecord(
            final NativePreparedDelivery prepared,
            final PulsarPreparedRecord record,
            final ArtifactGenerationSet artifacts,
            final byte[] physicalEnqueueAttemptId) {
        return nativeSubmission.submitPreparedRecord(prepared, record, artifacts, physicalEnqueueAttemptId);
    }

    private CompletionStage<SubmissionOutcomeMessage> submitManaged(
            final PreparedCommand command, final long receiptQueryUntilEpochMs, final byte[] physicalEnqueueAttemptId) {
        try {
            final CompletionStage<EnqueueOutcomeMessage> managedOutcome =
                    managedIngress.enqueueOutcome(command, receiptQueryUntilEpochMs, physicalEnqueueAttemptId);
            if (managedOutcome == null) {
                return managedFailure(command, physicalEnqueueAttemptId);
            }
            try {
                final CompletionStage<SubmissionOutcomeMessage> handled = managedOutcome.handle(
                        (outcome, error) -> managedOutcome(command, physicalEnqueueAttemptId, outcome, error));
                return handled == null ? managedFailure(command, physicalEnqueueAttemptId) : handled;
            } catch (RuntimeException registrationFailure) {
                // The managed transport may already have reached Producer
                // ownership. A wrapper callback-registration failure is
                // therefore not evidence of non-persistence.
                return managedFailure(command, physicalEnqueueAttemptId);
            }
        } catch (RuntimeException submissionFailure) {
            // Preserve the same conservative boundary if an adapter throws
            // while returning its CompletionStage.
            return managedFailure(command, physicalEnqueueAttemptId);
        }
    }

    private CompletionStage<SubmissionOutcomeMessage> submitManaged(
            final PreparedCommand command,
            final QueuedReceiptQueryPolicy routePolicy,
            final byte[] physicalEnqueueAttemptId) {
        if (!(managedIngress instanceof PolicyBoundWireCommandIngressAdapter policyBoundIngress)) {
            return CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.ROUTE_SNAPSHOT_UNAVAILABLE)));
        }
        try {
            final CompletionStage<EnqueueOutcomeMessage> managedOutcome =
                    policyBoundIngress.enqueueOutcome(command, routePolicy, physicalEnqueueAttemptId);
            if (managedOutcome == null) {
                return managedFailure(command, physicalEnqueueAttemptId);
            }
            try {
                final CompletionStage<SubmissionOutcomeMessage> handled = managedOutcome.handle(
                        (outcome, error) -> managedOutcome(command, physicalEnqueueAttemptId, outcome, error));
                return handled == null ? managedFailure(command, physicalEnqueueAttemptId) : handled;
            } catch (RuntimeException registrationFailure) {
                return managedFailure(command, physicalEnqueueAttemptId);
            }
        } catch (RuntimeException submissionFailure) {
            return managedFailure(command, physicalEnqueueAttemptId);
        }
    }

    private static CompletionStage<SubmissionOutcomeMessage> managedFailure(
            final PreparedCommand command, final byte[] physicalAttemptId) {
        try {
            return CompletableFuture.completedFuture(managedFailureOutcome(command, physicalAttemptId));
        } catch (RuntimeException invalidAttempt) {
            // An invalid physical attempt cannot identify a Producer call;
            // keep the local rejection definitive even on a broken wrapper.
            return CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.INVALID_PREPARED_COMMAND)));
        }
    }

    /**
     * Projects both synchronous and asynchronous managed-stage failures. A
     * failed CompletionStage is not proof that the Broker rejected a request:
     * the managed transport may already have transferred Producer ownership.
     */
    private static SubmissionOutcomeMessage managedOutcome(
            final PreparedCommand command,
            final byte[] physicalAttemptId,
            final EnqueueOutcomeMessage outcome,
            final Throwable error) {
        if (error != null || outcome == null) {
            return managedFailureOutcome(command, physicalAttemptId);
        }
        try {
            return SubmissionOutcomeMessage.managed(outcome);
        } catch (RuntimeException malformedOutcome) {
            // A malformed stage value is no more evidence of non-persistence
            // than an exceptional completion.
            return managedFailureOutcome(command, physicalAttemptId);
        }
    }

    private static SubmissionOutcomeMessage managedFailureOutcome(
            final PreparedCommand command, final byte[] physicalAttemptId) {
        try {
            return SubmissionOutcomeMessage.managed(WireIngressOutcomeSupport.uncertain(
                    command, physicalAttemptId, StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        } catch (RuntimeException invalidAttempt) {
            // An invalid physical attempt cannot identify a Producer call;
            // keep the local rejection definitive even on a broken wrapper.
            return SubmissionOutcomeMessage.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.INVALID_PREPARED_COMMAND));
        }
    }

    @Override
    public void close() {
        closeGuard.close(() -> {
            Throwable failure = null;
            try {
                managedIngress.close();
            } catch (RuntimeException | Error exception) {
                failure = appendCloseFailure(failure, exception);
            }
            try {
                nativeSubmission.close();
            } catch (RuntimeException | Error exception) {
                failure = appendCloseFailure(failure, exception);
            }
            if (failure != null) {
                throwUnchecked(failure);
            }
        });
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }
}
