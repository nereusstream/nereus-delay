package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.StableCode;

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

    public PreparedSubmissionAdapter(final WireCommandIngressAdapter managedIngress,
                                     final PinnedPulsarNativeSubmissionAdapter nativeSubmission) {
        this.managedIngress = Objects.requireNonNull(managedIngress, "managedIngress");
        this.nativeSubmission = Objects.requireNonNull(nativeSubmission, "nativeSubmission");
    }

    /**
     * Submits the exact prepared branch.  The managed receipt query boundary
     * is supplied by the route policy; native outcomes ignore it because a
     * native receipt has no managed query authority.
     */
    public CompletionStage<SubmissionOutcomeMessageV1> submit(final PreparedSubmissionV1 submission,
                                                              final long receiptQueryUntilEpochMs,
                                                              final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(submission, "submission");
        if (submission.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeFrameV1(submission.managedFrame());
            if (closeGuard.isClosed()) {
                return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                        WireIngressOutcomeSupport.localDefinite(command, StableCode.CLIENT_CLOSED)));
            }
            try {
                final CompletionStage<EnqueueOutcomeMessageV1> managedOutcome =
                        managedIngress.enqueueOutcomeV1(command, receiptQueryUntilEpochMs, physicalEnqueueAttemptId);
                if (managedOutcome == null) {
                    return managedFailure(command, physicalEnqueueAttemptId);
                }
                try {
                    return managedOutcome.thenApply(SubmissionOutcomeMessageV1::managed);
                } catch (RuntimeException registrationFailure) {
                    // The managed transport may already have reached Producer
                    // ownership. A wrapper callback-registration failure is
                    // therefore not evidence of non-persistence.
                    return managedFailure(command, physicalEnqueueAttemptId);
                }
            } catch (RuntimeException submissionFailure) {
                // Preserve the same conservative boundary if an adapter
                // throws while returning its CompletionStage.
                return managedFailure(command, physicalEnqueueAttemptId);
            }
        }
        // The native adapter has its own pinned close gate and therefore
        // returns the exact native-branch local-definite outcome when this
        // wrapper has already been fenced. Keep the prepared branch fixed.
        return nativeSubmission.submit(submission.nativePrepared(), physicalEnqueueAttemptId);
    }

    private static CompletionStage<SubmissionOutcomeMessageV1> managedFailure(final PreparedCommand command,
                                                                                 final byte[] physicalAttemptId) {
        try {
            return uncertainManaged(command, physicalAttemptId);
        } catch (RuntimeException invalidAttempt) {
            // An invalid physical attempt cannot identify a Producer call;
            // keep the local rejection definitive even on a broken wrapper.
            return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.INVALID_PREPARED_COMMAND)));
        }
    }

    private static CompletionStage<SubmissionOutcomeMessageV1> uncertainManaged(final PreparedCommand command,
                                                                                  final byte[] physicalAttemptId) {
        return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                WireIngressOutcomeSupport.uncertain(command, physicalAttemptId,
                        StableCode.ENQUEUE_RESULT_UNCERTAIN, null)));
    }

    @Override
    public void close() {
        closeGuard.close(() -> {
            RuntimeException failure = null;
            try {
                managedIngress.close();
            } catch (RuntimeException exception) {
                failure = exception;
            }
            try {
                nativeSubmission.close();
            } catch (RuntimeException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
            if (failure != null) {
                throw failure;
            }
        });
    }
}
