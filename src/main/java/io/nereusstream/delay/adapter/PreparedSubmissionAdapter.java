package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Dispatches an already prepared, immutable branch without reselecting a
 * managed/native path after I/O or uncertainty.
 */
public final class PreparedSubmissionAdapter implements AutoCloseable {
    private final WireCommandIngressAdapter managedIngress;
    private final PinnedPulsarNativeSubmissionAdapter nativeSubmission;

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
            final PreparedCommand command = CommandCodec.decodeFrame(submission.managedFrame());
            return managedIngress.enqueueOutcomeV1(command, receiptQueryUntilEpochMs, physicalEnqueueAttemptId)
                    .thenApply(SubmissionOutcomeMessageV1::managed);
        }
        return nativeSubmission.submit(submission.nativePrepared(), physicalEnqueueAttemptId);
    }

    @Override
    public void close() {
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
    }
}
