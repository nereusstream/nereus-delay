package com.nereusstream.delay.client;

import com.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import com.nereusstream.delay.adapter.QueuedReceiptQueryPolicy;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.FirstScheduleEligibility;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PublicDestinationBindingView;
import com.nereusstream.delay.protocol.PublicEvidenceRef;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.CommandResult;
import java.util.List;
import java.util.concurrent.CompletionStage;

/** Client contract separating preparation, queueing and authoritative application. */
public interface DelayClient extends AutoCloseable {
    PreparedCommand prepareSchedule(ScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareSchedule(CanonicalScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareLargeSchedule(LargeScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareLargeSchedule(
            CanonicalScheduleIntent intentWithoutPayload,
            long expectedPayloadLength,
            byte[] payloadSha256,
            long reservationTtlMs,
            PayloadProofTrustSetRef trustSet,
            ProfileRef objectStoreProfile,
            long retryUntilEpochMs);

    PreparedCommand prepareLargePayloadCommit(
            PayloadReservationReceipt reservation, CanonicalPayloadCommitProof proof, long retryUntilEpochMs);

    PreparedCommand prepareCancel(DelayMessageId messageId, MessagePrecondition precondition, long retryUntilEpochMs);

    PreparedCommand prepareCancel(DelayMessageId messageId, int expectedGeneration, long retryUntilEpochMs);

    PreparedCommand prepareReschedule(
            DelayMessageId messageId,
            MessagePrecondition precondition,
            long deliverAtEpochMs,
            long expireAtEpochMs,
            long retryUntilEpochMs);

    PreparedCommand prepareReschedule(
            DelayMessageId messageId,
            int expectedGeneration,
            long deliverAtEpochMs,
            long expireAtEpochMs,
            long retryUntilEpochMs);

    /**
     * Wraps one strict managed command as an immutable prepared-submission
     * branch. This performs no transport I/O and never selects a native path.
     */
    PreparedSubmission prepareManagedSubmission(PreparedCommand command);

    /**
     * Prepares and freezes one Schedule branch through the shared zero-I/O
     * Semantic Core. AUTO_FAST eligibility comes only from the configured
     * local verified-snapshot provider; callers cannot supply a target or
     * credential candidate.
     */
    PreparedSubmission prepareScheduleSubmission(
            CanonicalScheduleIntent intent, long retryUntilEpochMs, SubmissionMode submissionMode);

    /**
     * Selects and freezes the managed/native branch before any transport I/O.
     * An ineligible native candidate returns the exact managed prepared frame.
     */
    PreparedSubmission prepareAutoFast(AutoFastSchedule request);

    /** Selects each AUTO_FAST item independently and preserves input order. */
    List<PreparedSubmission> prepareAutoFastBatch(List<AutoFastSchedule> requests);

    /**
     * Submits the exact prepared branch through the configured transport seam.
     * A retry must reuse the same submission and physical attempt identity.
     */
    CompletionStage<SubmissionOutcomeMessage> submit(
            PreparedSubmission submission, long receiptQueryUntilEpochMs, byte[] physicalEnqueueAttemptId);

    /**
     * Strict managed submission using the immutable Route query-policy
     * snapshot; callers cannot supply an absolute receipt boundary.
     */
    CompletionStage<SubmissionOutcomeMessage> submit(
            PreparedSubmission submission, QueuedReceiptQueryPolicy routePolicy, byte[] physicalEnqueueAttemptId);

    /** Direct embedded ingress for an already prepared command. */
    CompletionStage<EnqueueOutcome> enqueue(PreparedCommand command);

    /** Direct embedded batch ingress with independent ordered outcomes. */
    CompletionStage<List<EnqueueOutcome>> enqueueBatch(List<PreparedCommand> commands);

    /** Queries the command result using its queued receipt and source barrier. */
    CompletionStage<CommandQueryResponse> getCommandResult(
            CanonicalCommandQueuedReceipt receipt,
            long nowEpochMs,
            long fullResultRetainUntilEpochMs,
            PublicDestinationBindingView binding);

    /** Queries with a retention boundary derived from the result Source Position. */
    CompletionStage<CommandQueryResponse> getCommandResult(
            CanonicalCommandQueuedReceipt receipt,
            long nowEpochMs,
            CommandResultRetentionPolicy retentionPolicy,
            PublicDestinationBindingView binding);

    CompletionStage<CommandQueryResponse> awaitApplied(
            CanonicalCommandQueuedReceipt receipt,
            long nowEpochMs,
            long fullResultRetainUntilEpochMs,
            PublicDestinationBindingView binding);

    /** Awaits application using the immutable result-retention policy. */
    CompletionStage<CommandQueryResponse> awaitApplied(
            CanonicalCommandQueuedReceipt receipt,
            long nowEpochMs,
            CommandResultRetentionPolicy retentionPolicy,
            PublicDestinationBindingView binding);

    /** Queries the message projection with caller-supplied bounded policy inputs. */
    CompletionStage<MessageQueryResponse> getMessage(
            DelayMessageId messageId,
            PublicDestinationBindingView binding,
            DlqExportState dlqExportState,
            PublicEvidenceRef evidence,
            FirstScheduleEligibility unknownEligibility);

    CompletionStage<PayloadUploadHandleResponse> issuePayloadUploadHandle(
            PayloadReservationReceipt reservation, UploadHandleKind kind, long nowEpochMs);

    CompletionStage<PayloadAttestationResponse> attestPayloadUpload(
            PayloadReservationReceipt reservation, OpaquePayloadUploadHandle handle, long nowEpochMs);

    CompletionStage<CommandResult> awaitApplied(CommandQueuedReceipt receipt);

    @Override
    void close();
}
