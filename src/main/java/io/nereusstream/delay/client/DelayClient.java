package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.FirstScheduleEligibilityV1;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.PublicEvidenceRefV1;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.CommandResult;

import java.util.List;
import java.util.concurrent.CompletionStage;

/** Client contract separating preparation, queueing and authoritative application. */
public interface DelayClient extends AutoCloseable {
    PreparedCommand prepareSchedule(ScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareScheduleV1(ScheduleIntentV1 intent, long retryUntilEpochMs);

    PreparedCommand prepareLargeSchedule(LargeScheduleIntent intent, long retryUntilEpochMs);

    PreparedCommand prepareLargeScheduleV1(ScheduleIntentV1 intentWithoutPayload,
                                           long expectedPayloadLength, byte[] payloadSha256,
                                           long reservationTtlMs, PayloadProofTrustSetRefV1 trustSet,
                                           long retryUntilEpochMs);

    PreparedCommand prepareLargePayloadCommit(PayloadReservationReceiptV1 reservation,
                                               PayloadCommitProofV1 proof, long retryUntilEpochMs);

    PreparedCommand prepareCancel(DelayMessageId messageId, int expectedGeneration, long retryUntilEpochMs);

    PreparedCommand prepareCancelV1(DelayMessageId messageId, MessagePreconditionV1 precondition,
                                    long retryUntilEpochMs);

    PreparedCommand prepareReschedule(DelayMessageId messageId, int expectedGeneration, long deliverAtEpochMs,
                                      long expireAtEpochMs, long retryUntilEpochMs);

    PreparedCommand prepareRescheduleV1(DelayMessageId messageId, MessagePreconditionV1 precondition,
                                        long deliverAtEpochMs, long expireAtEpochMs, long retryUntilEpochMs);

    /**
     * Wraps one strict V1 managed command as an immutable prepared-submission
     * branch. This performs no transport I/O and never selects a native path.
     */
    PreparedSubmissionV1 prepareManagedSubmissionV1(PreparedCommand command);

    /**
     * Selects and freezes the managed/native branch before any transport I/O.
     * An ineligible native candidate returns the exact managed prepared frame.
     */
    PreparedSubmissionV1 prepareAutoFast(AutoFastSchedule request);

    /**
     * Submits the exact prepared branch through the configured transport seam.
     * A retry must reuse the same submission and physical attempt identity.
     */
    CompletionStage<SubmissionOutcomeMessageV1> submit(PreparedSubmissionV1 submission,
                                                        long receiptQueryUntilEpochMs,
                                                        byte[] physicalEnqueueAttemptId);

    CompletionStage<EnqueueOutcome> enqueue(PreparedCommand command);

    /** Strict V1 ingress; legacy command bodies are rejected before source admission. */
    CompletionStage<EnqueueOutcome> enqueueV1(PreparedCommand command);

    /** Enqueues each prepared command independently and returns outcomes in input order. */
    CompletionStage<List<EnqueueOutcome>> enqueueBatch(List<PreparedCommand> commands);

    /** Strict V1 batch ingress with independent ordered outcomes. */
    CompletionStage<List<EnqueueOutcome>> enqueueBatchV1(List<PreparedCommand> commands);

    /** Queries the V1 command result using its queued receipt and source barrier. */
    CompletionStage<CommandQueryResponseV1> getCommandResult(CommandQueuedReceiptV1 receipt,
                                                              long nowEpochMs,
                                                              long fullResultRetainUntilEpochMs,
                                                              PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> awaitAppliedV1(CommandQueuedReceiptV1 receipt,
                                                            long nowEpochMs,
                                                            long fullResultRetainUntilEpochMs,
                                                            PublicDestinationBindingViewV1 binding);

    /** Queries the V1 message projection with caller-supplied bounded policy inputs. */
    CompletionStage<MessageQueryResponseV1> getMessage(DelayMessageId messageId,
                                                        PublicDestinationBindingViewV1 binding,
                                                        DlqExportStateV1 dlqExportState,
                                                        PublicEvidenceRefV1 evidence,
                                                        FirstScheduleEligibilityV1 unknownEligibility);

    CompletionStage<PayloadUploadHandleResponseV1> issuePayloadUploadHandle(
            PayloadReservationReceiptV1 reservation, UploadHandleKindV1 kind, long nowEpochMs);

    CompletionStage<PayloadAttestationResponseV1> attestPayloadUpload(
            PayloadReservationReceiptV1 reservation, OpaquePayloadUploadHandleV1 handle, long nowEpochMs);

    CompletionStage<CommandResult> awaitApplied(CommandQueuedReceipt receipt);

    @Override
    void close();
}
