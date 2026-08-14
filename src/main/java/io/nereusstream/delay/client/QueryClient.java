package io.nereusstream.delay.client;

import io.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.FirstScheduleEligibilityV1;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.PublicEvidenceRefV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.CommandResult;

import java.util.concurrent.CompletionStage;

/** Query/application facade explicitly supplied to the production client builder. */
public interface QueryClient extends AutoCloseable {
    CompletionStage<CommandQueryResponseV1> getCommandResult(CommandQueuedReceiptV1 receipt, long nowEpochMs,
                                                             long fullResultRetainUntilEpochMs,
                                                             PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> getCommandResult(CommandQueuedReceiptV1 receipt, long nowEpochMs,
                                                             CommandResultRetentionPolicy retentionPolicy,
                                                             PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> awaitAppliedV1(CommandQueuedReceiptV1 receipt, long nowEpochMs,
                                                           long fullResultRetainUntilEpochMs,
                                                           PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> awaitAppliedV1(CommandQueuedReceiptV1 receipt, long nowEpochMs,
                                                           CommandResultRetentionPolicy retentionPolicy,
                                                           PublicDestinationBindingViewV1 binding);

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
    default void close() {
        // Query implementations with resources override this method.
    }
}
