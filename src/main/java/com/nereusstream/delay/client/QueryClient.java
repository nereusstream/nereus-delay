package com.nereusstream.delay.client;

import com.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import com.nereusstream.delay.protocol.CommandQueryResponseV1;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportStateV1;
import com.nereusstream.delay.protocol.FirstScheduleEligibilityV1;
import com.nereusstream.delay.protocol.MessageQueryResponseV1;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import com.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import com.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import com.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import com.nereusstream.delay.protocol.PublicEvidenceRefV1;
import com.nereusstream.delay.protocol.UploadHandleKindV1;
import com.nereusstream.delay.runtime.CommandResult;
import java.util.concurrent.CompletionStage;

/** Query/application facade explicitly supplied to the production client builder. */
public interface QueryClient extends AutoCloseable {
    CompletionStage<CommandQueryResponseV1> getCommandResult(
            CommandQueuedReceiptV1 receipt,
            long nowEpochMs,
            long fullResultRetainUntilEpochMs,
            PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> getCommandResult(
            CommandQueuedReceiptV1 receipt,
            long nowEpochMs,
            CommandResultRetentionPolicy retentionPolicy,
            PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> awaitAppliedV1(
            CommandQueuedReceiptV1 receipt,
            long nowEpochMs,
            long fullResultRetainUntilEpochMs,
            PublicDestinationBindingViewV1 binding);

    CompletionStage<CommandQueryResponseV1> awaitAppliedV1(
            CommandQueuedReceiptV1 receipt,
            long nowEpochMs,
            CommandResultRetentionPolicy retentionPolicy,
            PublicDestinationBindingViewV1 binding);

    CompletionStage<MessageQueryResponseV1> getMessage(
            DelayMessageId messageId,
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
