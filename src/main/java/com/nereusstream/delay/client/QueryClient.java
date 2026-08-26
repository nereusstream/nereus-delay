package com.nereusstream.delay.client;

import com.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.FirstScheduleEligibility;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PublicDestinationBindingView;
import com.nereusstream.delay.protocol.PublicEvidenceRef;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.CommandResult;
import java.util.concurrent.CompletionStage;

/** Query/application facade explicitly supplied to the production client builder. */
public interface QueryClient extends AutoCloseable {
    CompletionStage<CommandQueryResponse> getCommandResult(
            CanonicalCommandQueuedReceipt receipt,
            long nowEpochMs,
            long fullResultRetainUntilEpochMs,
            PublicDestinationBindingView binding);

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

    CompletionStage<CommandQueryResponse> awaitApplied(
            CanonicalCommandQueuedReceipt receipt,
            long nowEpochMs,
            CommandResultRetentionPolicy retentionPolicy,
            PublicDestinationBindingView binding);

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
    default void close() {
        // Query implementations with resources override this method.
    }
}
