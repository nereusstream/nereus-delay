package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import java.util.concurrent.CompletionStage;

/** Command ingress boundary that can emit the closed NDR1 managed outcome union. */
public interface WireCommandIngressAdapter extends CommandIngressAdapter {
    /**
     * Enqueues one prepared command and projects the exact Broker response into
     * NDR1.  {@code physicalEnqueueAttemptId} is required when the call reaches
     * Producer ownership; a local rejection may omit it.
     */
    CompletionStage<EnqueueOutcomeMessageV1> enqueueOutcomeV1(
            PreparedCommand command, long receiptQueryUntilEpochMs, byte[] physicalEnqueueAttemptId);
}
