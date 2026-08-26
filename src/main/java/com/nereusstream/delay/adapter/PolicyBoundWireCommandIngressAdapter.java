package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.EnqueueOutcomeMessage;
import com.nereusstream.delay.protocol.PreparedCommand;
import java.util.concurrent.CompletionStage;

/**
 * Strict managed ingress capability whose queued-receipt boundary is bound to
 * an immutable Route policy instead of an SDK-supplied absolute timestamp.
 */
public interface PolicyBoundWireCommandIngressAdapter extends WireCommandIngressAdapter {
    /**
     * Enqueues using the policy bound when the adapter was constructed. The
     * supplied policy is a binding assertion and must equal that snapshot.
     */
    CompletionStage<EnqueueOutcomeMessage> enqueueOutcome(
            PreparedCommand command, QueuedReceiptQueryPolicy routePolicy, byte[] physicalEnqueueAttemptId);
}
