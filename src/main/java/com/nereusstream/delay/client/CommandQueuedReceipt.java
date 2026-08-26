package com.nereusstream.delay.client;

import com.nereusstream.delay.adapter.QueuedReceiptQueryPolicy;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt.SafeBrokerAck;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import java.util.Objects;

/** Receipt proving only durable ingress queuing, never authoritative apply. */
public record CommandQueuedReceipt(
        CommandId commandId, DelayMessageId delayMessageId, ShardId shardId, SourcePosition sourcePosition) {
    public CommandQueuedReceipt {
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!shardId.equals(commandId.routingId().shardId())
                || !shardId.equals(delayMessageId.routingId().shardId())
                || !shardId.equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("receipt identity does not belong to shard");
        }
    }

    /**
     * Projects this legacy in-memory locator into the canonical serializable
     *Current receipt after the caller supplies the immutable query policy boundary
     * and the per-attempt Broker evidence required by the wire contract.
     */
    public CanonicalCommandQueuedReceipt to(
            final PreparedCommand preparedCommand,
            final SafeBrokerAck brokerAck,
            final long receiptQueryUntilEpochMs,
            final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(preparedCommand, "preparedCommand");
        if (!commandId.equals(preparedCommand.commandId())
                || !delayMessageId.equals(preparedCommand.delayMessageId())
                || !shardId.equals(preparedCommand.shardId())) {
            throw new IllegalArgumentException("prepared command does not match queued receipt locator");
        }
        return CanonicalCommandQueuedReceipt.create(
                preparedCommand, sourcePosition, brokerAck, receiptQueryUntilEpochMs, physicalEnqueueAttemptId);
    }

    /** Projects using the immutable Route policy rather than a caller timestamp. */
    public CanonicalCommandQueuedReceipt to(
            final PreparedCommand preparedCommand,
            final SafeBrokerAck brokerAck,
            final QueuedReceiptQueryPolicy routePolicy,
            final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(routePolicy, "routePolicy");
        return to(preparedCommand, brokerAck, routePolicy.queryUntil(sourcePosition), physicalEnqueueAttemptId);
    }
}
