package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1.SafeBrokerAck;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Objects;

/** Receipt proving only durable ingress queuing, never authoritative apply. */
public record CommandQueuedReceipt(
        CommandId commandId,
        DelayMessageId delayMessageId,
        ShardId shardId,
        SourcePosition sourcePosition) {
    public CommandQueuedReceipt {
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!shardId.equals(commandId.routingId().shardId()) || !shardId.equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("receipt identity does not belong to shard");
        }
    }

    /**
     * Projects this legacy in-memory locator into the canonical serializable
     * V1 receipt after the caller supplies the immutable query policy boundary
     * and the per-attempt Broker evidence required by the wire contract.
     */
    public CommandQueuedReceiptV1 toV1(final PreparedCommand preparedCommand, final SafeBrokerAck brokerAck,
                                       final long receiptQueryUntilEpochMs,
                                       final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(preparedCommand, "preparedCommand");
        if (!commandId.equals(preparedCommand.commandId()) || !delayMessageId.equals(preparedCommand.delayMessageId())
                || !shardId.equals(preparedCommand.shardId())) {
            throw new IllegalArgumentException("prepared command does not match queued receipt locator");
        }
        return CommandQueuedReceiptV1.create(preparedCommand, sourcePosition, brokerAck,
                receiptQueryUntilEpochMs, physicalEnqueueAttemptId);
    }
}
