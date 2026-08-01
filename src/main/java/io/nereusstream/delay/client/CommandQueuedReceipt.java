package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
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
}

