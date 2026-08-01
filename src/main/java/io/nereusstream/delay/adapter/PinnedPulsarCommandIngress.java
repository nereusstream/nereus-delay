package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.CommandQueuedReceipt;
import io.nereusstream.delay.client.EnqueueOutcome;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Pulsar ingress adapter requiring a per-SEND resource guard at Broker ownership. */
public final class PinnedPulsarCommandIngress implements CommandIngressAdapter {
    private final PulsarIngressResource resource;
    private final PulsarSendTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean();

    public PinnedPulsarCommandIngress(final PulsarIngressResource resource, final PulsarSendTransport transport) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    @Override
    public CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        Objects.requireNonNull(command, "command");
        if (closed.get()) {
            return completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.CLIENT_CLOSED.wireValue()));
        }
        if (!resource.shardId().equals(command.shardId())) {
            return completed(EnqueueOutcome.definitelyNotQueued(command,
                    StableCode.INGRESS_ROUTE_MISMATCH.wireValue()));
        }
        final PulsarSendRequest request;
        try {
            request = PulsarSendRequest.from(resource, command, CommandCodec.encodeFrame(command));
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.INVALID_PREPARED_COMMAND.wireValue()));
        }
        final CompletionStage<PulsarSendResult> result;
        try {
            result = transport.send(request);
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        if (result == null) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        return result.handle((send, error) -> error == null ? map(command, send)
                : EnqueueOutcome.uncertain(command, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue()));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            transport.close();
        }
    }

    private EnqueueOutcome map(final PreparedCommand command, final PulsarSendResult result) {
        if (result == null) {
            return EnqueueOutcome.uncertain(command, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN.wireValue());
        }
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> EnqueueOutcome.definitelyNotQueued(command, result.stableCode());
            case UNKNOWN -> EnqueueOutcome.uncertain(command, result.stableCode());
            case PERSISTED -> persisted(command, result);
        };
    }

    private EnqueueOutcome persisted(final PreparedCommand command, final PulsarSendResult result) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !java.util.Arrays.equals(resource.resourceIncarnation(), result.resourceIncarnation())
                || !resource.physicalTopic().equals(result.physicalTopic())
                || resource.physicalTopicCreationTimestamp() != result.physicalTopicCreationTimestamp()
                || resource.partition() != result.partition()) {
            return EnqueueOutcome.uncertain(command, StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        final PulsarSourcePosition position = new PulsarSourcePosition(command.shardId(),
                result.resourceIncarnation(), result.physicalTopic(), result.ledgerId(), result.entryId(),
                result.batchIndex(), result.batchSize(), result.batched()
                        ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                result.brokerEntryTimestampEpochMs());
        return EnqueueOutcome.queued(command, new CommandQueuedReceipt(command.commandId(), command.delayMessageId(),
                command.shardId(), position));
    }

    private static CompletionStage<EnqueueOutcome> completed(final EnqueueOutcome outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    @FunctionalInterface
    public interface PulsarSendTransport extends AutoCloseable {
        CompletionStage<PulsarSendResult> send(PulsarSendRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
