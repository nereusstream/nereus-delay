package io.nereusstream.delay.adapter;

import io.nereusstream.delay.client.CommandQueuedReceipt;
import io.nereusstream.delay.client.EnqueueOutcome;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka ingress adapter whose transport is required to carry the exact pinned
 * native topic UUID in the Produce request. A stock name-only producer cannot
 * implement this interface safely.
 */
public final class PinnedKafkaCommandIngress implements CommandIngressAdapter {
    private final KafkaIngressResource resource;
    private final KafkaProduceTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean();

    public PinnedKafkaCommandIngress(final KafkaIngressResource resource, final KafkaProduceTransport transport) {
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
        final KafkaProduceRequest request;
        try {
            request = KafkaProduceRequest.from(resource, command, CommandCodec.encodeFrame(command));
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.definitelyNotQueued(command, StableCode.INVALID_PREPARED_COMMAND.wireValue()));
        }
        final CompletionStage<KafkaProduceResult> result;
        try {
            result = transport.produce(request);
        } catch (RuntimeException exception) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        if (result == null) {
            return completed(EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
        }
        return result.handle((produce, error) -> error == null ? map(command, produce)
                : EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue()));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            transport.close();
        }
    }

    private EnqueueOutcome map(final PreparedCommand command, final KafkaProduceResult result) {
        if (result == null) {
            return EnqueueOutcome.uncertain(command, StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
        }
        return switch (result.disposition()) {
            case DEFINITIVELY_NOT_PERSISTED -> EnqueueOutcome.definitelyNotQueued(command, result.stableCode());
            case UNKNOWN -> EnqueueOutcome.uncertain(command, result.stableCode());
            case PERSISTED -> persisted(command, result);
        };
    }

    private EnqueueOutcome persisted(final PreparedCommand command, final KafkaProduceResult result) {
        if (!resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                || !resource.nativeTopicUuid().equals(result.nativeTopicUuid())
                || resource.partition() != result.partition()) {
            return EnqueueOutcome.uncertain(command, StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        final KafkaSourcePosition position = new KafkaSourcePosition(command.shardId(),
                result.authenticatedClusterId(), result.nativeTopicUuid(), result.offset(), result.leaderEpoch(),
                result.brokerLogAppendTimeEpochMs());
        return EnqueueOutcome.queued(command, new CommandQueuedReceipt(command.commandId(), command.delayMessageId(),
                command.shardId(), position));
    }

    private static CompletionStage<EnqueueOutcome> completed(final EnqueueOutcome outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    @FunctionalInterface
    public interface KafkaProduceTransport extends AutoCloseable {
        CompletionStage<KafkaProduceResult> produce(KafkaProduceRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
