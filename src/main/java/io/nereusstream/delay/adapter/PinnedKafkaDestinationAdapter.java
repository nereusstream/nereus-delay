package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Kafka target adapter requiring a request-level pinned topic UUID transport. */
public final class PinnedKafkaDestinationAdapter implements DestinationPublishAdapter {
    private final KafkaTargetResource resource;
    private final KafkaDestinationTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean();

    public PinnedKafkaDestinationAdapter(final KafkaTargetResource resource,
                                         final KafkaDestinationTransport transport) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        if (closed.get()) {
            return completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final KafkaDestinationRequest transportRequest;
        try {
            transportRequest = KafkaDestinationRequest.from(resource, request);
        } catch (RuntimeException exception) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
        }
        final CompletionStage<DestinationPublishResult> result;
        try {
            result = transport.publish(transportRequest);
        } catch (RuntimeException exception) {
            return completed(DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        }
        if (result == null) {
            return completed(DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        }
        return result.handle((value, error) -> error == null && value != null ? value
                : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            transport.close();
        }
    }

    private static CompletionStage<DestinationPublishResult> completed(final DestinationPublishResult result) {
        return CompletableFuture.completedFuture(result);
    }

    @FunctionalInterface
    public interface KafkaDestinationTransport extends AutoCloseable {
        CompletionStage<DestinationPublishResult> publish(KafkaDestinationRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
