package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Pulsar target adapter requiring a Broker guard for every SEND. */
public final class PinnedPulsarDestinationAdapter implements DestinationPublishAdapter {
    private final PulsarTargetResource resource;
    private final PulsarDestinationTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean();

    public PinnedPulsarDestinationAdapter(final PulsarTargetResource resource,
                                          final PulsarDestinationTransport transport) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        if (closed.get()) {
            return completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final PulsarDestinationRequest transportRequest;
        try {
            transportRequest = PulsarDestinationRequest.from(resource, request);
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
        return result.handle((value, error) -> error == null && value != null ? validate(value)
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

    private DestinationPublishResult validate(final DestinationPublishResult result) {
        if (result.disposition() != DestinationPublishResult.Disposition.PUBLISHED) {
            return result;
        }
        final BrokerResourceIdentityV1 identity = result.brokerResource();
        if (identity == null || identity.kind() != BrokerResourceIdentityV1.Kind.PULSAR
                || !resource.authenticatedClusterId().equals(identity.pulsar().authenticatedClusterId())
                || !java.util.Arrays.equals(resource.resourceIncarnation(), identity.pulsar().resourceIncarnation())
                || !resource.physicalTopic().equals(identity.pulsar().physicalTopic())
                || resource.physicalTopicCreationTimestamp() != identity.pulsar().physicalTopicCreationTimestamp()
                || resource.partition() != result.brokerPartition()) {
            return DestinationPublishResult.unknown(StableCode.RESOURCE_INCARNATION_MISMATCH, result.evidence());
        }
        return result;
    }

    @FunctionalInterface
    public interface PulsarDestinationTransport extends AutoCloseable {
        CompletionStage<DestinationPublishResult> publish(PulsarDestinationRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
