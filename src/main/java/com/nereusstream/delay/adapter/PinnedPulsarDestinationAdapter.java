package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Pulsar target adapter requiring a Broker guard for every SEND. */
public final class PinnedPulsarDestinationAdapter implements DestinationPublishAdapter {
    private final PulsarTargetResource resource;
    private final PulsarDestinationTransport transport;
    private final PulsarDestinationTimingPolicy timingPolicy;
    private final CloseGuard closeGuard = new CloseGuard();

    public PinnedPulsarDestinationAdapter(
            final PulsarTargetResource resource, final PulsarDestinationTransport transport) {
        this(resource, transport, PulsarDestinationTimingPolicy.ordinaryManaged());
    }

    /**
     * Creates an adapter with an explicit managed timing policy. The policy
     * is a local pre-transport guard; Profile/capability authority remains
     * responsible for deciding whether the certified handoff is permitted.
     */
    public PinnedPulsarDestinationAdapter(
            final PulsarTargetResource resource,
            final PulsarDestinationTransport transport,
            final PulsarDestinationTimingPolicy timingPolicy) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.timingPolicy = Objects.requireNonNull(timingPolicy, "timingPolicy");
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        return closeGuard.invokeIfOpen(
                () -> publishOpen(request),
                () -> completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null)));
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(
            final DestinationPublishRequest request,
            final SourcePosition sourcePosition,
            final byte[] preparedPublishHash) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        return closeGuard.invokeIfOpen(
                () -> publishOpen(request, sourcePosition, preparedPublishHash),
                () -> completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null)));
    }

    private CompletionStage<DestinationPublishResult> publishOpen(final DestinationPublishRequest request) {
        return publishOpen(request, null, null);
    }

    private CompletionStage<DestinationPublishResult> publishOpen(
            final DestinationPublishRequest request,
            final SourcePosition sourcePosition,
            final byte[] preparedPublishHash) {
        // The source and target adapters may differ. The source position is
        // still bound to the exact Delay Shard, while the target transport
        // proves the independent Pulsar resource identity and SEND outcome.
        if (sourcePosition != null
                && !request.delayMessageId().routingId().shardId().equals(sourcePosition.shardId())) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
        }
        try {
            timingPolicy.validate(request);
        } catch (RuntimeException exception) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
        }
        final PulsarDestinationRequest transportRequest;
        try {
            transportRequest = PulsarDestinationRequest.from(resource, request);
        } catch (RuntimeException exception) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
        }
        final CompletionStage<DestinationPublishResult> result;
        try {
            result = sourcePosition == null
                    ? transport.publish(transportRequest)
                    : transport.publish(transportRequest, sourcePosition, preparedPublishHash);
        } catch (RuntimeException exception) {
            // A synchronous transport exception does not prove that the
            // request stopped before Producer ownership. Mark the logical
            // UNKNOWN so the physical-admission wrapper retains its charge.
            return UnobservedDestinationPublishStage.unknown();
        }
        if (result == null) {
            // A missing stage provides no physical completion observation.
            return UnobservedDestinationPublishStage.unknown();
        }
        try {
            final CompletionStage<DestinationPublishResult> handled =
                    result.handle((value, error) -> error == null && value != null
                            ? validate(value)
                            : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
            return handled == null ? UnobservedDestinationPublishStage.unknown() : handled;
        } catch (RuntimeException registrationFailure) {
            try {
                final CompletableFuture<DestinationPublishResult> future = result.toCompletableFuture();
                if (future == null) {
                    throw new IllegalStateException("CompletionStage returned a null CompletableFuture view");
                }
                final CompletionStage<DestinationPublishResult> handled =
                        future.handle((value, error) -> error == null && value != null
                                ? validate(value)
                                : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
                return handled == null ? UnobservedDestinationPublishStage.unknown() : handled;
            } catch (RuntimeException fallbackFailure) {
                // Callback registration itself is not evidence that the
                // Broker did not publish after Producer ownership. Return a
                // marked logical UNKNOWN so a physical-admission wrapper can
                // retain the charge for certified teardown/release.
                return UnobservedDestinationPublishStage.unknown();
            }
        }
    }

    @Override
    public void close() {
        closeGuard.close(transport::close);
    }

    private static CompletionStage<DestinationPublishResult> completed(final DestinationPublishResult result) {
        return CompletableFuture.completedFuture(result);
    }

    private DestinationPublishResult validate(final DestinationPublishResult result) {
        if (result.disposition() != DestinationPublishResult.Disposition.PUBLISHED) {
            return result;
        }
        final BrokerResourceIdentity identity = result.brokerResource();
        if (identity == null
                || identity.kind() != BrokerResourceIdentity.Kind.PULSAR
                || !resource.authenticatedClusterId().equals(identity.pulsar().authenticatedClusterId())
                || !java.util.Arrays.equals(
                        resource.resourceIncarnation(), identity.pulsar().resourceIncarnation())
                || !resource.physicalTopic().equals(identity.pulsar().physicalTopic())
                || resource.physicalTopicCreationTimestamp()
                        != identity.pulsar().physicalTopicCreationTimestamp()
                || resource.partition() != result.brokerPartition()) {
            return DestinationPublishResult.unknown(StableCode.RESOURCE_INCARNATION_MISMATCH, result.evidence());
        }
        return result;
    }

    @FunctionalInterface
    public interface PulsarDestinationTransport extends AutoCloseable {
        CompletionStage<DestinationPublishResult> publish(PulsarDestinationRequest request);

        /** Source-bound transport hook; legacy transports use the request-only path. */
        default CompletionStage<DestinationPublishResult> publish(
                final PulsarDestinationRequest request,
                final SourcePosition sourcePosition,
                final byte[] preparedPublishHash) {
            Objects.requireNonNull(sourcePosition, "sourcePosition");
            Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
            return publish(request);
        }

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
