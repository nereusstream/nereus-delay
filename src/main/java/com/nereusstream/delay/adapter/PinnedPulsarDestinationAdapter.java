package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
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
    private final boolean nativePreparedDeliveryEnabled;
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
        this(resource, transport, timingPolicy, false);
    }

    /**
     * Creates an adapter with an explicit activation bit for the native
     * prepared-record path. The default constructors keep H0 fail-closed;
     * only H6 activation may supply {@code true}.
     */
    public PinnedPulsarDestinationAdapter(
            final PulsarTargetResource resource,
            final PulsarDestinationTransport transport,
            final PulsarDestinationTimingPolicy timingPolicy,
            final boolean nativePreparedDeliveryEnabled) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.timingPolicy = Objects.requireNonNull(timingPolicy, "timingPolicy");
        this.nativePreparedDeliveryEnabled = nativePreparedDeliveryEnabled;
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

    /**
     * Sends a final prepared record only through a transport that explicitly
     * implements the current record projection. Native records remain blocked
     * unless the H6 activation bit was supplied at construction time.
     */
    public CompletionStage<DestinationPublishResult> publishPreparedRecord(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        return closeGuard.invokeIfOpen(
                () -> publishPreparedRecordOpen(record, artifacts),
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
        // H0 blocks the incomplete native handoff before timing-policy
        // validation can authorize any Pulsar transport ownership.
        if (request.actionAtEpochMs() < request.deliverAtEpochMs()) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null));
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

    private CompletionStage<DestinationPublishResult> publishPreparedRecordOpen(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
        final BrokerResourceIdentity target = record.template().targetResource();
        if (target.kind() != BrokerResourceIdentity.Kind.PULSAR
                || !resource.authenticatedClusterId().equals(target.pulsar().authenticatedClusterId())
                || !java.util.Arrays.equals(
                        resource.resourceIncarnation(), target.pulsar().resourceIncarnation())
                || !resource.physicalTopic().equals(target.pulsar().physicalTopic())
                || resource.physicalTopicCreationTimestamp() != target.pulsar().physicalTopicCreationTimestamp()
                || record.template().physicalPartition() != resource.partition()) {
            return completed(
                    DestinationPublishResult.definitelyNotPublished(StableCode.PREPARED_SUBMISSION_MISMATCH, null));
        }
        if (record.template().deliveryContract().isNative() && !nativePreparedDeliveryEnabled) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final CompletionStage<DestinationPublishResult> result;
        try {
            result = transport.publishPreparedRecord(record, artifacts);
        } catch (RuntimeException exception) {
            return UnobservedDestinationPublishStage.unknown();
        }
        if (result == null) {
            return UnobservedDestinationPublishStage.unknown();
        }
        try {
            return result.handle((value, error) -> error == null && value != null
                    ? validate(value)
                    : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null));
        } catch (RuntimeException registrationFailure) {
            return UnobservedDestinationPublishStage.unknown();
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

        /** Current final-record hook; legacy transports fail closed by default. */
        default CompletionStage<DestinationPublishResult> publishPreparedRecord(
                final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(artifacts, "artifacts");
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }

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
