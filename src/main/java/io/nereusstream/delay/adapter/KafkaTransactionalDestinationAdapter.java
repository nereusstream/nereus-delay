package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Kafka K2 target-plus-receipt adapter.
 *
 * <p>The exact local receipt mapping is persisted before the transport is
 * invoked. The transport receives a closed pair and must send both records in
 * one Kafka transaction. This adapter deliberately requires the authoritative
 * source position at publish time; the ordinary one-argument Destination
 * Publish API cannot manufacture that identity.</p>
 */
public final class KafkaTransactionalDestinationAdapter implements DestinationPublishAdapter {
    private final KafkaTargetResource targetResource;
    private final KafkaReceiptResource receiptResource;
    private final String targetPhysicalTopic;
    private final String receiptPhysicalTopic;
    private final KafkaReceiptJournal journal;
    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final byte[] transactionalIdentitySha256;
    private final KafkaTransactionalDestinationTransport transport;
    private final CloseGuard closeGuard = new CloseGuard();

    public KafkaTransactionalDestinationAdapter(final KafkaTargetResource targetResource,
                                                final KafkaReceiptResource receiptResource,
                                                final String targetPhysicalTopic,
                                                final String receiptPhysicalTopic,
                                                final KafkaReceiptJournal journal,
                                                final DestinationLaneId laneId,
                                                final byte[] laneIncarnation,
                                                final byte[] transactionalIdentitySha256,
                                                final KafkaTransactionalDestinationTransport transport) {
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        this.receiptResource = Objects.requireNonNull(receiptResource, "receiptResource");
        this.targetPhysicalTopic = requireTopic(targetPhysicalTopic, "targetPhysicalTopic");
        this.receiptPhysicalTopic = requireTopic(receiptPhysicalTopic, "receiptPhysicalTopic");
        this.journal = Objects.requireNonNull(journal, "journal");
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        Bytes.requireLength(transactionalIdentitySha256, 32, "transactionalIdentitySha256");
        this.laneIncarnation = Bytes.copy(laneIncarnation);
        this.transactionalIdentitySha256 = Bytes.copy(transactionalIdentitySha256);
        this.transport = Objects.requireNonNull(transport, "transport");
        if (!targetResource.authenticatedClusterId().equals(receiptResource.authenticatedClusterId())) {
            throw new IllegalArgumentException("Kafka target and receipt must use the same authenticated cluster");
        }
    }

    /**
     * The generic adapter method cannot prove the source position required by
     * the receipt journal, so it remains conservatively unavailable.
     */
    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        return closeGuard.invokeIfOpen(() -> completed(DestinationPublishResult.unknown(
                        StableCode.CAPABILITY_UNAVAILABLE, null)),
                () -> completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null)));
    }

    /** Publishes the exact target and receipt pair using the supplied source authority. */
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request,
                                                              final SourcePosition sourcePosition) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        return closeGuard.invokeIfOpen(() -> publishOpen(request, sourcePosition),
                () -> completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null)));
    }

    /** Publishes with the exact Prepared Publish hash retained by the admission record. */
    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request,
                                                              final SourcePosition sourcePosition,
                                                              final byte[] preparedPublishHash) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        return closeGuard.invokeIfOpen(() -> publishOpen(request, sourcePosition, preparedPublishHash),
                () -> completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null)));
    }

    private CompletionStage<DestinationPublishResult> publishOpen(final DestinationPublishRequest request,
                                                                   final SourcePosition sourcePosition) {
        return completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    private CompletionStage<DestinationPublishResult> publishOpen(final DestinationPublishRequest request,
                                                                   final SourcePosition sourcePosition,
                                                                   final byte[] preparedPublishHash) {
        final KafkaReceiptJournal.ProducerKey producerKey;
        final KafkaReceiptJournal.AttemptIdentity identity;
        try {
            if (!(sourcePosition instanceof KafkaSourcePosition)
                    || !request.delayMessageId().routingId().shardId().equals(sourcePosition.shardId())) {
                return completed(DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
            }
            producerKey = new KafkaReceiptJournal.ProducerKey(laneId, laneIncarnation,
                    transactionalIdentitySha256, targetResource);
            identity = new KafkaReceiptJournal.AttemptIdentity(request.delayMessageId(), request.generation(),
                    request.publishAttemptId(), preparedPublishHash, sourcePosition.brokerPersistenceTimeEpochMs(),
                    sourcePosition.canonicalBytes());
        } catch (RuntimeException invalid) {
            return completed(DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
        }

        final CompletionStage<DestinationPublishResult> result;
        try {
            result = journal.sendAfterMapped(producerKey, identity, mapping -> transport.publish(
                    KafkaTransactionalDestinationRequest.create(targetPhysicalTopic,
                            KafkaDestinationRequest.from(targetResource, request), receiptPhysicalTopic,
                            receiptResource, mapping)));
        } catch (KafkaReceiptJournal.JournalException | IllegalArgumentException failure) {
            return completed(DestinationPublishResult.definitelyNotPublished(failure instanceof KafkaReceiptJournal.JournalException
                    ? ((KafkaReceiptJournal.JournalException) failure).stableCode() : StableCode.INVALID_METADATA, null));
        } catch (RuntimeException failure) {
            return completed(DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        if (result == null) {
            return completed(DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        try {
            return result.handle((value, error) -> error == null && value != null ? validate(value, request)
                    : DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        } catch (RuntimeException registrationFailure) {
            return completed(DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
    }

    @Override
    public void close() {
        closeGuard.close(transport::close);
    }

    private DestinationPublishResult validate(final DestinationPublishResult result,
                                               final DestinationPublishRequest request) {
        if (result.disposition() != DestinationPublishResult.Disposition.PUBLISHED) {
            return result;
        }
        final BrokerResourceIdentityV1 identity = result.brokerResource();
        if (identity == null || identity.kind() != BrokerResourceIdentityV1.Kind.KAFKA
                || !targetResource.authenticatedClusterId().equals(identity.kafka().authenticatedClusterId())
                || !targetResource.nativeTopicUuid().equals(identity.kafka().nativeTopicUuid())
                || targetResource.partition() != result.brokerPartition()
                || !java.util.Arrays.equals(request.publishAttemptId(), result.externalDeliveryIdentity())) {
            return DestinationPublishResult.unknown(StableCode.RESOURCE_INCARNATION_MISMATCH, result.evidence());
        }
        return result;
    }

    private static CompletionStage<DestinationPublishResult> completed(final DestinationPublishResult result) {
        return CompletableFuture.completedFuture(result);
    }

    private static String requireTopic(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }

    @FunctionalInterface
    public interface KafkaTransactionalDestinationTransport extends AutoCloseable {
        CompletionStage<DestinationPublishResult> publish(KafkaTransactionalDestinationRequest request);

        @Override
        default void close() {
            // Implementations close their producer/connection here.
        }
    }
}
