package io.nereusstream.delay.transport;

import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import io.nereusstream.delay.adapter.PulsarDestinationRequest;
import io.nereusstream.delay.adapter.PulsarSendAckEvidence;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TopicResourceGuardException;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Source-bound P1 destination transport producing typed SEND ACK evidence. */
public final class PulsarClientArtifactDestinationTransport
        implements PinnedPulsarDestinationAdapter.PulsarDestinationTransport {
    private final Producer<byte[]> producer;
    private final String authenticatedClusterId;
    private final byte[] resourceIncarnation;
    private final String physicalTopic;
    private final long physicalTopicCreationTimestamp;
    private final int partition;
    private final byte[] producerNameHash;
    private final TopicResourceGuard expectedGuard;
    private final PublishEvidenceProvider publishEvidenceProvider;

    public PulsarClientArtifactDestinationTransport(final Producer<byte[]> producer,
                                                    final String authenticatedClusterId,
                                                    final byte[] resourceIncarnation,
                                                    final String physicalTopic,
                                                    final long physicalTopicCreationTimestamp,
                                                    final int partition,
                                                    final byte[] producerNameHash) {
        this(producer, authenticatedClusterId, resourceIncarnation, physicalTopic,
                physicalTopicCreationTimestamp, partition, producerNameHash, null);
    }

    /**
     * Creates a source-bound destination transport with an optional evidence
     * provider used only after the Producer completion is uncertain. A
     * provider must return typed PULSAR_SEND_ACK evidence bound to this exact
     * request; a missing or invalid reread remains UNKNOWN.
     */
    public PulsarClientArtifactDestinationTransport(final Producer<byte[]> producer,
                                                    final String authenticatedClusterId,
                                                    final byte[] resourceIncarnation,
                                                    final String physicalTopic,
                                                    final long physicalTopicCreationTimestamp,
                                                    final int partition,
                                                    final byte[] producerNameHash,
                                                    final PublishEvidenceProvider publishEvidenceProvider) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.authenticatedClusterId = Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        this.resourceIncarnation = Bytes.copy(resourceIncarnation);
        this.physicalTopic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (physicalTopic.isBlank() || partition < 0) {
            throw new IllegalArgumentException("invalid Pulsar destination target");
        }
        this.physicalTopicCreationTimestamp = physicalTopicCreationTimestamp;
        this.partition = partition;
        Bytes.requireLength(producerNameHash, 32, "producerNameHash");
        this.producerNameHash = Bytes.copy(producerNameHash);
        this.publishEvidenceProvider = publishEvidenceProvider;
        this.expectedGuard = new TopicResourceGuard(authenticatedClusterId, this.resourceIncarnation,
                physicalTopicCreationTimestamp);
        if (!physicalTopic.equals(producer.getTopic())) {
            throw new IllegalArgumentException("Pulsar producer topic is not the pinned physical topic");
        }
    }

    /** Request-only publication cannot bind the required prepared hash. */
    @Override
    public CompletionStage<DestinationPublishResult> publish(final PulsarDestinationRequest request) {
        Objects.requireNonNull(request, "request");
        return CompletableFuture.completedFuture(DestinationPublishResult.unknown(
                StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final PulsarDestinationRequest request,
                                                              final SourcePosition sourcePosition,
                                                              final byte[] preparedPublishHash) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        if (!(sourcePosition instanceof PulsarSourcePosition)
                || !request.delayMessageId().routingId().shardId().equals(sourcePosition.shardId())) {
            return CompletableFuture.completedFuture(DestinationPublishResult.definitelyNotPublished(
                    StableCode.INVALID_METADATA, null));
        }
        if (!matches(request)) {
            return CompletableFuture.completedFuture(DestinationPublishResult.unknown(
                    StableCode.RESOURCE_INCARNATION_MISMATCH, null));
        }
        try {
            return producer.newMessage().value(request.payload()).sendAsync()
                    .handle((messageId, failure) -> failure == null
                            ? success(request, preparedPublishHash, messageId)
                            : failure(request, preparedPublishHash, failure));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failure(request, preparedPublishHash, failure));
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar destination producer close failed", failure);
        }
    }

    private DestinationPublishResult success(final PulsarDestinationRequest request,
                                             final byte[] preparedPublishHash,
                                             final MessageId messageId) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || partition != guarded.partition()
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0 || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != partition) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation = new TopicResourceGuardAttestation(
                expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0
                || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        try {
            final PublishEvidenceV1 typed = PulsarSendAckEvidence.published(request, preparedPublishHash,
                    producerNameHash, advanced.getLedgerId(), advanced.getEntryId(), normalizedBatchIndex,
                    evidence.brokerEntryTimestamp(), evidence.sequenceId(), evidence.authenticatedResponseCommandSha256());
            if (typed.evidenceKind() != PublishEvidenceKindV1.PULSAR_SEND_ACK
                    || typed.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
                throw new IllegalArgumentException("Pulsar destination evidence branch mismatch");
            }
            typed.requireBusinessMutation(request.publishAttemptId(), true);
            return DestinationPublishResult.published(BrokerResourceIdentityV1.pulsar(
                            new PulsarBrokerResourceIdentityV1(authenticatedClusterId, resourceIncarnation,
                                    physicalTopic, physicalTopicCreationTimestamp)), partition,
                    request.publishAttemptId(), evidence.brokerEntryTimestamp(), typed.canonicalBytes());
        } catch (RuntimeException evidenceFailure) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
    }

    private DestinationPublishResult failure(final PulsarDestinationRequest request,
                                             final byte[] preparedPublishHash,
                                             final Throwable failure) {
        final TopicResourceGuardException guardFailure = unwrap(failure);
        if (guardFailure != null && guardFailure.definitelyNotPersisted()) {
            return DestinationPublishResult.unknown(StableCode.BROKER_DEFINITIVE_NOT_PERSISTED, null);
        }
        if (publishEvidenceProvider != null) {
            try {
                final Optional<ResolvedPublish> resolved = publishEvidenceProvider.resolve(
                        request, preparedPublishHash, failure);
                if (resolved != null && resolved.isPresent()) {
                    final ResolvedPublish candidate = resolved.get();
                    final PublishEvidenceV1 evidence = candidate.evidence();
                    if (evidence.evidenceKind() != PublishEvidenceKindV1.PULSAR_SEND_ACK
                            || evidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
                        throw new IllegalArgumentException("Pulsar recovery provider returned the wrong evidence");
                    }
                    evidence.requireBusinessMutation(request.publishAttemptId(), true);
                    return DestinationPublishResult.published(BrokerResourceIdentityV1.pulsar(
                                    new PulsarBrokerResourceIdentityV1(authenticatedClusterId, resourceIncarnation,
                                            physicalTopic, physicalTopicCreationTimestamp)), partition,
                            request.publishAttemptId(), candidate.brokerPersistenceTimeEpochMs(),
                            evidence.canonicalBytes());
                }
            } catch (RuntimeException ignored) {
                // A provider cannot turn an unverified or divergent reread into PUBLISHED.
            }
        }
        return DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
    }

    private boolean matches(final PulsarDestinationRequest request) {
        return authenticatedClusterId.equals(request.authenticatedClusterId())
                && Arrays.equals(resourceIncarnation, request.resourceIncarnation())
                && physicalTopic.equals(request.physicalTopic())
                && physicalTopicCreationTimestamp == request.physicalTopicCreationTimestamp()
                && partition == request.partition();
    }

    private static TopicResourceGuardException unwrap(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof TopicResourceGuardException guardFailure) {
                return guardFailure;
            }
            current = current.getCause();
        }
        return null;
    }

    /** Source-bound proof returned by a recovery provider after SEND uncertainty. */
    public record ResolvedPublish(PublishEvidenceV1 evidence, long brokerPersistenceTimeEpochMs) {
        public ResolvedPublish {
            Objects.requireNonNull(evidence, "evidence");
            if (brokerPersistenceTimeEpochMs < 0) {
                throw new IllegalArgumentException("brokerPersistenceTimeEpochMs must be non-negative");
            }
        }
    }

    /**
     * Optional source-bound recovery hook. Implementations must reread or
     * otherwise prove the exact Broker outcome; returning empty keeps UNKNOWN.
     */
    @FunctionalInterface
    public interface PublishEvidenceProvider {
        Optional<ResolvedPublish> resolve(PulsarDestinationRequest request, byte[] preparedPublishHash,
                                           Throwable failure);
    }
}
