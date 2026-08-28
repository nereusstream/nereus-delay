package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import com.nereusstream.delay.adapter.PulsarDestinationRequest;
import com.nereusstream.delay.adapter.PulsarSendAckEvidence;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendErrorEvidence;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TopicResourceGuardException;

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
    private final boolean nativePreparedDeliveryEnabled;

    public PulsarClientArtifactDestinationTransport(
            final Producer<byte[]> producer,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final int partition,
            final byte[] producerNameHash) {
        this(
                producer,
                authenticatedClusterId,
                resourceIncarnation,
                physicalTopic,
                physicalTopicCreationTimestamp,
                partition,
                producerNameHash,
                null,
                false);
    }

    /**
     * Creates a source-bound destination transport with an optional evidence
     * provider used only after the Producer completion is uncertain. A
     * provider must return typed PULSAR_SEND_ACK evidence bound to this exact
     * request; a missing or invalid reread remains UNKNOWN.
     */
    public PulsarClientArtifactDestinationTransport(
            final Producer<byte[]> producer,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final int partition,
            final byte[] producerNameHash,
            final PublishEvidenceProvider publishEvidenceProvider) {
        this(
                producer,
                authenticatedClusterId,
                resourceIncarnation,
                physicalTopic,
                physicalTopicCreationTimestamp,
                partition,
                producerNameHash,
                publishEvidenceProvider,
                false);
    }

    /** Creates the transport with an explicit H6 native-record activation bit. */
    public PulsarClientArtifactDestinationTransport(
            final Producer<byte[]> producer,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final int partition,
            final byte[] producerNameHash,
            final PublishEvidenceProvider publishEvidenceProvider,
            final boolean nativePreparedDeliveryEnabled) {
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
        final byte[] actualProducerNameHash =
                Bytes.sha256(Bytes.utf8(Objects.requireNonNull(producer.getProducerName(), "Pulsar producer name")));
        if (!Bytes.constantTimeEquals(actualProducerNameHash, producerNameHash)) {
            throw new IllegalArgumentException("Pulsar producer name hash does not match the live Producer");
        }
        this.producerNameHash = Bytes.copy(producerNameHash);
        this.publishEvidenceProvider = publishEvidenceProvider;
        this.nativePreparedDeliveryEnabled = nativePreparedDeliveryEnabled;
        this.expectedGuard = new TopicResourceGuard(
                authenticatedClusterId, this.resourceIncarnation, physicalTopicCreationTimestamp);
        if (!physicalTopic.equals(producer.getTopic())) {
            throw new IllegalArgumentException("Pulsar producer topic is not the pinned physical topic");
        }
    }

    /** Request-only publication cannot bind the required prepared hash. */
    @Override
    public CompletionStage<DestinationPublishResult> publish(final PulsarDestinationRequest request) {
        Objects.requireNonNull(request, "request");
        return CompletableFuture.completedFuture(
                DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(
            final PulsarDestinationRequest request,
            final SourcePosition sourcePosition,
            final byte[] preparedPublishHash) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        // The source can be Kafka when the Delay route crosses adapters. The
        // target proof below remains strictly Pulsar-native; only the common
        // Delay Shard identity is checked at this seam.
        if (!request.delayMessageId().routingId().shardId().equals(sourcePosition.shardId())) {
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.definitelyNotPublished(StableCode.INVALID_METADATA, null));
        }
        if (!matches(request)) {
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.RESOURCE_INCARNATION_MISMATCH, null));
        }
        // Defense in depth for direct transport callers: H0 must reject
        // before Producer ownership even when the adapter is bypassed.
        if (request.actionAtEpochMs() < request.deliverAtEpochMs()) {
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        try {
            return producer.newMessage()
                    .value(request.payload())
                    .sendAsync()
                    .handle((messageId, failure) -> failure == null
                            ? success(request, preparedPublishHash, messageId)
                            : failure(request, preparedPublishHash, failure));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failure(request, preparedPublishHash, failure));
        }
    }

    /**
     * Publishes the final logical record through the source-locked P1
     * encoder. The caller must have completed Journal ownership and pass the
     * exact ArtifactGenerationSet used by the record.
     */
    public CompletionStage<DestinationPublishResult> publishPreparedRecord(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        if (!Arrays.equals(record.artifactGenerationSetDigest(), artifacts.setDigest())
                || !record.template().targetResource().equals(expectedTarget())
                || record.template().physicalPartition() != partition
                || artifacts.pulsarRecordGeneration() != PulsarPreparedRecord.SCHEMA_GENERATION) {
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.unknown(StableCode.PREPARED_SUBMISSION_MISMATCH, null));
        }
        if (record.template().deliveryContract().isNative() && !nativePreparedDeliveryEnabled) {
            return CompletableFuture.completedFuture(
                    DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        try {
            return PulsarClientArtifactRecordEncoder.send(producer, record)
                    .handle((messageId, failure) -> failure == null
                            ? success(record, artifacts, messageId)
                            : failure(record, artifacts, failure));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failure(record, artifacts, failure));
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

    private DestinationPublishResult success(
            final PulsarDestinationRequest request, final byte[] preparedPublishHash, final MessageId messageId) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || partition != guarded.partition()
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != partition) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0 || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        try {
            final PublishEvidence typed = PulsarSendAckEvidence.published(
                    request,
                    preparedPublishHash,
                    producerNameHash,
                    advanced.getLedgerId(),
                    advanced.getEntryId(),
                    normalizedBatchIndex,
                    evidence.brokerEntryTimestamp(),
                    evidence.sequenceId(),
                    evidence.authenticatedResponseCommandSha256());
            if (typed.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                    || typed.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
                throw new IllegalArgumentException("Pulsar destination evidence branch mismatch");
            }
            typed.requireBusinessMutation(request.publishAttemptId(), true);
            return DestinationPublishResult.published(
                    BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                            authenticatedClusterId,
                            resourceIncarnation,
                            physicalTopic,
                            physicalTopicCreationTimestamp)),
                    partition,
                    request.publishAttemptId(),
                    evidence.brokerEntryTimestamp(),
                    typed.canonicalBytes());
        } catch (RuntimeException evidenceFailure) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
    }

    private DestinationPublishResult success(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts, final MessageId messageId) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || partition != guarded.partition()
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != partition) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        final int batchSize = rawBatchIndex < 0 ? 1 : rawBatchSize;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0 || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        try {
            final PublishEvidence typed = PulsarSendAckEvidence.publishedRecord(
                    record,
                    artifacts,
                    producerNameHash,
                    advanced.getLedgerId(),
                    advanced.getEntryId(),
                    normalizedBatchIndex,
                    batchSize,
                    evidence.brokerEntryTimestamp(),
                    evidence.protocolVersion(),
                    evidence.connectionGeneration(),
                    evidence.producerId(),
                    evidence.sequenceId(),
                    evidence.sendCommandSha256(),
                    evidence.authenticatedResponseCommandSha256());
            return DestinationPublishResult.published(
                    expectedTarget(),
                    partition,
                    record.externalIdentity().identity(),
                    evidence.brokerEntryTimestamp(),
                    typed.canonicalBytes());
        } catch (RuntimeException evidenceFailure) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
    }

    private DestinationPublishResult failure(
            final PulsarDestinationRequest request, final byte[] preparedPublishHash, final Throwable failure) {
        final TopicResourceGuardException guardFailure = unwrap(failure);
        if (guardFailure != null && guardFailure.definitelyNotPersisted()) {
            if (guardFailure.responseEvidence().isPresent()) {
                return DestinationPublishResult.definitelyNotPublished(
                        StableCode.BROKER_DEFINITIVE_NOT_PERSISTED,
                        encodeErrorEvidence(guardFailure.responseEvidence().get()));
            }
            // The disposition alone is not the exact authenticated proof
            // required by the legacy result branch.
            return DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
        }
        if (publishEvidenceProvider != null) {
            try {
                final Optional<ResolvedPublish> resolved =
                        publishEvidenceProvider.resolve(request, preparedPublishHash, failure);
                if (resolved != null && resolved.isPresent()) {
                    final ResolvedPublish candidate = resolved.get();
                    final PublishEvidence evidence = candidate.evidence();
                    if (evidence.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                            || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
                        throw new IllegalArgumentException("Pulsar recovery provider returned the wrong evidence");
                    }
                    com.nereusstream.delay.adapter.PulsarSendAckEvidence.requireExactBinding(
                            evidence,
                            request,
                            preparedPublishHash,
                            producerNameHash,
                            candidate.brokerPersistenceTimeEpochMs());
                    return DestinationPublishResult.published(
                            BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                                    authenticatedClusterId,
                                    resourceIncarnation,
                                    physicalTopic,
                                    physicalTopicCreationTimestamp)),
                            partition,
                            request.publishAttemptId(),
                            candidate.brokerPersistenceTimeEpochMs(),
                            evidence.canonicalBytes());
                }
            } catch (RuntimeException ignored) {
                // A provider cannot turn an unverified or divergent reread into PUBLISHED.
            }
        }
        return DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
    }

    private DestinationPublishResult failure(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts, final Throwable failure) {
        final TopicResourceGuardException guardFailure = unwrap(failure);
        if (guardFailure != null && guardFailure.definitelyNotPersisted()) {
            final byte[] evidence = guardFailure.responseEvidence().isPresent()
                    ? encodeErrorEvidence(guardFailure.responseEvidence().get())
                    : null;
            return DestinationPublishResult.definitelyNotPublished(
                    StableCode.BROKER_DEFINITIVE_NOT_PERSISTED, evidence);
        }
        if (publishEvidenceProvider != null) {
            try {
                final Optional<ResolvedRecordPublish> resolved =
                        publishEvidenceProvider.resolve(record, artifacts, failure);
                if (resolved != null && resolved.isPresent()) {
                    final ResolvedRecordPublish candidate = resolved.get();
                    PulsarSendAckEvidence.requireExactBindingForRecord(
                            candidate.evidence(),
                            record,
                            artifacts,
                            producerNameHash,
                            candidate.ledgerId(),
                            candidate.entryId(),
                            candidate.normalizedBatchIndex(),
                            candidate.batchSize(),
                            candidate.brokerPersistenceTimeEpochMs(),
                            candidate.p1ProtocolVersion(),
                            candidate.connectionGeneration(),
                            candidate.producerId(),
                            candidate.actualSequenceId(),
                            candidate.sendCommandSha256(),
                            candidate.authenticatedResponseCommandSha256());
                    return DestinationPublishResult.published(
                            expectedTarget(),
                            partition,
                            record.externalIdentity().identity(),
                            candidate.brokerPersistenceTimeEpochMs(),
                            candidate.evidence().canonicalBytes());
                }
            } catch (RuntimeException ignored) {
                // A provider cannot promote a foreign record, command, ACK, or position to PUBLISHED.
            }
        }
        return DestinationPublishResult.unknown(StableCode.ENQUEUE_RESULT_UNCERTAIN, null);
    }

    private BrokerResourceIdentity expectedTarget() {
        return BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                authenticatedClusterId, resourceIncarnation, physicalTopic, physicalTopicCreationTimestamp));
    }

    private static byte[] encodeErrorEvidence(final GuardedSendErrorEvidence evidence) {
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(128);
            final DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(1);
            output.writeInt(evidence.protocolVersion());
            output.writeLong(evidence.connectionGeneration());
            output.writeLong(evidence.producerId());
            output.writeLong(evidence.sequenceId());
            output.writeInt(evidence.serverErrorCode());
            writeBytes(output, evidence.sendCommandSha256());
            writeBytes(output, evidence.authenticatedResponseCommandSha256());
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar error evidence encoding failed", impossible);
        }
    }

    private static void writeBytes(final DataOutputStream output, final byte[] value) throws IOException {
        output.writeInt(value.length);
        output.write(value);
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
    public record ResolvedPublish(PublishEvidence evidence, long brokerPersistenceTimeEpochMs) {
        public ResolvedPublish {
            Objects.requireNonNull(evidence, "evidence");
            if (brokerPersistenceTimeEpochMs < 0) {
                throw new IllegalArgumentException("brokerPersistenceTimeEpochMs must be non-negative");
            }
        }
    }

    /** Exact generation-2 SEND/ACK proof returned after a prepared-record response loss. */
    public record ResolvedRecordPublish(
            PublishEvidence evidence,
            long ledgerId,
            long entryId,
            int normalizedBatchIndex,
            int batchSize,
            long brokerPersistenceTimeEpochMs,
            int p1ProtocolVersion,
            long connectionGeneration,
            long producerId,
            long actualSequenceId,
            byte[] sendCommandSha256,
            byte[] authenticatedResponseCommandSha256) {
        public ResolvedRecordPublish {
            Objects.requireNonNull(evidence, "evidence");
            if (ledgerId < 0
                    || entryId < 0
                    || normalizedBatchIndex < 0
                    || batchSize <= 0
                    || Integer.compareUnsigned(normalizedBatchIndex, batchSize) >= 0
                    || brokerPersistenceTimeEpochMs < 0
                    || p1ProtocolVersion <= 0
                    || connectionGeneration < 0
                    || producerId < 0
                    || actualSequenceId < 0) {
                throw new IllegalArgumentException("resolved prepared-record position/evidence is invalid");
            }
            Bytes.requireLength(sendCommandSha256, 32, "sendCommandSha256");
            Bytes.requireLength(authenticatedResponseCommandSha256, 32, "authenticatedResponseCommandSha256");
            sendCommandSha256 = Bytes.copy(sendCommandSha256);
            authenticatedResponseCommandSha256 = Bytes.copy(authenticatedResponseCommandSha256);
        }

        @Override
        public byte[] sendCommandSha256() {
            return Bytes.copy(sendCommandSha256);
        }

        @Override
        public byte[] authenticatedResponseCommandSha256() {
            return Bytes.copy(authenticatedResponseCommandSha256);
        }
    }

    /**
     * Optional source-bound recovery hook. Implementations must reread or
     * otherwise prove the exact Broker outcome; returning empty keeps UNKNOWN.
     */
    @FunctionalInterface
    public interface PublishEvidenceProvider {
        Optional<ResolvedPublish> resolve(
                PulsarDestinationRequest request, byte[] preparedPublishHash, Throwable failure);

        /** Prepared-record recovery is a separate closed evidence branch; legacy providers remain empty. */
        default Optional<ResolvedRecordPublish> resolve(
                final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts, final Throwable failure) {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(artifacts, "artifacts");
            Objects.requireNonNull(failure, "failure");
            return Optional.empty();
        }
    }
}
