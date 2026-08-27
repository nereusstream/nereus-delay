package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PinnedPulsarCommandIngress;
import com.nereusstream.delay.adapter.PulsarNativeSendRequest;
import com.nereusstream.delay.adapter.PulsarSendAckEvidence;
import com.nereusstream.delay.adapter.PulsarSendRequest;
import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.StableCode;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
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

/** Binding to a P1 Producer created with the first-class TopicResourceGuard. */
public final class PulsarClientArtifactSendTransport
        implements PinnedPulsarCommandIngress.PulsarSendTransport,
                com.nereusstream.delay.adapter.PinnedPulsarNativeSubmissionAdapter.PulsarNativeSendTransport {
    private final Producer<byte[]> producer;
    private final String authenticatedClusterId;
    private final byte[] resourceIncarnation;
    private final String physicalTopic;
    private final long physicalTopicCreationTimestamp;
    private final int partition;
    private final TopicResourceGuard expectedGuard;
    private final boolean nativePreparedDeliveryEnabled;

    public PulsarClientArtifactSendTransport(
            final Producer<byte[]> producer,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final int partition) {
        this(
                producer,
                authenticatedClusterId,
                resourceIncarnation,
                physicalTopic,
                physicalTopicCreationTimestamp,
                partition,
                false);
    }

    /** Creates the transport with an explicit H6 native-record activation bit. */
    public PulsarClientArtifactSendTransport(
            final Producer<byte[]> producer,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final int partition,
            final boolean nativePreparedDeliveryEnabled) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.authenticatedClusterId = Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        this.resourceIncarnation = Objects.requireNonNull(resourceIncarnation, "resourceIncarnation")
                .clone();
        this.physicalTopic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        this.physicalTopicCreationTimestamp = physicalTopicCreationTimestamp;
        this.partition = partition;
        this.nativePreparedDeliveryEnabled = nativePreparedDeliveryEnabled;
        this.expectedGuard = new TopicResourceGuard(
                authenticatedClusterId, this.resourceIncarnation, physicalTopicCreationTimestamp);
        if (!physicalTopic.equals(producer.getTopic())) {
            throw new IllegalArgumentException("Pulsar producer topic is not the pinned physical topic");
        }
    }

    @Override
    public CompletionStage<PulsarSendResult> send(final PulsarSendRequest request) {
        Objects.requireNonNull(request, "request");
        if (!matches(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopic(),
                request.physicalTopicCreationTimestamp(),
                request.partition())) {
            return CompletableFuture.completedFuture(PulsarSendResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue(), null));
        }
        return sendFrame(request.frame());
    }

    @Override
    public CompletionStage<PulsarSendResult> send(final PulsarNativeSendRequest request) {
        Objects.requireNonNull(request, "request");
        if (!matches(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopic(),
                request.physicalTopicCreationTimestamp(),
                request.partition())) {
            return CompletableFuture.completedFuture(PulsarSendResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue(), null));
        }
        // H0 disables the native physical record path before newMessage().
        return CompletableFuture.completedFuture(PulsarSendResult.definitelyNotPersisted(
                com.nereusstream.delay.protocol.StableCode.CAPABILITY_UNAVAILABLE.wireValue(), null));
    }

    /**
     * Sends a validated final record through this guarded Producer. This
     * entry point is separate from the legacy command/native request methods
     * so H0 callers remain fail-closed until the H5 admission path is active.
     */
    @Override
    public CompletionStage<PulsarSendResult> sendPreparedRecord(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        if (!Arrays.equals(record.artifactGenerationSetDigest(), artifacts.setDigest())
                || !record.template().targetResource().equals(expectedTarget())
                || record.template().physicalPartition() != partition
                || record.template().physicalPartition() > Integer.MAX_VALUE) {
            return CompletableFuture.completedFuture(
                    PulsarSendResult.unknown(StableCode.PREPARED_SUBMISSION_MISMATCH.wireValue(), null));
        }
        if (record.template().deliveryContract().isNative() && !nativePreparedDeliveryEnabled) {
            return CompletableFuture.completedFuture(
                    PulsarSendResult.definitelyNotPersisted(StableCode.CAPABILITY_UNAVAILABLE.wireValue(), null));
        }
        try {
            return PulsarClientArtifactRecordEncoder.send(producer, record)
                    .handle((messageId, failure) ->
                            failure == null ? success(record, artifacts, messageId) : failure(failure));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failure(failure));
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar guarded producer close failed", failure);
        }
    }

    private CompletionStage<PulsarSendResult> sendFrame(final byte[] frame) {
        try {
            return producer.newMessage()
                    .value(frame)
                    .sendAsync()
                    .handle((messageId, failure) -> failure == null ? success(messageId) : failure(failure));
        } catch (RuntimeException failure) {
            return CompletableFuture.completedFuture(failure(failure));
        }
    }

    private PulsarSendResult success(final MessageId messageId) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || partition != guarded.partition()
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0) {
            return PulsarSendResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final boolean batched = rawBatchIndex >= 0;
        final int batchIndex = batched ? rawBatchIndex : 0;
        final int batchSize = batched ? rawBatchSize : 1;
        if (batchSize <= 0 || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
            return PulsarSendResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return PulsarSendResult.unknown(
                    com.nereusstream.delay.protocol.StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
        return new PulsarSendResult(
                PulsarSendResult.Disposition.PERSISTED,
                authenticatedClusterId,
                resourceIncarnation,
                physicalTopic,
                physicalTopicCreationTimestamp,
                partition,
                advanced.getLedgerId(),
                advanced.getEntryId(),
                batchIndex,
                batchSize,
                batched,
                guarded.brokerEntryTimestamp(),
                com.nereusstream.delay.protocol.StableCode.OK.wireValue(),
                encodeRequestEvidence(evidence),
                encodeResponseEvidence(evidence));
    }

    private PulsarSendResult success(
            final PulsarPreparedRecord record, final ArtifactGenerationSet artifacts, final MessageId messageId) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || partition != guarded.partition()
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0) {
            return PulsarSendResult.unknown(StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final boolean batched = rawBatchIndex >= 0;
        final int batchIndex = batched ? rawBatchIndex : 0;
        final int batchSize = batched ? rawBatchSize : 1;
        if (batchSize <= 0 || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
            return PulsarSendResult.unknown(StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
        final GuardedSendSuccessEvidence response = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(response.attestation())
                || response.ledgerId() != advanced.getLedgerId()
                || response.entryId() != advanced.getEntryId()
                || response.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return PulsarSendResult.unknown(StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
        final byte[] producerNameHash = Bytes.sha256(Bytes.utf8(producer.getProducerName()));
        try {
            final PublishEvidence typed = PulsarSendAckEvidence.publishedRecord(
                    record,
                    artifacts,
                    producerNameHash,
                    advanced.getLedgerId(),
                    advanced.getEntryId(),
                    batchIndex,
                    batchSize,
                    response.brokerEntryTimestamp(),
                    response.protocolVersion(),
                    response.connectionGeneration(),
                    response.producerId(),
                    response.sequenceId(),
                    response.sendCommandSha256(),
                    response.authenticatedResponseCommandSha256());
            return new PulsarSendResult(
                    PulsarSendResult.Disposition.PERSISTED,
                    authenticatedClusterId,
                    resourceIncarnation,
                    physicalTopic,
                    physicalTopicCreationTimestamp,
                    partition,
                    advanced.getLedgerId(),
                    advanced.getEntryId(),
                    batchIndex,
                    batchSize,
                    batched,
                    response.brokerEntryTimestamp(),
                    StableCode.OK.wireValue(),
                    encodeRequestEvidence(response),
                    typed.canonicalBytes());
        } catch (RuntimeException evidenceFailure) {
            return PulsarSendResult.unknown(StableCode.INTEGRITY_ERROR.wireValue(), null);
        }
    }

    private PulsarSendResult failure(final Throwable failure) {
        final TopicResourceGuardException guardFailure = unwrap(failure);
        if (guardFailure != null
                && guardFailure.definitelyNotPersisted()
                && guardFailure.responseEvidence().isPresent()) {
            return PulsarSendResult.definitelyNotPersisted(
                    com.nereusstream.delay.protocol.StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue(),
                    encodeErrorEvidence(guardFailure.responseEvidence().get()));
        }
        final byte[] evidence =
                guardFailure == null || guardFailure.responseEvidence().isEmpty()
                        ? null
                        : encodeErrorEvidence(guardFailure.responseEvidence().get());
        return PulsarSendResult.unknown(
                com.nereusstream.delay.protocol.StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue(), evidence);
    }

    private boolean matches(
            final String cluster,
            final byte[] incarnation,
            final String topic,
            final long creationTimestamp,
            final int requestPartition) {
        return authenticatedClusterId.equals(cluster)
                && Arrays.equals(resourceIncarnation, incarnation)
                && physicalTopic.equals(topic)
                && physicalTopicCreationTimestamp == creationTimestamp
                && partition == requestPartition;
    }

    private BrokerResourceIdentity expectedTarget() {
        return BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                authenticatedClusterId, resourceIncarnation, physicalTopic, physicalTopicCreationTimestamp));
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

    private static byte[] encodeRequestEvidence(final GuardedSendSuccessEvidence evidence) {
        return encodeSuccess(evidence, false);
    }

    private static byte[] encodeResponseEvidence(final GuardedSendSuccessEvidence evidence) {
        return encodeSuccess(evidence, true);
    }

    private static byte[] encodeSuccess(final GuardedSendSuccessEvidence evidence, final boolean response) {
        try {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(192);
            final DataOutputStream output = new DataOutputStream(bytes);
            output.writeInt(1);
            output.writeBoolean(response);
            output.writeInt(evidence.protocolVersion());
            output.writeLong(evidence.connectionGeneration());
            output.writeLong(evidence.producerId());
            output.writeLong(evidence.sequenceId());
            writeText(output, evidence.attestation().authenticatedClusterId());
            writeBytes(output, evidence.attestation().resourceIncarnation());
            output.writeLong(evidence.attestation().topicCreationTimestamp());
            writeText(output, evidence.attestation().physicalTopic());
            output.writeInt(evidence.attestation().partition());
            output.writeLong(evidence.ledgerId());
            output.writeLong(evidence.entryId());
            output.writeLong(evidence.brokerEntryTimestamp());
            writeBytes(output, evidence.sendCommandSha256());
            writeBytes(output, evidence.authenticatedResponseCommandSha256());
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory Pulsar evidence encoding failed", impossible);
        }
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

    private static void writeText(final DataOutputStream output, final String value) throws IOException {
        writeBytes(output, value.getBytes(StandardCharsets.UTF_8));
    }
}
