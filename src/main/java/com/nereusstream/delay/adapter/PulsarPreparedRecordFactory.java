package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarReservedProperties;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import java.util.Arrays;
import java.util.Objects;

/**
 * The only main-code factory for the final Pulsar record projection.
 *
 * <p>Admission freezes the descriptor/template. Managed Journal mapping then
 * supplies the sequence authority. This class performs the narrow join and
 * never allocates a new payload, metadata map, or timing value.</p>
 */
public final class PulsarPreparedRecordFactory {
    private PulsarPreparedRecordFactory() {}

    /**
     * Pure Target record join. The sequence is a supplied Journal value, not proof that its mapping is durable.
     * The final Target preflight must validate the protected mapping and live Owner/Store/credential/send token.
     */
    public static PulsarPreparedRecord targetManaged(
            final TargetOrdinaryPublicationBinding publication,
            final TargetMessageRecord message,
            final PayloadForPublish payload,
            final ResolvedPayload resolved,
            final PulsarSequenceAuthority sequence,
            final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(payload, "payload");
        if (!publication.locator().equals(message.locator())
                || message.stateVersion() != TargetQueueState.nextRevision(publication.claimedMessageVersion())
                || message.runtime().currentWorkKind() != CurrentSendWorkKind.PUBLISHING
                || !Arrays.equals(message.runtime().publishAttemptId(), publication.publishAttemptId())
                || message.runtime().admissionsUsed() != publication.attemptNo()
                || (message.inlinePayload() != null
                        ? !payload.hasInlinePayload()
                                || !Arrays.equals(message.inlinePayload(), payload.inlinePayload())
                        : !payload.hasObject()
                                || !message.payloadReference().equals(
                                        PayloadReference.fromDescriptor(payload.object())))) {
            throw new IllegalArgumentException("Target record differs from the exact newly admitted Message/payload");
        }
        final var template = targetTemplate(publication, payload);
        final var record = new PulsarPreparedRecord(
                template, template.recordTemplateHash(), resolved, sequence,
                ExternalDeliveryIdentity.publishAttempt(publication.publishAttemptId()),
                publication.preparedPublishHash(),
                PulsarReservedProperties.all(template.reservedMetadata(), publication.publishAttemptId(),
                        publication.preparedPublishHash()),
                Objects.requireNonNull(artifacts, "artifacts").setDigest());
        requireTargetBinding(publication, record, artifacts);
        return record;
    }

    /** Immutable byte/identity checks only; no Source, Journal, physical ownership or credential authorization. */
    public static void requireTargetBinding(
            final TargetOrdinaryPublicationBinding publication,
            final PulsarPreparedRecord record,
            final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        if (!record.template().equals(targetTemplate(publication, record.template().payload()))
                || record.sequenceAuthority().kind() != PulsarSequenceAuthority.Kind.MANAGED_JOURNAL
                || !Arrays.equals(record.sequenceAuthority().producerNameHash(),
                        Bytes.sha256(publication.channel().context().producerIdentity()))
                || !record.externalIdentity().equals(
                        ExternalDeliveryIdentity.publishAttempt(publication.publishAttemptId()))
                || !Arrays.equals(record.preparedIdentityHash(), publication.preparedPublishHash())
                || !Arrays.equals(record.artifactGenerationSetDigest(), publication.artifactGenerationSetDigest())
                || !Arrays.equals(artifacts.setDigest(), publication.artifactGenerationSetDigest())) {
            throw new IllegalArgumentException(
                    "Pulsar Target record changes its frozen channel/request/artifact identity");
        }
    }

    private static PulsarRecordTemplate targetTemplate(
            final TargetOrdinaryPublicationBinding publication, final PayloadForPublish payload) {
        Objects.requireNonNull(publication, "publication");
        if (publication.physical().resource().kind() != BrokerResourceIdentity.Kind.PULSAR
                || publication.channel().context().kind() != ChannelKind.PULSAR_DEDUP_PRODUCER
                || publication.adapterMetadata().kind() != com.nereusstream.delay.protocol.AdapterMetadata.Kind.PULSAR
                || payload.length() != publication.payloadLength()
                || !Arrays.equals(payload.payloadSha256(), publication.payloadSha256())) {
            throw new IllegalArgumentException(
                    "Target managed Journal record requires its exact Pulsar payload/channel");
        }
        final var metadata = publication.adapterMetadata().pulsar();
        final byte[] keyBytes = metadata.partitionKey();
        final var key = keyBytes == null ? PulsarKey.none()
                : metadata.keyEncoding() == PulsarMetadata.KeyEncoding.UTF8
                        ? PulsarKey.utf8(keyBytes) : PulsarKey.binary(keyBytes);
        return new PulsarRecordTemplate(
                publication.physical().resource(), publication.physical().physicalPartition(), key,
                metadata.orderingKey(), metadata.properties(), publication.eventTimeEpochMs(),
                publication.reservedMetadata(), DeliveryContract.NEREUS_MANAGED_NOT_BEFORE, null,
                payload, publication.artifactGenerationSetDigest());
    }

    /** Constructs the managed record after an exact durable Journal mapping. */
    public static PulsarPreparedRecord managed(
            final PreparedPublishDescriptor descriptor,
            final PulsarAttemptJournal.Mapping mapping,
            final ResolvedPayload resolvedPayload,
            final ArtifactGenerationSet artifacts) {
        final PreparedPublishDescriptor exact = currentPulsarDescriptor(descriptor);
        final PulsarAttemptJournal.Mapping exactMapping = Objects.requireNonNull(mapping, "mapping");
        final ResolvedPayload exactPayload = Objects.requireNonNull(resolvedPayload, "resolvedPayload");
        final ArtifactGenerationSet exactArtifacts = requireArtifacts(artifacts, exact);
        if (!exactMapping.delayMessageId().equals(exact.messageId())
                || exactMapping.generation() != exact.generation()
                || !Arrays.equals(exactMapping.publishAttemptId(), exact.publishAttemptId())
                || !Arrays.equals(exactMapping.preparedPublishHash(), exact.preparedPublishHash())
                || !exactMapping.isCurrentGeneration()
                || !Arrays.equals(exactMapping.recordTemplateHash(), exact.recordTemplateHash())
                || exactMapping.deliveryContract() != exact.deliveryContract()
                || !Arrays.equals(exactMapping.artifactGenerationSetDigest(), exact.artifactGenerationSetDigest())
                || !exactMapping.producer().laneId().equals(exact.destinationLaneId())
                || !Arrays.equals(exactMapping.producer().laneIncarnation(), exact.laneIncarnation())
                || !Arrays.equals(
                        exactMapping.producer().stableProducerNameHash(),
                        exact.channel().producerOrTransactionalIdentitySha256())
                || exactMapping.producer().target().partition() != exact.physicalPartition()) {
            throw new IllegalArgumentException("Journal mapping does not match the prepared descriptor");
        }
        if (!mappingTarget(exactMapping).equals(exact.targetResource())) {
            throw new IllegalArgumentException("Journal Producer target differs from the prepared descriptor");
        }
        if (exactMapping.sequenceId() < 0) {
            throw new IllegalArgumentException("Journal sequence must be non-negative");
        }
        final PulsarPreparedRecord record = new PulsarPreparedRecord(
                exact.pulsarRecordTemplate(),
                exact.recordTemplateHash(),
                exactPayload,
                PulsarSequenceAuthority.managedJournal(
                        exactMapping.mappingId(),
                        exactMapping.sequenceId(),
                        exactMapping.producer().stableProducerNameHash()),
                ExternalDeliveryIdentity.publishAttempt(exact.publishAttemptId()),
                exact.preparedPublishHash(),
                PulsarReservedProperties.all(
                        exact.pulsarRecordTemplate().reservedMetadata(),
                        exact.publishAttemptId(),
                        exact.preparedPublishHash()),
                exactArtifacts.setDigest());
        return record;
    }

    /** Constructs an AUTO_FAST record; no Journal sequence is accepted. */
    public static PulsarPreparedRecord nativeDelivery(
            final PreparedPublishDescriptor descriptor,
            final ResolvedPayload resolvedPayload,
            final ArtifactGenerationSet artifacts,
            final byte[] nativeDeliveryId,
            final byte[] submissionHash) {
        final PreparedPublishDescriptor exact = currentPulsarDescriptor(descriptor);
        final ResolvedPayload exactPayload = Objects.requireNonNull(resolvedPayload, "resolvedPayload");
        final ArtifactGenerationSet exactArtifacts = requireArtifacts(artifacts, exact);
        if (exact.deliveryContract() != DeliveryContract.PULSAR_NATIVE_DELIVERY) {
            throw new IllegalArgumentException("native records require the Pulsar native delivery contract");
        }
        Bytes.requireLength(submissionHash, PulsarPreparedRecord.HASH_LENGTH, "submissionHash");
        final PulsarPreparedRecord record = new PulsarPreparedRecord(
                exact.pulsarRecordTemplate(),
                exact.recordTemplateHash(),
                exactPayload,
                PulsarSequenceAuthority.producerAssigned(),
                ExternalDeliveryIdentity.nativeDelivery(nativeDeliveryId),
                submissionHash,
                PulsarReservedProperties.all(
                        exact.pulsarRecordTemplate().reservedMetadata(), exact.publishAttemptId(), submissionHash),
                exactArtifacts.setDigest());
        return record;
    }

    private static PreparedPublishDescriptor currentPulsarDescriptor(final PreparedPublishDescriptor descriptor) {
        final PreparedPublishDescriptor exact = Objects.requireNonNull(descriptor, "descriptor");
        if (exact.descriptorVersion() != ArtifactGenerationSet.DESCRIPTOR_GENERATION
                || exact.adapterEncodingVersion() != ArtifactGenerationSet.ADAPTER_ENCODING_GENERATION
                || exact.adapterKind() != AdapterKind.PULSAR
                || exact.pulsarRecordTemplate() == null
                || exact.recordTemplateHash() == null) {
            throw new IllegalArgumentException("final Pulsar record requires a current descriptor/template");
        }
        return exact;
    }

    private static ArtifactGenerationSet requireArtifacts(
            final ArtifactGenerationSet artifacts, final PreparedPublishDescriptor descriptor) {
        final ArtifactGenerationSet exact = Objects.requireNonNull(artifacts, "artifacts");
        if (!Arrays.equals(descriptor.artifactGenerationSetDigest(), exact.setDigest())) {
            throw new IllegalArgumentException("descriptor and ArtifactGenerationSet differ");
        }
        return exact;
    }

    private static BrokerResourceIdentity mappingTarget(final PulsarAttemptJournal.Mapping mapping) {
        final PulsarTargetResource target = mapping.producer().target();
        return BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                target.authenticatedClusterId(),
                target.resourceIncarnation(),
                target.physicalTopic(),
                target.physicalTopicCreationTimestamp()));
    }
}
