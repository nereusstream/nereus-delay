package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PreparedPublishDescriptor;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarReservedProperties;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.ResolvedPayload;
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
        if (exact.deliveryContract() != DeliveryContract.NEREUS_MANAGED_NOT_BEFORE) {
            throw new IllegalArgumentException("managed Journal records require the ordinary delivery contract");
        }
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
