package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NativePreparedRecordContext;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarReservedProperties;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.ReservedPublishMetadata;
import com.nereusstream.delay.protocol.ResolvedPayload;
import java.util.Arrays;
import java.util.Objects;

/**
 * Joins a current AUTO_FAST envelope with its logical identity and exact
 * artifact set. Unlike the managed factory, this path deliberately has no
 * Journal or sequence allocator: the final record uses producer-assigned
 * sequence authority.
 */
public final class PulsarNativePreparedRecordFactory {
    private PulsarNativePreparedRecordFactory() {}

    public static PulsarPreparedRecord create(
            final NativePreparedDelivery prepared,
            final NativePreparedRecordContext context,
            final ArtifactGenerationSet artifacts) {
        final NativePreparedDelivery exact = Objects.requireNonNull(prepared, "prepared");
        final NativePreparedRecordContext exactContext = Objects.requireNonNull(context, "context");
        final ArtifactGenerationSet exactArtifacts = Objects.requireNonNull(artifacts, "artifacts");
        if (!exact.isCurrentGeneration()
                || exact.nativeDeliveryPolicy() != NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF
                || exact.deliveryContract() != DeliveryContract.PULSAR_NATIVE_DELIVERY
                || exact.handoffPolicySnapshot() == null
                || !Arrays.equals(exactContext.artifactGenerationSetDigest(), exactArtifacts.setDigest())) {
            throw new IllegalArgumentException("AUTO_FAST native record inputs are not current and exact");
        }
        if (!exact.destination().equals(exact.capabilitySnapshot().destination())
                || !exact.capability().equals(exact.capabilitySnapshot().capability())
                || !exact.target().equals(exact.capabilitySnapshot().target())
                || exact.physicalPartition() != exact.capabilitySnapshot().physicalPartition()) {
            throw new IllegalArgumentException("AUTO_FAST native record snapshot identity mismatch");
        }

        final PulsarMetadata metadata = exact.metadata();
        final PulsarKey key = metadata.partitionKey() == null
                ? PulsarKey.none()
                : metadata.keyEncoding() == PulsarMetadata.KeyEncoding.UTF8
                        ? PulsarKey.utf8(metadata.partitionKey())
                        : PulsarKey.binary(metadata.partitionKey());
        final ReservedPublishMetadata reserved = new ReservedPublishMetadata(
                exactContext.routeIncarnation(),
                exactContext.shardPartition(),
                exactContext.messageId(),
                exactContext.generation(),
                exactContext.publishAttemptId(),
                exact.destination().semanticHash(),
                exact.capability().semanticHash(),
                exact.deliverAtEpochMs(),
                DeliveryMode.MANAGED);
        final PulsarRecordTemplate template = new PulsarRecordTemplate(
                BrokerResourceIdentity.pulsar(exact.target()),
                Integer.toUnsignedLong(exact.physicalPartition()),
                key,
                metadata.orderingKey(),
                metadata.properties(),
                exact.eventTimeEpochMs(),
                reserved,
                DeliveryContract.PULSAR_NATIVE_DELIVERY,
                exact.deliverAtEpochMs(),
                PayloadForPublish.inline(exact.inlinePayload()),
                exactArtifacts.setDigest());
        final ResolvedPayload payload = ResolvedPayload.of(exact.inlinePayload());
        return new PulsarPreparedRecord(
                template,
                template.recordTemplateHash(),
                payload,
                PulsarSequenceAuthority.producerAssigned(),
                ExternalDeliveryIdentity.nativeDelivery(exact.nativeDeliveryId()),
                exact.submissionHash(),
                PulsarReservedProperties.all(reserved, exactContext.publishAttemptId(), exact.submissionHash()),
                exactArtifacts.setDigest());
    }
}
