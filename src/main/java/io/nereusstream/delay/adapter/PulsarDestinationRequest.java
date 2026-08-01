package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;

import java.util.Objects;

/** Request handed to a per-SEND guarded Pulsar destination transport. */
public record PulsarDestinationRequest(
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        int partition,
        DestinationLaneId laneId,
        byte[] laneIncarnation,
        DelayMessageId delayMessageId,
        int generation,
        byte[] publishAttemptId,
        long actionAtEpochMs,
        long deliverAtEpochMs,
        byte[] payload,
        byte[] adapterMetadata) {
    public PulsarDestinationRequest {
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(adapterMetadata, "adapterMetadata");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        Bytes.requireLength(publishAttemptId, 32, "publishAttemptId");
        if (authenticatedClusterId.isBlank() || physicalTopic.isBlank() || partition < 0 || generation < 0
                || actionAtEpochMs < 0 || deliverAtEpochMs < actionAtEpochMs) {
            throw new IllegalArgumentException("invalid Pulsar destination request");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
        laneIncarnation = Bytes.copy(laneIncarnation);
        publishAttemptId = Bytes.copy(publishAttemptId);
        payload = Bytes.copy(payload);
        adapterMetadata = Bytes.copy(adapterMetadata);
    }

    public static PulsarDestinationRequest from(final PulsarTargetResource resource,
                                                final DestinationPublishRequest request) {
        return new PulsarDestinationRequest(resource.authenticatedClusterId(), resource.resourceIncarnation(),
                resource.physicalTopic(), resource.partition(), request.laneId(), request.laneIncarnation(),
                request.delayMessageId(), request.generation(), request.publishAttemptId(), request.actionAtEpochMs(),
                request.deliverAtEpochMs(), request.payload(), request.adapterMetadata());
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    @Override
    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    @Override
    public byte[] payload() {
        return Bytes.copy(payload);
    }

    @Override
    public byte[] adapterMetadata() {
        return Bytes.copy(adapterMetadata);
    }
}
