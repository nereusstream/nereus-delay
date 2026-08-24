package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.UUID;

/** Request handed to a pinned Kafka destination transport. */
public record KafkaDestinationRequest(
        String authenticatedClusterId,
        UUID nativeTopicUuid,
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
    public KafkaDestinationRequest {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(adapterMetadata, "adapterMetadata");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        Bytes.requireLength(publishAttemptId, 32, "publishAttemptId");
        if (actionAtEpochMs < 0 || deliverAtEpochMs < actionAtEpochMs) {
            throw new IllegalArgumentException("invalid Kafka destination request");
        }
        // Kafka V1 has no certified delayed-handoff branch.  An early
        // actionAt is only meaningful for the Pulsar guarded handoff path;
        // allowing it here would let a malformed adapter request publish
        // before the consumer-visible deliverAt boundary.
        if (actionAtEpochMs != deliverAtEpochMs) {
            throw new IllegalArgumentException("Kafka managed timing requires actionAt=deliverAt");
        }
        laneIncarnation = Bytes.copy(laneIncarnation);
        publishAttemptId = Bytes.copy(publishAttemptId);
        payload = Bytes.copy(payload);
        adapterMetadata = Bytes.copy(adapterMetadata);
    }

    public static KafkaDestinationRequest from(
            final KafkaTargetResource resource, final DestinationPublishRequest request) {
        return new KafkaDestinationRequest(
                resource.authenticatedClusterId(),
                resource.nativeTopicUuid(),
                resource.partition(),
                request.laneId(),
                request.laneIncarnation(),
                request.delayMessageId(),
                request.generation(),
                request.publishAttemptId(),
                request.actionAtEpochMs(),
                request.deliverAtEpochMs(),
                request.payload(),
                request.adapterMetadata());
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

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
