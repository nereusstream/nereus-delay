package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import java.util.Objects;

/** Immutable target record prepared after a durable Publish Admission. */
public record DestinationPublishRequest(
        DestinationLaneId laneId,
        byte[] laneIncarnation,
        DelayMessageId delayMessageId,
        int generation,
        byte[] publishAttemptId,
        long actionAtEpochMs,
        long deliverAtEpochMs,
        byte[] payload,
        byte[] adapterMetadata) {
    public DestinationPublishRequest {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        Bytes.requireLength(publishAttemptId, 32, "publishAttemptId");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(adapterMetadata, "adapterMetadata");
        if (actionAtEpochMs < 0 || deliverAtEpochMs < actionAtEpochMs) {
            throw new IllegalArgumentException("invalid destination publish timing or generation");
        }
        laneIncarnation = Bytes.copy(laneIncarnation);
        publishAttemptId = Bytes.copy(publishAttemptId);
        payload = Bytes.copy(payload);
        adapterMetadata = Bytes.copy(adapterMetadata);
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
