package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.NativePreparedRefV1;

import java.util.Objects;

/** Request handed to a guarded Pulsar native delayed-delivery transport. */
public record PulsarNativeSendRequest(
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition,
        byte[] nativeDeliveryId,
        byte[] submissionHash,
        byte[] preparedBytes) {
    public PulsarNativeSendRequest {
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        Bytes.requireLength(nativeDeliveryId, NativePreparedRefV1.NATIVE_DELIVERY_ID_LENGTH,
                "nativeDeliveryId");
        Bytes.requireLength(submissionHash, NativePreparedDeliveryV1.HASH_LENGTH, "submissionHash");
        Objects.requireNonNull(preparedBytes, "preparedBytes");
        if (authenticatedClusterId.isBlank() || physicalTopic.isBlank() || physicalTopicCreationTimestamp < 0
                || partition < 0 || preparedBytes.length == 0) {
            throw new IllegalArgumentException("invalid Pulsar native send request");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
        nativeDeliveryId = Bytes.copy(nativeDeliveryId);
        submissionHash = Bytes.copy(submissionHash);
        preparedBytes = Bytes.copy(preparedBytes);
    }

    public static PulsarNativeSendRequest from(final PulsarTargetResource resource,
                                                final NativePreparedDeliveryV1 prepared) {
        Objects.requireNonNull(prepared, "prepared");
        return new PulsarNativeSendRequest(resource.authenticatedClusterId(), resource.resourceIncarnation(),
                resource.physicalTopic(), resource.physicalTopicCreationTimestamp(), resource.partition(),
                prepared.nativeDeliveryId(), prepared.submissionHash(), prepared.canonicalBytes());
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] nativeDeliveryId() {
        return Bytes.copy(nativeDeliveryId);
    }

    @Override
    public byte[] submissionHash() {
        return Bytes.copy(submissionHash);
    }

    @Override
    public byte[] preparedBytes() {
        return Bytes.copy(preparedBytes);
    }
}
