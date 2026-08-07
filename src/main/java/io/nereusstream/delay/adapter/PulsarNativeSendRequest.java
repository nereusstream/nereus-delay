package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.NativePreparedRefV1;

import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

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
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        physicalTopic = canonicalText(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        Bytes.requireLength(nativeDeliveryId, NativePreparedRefV1.NATIVE_DELIVERY_ID_LENGTH,
                "nativeDeliveryId");
        Bytes.requireLength(submissionHash, NativePreparedDeliveryV1.HASH_LENGTH, "submissionHash");
        Objects.requireNonNull(preparedBytes, "preparedBytes");
        if (physicalTopicCreationTimestamp < 0
                || partition < 0 || preparedBytes.length == 0) {
            throw new IllegalArgumentException("invalid Pulsar native send request");
        }
        requireNonZero(nativeDeliveryId, "nativeDeliveryId");
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

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value) || value.isBlank() || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static void requireNonZero(final byte[] value, final String name) {
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
