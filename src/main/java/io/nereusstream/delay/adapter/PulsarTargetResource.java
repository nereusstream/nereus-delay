package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Objects;

/** Exact Pulsar target resource identity bound to a Destination Profile. */
public record PulsarTargetResource(
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition) {
    public PulsarTargetResource {
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        if (authenticatedClusterId.isBlank() || physicalTopic.isBlank() || physicalTopicCreationTimestamp < 0
                || partition < 0) {
            throw new IllegalArgumentException("invalid Pulsar target resource");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }
}
