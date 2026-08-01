package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;

/** Exact Pulsar Command Topic identity and Broker guard token. */
public record PulsarIngressResource(
        ShardId shardId,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        int partition) {
    public PulsarIngressResource {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        if (authenticatedClusterId.isBlank() || physicalTopic.isBlank() || partition < 0
                || partition != shardId.partition()) {
            throw new IllegalArgumentException("invalid pinned Pulsar ingress resource");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }
}
