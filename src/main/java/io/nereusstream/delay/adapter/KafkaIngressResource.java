package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;
import java.util.UUID;

/** Exact Kafka Command Topic identity pinned by a Route registration. */
public record KafkaIngressResource(
        ShardId shardId,
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        int partition) {
    public KafkaIngressResource {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (authenticatedClusterId.isBlank() || partition < 0 || partition != shardId.partition()) {
            throw new IllegalArgumentException("invalid pinned Kafka ingress resource");
        }
    }
}
