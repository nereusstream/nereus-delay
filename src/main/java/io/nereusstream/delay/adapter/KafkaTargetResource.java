package io.nereusstream.delay.adapter;

import java.util.Objects;
import java.util.UUID;

/** Exact Kafka target topic identity pinned by a Destination Profile. */
public record KafkaTargetResource(String authenticatedClusterId, UUID nativeTopicUuid, int partition) {
    public KafkaTargetResource {
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (authenticatedClusterId.isBlank() || partition < 0) {
            throw new IllegalArgumentException("invalid Kafka target resource");
        }
    }
}
