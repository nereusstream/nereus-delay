package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;

import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

/** Exact Pulsar Command Topic identity and Broker guard token. */
public record PulsarIngressResource(
        ShardId shardId,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition) {
    public PulsarIngressResource {
        Objects.requireNonNull(shardId, "shardId");
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        physicalTopic = canonicalText(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        if (partition != shardId.partition()) {
            throw new IllegalArgumentException("invalid pinned Pulsar ingress resource");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
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
}
