package io.nereusstream.delay.adapter;

import java.util.Objects;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

/** Exact Kafka target topic identity pinned by a Destination Profile. */
public record KafkaTargetResource(String authenticatedClusterId, UUID nativeTopicUuid, int partition) {
    public KafkaTargetResource {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (partition < 0) {
            throw new IllegalArgumentException("invalid Kafka target resource");
        }
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
