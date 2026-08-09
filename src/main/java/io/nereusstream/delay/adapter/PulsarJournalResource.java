package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;

/** Exact physical identity of one Nereus-owned Pulsar Attempt Journal. */
public record PulsarJournalResource(
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition) {
    public PulsarJournalResource {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        physicalTopic = canonicalText(physicalTopic, "physicalTopic");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
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
