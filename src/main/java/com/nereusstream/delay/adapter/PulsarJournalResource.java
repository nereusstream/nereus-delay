package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarJournalGenerationResource;
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
        // The Registry encodes physical partitions as uint32. Keep the
        // complete raw bit pattern here; a Java signed high bit is not an
        // invalid partition and must round-trip through the typed Journal
        // resource projection just like other uint32 fields.
        resourceIncarnation = Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    /** Returns this Journal identity in the Registry's typed resource value. */
    public PulsarJournalGenerationResource protocolResource(final long evidenceGeneration) {
        return new PulsarJournalGenerationResource(
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        authenticatedClusterId, resourceIncarnation, physicalTopic, physicalTopicCreationTimestamp)),
                partition,
                evidenceGeneration);
    }

    /** Returns the full ExactResourceIdentity wrapper for GC/protection bindings. */
    public byte[] exactResourceCanonicalBytes(final long evidenceGeneration) {
        return protocolResource(evidenceGeneration).exactResourceCanonicalBytes();
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
