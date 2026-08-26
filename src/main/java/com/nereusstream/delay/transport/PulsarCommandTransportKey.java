package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.AdapterKind;
import java.text.Normalizer;
import java.util.Objects;

/** Exact Pulsar physical topic/resource incarnation plus credential binding. */
public record PulsarCommandTransportKey(
        String authenticatedClusterId,
        String canonicalPhysicalTopic,
        Bytes32 resourceIncarnation,
        long topicCreationTimestamp,
        int partition,
        CredentialBindingKey credentialBinding)
        implements CommandTransportKey {
    public PulsarCommandTransportKey {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        canonicalPhysicalTopic = canonicalText(canonicalPhysicalTopic, "canonicalPhysicalTopic");
        resourceIncarnation = Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        if (topicCreationTimestamp < 0 || partition < 0) {
            throw new IllegalArgumentException("invalid Pulsar transport identity");
        }
        credentialBinding = Objects.requireNonNull(credentialBinding, "credentialBinding");
    }

    @Override
    public AdapterKind kind() {
        return AdapterKind.PULSAR;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
