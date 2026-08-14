package io.nereusstream.delay.transport;

import io.nereusstream.delay.protocol.AdapterKindV1;

import java.text.Normalizer;
import java.util.Objects;
import java.util.UUID;

/** Exact Kafka topic identity plus credential binding. */
public record KafkaCommandTransportKey(
        String authenticatedClusterId,
        String canonicalTopic,
        UUID nativeTopicUuid,
        int partition,
        CredentialBindingKey credentialBinding) implements CommandTransportKey {
    public KafkaCommandTransportKey {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        canonicalTopic = canonicalText(canonicalTopic, "canonicalTopic");
        nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (nativeTopicUuid.equals(new UUID(0, 0)) || partition < 0) {
            throw new IllegalArgumentException("invalid Kafka transport identity");
        }
        credentialBinding = Objects.requireNonNull(credentialBinding, "credentialBinding");
    }

    @Override
    public AdapterKindV1 kind() {
        return AdapterKindV1.KAFKA;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
