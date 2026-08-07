package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.PreparedCommand;

import java.util.Objects;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

/** Request handed to a request-level pinned-topic Kafka transport. */
public record KafkaProduceRequest(
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        int partition,
        CommandId commandId,
        byte[] frame) {
    public KafkaProduceRequest {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(frame, "frame");
        if (partition < 0 || frame.length == 0) {
            throw new IllegalArgumentException("invalid Kafka produce request");
        }
        frame = Bytes.copy(frame);
    }

    public static KafkaProduceRequest from(final KafkaIngressResource resource, final PreparedCommand command,
                                           final byte[] frame) {
        return new KafkaProduceRequest(resource.authenticatedClusterId(), resource.nativeTopicUuid(),
                resource.partition(), command.commandId(), frame);
    }

    @Override
    public byte[] frame() {
        return Bytes.copy(frame);
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
