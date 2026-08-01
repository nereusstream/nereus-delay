package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.PreparedCommand;

import java.util.Objects;
import java.util.UUID;

/** Request handed to a request-level pinned-topic Kafka transport. */
public record KafkaProduceRequest(
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        int partition,
        CommandId commandId,
        byte[] frame) {
    public KafkaProduceRequest {
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(frame, "frame");
        if (authenticatedClusterId.isBlank() || partition < 0 || frame.length == 0) {
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
}
