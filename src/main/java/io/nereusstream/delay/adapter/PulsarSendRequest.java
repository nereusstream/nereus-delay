package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.PreparedCommand;

import java.util.Objects;

/** Request handed to a Broker-guarded Pulsar transport. */
public record PulsarSendRequest(
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        int partition,
        CommandId commandId,
        byte[] frame) {
    public PulsarSendRequest {
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(frame, "frame");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        if (authenticatedClusterId.isBlank() || physicalTopic.isBlank() || partition < 0 || frame.length == 0) {
            throw new IllegalArgumentException("invalid Pulsar send request");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
        frame = Bytes.copy(frame);
    }

    public static PulsarSendRequest from(final PulsarIngressResource resource, final PreparedCommand command,
                                         final byte[] frame) {
        return new PulsarSendRequest(resource.authenticatedClusterId(), resource.resourceIncarnation(),
                resource.physicalTopic(), resource.partition(), command.commandId(), frame);
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] frame() {
        return Bytes.copy(frame);
    }
}
