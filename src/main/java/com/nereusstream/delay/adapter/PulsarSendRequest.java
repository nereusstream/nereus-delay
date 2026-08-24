package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.transport.TransportRequest;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;

/** Request handed to a Broker-guarded Pulsar transport. */
public record PulsarSendRequest(
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition,
        CommandId commandId,
        byte[] frame)
        implements TransportRequest {
    public PulsarSendRequest {
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
        physicalTopic = canonicalText(physicalTopic, "physicalTopic");
        Objects.requireNonNull(commandId, "commandId");
        Objects.requireNonNull(frame, "frame");
        Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
        if (frame.length == 0) {
            throw new IllegalArgumentException("invalid Pulsar send request");
        }
        resourceIncarnation = Bytes.copy(resourceIncarnation);
        frame = Bytes.copy(frame);
    }

    public static PulsarSendRequest from(
            final PulsarIngressResource resource, final PreparedCommand command, final byte[] frame) {
        return new PulsarSendRequest(
                resource.authenticatedClusterId(),
                resource.resourceIncarnation(),
                resource.physicalTopic(),
                resource.physicalTopicCreationTimestamp(),
                resource.partition(),
                command.commandId(),
                frame);
    }

    @Override
    public byte[] resourceIncarnation() {
        return Bytes.copy(resourceIncarnation);
    }

    @Override
    public byte[] frame() {
        return Bytes.copy(frame);
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
