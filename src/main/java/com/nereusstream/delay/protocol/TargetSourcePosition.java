package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Resource bounds for full Source Positions retained by the Target format. */
public final class TargetSourcePosition {
    public static final int MAX_CLUSTER_UTF8_BYTES = 256;
    public static final int MAX_TOPIC_UTF8_BYTES = 1 << 20;
    public static final int MAX_KAFKA_CANONICAL_BYTES = 1 + 16 + 4 + MAX_CLUSTER_UTF8_BYTES + 16 + 4 + 8 + 1 + 4 + 8;
    public static final int MAX_PULSAR_CANONICAL_BYTES =
            1 + 16 + 4 + 32 + 4 + MAX_TOPIC_UTF8_BYTES + 4 + 8 + 8 + 4 + 4 + 1 + 8;
    public static final int MAX_CANONICAL_BYTES = Math.max(MAX_KAFKA_CANONICAL_BYTES, MAX_PULSAR_CANONICAL_BYTES);

    private TargetSourcePosition() {}

    public static SourcePosition decode(final byte[] encoded) {
        if (encoded == null
                || encoded.length == 0
                || encoded.length > MAX_CANONICAL_BYTES
                || (encoded[0] == 1 && encoded.length > MAX_KAFKA_CANONICAL_BYTES)) {
            throw new IllegalArgumentException("Target source position exceeds its encoding bound");
        }
        return requireBounded(SourcePositionCodec.decode(encoded));
    }

    public static SourcePosition requireBounded(final SourcePosition position) {
        Objects.requireNonNull(position, "position");
        if (position instanceof KafkaSourcePosition kafka) {
            requireTextBound(kafka.authenticatedClusterId(), MAX_CLUSTER_UTF8_BYTES);
            if (kafka.nativeTopicUuid().getMostSignificantBits() == 0
                    && kafka.nativeTopicUuid().getLeastSignificantBits() == 0) {
                throw new IllegalArgumentException("Target Kafka source resource is unassigned");
            }
        } else if (position instanceof PulsarSourcePosition pulsar) {
            requireTextBound(pulsar.physicalTopic(), MAX_TOPIC_UTF8_BYTES);
            if (Arrays.equals(pulsar.brokerResourceIncarnation(), new byte[32])) {
                throw new IllegalArgumentException("Target Pulsar source resource is unassigned");
            }
        }
        return position;
    }

    private static void requireTextBound(final String text, final int maximumBytes) {
        if (text.length() > maximumBytes || text.getBytes(StandardCharsets.UTF_8).length > maximumBytes) {
            throw new IllegalArgumentException("Target source identity text exceeds its UTF-8 bound");
        }
    }
}
