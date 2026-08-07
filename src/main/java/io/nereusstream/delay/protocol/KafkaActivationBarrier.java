package io.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.UUID;

/** Kafka read-committed LSO barrier: the next readable offset is exclusive. */
public record KafkaActivationBarrier(
        ShardId shardId,
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        long exclusiveOffset) implements SourceActivationBarrier {
    public KafkaActivationBarrier {
        Objects.requireNonNull(shardId, "shardId");
        authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (authenticatedClusterId.isBlank() || exclusiveOffset < 0) {
            throw new IllegalArgumentException("invalid Kafka activation barrier");
        }
    }

    @Override
    public SourcePositionKind kind() {
        return SourcePositionKind.KAFKA;
    }

    @Override
    public void validatePosition(final SourcePosition position) {
        if (!(position instanceof KafkaSourcePosition kafka)
                || !shardId.equals(kafka.shardId())
                || !authenticatedClusterId.equals(kafka.authenticatedClusterId())
                || !nativeTopicUuid.equals(kafka.nativeTopicUuid())) {
            throw new IllegalArgumentException("Kafka activation barrier source identity mismatch");
        }
    }

    @Override
    public boolean reachedBy(final SourcePosition lastAppliedPosition) {
        if (lastAppliedPosition == null) {
            return exclusiveOffset == 0;
        }
        validatePosition(lastAppliedPosition);
        final KafkaSourcePosition kafka = (KafkaSourcePosition) lastAppliedPosition;
        final long nextReadableOffset = kafka.offset() == Long.MAX_VALUE
                ? Long.MAX_VALUE : kafka.offset() + 1;
        return nextReadableOffset >= exclusiveOffset;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value) || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be canonical UTF-8");
        }
        return value;
    }
}
