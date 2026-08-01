package io.nereusstream.delay.protocol;

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
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
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
    public boolean reachedBy(final SourcePosition lastAppliedPosition) {
        if (lastAppliedPosition == null) {
            return exclusiveOffset == 0;
        }
        if (!(lastAppliedPosition instanceof KafkaSourcePosition kafka)
                || !shardId.equals(kafka.shardId())
                || !authenticatedClusterId.equals(kafka.authenticatedClusterId())
                || !nativeTopicUuid.equals(kafka.nativeTopicUuid())) {
            throw new IllegalArgumentException("Kafka activation barrier source identity mismatch");
        }
        return Math.addExact(kafka.offset(), 1) >= exclusiveOffset;
    }
}
