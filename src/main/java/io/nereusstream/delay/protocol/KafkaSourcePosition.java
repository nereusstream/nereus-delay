package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Kafka source position with the immutable native topic identity. */
public record KafkaSourcePosition(
        ShardId shardId,
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        long offset,
        Integer leaderEpoch,
        long brokerLogAppendTimeEpochMs) implements SourcePosition {
    public KafkaSourcePosition {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
        Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        if (authenticatedClusterId.isBlank() || offset < 0 || brokerLogAppendTimeEpochMs < 0) {
            throw new IllegalArgumentException("invalid Kafka source position");
        }
        if (leaderEpoch != null && leaderEpoch < 0) {
            throw new IllegalArgumentException("leader epoch must be non-negative");
        }
    }

    @Override
    public SourcePositionKind kind() {
        return SourcePositionKind.KAFKA;
    }

    @Override
    public long brokerPersistenceTimeEpochMs() {
        return brokerLogAppendTimeEpochMs;
    }

    @Override
    public byte[] canonicalBytes() {
        final byte[] cluster = authenticatedClusterId.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer result = ByteBuffer.allocate(1 + 16 + 4 + cluster.length + 16 + 4 + 8 + 1
                + (leaderEpoch == null ? 0 : 4) + 8);
        result.put((byte) kind().wireValue());
        result.put(shardId.routeIncarnation().bytes());
        result.putInt(cluster.length).put(cluster);
        result.putLong(nativeTopicUuid.getMostSignificantBits()).putLong(nativeTopicUuid.getLeastSignificantBits());
        result.putInt(shardId.partition()).putLong(offset);
        result.put((byte) (leaderEpoch == null ? 0 : 1));
        if (leaderEpoch != null) {
            result.putInt(leaderEpoch);
        }
        result.putLong(brokerLogAppendTimeEpochMs);
        return result.array();
    }

    @Override
    public int compareWithinShard(final SourcePosition other) {
        final KafkaSourcePosition that = (KafkaSourcePosition) other;
        int result = Long.compare(offset, that.offset);
        if (result == 0) {
            result = compareNullable(leaderEpoch, that.leaderEpoch);
        }
        if (result == 0) {
            result = Long.compare(brokerLogAppendTimeEpochMs, that.brokerLogAppendTimeEpochMs);
        }
        return result;
    }

    private static int compareNullable(final Integer left, final Integer right) {
        if (left == null) {
            return right == null ? 0 : -1;
        }
        return right == null ? 1 : Integer.compare(left, right);
    }
}
