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
    public byte[] sourceOrderToken() {
        return ByteBuffer.allocate(1 + 8).put((byte) 1).putLong(offset).array();
    }

    @Override
    public boolean sameSourceIdentity(final SourcePosition other) {
        if (!(other instanceof KafkaSourcePosition that)) {
            return false;
        }
        return authenticatedClusterId.equals(that.authenticatedClusterId)
                && nativeTopicUuid.equals(that.nativeTopicUuid);
    }

    @Override
    public int compareWithinShard(final SourcePosition other) {
        final KafkaSourcePosition that = (KafkaSourcePosition) other;
        // A Kafka partition offset is the physical Shard Log order.  Leader
        // epoch and append time are authenticated metadata on that record,
        // not a second order dimension.  Treating either field as a tie-break
        // would let two canonical positions for one offset appear as a later
        // record and bypass the exact-position integrity fence.
        return Long.compare(offset, that.offset);
    }
}
