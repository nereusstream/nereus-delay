package io.nereusstream.delay.protocol;

/** Monotonic source identity used as the sole Shard Log order. */
public sealed interface SourcePosition extends Comparable<SourcePosition>
        permits KafkaSourcePosition, PulsarSourcePosition {
    SourcePositionKind kind();

    ShardId shardId();

    byte[] canonicalBytes();

    /** Closed key variant used in timeline ordering; it is not the full position encoding. */
    byte[] sourceOrderToken();

    long brokerPersistenceTimeEpochMs();

    @Override
    default int compareTo(final SourcePosition other) {
        if (other == null || kind() != other.kind() || !shardId().equals(other.shardId())) {
            throw new IllegalArgumentException("positions are not comparable");
        }
        return compareWithinShard(other);
    }

    int compareWithinShard(SourcePosition other);
}
