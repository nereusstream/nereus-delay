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
        if (!sameSourceIdentity(other)) {
            throw new IllegalArgumentException("positions belong to different source resources");
        }
        return compareWithinShard(other);
    }

    /**
     * Returns whether two positions are from the same authenticated physical
     * source. A ShardId alone is not sufficient: a route can be accidentally
     * fed records from a replacement topic or broker resource incarnation.
     */
    boolean sameSourceIdentity(SourcePosition other);

    int compareWithinShard(SourcePosition other);
}
