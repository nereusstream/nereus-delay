package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Canonical route-partition subject shared by every System Mutation. */
public final class ShardSubjectV1 {
    private final RouteIncarnation routeIncarnation;
    private final int partition;

    public ShardSubjectV1(final RouteIncarnation routeIncarnation, final int partition) {
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.partition = partition;
    }

    public ShardSubjectV1(final ShardId shardId) {
        this(Objects.requireNonNull(shardId, "shardId").routeIncarnation(), shardId.partition());
    }

    public RouteIncarnation routeIncarnation() {
        return routeIncarnation;
    }

    public int partition() {
        return partition;
    }

    public ShardId shardId() {
        return new ShardId(routeIncarnation, partition);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, routeIncarnation.bytes());
            CanonicalProtobuf.uint32Bits(output, 2, partition);
        });
    }

    /** Raw hash-preimage form fixed by the System Mutation Registry. */
    public byte[] canonicalHashBytes() {
        return Bytes.concat(routeIncarnation.bytes(), Bytes.u32beBits(partition));
    }

    public static ShardSubjectV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ShardSubjectV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "ShardSubjectV1");
        final int partition = QueryCodecSupport.uint32Bits(fields.get(1), 2);
        final ShardSubjectV1 result = new ShardSubjectV1(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(0), 1, RouteIncarnation.LENGTH)), partition);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ShardSubjectV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ShardSubjectV1 that
                && partition == that.partition
                && routeIncarnation.equals(that.routeIncarnation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routeIncarnation, partition);
    }
}
