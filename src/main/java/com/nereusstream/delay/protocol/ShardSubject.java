package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Canonical route-partition subject shared by every System Mutation. */
public final class ShardSubject {
    private final RouteIncarnation routeIncarnation;
    private final int partition;

    public ShardSubject(final RouteIncarnation routeIncarnation, final int partition) {
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.partition = partition;
    }

    public ShardSubject(final ShardId shardId) {
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

    public static ShardSubject decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ShardSubject");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "ShardSubject");
        final int partition = QueryCodecSupport.uint32Bits(fields.get(1), 2);
        final ShardSubject result = new ShardSubject(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(0), 1, RouteIncarnation.LENGTH)), partition);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ShardSubject");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ShardSubject that
                && partition == that.partition
                && routeIncarnation.equals(that.routeIncarnation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routeIncarnation, partition);
    }
}
