package com.nereusstream.delay.protocol;

import java.util.Objects;

/** A physical ingress route partition and therefore one V1 Delay Shard. */
public record ShardId(RouteIncarnation routeIncarnation, int partition) {
    public ShardId {
        Objects.requireNonNull(routeIncarnation, "routeIncarnation");
    }

    /** Returns the partition as its canonical unsigned-32 numeric value. */
    public long unsignedPartition() {
        return Integer.toUnsignedLong(partition);
    }
}
