package io.nereusstream.delay.protocol;

import java.util.Objects;

/** A physical ingress route partition and therefore one V1 Delay Shard. */
public record ShardId(RouteIncarnation routeIncarnation, int partition) {
    public ShardId {
        Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
    }
}

