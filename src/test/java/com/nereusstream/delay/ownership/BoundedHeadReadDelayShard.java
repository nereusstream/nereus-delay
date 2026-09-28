package com.nereusstream.delay.ownership;

import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.HeadReadPolicy;
import com.nereusstream.delay.store.ShardStore;

/** Finite test fixture policy for strict Owner activation; not a production envelope certificate. */
final class BoundedHeadReadDelayShard {
    private static final HeadReadPolicy POLICY = new HeadReadPolicy(4096, 64L << 20, 60_000_000_000L);

    private BoundedHeadReadDelayShard() {}

    static DelayShard create(final ShardStore store, final DelayShardConfig config) {
        return new DelayShard(store, config, POLICY);
    }
}
