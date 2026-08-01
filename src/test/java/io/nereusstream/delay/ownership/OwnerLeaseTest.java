package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnerLeaseTest {
    @Test
    void epochsFenceOldOwnerAndLeaseLossStopsLocalWork() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease first = authority.acquire(shard, "worker-a", 100, 10).orElseThrow();
        assertTrue(authority.renew(first, 105, 10).isPresent());
        assertFalse(authority.acquire(shard, "worker-b", 114, 10).isPresent());
        assertTrue(authority.acquire(shard, "worker-b", 115, 10).isPresent());
        assertEquals(2, authority.current(shard).orElseThrow().ownerEpoch());
    }
}
