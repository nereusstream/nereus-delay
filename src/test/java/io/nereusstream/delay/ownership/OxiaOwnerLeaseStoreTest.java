package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OxiaOwnerLeaseStoreTest {
    @Test
    void delegatesCasAndPreservesFencedIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        final OwnerLease first = store.acquire(shard, "worker-a", 100, 20).orElseThrow();
        final OwnerLease renewed = store.renew(first, 110, 20).orElseThrow();

        assertEquals(first.ownerEpoch(), renewed.ownerEpoch());
        assertTrue(store.current(shard).isPresent());
        assertTrue(store.release(renewed));
        assertTrue(store.current(shard).isEmpty());
    }

    @Test
    void rejectsBackendAcquireResultForAnotherOwner() {
        final ShardId requested = new ShardId(RouteIncarnation.random(), 1);
        final ShardId returned = new ShardId(RouteIncarnation.random(), 2);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId, final long now,
                                                 final long duration) {
                return Optional.of(new OwnerLease(returned, "other", 1, nonZero(32, 1), now + duration));
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease expected, final long now, final long duration) {
                return Optional.empty();
            }

            @Override
            public boolean release(final OwnerLease expected) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId shardId) {
                return Optional.empty();
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertThrows(IllegalStateException.class, () -> store.acquire(requested, "worker-a", 100, 20));
    }

    @Test
    void rejectsRenewalThatChangesEpochOrToken() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final OwnerLease expected = new OwnerLease(shard, "worker-a", 4, nonZero(32, 2), 200);
        final OxiaOwnerLeaseStore.LeaseCasBackend backend = new OxiaOwnerLeaseStore.LeaseCasBackend() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId, final long now,
                                                 final long duration) {
                return Optional.empty();
            }

            @Override
            public Optional<OwnerLease> renew(final OwnerLease ignored, final long now, final long duration) {
                return Optional.of(new OwnerLease(shard, "worker-a", 5, nonZero(32, 2), now + duration));
            }

            @Override
            public boolean release(final OwnerLease ignored) {
                return false;
            }

            @Override
            public Optional<OwnerLease> current(final ShardId ignored) {
                return Optional.empty();
            }
        };
        final OxiaOwnerLeaseStore store = new OxiaOwnerLeaseStore(backend);
        assertThrows(IllegalStateException.class, () -> store.renew(expected, 150, 20));
    }

    private static byte[] nonZero(final int length, final int firstByte) {
        final byte[] value = new byte[length];
        value[0] = (byte) firstByte;
        return value;
    }
}
