package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OwnerLeaseTest {
    @TempDir
    Path tempDir;

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

    @Test
    void ownerCannotApplyBeforeRestoreAndCatchUpBarriers() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-a", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            final PreparedCommand command = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("owner-lane")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final UUID topic = UUID.randomUUID();
            final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                    null, 1_000);
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> owned.apply(command, position, 101));
            owned.markCatchingUp(new KafkaActivationBarrier(shardId, "cluster", topic, 1));
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> owned.activateForCommands(101));
            final KafkaSourcePosition replacement = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                    0, null, 1_000);
            assertThrows(IllegalArgumentException.class, () -> owned.recordCatchup(replacement));
            owned.recordCatchup(position);
            owned.activateForCommands(101);
            assertTrue(owned.apply(command, position, 101).stableCode()
                    == io.nereusstream.delay.protocol.StableCode.SCHEDULED);
            owned.beginDrain();
            assertEquals(ShardLifecycleState.DRAINING, owned.state());
        }
    }

    @Test
    void leaseRenewalCannotChangeTokenOrMoveExpiryBackwards() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final OwnerLease lease = new OwnerLease(shard, "worker-a", 7, new byte[32], 200);
        final OwnedDelayShard owned;
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("renewal"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            assertThrows(IllegalArgumentException.class,
                    () -> owned.updateLease(new OwnerLease(shard, "worker-b", 7, new byte[32], 250)));
            final byte[] wrongToken = new byte[32];
            wrongToken[0] = 1;
            assertThrows(IllegalArgumentException.class,
                    () -> owned.updateLease(new OwnerLease(shard, "worker-a", 7, wrongToken, 250)));
            assertThrows(IllegalArgumentException.class,
                    () -> owned.updateLease(new OwnerLease(shard, "worker-a", 7, new byte[32], 199)));
            owned.updateLease(new OwnerLease(shard, "worker-a", 7, new byte[32], 250));
            assertEquals(250, owned.lease().expiresAtEpochMs());
        }
    }
}
