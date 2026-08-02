package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnerDrainCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void drainStopsAdmissionFlushesCheckpointsClosesAndReleasesLease() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 46);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-drain", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final Path checkpoint = tempDir.resolve("drain-final-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final AtomicInteger stopCalls = new AtomicInteger();
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(500, 0, checkpoint), () -> 101,
                    stopCalls::incrementAndGet);

            assertEquals(0, result.revokedClaims());
            assertEquals(0, result.callbackPolls());
            assertEquals(checkpoint, result.finalCheckpointPath());
            assertEquals(1, stopCalls.get());
            assertTrue(Files.isRegularFile(checkpoint.resolve("CURRENT")));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isEmpty());
            assertEquals(1, store.runtimeMetadata().lastOpenedOwnerEpoch());
            store.close();
        }
    }

    @Test
    void expiredDrainDeadlineLeavesDbAndLeaseForAVisibleRetry() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 47);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-deadline"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-deadline", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(101, 0, null), () -> 101, () -> { }));

            assertEquals(ShardLifecycleState.DRAINING, owned.state());
            assertTrue(backend.current(shardId).isPresent());
            assertFalse(store.runtimeMetadata().cleanCloseMarker());
            store.close();
            assertTrue(backend.release(acquired));
        }
    }

    private static OwnedDelayShard activeOwnedShard(final ShardStore store, final OwnerLease lease,
                                                    final OxiaOwnerLeaseStore authority, final ShardId shardId) {
        final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "drain-cluster", topic, 0, null, 1_000);
        owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("drain-assignment")), 1,
                new KafkaActivationBarrier(shardId, "drain-cluster", topic, 0)));
        owned.recordCatchup(position);
        owned.activateForCommands(authority, 101);
        return owned;
    }
}
