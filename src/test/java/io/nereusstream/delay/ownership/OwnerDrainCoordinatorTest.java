package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnerDrainCoordinatorTest {
    @TempDir
    Path tempDir;

    @Test
    void constructorRejectsAnotherStoreIncarnationForTheSameShard() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 70);
        final ShardStoreConfig firstConfig = ShardStoreConfig.defaults(tempDir.resolve("drain-first-store"));
        final ShardStoreConfig secondConfig = ShardStoreConfig.defaults(tempDir.resolve("drain-second-store"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-drain-store-fence", 100, 500)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources firstResources = new SharedRocksDbResources(firstConfig);
             SharedRocksDbResources secondResources = new SharedRocksDbResources(secondConfig);
             ShardStore firstStore = ShardStore.open(firstConfig, shardId, firstResources);
             ShardStore secondStore = ShardStore.open(secondConfig, shardId, secondResources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(
                    new DelayShard(firstStore, DelayShardConfig.defaults()), acquired);

            assertThrows(IllegalArgumentException.class, () -> new OwnerDrainCoordinator(
                    owned, secondStore, secondResources, authority, workClasses(1)));
            assertFalse(secondStore.isCloseStarted());
            assertTrue(backend.current(shardId).isPresent());
        }
    }

    @Test
    void drainStopsAdmissionFlushesCheckpointsClosesAndReleasesLease() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 46);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-drain", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final Path checkpoint = tempDir.resolve("drain-final-checkpoint");
        final byte[] checkpointId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("drain-checkpoint")), 16);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final AtomicInteger stopCalls = new AtomicInteger();
            assertThrows(NullPointerException.class,
                    () -> new OwnerDrainCoordinator(owned, store, resources, authority, null));
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                    authority, workClasses(1));

            final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(500, 0, checkpoint, checkpointId), () -> 101,
                    stopCalls::incrementAndGet);

            assertEquals(0, result.revokedClaims());
            assertEquals(0, result.callbackPolls());
            assertEquals(checkpoint, result.finalCheckpointPath());
            assertEquals(1, stopCalls.get());
            assertTrue(Files.isRegularFile(checkpoint.resolve("CURRENT")));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isEmpty());
            assertEquals(1, store.runtimeMetadata().lastOpenedOwnerEpoch());
            org.junit.jupiter.api.Assertions.assertArrayEquals(checkpointId,
                    store.runtimeMetadata().lastCheckpointId());
            store.close();
        }
    }

    @Test
    void finalCheckpointWaitsForTheSharedCheckpointWorkClassBeforeClosingStore() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 61);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-checkpoint-queue"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-drain-queue", 100, 5_000).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final Path checkpoint = tempDir.resolve("drain-checkpoint-queue-output");
        final byte[] checkpointId = bytes(16, 77);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final WorkClassExecutionRegistry workClasses = workClasses(2);
            final WorkClassTask occupied = new WorkClassTask(WorkClass.CHECKPOINT, "occupied-drain", 1);
            workClasses.submit(occupied, () -> { });
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                    authority, workClasses);

            final OwnerDrainCoordinator.DrainResult waiting = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(4_000, 0, checkpoint, checkpointId), () -> 101,
                    () -> { });
            assertEquals(WorkClass.CHECKPOINT, waiting.pendingCheckpointTask().workClass());
            assertTrue(Files.notExists(checkpoint));
            assertFalse(store.isClosed());
            assertEquals(ShardLifecycleState.DRAINING, owned.state());
            assertTrue(backend.current(shardId).isPresent());

            workClasses.runTurn(new SchedulerBudget(1, 1, 1_000));
            final OwnerDrainCoordinator.DrainResult completed = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(4_000, 0, checkpoint, checkpointId), () -> 102,
                    () -> { });
            assertEquals(checkpoint, completed.finalCheckpointPath());
            assertTrue(Files.isRegularFile(checkpoint.resolve("CURRENT")));
            assertTrue(store.isClosed());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isEmpty());
        }
    }

    @Test
    void finalCheckpointQueueRejectionLeavesDrainingStateForExactRetry() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 62);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-checkpoint-rejected"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-drain-rejected", 100, 5_000).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final Path checkpoint = tempDir.resolve("drain-checkpoint-rejected-output");
        final byte[] checkpointId = bytes(16, 88);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            workClasses.submit(new WorkClassTask(WorkClass.CHECKPOINT, "occupied-rejected", 1), () -> { });
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                    authority, workClasses);
            final OwnerDrainCoordinator.DrainRequest request =
                    new OwnerDrainCoordinator.DrainRequest(4_000, 0, checkpoint, checkpointId);

            assertThrows(IllegalStateException.class, () -> coordinator.drain(request, () -> 101, () -> { }));
            assertEquals(ShardLifecycleState.DRAINING, owned.state());
            assertTrue(backend.current(shardId).isPresent());
            assertTrue(Files.notExists(checkpoint));
            assertEquals(null, store.runtimeMetadata().lastCheckpointId());

            workClasses.runTurn(new SchedulerBudget(1, 1, 1_000));
            final OwnerDrainCoordinator.DrainResult completed = coordinator.drain(request, () -> 102, () -> { });
            assertEquals(checkpoint, completed.finalCheckpointPath());
            assertTrue(Files.isRegularFile(checkpoint.resolve("CURRENT")));
            assertTrue(store.isClosed());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isEmpty());
        }
    }

    @Test
    void uncertainStoreClosesAndReleasesOnlyTheMatchingOwnerLease() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 55);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-uncertain-store"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-uncertain-store", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            assertThrows(ShardStore.RocksDbWriteFailure.class,
                    () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(4),
                            Bytes.utf8("malformed-ingress-fence"))));
            final AtomicInteger stopCalls = new AtomicInteger();
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                    authority, workClasses(1));

            final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet);

            assertEquals(0, result.revokedClaims());
            assertEquals(0, result.callbackPolls());
            assertEquals(null, result.finalCheckpointPath());
            assertEquals(1, stopCalls.get());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(store.isClosed());
            assertTrue(backend.current(shardId).isEmpty());
        }
    }

    @Test
    void uncertainStoreWithExternalCloseStillStopsSourceBeforeRelease() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 60);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-uncertain-external-close"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-uncertain-external-close", 100, 500)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            try {
                final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
                assertThrows(ShardStore.RocksDbWriteFailure.class,
                        () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(4),
                                Bytes.utf8("malformed-ingress-fence"))));
                // Simulate a caller that began native teardown before the drain
                // coordinator observed the uncertain write boundary.
                store.close();
                final AtomicInteger stopCalls = new AtomicInteger();
                final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                        authority);

                coordinator.drain(new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                        stopCalls::incrementAndGet);

                assertEquals(1, stopCalls.get());
                assertEquals(ShardLifecycleState.FENCED, owned.state());
                assertTrue(store.isClosed());
                assertTrue(backend.current(shardId).isEmpty());
            } finally {
                store.close();
            }
        }
    }

    @Test
    void uncertainStoreFencesBeforeStopCallbackFailureCanEscape() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 58);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-uncertain-callback"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-uncertain-callback", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            assertThrows(ShardStore.RocksDbWriteFailure.class,
                    () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(4),
                            Bytes.utf8("malformed-ingress-fence"))));
            final IllegalStateException callbackFailure = new IllegalStateException("source stop failed");
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                    authority, workClasses(1));

            assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    () -> { throw callbackFailure; }));

            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isPresent());
            assertFalse(store.isClosed());
        }
    }

    @Test
    void uncertainStoreNeverReleasesAReplacementOwnerLease() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 57);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-uncertain-replacement"));
        final InMemoryOwnerLeaseStore delegate = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = delegate.acquire(shardId, "worker-uncertain-replacement", 100, 500).orElseThrow();
        final OwnerLease replacement = new OwnerLease(shardId, "worker-new", acquired.ownerEpoch() + 1,
                Bytes.sha256(Bytes.utf8("uncertain-replacement")), 500, null,
                ShardLifecycleState.ACTIVE_FOR_COMMANDS);
        final Path checkpoint = tempDir.resolve("drain-uncertain-replacement-checkpoint");
        Files.createDirectories(checkpoint);
        Files.writeString(checkpoint.resolve("CURRENT"), "replacement-visible");
        final LeaseLossAfterCheckpointBackend backend =
                new LeaseLossAfterCheckpointBackend(delegate, replacement, checkpoint);
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            assertThrows(ShardStore.RocksDbWriteFailure.class,
                    () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(4),
                            Bytes.utf8("malformed-ingress-fence"))));
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> { }));

            assertEquals("owner lease changed while closing uncertain Store", failure.getMessage());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(store.isClosed());
            assertEquals(replacement, backend.current(shardId).orElseThrow());
            assertTrue(delegate.current(shardId).isPresent());
        }
    }

    @Test
    void uncertainStoreCloseFailureRetainsAReproducibleTeardownRetry() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 56);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-uncertain-retry"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-uncertain-retry", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            assertThrows(ShardStore.RocksDbWriteFailure.class,
                    () -> store.write(batch -> batch.put(ColumnFamily.META, KeyCodec.metaFixed(4),
                            Bytes.utf8("malformed-ingress-fence"))));
            final AtomicInteger stopCalls = new AtomicInteger();
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);
            resources.releaseDbSlot();

            assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(store.isCloseStarted());
            assertFalse(store.isClosed());
            assertTrue(backend.current(shardId).isPresent());
            assertEquals(1, stopCalls.get());

            resources.acquireDbSlot();
            final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet);
            assertEquals(0, result.revokedClaims());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(store.isClosed());
            assertTrue(backend.current(shardId).isEmpty());
            assertEquals(1, stopCalls.get());
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

    @Test
    void externallyStartedStoreCloseEntersDrainAndReleasesTheMatchingLease() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 59);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-external-close"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-external-close", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            try {
                store.close();
                final AtomicInteger stopCalls = new AtomicInteger();
                final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

                final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                        new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                        stopCalls::incrementAndGet);

                assertEquals(0, result.revokedClaims());
                assertEquals(0, result.callbackPolls());
                assertEquals(1, stopCalls.get());
                assertEquals(ShardLifecycleState.FENCED, owned.state());
                assertTrue(store.isClosed());
                assertTrue(backend.current(shardId).isEmpty());
            } finally {
                store.close();
            }
        }
    }

    @Test
    void storeCloseFailureLeavesDrainingStateForRetryableTeardown() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 53);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-close-retry"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-close-retry", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            // Force the first close's DB-slot release to fail after native
            // teardown.  This is a deterministic injection of the same
            // retryable slot-release failure that a worker can observe during
            // a real resource race.
            resources.releaseDbSlot();
            final AtomicInteger stopCalls = new AtomicInteger();
            assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet));
            assertEquals(ShardLifecycleState.DRAINING, owned.state());
            assertTrue(backend.current(shardId).isPresent());
            assertTrue(store.isCloseStarted());
            assertFalse(store.isClosed());
            assertEquals(1, stopCalls.get());

            // Restore the test slot accounting and retry only the teardown;
            // no Claims/callbacks/flush decisions are replayed.
            resources.acquireDbSlot();
            final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    stopCalls::incrementAndGet);
            assertEquals(0, result.revokedClaims());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(store.isClosed());
            assertTrue(backend.current(shardId).isEmpty());
            assertEquals(1, stopCalls.get());
        }
    }

    @Test
    void unconfirmedLeaseReleaseKeepsClosedDrainRetryable() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 54);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-release-retry"));
        final FlakyReleaseBackend backend = new FlakyReleaseBackend();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final OwnerLease acquired = authority.acquire(shardId, "worker-release-retry", 100, 500).orElseThrow();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            // The first release response is unconfirmed while the exact lease
            // remains present. Closing the DB must not fence the local state
            // before this lease can be retried.
            assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> { }));
            assertEquals(ShardLifecycleState.DRAINING, owned.state());
            assertTrue(store.isClosed());
            assertTrue(backend.current(shardId).isPresent());
            assertEquals(1, backend.releaseCalls);

            final OwnerDrainCoordinator.DrainResult result = coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> { });
            assertEquals(0, result.revokedClaims());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isEmpty());
            assertEquals(2, backend.releaseCalls);
        }
    }

    @Test
    void drainReleasesTheCurrentLeaseAfterAQuiescenceRenewal() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 48);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-renewal"));
        final RecordingLeaseBackend backend = new RecordingLeaseBackend();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final OwnerLease acquired = authority.acquire(shardId, "worker-renewal", 100, 500).orElseThrow();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            coordinator.drain(new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> {
                final OwnerLease renewed = authority.renew(owned.lease(), 110, 1_000).orElseThrow();
                owned.updateLease(renewed);
            });

            assertEquals(1_110, backend.releasedLease.expiresAtEpochMs());
        }
    }

    @Test
    void drainOffersOnlyTheLastDurablyAppliedSourcePositionToTheHintCommitter() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 49);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-source-hint"));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-source-hint", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final UUID topic = UUID.randomUUID();
            final KafkaSourcePosition persisted = new KafkaSourcePosition(shardId, "drain-source-hint-cluster",
                    topic, 1, null, 1_001);
            final PreparedCommand command = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("drain-source-hint-lane")),
                            2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("hint")), 9_000);
            owned.shard().apply(command, persisted);
            final AtomicReference<SourcePosition> committedHint = new AtomicReference<>();
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

            coordinator.drain(new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    new OwnerDrainCoordinator.DrainCallbacks() {
                        @Override
                        public void stopSourceAndScheduling() {
                        }

                        @Override
                        public void commitSourceHint(final SourcePosition persistedPosition) {
                            committedHint.set(persistedPosition);
                        }
                    });

            assertEquals(persisted, committedHint.get());
        }
    }

    @Test
    void leaseLossAfterFinalCheckpointFencesWithoutClosingOrReleasingTheNewOwner() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 50);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("drain-checkpoint-lease-loss"));
        final InMemoryOwnerLeaseStore delegate = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = delegate.acquire(shardId, "worker-checkpoint-lease-loss", 100, 500).orElseThrow();
        final OwnerLease replacement = new OwnerLease(shardId, "worker-new", acquired.ownerEpoch() + 1,
                Bytes.sha256(Bytes.utf8("replacement-lease")), 500, null, ShardLifecycleState.DRAINING);
        final Path checkpoint = tempDir.resolve("drain-checkpoint-lease-loss-output");
        final LeaseLossAfterCheckpointBackend backend =
                new LeaseLossAfterCheckpointBackend(delegate, replacement, checkpoint);
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources,
                    authority, workClasses(1));

            assertThrows(IllegalStateException.class, () -> coordinator.drain(
                    new OwnerDrainCoordinator.DrainRequest(5_000, 0, checkpoint), () -> 101, () -> { }));

            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(Files.isRegularFile(checkpoint.resolve("CURRENT")));
            assertEquals(replacement, backend.current(shardId).orElseThrow());
        }
    }

    @Test
    void duplicateCoordinatorCannotDrainTheSameShardConcurrently() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 51);
        final ShardStoreConfig config = new ShardStoreConfig(tempDir.resolve("drain-duplicate"),
                1, 1, 256, 256, 2, 4L * 1024 * 1024, 4L * 1024 * 1024,
                1, 1, 1, 4L * 1024 * 1024, 2, 4L * 1024 * 1024, 1, 1, 2);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-duplicate", 100, 500).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);
            final AtomicReference<RuntimeException> duplicateFailure = new AtomicReference<>();

            coordinator.drain(new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                    new OwnerDrainCoordinator.DrainCallbacks() {
                        @Override
                        public void stopSourceAndScheduling() {
                            try {
                                new OwnerDrainCoordinator(owned, store, resources, authority).drain(
                                        new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101,
                                        () -> { });
                            } catch (RuntimeException failure) {
                                duplicateFailure.set(failure);
                            }
                        }
                    });

            assertTrue(duplicateFailure.get() instanceof IllegalStateException);
            assertEquals("owner drain is already in progress for this shard", duplicateFailure.get().getMessage());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(backend.current(shardId).isEmpty());
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

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 10_000,
                    maxQueueRecords, 10_000, 1_000, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 1, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 20_000), new AtomicLong()::get);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class RecordingLeaseBackend implements OxiaOwnerLeaseStore.LeaseCasBackend {
        private final InMemoryOwnerLeaseStore delegate = new InMemoryOwnerLeaseStore();
        private OwnerLease releasedLease;

        @Override
        public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                            final long nowEpochMs, final long leaseDurationMs) {
            return delegate.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                            final byte[] sessionIdentity, final long nowEpochMs,
                                            final long leaseDurationMs) {
            return delegate.acquire(assignment, ownerId, sessionIdentity, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                          final long leaseDurationMs) {
            return delegate.renew(expected, nowEpochMs, leaseDurationMs);
        }

        @Override
        public boolean release(final OwnerLease expected) {
            final OwnerLease current = delegate.current(expected.shardId()).orElse(null);
            if (current == null || current.expiresAtEpochMs() != expected.expiresAtEpochMs()) {
                return false;
            }
            releasedLease = expected;
            return delegate.release(expected);
        }

        @Override
        public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
            return delegate.transition(expected, nextState);
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return delegate.current(shardId);
        }
    }

    private static final class FlakyReleaseBackend implements OxiaOwnerLeaseStore.LeaseCasBackend {
        private final InMemoryOwnerLeaseStore delegate = new InMemoryOwnerLeaseStore();
        private int releaseCalls;

        @Override
        public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                            final long nowEpochMs, final long leaseDurationMs) {
            return delegate.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                            final byte[] sessionIdentity, final long nowEpochMs,
                                            final long leaseDurationMs) {
            return delegate.acquire(assignment, ownerId, sessionIdentity, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                          final long leaseDurationMs) {
            return delegate.renew(expected, nowEpochMs, leaseDurationMs);
        }

        @Override
        public boolean release(final OwnerLease expected) {
            releaseCalls++;
            return releaseCalls == 1 ? false : delegate.release(expected);
        }

        @Override
        public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
            return delegate.transition(expected, nextState);
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return delegate.current(shardId);
        }
    }

    private static final class LeaseLossAfterCheckpointBackend implements OxiaOwnerLeaseStore.LeaseCasBackend {
        private final InMemoryOwnerLeaseStore delegate;
        private final OwnerLease replacement;
        private final Path checkpoint;

        private LeaseLossAfterCheckpointBackend(final InMemoryOwnerLeaseStore delegate,
                                                final OwnerLease replacement, final Path checkpoint) {
            this.delegate = delegate;
            this.replacement = replacement;
            this.checkpoint = checkpoint;
        }

        @Override
        public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                            final long nowEpochMs, final long leaseDurationMs) {
            return delegate.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                            final byte[] sessionIdentity, final long nowEpochMs,
                                            final long leaseDurationMs) {
            return delegate.acquire(assignment, ownerId, sessionIdentity, nowEpochMs, leaseDurationMs);
        }

        @Override
        public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                          final long leaseDurationMs) {
            return delegate.renew(expected, nowEpochMs, leaseDurationMs);
        }

        @Override
        public boolean release(final OwnerLease expected) {
            return delegate.release(expected);
        }

        @Override
        public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
            return delegate.transition(expected, nextState);
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return Files.isRegularFile(checkpoint.resolve("CURRENT"))
                    ? Optional.of(replacement) : delegate.current(shardId);
        }
    }
}
