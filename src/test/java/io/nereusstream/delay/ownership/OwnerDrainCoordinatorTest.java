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
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        final byte[] checkpointId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("drain-checkpoint")), 16);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final ShardStore store = ShardStore.open(config, shardId, resources);
            final OwnedDelayShard owned = activeOwnedShard(store, acquired, authority, shardId);
            final AtomicInteger stopCalls = new AtomicInteger();
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

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
            final OwnerDrainCoordinator coordinator = new OwnerDrainCoordinator(owned, store, resources, authority);

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
