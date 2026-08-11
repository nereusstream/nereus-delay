package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ClaimRecord;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OwnerLeaseTest {
    @TempDir
    Path tempDir;

    @Test
    void negativeClockCannotMakeOwnerLeaseValid() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final OwnerLease lease = new OwnerLease(shard, "worker-a", 1, new byte[32], 100);

        assertFalse(lease.validAt(-1));
        assertTrue(lease.validAt(0));
        assertFalse(lease.validAt(100));
    }

    @Test
    void shardOnlyOwnerLeaseStoreCannotFallbackForContextBoundAssignment() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final InMemoryOwnerLeaseStore delegate = new InMemoryOwnerLeaseStore();
        final OwnerLeaseStore shardOnly = new OwnerLeaseStore() {
            @Override
            public Optional<OwnerLease> acquire(final ShardId ignored, final String ownerId,
                                                final long nowEpochMs, final long leaseDurationMs) {
                return delegate.acquire(ignored, ownerId, nowEpochMs, leaseDurationMs);
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
            public Optional<OwnerLease> current(final ShardId requested) {
                return delegate.current(requested);
            }
        };
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("default-assignment")), 1,
                new KafkaActivationBarrier(shard, "cluster", UUID.randomUUID(), 0));

        assertTrue(shardOnly.acquire(assignment, "worker-a", Bytes.sha256(Bytes.utf8("session")),
                100, 100).isEmpty());
        assertTrue(delegate.current(shard).isEmpty());
    }

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
    void renewalCannotMoveTheLiveExpiryBackwards() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 22);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shard, "worker-monotonic-renewal", 100, 100).orElseThrow();

        assertTrue(authority.renew(lease, 110, 20).isEmpty());
        assertEquals(200, authority.current(shard).orElseThrow().expiresAtEpochMs());
        assertTrue(authority.renew(lease, 110, 100).isPresent());
        assertEquals(210, authority.current(shard).orElseThrow().expiresAtEpochMs());
    }

    @Test
    void ownerEpochSuccessorUsesTheCompleteUnsignedDomain() {
        assertEquals(Long.MIN_VALUE, InMemoryOwnerLeaseStore.nextEpoch(Long.MAX_VALUE));
        assertEquals(-2L, InMemoryOwnerLeaseStore.nextEpoch(-3L));
        assertThrows(IllegalStateException.class, () -> InMemoryOwnerLeaseStore.nextEpoch(-1L));
    }

    @Test
    void overflowingAcquireExpiryDoesNotConsumeOwnerEpoch() {
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);

        assertThrows(ArithmeticException.class,
                () -> authority.acquire(shard, "worker-overflow", Long.MAX_VALUE, 1));

        final OwnerLease acquired = authority.acquire(shard, "worker-retry", 0, 10).orElseThrow();
        assertEquals(1, acquired.ownerEpoch());
    }

    @Test
    void invalidAcquireValueDoesNotConsumeOwnerEpoch() {
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 21);

        assertThrows(IllegalArgumentException.class, () -> authority.acquire(shard, "", 0, 10));

        final OwnerLease acquired = authority.acquire(shard, "worker-retry", 0, 10).orElseThrow();
        assertEquals(1, acquired.ownerEpoch());
    }

    @Test
    void authoritativeApplyFencesAStillLocallyValidStaleLease() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(shardId, "worker-stale", 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("authoritative-apply"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition firstPosition = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final KafkaSourcePosition secondPosition = new KafkaSourcePosition(shardId, "cluster", topic, 1, null, 1_001);
        final PreparedCommand first = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("authoritative-first")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("first")), 10_000);
        final PreparedCommand second = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("authoritative-second")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("second")), 10_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("authoritative-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(firstPosition);
            owned.activateForCommands(authority, 101);
            assertEquals(StableCode.SCHEDULED,
                    owned.applyAuthoritatively(authority, first, firstPosition, 101).stableCode());
            assertEquals(lease.ownerEpoch(), store.runtimeMetadata().lastOpenedOwnerEpoch());

            assertTrue(backend.release(owned.lease()));
            backend.acquire(shardId, "worker-new", 150, 100).orElseThrow();
            assertThrows(IllegalStateException.class,
                    () -> owned.applyAuthoritatively(authority, second, secondPosition, 151));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertEquals(null, owned.shard().getMessage(second.delayMessageId()));
        }
    }

    @Test
    void activationRequeuesRestoredClaimBeforeOpeningCommandGate() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-recovery", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-recovery-activation"));
        final UUID topic = UUID.randomUUID();
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-recovery-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT,
                        Bytes.utf8("claim-recovery")), 10_000);
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"),
                lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("lease")));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard delegate = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, delegate.apply(schedule, position).stableCode());
            delegate.updateLaneReadiness(lane, io.nereusstream.delay.runtime.RuntimeReadiness.READY);
            final ClaimRecord claim = delegate.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());

            final OwnedDelayShard owned = new OwnedDelayShard(delegate, lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("claim-recovery-assignment")),
                    1, new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.activateForCommands(101);

            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
            assertEquals(MessageStatus.SCHEDULED, delegate.getMessage(schedule.delayMessageId()).status());
            assertNull(delegate.getClaim(claim.claimId(), lease.ownerEpoch()));
            assertEquals(1, delegate.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void activationFatalAuthorityFailureFencesTheLocalOwnerGate() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 190);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(shardId, "worker-fatal-activation", 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(
                new ErrorTransitionOwnerLeaseStore(backend));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fatal-activation"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("fatal-activation-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(position);

            assertThrows(AssertionError.class, () -> owned.activateForCommands(authority, 101));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
        }
    }

    @Test
    void drainFatalAuthorityFailureFencesTheLocalOwnerGate() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 191);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(shardId, "worker-fatal-drain", 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(
                new ErrorTransitionOwnerLeaseStore(backend));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fatal-drain"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("fatal-drain-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(position);
            owned.activateForCommands(new OxiaOwnerLeaseStore(backend), 101);

            assertThrows(AssertionError.class, () -> owned.beginDrain(authority, 101));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
        }
    }

    @Test
    void strictActivationRequiresThePersistedShardControlSnapshot() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 42);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-control-snapshot", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("strict-control-activation"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final CompatibleControlSnapshotV1 snapshot = controlSnapshot(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("strict-control-assignment")),
                    1, new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(position);
            assertThrows(IllegalStateException.class,
                    () -> owned.activateForCommandsWithControlSnapshot(snapshot, 101));
            assertEquals(ShardLifecycleState.CATCHING_UP, owned.state());

            store.recordControlSnapshot(snapshot);
            owned.activateForCommandsWithControlSnapshot(snapshot, 101);
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
        }
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
            final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 1);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-1")), 1, barrier));
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
    void authorityGatedDrainRequiresTheExactLeaseSuccessor() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-drain", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("authority-drain"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    acquired);
            final UUID topic = UUID.randomUUID();
            final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                    null, 1_000);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("drain-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(position);
            final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
            owned.activateForCommands(authority, 101);
            owned.beginDrain(authority, 101);
            assertEquals(ShardLifecycleState.DRAINING, owned.state());
            assertEquals(ShardLifecycleState.DRAINING, authority.current(shardId).orElseThrow().state());
            assertEquals(acquired.ownerEpoch(), owned.lease().ownerEpoch());
            assertThrows(IllegalStateException.class, () -> owned.beginDrain(authority, 101));
        }
    }

    @Test
    void authorityGatedActivationKeepsLocalGateClosedDuringLeaseCas() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final ObservingLeaseStore backend = new ObservingLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-activation", 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("activation-cas-state"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                null, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    acquired);
            backend.observedShard.set(owned);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("activation-cas")), 1,
                    new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(position);

            owned.activateForCommands(authority, 101);

            assertEquals(ShardLifecycleState.CATCHING_UP, backend.stateAtTransition.get());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
        }
    }

    @Test
    void authorityGatedDrainFailsClosedWhenLeaseIsExpired() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease acquired = backend.acquire(shardId, "worker-expired-drain", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("expired-drain"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    acquired);
            final UUID topic = UUID.randomUUID();
            final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                    null, 1_000);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("expired-drain-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "cluster", topic, 0)));
            owned.recordCatchup(position);
            final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
            owned.activateForCommands(authority, 101);

            assertThrows(IllegalStateException.class, () -> owned.beginDrain(authority, 200));
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, authority.current(shardId).orElseThrow().state());
        }
    }

    @Test
    void catchupReplayAppliesCommandsBeforeActivationAndAdvancesOnlyAfterCommit() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 10);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-replay", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("catchup-replay"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 1);
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("catchup-replay-lane")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-replay")), 1,
                    barrier));
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    owned.replayCatchup(List.of(new SourceReplayRecord(command, position, null, null)), 101)
                            .get(0).stableCode());
            assertEquals(position, owned.lastCatchupPosition());
            owned.activateForCommands(101);
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    owned.apply(command, position, 101).stableCode());
            assertThrows(IllegalStateException.class,
                    () -> owned.replayCatchup(List.of(new SourceReplayRecord(command,
                            new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 999), null, null)), 101));
        }
    }

    @Test
    void boundedCatchupTurnRetainsTheCursorForTheNextTurn() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 16);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-bounded-replay", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("bounded-catchup-replay"));
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 2);
        final KafkaSourcePosition firstPosition = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                null, 1_000);
        final KafkaSourcePosition secondPosition = new KafkaSourcePosition(shardId, "cluster", topic, 1,
                null, 1_001);
        final PreparedCommand first = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("bounded-first")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("first")), 10_000);
        final PreparedCommand second = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("bounded-second")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("second")), 10_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("bounded-assignment")), 1,
                    barrier));
            final SourceReplayCursor<SourceReplayRecord> cursor = SourceReplayCursor.of(List.of(
                    new SourceReplayRecord(first, firstPosition, null, null),
                    new SourceReplayRecord(second, secondPosition, null, null)).iterator());
            final ReplayTurnBudget budget = new ReplayTurnBudget(1, Long.MAX_VALUE, Long.MAX_VALUE);

            final SourceReplayTurn<io.nereusstream.delay.runtime.CommandResult> firstTurn =
                    owned.replayCatchupTurn(cursor, 101, budget);
            assertEquals(1, firstTurn.results().size());
            assertTrue(firstTurn.hasMore());
            assertEquals(firstPosition, owned.lastCatchupPosition());

            final SourceReplayTurn<io.nereusstream.delay.runtime.CommandResult> secondTurn =
                    owned.replayCatchupTurn(cursor, 101, budget);
            assertEquals(1, secondTurn.results().size());
            assertFalse(secondTurn.hasMore());
            assertEquals(secondPosition, owned.lastCatchupPosition());
        }
    }

    @Test
    void boundedCatchupTurnRejectsARecordLargerThanTheCanonicalByteCap() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-byte-budget", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("byte-budget-replay"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                null, 1_000);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 1);
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("byte-budget-lane")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("byte-budget-assignment")), 1,
                    barrier));
            final SourceReplayCursor<SourceReplayRecord> cursor = SourceReplayCursor.of(List.of(
                    new SourceReplayRecord(command, position, null, null)).iterator());
            assertThrows(IllegalArgumentException.class,
                    () -> owned.replayCatchupTurn(cursor, 101, new ReplayTurnBudget(1, 1, Long.MAX_VALUE)));
            assertEquals(null, owned.lastCatchupPosition());
        }
    }

    @Test
    void liveCatchupClockFencesBeforeApplyingAfterLeaseExpiry() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 15);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-live-clock", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("live-catchup-clock"));
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 2);
        final KafkaSourcePosition firstPosition = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final KafkaSourcePosition secondPosition = new KafkaSourcePosition(shardId, "cluster", topic, 1, null, 1_001);
        final PreparedCommand first = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("live-clock-first")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("first")), 10_000);
        final PreparedCommand second = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("live-clock-second")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("second")), 10_000);
        final AtomicInteger clockReads = new AtomicInteger();
        final java.util.function.LongSupplier liveClock = () -> clockReads.getAndIncrement() < 2 ? 101 : 200;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("live-clock-assignment")), 1,
                    barrier));

            assertThrows(IllegalStateException.class, () -> owned.replayCatchup(List.of(
                    new SourceReplayRecord(first, firstPosition, null, null),
                    new SourceReplayRecord(second, secondPosition, null, null)), liveClock));

            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertEquals(firstPosition, owned.lastCatchupPosition());
            assertEquals(null, owned.shard().getMessage(second.delayMessageId()));
        }
    }

    @Test
    void v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 11);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-gap", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("catchup-gap"));
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 3);
        final KafkaSourcePosition first = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final KafkaSourcePosition gap = new KafkaSourcePosition(shardId, "cluster", topic, 2, null, 1_002);
        final PreparedCommand firstCommand = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("gap-lane-1")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("first")), 10_000);
        final PreparedCommand gapCommand = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("gap-lane-2")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("gap")), 10_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("gap-assignment")), 1,
                    barrier), SourceReplaySuccessor.strictKafka());
            final SourceReplayRecord firstRecord = new SourceReplayRecord(firstCommand, first, null, null);
            final SourceReplayRecord gapRecord = new SourceReplayRecord(gapCommand, gap, null, null);
            final SourceReplayCursor<SourceReplayRecord> cursor = SourceReplayCursor.of(List.of(
                    firstRecord, gapRecord).iterator());
            assertThrows(IllegalStateException.class,
                    () -> owned.replayCatchupTurn(cursor, 101, ReplayTurnBudget.unbounded()));
            assertEquals(first, owned.lastCatchupPosition());
            assertEquals(gapRecord, cursor.peek());
        }
    }

    @Test
    void catchupReplayAppliesSignedSystemMutationsBeforeActivation() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 13);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-system-replay", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-catchup-replay"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition position = new KafkaSourcePosition(shardId, "cluster", topic, 0, null, 1_000);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 1);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(2_000, 2_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("replay-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("replay-proof")), 0, null);
        final byte[] proofId = Bytes.sha256(Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()), Bytes.i64be(2_000),
                Bytes.u32be(1), Bytes.lp32(proof.canonicalBytes()));
        final AuthorIdentity author = AuthorIdentity.fence(Bytes.utf8("replay-fence"), 1);
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.TIME_FENCE, 9_000,
                proofId, timeFenceBody(shardId, 2_000, 1, proofId, proof.canonicalBytes()),
                author.canonicalBytes(), 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-system-replay")),
                    1, barrier));
            assertEquals(StableCode.OK, owned.replaySystemMutations(
                    List.of(new SourceReplayMutation(mutation, position, null, null)), keyPair.getPublic(), 101)
                    .get(0).stableCode());
            assertEquals(position, owned.lastCatchupPosition());
            owned.activateForCommands(101);
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
        }
    }

    @Test
    void mixedCatchupReplayKeepsCommandAndSystemMutationInOneSourceOrder() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 14);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-mixed-replay", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("mixed-catchup-replay"));
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition commandPosition = new KafkaSourcePosition(shardId, "cluster", topic, 0,
                null, 1_000);
        final KafkaSourcePosition mutationPosition = new KafkaSourcePosition(shardId, "cluster", topic, 1,
                null, 1_001);
        final KafkaSourcePosition duplicateCommandPosition = new KafkaSourcePosition(shardId, "cluster", topic, 2,
                null, 1_002);
        final KafkaSourcePosition duplicateMutationPosition = new KafkaSourcePosition(shardId, "cluster", topic, 3,
                null, 1_003);
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 4);
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("mixed-replay-lane")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(2_000, 2_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("mixed-replay-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("mixed-replay-proof")), 0, null);
        final byte[] proofId = Bytes.sha256(Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()), Bytes.i64be(2_000),
                Bytes.u32be(1), Bytes.lp32(proof.canonicalBytes()));
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.TIME_FENCE, 9_000,
                proofId, timeFenceBody(shardId, 2_000, 1, proofId, proof.canonicalBytes()),
                AuthorIdentity.fence(Bytes.utf8("mixed-replay-fence"), 1).canonicalBytes(), 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-mixed-replay")),
                    1, barrier));
            final List<SourceReplayOutcome> outcomes = owned.replay(List.of(
                    new SourceReplayRecord(command, commandPosition, null, null),
                    new SourceReplayMutation(mutation, mutationPosition, null, null),
                    new SourceReplayRecord(command, duplicateCommandPosition, null, null),
                    new SourceReplayMutation(mutation, duplicateMutationPosition, null, null)),
                    keyPair.getPublic(), 101);
            assertEquals(4, outcomes.size());
            assertTrue(outcomes.get(0).isCommand());
            assertEquals(StableCode.SCHEDULED, outcomes.get(0).commandResult().stableCode());
            assertFalse(outcomes.get(1).isCommand());
            assertEquals(StableCode.OK, outcomes.get(1).systemMutationResult().stableCode());
            assertEquals(duplicateCommandPosition, outcomes.get(2).position());
            assertArrayEquals(duplicateCommandPosition.canonicalBytes(), outcomes.get(2).commandResult()
                    .appliedSourcePosition());
            assertEquals(duplicateMutationPosition, outcomes.get(3).position());
            assertArrayEquals(duplicateMutationPosition.canonicalBytes(), outcomes.get(3).systemMutationResult()
                    .appliedSourcePosition());
            assertArrayEquals(commandPosition.canonicalBytes(), owned.shard().getCommandResult(command.commandId())
                    .appliedSourcePosition());
            assertArrayEquals(mutationPosition.canonicalBytes(), owned.shard()
                    .getSystemMutationResult(mutation.systemMutationId()).appliedSourcePosition());
            assertEquals(duplicateMutationPosition, owned.lastCatchupPosition());
            owned.activateForCommands(101);
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, owned.state());
        }
    }

    @Test
    void sourceAssignmentMustMatchLeaseContextAndActivationUsesAuthorityCas() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 0);
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("assignment-8")), 1, barrier);
        final SourceAssignment differentAssignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("different-assignment-8")), 2, barrier);
        final byte[] session = Bytes.sha256(Bytes.utf8("session-8"));
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(differentAssignment, "worker-a", session, 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lease-context"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            assertThrows(IllegalArgumentException.class, () -> owned.markCatchingUp(assignment));

            final InMemoryOwnerLeaseStore matchingAuthority = new InMemoryOwnerLeaseStore();
            final OwnedDelayShard matching = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    matchingAuthority.acquire(assignment, "worker-b", Bytes.sha256(Bytes.utf8("session-8b")), 100, 100)
                            .orElseThrow());
            matching.markCatchingUp(assignment);
            matching.activateForCommands(new OxiaOwnerLeaseStore(matchingAuthority), 101);
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, matching.state());
            assertEquals(ShardLifecycleState.ACTIVE_FOR_COMMANDS, matching.lease().state());
        }
    }

    @Test
    void sourceAssignmentEpochMustMatchLeaseContextEvenWhenIdIsReused() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 0);
        final byte[] reusedId = Bytes.sha256(Bytes.utf8("reused-assignment-id"));
        final SourceAssignment leaseAssignment = new SourceAssignment(shardId, reusedId, 4, barrier);
        final SourceAssignment replayedAssignment = new SourceAssignment(shardId, reusedId.clone(), 5, barrier);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(leaseAssignment, "worker-epoch",
                Bytes.sha256(Bytes.utf8("epoch-session")), 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("assignment-epoch"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            assertThrows(IllegalArgumentException.class, () -> owned.markCatchingUp(replayedAssignment));
        }
    }

    @Test
    void legacyZeroEpochLeaseContextCannotAuthorizeV1Assignment() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 12);
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 0);
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("positive-assignment-epoch")), 1, barrier);
        final OwnerLease legacyContextLease = new OwnerLease(shardId, "worker-legacy-context", 1,
                Bytes.sha256(Bytes.utf8("legacy-context-token")), 200,
                new OwnerLeaseContext(assignment.assignmentId(), Bytes.sha256(Bytes.utf8("legacy-session"))),
                ShardLifecycleState.ACQUIRING);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("legacy-context-epoch"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    legacyContextLease);
            assertThrows(IllegalArgumentException.class, () -> owned.markCatchingUp(assignment));
            assertEquals(ShardLifecycleState.RESTORING, owned.state());
        }
    }

    @Test
    void lifecycleCasRejectsBackwardTransitionsAndFencedLeaseReactivation() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 15);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease acquiring = authority.acquire(shard, "worker-lifecycle", 100, 100).orElseThrow();
        final OwnerLease active = authority.transition(acquiring, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                .orElseThrow();
        assertTrue(authority.transition(active, ShardLifecycleState.RESTORING).isEmpty());

        final OwnerLease fenced = authority.transition(active, ShardLifecycleState.FENCED).orElseThrow();
        assertTrue(authority.transition(fenced, ShardLifecycleState.ACTIVE_FOR_COMMANDS).isEmpty());
        assertTrue(authority.transition(fenced, ShardLifecycleState.ACQUIRING).isPresent());
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

    @Test
    void emptyKafkaBarrierStillPinsTheFirstAppliedRecord() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 6);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-empty", 100, 100).orElseThrow();
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("empty-barrier"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            final UUID topic = UUID.randomUUID();
            final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 0);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-empty")), 1,
                    barrier));
            final KafkaSourcePosition sameSource = new KafkaSourcePosition(shardId, "cluster", topic, 0, null,
                    1_000);
            assertThrows(IllegalArgumentException.class,
                    () -> owned.recordCatchup(sameSource, 1L, Bytes.sha256(Bytes.utf8("unexpected-proof"))));
            owned.activateForCommands(101);
            final PreparedCommand command = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("empty-barrier-lane")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final KafkaSourcePosition replacement = new KafkaSourcePosition(shardId, "cluster", UUID.randomUUID(),
                    0, null, 1_000);
            assertThrows(IllegalArgumentException.class, () -> owned.apply(command, replacement, 101));
        }
    }

    @Test
    void emptyPulsarBarrierRejectsAStalePersistedCursorBeforeActivation() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-empty-pulsar", 100, 100).orElseThrow();
        final byte[] staleResource = Bytes.sha256(Bytes.utf8("stale-pulsar-resource"));
        final byte[] assignedResource = Bytes.sha256(Bytes.utf8("assigned-pulsar-resource"));
        final byte[] guard = Bytes.sha256(Bytes.utf8("empty-pulsar-guard"));
        final String topic = "persistent://tenant/empty-pulsar";
        final PulsarSourcePosition stalePosition = new PulsarSourcePosition(shardId, staleResource, topic,
                4, 8, 0, 1, PulsarSourcePosition.EntryKind.NON_BATCH, 1_000);
        final PulsarActivationBarrier barrier = PulsarActivationBarrier.empty(shardId, assignedResource, topic,
                7, guard);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("empty-pulsar-barrier"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard delegate = new DelayShard(store, DelayShardConfig.defaults());
            final PreparedCommand command = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("empty-pulsar-lane")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            delegate.apply(command, stalePosition);

            final OwnedDelayShard owned = new OwnedDelayShard(delegate, lease);
            owned.markCatchingUp(new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("assignment-empty-pulsar")), 1, barrier));
            assertThrows(IllegalArgumentException.class, () -> owned.activateForCommands(101));
            assertEquals(ShardLifecycleState.CATCHING_UP, owned.state());
        }
    }

    @Test
    void catchupCursorRejectsSameKafkaOffsetWithDifferentCanonicalMetadata() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 16);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-cursor-fence", 100, 100).orElseThrow();
        final UUID topic = UUID.randomUUID();
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 1);
        final KafkaSourcePosition first = new KafkaSourcePosition(shardId, "cluster", topic, 0, 3, 1_000);
        final KafkaSourcePosition conflicting = new KafkaSourcePosition(shardId, "cluster", topic, 0, 4, 1_001);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("catchup-cursor-fence"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("assignment-cursor-fence")), 1, barrier));
            owned.recordCatchup(first);
            assertThrows(IllegalStateException.class, () -> owned.recordCatchup(conflicting));
            assertEquals(first, owned.lastCatchupPosition());
        }
    }

    @Test
    void pulsarCatchupAndApplyRequireTheGuardedSourceConnectionGeneration() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-pulsar", 100, 100).orElseThrow();
        final byte[] resource = Bytes.sha256(Bytes.utf8("pulsar-resource"));
        final byte[] guard = Bytes.sha256(Bytes.utf8("guard-generation-7"));
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shardId, resource,
                "persistent://tenant/commands-partition-7", 4, 8, 2, 3, Long.MIN_VALUE, guard, false);
        final PulsarSourcePosition catchup = new PulsarSourcePosition(shardId, resource,
                "persistent://tenant/commands-partition-7", 4, 8, 2, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1_000);
        final PulsarSourcePosition next = new PulsarSourcePosition(shardId, resource,
                "persistent://tenant/commands-partition-7", 4, 9, 0, 1,
                PulsarSourcePosition.EntryKind.NON_BATCH, 1_001);
        final PreparedCommand catchupCommand = PreparedCommand.schedule(shardId,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("pulsar-catchup-lane")), 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("catchup-payload")), 10_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("pulsar-generation"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-pulsar")), 1,
                    barrier));
            assertThrows(IllegalArgumentException.class, () -> owned.recordCatchup(catchup));
            owned.replayCatchup(List.of(new SourceReplayRecord(catchupCommand, catchup, Long.MIN_VALUE, guard)), 101);
            owned.activateForCommands(101);
            final PreparedCommand command = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("pulsar-generation-lane")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            assertThrows(IllegalArgumentException.class, () -> owned.apply(command, next, 101));
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    owned.apply(command, next, 101, Long.MIN_VALUE, guard).stableCode());
        }
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shardId) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 101), 1, bytes(32, 102), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 103), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] timeFenceBody(final ShardId shard, final long closeThrough, final int keyVersion,
                                        final byte[] proofId, final byte[] proof) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, closeThrough);
            CanonicalProtobuf.uint32(output, 11, keyVersion);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }

    private static final class ObservingLeaseStore implements OwnerLeaseStore {
        private final InMemoryOwnerLeaseStore delegate = new InMemoryOwnerLeaseStore();
        private final AtomicReference<OwnedDelayShard> observedShard = new AtomicReference<>();
        private final AtomicReference<ShardLifecycleState> stateAtTransition = new AtomicReference<>();

        @Override
        public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                             final long nowEpochMs, final long leaseDurationMs) {
            return delegate.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs);
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
            stateAtTransition.set(observedShard.get().state());
            return delegate.transition(expected, nextState);
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return delegate.current(shardId);
        }
    }

    private static final class ErrorTransitionOwnerLeaseStore implements OwnerLeaseStore {
        private final InMemoryOwnerLeaseStore delegate;

        private ErrorTransitionOwnerLeaseStore(final InMemoryOwnerLeaseStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId,
                                            final long nowEpochMs, final long leaseDurationMs) {
            return delegate.acquire(shardId, ownerId, nowEpochMs, leaseDurationMs);
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
            throw new AssertionError("simulated fatal Oxia transition failure");
        }

        @Override
        public Optional<OwnerLease> current(final ShardId shardId) {
            return delegate.current(shardId);
        }
    }
}
