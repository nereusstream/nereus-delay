package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
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
import java.util.List;
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
    void pulsarCatchupAndApplyRequireTheGuardedSourceConnectionGeneration() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final InMemoryOwnerLeaseStore authority = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = authority.acquire(shardId, "worker-pulsar", 100, 100).orElseThrow();
        final byte[] resource = Bytes.sha256(Bytes.utf8("pulsar-resource"));
        final byte[] guard = Bytes.sha256(Bytes.utf8("guard-generation-7"));
        final PulsarActivationBarrier barrier = new PulsarActivationBarrier(shardId, resource,
                "persistent://tenant/commands-partition-7", 4, 8, 2, 7, guard, false);
        final PulsarSourcePosition catchup = new PulsarSourcePosition(shardId, resource,
                "persistent://tenant/commands-partition-7", 4, 8, 2, 3,
                PulsarSourcePosition.EntryKind.BATCH, 1_000);
        final PulsarSourcePosition next = new PulsarSourcePosition(shardId, resource,
                "persistent://tenant/commands-partition-7", 4, 9, 0, 1,
                PulsarSourcePosition.EntryKind.NON_BATCH, 1_001);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("pulsar-generation"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(new SourceAssignment(shardId, Bytes.sha256(Bytes.utf8("assignment-pulsar")), 1,
                    barrier));
            assertThrows(IllegalArgumentException.class, () -> owned.recordCatchup(catchup));
            owned.recordCatchup(catchup, 7L, guard);
            owned.activateForCommands(101);
            final PreparedCommand command = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("pulsar-generation-lane")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            assertThrows(IllegalArgumentException.class, () -> owned.apply(command, next, 101));
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    owned.apply(command, next, 101, 7L, guard).stableCode());
        }
    }
}
