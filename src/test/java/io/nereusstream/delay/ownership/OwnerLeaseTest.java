package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
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
        final KafkaActivationBarrier barrier = new KafkaActivationBarrier(shardId, "cluster", topic, 2);
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
                    new SourceReplayMutation(mutation, mutationPosition, null, null)), keyPair.getPublic(), 101);
            assertEquals(2, outcomes.size());
            assertTrue(outcomes.get(0).isCommand());
            assertEquals(StableCode.SCHEDULED, outcomes.get(0).commandResult().stableCode());
            assertFalse(outcomes.get(1).isCommand());
            assertEquals(StableCode.OK, outcomes.get(1).systemMutationResult().stableCode());
            assertEquals(mutationPosition, owned.lastCatchupPosition());
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
}
