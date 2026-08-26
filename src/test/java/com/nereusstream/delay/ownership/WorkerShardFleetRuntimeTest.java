package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.CheckpointPublicationCoordinator;
import com.nereusstream.delay.store.CheckpointScheduler;
import com.nereusstream.delay.store.CheckpointUploadIntentStore;
import com.nereusstream.delay.store.RecoveryCatalog;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerCheckpointRuntime;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkerShardFleetRuntimeTest {
    @TempDir
    Path tempDir;

    @Test
    void sourceTurnsRoundRobinAcrossAcceptedShardsAndDoNotInventOptionalGraphs() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fleet-resources"));
        final WorkClassExecutionRegistry workClasses = workClasses();
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                Fixture first = new Fixture(config, resources, workClasses, authority, backend, 1, "first", true);
                Fixture second = new Fixture(config, resources, workClasses, authority, backend, 2, "second", false)) {
            final WorkerShardFleetRuntime fleet =
                    new WorkerShardFleetRuntime(workClasses, resources, List.of(first.runtime, second.runtime));

            assertEquals(List.of(first.shard, second.shard), fleet.shardIds());
            assertEquals(
                    first.shard, fleet.runNextSourceTurn(budget(), () -> 101).shardId());
            assertEquals(
                    second.shard, fleet.runNextSourceTurn(budget(), () -> 101).shardId());
            assertEquals(
                    first.shard, fleet.runNextSourceTurn(budget(), () -> 101).shardId());
            assertTrue(fleet.runNextSchedulingTurn(budget()).isEmpty());
            assertTrue(fleet.runNextCommandTurn(budget()).isEmpty());
            assertEquals(
                    first.shard,
                    fleet.runNextCheckpointTurn(budget()).orElseThrow().shardId());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new WorkerShardFleetRuntime(workClasses, resources, List.of(first.runtime, first.runtime)));

            first.runtime.registerCheckpoint(101);
            final CheckpointScheduler.ScheduledCheckpoint checkpointClaim =
                    first.runtime.claimDueCheckpoints(201, 1).get(0);
            assertThrows(
                    IllegalStateException.class,
                    () -> first.runtime.drain(
                            new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> {}));
            assertTrue(first.checkpointRuntime.scheduler().isInFlight(first.shard));
            first.checkpointRuntime.scheduler().complete(checkpointClaim, 201);
            first.runtime.drain(new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> {});
            assertEquals(0, first.checkpointRuntime.scheduler().size());
            second.runtime.drain(new OwnerDrainCoordinator.DrainRequest(5_000, 0, null), () -> 101, () -> {});
            fleet.close();
            assertThrows(IllegalStateException.class, () -> fleet.runNextSourceTurn(budget(), () -> 101));
        }
    }

    @Test
    void closeAttemptsEveryShardAndRetainsTheFirstDrainFailure() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fleet-close-resources"));
        final WorkClassExecutionRegistry workClasses = workClasses();
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                Fixture first =
                        new Fixture(config, resources, workClasses, authority, backend, 1, "close-first", false);
                Fixture second =
                        new Fixture(config, resources, workClasses, authority, backend, 2, "close-second", false)) {
            final WorkerShardFleetRuntime fleet =
                    new WorkerShardFleetRuntime(workClasses, resources, List.of(first.runtime, second.runtime));

            final IllegalStateException failure = assertThrows(IllegalStateException.class, fleet::close);

            assertEquals("Worker shard runtime must complete owner drain before close", failure.getMessage());
            assertEquals(1, failure.getSuppressed().length);
            assertEquals(
                    "Worker shard runtime must complete owner drain before close",
                    failure.getSuppressed()[0].getMessage());
        }
    }

    private static SchedulerBudget budget() {
        return new SchedulerBudget(1, 1_000_000, 1_000);
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass =
                    switch (workClass) {
                        case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                        case QUERY, CHECKPOINT -> false;
                    };
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            4,
                            1_000_000,
                            4,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), new AtomicLong()::get);
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shard;
        private final ShardStore store;
        private final WorkerShardRuntime runtime;
        private final WorkerCheckpointRuntime checkpointRuntime;

        private Fixture(
                final ShardStoreConfig config,
                final SharedRocksDbResources resources,
                final WorkClassExecutionRegistry workClasses,
                final OxiaOwnerLeaseStore authority,
                final InMemoryOwnerLeaseStore backend,
                final int partition,
                final String identity,
                final boolean withCheckpoint)
                throws Exception {
            shard = new ShardId(RouteIncarnation.random(), partition);
            final SourceAssignment assignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("fleet-assignment-" + identity)),
                    1,
                    new KafkaActivationBarrier(shard, "fleet-cluster", UUID.randomUUID(), 0));
            final OwnerLease lease = backend.acquire(
                            assignment,
                            "fleet-owner-" + identity,
                            Bytes.sha256(Bytes.utf8("fleet-session-" + identity)),
                            100,
                            100)
                    .orElseThrow();
            store = ShardStore.open(config, shard, resources);
            final OwnerIdentity owner = new OwnerIdentity(
                    Bytes.utf8("fleet-deployment"),
                    Bytes.utf8("fleet-worker-" + identity),
                    lease.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("fleet-owner-fence-" + identity)));
            final OwnedDelayShard owned =
                    new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            if (withCheckpoint) {
                checkpointRuntime = new WorkerCheckpointRuntime(
                        workClasses,
                        new CheckpointScheduler(100, 0, 1),
                        store,
                        new CheckpointPublicationCoordinator(
                                resources, new CheckpointUploadIntentStore(), new RecoveryCatalog()),
                        request -> {});
                runtime = new WorkerShardRuntime(
                        () -> Optional.empty(),
                        workClasses,
                        owned,
                        store,
                        resources,
                        authority,
                        java.security.KeyPairGenerator.getInstance("Ed25519")
                                .generateKeyPair()
                                .getPublic(),
                        null,
                        null,
                        checkpointRuntime);
            } else {
                checkpointRuntime = null;
                runtime = new WorkerShardRuntime(
                        () -> Optional.empty(),
                        workClasses,
                        owned,
                        store,
                        resources,
                        authority,
                        java.security.KeyPairGenerator.getInstance("Ed25519")
                                .generateKeyPair()
                                .getPublic());
            }
        }

        @Override
        public void close() {
            if (!store.isClosed()) {
                store.close();
            }
        }
    }
}
