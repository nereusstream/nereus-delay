package io.nereusstream.delay.ownership;

import io.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import io.nereusstream.delay.adapter.DestinationPublishResult;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerShardRuntimePhysicalLookupTest {
    @TempDir
    Path tempDir;

    @Test
    void persistedPhysicalEntryRejectsMissingAttemptBeforeAdapterOrOutcome() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resources"));
        final WorkClassExecutionRegistry workClasses = workClasses();
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("lookup-assignment")), 1,
                new KafkaActivationBarrier(shard, "lookup-cluster", UUID.randomUUID(), 0));
        final OwnerLease lease = backend.acquire(assignment, "lookup-owner",
                Bytes.sha256(Bytes.utf8("lookup-session")), 100, 100).orElseThrow();

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("lookup-deployment"),
                    Bytes.utf8("lookup-worker"), lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("lookup-fence")));
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            final OutcomeWorkClassExecutor outcomes = new OutcomeWorkClassExecutor(workClasses, owned, authority,
                    ignored -> ShardLogMutationAppender.AppendOutcome.unknown());
            final WorkerPhysicalPublishExecutor physical = new WorkerPhysicalPublishExecutor(
                    request -> java.util.concurrent.CompletableFuture.completedFuture(
                            DestinationPublishResult.unknown(
                                    io.nereusstream.delay.protocol.StableCode.DESTINATION_OUTCOME_UNKNOWN, null)),
                    new DestinationPhysicalAdmission(1, 100), workClasses, Runnable::run, outcomes,
                    (attempt, request, clock) -> WorkerPhysicalPublishExecutor.Decision.allowed(),
                    (attempt, request, result) -> { throw new AssertionError("outcome factory was called"); },
                    () -> { throw new AssertionError("missing attempt must not fence the owner"); });
            try (physical) {
                final WorkerShardRuntime runtime = new WorkerShardRuntime(() -> Optional.empty(), workClasses,
                        owned, store, resources, authority, KeyPairGenerator.getInstance("Ed25519")
                        .generateKeyPair().getPublic(), null, null, null, null, physical);

                assertThrows(IllegalStateException.class, () -> runtime.submitPhysicalPublish(
                        Bytes.sha256(Bytes.utf8("missing-attempt")), Bytes.utf8("payload"), () -> 101));
            }
        }
    }

    @Test
    void sourceBoundPhysicalEntryStopsAtTheBoundedSourceWaitWithoutAnAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("source-bound-store"));
        final WorkClassExecutionRegistry workClasses = workClasses();
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final UUID sourceTopic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("source-bound-assignment")), 1,
                new KafkaActivationBarrier(shard, "source-bound-cluster", sourceTopic, 0));
        final OwnerLease lease = backend.acquire(assignment, "source-bound-owner",
                Bytes.sha256(Bytes.utf8("source-bound-session")), 100, 100).orElseThrow();

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("source-bound-deployment"),
                    Bytes.utf8("source-bound-worker"), lease.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("source-bound-fence")));
            final OwnedDelayShard owned = new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()),
                    lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            final OutcomeWorkClassExecutor outcomes = new OutcomeWorkClassExecutor(workClasses, owned, authority,
                    ignored -> ShardLogMutationAppender.AppendOutcome.unknown());
            final WorkerPhysicalPublishExecutor physical = new WorkerPhysicalPublishExecutor(
                    request -> java.util.concurrent.CompletableFuture.completedFuture(
                            DestinationPublishResult.unknown(
                                    io.nereusstream.delay.protocol.StableCode.DESTINATION_OUTCOME_UNKNOWN, null)),
                    new DestinationPhysicalAdmission(1, 100), workClasses, Runnable::run, outcomes,
                    (attempt, request, clock) -> WorkerPhysicalPublishExecutor.Decision.allowed(),
                    (attempt, request, result) -> { throw new AssertionError("source wait must not publish"); },
                    () -> { throw new AssertionError("source wait must not fence the owner"); });
            try (physical) {
                final WorkerShardRuntime runtime = new WorkerShardRuntime(() -> Optional.empty(), workClasses,
                        owned, store, resources, authority, KeyPairGenerator.getInstance("Ed25519")
                        .generateKeyPair().getPublic(), null, null, null, null, physical);
                final SourcePosition admissionPosition = new KafkaSourcePosition(shard, "source-bound-cluster",
                        sourceTopic, 0, null, 2_000);

                final WorkerShardRuntime.SourceBoundPhysicalPublishTurn result =
                        runtime.runSourceBoundPhysicalPublish(Bytes.sha256(Bytes.utf8("missing-attempt")),
                                admissionPosition, new SchedulerBudget(1, 1_000_000,
                                        TimeUnit.SECONDS.toNanos(1)), 1,
                                ignored -> Optional.of(Bytes.utf8("payload")), () -> 101);

                org.junit.jupiter.api.Assertions.assertEquals(
                        WorkerShardRuntime.SourceBoundPhysicalPublishStatus.SOURCE_TURN_LIMIT,
                        result.status());
                org.junit.jupiter.api.Assertions.assertEquals(1, result.sourceTurns());
                org.junit.jupiter.api.Assertions.assertEquals(
                        io.nereusstream.delay.ownership.SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE,
                        result.lastSourceTurn().orElseThrow().status());
                org.junit.jupiter.api.Assertions.assertTrue(result.physicalSubmission().isEmpty());
            }
        }
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 2, 1_000_000, 2, 1_000_000, 1_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }
}
