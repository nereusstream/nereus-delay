package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourceApplyWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void mixedActiveSourceRecordsUseExactBoundedActionsAndBrokerOwnedFailure() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 26);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("source-work-assignment")),
                7,
                new KafkaActivationBarrier(shard, "source-work-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(
                        assignment, "source-work-owner", Bytes.sha256(Bytes.utf8("source-work-session")), 100, 100)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final KafkaSourcePosition commandPosition =
                new KafkaSourcePosition(shard, "source-work-cluster", topic, 0, null, 1_000);
        final KafkaSourcePosition mutationPosition =
                new KafkaSourcePosition(shard, "source-work-cluster", topic, 1, null, 1_001);
        final KafkaSourcePosition failedPosition =
                new KafkaSourcePosition(shard, "source-work-cluster", topic, 2, null, 1_002);
        final PreparedCommand command = schedule(shard, "source-work-command");
        final PreparedCommand expiredCommand = schedule(shard, "source-work-expired");
        final SystemMutation mutation = timeFence(shard, keyPair);
        final SourceReplayRecord commandEntry = new SourceReplayRecord(command, commandPosition, null, null);
        final SourceReplayMutation mutationEntry = new SourceReplayMutation(mutation, mutationPosition, null, null);
        final SourceReplayRecord failedEntry = new SourceReplayRecord(expiredCommand, failedPosition, null, null);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("source-work-store"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shard, resources)) {
            final OwnedDelayShard owned =
                    new OwnedDelayShard(new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(commandPosition);
            owned.activateForCommands(authority, 101);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final SourceApplyWorkClassExecutor executor =
                    new SourceApplyWorkClassExecutor(workClasses, owned, authority, keyPair.getPublic());

            workClasses.submit(new WorkClassTask(WorkClass.SOURCE_APPLY, "occupied", 1), () -> {});
            assertThrows(IllegalStateException.class, () -> executor.submit(commandEntry, () -> 101));
            assertEquals(1, workClasses.registeredActions());
            assertNull(owned.shard().getMessage(command.delayMessageId()));
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final SourceApplyWorkClassExecutor.Submission commandSubmission = executor.submit(commandEntry, () -> 101);
            assertEquals(
                    commandPosition.canonicalBytes().length + CommandCodec.encodeFrame(command).length,
                    commandSubmission.task().bytes());
            assertTrue(commandSubmission.outcome().isEmpty());
            assertEquals(
                    List.of(commandSubmission.task()), workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            final SourceApplyWorkClassExecutor.ApplyOutcome commandOutcome =
                    commandSubmission.outcome().orElseThrow();
            assertTrue(commandOutcome.failure() == null);
            assertTrue(commandOutcome.result().isCommand());
            assertEquals(
                    StableCode.SCHEDULED,
                    commandOutcome.result().commandResult().stableCode());
            assertArrayEquals(
                    commandPosition.canonicalBytes(),
                    commandOutcome.result().commandResult().appliedSourcePosition());

            final SourceApplyWorkClassExecutor.Submission mutationSubmission =
                    executor.submit(mutationEntry, () -> 101);
            assertEquals(
                    mutationPosition.canonicalBytes().length + mutation.encodeFrame().length,
                    mutationSubmission.task().bytes());
            assertEquals(
                    List.of(mutationSubmission.task()), workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            final SourceApplyWorkClassExecutor.ApplyOutcome mutationOutcome =
                    mutationSubmission.outcome().orElseThrow();
            assertTrue(mutationOutcome.failure() == null);
            assertFalse(mutationOutcome.result().isCommand());
            assertEquals(
                    StableCode.OK,
                    mutationOutcome.result().systemMutationResult().stableCode());
            assertArrayEquals(
                    mutationPosition.canonicalBytes(),
                    mutationOutcome.result().systemMutationResult().appliedSourcePosition());

            final SourceApplyWorkClassExecutor.Submission failed = executor.submit(failedEntry, () -> 200);
            assertEquals(List.of(failed.task()), workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            final SourceApplyWorkClassExecutor.ApplyOutcome failedOutcome =
                    failed.outcome().orElseThrow();
            assertTrue(failedOutcome.result() == null);
            assertEquals(
                    "shard owner lease is not active", failedOutcome.failure().getMessage());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertNull(owned.shard().getMessage(expiredCommand.delayMessageId()));
            assertTrue(workClasses.state(failed.task()).isEmpty());
            assertEquals(0, workClasses.registeredActions());
        }
    }

    private static PreparedCommand schedule(final ShardId shard, final String identity) {
        return PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8(identity + "-lane")),
                        2_000,
                        5_000,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8(identity)),
                10_000);
    }

    private static SystemMutation timeFence(final ShardId shard, final KeyPair keyPair) {
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(
                2_000,
                2_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("source-work-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("source-work-proof")),
                0,
                null);
        final byte[] proofId = Bytes.sha256(
                Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                shard.routeIncarnation().bytes(),
                Bytes.u32be(shard.partition()),
                Bytes.i64be(2_000),
                Bytes.u32be(1),
                Bytes.lp32(proof.canonicalBytes()));
        return SystemMutation.signed(
                shard,
                SystemMutationType.TIME_FENCE,
                9_000,
                proofId,
                timeFenceBody(shard, proofId, proof.canonicalBytes()),
                AuthorIdentity.fence(Bytes.utf8("source-work-fence"), 1).canonicalBytes(),
                1,
                keyPair.getPrivate());
    }

    private static byte[] timeFenceBody(final ShardId shard, final byte[] proofId, final byte[] proof) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, 2_000);
            CanonicalProtobuf.uint32(output, 11, 1);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
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
                            maxQueueRecords,
                            1_000_000,
                            maxQueueRecords,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), new AtomicLong()::get);
    }
}
