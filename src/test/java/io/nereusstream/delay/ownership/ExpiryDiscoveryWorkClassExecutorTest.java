package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpiryDiscoveryWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void discoversOnlyInsideBoundedExpiryTurnWithoutChangingMessage() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("success"), 2)) {
            final SchedulerBudget scanBudget = new SchedulerBudget(1, 4_096, 1_000);
            final AtomicInteger ownerClockReads = new AtomicInteger();
            final AtomicLong scanClock = new AtomicLong();
            final ExpiryDiscoveryWorkClassExecutor.Submission submission = fixture.executor().submit(
                    evidence(), scanBudget, () -> {
                        ownerClockReads.incrementAndGet();
                        return 101;
                    }, scanClock::getAndIncrement);

            assertTrue(submission.discovered().isEmpty());
            assertEquals(0, ownerClockReads.get());
            final int domainBytes = Bytes.utf8("nereus-delay-expiry-discovery-task-v1\0").length;
            final int shardBytes = 16 + 4;
            final int evidenceBytes = 4 + evidence().canonicalBytes().length;
            final int budgetBytes = 4 + 8 + 8;
            assertEquals(scanBudget.maxBytes() + domainBytes + shardBytes + evidenceBytes + budgetBytes,
                    submission.task().bytes());

            assertEquals(List.of(submission.task()), fixture.workClasses.runTurn(fixture.turnBudget()));
            assertEquals(1, ownerClockReads.get());
            assertEquals(List.of(fixture.messageId), submission.discovered().orElseThrow().stream()
                    .map(DelayShard.ExpiryWork::messageId).toList());
            assertEquals(MessageStatus.SCHEDULED, fixture.shard.getMessage(fixture.messageId).status());
        }
    }

    @Test
    void queueRejectionReadsNeitherClockNorRocksDb() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("rejected"), 1)) {
            final AtomicInteger ownerClockReads = new AtomicInteger();
            final AtomicInteger scanClockReads = new AtomicInteger();
            fixture.workClasses.submit(new WorkClassTask(WorkClass.EXPIRY, "occupied", 1), () -> {
            });

            assertThrows(IllegalStateException.class, () -> fixture.executor().submit(
                    evidence(), new SchedulerBudget(1, 4_096, 1_000),
                    () -> {
                        ownerClockReads.incrementAndGet();
                        return 101;
                    }, () -> {
                        scanClockReads.incrementAndGet();
                        return 0;
                    }));
            assertEquals(0, ownerClockReads.get());
            assertEquals(0, scanClockReads.get());
            assertEquals(MessageStatus.SCHEDULED, fixture.shard.getMessage(fixture.messageId).status());
        }
    }

    @Test
    void undersizedCandidateEnvelopeFailsClosedAndFencesOwner() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("undersized"), 1)) {
            final ExpiryDiscoveryWorkClassExecutor.Submission submission = fixture.executor().submit(
                    evidence(), new SchedulerBudget(1, 1, 1_000), () -> 101, () -> 0);

            assertThrows(IllegalStateException.class, () -> fixture.workClasses.runTurn(fixture.turnBudget()));
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
            assertTrue(submission.discovered().isEmpty());
            assertEquals(WorkClassExecutionRegistry.ExecutionState.FAILED,
                    fixture.workClasses.state(submission.task()).orElseThrow());
        }
    }

    @Test
    void expiredOwnerFencesBeforeScanClockIsRead() throws Exception {
        try (Fixture fixture = new Fixture(tempDir.resolve("expired-owner"), 1)) {
            final AtomicInteger scanClockReads = new AtomicInteger();
            final ExpiryDiscoveryWorkClassExecutor.Submission submission = fixture.executor().submit(
                    evidence(), new SchedulerBudget(1, 4_096, 1_000), () -> 200, () -> {
                        scanClockReads.incrementAndGet();
                        return 0;
                    });

            assertThrows(IllegalStateException.class, () -> fixture.workClasses.runTurn(fixture.turnBudget()));
            assertEquals(0, scanClockReads.get());
            assertEquals(ShardLifecycleState.FENCED, fixture.owned.state());
            assertTrue(submission.discovered().isEmpty());
        }
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(5_000, 5_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("expiry-discovery-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("expiry-discovery-proof")), 0, null);
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 1_000_000,
                    maxQueueRecords, 1_000_000, 1_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }

    private static final class Fixture implements AutoCloseable {
        private final ShardId shardId = new ShardId(RouteIncarnation.random(), 55);
        private final UUID topic = UUID.randomUUID();
        private final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        private final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        private final SharedRocksDbResources resources;
        private final ShardStore store;
        private final DelayShard shard;
        private final OwnedDelayShard owned;
        private final WorkClassExecutionRegistry workClasses;
        private final io.nereusstream.delay.protocol.DelayMessageId messageId;

        private Fixture(final Path root, final int maxQueueRecords) throws Exception {
            final SourceAssignment assignment = new SourceAssignment(shardId,
                    Bytes.sha256(Bytes.utf8("expiry-discovery-assignment")), 1,
                    new KafkaActivationBarrier(shardId, "expiry-discovery-cluster", topic, 0));
            final OwnerLease lease = backend.acquire(assignment, "expiry-discovery-owner",
                    Bytes.sha256(Bytes.utf8("expiry-discovery-session")), 100, 100).orElseThrow();
            final ShardStoreConfig config = ShardStoreConfig.defaults(root);
            resources = new SharedRocksDbResources(config);
            store = ShardStore.open(config, shardId, resources);
            shard = new DelayShard(store, DelayShardConfig.defaults());
            final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("expiry-discovery-lane"));
            final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                    new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT,
                            Bytes.utf8("expiry-discovery-payload")), 9_000);
            messageId = schedule.delayMessageId();
            shard.apply(schedule, new KafkaSourcePosition(shardId, "expiry-discovery-cluster", topic,
                    0, null, 1_000));
            owned = new OwnedDelayShard(shard, lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.activateForCommands(authority, 101);
            workClasses = workClasses(maxQueueRecords);
        }

        private ExpiryDiscoveryWorkClassExecutor executor() {
            return new ExpiryDiscoveryWorkClassExecutor(workClasses, owned, authority);
        }

        private SchedulerBudget turnBudget() {
            return new SchedulerBudget(1, 1_000_000, 1_000);
        }

        @Override
        public void close() {
            store.close();
            resources.close();
        }
    }
}
