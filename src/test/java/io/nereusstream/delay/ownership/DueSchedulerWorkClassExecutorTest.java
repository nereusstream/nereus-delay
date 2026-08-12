package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.ReadyIndexValue;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.runtime.TimelineEntry;
import io.nereusstream.delay.scheduler.PersistentLaneScheduler;
import io.nereusstream.delay.scheduler.ScheduleWorkItem;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DueSchedulerWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void readyDiscoveryUsesBoundedQueueAndExecutionTimeOwnerFence() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 27);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("due-work-assignment")), 8,
                new KafkaActivationBarrier(shard, "due-work-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "due-work-owner",
                Bytes.sha256(Bytes.utf8("due-work-session")), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "due-work-cluster", topic,
                0, null, 1_000);
        final DestinationLaneId laneId = DestinationLaneId.derive(Bytes.utf8("due-work-lane"));
        final LaneRecord lane = new LaneRecord(laneId, incarnation(), 1, 1,
                io.nereusstream.delay.runtime.AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 2_000);
        final io.nereusstream.delay.protocol.DelayMessageId messageId =
                io.nereusstream.delay.protocol.DelayMessageId.random(shard);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                2_000, 9_000, laneId, OrderingMode.BEST_EFFORT, Bytes.utf8("due-work-payload"),
                source.canonicalBytes());
        final byte[] timelineKey = KeyCodec.timelineDue(laneId, 2_000, source.sourceOrderToken(),
                messageId, message.generation());
        final byte[] readyKey = KeyCodec.timelineReady(2_000, laneId, lane.laneVersion());
        final ReadyIndexValue ready = new ReadyIndexValue(laneId, 2_000, lane.laneVersion(),
                messageId, message.generation(), Bytes.sha256(timelineKey));
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("due-work-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("due-work-time-proof")), 0, null);
        final SchedulerBudget budget = new SchedulerBudget(1, 4_096, 1_000_000_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("due-work-store"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shard, resources)) {
            final OwnedDelayShard owned = new OwnedDelayShard(
                    new DelayShard(store, DelayShardConfig.defaults()), lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(source);
            owned.activateForCommands(authority, 101);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(laneId),
                        LaneRecordEnvelopeV1.active(lane.encode()).canonicalBytes());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), message.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey,
                        new TimelineEntry(messageId, message.generation()).encode());
                batch.putValue(ColumnFamily.TIMELINE, 3, readyKey, ready.encode());
            });
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(lane);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final DueSchedulerWorkClassExecutor executor = new DueSchedulerWorkClassExecutor(
                    workClasses, owned, authority, scheduler);

            workClasses.submit(new WorkClassTask(WorkClass.DUE_SCHEDULER, "occupied", 1), () -> {
            });
            assertThrows(IllegalStateException.class, () -> executor.submit(evidence, budget, () -> 101));
            assertEquals(0, scheduler.snapshot().lanes().get(0).pendingItems());
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final DueSchedulerWorkClassExecutor.Submission submitted =
                    executor.submit(evidence, budget, () -> 101);
            assertEquals(16 + 4 + 4 + 8 + 8 + 4 + evidence.canonicalBytes().length + budget.maxBytes(),
                    submitted.task().bytes());
            assertTrue(submitted.discovered().isEmpty());
            assertEquals(List.of(submitted.task()),
                    workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            assertEquals(List.of(messageId), submitted.discovered().orElseThrow().stream()
                    .map(ScheduleWorkItem::messageId).toList());
            assertEquals(0, workClasses.registeredActions());

            final DueSchedulerWorkClassExecutor.Submission expired =
                    executor.submit(evidence, budget, () -> 200);
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            assertEquals("shard owner lease is not active", failure.getMessage());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(expired.discovered().isEmpty());
            assertEquals(WorkClassExecutionRegistry.ExecutionState.FAILED,
                    workClasses.state(expired.task()).orElseThrow());
            assertEquals(1, workClasses.registeredActions());
        }
    }

    private static byte[] incarnation() {
        final byte[] bytes = new byte[16];
        bytes[15] = 1;
        return bytes;
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
                16, 2_000_000), new AtomicLong()::get);
    }
}
