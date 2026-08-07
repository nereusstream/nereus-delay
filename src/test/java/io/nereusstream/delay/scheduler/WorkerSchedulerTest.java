package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerSchedulerTest {
    @Test
    void blockedShardDoesNotPauseAnotherShard() {
        final ShardId blockedShard = shard(1);
        final ShardId healthyShard = shard(2);
        final DestinationLaneId blockedLane = lane(1);
        final DestinationLaneId healthyLane = lane(2);
        final WorkerScheduler worker = WorkerScheduler.defaults();
        final LaneScheduler blockedScheduler = LaneScheduler.defaults();
        final LaneScheduler healthyScheduler = LaneScheduler.defaults();
        worker.registerShard(blockedShard, 1, blockedScheduler);
        worker.registerShard(healthyShard, 1, healthyScheduler);
        worker.registerLane(blockedShard, laneRecord(blockedLane));
        worker.registerLane(healthyShard, laneRecord(healthyLane));
        worker.offer(item(blockedShard, blockedLane, 1));
        worker.offer(item(healthyShard, healthyLane, 2));
        worker.markShardBlocked(blockedShard);

        final List<ScheduleWorkItem> result = worker.poll(new SchedulerBudget(8, 1024, 1_000_000_000));

        assertEquals(List.of(healthyLane), result.stream().map(ScheduleWorkItem::laneId).toList());
    }

    @Test
    void emptyShardDoesNotConsumeOuterDeficitVisit() {
        final ShardId emptyShard = shard(5);
        final ShardId healthyShard = shard(6);
        final DestinationLaneId emptyLane = lane(5);
        final DestinationLaneId healthyLane = lane(6);
        final WorkerScheduler worker = new WorkerScheduler(10, 64);
        worker.registerShard(emptyShard, 1, LaneScheduler.defaults());
        worker.registerShard(healthyShard, 1, LaneScheduler.defaults());
        worker.registerLane(emptyShard, laneRecord(emptyLane));
        worker.registerLane(healthyShard, laneRecord(healthyLane));
        worker.offer(item(healthyShard, healthyLane, 1));

        final List<ScheduleWorkItem> result = worker.poll(new SchedulerBudget(1, 100, 1_000_000_000));

        assertEquals(List.of(healthyLane), result.stream().map(ScheduleWorkItem::laneId).toList());
    }

    @Test
    void recoveryFirstPassServesEveryEligibleShardBeforeRepeatingOne() {
        final ShardId first = shard(7);
        final ShardId second = shard(8);
        final DestinationLaneId firstLane = lane(7);
        final DestinationLaneId secondLane = lane(8);
        final WorkerScheduler worker = new WorkerScheduler(10, 64);
        worker.registerShard(first, 1, LaneScheduler.defaults());
        worker.registerShard(second, 1, LaneScheduler.defaults());
        worker.registerLane(first, laneRecord(firstLane));
        worker.registerLane(second, laneRecord(secondLane));
        worker.offer(item(first, firstLane, 1));
        worker.offer(item(first, firstLane, 2));
        worker.offer(item(second, secondLane, 1));
        worker.offer(item(second, secondLane, 2));

        final List<ScheduleWorkItem> result = worker.poll(new SchedulerBudget(3, 100, 1_000_000_000));

        assertEquals(3, result.size());
        assertTrue(!result.get(0).messageId().routingId().shardId()
                .equals(result.get(1).messageId().routingId().shardId()));
    }

    @Test
    void restoreStartsANewOuterFirstPass() {
        final ShardId first = shard(9);
        final ShardId second = shard(10);
        final DestinationLaneId firstLane = lane(9);
        final DestinationLaneId secondLane = lane(10);
        final WorkerScheduler original = new WorkerScheduler(10, 64);
        original.registerShard(first, 1, LaneScheduler.defaults());
        original.registerShard(second, 1, LaneScheduler.defaults());
        original.registerLane(first, laneRecord(firstLane));
        original.registerLane(second, laneRecord(secondLane));
        original.offer(item(first, firstLane, 1));
        original.offer(item(second, secondLane, 1));
        original.poll(new SchedulerBudget(1, 100, 1_000_000_000));
        final WorkerScheduler.WorkerSnapshot saved = original.snapshot();

        final WorkerScheduler restored = new WorkerScheduler(10, 64);
        restored.registerShard(first, 1, LaneScheduler.defaults());
        restored.registerShard(second, 1, LaneScheduler.defaults());
        restored.registerLane(first, laneRecord(firstLane));
        restored.registerLane(second, laneRecord(secondLane));
        restored.offer(item(first, firstLane, 2));
        restored.offer(item(second, secondLane, 2));
        restored.restore(saved);

        final List<ScheduleWorkItem> result = restored.poll(new SchedulerBudget(2, 100, 1_000_000_000));

        assertEquals(2, result.size());
        assertTrue(!result.get(0).messageId().routingId().shardId()
                .equals(result.get(1).messageId().routingId().shardId()));
    }

    @Test
    void rejectsQuantumAndWeightArithmeticOverflow() {
        assertThrows(IllegalArgumentException.class,
                () -> new WorkerScheduler(Long.MAX_VALUE, 1));

        final WorkerScheduler worker = new WorkerScheduler(Long.MAX_VALUE / 4, 1);
        assertThrows(IllegalArgumentException.class,
                () -> worker.registerShard(shard(20), Integer.MAX_VALUE, LaneScheduler.defaults()));
    }

    @Test
    void outerVisitLimitUsesWideArithmetic() {
        assertEquals(0, WorkerScheduler.boundedVisitLimit(64, 0));
        assertEquals(64, WorkerScheduler.boundedVisitLimit(64, Integer.MAX_VALUE));
        assertEquals((long) Integer.MAX_VALUE,
                WorkerScheduler.boundedVisitLimit(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    @Test
    void saturatesRestoredDeficitBeforeServing() {
        final ShardId shard = shard(21);
        final DestinationLaneId lane = lane(21);
        final WorkerScheduler worker = new WorkerScheduler(10, 1);
        worker.registerShard(shard, 1, LaneScheduler.defaults());
        worker.registerLane(shard, laneRecord(lane));
        worker.offer(item(shard, lane, 1));
        worker.restore(new WorkerScheduler.WorkerSnapshot(0, 0,
                List.of(new WorkerScheduler.ShardSnapshot(shard, 1, Long.MAX_VALUE, 0, false))));

        assertEquals(List.of(lane), worker.poll(new SchedulerBudget(1, 100, 1_000_000_000)).stream()
                .map(ScheduleWorkItem::laneId).toList());
        assertEquals(39, worker.snapshot().shards().get(0).deficit());
    }

    @Test
    void saturatesRoundGenerationBeforeServingAtLongMaximum() {
        final ShardId shard = shard(22);
        final DestinationLaneId lane = lane(22);
        final WorkerScheduler worker = new WorkerScheduler(10, 1);
        worker.registerShard(shard, 1, LaneScheduler.defaults());
        worker.registerLane(shard, laneRecord(lane));
        worker.offer(item(shard, lane, 1));
        worker.restore(new WorkerScheduler.WorkerSnapshot(0, Long.MAX_VALUE,
                List.of(new WorkerScheduler.ShardSnapshot(shard, 1, 10, Long.MAX_VALUE, false))));

        assertEquals(List.of(lane), worker.poll(new SchedulerBudget(1, 100, 1_000_000_000)).stream()
                .map(ScheduleWorkItem::laneId).toList());
        assertEquals(Long.MAX_VALUE, worker.snapshot().roundGeneration());
        assertEquals(Long.MAX_VALUE, worker.snapshot().shards().get(0).lastServedRound());
    }

    @Test
    void outerFairnessCountersCanBeRestored() {
        final ShardId first = shard(3);
        final ShardId second = shard(4);
        final DestinationLaneId firstLane = lane(3);
        final DestinationLaneId secondLane = lane(4);
        final WorkerScheduler worker = new WorkerScheduler(10, 64);
        worker.registerShard(first, 2, LaneScheduler.defaults());
        worker.registerShard(second, 1, LaneScheduler.defaults());
        worker.registerLane(first, laneRecord(firstLane));
        worker.registerLane(second, laneRecord(secondLane));
        worker.markShardBlocked(first);
        for (int index = 0; index < 4; index++) {
            worker.offer(item(first, firstLane, index));
            worker.offer(item(second, secondLane, index));
        }
        worker.poll(new SchedulerBudget(1, 100, 1_000_000_000));
        final WorkerScheduler.WorkerSnapshot saved = worker.snapshot();

        final WorkerScheduler restored = new WorkerScheduler(10, 64);
        restored.registerShard(first, 2, LaneScheduler.defaults());
        restored.registerShard(second, 1, LaneScheduler.defaults());
        restored.registerLane(first, laneRecord(firstLane));
        restored.registerLane(second, laneRecord(secondLane));
        restored.restore(saved);

        assertEquals(saved.roundGeneration(), restored.snapshot().roundGeneration());
        assertFalse(restored.snapshot().shards().isEmpty());
        assertTrue(restored.snapshot().shards().stream()
                .filter(snapshot -> snapshot.shardId().equals(first))
                .findFirst().orElseThrow().blocked());
    }

    private static LaneRecord laneRecord(final DestinationLaneId lane) {
        return new LaneRecord(lane, new byte[16], 1, 0, AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
    }

    private static ScheduleWorkItem item(final ShardId shard, final DestinationLaneId lane, final int generation) {
        return new ScheduleWorkItem(lane, DelayMessageId.random(shard), generation, generation, 1);
    }

    private static ShardId shard(final int partition) {
        return new ShardId(RouteIncarnation.random(), partition);
    }

    private static DestinationLaneId lane(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return new DestinationLaneId(bytes);
    }
}
