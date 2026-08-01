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
