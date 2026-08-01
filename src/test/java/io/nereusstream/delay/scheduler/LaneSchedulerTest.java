package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LaneSchedulerTest {
    @TempDir
    Path tempDir;

    @Test
    void blockedLaneDoesNotPauseHealthyLane() {
        final DestinationLaneId bad = lane(1);
        final DestinationLaneId healthy = lane(2);
        final LaneScheduler scheduler = LaneScheduler.defaults();
        scheduler.register(record(bad, 1));
        scheduler.register(record(healthy, 1));
        scheduler.offer(item(bad, 1));
        scheduler.offer(item(healthy, 2));
        scheduler.markBlocked(bad);

        final List<ScheduleWorkItem> result = scheduler.poll(new SchedulerBudget(8, 1024 * 1024, 1_000_000_000));

        assertEquals(List.of(healthy), result.stream().map(ScheduleWorkItem::laneId).toList());
        assertEquals(1, scheduler.pendingItems(bad));
        assertEquals(0, scheduler.pendingItems(healthy));
    }

    @Test
    void weightedDeficitRoundRobinEventuallyServicesBothLanes() {
        final DestinationLaneId first = lane(3);
        final DestinationLaneId second = lane(4);
        final LaneScheduler scheduler = new LaneScheduler(10, 64);
        scheduler.register(record(first, 2));
        scheduler.register(record(second, 1));
        for (int index = 0; index < 12; index++) {
            scheduler.offer(item(first, index));
            scheduler.offer(item(second, index));
        }

        final List<ScheduleWorkItem> firstVisit = scheduler.poll(new SchedulerBudget(64, 100, 1_000_000_000));
        final List<ScheduleWorkItem> secondVisit = scheduler.poll(new SchedulerBudget(64, 100, 1_000_000_000));

        // Both visits make progress and the lower-weight lane is not starved.
        org.junit.jupiter.api.Assertions.assertFalse(firstVisit.isEmpty());
        org.junit.jupiter.api.Assertions.assertFalse(secondVisit.isEmpty());
        org.junit.jupiter.api.Assertions.assertTrue(firstVisit.stream().anyMatch(item -> item.laneId().equals(second))
                || secondVisit.stream().anyMatch(item -> item.laneId().equals(second)));
    }

    @Test
    void fairnessCountersSurviveOwnerRestart() {
        final DestinationLaneId lane = lane(5);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 5);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        LaneScheduler.SchedulerSnapshot saved;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(lane, 2));
            scheduler.offer(item(lane, 1));
            scheduler.poll(new SchedulerBudget(1, 1024, 1_000_000_000));
            saved = scheduler.snapshot();
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            scheduler.register(record(lane, 2));
            scheduler.restorePersistedState();
            final LaneScheduler.LaneSnapshot restored = scheduler.snapshot().lanes().get(0);
            final LaneScheduler.LaneSnapshot expected = saved.lanes().get(0);
            assertEquals(expected.deficit(), restored.deficit());
            assertEquals(expected.lastServedRound(), restored.lastServedRound());
            assertEquals(expected.weight(), restored.weight());
        }
    }

    private static LaneRecord record(final DestinationLaneId lane, final int weight) {
        return new LaneRecord(lane, new byte[16], 1, 0, AdmissionGate.OPEN, RuntimeReadiness.READY, weight, 0);
    }

    private static ScheduleWorkItem item(final DestinationLaneId lane, final int generation) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), generation);
        return new ScheduleWorkItem(lane, DelayMessageId.random(shard), generation, generation, 1);
    }

    private static DestinationLaneId lane(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return new DestinationLaneId(bytes);
    }
}
