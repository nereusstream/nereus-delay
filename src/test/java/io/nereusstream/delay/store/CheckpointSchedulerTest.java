package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointSchedulerTest {
    @Test
    void deterministicJitterStaysWithinIntervalAndReschedulesAfterCompletion() {
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(1, 2)), 3);
        final CheckpointScheduler first = new CheckpointScheduler(1_000, 20, 2);
        final CheckpointScheduler second = new CheckpointScheduler(1_000, 20, 2);
        final long firstDue = first.register(shard, 10_000);
        assertEquals(firstDue, second.register(shard, 10_000));
        assertTrue(firstDue >= 10_800 && firstDue <= 11_200);
        assertTrue(first.claimDue(firstDue - 1, 1).isEmpty());
        assertEquals(List.of(new CheckpointScheduler.ScheduledCheckpoint(shard, firstDue)),
                first.claimDue(firstDue, 1));
        assertTrue(first.isInFlight(shard));
        assertTrue(first.claimDue(firstDue + 1, 1).isEmpty());
        final long nextDue = first.complete(shard, 12_000);
        assertTrue(nextDue >= 12_800 && nextDue <= 13_200);
        assertFalse(first.isInFlight(shard));
    }

    @Test
    void claimsAreBoundedStableAndRegistrationCannotExceedLimit() {
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final ShardId first = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(3, 4)), 0);
        final ShardId second = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(5, 6)), 0);
        scheduler.register(first, 0);
        assertThrows(IllegalStateException.class, () -> scheduler.register(first, 0));
        assertThrows(IllegalStateException.class, () -> scheduler.register(second, 0));
        assertEquals(1, scheduler.claimDue(100, 10).size());
        assertThrows(IllegalStateException.class, () -> scheduler.unregister(first));
        assertNotEquals(0, scheduler.complete(first, 100));
        scheduler.unregister(first);
        assertEquals(0, scheduler.size());
    }
}
