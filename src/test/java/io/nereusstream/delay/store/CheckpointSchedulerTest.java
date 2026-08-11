package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
        final CheckpointScheduler.ScheduledCheckpoint firstClaim = first.claimDue(firstDue, 1).get(0);
        assertEquals(new CheckpointScheduler.ScheduledCheckpoint(shard, firstDue), firstClaim);
        assertTrue(first.isInFlight(shard));
        assertTrue(first.claimDue(firstDue + 1, 1).isEmpty());
        final long nextDue = first.complete(firstClaim, 12_000);
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
        final CheckpointScheduler.ScheduledCheckpoint claim = scheduler.claimDue(100, 10).get(0);
        assertThrows(IllegalStateException.class, () -> scheduler.unregister(first));
        assertNotEquals(0, scheduler.complete(claim, 100));
        scheduler.unregister(first);
        assertEquals(0, scheduler.size());
    }

    @Test
    void largeIntervalJitterUsesCheckedWideArithmetic() {
        assertDoesNotThrow(() -> new CheckpointScheduler(Long.MAX_VALUE, 50, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new CheckpointScheduler(Long.MAX_VALUE, 99, 1));
    }

    @Test
    void completionAtEpochBoundarySaturatesWithoutStrandingClaim() {
        final CheckpointScheduler scheduler = new CheckpointScheduler(Long.MAX_VALUE, 0, 1);
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(13, 14)), 0);
        scheduler.register(shard, 0);

        final CheckpointScheduler.ScheduledCheckpoint claim = scheduler.claimDue(Long.MAX_VALUE, 1).get(0);
        assertEquals(Long.MAX_VALUE, scheduler.complete(claim, Long.MAX_VALUE));
        assertFalse(scheduler.isInFlight(shard));
    }

    @Test
    void maximumIntervalWithJitterStillRegistersEveryShard() {
        final RouteIncarnation route = RouteIncarnation.fromUuid(new java.util.UUID(15, 16));
        for (int partition = 0; partition < 256; partition++) {
            final CheckpointScheduler scheduler = new CheckpointScheduler(Long.MAX_VALUE, 50, 1);
            final ShardId shard = new ShardId(route, partition);
            assertDoesNotThrow(() -> scheduler.register(shard, 0));
        }
    }

    @Test
    void lateCompletionFromAnEarlierClaimCannotRescheduleANewerClaim() {
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(7, 8)), 0);
        scheduler.register(shard, 0);

        final CheckpointScheduler.ScheduledCheckpoint firstClaim = scheduler.claimDue(100, 1).get(0);
        scheduler.complete(firstClaim, 100);
        final CheckpointScheduler.ScheduledCheckpoint secondClaim = scheduler.claimDue(200, 1).get(0);

        assertThrows(IllegalStateException.class, () -> scheduler.complete(firstClaim, 201));
        assertTrue(scheduler.isInFlight(shard));
        assertNotEquals(0, scheduler.complete(secondClaim, 201));
    }

    @Test
    void valueEqualReconstructedClaimCannotCompleteTheInFlightAttempt() {
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(11, 12)), 0);
        scheduler.register(shard, 0);

        final CheckpointScheduler.ScheduledCheckpoint claim = scheduler.claimDue(100, 1).get(0);
        final CheckpointScheduler.ScheduledCheckpoint reconstructed =
                new CheckpointScheduler.ScheduledCheckpoint(shard, claim.dueAtEpochMs());

        assertEquals(claim, reconstructed);
        assertThrows(IllegalStateException.class, () -> scheduler.complete(reconstructed, 100));
        assertTrue(scheduler.isInFlight(shard));
        assertNotEquals(0, scheduler.complete(claim, 100));
    }

    @Test
    @SuppressWarnings("deprecation")
    void shardOnlyCompletionFailsClosedBecauseItCannotCarryClaimIdentity() {
        final CheckpointScheduler scheduler = new CheckpointScheduler(100, 0, 1);
        final ShardId shard = new ShardId(RouteIncarnation.fromUuid(new java.util.UUID(9, 10)), 0);
        scheduler.register(shard, 0);
        scheduler.claimDue(100, 1);

        assertThrows(IllegalStateException.class, () -> scheduler.complete(shard, 100));
        assertTrue(scheduler.isInFlight(shard));
    }
}
