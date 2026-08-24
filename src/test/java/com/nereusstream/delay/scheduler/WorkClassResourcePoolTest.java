package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkClassResourcePoolTest {
    @Test
    void nonBorrowableMinimumsProtectOtherClasses() {
        final WorkClassResourcePool pool = pool(new AtomicLong(0), 2, 8, 100);
        final WorkClassResourcePool.ResourceLease source = pool.acquire(WorkClass.SOURCE_APPLY, 1, 1);
        final WorkClassResourcePool.ResourceLease query = pool.acquire(WorkClass.QUERY, 1, 1);
        assertFalse(source.borrowed());
        assertFalse(query.borrowed());
        assertThrows(IllegalStateException.class, () -> pool.acquire(WorkClass.SOURCE_APPLY, 1, 1));
        query.close();
        source.close();
        assertTrue(pool.snapshot().activeLeases() == 0);
    }

    @Test
    void borrowedLeaseHasBoundedHoldAndExactIdempotentRelease() {
        final AtomicLong now = new AtomicLong(0);
        final WorkClassResourcePool pool = pool(now, 4, 16, 10);
        final WorkClassResourcePool.ResourceLease source = pool.acquire(WorkClass.SOURCE_APPLY, 2, 8);
        assertTrue(source.borrowed());
        now.set(10);
        pool.requireWithinBorrowedHold(source);
        now.set(11);
        assertThrows(IllegalStateException.class, () -> pool.requireWithinBorrowedHold(source));
        source.close();
        source.close();
        assertTrue(pool.snapshot().activeLeases() == 0);
    }

    @Test
    void invalidPoolMinimaAndClockFailClosed() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = policies(1, 1);
        assertThrows(IllegalArgumentException.class, () -> new WorkClassResourcePool(policies, 1, 1, 1, () -> 0));
        final AtomicLong now = new AtomicLong(1);
        final WorkClassResourcePool pool = pool(now, 4, 16, 10);
        now.set(0);
        assertThrows(IllegalStateException.class, () -> pool.acquire(WorkClass.QUERY, 1, 1));
    }

    @Test
    void acquisitionArithmeticOverflowFailsClosed() {
        final AtomicLong now = new AtomicLong(0);
        final WorkClassResourcePool pool =
                new WorkClassResourcePool(policies(0, 0), Long.MAX_VALUE, Long.MAX_VALUE, 10, now::get);
        pool.acquire(WorkClass.GC, Long.MAX_VALUE, 0);

        assertThrows(IllegalStateException.class, () -> pool.acquire(WorkClass.GC, 1, 0));
    }

    private static WorkClassResourcePool pool(
            final AtomicLong now, final long totalRecords, final long totalBytes, final long maxHold) {
        return new WorkClassResourcePool(policies(1, 1), totalRecords, totalBytes, maxHold, now::get);
    }

    private static EnumMap<WorkClass, WorkClassPolicy> policies(final long minimumRecords, final long minimumBytes) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = workClass == WorkClass.SOURCE_APPLY || workClass == WorkClass.QUERY;
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            8,
                            64,
                            8,
                            16,
                            1_000,
                            protectedClass ? minimumRecords : 0,
                            protectedClass ? minimumBytes : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return policies;
    }
}
