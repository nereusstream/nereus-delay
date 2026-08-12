package io.nereusstream.delay.scheduler;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkClassEventLoopTest {
    @Test
    void resourceRejectionRestoresQueueAndReleasesEarlierTurnLeases() {
        final AtomicLong now = new AtomicLong();
        final EnumMap<WorkClass, WorkClassPolicy> policies = policies();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies, 1, 64, 100, now::get);
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies, 100, now::get), resources);
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-1", 8));
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-2", 8));

        assertThrows(IllegalStateException.class,
                () -> loop.poll(new SchedulerBudget(2, 32, 1_000)));
        assertEquals(2, loop.pending(WorkClass.QUERY));
        assertEquals(0, resources.snapshot().activeLeases());

        final WorkClassEventLoop.Turn turn = loop.poll(new SchedulerBudget(1, 32, 1_000));
        assertEquals(List.of(new WorkClassTask(WorkClass.QUERY, "query-1", 8)), turn.tasks());
        turn.close();
        assertEquals(1, loop.pending(WorkClass.QUERY));
        assertEquals(0, resources.snapshot().activeLeases());
    }

    @Test
    void aTurnMustCloseBeforeTheNextBoundedPollAndCloseIsIdempotent() {
        final AtomicLong now = new AtomicLong();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 1, 64, 100, now::get);
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, now::get), resources);
        loop.offer(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-1", 8));

        final WorkClassEventLoop.Turn turn = loop.poll(new SchedulerBudget(1, 32, 1_000));
        assertFalse(turn.isClosed());
        assertEquals(1, resources.snapshot().activeLeases());
        assertThrows(IllegalStateException.class,
                () -> loop.poll(new SchedulerBudget(1, 32, 1_000)));

        turn.close();
        turn.close();
        assertTrue(turn.isClosed());
        assertEquals(0, resources.snapshot().activeLeases());
        assertTrue(loop.poll(new SchedulerBudget(1, 32, 1_000)).isEmpty());
    }

    @Test
    void borrowedHoldViolationStillReleasesEveryLease() {
        final AtomicLong now = new AtomicLong();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 2, 64, 10, now::get);
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, now::get), resources);
        loop.offer(new WorkClassTask(WorkClass.GC, "gc-1", 8));
        final WorkClassEventLoop.Turn turn = loop.poll(new SchedulerBudget(1, 32, 1_000));

        now.set(11);
        assertThrows(IllegalStateException.class, turn::requireWithinBorrowedHold);
        assertThrows(IllegalStateException.class, turn::close);
        assertTrue(turn.isClosed());
        assertEquals(0, resources.snapshot().activeLeases());
    }

    @Test
    void fatalHoldCheckStillReleasesEveryLeaseAndClosesTheTurn() {
        final AtomicLong schedulerNow = new AtomicLong();
        final AtomicReference<Error> resourceClockFailure = new AtomicReference<>();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 2, 64, 10, () -> {
            final Error failure = resourceClockFailure.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
            return 0;
        });
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, schedulerNow::get), resources);
        loop.offer(new WorkClassTask(WorkClass.GC, "gc-fatal-clock", 8));
        final WorkClassEventLoop.Turn turn = loop.poll(new SchedulerBudget(1, 32, 1_000));
        final AssertionError fatalClockFailure = new AssertionError("resource clock failed");

        resourceClockFailure.set(fatalClockFailure);
        assertEquals(fatalClockFailure, assertThrows(AssertionError.class, turn::close));
        assertTrue(turn.isClosed());
        assertEquals(0, resources.snapshot().activeLeases());
        assertTrue(loop.poll(new SchedulerBudget(1, 32, 1_000)).isEmpty());
    }

    @Test
    void runTurnReleasesResourcesBeforeRethrowingExecutorFailure() {
        final AtomicLong now = new AtomicLong();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 1, 64, 100, now::get);
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, now::get), resources);
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-1", 8));
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-2", 8));
        final AtomicReference<WorkClassTask> executed = new AtomicReference<>();
        final IllegalStateException failure = new IllegalStateException("executor failed");

        assertThrows(IllegalStateException.class, () -> loop.runTurn(
                new SchedulerBudget(1, 32, 1_000), task -> {
                    executed.set(task);
                    throw failure;
                }));

        assertEquals(new WorkClassTask(WorkClass.QUERY, "query-1", 8), executed.get());
        assertEquals(0, resources.snapshot().activeLeases());
        assertEquals(1, loop.pending(WorkClass.QUERY));

        loop.runTurn(new SchedulerBudget(1, 32, 1_000), task ->
                assertEquals(new WorkClassTask(WorkClass.QUERY, "query-2", 8), task));
        assertEquals(0, resources.snapshot().activeLeases());
        assertEquals(0, loop.pending(WorkClass.QUERY));
    }

    private static EnumMap<WorkClass, WorkClassPolicy> policies() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            policies.put(workClass, new WorkClassPolicy(1, 8, 64, 8, 32, 1_000,
                    0, 0, workClass == WorkClass.LEASE_FENCE));
        }
        return policies;
    }
}
