package io.nereusstream.delay.scheduler;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    void concurrentCloseAndPollDoNotInvertEventLoopAndTurnLocks() throws Exception {
        final AtomicLong schedulerNow = new AtomicLong();
        final AtomicBoolean blockResourceClock = new AtomicBoolean();
        final CountDownLatch holdCheckEntered = new CountDownLatch(1);
        final CountDownLatch allowHoldCheck = new CountDownLatch(1);
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 1, 64, 100, () -> {
            if (blockResourceClock.get()) {
                holdCheckEntered.countDown();
                try {
                    if (!allowHoldCheck.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release the Turn hold check");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Turn hold check was interrupted", interrupted);
                }
            }
            return 0;
        });
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, schedulerNow::get), resources);
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-lock-order", 8));
        final WorkClassEventLoop.Turn turn = loop.poll(new SchedulerBudget(1, 32, 1_000));
        blockResourceClock.set(true);

        final CompletableFuture<Void> closeCompleted = new CompletableFuture<>();
        Thread.ofPlatform().daemon().start(() -> complete(closeCompleted, turn::close));
        assertTrue(holdCheckEntered.await(5, TimeUnit.SECONDS));

        final CompletableFuture<Void> pollCompleted = new CompletableFuture<>();
        Thread.ofPlatform().daemon().start(() -> complete(pollCompleted, () -> {
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> loop.poll(new SchedulerBudget(1, 32, 1_000)));
            assertEquals("previous WorkClass turn must be closed before polling again", failure.getMessage());
        }));
        try {
            // poll must observe the still-open Turn without waiting for its
            // monitor; close is deliberately blocked while holding that lock.
            pollCompleted.get(2, TimeUnit.SECONDS);
        } finally {
            blockResourceClock.set(false);
            allowHoldCheck.countDown();
        }
        closeCompleted.get(5, TimeUnit.SECONDS);
        assertTrue(turn.isClosed());
        assertEquals(0, resources.snapshot().activeLeases());
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

    @Test
    void holdFailureBeforeTheFirstHandlerRequeuesTheWholeUnstartedTurn() {
        final AtomicLong schedulerNow = new AtomicLong();
        final AtomicInteger resourceClockReads = new AtomicInteger();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 2, 64, 10,
                () -> resourceClockReads.incrementAndGet() >= 4 ? 11 : 0);
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, schedulerNow::get), resources);
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-hold-1", 8));
        loop.offer(new WorkClassTask(WorkClass.QUERY, "query-hold-2", 8));
        final AtomicBoolean handlerCalled = new AtomicBoolean();

        assertThrows(IllegalStateException.class, () -> loop.runTurn(
                new SchedulerBudget(2, 16, 1_000), ignored -> handlerCalled.set(true)));

        assertFalse(handlerCalled.get());
        assertEquals(2, loop.pending(WorkClass.QUERY));
        assertEquals(16, loop.pendingBytes(WorkClass.QUERY));
        assertEquals(0, resources.snapshot().activeLeases());
    }

    @Test
    void activeTurnReservesEnoughQueueCapacityForUnstartedTasks() {
        final AtomicLong now = new AtomicLong();
        final WorkClassResourcePool resources = new WorkClassResourcePool(policies(), 2, 64, 100, now::get);
        final WorkClassEventLoop loop = new WorkClassEventLoop(
                new WorkClassScheduler(policies(), 100, now::get), resources);
        loop.offer(new WorkClassTask(WorkClass.QUERY, "selected-1", 8));
        loop.offer(new WorkClassTask(WorkClass.QUERY, "selected-2", 8));
        final WorkClassEventLoop.Turn turn = loop.poll(new SchedulerBudget(2, 16, 1_000));

        for (int index = 0; index < 6; index++) {
            loop.offer(new WorkClassTask(WorkClass.QUERY, "queued-" + index, 8));
        }
        assertThrows(IllegalStateException.class,
                () -> loop.offer(new WorkClassTask(WorkClass.QUERY, "capacity-stolen", 8)));

        turn.close();
        loop.offer(new WorkClassTask(WorkClass.QUERY, "after-close-1", 8));
        loop.offer(new WorkClassTask(WorkClass.QUERY, "after-close-2", 8));
        assertEquals(8, loop.pending(WorkClass.QUERY));
    }

    private static EnumMap<WorkClass, WorkClassPolicy> policies() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            policies.put(workClass, new WorkClassPolicy(1, 8, 64, 8, 32, 1_000,
                    0, 0, workClass == WorkClass.LEASE_FENCE));
        }
        return policies;
    }

    private static void complete(final CompletableFuture<Void> completion, final Runnable action) {
        try {
            action.run();
            completion.complete(null);
        } catch (Throwable failure) {
            completion.completeExceptionally(failure);
        }
    }
}
