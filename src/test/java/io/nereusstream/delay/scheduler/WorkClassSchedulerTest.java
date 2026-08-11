package io.nereusstream.delay.scheduler;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkClassSchedulerTest {
    @Test
    void queueAndTaskCapsFailClosedBeforeEnqueue() {
        final WorkClassScheduler scheduler = scheduler(100);
        scheduler.offer(new WorkClassTask(WorkClass.QUERY, "q-1", 8));
        assertEquals(1, scheduler.pending(WorkClass.QUERY));
        assertEquals(8, scheduler.pendingBytes(WorkClass.QUERY));
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.offer(new WorkClassTask(WorkClass.QUERY, "too-large", 17)));
        for (int index = 2; index <= 8; index++) {
            scheduler.offer(new WorkClassTask(WorkClass.QUERY, "q-" + index, 8));
        }
        assertThrows(IllegalStateException.class,
                () -> scheduler.offer(new WorkClassTask(WorkClass.QUERY, "q-9", 1)));
    }

    @Test
    void leaseFenceIsPreemptiveAndClassTurnCapBoundsOneTurn() {
        final WorkClassScheduler scheduler = scheduler(100);
        scheduler.offer(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-1", 1));
        scheduler.offer(new WorkClassTask(WorkClass.LEASE_FENCE, "fence-1", 1));
        final List<WorkClassTask> first = scheduler.poll(new SchedulerBudget(1, 10, 1_000));
        assertEquals(List.of(new WorkClassTask(WorkClass.LEASE_FENCE, "fence-1", 1)), first);

        scheduler.offer(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-2", 1));
        scheduler.offer(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-3", 1));
        scheduler.offer(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-4", 1));
        final List<WorkClassTask> sourceTurn = scheduler.poll(new SchedulerBudget(10, 100, 1_000));
        assertEquals(2, sourceTurn.stream().filter(task -> task.workClass() == WorkClass.SOURCE_APPLY).count());
        assertEquals(2, scheduler.pending(WorkClass.SOURCE_APPLY));
    }

    @Test
    void continuousPreemptiveQueueYieldsAcrossSmallPolls() {
        final WorkClassScheduler scheduler = scheduler(100);
        scheduler.offer(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-1", 1));
        scheduler.offer(new WorkClassTask(WorkClass.LEASE_FENCE, "fence-1", 1));
        scheduler.offer(new WorkClassTask(WorkClass.LEASE_FENCE, "fence-2", 1));

        assertEquals(List.of(new WorkClassTask(WorkClass.LEASE_FENCE, "fence-1", 1)),
                scheduler.poll(new SchedulerBudget(1, 10, 1_000)));
        // A caller that takes one task per poll must not let the still queued
        // preemptive class starve source application forever.
        assertEquals(List.of(new WorkClassTask(WorkClass.SOURCE_APPLY, "source-1", 1)),
                scheduler.poll(new SchedulerBudget(1, 10, 1_000)));
        assertEquals(List.of(new WorkClassTask(WorkClass.LEASE_FENCE, "fence-2", 1)),
                scheduler.poll(new SchedulerBudget(1, 10, 1_000)));
    }

    @Test
    void overdueClassIsServedBeforeAHealthyClass() {
        final AtomicLong now = new AtomicLong(0);
        final WorkClassScheduler scheduler = scheduler(now, 10);
        scheduler.offer(new WorkClassTask(WorkClass.QUERY, "query-1", 1));
        now.set(10);
        assertEquals(WorkClass.QUERY,
                scheduler.poll(new SchedulerBudget(1, 10, 1_000)).get(0).workClass());
    }

    @Test
    void globalByteBudgetDoesNotDropAnUnserviceableHead() {
        final WorkClassScheduler scheduler = scheduler(100);
        scheduler.offer(new WorkClassTask(WorkClass.QUERY, "query-1", 8));
        assertEquals(List.of(), scheduler.poll(new SchedulerBudget(1, 7, 1_000)));
        assertEquals(1, scheduler.pending(WorkClass.QUERY));
    }

    @Test
    void invalidClockSampleDoesNotDropHeadBeforeTurnMutation() {
        final AtomicInteger calls = new AtomicInteger();
        final LongSupplier clock = () -> calls.getAndIncrement() < 4 ? 0 : -1;
        final WorkClassScheduler scheduler = scheduler(clock, 100);
        scheduler.offer(new WorkClassTask(WorkClass.QUERY, "query-1", 1));

        assertThrows(IllegalStateException.class,
                () -> scheduler.poll(new SchedulerBudget(1, 10, 1_000)));
        assertEquals(1, scheduler.pending(WorkClass.QUERY));
        assertEquals(1, scheduler.pendingBytes(WorkClass.QUERY));
    }

    @Test
    void clockFailureAfterAHeadWasSelectedRollsBackTheWholePoll() {
        final AtomicInteger calls = new AtomicInteger();
        final LongSupplier clock = () -> calls.incrementAndGet() <= 5 ? 0 : -1;
        final WorkClassScheduler scheduler = scheduler(clock, 100);
        scheduler.offer(new WorkClassTask(WorkClass.QUERY, "query-1", 1));
        scheduler.offer(new WorkClassTask(WorkClass.QUERY, "query-2", 1));

        assertThrows(IllegalStateException.class,
                () -> scheduler.poll(new SchedulerBudget(10, 10, 1_000)));
        assertEquals(2, scheduler.pending(WorkClass.QUERY));
        assertEquals(2, scheduler.pendingBytes(WorkClass.QUERY));
    }

    private static WorkClassScheduler scheduler(final long now, final long delay) {
        return scheduler(new AtomicLong(now), delay);
    }

    private static WorkClassScheduler scheduler(final long delay) {
        return scheduler(new AtomicLong(0), delay);
    }

    private static WorkClassScheduler scheduler(final AtomicLong now, final long delay) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean preemptive = workClass == WorkClass.LEASE_FENCE;
            final int maxRecordsPerTurn = workClass == WorkClass.SOURCE_APPLY ? 2 : 8;
            policies.put(workClass, new WorkClassPolicy(1, 8, 64, maxRecordsPerTurn, 16,
                    1_000, preemptive ? 1 : 0, preemptive ? 1 : 0, preemptive));
        }
        return new WorkClassScheduler(policies, delay, now::get);
    }

    private static WorkClassScheduler scheduler(final LongSupplier clock, final long delay) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean preemptive = workClass == WorkClass.LEASE_FENCE;
            final int maxRecordsPerTurn = workClass == WorkClass.SOURCE_APPLY ? 2 : 8;
            policies.put(workClass, new WorkClassPolicy(1, 8, 64, maxRecordsPerTurn, 16,
                    1_000, preemptive ? 1 : 0, preemptive ? 1 : 0, preemptive));
        }
        return new WorkClassScheduler(policies, delay, clock);
    }
}
