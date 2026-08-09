package io.nereusstream.delay.scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Local bounded scheduler for the V1 Worker event-loop work classes.
 *
 * <p>This primitive owns queue and turn accounting only.  It deliberately
 * does not execute callbacks, perform WriteBatch admission, or grant shared
 * I/O tokens; production Worker wiring must compose those authorities around
 * the returned bounded task list.</p>
 */
public final class WorkClassScheduler {
    private final EnumMap<WorkClass, ClassState> states = new EnumMap<>(WorkClass.class);
    private final List<WorkClass> order = List.of(WorkClass.values());
    private final long maxEventLoopClassDelayNanos;
    private final LongSupplier clockNanos;
    private int cursor;
    private long lastClockNanos;

    public WorkClassScheduler(final Map<WorkClass, WorkClassPolicy> policies,
                              final long maxEventLoopClassDelayNanos,
                              final LongSupplier clockNanos) {
        Objects.requireNonNull(policies, "policies");
        if (!EnumSet.allOf(WorkClass.class).equals(policies.keySet())) {
            throw new IllegalArgumentException("policies must cover every V1 work class exactly");
        }
        if (maxEventLoopClassDelayNanos <= 0) {
            throw new IllegalArgumentException("maxEventLoopClassDelayNanos must be positive");
        }
        this.maxEventLoopClassDelayNanos = maxEventLoopClassDelayNanos;
        this.clockNanos = Objects.requireNonNull(clockNanos, "clockNanos");
        lastClockNanos = readClock();
        for (WorkClass workClass : order) {
            states.put(workClass, new ClassState(workClass, policies.get(workClass), lastClockNanos));
        }
    }

    public synchronized void offer(final WorkClassTask task) {
        Objects.requireNonNull(task, "task");
        final ClassState state = state(task.workClass());
        if (task.bytes() > state.policy.maxBytesPerTurn()) {
            throw new IllegalArgumentException("work-class task exceeds its per-turn byte cap");
        }
        if (state.queue.size() >= state.policy.maxQueueRecords()
                || task.bytes() > state.policy.maxQueueBytes() - state.queuedBytes) {
            throw new IllegalStateException("work-class queue capacity exceeded for " + task.workClass());
        }
        state.queue.addLast(task);
        state.queuedBytes = Math.addExact(state.queuedBytes, task.bytes());
    }

    /** Polls a bounded turn, returning task identities without executing them. */
    public synchronized List<WorkClassTask> poll(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        final long started = readClock();
        final EnumMap<WorkClass, TurnUsage> usage = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : order) {
            usage.put(workClass, new TurnUsage());
        }
        final List<WorkClassTask> result = new ArrayList<>();
        long bytes = 0;
        int noProgressRounds = 0;
        replenishCredits();
        while (result.size() < budget.maxMessages() && bytes < budget.maxBytes()) {
            final long elapsed = elapsedSince(started, readClock());
            if (elapsed >= budget.maxElapsedNanos()) {
                break;
            }
            final int priority = priorityIndex(usage, budget.maxBytes() - bytes, elapsed);
            final int selected = priority >= 0 ? priority : normalIndex(usage,
                    budget.maxBytes() - bytes, elapsed);
            if (selected < 0) {
                if (noProgressRounds == 0) {
                    replenishCredits();
                    noProgressRounds = 1;
                    continue;
                }
                break;
            }
            final ClassState state = states.get(order.get(selected));
            final WorkClassTask task = state.queue.removeFirst();
            state.queuedBytes -= task.bytes();
            final TurnUsage classUsage = usage.get(state.workClass);
            classUsage.records++;
            classUsage.bytes = Math.addExact(classUsage.bytes, task.bytes());
            state.credits--;
            state.lastServedNanos = readClock();
            cursor = (selected + 1) % order.size();
            noProgressRounds = 0;
            result.add(task);
            bytes = Math.addExact(bytes, task.bytes());
        }
        return List.copyOf(result);
    }

    public synchronized int pending(final WorkClass workClass) {
        return state(workClass).queue.size();
    }

    public synchronized long pendingBytes(final WorkClass workClass) {
        return state(workClass).queuedBytes;
    }

    public synchronized WorkClassPolicy policy(final WorkClass workClass) {
        return state(workClass).policy;
    }

    private int priorityIndex(final EnumMap<WorkClass, TurnUsage> usage,
                              final long remainingBytes,
                              final long elapsedNanos) {
        int selected = -1;
        long oldest = Long.MAX_VALUE;
        for (int index = 0; index < order.size(); index++) {
            final ClassState state = states.get(order.get(index));
            if (state.queue.isEmpty() || !canServe(state, usage.get(state.workClass), remainingBytes, elapsedNanos)) {
                continue;
            }
            final boolean preemptive = state.policy.preemptive();
            final boolean overdue = elapsedSince(state.lastServedNanos, readClock())
                    >= maxEventLoopClassDelayNanos;
            if (preemptive || overdue) {
                if (preemptive && (selected < 0 || !states.get(order.get(selected)).policy.preemptive())) {
                    selected = index;
                    oldest = state.lastServedNanos;
                } else if (!preemptive && (selected < 0
                        || (!states.get(order.get(selected)).policy.preemptive()
                        && overdue && state.lastServedNanos < oldest))) {
                    selected = index;
                    oldest = state.lastServedNanos;
                }
            }
        }
        return selected;
    }

    private int normalIndex(final EnumMap<WorkClass, TurnUsage> usage,
                            final long remainingBytes,
                            final long elapsedNanos) {
        for (int offset = 0; offset < order.size(); offset++) {
            final int index = (cursor + offset) % order.size();
            final ClassState state = states.get(order.get(index));
            if (!state.queue.isEmpty() && state.credits > 0
                    && canServe(state, usage.get(state.workClass), remainingBytes, elapsedNanos)) {
                return index;
            }
        }
        return -1;
    }

    private boolean canServe(final ClassState state, final TurnUsage usage,
                             final long remainingBytes, final long elapsedNanos) {
        final WorkClassPolicy policy = state.policy;
        final WorkClassTask head = state.queue.peekFirst();
        return head != null
                && usage.records < policy.maxRecordsPerTurn()
                && usage.bytes <= policy.maxBytesPerTurn() - head.bytes()
                && head.bytes() <= remainingBytes
                && elapsedNanos < policy.maxTimePerTurnNanos();
    }

    private void replenishCredits() {
        for (ClassState state : states.values()) {
            state.credits = state.credits > Integer.MAX_VALUE - state.policy.weight()
                    ? Integer.MAX_VALUE : state.credits + state.policy.weight();
        }
    }

    private ClassState state(final WorkClass workClass) {
        return states.get(Objects.requireNonNull(workClass, "workClass"));
    }

    private long readClock() {
        final long now = clockNanos.getAsLong();
        if (now < 0 || now < lastClockNanos) {
            throw new IllegalStateException("work-class clock must be monotonic and non-negative");
        }
        lastClockNanos = now;
        return now;
    }

    private static long elapsedSince(final long start, final long end) {
        try {
            return Math.subtractExact(end, start);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static final class ClassState {
        private final WorkClass workClass;
        private final WorkClassPolicy policy;
        private final ArrayDeque<WorkClassTask> queue = new ArrayDeque<>();
        private long queuedBytes;
        private int credits;
        private long lastServedNanos;

        private ClassState(final WorkClass workClass, final WorkClassPolicy policy, final long nowNanos) {
            this.workClass = workClass;
            this.policy = Objects.requireNonNull(policy, "work-class policy");
            this.lastServedNanos = nowNanos;
        }
    }

    private static final class TurnUsage {
        private int records;
        private long bytes;
    }
}
