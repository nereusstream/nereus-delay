package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.RuntimeReadiness;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Destination-Lane weighted deficit round robin. Lane circuit/failure state is
 * deliberately local to the lane and never pauses source command application.
 */
public final class LaneScheduler {
    private static final long DEFAULT_QUANTUM_BYTES = 64 * 1024;
    private static final long MAX_DEFICIT_MULTIPLIER = 4;

    private final long quantumBytes;
    private long maxDeficitBytes;
    private final int maxVisitMessages;
    private final Map<DestinationLaneId, LaneQueue> lanes = new HashMap<>();
    private final List<DestinationLaneId> ring = new ArrayList<>();
    private int cursor;
    private long roundGeneration;

    public LaneScheduler(final long quantumBytes, final int maxVisitMessages) {
        if (quantumBytes <= 0 || maxVisitMessages <= 0) {
            throw new IllegalArgumentException("scheduler limits must be positive");
        }
        this.quantumBytes = quantumBytes;
        this.maxDeficitBytes = checkedDeficitCap(quantumBytes);
        this.maxVisitMessages = maxVisitMessages;
    }

    public static LaneScheduler defaults() {
        return new LaneScheduler(DEFAULT_QUANTUM_BYTES, 64);
    }

    public synchronized void register(final LaneRecord lane) {
        Objects.requireNonNull(lane, "lane");
        final long weightIncrement = checkedWeightIncrement(lane.weight());
        final LaneQueue existing = lanes.get(lane.laneId());
        if (existing == null) {
            maxDeficitBytes = Math.max(maxDeficitBytes, weightIncrement);
            lanes.put(lane.laneId(), new LaneQueue(lane));
            ring.add(lane.laneId());
        } else {
            existing.update(lane);
            maxDeficitBytes = Math.max(maxDeficitBytes, weightIncrement);
        }
    }

    public synchronized void offer(final ScheduleWorkItem item) {
        final LaneQueue lane = requireLane(item.laneId());
        lane.queue.addLast(item);
    }

    public synchronized List<ScheduleWorkItem> poll(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        return pollInternal(budget, Set.of(), false);
    }

    /**
     * Serves at most one item from each lane that is not already present in
     * {@code alreadyServed}. Persistent recovery uses this bounded mode for
     * the first rotation after an owner/store restart; normal scheduling keeps
     * the regular DRR behavior above.
     */
    synchronized List<ScheduleWorkItem> pollRecoveryFirstPass(final SchedulerBudget budget,
                                                               final Set<DestinationLaneId> alreadyServed) {
        Objects.requireNonNull(alreadyServed, "alreadyServed");
        return pollInternal(budget, Set.copyOf(alreadyServed), true);
    }

    private List<ScheduleWorkItem> pollInternal(final SchedulerBudget budget,
                                                final Set<DestinationLaneId> excludedLanes,
                                                final boolean onePerLane) {
        Objects.requireNonNull(budget, "budget");
        final long started = System.nanoTime();
        final List<ScheduleWorkItem> result = new ArrayList<>();
        final Set<DestinationLaneId> servedThisPoll = new HashSet<>();
        long bytes = 0;
        int visits = 0;
        final int ringSize = ring.size();
        if (ringSize == 0) {
            return result;
        }
        final long ringVisitLimit = boundedRingVisitLimit(ringSize);
        while (visits < ringVisitLimit && result.size() < Math.min(maxVisitMessages, budget.maxMessages())
                && bytes < budget.maxBytes() && System.nanoTime() - started < budget.maxElapsedNanos()) {
            final DestinationLaneId id = ring.get(cursor % ringSize);
            cursor = (cursor + 1) % ringSize;
            visits++;
            if (excludedLanes.contains(id) || (onePerLane && !servedThisPoll.add(id))) {
                continue;
            }
            final LaneQueue lane = lanes.get(id);
            if (lane == null || !lane.schedulable() || lane.queue.isEmpty()) {
                servedThisPoll.remove(id);
                continue;
            }
            final ScheduleWorkItem head = lane.queue.peekFirst();
            final long increment = checkedWeightIncrement(lane.weight);
            lane.deficit = Math.min(saturatingAdd(lane.deficit, increment),
                    Math.max(maxDeficitBytes, head.accountedBytes()));
            if (head.accountedBytes() > lane.deficit || head.accountedBytes() > budget.maxBytes() - bytes) {
                continue;
            }
            lane.queue.removeFirst();
            lane.deficit -= head.accountedBytes();
            roundGeneration = nextRoundGeneration(roundGeneration);
            lane.lastServedRound = roundGeneration;
            result.add(head);
            bytes = Math.addExact(bytes, head.accountedBytes());
        }
        return result;
    }

    /**
     * Returns the bounded two-rotation visit budget without narrowing the
     * multiplication back to an overflowing {@code int}.
     */
    static long boundedRingVisitLimit(final int ringSize) {
        if (ringSize <= 0) {
            return 0;
        }
        return (long) ringSize * 2L;
    }

    public synchronized void markBlocked(final DestinationLaneId laneId) {
        final LaneQueue lane = requireLane(laneId);
        lane.runtimeReadiness = RuntimeReadiness.BLOCKED;
    }

    public synchronized void markReady(final DestinationLaneId laneId) {
        final LaneQueue lane = requireLane(laneId);
        if (lane.gate != io.nereusstream.delay.runtime.AdmissionGate.OPEN) {
            throw new IllegalStateException("closed or paused lane cannot be marked ready");
        }
        lane.runtimeReadiness = RuntimeReadiness.READY;
    }

    public synchronized void requeueFirst(final ScheduleWorkItem item) {
        requireLane(item.laneId()).queue.addFirst(item);
    }

    public synchronized int pendingItems(final DestinationLaneId laneId) {
        return requireLane(laneId).queue.size();
    }

    /** Returns the current in-memory head without removing it. */
    synchronized ScheduleWorkItem pendingHead(final DestinationLaneId laneId) {
        return requireLane(laneId).queue.peekFirst();
    }

    /**
     * Returns the smallest currently schedulable Lane-head size, or zero when
     * no Lane can make progress. The outer Worker DRR uses this to avoid
     * making a valid record larger than the outer deficit cap permanently
     * unserviceable.
     */
    synchronized long minimumSchedulableHeadBytes() {
        long minimum = Long.MAX_VALUE;
        for (LaneQueue lane : lanes.values()) {
            if (lane.schedulable() && !lane.queue.isEmpty()) {
                minimum = Math.min(minimum, lane.queue.peekFirst().accountedBytes());
            }
        }
        return minimum == Long.MAX_VALUE ? 0 : minimum;
    }

    public synchronized long roundGeneration() {
        return roundGeneration;
    }

    public synchronized SchedulerSnapshot snapshot() {
        final List<LaneSnapshot> states = lanes.values().stream()
                .sorted(Comparator.comparing(state -> state.laneId.toString()))
                .map(state -> new LaneSnapshot(state.laneId, state.weight, state.deficit, state.lastServedRound,
                        state.queue.size(), state.schedulable()))
                .toList();
        return new SchedulerSnapshot(cursor, roundGeneration, states);
    }

    /** Returns the semantic successor order used by the inner DRR ring. */
    public synchronized List<DestinationLaneId> ringOrder() {
        return List.copyOf(ring);
    }

    /** Returns Lane snapshots in semantic ring order rather than key order. */
    public synchronized List<LaneSnapshot> orderedSnapshot() {
        return ring.stream().map(lanes::get)
                .filter(Objects::nonNull)
                .map(state -> new LaneSnapshot(state.laneId, state.weight, state.deficit, state.lastServedRound,
                        state.queue.size(), state.schedulable()))
                .toList();
    }

    /** Rebuilds the in-memory ring from a validated persisted successor order. */
    public synchronized void restoreRing(final List<DestinationLaneId> persistedOrder) {
        Objects.requireNonNull(persistedOrder, "persistedOrder");
        final Set<DestinationLaneId> seen = new HashSet<>();
        final List<DestinationLaneId> rebuilt = new ArrayList<>();
        for (DestinationLaneId laneId : persistedOrder) {
            if (!seen.add(laneId) || !lanes.containsKey(laneId)) {
                continue;
            }
            rebuilt.add(laneId);
        }
        for (DestinationLaneId laneId : ring) {
            if (seen.add(laneId)) {
                rebuilt.add(laneId);
            }
        }
        ring.clear();
        ring.addAll(rebuilt);
        cursor = ring.isEmpty() ? 0 : cursor % ring.size();
    }

    /** Replaces the active ring with an authority-validated successor order. */
    public synchronized void rebuildActiveRing(final List<DestinationLaneId> activeOrder) {
        Objects.requireNonNull(activeOrder, "activeOrder");
        final Set<DestinationLaneId> seen = new HashSet<>();
        final List<DestinationLaneId> rebuilt = new ArrayList<>();
        for (DestinationLaneId laneId : activeOrder) {
            if (seen.add(laneId) && lanes.containsKey(laneId)) {
                rebuilt.add(laneId);
            }
        }
        ring.clear();
        ring.addAll(rebuilt);
        cursor = ring.isEmpty() ? 0 : cursor % ring.size();
    }

    /** Adds one registered Lane to the active ring after a READY transition. */
    public synchronized void activateLane(final DestinationLaneId laneId) {
        requireLane(laneId);
        if (!ring.contains(laneId)) {
            ring.add(laneId);
        }
    }

    /** Removes one Lane from the active ring after a fenced readiness loss. */
    public synchronized void deactivateLane(final DestinationLaneId laneId) {
        requireLane(laneId);
        final int removed = ring.indexOf(laneId);
        if (removed < 0) {
            return;
        }
        ring.remove(removed);
        if (ring.isEmpty()) {
            cursor = 0;
        } else if (removed < cursor) {
            cursor--;
        } else {
            cursor %= ring.size();
        }
    }

    /**
     * Removes a terminal Lane from the local scheduler registry.
     *
     * <p>The source-ordered terminal guard and any Adapter teardown authority
     * remain outside this in-memory scheduler.  This method only accepts a
     * Lane whose exact incarnation is fenced, whose gate is terminal, and
     * whose queue is empty; an old callback cannot remove a replacement
     * registration.</p>
     */
    public synchronized void unregister(final DestinationLaneId laneId,
                                        final byte[] laneIncarnation) {
        final LaneQueue lane = requireLane(laneId);
        Bytes.requireLength(laneIncarnation, 16, "laneIncarnation");
        if (!Arrays.equals(lane.laneIncarnation, laneIncarnation)) {
            throw new IllegalArgumentException("lane incarnation mismatch");
        }
        if (lane.gate != io.nereusstream.delay.runtime.AdmissionGate.CLOSED
                && lane.gate != io.nereusstream.delay.runtime.AdmissionGate.RETIRED) {
            throw new IllegalStateException("only a terminal Lane can be unregistered");
        }
        if (!lane.queue.isEmpty()) {
            throw new IllegalStateException("cannot unregister a Lane with pending work");
        }
        deactivateLane(laneId);
        lanes.remove(laneId);
    }

    /** Replaces all in-memory work with the exact READY projection recovered from storage. */
    public synchronized void replacePending(final List<ScheduleWorkItem> items) {
        Objects.requireNonNull(items, "items");
        lanes.values().forEach(lane -> lane.queue.clear());
        for (ScheduleWorkItem item : items) {
            final LaneQueue lane = requireLane(item.laneId());
            if (!lane.schedulable()) {
                throw new IllegalStateException("READY work belongs to a non-schedulable lane: " + item.laneId());
            }
            lane.queue.addLast(item);
        }
    }

    /** Restores fair-scheduling counters after all current Lane records are registered. */
    public synchronized void restore(final SchedulerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.cursor() < 0 || snapshot.roundGeneration() < 0) {
            throw new IllegalArgumentException("invalid scheduler snapshot");
        }
        for (LaneSnapshot saved : snapshot.lanes()) {
            final LaneQueue lane = lanes.get(saved.laneId());
            if (lane == null || lane.weight != saved.weight()) {
                continue;
            }
            if (saved.deficit() < 0 || saved.lastServedRound() < 0) {
                throw new IllegalArgumentException("invalid lane scheduler counters");
            }
            lane.deficit = saved.deficit();
            lane.lastServedRound = saved.lastServedRound();
        }
        cursor = ring.isEmpty() ? 0 : snapshot.cursor() % ring.size();
        roundGeneration = snapshot.roundGeneration();
    }

    private LaneQueue requireLane(final DestinationLaneId laneId) {
        final LaneQueue lane = lanes.get(Objects.requireNonNull(laneId, "laneId"));
        if (lane == null) {
            throw new IllegalArgumentException("lane is not registered: " + laneId);
        }
        return lane;
    }

    private long checkedWeightIncrement(final int weight) {
        if (weight <= 0) {
            throw new IllegalArgumentException("lane weight must be positive");
        }
        try {
            return Math.multiplyExact((long) weight, quantumBytes);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("lane weight and scheduler quantum overflow", e);
        }
    }

    private static long checkedDeficitCap(final long quantumBytes) {
        try {
            return Math.multiplyExact(quantumBytes, MAX_DEFICIT_MULTIPLIER);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("scheduler quantum deficit cap overflows", e);
        }
    }

    private static long saturatingAdd(final long left, final long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long nextRoundGeneration(final long current) {
        return current == Long.MAX_VALUE ? Long.MAX_VALUE : current + 1;
    }

    public record SchedulerSnapshot(int cursor, long roundGeneration, List<LaneSnapshot> lanes) {
        public SchedulerSnapshot {
            lanes = List.copyOf(lanes);
        }
    }

    public record LaneSnapshot(DestinationLaneId laneId, int weight, long deficit, long lastServedRound,
                               int pendingItems, boolean schedulable) {
    }

    private static final class LaneQueue {
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final Deque<ScheduleWorkItem> queue = new ArrayDeque<>();
        private int weight;
        private long deficit;
        private long lastServedRound;
        private io.nereusstream.delay.runtime.AdmissionGate gate;
        private RuntimeReadiness runtimeReadiness;

        private LaneQueue(final LaneRecord lane) {
            laneId = lane.laneId();
            laneIncarnation = lane.laneIncarnation();
            update(lane);
        }

        private void update(final LaneRecord lane) {
            if (!Arrays.equals(laneIncarnation, lane.laneIncarnation())) {
                throw new IllegalArgumentException("lane incarnation cannot change for a registered lane: " + laneId);
            }
            weight = lane.weight();
            gate = lane.admissionGate();
            runtimeReadiness = lane.runtimeReadiness();
        }

        private boolean schedulable() {
            return gate == io.nereusstream.delay.runtime.AdmissionGate.OPEN
                    && runtimeReadiness == RuntimeReadiness.READY;
        }
    }
}
