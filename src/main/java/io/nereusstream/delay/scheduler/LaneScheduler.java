package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.RuntimeReadiness;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Destination-Lane weighted deficit round robin. Lane circuit/failure state is
 * deliberately local to the lane and never pauses source command application.
 */
public final class LaneScheduler {
    private static final long DEFAULT_QUANTUM_BYTES = 64 * 1024;
    private static final long MAX_DEFICIT_MULTIPLIER = 4;

    private final long quantumBytes;
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
        this.maxVisitMessages = maxVisitMessages;
    }

    public static LaneScheduler defaults() {
        return new LaneScheduler(DEFAULT_QUANTUM_BYTES, 64);
    }

    public synchronized void register(final LaneRecord lane) {
        Objects.requireNonNull(lane, "lane");
        final LaneQueue existing = lanes.get(lane.laneId());
        if (existing == null) {
            lanes.put(lane.laneId(), new LaneQueue(lane));
            ring.add(lane.laneId());
        } else {
            existing.update(lane);
        }
    }

    public synchronized void offer(final ScheduleWorkItem item) {
        final LaneQueue lane = requireLane(item.laneId());
        lane.queue.addLast(item);
    }

    public synchronized List<ScheduleWorkItem> poll(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        final long started = System.nanoTime();
        final List<ScheduleWorkItem> result = new ArrayList<>();
        long bytes = 0;
        int visits = 0;
        final int ringSize = ring.size();
        if (ringSize == 0) {
            return result;
        }
        while (visits < ringSize * 2 && result.size() < Math.min(maxVisitMessages, budget.maxMessages())
                && bytes < budget.maxBytes() && System.nanoTime() - started < budget.maxElapsedNanos()) {
            final DestinationLaneId id = ring.get(cursor % ringSize);
            cursor = (cursor + 1) % ringSize;
            visits++;
            final LaneQueue lane = lanes.get(id);
            if (lane == null || !lane.schedulable() || lane.queue.isEmpty()) {
                continue;
            }
            lane.deficit = Math.min(lane.deficit + lane.weight * quantumBytes,
                    Math.max(quantumBytes * MAX_DEFICIT_MULTIPLIER, lane.queue.peekFirst().accountedBytes()));
            final ScheduleWorkItem head = lane.queue.peekFirst();
            if (head.accountedBytes() > lane.deficit || head.accountedBytes() > budget.maxBytes() - bytes) {
                continue;
            }
            lane.queue.removeFirst();
            lane.deficit -= head.accountedBytes();
            lane.lastServedRound = ++roundGeneration;
            result.add(head);
            bytes += head.accountedBytes();
        }
        return result;
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
        private final Deque<ScheduleWorkItem> queue = new ArrayDeque<>();
        private int weight;
        private long deficit;
        private long lastServedRound;
        private io.nereusstream.delay.runtime.AdmissionGate gate;
        private RuntimeReadiness runtimeReadiness;

        private LaneQueue(final LaneRecord lane) {
            laneId = lane.laneId();
            update(lane);
        }

        private void update(final LaneRecord lane) {
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
