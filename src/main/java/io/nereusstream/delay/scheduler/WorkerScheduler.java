package io.nereusstream.delay.scheduler;

import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.LaneRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Bounded outer DRR over shard-local Lane schedulers. A blocked shard only
 * removes itself from the outer ring; it cannot pause another shard.
 */
public final class WorkerScheduler {
    private static final long DEFAULT_QUANTUM_BYTES = 256 * 1024;
    private static final long MAX_DEFICIT_MULTIPLIER = 4;

    private final long quantumBytes;
    private long maxDeficitBytes;
    private final int maxVisitShards;
    private final Map<ShardId, ShardQueue> shards = new HashMap<>();
    private final List<ShardId> ring = new ArrayList<>();
    private final Set<ShardId> recoveryServed = new HashSet<>();
    private int cursor;
    private long roundGeneration;
    private boolean recoveryFirstPass = true;

    public WorkerScheduler(final long quantumBytes, final int maxVisitShards) {
        if (quantumBytes <= 0 || maxVisitShards <= 0) {
            throw new IllegalArgumentException("worker scheduler limits must be positive");
        }
        this.quantumBytes = quantumBytes;
        this.maxDeficitBytes = checkedDeficitCap(quantumBytes);
        this.maxVisitShards = maxVisitShards;
    }

    public static WorkerScheduler defaults() {
        return new WorkerScheduler(DEFAULT_QUANTUM_BYTES, 64);
    }

    public synchronized void registerShard(final ShardId shardId, final int weight,
                                           final LaneScheduler laneScheduler) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(laneScheduler, "laneScheduler");
        if (weight <= 0) {
            throw new IllegalArgumentException("shard weight must be positive");
        }
        final long weightIncrement = checkedWeightIncrement(weight);
        final ShardQueue existing = shards.get(shardId);
        if (existing == null) {
            maxDeficitBytes = Math.max(maxDeficitBytes, weightIncrement);
            shards.put(shardId, new ShardQueue(shardId, weight, laneScheduler));
            ring.add(shardId);
            recoveryFirstPass = true;
            recoveryServed.clear();
        } else if (existing.weight != weight || existing.scheduler != laneScheduler) {
            throw new IllegalArgumentException("shard is already registered with different scheduler settings");
        }
    }

    public synchronized void registerLane(final ShardId shardId, final LaneRecord lane) {
        requireShard(shardId).scheduler.register(lane);
    }

    public synchronized void offer(final ScheduleWorkItem item) {
        final ShardId shardId = item.messageId().routingId().shardId();
        requireShard(shardId).scheduler.offer(item);
    }

    public synchronized List<ScheduleWorkItem> poll(final SchedulerBudget budget) {
        // Compatibility overload; production scheduling must pass the
        // trusted due-through timestamp below.
        return poll(Long.MAX_VALUE, budget);
    }

    /** Polls only work that is due through the supplied trusted time. */
    public synchronized List<ScheduleWorkItem> poll(final long dueThroughEpochMs,
                                                     final SchedulerBudget budget) {
        requireDueThrough(dueThroughEpochMs);
        Objects.requireNonNull(budget, "budget");
        final long started = System.nanoTime();
        final List<ScheduleWorkItem> result = new ArrayList<>();
        long bytes = 0;
        int visits = 0;
        if (ring.isEmpty()) {
            return result;
        }
        final long visitLimit = boundedVisitLimit(maxVisitShards, ring.size());
        while (visits < visitLimit
                && result.size() < budget.maxMessages() && bytes < budget.maxBytes()
                && System.nanoTime() - started < budget.maxElapsedNanos()) {
            final ShardQueue shard = shards.get(ring.get(cursor % ring.size()));
            cursor = (cursor + 1) % ring.size();
            visits++;
            if (shard == null || !shard.schedulable()) {
                continue;
            }
            final boolean firstPassVisit = recoveryFirstPass;
            final Set<ShardId> eligible = firstPassVisit ? eligibleShards() : Set.of();
            if (firstPassVisit && recoveryServed.contains(shard.shardId)) {
                continue;
            }
            shard.deficit = Math.min(saturatingAdd(shard.deficit, checkedWeightIncrement(shard.weight)),
                    Math.max(maxDeficitBytes, 1));
            final long remainingBytes = budget.maxBytes() - bytes;
            final long shardHeadBytes = shard.scheduler.minimumSchedulableHeadBytes();
            final long deficitOrHead = Math.max(shard.deficit, shardHeadBytes);
            final long shardBudgetBytes = Math.min(remainingBytes, deficitOrHead);
            if (shardBudgetBytes <= 0) {
                continue;
            }
            final int visitMaxMessages = firstPassVisit ? 1 : budget.maxMessages() - result.size();
            final List<ScheduleWorkItem> visit = shard.scheduler.poll(dueThroughEpochMs, new SchedulerBudget(
                    visitMaxMessages, shardBudgetBytes,
                    Math.max(1, budget.maxElapsedNanos() - (System.nanoTime() - started))));
            if (visit.isEmpty()) {
                continue;
            }
            long visitBytes = 0;
            for (ScheduleWorkItem item : visit) {
                result.add(item);
                visitBytes = Math.addExact(visitBytes, item.accountedBytes());
            }
            shard.deficit = Math.max(0, shard.deficit - visitBytes);
            roundGeneration = nextRoundGeneration(roundGeneration);
            shard.lastServedRound = roundGeneration;
            bytes = Math.addExact(bytes, visitBytes);
            if (firstPassVisit) {
                recoveryServed.add(shard.shardId);
                if (recoveryServed.containsAll(eligibleShards())) {
                    recoveryFirstPass = false;
                    recoveryServed.clear();
                }
            }
        }
        return result;
    }

    private static void requireDueThrough(final long dueThroughEpochMs) {
        if (dueThroughEpochMs < 0) {
            throw new IllegalArgumentException("scheduler due-through time must be non-negative");
        }
    }

    /**
     * Computes the outer two-rotation cap in a wide type so a large ring
     * cannot turn the bounded loop condition into a negative value.
     */
    static long boundedVisitLimit(final int maxVisitShards, final int ringSize) {
        if (maxVisitShards <= 0 || ringSize <= 0) {
            return 0;
        }
        return Math.min((long) maxVisitShards, (long) ringSize * 2L);
    }

    public synchronized void markShardBlocked(final ShardId shardId) {
        requireShard(shardId).blocked = true;
    }

    public synchronized void markShardReady(final ShardId shardId) {
        requireShard(shardId).blocked = false;
        recoveryFirstPass = true;
        recoveryServed.clear();
    }

    public synchronized WorkerSnapshot snapshot() {
        final List<ShardSnapshot> states = shards.values().stream()
                .sorted(Comparator.comparing(state -> state.shardId.toString()))
                .map(state -> new ShardSnapshot(state.shardId, state.weight, state.deficit,
                        state.lastServedRound, state.blocked)).toList();
        return new WorkerSnapshot(cursor, roundGeneration, states);
    }

    public synchronized void restore(final WorkerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        for (ShardSnapshot saved : snapshot.shards()) {
            final ShardQueue shard = shards.get(saved.shardId());
            if (shard == null || shard.weight != saved.weight()) {
                continue;
            }
            if (saved.deficit() < 0 || saved.lastServedRound() < 0) {
                throw new IllegalArgumentException("invalid worker scheduler counters");
            }
            shard.deficit = saved.deficit();
            shard.lastServedRound = saved.lastServedRound();
            shard.blocked = saved.blocked();
        }
        cursor = ring.isEmpty() ? 0 : Math.floorMod(snapshot.cursor(), ring.size());
        roundGeneration = snapshot.roundGeneration();
        recoveryFirstPass = true;
        recoveryServed.clear();
    }

    private Set<ShardId> eligibleShards() {
        final Set<ShardId> eligible = new HashSet<>();
        for (ShardQueue shard : shards.values()) {
            if (shard.schedulable()) {
                eligible.add(shard.shardId);
            }
        }
        return eligible;
    }

    private ShardQueue requireShard(final ShardId shardId) {
        final ShardQueue shard = shards.get(Objects.requireNonNull(shardId, "shardId"));
        if (shard == null) {
            throw new IllegalArgumentException("shard is not registered: " + shardId);
        }
        return shard;
    }

    private long checkedWeightIncrement(final int weight) {
        if (weight <= 0) {
            throw new IllegalArgumentException("shard weight must be positive");
        }
        try {
            return Math.multiplyExact((long) weight, quantumBytes);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("shard weight and scheduler quantum overflow", e);
        }
    }

    private static long checkedDeficitCap(final long quantumBytes) {
        try {
            return Math.multiplyExact(quantumBytes, MAX_DEFICIT_MULTIPLIER);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("worker scheduler quantum deficit cap overflows", e);
        }
    }

    private static long saturatingAdd(final long left, final long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long nextRoundGeneration(final long current) {
        return current == Long.MAX_VALUE ? Long.MAX_VALUE : current + 1;
    }

    public record WorkerSnapshot(int cursor, long roundGeneration, List<ShardSnapshot> shards) {
        public WorkerSnapshot {
            if (cursor < 0 || roundGeneration < 0) {
                throw new IllegalArgumentException("invalid worker scheduler snapshot");
            }
            shards = List.copyOf(shards);
        }
    }

    public record ShardSnapshot(ShardId shardId, int weight, long deficit, long lastServedRound, boolean blocked) {
        public ShardSnapshot {
            Objects.requireNonNull(shardId, "shardId");
            if (weight <= 0 || deficit < 0 || lastServedRound < 0) {
                throw new IllegalArgumentException("invalid shard scheduler snapshot");
            }
        }
    }

    private static final class ShardQueue {
        private final ShardId shardId;
        private final int weight;
        private final LaneScheduler scheduler;
        private long deficit;
        private long lastServedRound;
        private boolean blocked;

        private ShardQueue(final ShardId shardId, final int weight, final LaneScheduler scheduler) {
            this.shardId = shardId;
            this.weight = weight;
            this.scheduler = scheduler;
        }

        private boolean schedulable() {
            return !blocked && scheduler.snapshot().lanes().stream()
                    .anyMatch(lane -> lane.schedulable() && lane.pendingItems() > 0);
        }
    }
}
