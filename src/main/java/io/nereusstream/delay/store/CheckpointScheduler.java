package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded process-local checkpoint scheduler for the independently stored
 * Delay Shards in one Worker.
 *
 * <p>The schedule is deliberately not a durability authority.  A claimed
 * task must still create the complete shard checkpoint and publish its intent
 * through the checkpoint/catalog protocol.  This class only provides stable
 * interval/jitter placement and prevents one shard from being claimed twice
 * before the caller reports completion.</p>
 */
public final class CheckpointScheduler {
    private static final byte[] JITTER_DOMAIN = Bytes.utf8("nereus-delay-checkpoint-jitter-v1\0");
    private static final Comparator<ScheduledCheckpoint> ORDER = Comparator
            .comparingLong(ScheduledCheckpoint::dueAtEpochMs)
            .thenComparing(value -> Bytes.hex(value.shardId().routeIncarnation().bytes()))
            .thenComparingInt(value -> value.shardId().partition());

    private final long intervalMs;
    private final int jitterPercent;
    private final int maxScheduledShards;
    private final Map<ShardId, State> states = new HashMap<>();

    public CheckpointScheduler(final long intervalMs, final int jitterPercent,
                               final int maxScheduledShards) {
        if (intervalMs <= 0 || jitterPercent < 0 || jitterPercent >= 100 || maxScheduledShards <= 0) {
            throw new IllegalArgumentException("invalid checkpoint scheduler limits");
        }
        final long maxJitter = maxJitter(intervalMs, jitterPercent);
        if (maxJitter > (Long.MAX_VALUE - 1) / 2) {
            throw new IllegalArgumentException("checkpoint jitter range overflows");
        }
        this.intervalMs = intervalMs;
        this.jitterPercent = jitterPercent;
        this.maxScheduledShards = maxScheduledShards;
    }

    /** Registers a shard and returns its first deterministic due time. */
    public synchronized long register(final ShardId shardId, final long nowEpochMs) {
        Objects.requireNonNull(shardId, "shardId");
        requireTime(nowEpochMs, "nowEpochMs");
        if (states.containsKey(shardId)) {
            throw new IllegalStateException("checkpoint shard is already registered: " + shardId);
        }
        if (states.size() >= maxScheduledShards) {
            throw new IllegalStateException("checkpoint scheduler shard limit reached");
        }
        final long due = nextDue(nowEpochMs, shardId);
        states.put(shardId, new State(due, null, false));
        return due;
    }

    /** Removes an idle shard from this process-local schedule. */
    public synchronized void unregister(final ShardId shardId) {
        final State state = requireState(shardId);
        if (state.inFlight()) {
            throw new IllegalStateException("cannot unregister an in-flight checkpoint: " + shardId);
        }
        states.remove(shardId);
    }

    /**
     * Claims at most {@code limit} due shards.  Claimed shards are omitted
     * from later polls until the exact {@link ScheduledCheckpoint} returned
     * by this method is passed to {@link #complete(ScheduledCheckpoint, long)}.
     * The returned value is a process-local claim handle; callers must not
     * reconstruct it from the shard ID and due time.
     */
    public synchronized List<ScheduledCheckpoint> claimDue(final long nowEpochMs, final int limit) {
        requireTime(nowEpochMs, "nowEpochMs");
        if (limit <= 0) {
            throw new IllegalArgumentException("claim limit must be positive");
        }
        final List<ScheduledCheckpoint> due = new ArrayList<>();
        for (Map.Entry<ShardId, State> entry : states.entrySet()) {
            final State state = entry.getValue();
            if (!state.inFlight() && state.dueAtEpochMs() <= nowEpochMs) {
                due.add(new ScheduledCheckpoint(entry.getKey(), state.dueAtEpochMs()));
            }
        }
        due.sort(ORDER);
        final int claimed = Math.min(limit, due.size());
        for (int index = 0; index < claimed; index++) {
            final ScheduledCheckpoint task = due.get(index);
            states.put(task.shardId(), new State(task.dueAtEpochMs(), task, true));
        }
        return List.copyOf(due.subList(0, claimed));
    }

    /** Reschedules a successfully completed or failed checkpoint attempt. */
    public synchronized long complete(final ScheduledCheckpoint claimed, final long completedAtEpochMs) {
        requireTime(completedAtEpochMs, "completedAtEpochMs");
        Objects.requireNonNull(claimed, "claimed");
        final State state = requireState(claimed.shardId());
        if (!state.inFlight() || state.claim() != claimed) {
            throw new IllegalStateException("checkpoint claim is no longer current: " + claimed.shardId());
        }
        final long due = nextDue(completedAtEpochMs, claimed.shardId());
        states.put(claimed.shardId(), new State(due, null, false));
        return due;
    }

    /**
     * A completion that carries only a shard ID cannot be fenced against a
     * late callback from an earlier checkpoint attempt.  Keep this overload
     * as a source-compatible fail-closed trap for callers that have not yet
     * migrated to the exact claim handle.
     *
     * @deprecated pass the exact value returned by {@link #claimDue(long, int)}
     *             to {@link #complete(ScheduledCheckpoint, long)}.
     */
    @Deprecated
    public synchronized long complete(final ShardId shardId, final long completedAtEpochMs) {
        Objects.requireNonNull(shardId, "shardId");
        requireTime(completedAtEpochMs, "completedAtEpochMs");
        throw new IllegalStateException("checkpoint completion requires the exact claim handle");
    }

    public synchronized boolean isInFlight(final ShardId shardId) {
        return requireState(shardId).inFlight();
    }

    public synchronized int size() {
        return states.size();
    }

    private long nextDue(final long baseEpochMs, final ShardId shardId) {
        final long delay = Math.addExact(intervalMs, jitterOffsetMs(shardId));
        return Math.addExact(baseEpochMs, delay);
    }

    private long jitterOffsetMs(final ShardId shardId) {
        final long maxJitter = maxJitter(intervalMs, jitterPercent);
        if (maxJitter == 0) {
            return 0;
        }
        final byte[] digest = Bytes.sha256(JITTER_DOMAIN, shardId.routeIncarnation().bytes(),
                Bytes.u32be(shardId.partition()));
        final long sample = ByteBuffer.wrap(digest).getLong();
        final long span = Math.addExact(Math.multiplyExact(maxJitter, 2), 1);
        return Math.subtractExact(Long.remainderUnsigned(sample, span), maxJitter);
    }

    private State requireState(final ShardId shardId) {
        final State state = states.get(Objects.requireNonNull(shardId, "shardId"));
        if (state == null) {
            throw new IllegalArgumentException("checkpoint shard is not registered: " + shardId);
        }
        return state;
    }

    private static long maxJitter(final long intervalMs, final int jitterPercent) {
        return Math.multiplyExact(intervalMs, jitterPercent) / 100;
    }

    private static void requireTime(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private record State(long dueAtEpochMs, ScheduledCheckpoint claim, boolean inFlight) {
    }

    public record ScheduledCheckpoint(ShardId shardId, long dueAtEpochMs) {
        public ScheduledCheckpoint {
            Objects.requireNonNull(shardId, "shardId");
            if (dueAtEpochMs < 0) {
                throw new IllegalArgumentException("dueAtEpochMs must be non-negative");
            }
        }
    }
}
