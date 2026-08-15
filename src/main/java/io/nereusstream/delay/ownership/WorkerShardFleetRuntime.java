package io.nereusstream.delay.ownership;

import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.store.SharedRocksDbResources;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Bounded round-robin dispatch over the independently stored Worker shards in
 * one process.
 *
 * <p>The fleet owns no assignment, lease or broker authority.  Each shard
 * runtime has already crossed those gates before it is admitted here.  This
 * class only prevents the event loop from repeatedly servicing the first
 * ready shard while other accepted shards never receive a source, scheduling
 * or command turn.  The supplied budget applies to the selected shard turn;
 * the shared work-class registry remains the single process-wide queue and
 * admission authority.</p>
 */
public final class WorkerShardFleetRuntime implements AutoCloseable {
    private final WorkClassExecutionRegistry workClasses;
    private final SharedRocksDbResources resources;
    private final List<WorkerShardRuntime> shards;
    private int sourceCursor;
    private int schedulingCursor;
    private int commandCursor;
    private boolean closed;

    public WorkerShardFleetRuntime(final WorkClassExecutionRegistry workClasses,
                                   final SharedRocksDbResources resources,
                                   final List<WorkerShardRuntime> shardRuntimes) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.resources = Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(shardRuntimes, "shardRuntimes");
        if (shardRuntimes.isEmpty()) {
            throw new IllegalArgumentException("Worker shard fleet must contain at least one shard");
        }
        final Set<io.nereusstream.delay.protocol.ShardId> identities = new HashSet<>();
        final List<WorkerShardRuntime> admitted = new ArrayList<>(shardRuntimes.size());
        for (WorkerShardRuntime runtime : shardRuntimes) {
            final WorkerShardRuntime candidate = Objects.requireNonNull(runtime, "shard runtime");
            candidate.requireFleetComposition(this.workClasses, this.resources);
            if (!identities.add(candidate.shardId())) {
                throw new IllegalArgumentException("Worker shard fleet contains a duplicate shard: "
                        + candidate.shardId());
            }
            admitted.add(candidate);
        }
        this.shards = List.copyOf(admitted);
    }

    /** Returns the immutable admission order used as the deterministic tie-breaker. */
    public synchronized List<io.nereusstream.delay.protocol.ShardId> shardIds() {
        return shards.stream().map(WorkerShardRuntime::shardId).toList();
    }

    /** Runs one source turn on the next shard in round-robin order. */
    public synchronized SourceTurn runNextSourceTurn(final SchedulerBudget budget,
                                                      final LongSupplier ownerClock) {
        ensureOpen();
        final int index = sourceCursor;
        sourceCursor = (sourceCursor + 1) % shards.size();
        final WorkerShardRuntime runtime = shards.get(index);
        return new SourceTurn(runtime.shardId(), runtime.runSourceTurn(
                Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock")));
    }

    /**
     * Runs one scheduling turn on the next shard that has an active-owner
     * scheduling graph. Empty means this fleet was built from source-only
     * runtimes and therefore has no scheduling action to dispatch.
     */
    public synchronized Optional<SchedulingTurn> runNextSchedulingTurn(final SchedulerBudget budget) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(true, false);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(new SchedulingTurn(runtime.shardId(), runtime.runSchedulingTurn(
                Objects.requireNonNull(budget, "budget"))));
    }

    /** Runs one Claim/Publish command turn on the next command-enabled shard. */
    public synchronized Optional<CommandTurn> runNextCommandTurn(final SchedulerBudget budget) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(false, true);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(new CommandTurn(runtime.shardId(), runtime.runCommandTurn(
                Objects.requireNonNull(budget, "budget"))));
    }

    /** Refuses to bypass per-shard owner drain and source-loop closure. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        for (WorkerShardRuntime runtime : shards) {
            runtime.close();
        }
        closed = true;
    }

    private Optional<IndexedRuntime> nextRuntime(final boolean scheduling, final boolean command) {
        int cursor = scheduling ? schedulingCursor : commandCursor;
        for (int offset = 0; offset < shards.size(); offset++) {
            final int index = (cursor + offset) % shards.size();
            final WorkerShardRuntime runtime = shards.get(index);
            if ((scheduling && runtime.hasSchedulingRuntime()) || (command && runtime.hasCommandRuntime())) {
                if (scheduling) {
                    schedulingCursor = (index + 1) % shards.size();
                } else {
                    commandCursor = (index + 1) % shards.size();
                }
                return Optional.of(new IndexedRuntime(index, runtime));
            }
        }
        return Optional.empty();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Worker shard fleet is closed");
        }
    }

    private record IndexedRuntime(int index, WorkerShardRuntime runtime) {
    }

    public record SourceTurn(io.nereusstream.delay.protocol.ShardId shardId,
                             SourceApplyCoordinator.TurnResult result) {
        public SourceTurn {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(result, "result");
        }
    }

    public record SchedulingTurn(io.nereusstream.delay.protocol.ShardId shardId,
                                List<WorkClassTask> completedTasks) {
        public SchedulingTurn {
            Objects.requireNonNull(shardId, "shardId");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
        }
    }

    public record CommandTurn(io.nereusstream.delay.protocol.ShardId shardId,
                              List<WorkClassTask> completedTasks) {
        public CommandTurn {
            Objects.requireNonNull(shardId, "shardId");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
        }
    }
}
