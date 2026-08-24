package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.SharedRocksDbResources;
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
    private int checkpointCursor;
    private boolean closed;

    public WorkerShardFleetRuntime(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<WorkerShardRuntime> shardRuntimes) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.resources = Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(shardRuntimes, "shardRuntimes");
        if (shardRuntimes.isEmpty()) {
            throw new IllegalArgumentException("Worker shard fleet must contain at least one shard");
        }
        final Set<com.nereusstream.delay.protocol.ShardId> identities = new HashSet<>();
        final List<WorkerShardRuntime> admitted = new ArrayList<>(shardRuntimes.size());
        for (WorkerShardRuntime runtime : shardRuntimes) {
            final WorkerShardRuntime candidate = Objects.requireNonNull(runtime, "shard runtime");
            candidate.requireFleetComposition(this.workClasses, this.resources);
            if (!identities.add(candidate.shardId())) {
                throw new IllegalArgumentException(
                        "Worker shard fleet contains a duplicate shard: " + candidate.shardId());
            }
            admitted.add(candidate);
        }
        this.shards = List.copyOf(admitted);
    }

    /** Returns the immutable admission order used as the deterministic tie-breaker. */
    public synchronized List<com.nereusstream.delay.protocol.ShardId> shardIds() {
        return shards.stream().map(WorkerShardRuntime::shardId).toList();
    }

    /** Runs one source turn on the next shard in round-robin order. */
    public synchronized SourceTurn runNextSourceTurn(final SchedulerBudget budget, final LongSupplier ownerClock) {
        ensureOpen();
        final int index = sourceCursor;
        sourceCursor = (sourceCursor + 1) % shards.size();
        final WorkerShardRuntime runtime = shards.get(index);
        return new SourceTurn(
                runtime.shardId(),
                runtime.runSourceTurn(
                        Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock")));
    }

    /**
     * Runs one scheduling turn on the next shard that has an active-owner
     * scheduling graph. Empty means this fleet was built from source-only
     * runtimes and therefore has no scheduling action to dispatch.
     */
    public synchronized Optional<SchedulingTurn> runNextSchedulingTurn(final SchedulerBudget budget) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(true, false, false);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(new SchedulingTurn(
                runtime.shardId(), runtime.runSchedulingTurn(Objects.requireNonNull(budget, "budget"))));
    }

    /** Runs one Claim/Publish command turn on the next command-enabled shard. */
    public synchronized Optional<CommandTurn> runNextCommandTurn(final SchedulerBudget budget) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(false, true, false);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(
                new CommandTurn(runtime.shardId(), runtime.runCommandTurn(Objects.requireNonNull(budget, "budget"))));
    }

    /**
     * Runs one bounded due-to-Claim-to-Publish turn on the next shard that has
     * both scheduling and command graphs. The shard runtime owns the typed
     * Lane/provider fence; this fleet method only adds fair multi-shard
     * dispatch and never accepts a replacement provider from the caller.
     */
    public synchronized Optional<DueClaimPublishTurn> runNextDueClaimPublishTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock,
            final SchedulerBudget commandBudget,
            final int maxCommandTurns) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(true, true, false);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(new DueClaimPublishTurn(
                runtime.shardId(),
                runtime.runDueClaimPublishTurn(
                        Objects.requireNonNull(evidence, "trusted UTC evidence"),
                        Objects.requireNonNull(discoveryBudget, "discovery budget"),
                        claimDeadlineEpochMs,
                        Objects.requireNonNull(claimedCharge, "claimed charge"),
                        Objects.requireNonNull(ownerClock, "owner clock"),
                        Objects.requireNonNull(commandBudget, "command budget"),
                        maxCommandTurns)));
    }

    /**
     * Runs the next fair shard through due/Claim/Publish, source application
     * of the exact Admission append and the bounded physical publish bridge.
     * Only runtimes with all three graphs are selected; payload resolution
     * remains an explicit external authority.
     */
    public synchronized Optional<DueClaimPublishPhysicalTurn> runNextDueClaimPublishPhysicalTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock,
            final SchedulerBudget commandBudget,
            final int maxCommandTurns,
            final SchedulerBudget sourceBudget,
            final int maxSourceTurns,
            final WorkerShardRuntime.PublishPayloadProvider payloadProvider) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(true, true, false, true);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(new DueClaimPublishPhysicalTurn(
                runtime.shardId(),
                runtime.runDueClaimPublishPhysicalTurn(
                        Objects.requireNonNull(evidence, "trusted UTC evidence"),
                        Objects.requireNonNull(discoveryBudget, "discovery budget"),
                        claimDeadlineEpochMs,
                        Objects.requireNonNull(claimedCharge, "claimed charge"),
                        Objects.requireNonNull(ownerClock, "owner clock"),
                        Objects.requireNonNull(commandBudget, "command budget"),
                        maxCommandTurns,
                        Objects.requireNonNull(sourceBudget, "source budget"),
                        maxSourceTurns,
                        Objects.requireNonNull(payloadProvider, "payload provider"))));
    }

    /** Runs one recurring checkpoint turn on the next checkpoint-enabled shard. */
    public synchronized Optional<CheckpointTurn> runNextCheckpointTurn(final SchedulerBudget budget) {
        ensureOpen();
        final Optional<IndexedRuntime> selected = nextRuntime(false, false, true);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        final WorkerShardRuntime runtime = selected.get().runtime();
        return Optional.of(new CheckpointTurn(
                runtime.shardId(), runtime.runCheckpointTurn(Objects.requireNonNull(budget, "budget"))));
    }

    /** Refuses to bypass per-shard owner drain and source-loop closure. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        Throwable closeFailure = null;
        for (WorkerShardRuntime runtime : shards) {
            try {
                runtime.close();
            } catch (RuntimeException | Error failure) {
                // Every admitted shard must get a close attempt.  A shard
                // that still needs an owner-drain retry must not prevent a
                // later shard from releasing its Store/source resources.
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (closeFailure != null) {
            throwUnchecked(closeFailure);
        }
        closed = true;
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    private Optional<IndexedRuntime> nextRuntime(
            final boolean scheduling, final boolean command, final boolean checkpoint) {
        return nextRuntime(scheduling, command, checkpoint, false);
    }

    private Optional<IndexedRuntime> nextRuntime(
            final boolean scheduling, final boolean command, final boolean checkpoint, final boolean physical) {
        int cursor = scheduling ? schedulingCursor : command ? commandCursor : checkpointCursor;
        for (int offset = 0; offset < shards.size(); offset++) {
            final int index = (cursor + offset) % shards.size();
            final WorkerShardRuntime runtime = shards.get(index);
            if ((!scheduling || runtime.hasSchedulingRuntime())
                    && (!command || runtime.hasCommandRuntime())
                    && (!checkpoint || runtime.hasCheckpointRuntime())
                    && (!physical || runtime.hasPhysicalPublishExecutor())) {
                if (scheduling) {
                    schedulingCursor = (index + 1) % shards.size();
                } else if (command) {
                    commandCursor = (index + 1) % shards.size();
                } else {
                    checkpointCursor = (index + 1) % shards.size();
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

    private record IndexedRuntime(int index, WorkerShardRuntime runtime) {}

    public record SourceTurn(
            com.nereusstream.delay.protocol.ShardId shardId, SourceApplyCoordinator.TurnResult result) {
        public SourceTurn {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(result, "result");
        }
    }

    public record SchedulingTurn(com.nereusstream.delay.protocol.ShardId shardId, List<WorkClassTask> completedTasks) {
        public SchedulingTurn {
            Objects.requireNonNull(shardId, "shardId");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
        }
    }

    public record CommandTurn(com.nereusstream.delay.protocol.ShardId shardId, List<WorkClassTask> completedTasks) {
        public CommandTurn {
            Objects.requireNonNull(shardId, "shardId");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
        }
    }

    /** Result of one fair fleet dispatch to a shard's bounded Worker turn. */
    public record DueClaimPublishTurn(
            com.nereusstream.delay.protocol.ShardId shardId, WorkerShardRuntime.DueClaimPublishTurn result) {
        public DueClaimPublishTurn {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(result, "result");
        }
    }

    /** Result of one fair fleet dispatch through the source-bound physical path. */
    public record DueClaimPublishPhysicalTurn(
            com.nereusstream.delay.protocol.ShardId shardId, WorkerShardRuntime.DueClaimPublishPhysicalTurn result) {
        public DueClaimPublishPhysicalTurn {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(result, "result");
        }
    }

    public record CheckpointTurn(com.nereusstream.delay.protocol.ShardId shardId, List<WorkClassTask> completedTasks) {
        public CheckpointTurn {
            Objects.requireNonNull(shardId, "shardId");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
        }
    }
}
