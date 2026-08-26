package com.nereusstream.delay.store;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Worker-side composition for scheduled checkpoint publication.
 *
 * <p>The caller creates the exact pending upload intent and supplies the
 * Owner/session/intent prerequisite gate, manifest factory and upload adapter.
 * This class owns only the process-local schedule claim, bounded CHECKPOINT
 * queue handoff and retryable execution identity. It never infers a catalog
 * generation, object-store resource or Owner lease from local Store bytes.</p>
 */
public final class WorkerCheckpointRuntime {
    private final WorkClassExecutionRegistry workClasses;
    private final ShardStore store;
    private final SharedRocksDbResources resources;
    private final CheckpointScheduler scheduler;
    private final CheckpointWorkClassExecutor executor;

    public WorkerCheckpointRuntime(
            final WorkClassExecutionRegistry workClasses,
            final CheckpointScheduler scheduler,
            final ShardStore store,
            final CheckpointPublicationCoordinator publicationCoordinator,
            final CheckpointWorkClassExecutor.CheckpointPrerequisiteGate prerequisiteGate) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.store = Objects.requireNonNull(store, "store");
        final CheckpointPublicationCoordinator publication =
                Objects.requireNonNull(publicationCoordinator, "publicationCoordinator");
        this.resources = this.store.sharedResources();
        final CheckpointExecutionCoordinator execution =
                new CheckpointExecutionCoordinator(this.scheduler, this.store, publication);
        this.executor = new CheckpointWorkClassExecutor(
                this.workClasses, execution, Objects.requireNonNull(prerequisiteGate, "prerequisiteGate"));
    }

    /** Binds this recurring-checkpoint graph to the owning Worker composition. */
    public void requireWorkerComposition(
            final WorkClassExecutionRegistry expectedWorkClasses,
            final ShardStore expectedStore,
            final SharedRocksDbResources expectedResources) {
        if (workClasses != Objects.requireNonNull(expectedWorkClasses, "expectedWorkClasses")) {
            throw new IllegalArgumentException("checkpoint runtime uses another work-class registry");
        }
        if (store != Objects.requireNonNull(expectedStore, "expectedStore")) {
            throw new IllegalArgumentException("checkpoint runtime uses another Store");
        }
        if (resources != Objects.requireNonNull(expectedResources, "expectedResources")) {
            throw new IllegalArgumentException("checkpoint runtime uses another resource envelope");
        }
    }

    /** Registers one shard in the process-local due schedule. */
    public long register(final com.nereusstream.delay.protocol.ShardId shardId, final long nowEpochMs) {
        final com.nereusstream.delay.protocol.ShardId requested = Objects.requireNonNull(shardId, "shardId");
        requireStoreShard(requested);
        return scheduler.register(requested, nowEpochMs);
    }

    /** Claims at most {@code limit} exact handles for this Store's shard. */
    public List<CheckpointScheduler.ScheduledCheckpoint> claimDue(final long nowEpochMs, final int limit) {
        return scheduler.claimDueForShard(store.shardId(), nowEpochMs, limit);
    }

    /**
     * Claims at most one exact handle and builds its immutable execution
     * request before queue admission. A request-factory failure has not
     * started checkpoint I/O, so this method completes the same capability
     * immediately and leaves the schedule retryable. Once {@link #submit}
     * is entered, its existing preflight and queue-rejection ownership rules
     * remain authoritative.
     */
    public Optional<CheckpointWorkClassExecutor.Submission> claimDueAndSubmit(
            final long nowEpochMs, final ExecutionRequestFactory requestFactory, final LongSupplier completionClock) {
        final List<CheckpointScheduler.ScheduledCheckpoint> claimed = claimDue(nowEpochMs, 1);
        if (claimed.isEmpty()) {
            return Optional.empty();
        }
        final CheckpointScheduler.ScheduledCheckpoint exact = claimed.get(0);
        final CheckpointWorkClassExecutor.ExecutionRequest request;
        try {
            request = Objects.requireNonNull(requestFactory, "requestFactory").create(exact);
            if (request.claim() != exact) {
                throw new IllegalArgumentException("checkpoint request must retain the exact claimed handle");
            }
        } catch (RuntimeException | Error failure) {
            try {
                executor.completeWithoutExecution(exact, Objects.requireNonNull(completionClock, "completionClock"));
            } catch (RuntimeException | Error releaseFailure) {
                if (releaseFailure != failure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        }
        return Optional.of(submit(request));
    }

    /** Removes this Store's idle recurring schedule before Owner drain begins. */
    public void prepareForDrain() {
        final com.nereusstream.delay.protocol.ShardId shardId = store.shardId();
        if (!scheduler.isRegistered(shardId)) {
            return;
        }
        if (scheduler.isInFlight(shardId)) {
            throw new IllegalStateException("checkpoint schedule has an in-flight claim for Store shard");
        }
        scheduler.unregister(shardId);
    }

    /** Queues one exact checkpoint attempt after prerequisite preflight. */
    public CheckpointWorkClassExecutor.Submission submit(final CheckpointWorkClassExecutor.ExecutionRequest request) {
        return executor.submit(Objects.requireNonNull(request, "request"));
    }

    /** Runs one bounded shared Worker turn containing the queued checkpoint action. */
    public List<WorkClassTask> runTurn(final SchedulerBudget budget) {
        resources.requireRuntimeBusinessAdmission();
        return workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    public CheckpointScheduler scheduler() {
        return scheduler;
    }

    /** Builds one immutable request from the exact scheduler capability. */
    @FunctionalInterface
    public interface ExecutionRequestFactory {
        CheckpointWorkClassExecutor.ExecutionRequest create(CheckpointScheduler.ScheduledCheckpoint claim);
    }

    private void requireStoreShard(final com.nereusstream.delay.protocol.ShardId requested) {
        if (!store.shardId().equals(requested)) {
            throw new IllegalArgumentException("checkpoint runtime cannot schedule another Store shard");
        }
    }
}
