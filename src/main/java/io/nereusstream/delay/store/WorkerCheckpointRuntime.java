package io.nereusstream.delay.store;

import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.util.List;
import java.util.Objects;

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
    private final CheckpointScheduler scheduler;
    private final CheckpointWorkClassExecutor executor;

    public WorkerCheckpointRuntime(final WorkClassExecutionRegistry workClasses,
                                   final CheckpointScheduler scheduler,
                                   final ShardStore store,
                                   final CheckpointPublicationCoordinator publicationCoordinator,
                                   final CheckpointWorkClassExecutor.CheckpointPrerequisiteGate prerequisiteGate) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        final CheckpointExecutionCoordinator execution = new CheckpointExecutionCoordinator(
                this.scheduler, Objects.requireNonNull(store, "store"),
                Objects.requireNonNull(publicationCoordinator, "publicationCoordinator"));
        this.executor = new CheckpointWorkClassExecutor(this.workClasses, execution,
                Objects.requireNonNull(prerequisiteGate, "prerequisiteGate"));
    }

    /** Registers one shard in the process-local due schedule. */
    public long register(final io.nereusstream.delay.protocol.ShardId shardId, final long nowEpochMs) {
        return scheduler.register(Objects.requireNonNull(shardId, "shardId"), nowEpochMs);
    }

    /** Claims at most {@code limit} exact checkpoint schedule handles. */
    public List<CheckpointScheduler.ScheduledCheckpoint> claimDue(final long nowEpochMs, final int limit) {
        return scheduler.claimDue(nowEpochMs, limit);
    }

    /** Queues one exact checkpoint attempt after prerequisite preflight. */
    public CheckpointWorkClassExecutor.Submission submit(
            final CheckpointWorkClassExecutor.ExecutionRequest request) {
        return executor.submit(Objects.requireNonNull(request, "request"));
    }

    /** Runs one bounded shared Worker turn containing the queued checkpoint action. */
    public List<WorkClassTask> runTurn(final SchedulerBudget budget) {
        return workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    public CheckpointScheduler scheduler() {
        return scheduler;
    }
}
