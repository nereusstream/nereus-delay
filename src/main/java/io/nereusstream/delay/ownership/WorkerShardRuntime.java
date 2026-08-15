package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.ScheduleWorkItem;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ClaimRecord;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.CheckpointScheduler;
import io.nereusstream.delay.store.CheckpointWorkClassExecutor;
import io.nereusstream.delay.store.WorkerCheckpointRuntime;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Production composition for one owned Worker shard.
 *
 * <p>The runtime is deliberately narrow: it binds the broker-neutral source
 * apply loop to the owner drain coordinator and the process-wide resource
 * gate.  It does not create a Kafka/Pulsar client or an Oxia authority.  The
 * adapters provide those boundaries through {@link SourceRecordConsumer} and
 * {@link OxiaOwnerLeaseStore}.</p>
 *
 * <p>A source record retained after apply/ACK uncertainty blocks drain until
 * the same record is retried and ACKed.  Once drain starts, source turns are
 * fenced; a checkpoint that is still queued keeps the runtime retryable.  The
 * source is closed only after the owner drain has closed the Store and
 * released the matching lease.</p>
 */
public final class WorkerShardRuntime implements AutoCloseable {
    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final SharedRocksDbResources resources;
    private final WorkerSourceApplyLoop sourceLoop;
    private final OwnerDrainCoordinator drainCoordinator;
    private final WorkerSchedulingRuntime schedulingRuntime;
    private final WorkerCommandRuntime commandRuntime;
    private final WorkerCheckpointRuntime checkpointRuntime;
    private boolean sourcePaused;
    private boolean terminal;

    /** Creates the source/apply/drain composition for one owned shard. */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey) {
        this(sourceConsumer, workClasses, ownedShard, store, resources, authority, verificationKey, null);
    }

    /**
     * Creates the source/apply/drain composition with an active-owner
     * scheduling graph.  The graph shares the same WorkClass registry and is
     * fenced by this runtime when owner drain stops source admission.
     */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey,
                              final WorkerSchedulingRuntime schedulingRuntime) {
        this(sourceConsumer, workClasses, ownedShard, store, resources, authority, verificationKey,
                schedulingRuntime, null);
    }

    /** Creates the source/scheduling/Claim/Publish composition for one Worker shard. */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey,
                              final WorkerSchedulingRuntime schedulingRuntime,
                              final WorkerCommandRuntime commandRuntime) {
        this(sourceConsumer, workClasses, ownedShard, store, resources, authority, verificationKey,
                schedulingRuntime, commandRuntime, null);
    }

    /** Creates the source/scheduling/Claim/Publish/checkpoint composition for one Worker shard. */
    public WorkerShardRuntime(final SourceRecordConsumer sourceConsumer,
                              final WorkClassExecutionRegistry workClasses,
                              final OwnedDelayShard ownedShard,
                              final ShardStore store,
                              final SharedRocksDbResources resources,
                              final OxiaOwnerLeaseStore authority,
                              final PublicKey verificationKey,
                              final WorkerSchedulingRuntime schedulingRuntime,
                              final WorkerCommandRuntime commandRuntime,
                              final WorkerCheckpointRuntime checkpointRuntime) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.resources.bindWorkClassExecutionRegistry(this.workClasses);
        if (schedulingRuntime != null) {
            schedulingRuntime.requireWorkClassExecutionRegistry(this.workClasses);
        }
        if (commandRuntime != null) {
            commandRuntime.requireWorkClassExecutionRegistry(this.workClasses);
        }
        if (checkpointRuntime != null) {
            checkpointRuntime.requireWorkerComposition(this.workClasses, store, this.resources);
        }
        this.schedulingRuntime = schedulingRuntime;
        this.commandRuntime = commandRuntime;
        this.checkpointRuntime = checkpointRuntime;
        this.sourceLoop = new WorkerSourceApplyLoop(sourceConsumer, this.workClasses, this.ownedShard, authority,
                verificationKey);
        this.drainCoordinator = new OwnerDrainCoordinator(this.ownedShard, store, resources, authority,
                this.workClasses);
    }

    /** Returns the independently stored Shard identity owned by this runtime. */
    public io.nereusstream.delay.protocol.ShardId shardId() {
        return ownedShard.shard().shardId();
    }

    /** Whether this runtime has an active-owner scheduling graph attached. */
    boolean hasSchedulingRuntime() {
        return schedulingRuntime != null;
    }

    /** Whether this runtime has a Claim/Publish command graph attached. */
    boolean hasCommandRuntime() {
        return commandRuntime != null;
    }

    /** Whether this runtime has a recurring checkpoint graph attached. */
    boolean hasCheckpointRuntime() {
        return checkpointRuntime != null;
    }

    /** Validates process-wide graph/resource identity before fleet admission. */
    void requireFleetComposition(final WorkClassExecutionRegistry expectedWorkClasses,
                                 final SharedRocksDbResources expectedResources) {
        if (workClasses != Objects.requireNonNull(expectedWorkClasses, "expectedWorkClasses")) {
            throw new IllegalArgumentException("Worker shard runtime uses another work-class registry");
        }
        if (resources != Objects.requireNonNull(expectedResources, "expectedResources")) {
            throw new IllegalArgumentException("Worker shard runtime uses another resource envelope");
        }
    }

    /** Runs one bounded source turn while the Worker runtime is admitting work. */
    public synchronized SourceApplyCoordinator.TurnResult runSourceTurn(final SchedulerBudget workBudget,
                                                                          final LongSupplier ownerClock) {
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return sourceLoop.runTurn(workBudget, ownerClock);
    }

    /** Runs one bounded due/Claim/Publish/Checkpoint turn on the shared graph. */
    public synchronized List<WorkClassTask> runSchedulingTurn(final SchedulerBudget workBudget) {
        ensureSchedulingRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return schedulingRuntime.runTurn(workBudget);
    }

    /** Runs one due discovery action while source and scheduling admission remain open. */
    public synchronized WorkerSchedulingRuntime.DueTurn runDueTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final LongSupplier ownerClock) {
        ensureSchedulingRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return schedulingRuntime.runDueTurn(evidence, discoveryBudget, ownerClock);
    }

    /** Polls one strict READY candidate while the owner is still admitting work. */
    public synchronized List<ScheduleWorkItem> pollReady(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final LongSupplier ownerClock) {
        ensureSchedulingRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return schedulingRuntime.pollReady(evidence, budget, ownerClock);
    }

    /**
     * Polls at most one READY head and queues its derived-materialization
     * Claim handoff. The single-head bound preserves the scheduler's exact
     * requeue contract if Claim queue admission is rejected.
     */
    public synchronized Optional<ClaimHandoffWorkClassExecutor.Submission> pollAndSubmitClaim(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget budget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        ensureSchedulingRuntime();
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        final SchedulerBudget oneHead = new SchedulerBudget(1,
                Objects.requireNonNull(budget, "Claim poll budget").maxBytes(), budget.maxElapsedNanos());
        final List<ScheduleWorkItem> selected = schedulingRuntime.pollReady(
                Objects.requireNonNull(evidence, "trusted UTC evidence"), oneHead, ownerClock);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        if (selected.size() != 1) {
            throw new IllegalStateException("single-head Claim poll returned multiple READY items");
        }
        return Optional.of(commandRuntime.submitClaim(selected.get(0), evidence, claimDeadlineEpochMs,
                claimedCharge, ownerClock));
    }

    /**
     * Runs one bounded due-discovery action and queues at most one derived
     * Claim from the resulting READY ring.  The Claim action is returned
     * unexecuted so the caller can schedule its normal command turn through
     * the shared WorkClass registry.
     */
    public synchronized DueClaimTurn runDueAndSubmitClaim(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        final WorkerSchedulingRuntime.DueTurn due = runDueTurn(evidence, discoveryBudget, ownerClock);
        final Optional<ClaimHandoffWorkClassExecutor.Submission> claim = pollAndSubmitClaim(
                evidence, discoveryBudget, claimDeadlineEpochMs, claimedCharge, ownerClock);
        return new DueClaimTurn(due, claim);
    }

    /**
     * Runs a bounded due-to-Claim-to-Publish composition on the shared Worker
     * graph.  The exact Claim and Publish tasks are observed through bounded
     * fair command turns rather than by reaching into an executor's private
     * action.  An empty preparation result means that the external live
     * authority is not ready yet; the successful Claim and its active
     * reservation are returned for an evidence-driven retry or revoke.
     */
    public synchronized DueClaimPublishTurn runDueClaimPublishTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock,
            final SchedulerBudget commandBudget,
            final int maxCommandTurns,
            final PublishPreparationProvider preparationProvider) {
        ensureSchedulingRuntime();
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        final SchedulerBudget exactCommandBudget = Objects.requireNonNull(commandBudget, "command budget");
        if (maxCommandTurns <= 0) {
            throw new IllegalArgumentException("maxCommandTurns must be positive");
        }
        final PublishPreparationProvider provider = Objects.requireNonNull(preparationProvider,
                "preparation provider");
        final DueClaimTurn dueClaim = runDueAndSubmitClaim(evidence, discoveryBudget, claimDeadlineEpochMs,
                claimedCharge, ownerClock);
        if (dueClaim.claimSubmission().isEmpty()) {
            return new DueClaimPublishTurn(dueClaim, List.of(), Optional.empty(), Optional.empty(), List.of());
        }

        final ClaimHandoffWorkClassExecutor.Submission claimSubmission = dueClaim.claimSubmission().orElseThrow();
        final List<WorkClassTask> claimCompletedTasks = runCommandTurnsUntilCompleted(claimSubmission.task(),
                exactCommandBudget, maxCommandTurns);
        final ClaimHandoffWorkClassExecutor.ClaimHandoffResult claimResult = claimSubmission.result()
                .orElseThrow(() -> new IllegalStateException("Claim task completed without a result"));
        final Optional<ClaimHandoffWorkClassExecutor.ClaimHandoffResult> completedClaim = Optional.of(claimResult);
        if (claimResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            return new DueClaimPublishTurn(dueClaim, claimCompletedTasks, completedClaim, Optional.empty(),
                    List.of());
        }

        final Optional<WorkerCommandRuntime.PublishPreparation> preparation;
        try {
            preparation = Objects.requireNonNull(provider.prepare(claimResult), "Publish preparation result");
        } catch (RuntimeException | Error failure) {
            // A live prerequisite authority that throws has not proved that
            // the Claim can safely continue.  Fence this owner and retain
            // the exact Claim/reservation for evidence-driven recovery.
            ownedShard.fence();
            throw failure;
        }
        if (preparation.isEmpty()) {
            return new DueClaimPublishTurn(dueClaim, claimCompletedTasks, completedClaim, Optional.empty(),
                    List.of());
        }
        final PublishAdmissionWorkClassExecutor.Submission publishSubmission = submitPublish(claimResult,
                preparation.orElseThrow());
        final List<WorkClassTask> publishCompletedTasks = runCommandTurnsUntilCompleted(publishSubmission.task(),
                exactCommandBudget, maxCommandTurns);
        return new DueClaimPublishTurn(dueClaim, claimCompletedTasks, completedClaim,
                Optional.of(publishSubmission), publishCompletedTasks);
    }

    private List<WorkClassTask> runCommandTurnsUntilCompleted(final WorkClassTask target,
                                                               final SchedulerBudget commandBudget,
                                                               final int maxCommandTurns) {
        final List<WorkClassTask> completedTasks = new ArrayList<>();
        for (int turn = 0; turn < maxCommandTurns; turn++) {
            final Optional<WorkClassExecutionRegistry.ExecutionState> state = workClasses.state(target);
            if (state.isEmpty()) {
                return List.copyOf(completedTasks);
            }
            if (state.orElseThrow() == WorkClassExecutionRegistry.ExecutionState.FAILED) {
                throw new IllegalStateException("exact Worker command task failed: " + target.taskId());
            }
            final List<WorkClassTask> completedThisTurn = runCommandTurn(commandBudget);
            completedTasks.addAll(completedThisTurn);
            if (completedThisTurn.isEmpty() && workClasses.state(target).isPresent()) {
                throw new IllegalStateException("exact Worker command task made no bounded progress: "
                        + target.taskId());
            }
        }
        if (workClasses.state(target).isPresent()) {
            throw new IllegalStateException("exact Worker command task exceeded command-turn bound: "
                    + target.taskId());
        }
        return List.copyOf(completedTasks);
    }

    /** Queues one exact Claim handoff while source/command admission is open. */
    public synchronized ClaimHandoffWorkClassExecutor.Submission submitClaim(
            final WorkerCommandRuntime.ClaimRequest request) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitClaim(request);
    }

    /** Queues a Claim handoff whose V1 materialization is derived locally. */
    public synchronized ClaimHandoffWorkClassExecutor.Submission submitClaim(
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitClaim(item, evidence, claimDeadlineEpochMs, claimedCharge, ownerClock);
    }

    /** Queues one exact Publish Admission handoff while source/command admission is open. */
    public synchronized PublishAdmissionWorkClassExecutor.Submission submitPublish(
            final WorkerCommandRuntime.PublishRequest request) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitPublish(request);
    }

    /** Queues Publish Admission with a descriptor derived from the Claim. */
    public synchronized PublishAdmissionWorkClassExecutor.Submission submitPublish(
            final ClaimRecord claim,
            final ClaimExecutionAdmission.Reservation reservation,
            final ChannelResourceIdentityV1 channel,
            final ReadyCertificateV1 readyCertificate,
            final TrustedUtcIntervalEvidence decisionTime,
            final long retryUntilEpochMs,
            final int signingKeyVersion,
            final PrivateKey signingKey,
            final LongSupplier ownerClock) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitPublish(claim, reservation, channel, readyCertificate, decisionTime,
                retryUntilEpochMs, signingKeyVersion, signingKey, ownerClock);
    }

    /** Queues Publish Admission from one exact successful Claim handoff. */
    public synchronized PublishAdmissionWorkClassExecutor.Submission submitPublish(
            final ClaimHandoffWorkClassExecutor.ClaimHandoffResult claimResult,
            final WorkerCommandRuntime.PublishPreparation preparation) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitPublish(claimResult, preparation);
    }

    /** Runs one bounded Claim/Publish turn through the shared Worker graph. */
    public synchronized List<WorkClassTask> runCommandTurn(final SchedulerBudget budget) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.runTurn(budget);
    }

    /** Registers this shard in the recurring checkpoint schedule. */
    public synchronized long registerCheckpoint(final long nowEpochMs) {
        ensureCheckpointRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return checkpointRuntime.register(shardId(), nowEpochMs);
    }

    /** Claims exact checkpoint handles for this shard's recurring schedule. */
    public synchronized List<CheckpointScheduler.ScheduledCheckpoint> claimDueCheckpoints(
            final long nowEpochMs, final int limit) {
        ensureCheckpointRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return checkpointRuntime.claimDue(nowEpochMs, limit);
    }

    /** Claims one exact recurring checkpoint and queues its immutable request. */
    public synchronized Optional<CheckpointWorkClassExecutor.Submission> claimDueAndSubmitCheckpoint(
            final long nowEpochMs,
            final WorkerCheckpointRuntime.ExecutionRequestFactory requestFactory,
            final LongSupplier completionClock) {
        ensureCheckpointRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return checkpointRuntime.claimDueAndSubmit(nowEpochMs, requestFactory, completionClock);
    }

    /** Queues one exact recurring checkpoint attempt behind the Worker fence. */
    public synchronized CheckpointWorkClassExecutor.Submission submitCheckpoint(
            final CheckpointWorkClassExecutor.ExecutionRequest request) {
        ensureCheckpointRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return checkpointRuntime.submit(request);
    }

    /** Runs one bounded recurring checkpoint turn on the shared WorkClass graph. */
    public synchronized List<WorkClassTask> runCheckpointTurn(final SchedulerBudget budget) {
        ensureCheckpointRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return checkpointRuntime.runTurn(budget);
    }

    /** Returns the exact source entry retained across an ACK/apply boundary. */
    public synchronized Optional<SourceReplayEntry> pendingSourceEntry() {
        return sourceLoop.pendingEntry();
    }

    /** Returns whether the drain path has fenced new source turns. */
    public synchronized boolean sourcePaused() {
        return sourcePaused;
    }

    /**
     * Runs or retries the owner drain.  A pending source ACK is rejected
     * before the drain coordinator can transition the authoritative lease, so
     * the caller can retry the same source record without deadlocking the
     * source lifecycle.
     */
    public synchronized OwnerDrainCoordinator.DrainResult drain(
            final OwnerDrainCoordinator.DrainRequest request,
            final LongSupplier clock,
            final OwnerDrainCoordinator.DrainCallbacks callbacks) {
        ensureNotTerminal();
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(clock, "clock");
        final OwnerDrainCoordinator.DrainCallbacks delegate = Objects.requireNonNull(callbacks, "callbacks");
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("cannot drain with a pending source acknowledgement");
        }
        if (checkpointRuntime != null) {
            checkpointRuntime.prepareForDrain();
        }
        final OwnerDrainCoordinator.DrainResult result = drainCoordinator.drain(request, clock,
                new OwnerDrainCoordinator.DrainCallbacks() {
                    @Override
                    public void stopSourceAndScheduling() {
                        sourcePaused = true;
                        delegate.stopSourceAndScheduling();
                    }

                    @Override
                    public void pollOpenPublishAttempts(final List<io.nereusstream.delay.runtime.PublishAttemptLedger>
                                                                 openAttempts,
                                                         final int pollNumber) {
                        delegate.pollOpenPublishAttempts(openAttempts, pollNumber);
                    }

                    @Override
                    public void commitSourceHint(final io.nereusstream.delay.protocol.SourcePosition
                                                         persistedPosition) {
                        delegate.commitSourceHint(persistedPosition);
                    }
                });
        if (result.pendingCheckpointTask() == null) {
            // The precondition above proves that close cannot discard an
            // unacknowledged source record.  OwnerDrainCoordinator has also
            // completed Store close and exact lease release at this point.
            sourceLoop.close();
            terminal = true;
        }
        return result;
    }

    /**
     * Refuses to tear down an active owner.  Production shutdown must use the
     * same drain path so source, Store and lease boundaries remain ordered.
     */
    @Override
    public synchronized void close() {
        if (terminal) {
            return;
        }
        throw new IllegalStateException("Worker shard runtime must complete owner drain before close");
    }

    private void ensureSourceRunning() {
        ensureNotTerminal();
        if (sourcePaused) {
            throw new IllegalStateException("Worker source is paused for owner drain");
        }
    }

    private void ensureNotTerminal() {
        if (terminal) {
            throw new IllegalStateException("Worker shard runtime is closed");
        }
    }

    private void ensureSchedulingRuntime() {
        ensureNotTerminal();
        if (schedulingRuntime == null) {
            throw new IllegalStateException("Worker shard runtime has no active-owner scheduling graph");
        }
    }

    private void ensureCommandRuntime() {
        ensureNotTerminal();
        if (commandRuntime == null) {
            throw new IllegalStateException("Worker shard runtime has no Claim/Publish command graph");
        }
    }

    private void ensureCheckpointRuntime() {
        ensureNotTerminal();
        if (checkpointRuntime == null) {
            throw new IllegalStateException("Worker shard runtime has no recurring checkpoint graph");
        }
    }

    /** Evidence from one combined due-discovery to derived-Claim handoff. */
    public record DueClaimTurn(WorkerSchedulingRuntime.DueTurn dueTurn,
                               Optional<ClaimHandoffWorkClassExecutor.Submission> claimSubmission) {
        public DueClaimTurn {
            Objects.requireNonNull(dueTurn, "dueTurn");
            claimSubmission = Objects.requireNonNull(claimSubmission, "claimSubmission");
        }
    }

    /** External authority that prepares immutable Publish inputs after Claim success. */
    @FunctionalInterface
    public interface PublishPreparationProvider {
        Optional<WorkerCommandRuntime.PublishPreparation> prepare(
                ClaimHandoffWorkClassExecutor.ClaimHandoffResult claimResult);
    }

    /** Results from one bounded due-to-Claim-to-Publish Worker composition. */
    public record DueClaimPublishTurn(
            DueClaimTurn dueClaimTurn,
            List<WorkClassTask> claimCompletedTasks,
            Optional<ClaimHandoffWorkClassExecutor.ClaimHandoffResult> claimResult,
            Optional<PublishAdmissionWorkClassExecutor.Submission> publishSubmission,
            List<WorkClassTask> publishCompletedTasks) {
        public DueClaimPublishTurn {
            Objects.requireNonNull(dueClaimTurn, "dueClaimTurn");
            claimCompletedTasks = List.copyOf(Objects.requireNonNull(claimCompletedTasks,
                    "claimCompletedTasks"));
            claimResult = Objects.requireNonNull(claimResult, "claimResult");
            publishSubmission = Objects.requireNonNull(publishSubmission, "publishSubmission");
            publishCompletedTasks = List.copyOf(Objects.requireNonNull(publishCompletedTasks,
                    "publishCompletedTasks"));
        }
    }
}
