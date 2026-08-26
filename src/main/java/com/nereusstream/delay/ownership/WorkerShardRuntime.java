package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import com.nereusstream.delay.scheduler.ScheduleWorkItem;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.CheckpointScheduler;
import com.nereusstream.delay.store.CheckpointWorkClassExecutor;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerCheckpointRuntime;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Production composition for one owned Worker shard.
 *
 * <p>The runtime is deliberately narrow: it binds the broker-neutral source
 * apply loop to the owner drain coordinator and the process-wide resource
 * gate. It does not create a Kafka/Pulsar client or an Oxia authority. The
 * adapters provide those boundaries through {@link SourceRecordConsumer} and
 * {@link OxiaOwnerLeaseStore}.</p>
 *
 * <p>A source record retained after apply/ACK uncertainty blocks drain until
 * the same record is retried and ACKed. Once drain starts, source turns are
 * fenced; a checkpoint that is still queued keeps the runtime retryable. The
 * source is closed only after the owner drain has closed the Store and
 * released the matching lease.</p>
 */
public final class WorkerShardRuntime implements AutoCloseable {
    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final SharedRocksDbResources resources;
    private final WorkerSourceApplyLoop sourceLoop;
    private final OwnerDrainCoordinator drainCoordinator;
    private WorkerSchedulingRuntime schedulingRuntime;
    private WorkerCommandRuntime commandRuntime;
    private final WorkerCheckpointRuntime checkpointRuntime;
    private PublishPreparationProvider preparationProvider;
    private WorkerPhysicalPublishExecutor physicalPublishExecutor;
    private boolean sourcePaused;
    private boolean terminal;

    /** Creates the source/apply/drain composition for one owned shard. */
    public WorkerShardRuntime(
            final SourceRecordConsumer sourceConsumer,
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
     * scheduling graph. The graph shares the same WorkClass registry and is
     * fenced by this runtime when owner drain stops source admission.
     */
    public WorkerShardRuntime(
            final SourceRecordConsumer sourceConsumer,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final WorkerSchedulingRuntime schedulingRuntime) {
        this(
                sourceConsumer,
                workClasses,
                ownedShard,
                store,
                resources,
                authority,
                verificationKey,
                schedulingRuntime,
                null);
    }

    /** Creates the source/scheduling/Claim/Publish composition for one Worker shard. */
    public WorkerShardRuntime(
            final SourceRecordConsumer sourceConsumer,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final WorkerSchedulingRuntime schedulingRuntime,
            final WorkerCommandRuntime commandRuntime) {
        this(
                sourceConsumer,
                workClasses,
                ownedShard,
                store,
                resources,
                authority,
                verificationKey,
                schedulingRuntime,
                commandRuntime,
                null);
    }

    /** Creates the source/scheduling/Claim/Publish/checkpoint composition for one Worker shard. */
    public WorkerShardRuntime(
            final SourceRecordConsumer sourceConsumer,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final WorkerSchedulingRuntime schedulingRuntime,
            final WorkerCommandRuntime commandRuntime,
            final WorkerCheckpointRuntime checkpointRuntime) {
        this(
                sourceConsumer,
                workClasses,
                ownedShard,
                store,
                resources,
                authority,
                verificationKey,
                schedulingRuntime,
                commandRuntime,
                checkpointRuntime,
                null,
                null);
    }

    /** Creates the complete Worker graph with an optionally bound Publish preparation provider. */
    public WorkerShardRuntime(
            final SourceRecordConsumer sourceConsumer,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final WorkerSchedulingRuntime schedulingRuntime,
            final WorkerCommandRuntime commandRuntime,
            final WorkerCheckpointRuntime checkpointRuntime,
            final PublishPreparationProvider preparationProvider) {
        this(
                sourceConsumer,
                workClasses,
                ownedShard,
                store,
                resources,
                authority,
                verificationKey,
                schedulingRuntime,
                commandRuntime,
                checkpointRuntime,
                preparationProvider,
                null);
    }

    /** Creates the complete Worker graph with optional physical publish execution. */
    public WorkerShardRuntime(
            final SourceRecordConsumer sourceConsumer,
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey,
            final WorkerSchedulingRuntime schedulingRuntime,
            final WorkerCommandRuntime commandRuntime,
            final WorkerCheckpointRuntime checkpointRuntime,
            final PublishPreparationProvider preparationProvider,
            final WorkerPhysicalPublishExecutor physicalPublishExecutor) {
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
        this.preparationProvider = preparationProvider;
        this.physicalPublishExecutor = physicalPublishExecutor;
        this.sourceLoop = new WorkerSourceApplyLoop(
                sourceConsumer, this.workClasses, this.ownedShard, authority, verificationKey);
        this.drainCoordinator =
                new OwnerDrainCoordinator(this.ownedShard, store, resources, authority, this.workClasses);
    }

    /** Returns the independently stored Shard identity owned by this runtime. */
    public com.nereusstream.delay.protocol.ShardId shardId() {
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

    /** Whether this runtime has a bounded physical publish bridge attached. */
    boolean hasPhysicalPublishExecutor() {
        return physicalPublishExecutor != null;
    }

    /**
     * Binds the active-owner scheduling and Claim/Publish graph after a
     * source-only bootstrap has applied the Schedule and persisted its typed
     * Lane readiness projection. This is a one-way composition boundary: a
     * runtime cannot replace an already admitted graph or bind a provider
     * without the matching scheduling/command identities.
     */
    public synchronized void bindActiveOwnerPublishGraph(
            final WorkerSchedulingRuntime schedulingRuntime,
            final WorkerCommandRuntime commandRuntime,
            final PublishPreparationProvider preparationProvider) {
        ensureSourceRunning();
        if (this.schedulingRuntime != null || this.commandRuntime != null || this.preparationProvider != null) {
            throw new IllegalStateException("Worker active-owner publish graph is already bound");
        }
        final WorkerSchedulingRuntime exactScheduling = Objects.requireNonNull(schedulingRuntime, "scheduling runtime");
        final WorkerCommandRuntime exactCommand = Objects.requireNonNull(commandRuntime, "command runtime");
        final PublishPreparationProvider exactPreparation =
                Objects.requireNonNull(preparationProvider, "publish preparation provider");
        exactScheduling.requireWorkClassExecutionRegistry(workClasses);
        exactCommand.requireWorkClassExecutionRegistry(workClasses);
        this.schedulingRuntime = exactScheduling;
        this.commandRuntime = exactCommand;
        this.preparationProvider = exactPreparation;
    }

    /**
     * Binds the physical destination executor after source replay has
     * materialized the exact durable Lane identity. A source-only bootstrap
     * may therefore apply Prepare/Commit first instead of guessing a Lane
     * incarnation before the source record is persisted.
     */
    public synchronized void bindPhysicalPublishExecutor(final WorkerPhysicalPublishExecutor physicalPublishExecutor) {
        ensureSourceRunning();
        if (this.physicalPublishExecutor != null) {
            throw new IllegalStateException("Worker physical publish executor is already bound");
        }
        this.physicalPublishExecutor = Objects.requireNonNull(physicalPublishExecutor, "physicalPublishExecutor");
    }

    /** Validates process-wide graph/resource identity before fleet admission. */
    void requireFleetComposition(
            final WorkClassExecutionRegistry expectedWorkClasses, final SharedRocksDbResources expectedResources) {
        if (workClasses != Objects.requireNonNull(expectedWorkClasses, "expectedWorkClasses")) {
            throw new IllegalArgumentException("Worker shard runtime uses another work-class registry");
        }
        if (resources != Objects.requireNonNull(expectedResources, "expectedResources")) {
            throw new IllegalArgumentException("Worker shard runtime uses another resource envelope");
        }
    }

    /** Runs one bounded source turn while the Worker runtime is admitting work. */
    public synchronized SourceApplyCoordinator.TurnResult runSourceTurn(
            final SchedulerBudget workBudget, final LongSupplier ownerClock) {
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
            final TrustedUtcIntervalEvidence evidence, final SchedulerBudget budget, final LongSupplier ownerClock) {
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
        final SchedulerBudget oneHead = new SchedulerBudget(
                1, Objects.requireNonNull(budget, "Claim poll budget").maxBytes(), budget.maxElapsedNanos());
        final List<ScheduleWorkItem> selected = schedulingRuntime.pollReady(
                Objects.requireNonNull(evidence, "trusted UTC evidence"), oneHead, ownerClock);
        if (selected.isEmpty()) {
            return Optional.empty();
        }
        if (selected.size() != 1) {
            throw new IllegalStateException("single-head Claim poll returned multiple READY items");
        }
        return Optional.of(
                commandRuntime.submitClaim(selected.get(0), evidence, claimDeadlineEpochMs, claimedCharge, ownerClock));
    }

    /**
     * Runs one bounded due-discovery action and queues at most one derived
     * Claim from the resulting READY ring. The Claim action is returned
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
        final Optional<ClaimHandoffWorkClassExecutor.Submission> claim =
                pollAndSubmitClaim(evidence, discoveryBudget, claimDeadlineEpochMs, claimedCharge, ownerClock);
        return new DueClaimTurn(due, claim);
    }

    /**
     * Runs a bounded due-to-Claim-to-Publish composition on the shared Worker
     * graph. The exact Claim and Publish tasks are observed through bounded
     * fair command turns rather than by reaching into an executor's private
     * action. An empty preparation result means that the external live
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
        final PublishPreparationProvider provider = Objects.requireNonNull(preparationProvider, "preparation provider");
        final DueClaimTurn dueClaim =
                runDueAndSubmitClaim(evidence, discoveryBudget, claimDeadlineEpochMs, claimedCharge, ownerClock);
        if (dueClaim.claimSubmission().isEmpty()) {
            return new DueClaimPublishTurn(dueClaim, List.of(), Optional.empty(), Optional.empty(), List.of());
        }

        final ClaimHandoffWorkClassExecutor.Submission claimSubmission =
                dueClaim.claimSubmission().orElseThrow();
        final List<WorkClassTask> claimCompletedTasks =
                runCommandTurnsUntilCompleted(claimSubmission.task(), exactCommandBudget, maxCommandTurns);
        final ClaimHandoffWorkClassExecutor.ClaimHandoffResult claimResult = claimSubmission
                .result()
                .orElseThrow(() -> new IllegalStateException("Claim task completed without a result"));
        final Optional<ClaimHandoffWorkClassExecutor.ClaimHandoffResult> completedClaim = Optional.of(claimResult);
        if (claimResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED) {
            return new DueClaimPublishTurn(dueClaim, claimCompletedTasks, completedClaim, Optional.empty(), List.of());
        }

        final Optional<WorkerCommandRuntime.PublishPreparation> preparation;
        try {
            preparation = Objects.requireNonNull(provider.prepare(claimResult), "Publish preparation result");
        } catch (RuntimeException | Error failure) {
            // A live prerequisite authority that throws has not proved that
            // the Claim can safely continue. Fence this owner and retain
            // the exact Claim/reservation for evidence-driven recovery.
            ownedShard.fence();
            throw failure;
        }
        if (preparation.isEmpty()) {
            return new DueClaimPublishTurn(dueClaim, claimCompletedTasks, completedClaim, Optional.empty(), List.of());
        }
        final PublishAdmissionWorkClassExecutor.Submission publishSubmission =
                submitPublish(claimResult, preparation.orElseThrow());
        final List<WorkClassTask> publishCompletedTasks =
                runCommandTurnsUntilCompleted(publishSubmission.task(), exactCommandBudget, maxCommandTurns);
        return new DueClaimPublishTurn(
                dueClaim, claimCompletedTasks, completedClaim, Optional.of(publishSubmission), publishCompletedTasks);
    }

    /**
     * Runs the bounded due-to-Claim-to-Publish composition using the provider
     * bound when this Worker graph was assembled. A graph without that
     * binding fails closed instead of silently falling back to a caller that
     * may not have the typed Lane identity fence.
     */
    public synchronized DueClaimPublishTurn runDueClaimPublishTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock,
            final SchedulerBudget commandBudget,
            final int maxCommandTurns) {
        final PublishPreparationProvider provider = preparationProvider;
        if (provider == null) {
            throw new IllegalStateException("Worker shard runtime has no bound Publish preparation provider");
        }
        return runDueClaimPublishTurn(
                evidence,
                discoveryBudget,
                claimDeadlineEpochMs,
                claimedCharge,
                ownerClock,
                commandBudget,
                maxCommandTurns,
                provider);
    }

    private List<WorkClassTask> runCommandTurnsUntilCompleted(
            final WorkClassTask target, final SchedulerBudget commandBudget, final int maxCommandTurns) {
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
                throw new IllegalStateException(
                        "exact Worker command task made no bounded progress: " + target.taskId());
            }
        }
        if (workClasses.state(target).isPresent()) {
            throw new IllegalStateException(
                    "exact Worker command task exceeded command-turn bound: " + target.taskId());
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

    /** Queues a Claim handoff whose materialization is derived locally. */
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
            final ChannelResourceIdentity channel,
            final ReadyCertificate readyCertificate,
            final TrustedUtcIntervalEvidence decisionTime,
            final long retryUntilEpochMs,
            final int signingKeyVersion,
            final PrivateKey signingKey,
            final LongSupplier ownerClock) {
        ensureCommandRuntime();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return commandRuntime.submitPublish(
                claim,
                reservation,
                channel,
                readyCertificate,
                decisionTime,
                retryUntilEpochMs,
                signingKeyVersion,
                signingKey,
                ownerClock);
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

    /** Queues one PUBLISHING attempt behind the live physical gate and Outcome handoff. */
    public synchronized WorkerPhysicalPublishExecutor.Submission submitPhysicalPublish(
            final PublishAttemptLedger attempt,
            final DestinationPublishRequest request,
            final LongSupplier ownerClock) {
        ensurePhysicalPublishExecutor();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        return physicalPublishExecutor.submit(attempt, request, ownerClock);
    }

    /** Rebuilds the exact request from the retained Admission before handoff. */
    public synchronized WorkerPhysicalPublishExecutor.Submission submitPhysicalPublish(
            final PublishAttemptLedger attempt, final byte[] payload, final LongSupplier ownerClock) {
        return submitPhysicalPublish(
                attempt, WorkerPhysicalPublishExecutor.prepareRequest(attempt, payload), ownerClock);
    }

    /**
     * Reloads the exact persisted PUBLISHING ledger before physical handoff.
     * Callers cannot carry a stale in-memory attempt across source replay or
     * an UNCERTAIN transition; the bounded shard scan is the identity source.
     */
    public synchronized WorkerPhysicalPublishExecutor.Submission submitPhysicalPublish(
            final byte[] publishAttemptId, final byte[] payload, final LongSupplier ownerClock) {
        ensurePhysicalPublishExecutor();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        final PublishAttemptLedger persisted =
                ownedShard.shard().findOpenPublishAttempt(Objects.requireNonNull(publishAttemptId, "publishAttemptId"));
        if (persisted == null || persisted.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException("persisted physical publish attempt is not PUBLISHING");
        }
        return submitPhysicalPublish(persisted, payload, ownerClock);
    }

    /**
     * Replays the assigned source until one exact Admission Source Position
     * has been applied, then reloads its durable PUBLISHING ledger and starts
     * the bounded physical adapter bridge. The source turn remains the only
     * path that can create the local attempt projection; a caller cannot
     * manufacture a PUBLISHING ledger by supplying an attempt object.
     *
     * <p>An empty payload result is a deliberate external Object Store
     * deferral. The payload provider runs only after the source-applied
     * ledger has been reloaded, and the physical executor repeats the frozen
     * inline/length/hash validation before any destination call.</p>
     */
    public synchronized SourceBoundPhysicalPublishTurn runSourceBoundPhysicalPublish(
            final byte[] publishAttemptId,
            final SourcePosition admissionSourcePosition,
            final SchedulerBudget sourceBudget,
            final int maxSourceTurns,
            final PublishPayloadProvider payloadProvider,
            final LongSupplier ownerClock) {
        ensurePhysicalPublishExecutor();
        ensureSourceRunning();
        resources.requireRuntimeBusinessAdmission();
        final byte[] exactAttemptId = Bytes.copy(Objects.requireNonNull(publishAttemptId, "publishAttemptId"));
        final SourcePosition exactAdmissionPosition =
                Objects.requireNonNull(admissionSourcePosition, "admissionSourcePosition");
        if (!ownedShard.shard().shardId().equals(exactAdmissionPosition.shardId())) {
            throw new IllegalArgumentException("Admission Source Position belongs to another shard");
        }
        final SchedulerBudget exactSourceBudget = Objects.requireNonNull(sourceBudget, "source budget");
        if (maxSourceTurns <= 0) {
            throw new IllegalArgumentException("maxSourceTurns must be positive");
        }
        final PublishPayloadProvider exactPayloadProvider = Objects.requireNonNull(payloadProvider, "payload provider");
        final LongSupplier exactOwnerClock = Objects.requireNonNull(ownerClock, "owner clock");
        SourceApplyCoordinator.TurnResult lastSourceTurn = null;
        boolean admissionApplied = false;
        int sourceTurns = 0;
        while (true) {
            final PublishAttemptLedger persisted = ownedShard.shard().findOpenPublishAttempt(exactAttemptId);
            if (persisted != null) {
                if (!sameSourcePosition(
                        SourcePositionCodec.decode(persisted.sourcePosition()), exactAdmissionPosition)) {
                    return SourceBoundPhysicalPublishTurn.sourcePositionMismatch(
                            sourceTurns,
                            lastSourceTurn,
                            persisted,
                            new IllegalStateException(
                                    "PUBLISHING ledger Source Position differs from Admission append"));
                }
                if (persisted.state() != AttemptLedgerState.PUBLISHING) {
                    return SourceBoundPhysicalPublishTurn.attemptNotPublishing(sourceTurns, lastSourceTurn, persisted);
                }
                final Optional<byte[]> payload =
                        Objects.requireNonNull(exactPayloadProvider.load(persisted), "payload provider result");
                if (payload.isEmpty()) {
                    return SourceBoundPhysicalPublishTurn.payloadUnavailable(sourceTurns, lastSourceTurn, persisted);
                }
                final WorkerPhysicalPublishExecutor.Submission physical =
                        submitPhysicalPublish(persisted.publishAttemptId(), payload.orElseThrow(), exactOwnerClock);
                return physical.state() == WorkerPhysicalPublishExecutor.SubmissionState.DEFERRED
                        ? SourceBoundPhysicalPublishTurn.physicalDeferred(
                                sourceTurns, lastSourceTurn, persisted, physical)
                        : SourceBoundPhysicalPublishTurn.physicalSubmitted(
                                sourceTurns, lastSourceTurn, persisted, physical);
            }
            if (admissionApplied) {
                ownedShard.fence();
                return SourceBoundPhysicalPublishTurn.sourceAppliedWithoutAttempt(
                        sourceTurns,
                        lastSourceTurn,
                        new IllegalStateException("Admission source apply did not create its PUBLISHING ledger"));
            }
            if (sourceTurns >= maxSourceTurns) {
                return SourceBoundPhysicalPublishTurn.sourceTurnLimit(sourceTurns, lastSourceTurn);
            }

            lastSourceTurn = runSourceTurn(exactSourceBudget, exactOwnerClock);
            sourceTurns++;
            if (lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                if (sameSourcePosition(lastSourceTurn.entry().position(), exactAdmissionPosition)) {
                    admissionApplied = true;
                }
                continue;
            }
            if (lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    || lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS) {
                if (sourceTurns >= maxSourceTurns) {
                    return SourceBoundPhysicalPublishTurn.sourceTurnLimit(sourceTurns, lastSourceTurn);
                }
                continue;
            }
            return SourceBoundPhysicalPublishTurn.sourceApplyBlocked(
                    sourceTurns,
                    lastSourceTurn,
                    lastSourceTurn.failure() == null
                            ? new IllegalStateException("source turn stopped before Admission application")
                            : lastSourceTurn.failure());
        }
    }

    /**
     * Runs the bound due/Claim/Publish graph, source-applies its exact
     * Admission append, and starts the physical publish bridge. The method
     * returns before an asynchronous destination result is source-applied;
     * the returned physical submission retains the signed Outcome handoff
     * and the normal Worker command/source loops remain authoritative.
     */
    public synchronized DueClaimPublishPhysicalTurn runDueClaimPublishPhysicalTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock,
            final SchedulerBudget commandBudget,
            final int maxCommandTurns,
            final SchedulerBudget sourceBudget,
            final int maxSourceTurns,
            final PublishPayloadProvider payloadProvider) {
        ensurePhysicalPublishExecutor();
        Objects.requireNonNull(sourceBudget, "source budget");
        Objects.requireNonNull(payloadProvider, "payload provider");
        if (maxSourceTurns <= 0) {
            throw new IllegalArgumentException("maxSourceTurns must be positive");
        }
        final DueClaimPublishTurn dueClaimPublish = runDueClaimPublishTurn(
                evidence,
                discoveryBudget,
                claimDeadlineEpochMs,
                claimedCharge,
                ownerClock,
                commandBudget,
                maxCommandTurns);
        if (dueClaimPublish.publishSubmission().isEmpty()) {
            return new DueClaimPublishPhysicalTurn(dueClaimPublish, Optional.empty());
        }
        final PublishAdmissionWorkClassExecutor.Submission admission =
                dueClaimPublish.publishSubmission().orElseThrow();
        final PublishAdmissionWorkClassExecutor.AdmissionHandoffResult admissionResult = admission
                .result()
                .orElseThrow(() -> new IllegalStateException("Publish Admission task completed without a result"));
        if (admissionResult.kind() == PublishAdmissionWorkClassExecutor.ResultKind.UNKNOWN) {
            final UnknownAdmissionResolution recovery =
                    recoverUnknownPublishAdmission(admission.mutation(), sourceBudget, maxSourceTurns, ownerClock);
            if (recovery.position() == null) {
                final SourceBoundPhysicalPublishTurn blocked = recovery.failure() == null
                        ? SourceBoundPhysicalPublishTurn.sourceTurnLimit(
                                recovery.sourceTurns(), recovery.lastSourceTurn())
                        : SourceBoundPhysicalPublishTurn.sourceApplyBlocked(
                                recovery.sourceTurns(), recovery.lastSourceTurn(), recovery.failure());
                return new DueClaimPublishPhysicalTurn(dueClaimPublish, Optional.of(blocked));
            }
            return new DueClaimPublishPhysicalTurn(
                    dueClaimPublish,
                    Optional.of(runSourceBoundPhysicalPublish(
                            admission.mutation().logicalOperationIdentity(),
                            recovery.position(),
                            sourceBudget,
                            Math.max(1, maxSourceTurns - recovery.sourceTurns()),
                            payloadProvider,
                            ownerClock)));
        }
        if (admissionResult.kind() != PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED) {
            return new DueClaimPublishPhysicalTurn(dueClaimPublish, Optional.empty());
        }
        return new DueClaimPublishPhysicalTurn(
                dueClaimPublish,
                Optional.of(runSourceBoundPhysicalPublish(
                        admission.mutation().logicalOperationIdentity(),
                        admissionResult.sourcePosition(),
                        sourceBudget,
                        maxSourceTurns,
                        payloadProvider,
                        ownerClock)));
    }

    /**
     * Resolves an uncertain Publish Admission by replaying the exact mutation
     * from the assigned source. A source connection-generation change can
     * make the append evidence UNKNOWN after the broker accepted the bytes;
     * retrying the append would create a second logical record, so only an
     * exact source mutation match may reopen the physical handoff.
     */
    private synchronized UnknownAdmissionResolution recoverUnknownPublishAdmission(
            final SystemMutation expectedMutation,
            final SchedulerBudget sourceBudget,
            final int maxSourceTurns,
            final LongSupplier ownerClock) {
        Objects.requireNonNull(expectedMutation, "expectedMutation");
        Objects.requireNonNull(sourceBudget, "sourceBudget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        SourceApplyCoordinator.TurnResult lastSourceTurn = null;
        for (int sourceTurns = 0; sourceTurns < maxSourceTurns; sourceTurns++) {
            try {
                lastSourceTurn = runSourceTurn(sourceBudget, ownerClock);
            } catch (RuntimeException | Error failure) {
                return new UnknownAdmissionResolution(null, sourceTurns, lastSourceTurn, failure);
            }
            if (lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED
                    && lastSourceTurn.entry() instanceof SourceReplayMutation replayed
                    && Arrays.equals(
                            expectedMutation.encodeFrame(), replayed.mutation().encodeFrame())) {
                return new UnknownAdmissionResolution(replayed.position(), sourceTurns + 1, lastSourceTurn, null);
            }
            if (lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.WAITING_FOR_SOURCE
                    || lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.WAITING_FOR_WORK_CLASS
                    || lastSourceTurn.status() == SourceApplyCoordinator.TurnStatus.APPLIED_AND_ACKED) {
                continue;
            }
            final Throwable failure = lastSourceTurn.failure() == null
                    ? new IllegalStateException("source replay stopped before the exact uncertain Publish Admission")
                    : lastSourceTurn.failure();
            return new UnknownAdmissionResolution(null, sourceTurns + 1, lastSourceTurn, failure);
        }
        return new UnknownAdmissionResolution(null, maxSourceTurns, lastSourceTurn, null);
    }

    private record UnknownAdmissionResolution(
            SourcePosition position,
            int sourceTurns,
            SourceApplyCoordinator.TurnResult lastSourceTurn,
            Throwable failure) {}

    private static boolean sameSourcePosition(final SourcePosition first, final SourcePosition second) {
        if (first == null
                || second == null
                || !first.shardId().equals(second.shardId())
                || !first.sameSourceIdentity(second)) {
            return false;
        }
        if (first instanceof KafkaSourcePosition left && second instanceof KafkaSourcePosition right) {
            return left.offset() == right.offset()
                    && left.brokerLogAppendTimeEpochMs() == right.brokerLogAppendTimeEpochMs()
                    && (left.leaderEpoch() == null
                            || right.leaderEpoch() == null
                            || left.leaderEpoch().equals(right.leaderEpoch()));
        }
        return Arrays.equals(first.canonicalBytes(), second.canonicalBytes());
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
     * Runs or retries the owner drain. A pending source ACK is rejected
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
        final OwnerDrainCoordinator.DrainResult result =
                drainCoordinator.drain(request, clock, new OwnerDrainCoordinator.DrainCallbacks() {
                    @Override
                    public void stopSourceAndScheduling() {
                        sourcePaused = true;
                        delegate.stopSourceAndScheduling();
                    }

                    @Override
                    public void pollOpenPublishAttempts(
                            final List<com.nereusstream.delay.runtime.PublishAttemptLedger> openAttempts,
                            final int pollNumber) {
                        delegate.pollOpenPublishAttempts(openAttempts, pollNumber);
                    }

                    @Override
                    public void commitSourceHint(
                            final com.nereusstream.delay.protocol.SourcePosition persistedPosition) {
                        delegate.commitSourceHint(persistedPosition);
                    }
                });
        if (result.pendingCheckpointTask() == null) {
            // The precondition above proves that close cannot discard an
            // unacknowledged source record. OwnerDrainCoordinator has also
            // completed Store close and exact lease release at this point.
            sourceLoop.close();
            terminal = true;
        }
        return result;
    }

    /**
     * Refuses to tear down an active owner. Production shutdown must use the
     * same drain path so source, Store and lease boundaries remain ordered.
     */
    @Override
    public synchronized void close() {
        if (terminal) {
            return;
        }
        throw new IllegalStateException("Worker shard runtime must complete owner drain before close");
    }

    /**
     * Closes only the old source runtime after an external Owner fence and
     * before a source-assignment reactivation successor is published.
     *
     * <p>This is intentionally separate from normal drain: reactivation must
     * preserve the Store and its durable source position while the old
     * context-bound Owner is already FENCED. A pending source record is never
     * discarded; the caller must first prove that the old source is idle.</p>
     */
    public synchronized void closeForOwnerReactivation() {
        ensureNotTerminal();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("cannot reactivate with a pending source acknowledgement");
        }
        sourcePaused = true;
        sourceLoop.close();
        terminal = true;
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

    private void ensurePhysicalPublishExecutor() {
        ensureNotTerminal();
        if (physicalPublishExecutor == null) {
            throw new IllegalStateException("Worker shard runtime has no physical publish executor");
        }
    }

    /** External authority that resolves inline or Object Store payload bytes. */
    @FunctionalInterface
    public interface PublishPayloadProvider {
        Optional<byte[]> load(PublishAttemptLedger attempt);
    }

    /** Evidence from one combined due-discovery to derived-Claim handoff. */
    public record DueClaimTurn(
            WorkerSchedulingRuntime.DueTurn dueTurn,
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
            claimCompletedTasks = List.copyOf(Objects.requireNonNull(claimCompletedTasks, "claimCompletedTasks"));
            claimResult = Objects.requireNonNull(claimResult, "claimResult");
            publishSubmission = Objects.requireNonNull(publishSubmission, "publishSubmission");
            publishCompletedTasks = List.copyOf(Objects.requireNonNull(publishCompletedTasks, "publishCompletedTasks"));
        }
    }

    /** Result of one source-bound Admission-to-physical handoff. */
    public record SourceBoundPhysicalPublishTurn(
            SourceBoundPhysicalPublishStatus status,
            int sourceTurns,
            Optional<SourceApplyCoordinator.TurnResult> lastSourceTurn,
            Optional<PublishAttemptLedger> attempt,
            Optional<WorkerPhysicalPublishExecutor.Submission> physicalSubmission,
            Throwable failure) {
        public SourceBoundPhysicalPublishTurn {
            Objects.requireNonNull(status, "status");
            if (sourceTurns < 0) {
                throw new IllegalArgumentException("sourceTurns must be non-negative");
            }
            lastSourceTurn = Objects.requireNonNull(lastSourceTurn, "lastSourceTurn");
            attempt = Objects.requireNonNull(attempt, "attempt");
            physicalSubmission = Objects.requireNonNull(physicalSubmission, "physicalSubmission");
            if ((status == SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED
                            || status == SourceBoundPhysicalPublishStatus.PHYSICAL_DEFERRED)
                    != physicalSubmission.isPresent()) {
                throw new IllegalArgumentException("physical status must match physical submission");
            }
            if ((status == SourceBoundPhysicalPublishStatus.ATTEMPT_NOT_PUBLISHING
                            || status == SourceBoundPhysicalPublishStatus.PAYLOAD_UNAVAILABLE
                            || status == SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED
                            || status == SourceBoundPhysicalPublishStatus.PHYSICAL_DEFERRED)
                    != attempt.isPresent()) {
                throw new IllegalArgumentException("attempt status must match attempt evidence");
            }
            if (status == SourceBoundPhysicalPublishStatus.SOURCE_APPLY_BLOCKED
                    || status == SourceBoundPhysicalPublishStatus.SOURCE_POSITION_MISMATCH
                    || status == SourceBoundPhysicalPublishStatus.SOURCE_APPLIED_WITHOUT_ATTEMPT) {
                Objects.requireNonNull(failure, "blocked source result requires failure");
            }
        }

        private static SourceBoundPhysicalPublishTurn physicalSubmitted(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final PublishAttemptLedger attempt,
                final WorkerPhysicalPublishExecutor.Submission submission) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.PHYSICAL_SUBMITTED,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.of(attempt),
                    Optional.of(submission),
                    null);
        }

        private static SourceBoundPhysicalPublishTurn physicalDeferred(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final PublishAttemptLedger attempt,
                final WorkerPhysicalPublishExecutor.Submission submission) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.PHYSICAL_DEFERRED,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.of(attempt),
                    Optional.of(submission),
                    null);
        }

        private static SourceBoundPhysicalPublishTurn attemptNotPublishing(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final PublishAttemptLedger attempt) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.ATTEMPT_NOT_PUBLISHING,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.of(attempt),
                    Optional.empty(),
                    null);
        }

        private static SourceBoundPhysicalPublishTurn payloadUnavailable(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final PublishAttemptLedger attempt) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.PAYLOAD_UNAVAILABLE,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.of(attempt),
                    Optional.empty(),
                    null);
        }

        private static SourceBoundPhysicalPublishTurn sourceTurnLimit(
                final int sourceTurns, final SourceApplyCoordinator.TurnResult lastSourceTurn) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.SOURCE_TURN_LIMIT,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.empty(),
                    Optional.empty(),
                    null);
        }

        private static SourceBoundPhysicalPublishTurn sourceApplyBlocked(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final Throwable failure) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.SOURCE_APPLY_BLOCKED,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.empty(),
                    Optional.empty(),
                    Objects.requireNonNull(failure, "failure"));
        }

        private static SourceBoundPhysicalPublishTurn sourcePositionMismatch(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final PublishAttemptLedger attempt,
                final Throwable failure) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.SOURCE_POSITION_MISMATCH,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.of(attempt),
                    Optional.empty(),
                    Objects.requireNonNull(failure, "failure"));
        }

        private static SourceBoundPhysicalPublishTurn sourceAppliedWithoutAttempt(
                final int sourceTurns,
                final SourceApplyCoordinator.TurnResult lastSourceTurn,
                final Throwable failure) {
            return new SourceBoundPhysicalPublishTurn(
                    SourceBoundPhysicalPublishStatus.SOURCE_APPLIED_WITHOUT_ATTEMPT,
                    sourceTurns,
                    Optional.ofNullable(lastSourceTurn),
                    Optional.empty(),
                    Optional.empty(),
                    Objects.requireNonNull(failure, "failure"));
        }
    }

    public enum SourceBoundPhysicalPublishStatus {
        PHYSICAL_SUBMITTED,
        PHYSICAL_DEFERRED,
        ATTEMPT_NOT_PUBLISHING,
        PAYLOAD_UNAVAILABLE,
        SOURCE_TURN_LIMIT,
        SOURCE_APPLY_BLOCKED,
        SOURCE_POSITION_MISMATCH,
        SOURCE_APPLIED_WITHOUT_ATTEMPT
    }

    /** Result of the full bound due/Claim/Publish/source/physical composition. */
    public record DueClaimPublishPhysicalTurn(
            DueClaimPublishTurn dueClaimPublishTurn, Optional<SourceBoundPhysicalPublishTurn> physicalTurn) {
        public DueClaimPublishPhysicalTurn {
            Objects.requireNonNull(dueClaimPublishTurn, "dueClaimPublishTurn");
            physicalTurn = Objects.requireNonNull(physicalTurn, "physicalTurn");
        }
    }
}
