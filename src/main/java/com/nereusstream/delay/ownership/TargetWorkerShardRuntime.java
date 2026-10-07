package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetExpiryDiscoveryStore;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetNativePolicyTrustStore;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Active-owner Target source and maintenance on one Worker resource graph.
 *
 * <p>The host drives bounded turns and owns Owner drain and native source teardown. The shared
 * resource envelope gates both turns before source poll or GC task submission.
 */
public final class TargetWorkerShardRuntime
        implements TargetWorkerShardFleetRuntime.ShardTurns, TargetWorkerHostRuntime.Shard {
    /** Authority inputs for this Owner's Close and ordinary expiry maintenance. */
    public record Maintenance(
            TargetReservationControls.Authority controls,
            TargetReservationClosureWorkClassExecutor.Limits closeLimits,
            TargetReservationExpiryWorkClassExecutor.Limits expiryLimits,
            TargetStoreBackend.CommitAuthority physicalWrites,
            TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            LongSupplier ownerClock) {
        public Maintenance {
            Objects.requireNonNull(controls, "controls");
            Objects.requireNonNull(closeLimits, "closeLimits");
            Objects.requireNonNull(expiryLimits, "expiryLimits");
            Objects.requireNonNull(physicalWrites, "physicalWrites");
            Objects.requireNonNull(closureQuota, "closureQuota");
            Objects.requireNonNull(expiryQuota, "expiryQuota");
            Objects.requireNonNull(cursorQuota, "cursorQuota");
            Objects.requireNonNull(ownerClock, "ownerClock");
        }
    }

    /** Per-turn proof and signing inputs for scheduled message-expiry discovery. */
    public record MessageExpiryRequest(
            BoundedReadBudget discoveryBudget,
            TrustedUtcIntervalEvidence evidence,
            long retryUntilEpochMs,
            OwnerIdentity owner,
            int signingKeyVersion,
            PrivateKey signingKey) {
        public MessageExpiryRequest {
            Objects.requireNonNull(discoveryBudget, "discoveryBudget");
            Objects.requireNonNull(evidence, "evidence");
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(signingKey, "signingKey");
        }
    }

    private final ShardId shardId;
    private final WorkClassExecutionRegistry workClasses;
    private final SharedRocksDbResources resources;
    private final ShardStore store;
    private final WorkerSourceApplyLoop sourceLoop;
    private final TargetSourceApplyRuntime target;
    private final TargetReservationGcRuntime maintenance;
    private final TargetOwnerDrainCoordinator drainCoordinator;
    private final LongSupplier ownerClock;
    private boolean sourceAndMaintenancePaused;
    private volatile TargetMessageExpiryWorkClassExecutor messageExpiryHandoff;
    private volatile TargetMessageExpiryMaintenance messageExpiryMaintenance;
    private TargetCheckpointCandidateWorkClassExecutor.Submission pendingCheckpoint;
    private TargetPublishRecoveryMaintenance publishRecoveryMaintenance;
    private SourceRecordConsumer.CheckpointCut preparedCheckpointCut;

    TargetWorkerShardRuntime(
            final SourceRecordConsumer consumer,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime target,
            final Maintenance maintenanceInputs) {
        final var exactClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.workClasses = exactClasses;
        final var exactStore = Objects.requireNonNull(store, "store");
        this.store = exactStore;
        this.resources = Objects.requireNonNull(resources, "resources");
        this.resources.requireWorkerActivationReady();
        final var exactTarget = Objects.requireNonNull(target, "target");
        this.target = exactTarget;
        final var inputs = Objects.requireNonNull(maintenanceInputs, "maintenanceInputs");
        ownerClock = inputs.ownerClock();
        exactTarget.requireWorkerStore(exactStore, this.resources);
        this.resources.bindWorkClassExecutionRegistry(exactClasses);
        shardId = exactStore.shardId();
        sourceLoop = new WorkerSourceApplyLoop(Objects.requireNonNull(consumer, "consumer"), exactClasses, exactTarget);
        maintenance = exactTarget.newReservationGcRuntime(
                exactClasses,
                inputs.controls(),
                inputs.closeLimits(),
                inputs.expiryLimits(),
                inputs.physicalWrites(),
                inputs.closureQuota(),
                inputs.expiryQuota(),
                inputs.cursorQuota(),
                inputs.ownerClock());
        drainCoordinator =
                new TargetOwnerDrainCoordinator(exactStore, this.resources, exactTarget, sourceLoop, maintenance);
    }

    void bindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        target.bindTargetQueueChangeSignal(signal);
    }

    void unbindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        target.unbindTargetQueueChangeSignal(signal);
    }

    synchronized void configureTargetQueueHeadCache(final int maximumEntries) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.configureTargetQueueHeadCache(maximumEntries);
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void requireFleetComposition(
            final WorkClassExecutionRegistry expectedClasses, final SharedRocksDbResources expectedResources) {
        if (resources != Objects.requireNonNull(expectedResources, "expectedResources")
                || workClasses != Objects.requireNonNull(expectedClasses, "expectedClasses")) {
            throw new IllegalArgumentException("Target Worker shard uses another resource or WorkClass graph");
        }
    }

    public synchronized SourceApplyCoordinator.TurnResult runSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        preparedCheckpointCut = null;
        resources.requireRuntimeBusinessAdmission();
        return sourceLoop.runTurn(
                Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    public synchronized TargetReservationGcRuntime.Turn runMaintenanceTurn(final SchedulerBudget budget) {
        requireNewTurnsAdmitted();
        preparedCheckpointCut = null;
        resources.requireRuntimeBusinessAdmission();
        return maintenance.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    /** One caller-driven producer turn; the Host maintenance loop selects Shards in rotation. */
    @Override
    public Optional<WorkClassTask> runMessageExpiryMaintenanceTurn() {
        final var configured = messageExpiryMaintenance;
        return configured == null ? Optional.empty() : configured.runTurn();
    }

    /** Claims one previously selected head only while this exact Worker admits new business turns. */
    public synchronized TargetClaimRecord claim(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final long nowEpochMs,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.claim(
                budget,
                selected,
                owner,
                nowEpochMs,
                deadlineEpochMs,
                executionBytes,
                operationDigest,
                quota,
                physicalWrites,
                ownerClock);
    }

    /** Revokes an exact unadmitted Claim while this Worker still admits business turns. */
    public synchronized void revokeClaim(
            final BoundedReadBudget budget,
            final TargetClaimRecord expected,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.revokeClaim(budget, expected, operationDigest, quota, physicalWrites, ownerClock);
    }

    /** Reads one bounded page only while this exact Shard admits new Worker turns. */
    public synchronized TargetQueueSnapshotReader.Page scanTargetQueues(
            final BoundedReadBudget budget,
            final TargetPartitionId after,
            final int maximumTargets,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.scanTargetQueues(budget, after, maximumTargets, ownerClock);
    }

    /** Refreshes the exact physical Target's heads under current Worker admission. */
    public synchronized Optional<TargetQueueSnapshotReader.Entry> readTargetQueue(
            final BoundedReadBudget budget, final TargetPartitionId targetId, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readTargetQueue(budget, targetId, ownerClock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Applied readAppliedAdmission(
            final BoundedReadBudget budget, final com.nereusstream.delay.protocol.SystemMutation image,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readAppliedAdmission(budget, image, ownerClock);
    }

    /** Retained Admission proof for conservative recovery only; current Owner/read guards still apply. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Recovery readRecoveryAdmission(
            final BoundedReadBudget budget, final com.nereusstream.delay.protocol.SystemMutation image,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readRecoveryAdmission(budget, image, ownerClock);
    }

    /** Enumerates retained Admission references under this active Worker, with opaque same-view continuation. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverPublishRecovery(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverPublishRecovery(budget, continuation, maximumRows, ownerClock);
    }

    /** Install before Host ticks begin. History must return promptly and perform its bounded I/O asynchronously. */
    public synchronized TargetPublishRecoveryMaintenance configurePublishRecoveryMaintenance(
            final TargetPublishOutcomeMutationFactory outcomes, final ShardLogMutationAppender appender,
            final Supplier<BoundedReadBudget> reads, final TargetPublishRecoveryMaintenance.History history,
            final LongSupplier recoveryClock) {
        requirePublishRecoveryOwner(recoveryClock);
        if (publishRecoveryMaintenance != null) {
            throw new IllegalStateException("Target publish recovery maintenance is already configured");
        }
        final var handoff = new TargetOutcomeWorkClassExecutor(this, appender);
        final var executor = new TargetPublishRecoveryExecutor(this, handoff, outcomes);
        publishRecoveryMaintenance = new TargetPublishRecoveryMaintenance(
                this, executor, reads, history, recoveryClock);
        return publishRecoveryMaintenance;
    }

    synchronized void requirePublishRecoveryOwner(final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requirePublishRecoveryOwner(clock);
    }

    synchronized boolean publishRecoveryStillAdmitted(final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Reference reference,
            final LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        return target.publishRecoveryStillAdmitted(budget, reference, clock);
    }

    @Override
    public Optional<TargetPublishRecoveryMaintenance.Turn> runPublishRecoveryMaintenanceTurn() {
        final TargetPublishRecoveryMaintenance recovery;
        synchronized (this) {
            requireNewTurnsAdmitted();
            recovery = publishRecoveryMaintenance;
        }
        // Recovery rechecks this Worker for each action; avoid Worker -> recovery -> Worker lock inversion.
        return recovery == null ? Optional.empty() : Optional.of(recovery.runTurn());
    }

    synchronized void submitOutcomeAction(
            final WorkClassTask task, final com.nereusstream.delay.protocol.SystemMutation mutation,
            final LongSupplier ownerClock, final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireOutcomeWriter(mutation, ownerClock);
        workClasses.submit(task, action);
    }

    synchronized ShardLogMutationAppender.AppendOutcome appendOutcome(
            final com.nereusstream.delay.protocol.SystemMutation mutation,
            final ShardLogMutationAppender appender, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireOutcomeWriter(mutation, ownerClock);
        final var appended = Objects.requireNonNull(appender.append(mutation), "Target Outcome append");
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            target.requireCurrentExpiryLogPosition(appended.sourcePosition(), appended.sourceConnectionGeneration(),
                    appended.guardAttestationDigest(), ownerClock);
        }
        return appended;
    }

    synchronized Optional<SystemMutationResult> outcomeMutationResult(
            final com.nereusstream.delay.protocol.SystemMutation mutation, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.outcomeMutationResult(mutation, ownerClock);
    }

    /** Reads one bounded persisted active-message total for optional queue-depth telemetry. */
    public synchronized OptionalLong readTargetActiveMessages(
            final BoundedReadBudget budget, final TargetPartitionId targetId, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readTargetActiveMessages(budget, targetId, ownerClock);
    }

    /** Rechecks this source Shard's Store revision after paged head enumeration. */
    public synchronized TargetQueueSnapshotReader.Cut readTargetQueueCut(
            final BoundedReadBudget budget, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readTargetQueueCut(budget, ownerClock);
    }

    /** Reads one guarded message-expiry candidate without applying or appending it. */
    public synchronized TargetExpiryDiscoveryStore.Discovery discoverMessageExpiry(
            final BoundedReadBudget budget,
            final TargetExpiryDiscoveryStore.Cursor cursor,
            final TrustedUtcIntervalEvidence evidence,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverMessageExpiry(budget, cursor, evidence, ownerClock);
    }

    /** Creates this Shard's single source-preserving expiry handoff on the same Worker graph. */
    public synchronized TargetMessageExpiryWorkClassExecutor newMessageExpiryWorkClassExecutor(
            final ShardLogMutationAppender appender) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (messageExpiryHandoff != null) {
            throw new IllegalStateException("Target Worker message expiry executor is already created");
        }
        messageExpiryHandoff = new TargetMessageExpiryWorkClassExecutor(this, appender);
        return messageExpiryHandoff;
    }

    /**
     * Configures periodic discovery and append using per-turn trusted proof and signing inputs.
     * Call before Host maintenance starts; the provider must return a fresh bounded read budget.
     */
    public synchronized TargetMessageExpiryWorkClassExecutor configureMessageExpiryMaintenance(
            final ShardLogMutationAppender appender,
            final Supplier<MessageExpiryRequest> requestProvider) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (messageExpiryHandoff != null || messageExpiryMaintenance != null) {
            throw new IllegalStateException("Target Worker message expiry maintenance is already configured");
        }
        final var exactAppender = Objects.requireNonNull(appender, "appender");
        final var exactRequestProvider = Objects.requireNonNull(requestProvider, "requestProvider");
        final var handoff = new TargetMessageExpiryWorkClassExecutor(
                this, exactAppender);
        final var configured = new TargetMessageExpiryMaintenance(this, handoff, exactRequestProvider, ownerClock);
        messageExpiryHandoff = handoff;
        messageExpiryMaintenance = configured;
        return handoff;
    }

    synchronized void requireMessageExpiryMaintenanceConfigured() {
        if (messageExpiryMaintenance == null) {
            throw new IllegalStateException(
                    "Target Worker message expiry maintenance must be configured before Host start");
        }
    }

    synchronized void submitMessageExpiryAction(
            final WorkClassTask task,
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final LongSupplier ownerClock,
            final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireExpirySubmission(candidate, evidence, owner, ownerClock);
        workClasses.submit(Objects.requireNonNull(task, "task"), Objects.requireNonNull(action, "action"));
    }

    synchronized ShardLogMutationAppender.AppendOutcome appendMessageExpiry(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final com.nereusstream.delay.protocol.SystemMutation mutation,
            final ShardLogMutationAppender appender,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireExpiryAuthoritativelyStrict(candidate, evidence, owner, ownerClock);
        final var appended = Objects.requireNonNull(
                appender.append(Objects.requireNonNull(mutation, "mutation")), "Shard Log append outcome");
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            target.requireCurrentExpiryLogPosition(
                    appended.sourcePosition(),
                    appended.sourceConnectionGeneration(),
                    appended.guardAttestationDigest(),
                    ownerClock);
        }
        return appended;
    }

    synchronized boolean messageExpiryAppendApplied(
            final com.nereusstream.delay.protocol.SourcePosition position, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.expiryAppendApplied(position, ownerClock);
    }

    synchronized Optional<SystemMutationResult> messageExpiryMutationResult(
            final SystemMutation mutation, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.expiryMutationResult(mutation, ownerClock);
    }

    synchronized void fenceMessageExpiry() {
        target.fence();
    }

    /** Reads the frozen cost of one current head; Claim still rechecks Store and live authority. */
    public synchronized TargetHeadCostProbe.Cost probeSelectedHead(
            final BoundedReadBudget budget, final TargetHeadRef selected, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.probeSelectedHead(budget, selected, ownerClock);
    }

    /** Supplies Claim-time Native trust from this Worker's source-applied Shard Store. */
    public synchronized TargetNativePolicyTrustStore nativePolicyTrustStore(final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.nativePolicyTrustStore(ownerClock);
    }

    /** Admits only an ACK-settled active Shard to the bound, unpublished CHECKPOINT work class. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending source acknowledgement");
        }
        if (maintenance.hasPendingTurn()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending GC action");
        }
        requireMessageExpirySettledForCut();
        final var submitted = target.submitLocalCheckpointCandidate(
                workClasses, intents, ownerClock, checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Scheduled candidates additionally require an exact broker-confirmed source cut. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedCheckpointCandidate(
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return submitProtectedCheckpointCandidate(
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                protectCheckpointCut());
    }

    /** Captures the cut before a scheduler creates its pending intent, under the exact host Shard lock. */
    synchronized SourceRecordConsumer.CheckpointCut protectCheckpointCut() {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (maintenance.hasPendingTurn()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending GC action");
        }
        requireMessageExpirySettledForCut();
        final var cut = sourceLoop.checkpointCut(store.appliedShardLogPosition());
        preparedCheckpointCut = cut;
        return cut;
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedCheckpointCandidate(
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final SourceRecordConsumer.CheckpointCut cut) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut")
                || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target checkpoint cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        final var submitted = target.submitLocalCheckpointCandidate(
                workClasses,
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                cut::requireCurrent);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Runs one shared bounded turn and releases the local source/maintenance cut only after settlement. */
    public synchronized Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> runCheckpointTurn(
            final SchedulerBudget budget) {
        if (pendingCheckpoint == null) {
            return Optional.empty();
        }
        resources.requireRuntimeBusinessAdmission();
        if (pendingCheckpoint.outcome().isEmpty()) {
            workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
        }
        final var outcome = pendingCheckpoint.outcome();
        if (outcome.isPresent()) {
            pendingCheckpoint = null;
        }
        return outcome;
    }

    /** Exact queued checkpoint task, if it has not yet reached a terminal outcome. */
    public synchronized Optional<WorkClassTask> pendingCheckpointTask() {
        return pendingCheckpoint == null || pendingCheckpoint.outcome().isPresent()
                ? Optional.empty()
                : Optional.of(pendingCheckpoint.task());
    }

    /** Stops new source polls and maintenance submissions after retained maintenance work settles. */
    public synchronized void pauseNewTurns() {
        requireCheckpointSettled();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a pending source acknowledgement");
        }
        if (hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target Worker cannot pause a pending message expiry handoff");
        }
        sourceAndMaintenancePaused = true;
        preparedCheckpointCut = null;
    }

    /** Runs only the exact GC action already queued when admission was paused. */
    public synchronized Optional<TargetReservationGcRuntime.Turn> settlePendingMaintenance(
            final SchedulerBudget budget) {
        resources.requireRuntimeBusinessAdmission();
        return maintenance.settlePendingTurn(Objects.requireNonNull(budget, "budget"));
    }

    /** Advances only the retained expiry append and its source result by one bounded drain step. */
    @Override
    public Optional<WorkClassTask> settlePendingMessageExpiryForDrain(
            final SchedulerBudget workBudget,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        final var handoff = messageExpiryHandoff;
        if (handoff == null) {
            return Optional.empty();
        }
        final var submission = handoff.pendingSubmission();
        if (submission == null) {
            return Optional.empty();
        }
        final var exactWorkBudget = Objects.requireNonNull(workBudget, "workBudget");
        final var exactSourceBudget = Objects.requireNonNull(sourceBudget, "sourceBudget");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (submission.result().isEmpty()) {
            workClasses.runTurn(exactWorkBudget);
        }
        handoff.settlePending(clock);
        if (handoff.pendingSubmission() != null) {
            final var result = submission.result().orElse(null);
            if (result != null
                    && (result.kind() == TargetMessageExpiryWorkClassExecutor.ResultKind.APPENDED
                            || result.kind() == TargetMessageExpiryWorkClassExecutor.ResultKind.UNKNOWN)) {
                if (sourceLoop.pendingEntry().isPresent()) {
                    sourceLoop.settlePendingEntry(exactSourceBudget, clock);
                } else {
                    sourceLoop.runTurn(exactSourceBudget, clock);
                }
                handoff.settlePending(clock);
            }
        }
        final var pending = handoff.pendingSubmission();
        return pending == null ? Optional.empty() : Optional.of(pending.task());
    }

    public synchronized Optional<SourceReplayEntry> pendingSourceEntry() {
        return sourceLoop.pendingEntry();
    }

    /** Retries one retained source apply/ACK without polling a new record before drain. */
    public synchronized Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        if (sourceAndMaintenancePaused) {
            throw new IllegalStateException("Target Worker source and maintenance admission is paused");
        }
        if (sourceLoop.pendingEntry().isEmpty()) {
            return Optional.empty();
        }
        if (sourceLoop.pendingRequiresApply()) {
            resources.requireRuntimeBusinessAdmission();
        }
        return sourceLoop.settlePendingEntry(
                Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    /** Runs or retries strict Target drain after the host has closed its maintenance loop. */
    public synchronized TargetOwnerDrainCoordinator.Result drain(
            final TargetOwnerDrainCoordinator.Request request, final LongSupplier clock) {
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot drain a pending source acknowledgement");
        }
        pauseNewTurns();
        return drainCoordinator.drain(request, clock);
    }

    /** Closes the native source when no ACK is pending; Owner drain remains the host's responsibility. */
    public synchronized void closeSource() {
        sourceLoop.close();
    }

    private void requireNewTurnsAdmitted() {
        if (sourceAndMaintenancePaused) {
            throw new IllegalStateException("Target Worker source and maintenance admission is paused");
        }
        requireCheckpointSettled();
    }

    private void requireCheckpointSettled() {
        if (pendingCheckpoint != null && pendingCheckpoint.outcome().isPresent()) {
            pendingCheckpoint = null;
        }
        if (pendingCheckpoint != null) {
            throw new IllegalStateException("Target Worker checkpoint cut is still pending");
        }
    }

    private void requireMessageExpirySettledForCut() {
        if (hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending message expiry handoff");
        }
    }

    private boolean hasUnsettledMessageExpiry() {
        return messageExpiryHandoff != null && messageExpiryHandoff.hasUnsettledSubmission();
    }
}
