package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Active-owner Target source and reservation maintenance on one Worker resource graph.
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

    private final ShardId shardId;
    private final WorkClassExecutionRegistry workClasses;
    private final SharedRocksDbResources resources;
    private final ShardStore store;
    private final WorkerSourceApplyLoop sourceLoop;
    private final TargetSourceApplyRuntime target;
    private final TargetReservationGcRuntime maintenance;
    private final TargetOwnerDrainCoordinator drainCoordinator;
    private boolean sourceAndMaintenancePaused;
    private TargetCheckpointCandidateWorkClassExecutor.Submission pendingCheckpoint;
    private SourceRecordConsumer.CheckpointCut preparedCheckpointCut;

    public TargetWorkerShardRuntime(
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
        final var exactTarget = Objects.requireNonNull(target, "target");
        this.target = exactTarget;
        final var inputs = Objects.requireNonNull(maintenanceInputs, "maintenanceInputs");
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
                intents, ownerClock, checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits,
                protectCheckpointCut());
    }

    /** Captures the cut before a scheduler creates its pending intent, under the exact host Shard lock. */
    synchronized SourceRecordConsumer.CheckpointCut protectCheckpointCut() {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (maintenance.hasPendingTurn()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending GC action");
        }
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
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut") || maintenance.hasPendingTurn()) {
            throw new IllegalStateException("Target checkpoint cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        final var submitted = target.submitLocalCheckpointCandidate(
                workClasses, intents, ownerClock, checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits,
                cut::requireCurrent);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Runs one shared bounded turn and releases the local source/GC cut only after this action settles. */
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

    /** Stops new source polls and GC submissions before Owner drain begins. */
    public synchronized void pauseNewTurns() {
        requireCheckpointSettled();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a pending source acknowledgement");
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

    public synchronized Optional<SourceReplayEntry> pendingSourceEntry() {
        return sourceLoop.pendingEntry();
    }

    /** Retries one retained source apply/ACK without polling a new record before drain. */
    public synchronized Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        if (sourceAndMaintenancePaused) {
            throw new IllegalStateException("Target Worker source and GC admission is paused");
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
            throw new IllegalStateException("Target Worker source and GC admission is paused");
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
}
