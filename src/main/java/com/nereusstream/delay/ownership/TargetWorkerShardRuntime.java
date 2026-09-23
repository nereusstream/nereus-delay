package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Active-owner Target source and reservation maintenance on one Worker resource graph.
 *
 * <p>The host drives bounded turns and owns Owner drain and native source teardown. The shared
 * resource envelope gates both turns before source poll or GC task submission.
 */
public final class TargetWorkerShardRuntime implements TargetWorkerShardFleetRuntime.ShardTurns {
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
    private final WorkerSourceApplyLoop sourceLoop;
    private final TargetReservationGcRuntime maintenance;
    private final TargetOwnerDrainCoordinator drainCoordinator;
    private boolean sourceAndMaintenancePaused;

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
        this.resources = Objects.requireNonNull(resources, "resources");
        final var exactTarget = Objects.requireNonNull(target, "target");
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
        resources.requireRuntimeBusinessAdmission();
        return sourceLoop.runTurn(
                Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    public synchronized TargetReservationGcRuntime.Turn runMaintenanceTurn(final SchedulerBudget budget) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return maintenance.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    /** Stops new source polls and GC submissions before Owner drain begins. */
    public synchronized void pauseNewTurns() {
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a pending source acknowledgement");
        }
        sourceAndMaintenancePaused = true;
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
    }
}
