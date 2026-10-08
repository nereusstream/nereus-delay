package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Owns whole-fleet source admission, maintenance ticks and ordered local shutdown. */
public final class TargetWorkerHostRuntime {
    /** A concurrent Host turn owns this Shard; the attempted action has not started. */
    public static final class ShardAdmissionBusyException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final byte[] routeIncarnation;
        private final int partition;

        private ShardAdmissionBusyException(final ShardId shardId) {
            super("Target host Shard admission is already in progress");
            final var selected = Objects.requireNonNull(shardId, "shardId");
            routeIncarnation = selected.routeIncarnation().bytes();
            partition = selected.partition();
        }

        public ShardId shardId() {
            return new ShardId(new com.nereusstream.delay.protocol.RouteIncarnation(routeIncarnation), partition);
        }
    }

    interface Shard {
        ShardId shardId();

        Optional<SourceReplayEntry> pendingSourceEntry();

        Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
                SchedulerBudget budget, LongSupplier ownerClock);

        Optional<WorkClassTask> pendingCheckpointTask();

        Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> runCheckpointTurn(SchedulerBudget budget);

        default Optional<WorkClassTask> settlePendingMessageExpiryForDrain(
                final SchedulerBudget workBudget,
                final SchedulerBudget sourceBudget,
                final LongSupplier ownerClock) {
            return Optional.empty();
        }

        TargetOwnerDrainCoordinator.Result drain(TargetOwnerDrainCoordinator.Request request, LongSupplier clock);
    }

    public enum Status {
        PENDING_CHECKPOINT,
        PENDING_SOURCE,
        PENDING_GC,
        PENDING_MESSAGE_EXPIRY,
        RELEASED,
        UNCERTAIN_RELEASED,
        OWNER_LOST_CLOSED,
        FAILED
    }

    public record ShardDrain(
            ShardId shardId,
            Status status,
            SourceApplyCoordinator.TurnResult sourceTurn,
            WorkClassTask pendingCheckpointTask,
            WorkClassTask pendingGcTask,
            RuntimeException failure,
            WorkClassTask pendingMessageExpiryTask) {
        public ShardDrain(
                final ShardId shardId,
                final Status status,
                final SourceApplyCoordinator.TurnResult sourceTurn,
                final WorkClassTask pendingCheckpointTask,
                final WorkClassTask pendingGcTask,
                final RuntimeException failure) {
            this(shardId, status, sourceTurn, pendingCheckpointTask, pendingGcTask, failure, null);
        }

        public ShardDrain {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(status, "status");
            if ((status == Status.FAILED) != (failure != null)
                    || (status == Status.PENDING_CHECKPOINT) != (pendingCheckpointTask != null)
                    || (status == Status.PENDING_GC) != (pendingGcTask != null)
                    || (status == Status.PENDING_MESSAGE_EXPIRY) != (pendingMessageExpiryTask != null)) {
                throw new IllegalArgumentException("Target host drain result has inconsistent evidence");
            }
        }

        public boolean complete() {
            return status == Status.RELEASED
                    || status == Status.UNCERTAIN_RELEASED
                    || status == Status.OWNER_LOST_CLOSED;
        }
    }

    public record Result(List<ShardDrain> shards) {
        public Result {
            shards = List.copyOf(Objects.requireNonNull(shards, "shards"));
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("Target host drain requires shards");
            }
        }

        public boolean complete() {
            return shards.stream().allMatch(ShardDrain::complete);
        }
    }

    private final TargetWorkerShardFleetRuntime fleet;
    private final TargetWorkerMaintenanceLoop maintenanceLoop;
    private final List<Shard> shards;
    private TargetWorkerOrdinaryLoop ordinaryLoop;
    /** Bound applied to Workers admitted after ordinary scheduling has started. */
    private int ordinaryTargetCacheLimit;
    private final TargetStoreBackend.TargetQueueChangeSignal targetQueueChangeSignal =
            new TargetStoreBackend.TargetQueueChangeSignal();
    private final Set<ShardId> withdrawn = new HashSet<>();
    private final Set<ShardId> draining = new HashSet<>();
    private final Map<ShardId, ShardDrain> completed = new HashMap<>();
    private TargetWorkerTargetInventory.Snapshot pendingTargetInventory;
    private boolean stopping;
    private boolean drainAllActive;

    /** Starts bounded reservation and message-expiry ticks for one exact Worker graph. */
    public static TargetWorkerHostRuntime start(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shards,
            final SchedulerBudget maintenanceBudget,
            final Duration maintenanceInterval,
            final Consumer<Throwable> failureConsumer) {
        final var exactShards = List.copyOf(Objects.requireNonNull(shards, "shards"));
        exactShards.forEach(TargetWorkerShardRuntime::requireMessageExpiryMaintenanceConfigured);
        final var fleet = new TargetWorkerShardFleetRuntime(workClasses, resources, exactShards);
        final var loop =
                TargetWorkerMaintenanceLoop.create(fleet, maintenanceBudget, maintenanceInterval, failureConsumer);
        TargetWorkerHostRuntime host = null;
        try {
            host = new TargetWorkerHostRuntime(fleet, loop, exactShards);
            loop.start();
            return host;
        } catch (RuntimeException | Error failure) {
            if (host != null) {
                host.rollbackTargetQueueChangeSignalBindings(host.shards, failure);
            }
            try {
                loop.close();
            } catch (RuntimeException | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    /** Test seam; production creates the fleet and scheduler from the same exact shards. */
    TargetWorkerHostRuntime(
            final TargetWorkerShardFleetRuntime fleet,
            final TargetWorkerMaintenanceLoop maintenanceLoop,
            final List<? extends Shard> shards) {
        this.fleet = Objects.requireNonNull(fleet, "fleet");
        this.maintenanceLoop = Objects.requireNonNull(maintenanceLoop, "maintenanceLoop");
        this.shards = new ArrayList<>(Objects.requireNonNull(shards, "shards"));
        if (!fleet.shardIds().equals(this.shards.stream().map(Shard::shardId).toList())) {
            throw new IllegalArgumentException("Target host drain shards differ from maintenance fleet");
        }
        final var attemptedBindings = new ArrayList<Shard>();
        try {
            for (Shard shard : this.shards) {
                attemptedBindings.add(shard);
                bindTargetQueueChangeSignal(shard);
            }
        } catch (RuntimeException | Error failure) {
            rollbackTargetQueueChangeSignalBindings(attemptedBindings, failure);
            throw failure;
        }
    }

    /** Revision to capture before a bounded queue scan so intervening commits cannot be missed. */
    public long targetQueueChangeRevision() {
        return targetQueueChangeSignal.revision();
    }

    /** Publishes an external Native-policy wakeup without fabricating a changed Target head. */
    void signalNativePolicyChange() {
        targetQueueChangeSignal.signalChanges(Set.of(), true);
    }

    /**
     * Waits for a committed Target business change after a caller's pre-scan revision. A change
     * during the scan returns immediately; timeout remains the periodic safety recheck.
     */
    public boolean awaitTargetQueueChange(final long observedRevision, final Duration timeout)
            throws InterruptedException {
        return targetQueueChangeSignal.awaitChange(observedRevision, timeout);
    }

    TargetStoreBackend.TargetQueueChangeSignal.Changes drainTargetQueueChanges() {
        return targetQueueChangeSignal.drainChanges();
    }

    void registerTargetWakeups(final List<TargetPartitionId> targets) {
        targetQueueChangeSignal.registerTargets(Objects.requireNonNull(targets, "targets"));
    }

    void unregisterTargetWakeup(final TargetPartitionId target) {
        targetQueueChangeSignal.unregisterTarget(Objects.requireNonNull(target, "target"));
    }

    /** Waits for one transiently busy Shard admission to release before retrying a bounded read. */
    public synchronized boolean awaitShardAdmission(
            final ShardId shardId, final Duration timeout) throws InterruptedException {
        final var requested = Objects.requireNonNull(shardId, "shardId");
        final var exactTimeout = Objects.requireNonNull(timeout, "timeout");
        if (exactTimeout.isNegative()) {
            throw new IllegalArgumentException("Target admission wait cannot be negative");
        }
        final long timeoutNanos;
        try {
            timeoutNanos = exactTimeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Target admission wait exceeds nanoseconds", overflow);
        }
        final long started = System.nanoTime();
        long remaining = timeoutNanos;
        while (draining.contains(requested) && !stopping) {
            if (remaining <= 0) {
                return false;
            }
            final long millis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(remaining);
            final int nanos = (int) (remaining
                    - java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(millis));
            wait(millis, nanos);
            remaining = timeoutNanos - (System.nanoTime() - started);
        }
        return !draining.contains(requested);
    }

    /** Starts the single bounded ordinary-first Claim loop for this Host. */
    public synchronized TargetWorkerOrdinaryLoop startOrdinaryScheduling(
            final TargetWorkerTargetInventory.Limits inventoryLimits,
            final TargetWorkerOrdinaryDrr.Limits drrLimits,
            final SchedulerBudget turnBudget,
            final Duration recheckInterval,
            final TargetWorkerOrdinaryDrr.Requests requests,
            final TargetWorkerOrdinaryLoop.ClaimConsumer claimConsumer,
            final LongSupplier ownerClock,
            final LongSupplier schedulerClock,
            final LongSupplier monotonicClock,
            final Consumer<Throwable> failureConsumer) {
        return startOrdinaryScheduling(
                inventoryLimits,
                drrLimits,
                turnBudget,
                recheckInterval,
                requests,
                claimConsumer,
                ownerClock,
                schedulerClock,
                monotonicClock,
                null,
                failureConsumer);
    }

    /** Starts the ordinary-first loop with optional caller-owned bounded, best-effort process metrics. */
    public synchronized TargetWorkerOrdinaryLoop startOrdinaryScheduling(
            final TargetWorkerTargetInventory.Limits inventoryLimits,
            final TargetWorkerOrdinaryDrr.Limits drrLimits,
            final SchedulerBudget turnBudget,
            final Duration recheckInterval,
            final TargetWorkerOrdinaryDrr.Requests requests,
            final TargetWorkerOrdinaryLoop.ClaimConsumer claimConsumer,
            final LongSupplier ownerClock,
            final LongSupplier schedulerClock,
            final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics,
            final Consumer<Throwable> failureConsumer) {
        if (stopping) {
            throw new IllegalStateException("Target host ordinary admission is stopping");
        }
        if (ordinaryLoop != null) {
            throw new IllegalStateException("Target host ordinary scheduler is already started");
        }
        TargetWorkerOrdinaryLoop.validateStartConfiguration(
                inventoryLimits,
                drrLimits,
                turnBudget,
                recheckInterval,
                requests,
                claimConsumer,
                ownerClock,
                schedulerClock,
                monotonicClock,
                failureConsumer);
        final var loop = TargetWorkerOrdinaryLoop.prepare(
                this,
                inventoryLimits,
                drrLimits,
                turnBudget,
                recheckInterval,
                requests,
                claimConsumer,
                ownerClock,
                schedulerClock,
                monotonicClock,
                metrics,
                failureConsumer);
        try {
            targetQueueChangeSignal.configureTargetLimit(inventoryLimits.maximumTargets());
            for (Shard shard : shards) {
                if (shard instanceof TargetWorkerShardRuntime worker
                        && !withdrawn.contains(shard.shardId())
                        && !completed.containsKey(shard.shardId())) {
                    worker.configureTargetQueueHeadCache(inventoryLimits.maximumTargets());
                }
            }
            loop.start();
            ordinaryTargetCacheLimit = inventoryLimits.maximumTargets();
        } catch (RuntimeException | Error failure) {
            try {
                loop.close();
            } catch (RuntimeException | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
        ordinaryLoop = loop;
        return ordinaryLoop;
    }

    private void bindTargetQueueChangeSignal(final Shard shard) {
        if (shard instanceof TargetWorkerShardRuntime worker) {
            worker.bindTargetQueueChangeSignal(targetQueueChangeSignal);
        }
    }

    private void unbindTargetQueueChangeSignal(final Shard shard) {
        if (shard instanceof TargetWorkerShardRuntime worker) {
            worker.unbindTargetQueueChangeSignal(targetQueueChangeSignal);
        }
    }

    private void rollbackTargetQueueChangeSignalBindings(
            final List<? extends Shard> candidates, final Throwable failure) {
        for (int index = candidates.size() - 1; index >= 0; index--) {
            try {
                unbindTargetQueueChangeSignal(candidates.get(index));
            } catch (RuntimeException | Error rollbackFailure) {
                if (rollbackFailure != failure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
        }
    }

    /** Calls at most one Shard source turn while whole-fleet admission is open. */
    public synchronized TargetWorkerShardFleetRuntime.SourceTurn runNextSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        if (stopping) {
            throw new IllegalStateException("Target host source admission is stopping");
        }
        return fleet.runNextSourceTurn(budget, ownerClock);
    }

    /** Submits an unpublished candidate only through the currently admitted exact Worker Shard. */
    public TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final TargetWorkerShardRuntime expectedShard,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return withCheckpointAdmission(
                expectedShard,
                () -> expectedShard.submitLocalCheckpointCandidate(
                        intents, ownerClock, checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits));
    }

    /** Admits semantic companion creation through the exact host Shard lifecycle and protected Worker cut. */
    public TargetCheckpointCandidateWorkClassExecutor.Submission submitSemanticCheckpointCandidate(
            final TargetWorkerShardRuntime expectedShard, final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock, final Path checkpointPath, final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final Runnable protectionGuard) {
        return withCheckpointAdmission(expectedShard, () -> expectedShard.submitProtectedSemanticCheckpointCandidate(
                intents, ownerClock, checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits,
                snapshot, semanticLimits, protectionGuard));
    }

    /** Claims a selected Target head only on a currently admitted exact Shard instance. */
    public TargetClaimRecord claim(
            final TargetWorkerShardRuntime expectedShard,
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
        return withShardAdmission(
                expectedShard,
                () -> expectedShard.claim(
                        budget,
                        selected,
                        owner,
                        nowEpochMs,
                        deadlineEpochMs,
                        executionBytes,
                        operationDigest,
                        quota,
                        physicalWrites,
                        ownerClock));
    }

    /** Revokes one unadmitted Claim only through its currently admitted exact Shard instance. */
    public void revokeClaim(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final TargetClaimRecord expected,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        withShardAdmission(expectedShard, () -> {
            expectedShard.revokeClaim(budget, expected, operationDigest, quota, physicalWrites, ownerClock);
            return null;
        });
    }

    /** Rebuilds one source Shard's verified Target heads under exact host/Owner admission. */
    public TargetQueueSnapshotReader.Page scanTargetQueues(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final TargetPartitionId after,
            final int maximumTargets,
            final LongSupplier ownerClock) {
        return withShardAdmission(
                expectedShard, () -> expectedShard.scanTargetQueues(budget, after, maximumTargets, ownerClock));
    }

    /** Refreshes one Target summary only through its currently admitted source Shard. */
    public Optional<TargetQueueSnapshotReader.Entry> readTargetQueue(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final TargetPartitionId targetId,
            final LongSupplier ownerClock) {
        return withShardAdmission(expectedShard, () -> expectedShard.readTargetQueue(budget, targetId, ownerClock));
    }

    /** Reads one bounded persisted active-message total through the exact admitted source Shard. */
    public OptionalLong readTargetActiveMessages(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final TargetPartitionId targetId,
            final LongSupplier ownerClock) {
        return withShardAdmission(
                expectedShard,
                () -> expectedShard.readTargetActiveMessages(budget, targetId, ownerClock));
    }

    /** Fences a completed page sequence on the same exact source Shard. */
    public TargetQueueSnapshotReader.Cut readTargetQueueCut(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final LongSupplier ownerClock) {
        return withShardAdmission(expectedShard, () -> expectedShard.readTargetQueueCut(budget, ownerClock));
    }

    /** Rebuilds one bounded physical-Target inventory from every currently admitted source Shard. */
    public TargetWorkerTargetInventory.Result rebuildTargetInventory(
            final TargetWorkerTargetInventory.Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock) {
        return rebuildTargetInventory(limits, ownerClock, monotonicClock, null);
    }

    TargetWorkerTargetInventory.Result rebuildTargetInventory(
            final TargetWorkerTargetInventory.Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics) {
        final var current = currentTargetWorkers();
        synchronized (this) {
            pendingTargetInventory = null;
        }
        final var result =
                TargetWorkerTargetInventory.rebuild(this, current, limits, ownerClock, monotonicClock, metrics);
        synchronized (this) {
            pendingTargetInventory = result.snapshot();
        }
        return result;
    }

    /** Creates ordinary byte DRR from this Host's complete inventory; freeze its recovery pass before Claim. */
    public TargetWorkerOrdinaryDrr newOrdinaryDrr(
            final TargetWorkerTargetInventory.Result inventory,
            final TargetWorkerOrdinaryDrr.Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock) {
        return newOrdinaryDrr(inventory, limits, ownerClock, monotonicClock, null);
    }

    /** Creates ordinary byte DRR with optional bounded, best-effort process metrics. */
    public TargetWorkerOrdinaryDrr newOrdinaryDrr(
            final TargetWorkerTargetInventory.Result inventory,
            final TargetWorkerOrdinaryDrr.Limits limits,
            final LongSupplier ownerClock,
            final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics) {
        return new TargetWorkerOrdinaryDrr(
                this, consumeTargetInventory(inventory), limits, ownerClock, monotonicClock, metrics);
    }

    synchronized TargetWorkerTargetInventory.Snapshot consumeTargetInventory(
            final TargetWorkerTargetInventory.Result inventory) {
        final var complete = Objects.requireNonNull(inventory, "inventory");
        if (complete.stop() != TargetWorkerTargetInventory.Stop.COMPLETE) {
            throw new IllegalArgumentException("Target DRR requires a complete inventory");
        }
        if (complete.snapshot() != pendingTargetInventory) {
            throw new IllegalArgumentException("Target DRR inventory was not built by this Host");
        }
        pendingTargetInventory = null;
        return complete.snapshot();
    }

    synchronized List<TargetWorkerShardRuntime> currentTargetWorkers() {
        if (stopping) {
            throw new IllegalStateException("Target host inventory admission is stopping");
        }
        final List<TargetWorkerShardRuntime> current = new ArrayList<>();
        for (Shard shard : shards) {
            if (!withdrawn.contains(shard.shardId()) && !completed.containsKey(shard.shardId())) {
                if (!(shard instanceof TargetWorkerShardRuntime worker)) {
                    throw new IllegalStateException("Target host has a non-Worker Shard instance");
                }
                current.add(worker);
            }
        }
        return List.copyOf(current);
    }

    /** Lazily probes one head under exact host/Owner admission before fair byte-cost selection. */
    public TargetHeadCostProbe.Cost probeSelectedHead(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final LongSupplier ownerClock) {
        return withShardAdmission(expectedShard, () -> expectedShard.probeSelectedHead(budget, selected, ownerClock));
    }

    /** Configures the exact admitted Worker; asynchronous history is owned and bounded by the supplied provider. */
    public TargetPublishRecoveryMaintenance configurePublishRecoveryMaintenance(
            final TargetWorkerShardRuntime expectedShard, final TargetPublishOutcomeMutationFactory outcomes,
            final ShardLogMutationAppender appender, final Supplier<BoundedReadBudget> reads,
            final TargetPublishRecoveryMaintenance.History history, final LongSupplier ownerClock) {
        return withShardAdmission(expectedShard,
                () -> expectedShard.configurePublishRecoveryMaintenance(
                        outcomes, appender, reads, history, ownerClock));
    }

    /** Drives the same bounded recovery rotation used by the maintenance timer. */
    public Optional<TargetWorkerShardFleetRuntime.PublishRecoveryTurn> runNextPublishRecoveryTurn() {
        synchronized (this) {
            if (stopping) {
                throw new IllegalStateException("Target Host publish recovery admission is stopping");
            }
        }
        return fleet.runNextPublishRecoveryTurnIfPresent();
    }

    /** Test seam for the host lifecycle reservation without constructing a physical Target Store. */
    <T> T withCheckpointAdmission(final Shard expectedShard, final Supplier<T> admission) {
        return withShardAdmission(expectedShard, admission);
    }

    private <T> T withShardAdmission(final Shard expectedShard, final Supplier<T> admission) {
        Objects.requireNonNull(admission, "admission");
        final Shard shard;
        final ShardId shardId;
        synchronized (this) {
            shardId = Objects.requireNonNull(expectedShard, "shard").shardId();
            shard = requireShard(shardId);
            if (shard != expectedShard) {
                throw new IllegalArgumentException("Target host Shard instance has been replaced");
            }
            if (stopping || withdrawn.contains(shardId) || completed.containsKey(shardId)) {
                throw new IllegalStateException("Target host Shard admission is stopping or already in progress");
            }
            if (!draining.add(shardId)) {
                throw new ShardAdmissionBusyException(shardId);
            }
        }
        try {
            synchronized (shard) {
                return admission.get();
            }
        } finally {
            synchronized (this) {
                draining.remove(shardId);
                notifyAll();
            }
        }
    }

    /** Settles only an existing candidate, including after whole-host stop or Shard withdrawal. */
    public Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> settlePendingCheckpointTurn(
            final TargetWorkerShardRuntime expectedShard, final SchedulerBudget budget) {
        return settlePendingCheckpointTurn((Shard) expectedShard, budget);
    }

    /** Test seam; the exact Shard instance is reserved against concurrent drain and replacement. */
    Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> settlePendingCheckpointTurn(
            final Shard expectedShard, final SchedulerBudget budget) {
        return settlePendingCheckpointTurn(expectedShard, null, budget);
    }

    /** A scheduled claim may settle only the exact task returned by its own admission. */
    Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> settlePendingCheckpointTurn(
            final Shard expectedShard, final WorkClassTask expectedTask, final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        final Shard shard;
        final ShardId shardId;
        synchronized (this) {
            shardId = Objects.requireNonNull(expectedShard, "shard").shardId();
            shard = requireShard(shardId);
            if (shard != expectedShard) {
                throw new IllegalArgumentException("Target host checkpoint Shard instance has been replaced");
            }
            if (completed.containsKey(shardId) || !draining.add(shardId)) {
                throw new IllegalStateException("Target host checkpoint Shard is drained or already in progress");
            }
        }
        try {
            synchronized (shard) {
                final Optional<WorkClassTask> pending = shard.pendingCheckpointTask();
                if (expectedTask != null && (pending.isEmpty() || pending.orElseThrow() != expectedTask)) {
                    throw new IllegalStateException("Target host checkpoint task has changed");
                }
                return pending.isEmpty() ? Optional.empty() : shard.runCheckpointTurn(budget);
            }
        } finally {
            synchronized (this) {
                draining.remove(shardId);
                notifyAll();
            }
        }
    }

    /** Admits a new Shard or replaces a withdrawn instance after its Owner drain completes. */
    public void admitShard(final TargetWorkerShardRuntime shard) {
        admitShard(shard, shard);
    }

    /** Test seam for membership without constructing native Stores. */
    synchronized void admitShard(final Shard shard, final TargetWorkerShardFleetRuntime.ShardTurns turns) {
        if (stopping) {
            throw new IllegalStateException("Target host admission is stopping");
        }
        if (Objects.requireNonNull(shard, "shard") != Objects.requireNonNull(turns, "turns")) {
            throw new IllegalArgumentException("Target host source and maintenance instance differ");
        }
        final ShardId shardId = Objects.requireNonNull(shard.shardId(), "shardId");
        final int previousIndex = indexOfShard(shardId);
        if (previousIndex >= 0 && shards.get(previousIndex) == shard) {
            throw new IllegalArgumentException("Target host cannot readmit the same shard instance");
        }
        if (previousIndex >= 0
                && (!withdrawn.contains(shardId) || draining.contains(shardId) || !completed.containsKey(shardId))) {
            throw new IllegalStateException("Target host cannot replace a live or incompletely drained shard");
        }
        bindTargetQueueChangeSignal(shard);
        if (shard instanceof TargetWorkerShardRuntime worker && ordinaryTargetCacheLimit > 0) {
            worker.configureTargetQueueHeadCache(ordinaryTargetCacheLimit);
        }
        synchronized (fleet) {
            fleet.admit(turns);
            if (previousIndex < 0) {
                shards.add(shard);
            } else {
                shards.set(previousIndex, shard);
                withdrawn.remove(shardId);
                completed.remove(shardId);
            }
        }
    }

    /**
     * Stops all maintenance ticks before whole-host Store drain. Each call retries incomplete
     * Shards; pending source/GC/expiry and ordinary failures remain visible for a same-host retry.
     */
    public Result drainAll(
            final TargetOwnerDrainCoordinator.Request request,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourceBudget, "sourceBudget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        final TargetWorkerOrdinaryLoop currentOrdinaryLoop;
        synchronized (this) {
            if (drainAllActive) {
                throw new IllegalStateException("Target host whole-fleet drain is already active");
            }
            drainAllActive = true;
            stopping = true;
            currentOrdinaryLoop = ordinaryLoop;
        }

        try {
            if (currentOrdinaryLoop != null) {
                currentOrdinaryLoop.close();
            }
            maintenanceLoop.close();
            final List<Shard> drainShards;
            synchronized (this) {
                while (!draining.isEmpty()) {
                    try {
                        wait();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Target host drain interrupted while waiting for a shard", interrupted);
                    }
                }
                drainShards = List.copyOf(shards);
            }
            final var results = new ArrayList<ShardDrain>(drainShards.size());
            for (Shard shard : drainShards) {
                final ShardDrain prior;
                synchronized (this) {
                    prior = completed.get(shard.shardId());
                }
                final ShardDrain result = prior != null ? prior : drainOne(shard, request, sourceBudget, ownerClock);
                if (result.complete()) {
                    synchronized (this) {
                        completed.put(shard.shardId(), result);
                    }
                }
                results.add(result);
            }
            return new Result(results);
        } finally {
            synchronized (this) {
                drainAllActive = false;
                notifyAll();
            }
        }
    }

    /**
     * Withdraws one Shard from future source/maintenance selection, waits for its selected turn to exit,
     * then drains it while other Shards remain live. Pending source, GC, or expiry work is retried
     * with the same withdrawn Shard identity; the maintenance loop keeps serving the rest.
     */
    public ShardDrain drainShard(
            final TargetWorkerShardRuntime shard,
            final TargetOwnerDrainCoordinator.Request request,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        return drainShard((Shard) shard, request, sourceBudget, ownerClock);
    }

    /** The exact instance check fences a late withdrawal after the same ShardId is replaced. */
    ShardDrain drainShard(
            final Shard expectedShard,
            final TargetOwnerDrainCoordinator.Request request,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourceBudget, "sourceBudget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        final Shard shard;
        synchronized (this) {
            shard = requireShard(Objects.requireNonNull(expectedShard, "shard").shardId());
            if (shard != expectedShard) {
                throw new IllegalArgumentException("Target host shard instance has been replaced");
            }
            final ShardDrain prior = completed.get(shard.shardId());
            if (prior != null) {
                return prior;
            }
            final ShardId shardId = shard.shardId();
            if (stopping) {
                throw new IllegalStateException("Target host whole-fleet drain is stopping");
            }
            if (!draining.add(shardId)) {
                throw new IllegalStateException("Target host shard drain is already in progress");
            }
            if (!withdrawn.contains(shardId)) {
                try {
                    fleet.withdraw(shardId);
                    withdrawn.add(shardId);
                    targetQueueChangeSignal.signal();
                } catch (RuntimeException | Error failure) {
                    draining.remove(shardId);
                    notifyAll();
                    throw failure;
                }
            }
        }
        ShardDrain result = null;
        try {
            result = drainOne(shard, request, sourceBudget, ownerClock);
            return result;
        } finally {
            synchronized (this) {
                if (result != null && result.complete() && requireShard(shard.shardId()) == shard) {
                    completed.put(shard.shardId(), result);
                }
                draining.remove(shard.shardId());
                notifyAll();
            }
        }
    }

    private int indexOfShard(final ShardId shardId) {
        for (int index = 0; index < shards.size(); index++) {
            if (shardId.equals(shards.get(index).shardId())) {
                return index;
            }
        }
        return -1;
    }

    private Shard requireShard(final ShardId shardId) {
        final ShardId requested = Objects.requireNonNull(shardId, "shardId");
        return shards.stream()
                .filter(shard -> requested.equals(shard.shardId()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Target host does not contain shard " + requested));
    }

    private static ShardDrain drainOne(
            final Shard shard,
            final TargetOwnerDrainCoordinator.Request request,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        synchronized (shard) {
            SourceApplyCoordinator.TurnResult sourceTurn = null;
            try {
                final Optional<WorkClassTask> checkpoint = shard.pendingCheckpointTask();
                if (checkpoint.isPresent()) {
                    return new ShardDrain(
                            shard.shardId(), Status.PENDING_CHECKPOINT, null, checkpoint.orElseThrow(), null, null);
                }
                final Optional<WorkClassTask> pendingExpiry = shard.settlePendingMessageExpiryForDrain(
                        request.gcBudget(), sourceBudget, ownerClock);
                if (pendingExpiry.isPresent()) {
                    return new ShardDrain(
                            shard.shardId(),
                            Status.PENDING_MESSAGE_EXPIRY,
                            null,
                            null,
                            null,
                            null,
                            pendingExpiry.orElseThrow());
                }
                if (shard.pendingSourceEntry().isPresent()) {
                    sourceTurn = shard.settlePendingSourceTurn(sourceBudget, ownerClock)
                            .orElse(null);
                    if (shard.pendingSourceEntry().isPresent()) {
                        return new ShardDrain(shard.shardId(), Status.PENDING_SOURCE, sourceTurn, null, null, null);
                    }
                }
                final TargetOwnerDrainCoordinator.Result drained = shard.drain(request, ownerClock);
                return new ShardDrain(
                        shard.shardId(), map(drained.status()), sourceTurn, null, drained.pendingGcTask(), null);
            } catch (RuntimeException failure) {
                return new ShardDrain(shard.shardId(), Status.FAILED, sourceTurn, null, null, failure);
            }
        }
    }

    public synchronized boolean stopping() {
        return stopping;
    }

    public Throwable firstMaintenanceFailure() {
        return maintenanceLoop.firstFailure();
    }

    private static Status map(final TargetOwnerDrainCoordinator.Status status) {
        return switch (status) {
            case PENDING_GC -> Status.PENDING_GC;
            case RELEASED -> Status.RELEASED;
            case UNCERTAIN_RELEASED -> Status.UNCERTAIN_RELEASED;
            case OWNER_LOST_CLOSED -> Status.OWNER_LOST_CLOSED;
        };
    }
}
