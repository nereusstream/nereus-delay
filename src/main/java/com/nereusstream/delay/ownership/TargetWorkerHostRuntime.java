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
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Owns whole-fleet source admission, maintenance ticks and ordered local shutdown. */
public final class TargetWorkerHostRuntime {
    interface Shard {
        ShardId shardId();

        Optional<SourceReplayEntry> pendingSourceEntry();

        Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
                SchedulerBudget budget, LongSupplier ownerClock);

        Optional<WorkClassTask> pendingCheckpointTask();

        Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> runCheckpointTurn(SchedulerBudget budget);

        TargetOwnerDrainCoordinator.Result drain(TargetOwnerDrainCoordinator.Request request, LongSupplier clock);
    }

    public enum Status {
        PENDING_CHECKPOINT,
        PENDING_SOURCE,
        PENDING_GC,
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
            RuntimeException failure) {
        public ShardDrain {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(status, "status");
            if ((status == Status.FAILED) != (failure != null)
                    || (status == Status.PENDING_CHECKPOINT) != (pendingCheckpointTask != null)
                    || (status == Status.PENDING_GC) != (pendingGcTask != null)) {
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
    private final Set<ShardId> withdrawn = new HashSet<>();
    private final Set<ShardId> draining = new HashSet<>();
    private final Map<ShardId, ShardDrain> completed = new HashMap<>();
    private boolean stopping;

    /** Starts bounded reservation GC ticks for one exact Worker graph. */
    public static TargetWorkerHostRuntime start(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shards,
            final SchedulerBudget maintenanceBudget,
            final Duration maintenanceInterval,
            final Consumer<Throwable> failureConsumer) {
        final var exactShards = List.copyOf(Objects.requireNonNull(shards, "shards"));
        final var fleet = new TargetWorkerShardFleetRuntime(workClasses, resources, exactShards);
        final var loop =
                TargetWorkerMaintenanceLoop.start(fleet, maintenanceBudget, maintenanceInterval, failureConsumer);
        return new TargetWorkerHostRuntime(fleet, loop, exactShards);
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

    /** Lazily probes one head under exact host/Owner admission before fair byte-cost selection. */
    public TargetHeadCostProbe.Cost probeSelectedHead(
            final TargetWorkerShardRuntime expectedShard,
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final LongSupplier ownerClock) {
        return withShardAdmission(expectedShard, () -> expectedShard.probeSelectedHead(budget, selected, ownerClock));
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
            if (stopping || withdrawn.contains(shardId) || completed.containsKey(shardId) || !draining.add(shardId)) {
                throw new IllegalStateException("Target host Shard admission is stopping or already in progress");
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
     * Shards; pending source/GC and ordinary failures remain visible for a same-host retry.
     */
    public synchronized Result drainAll(
            final TargetOwnerDrainCoordinator.Request request,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(sourceBudget, "sourceBudget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        stopping = true;
        maintenanceLoop.close();
        while (!draining.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Target host drain interrupted while waiting for a shard", interrupted);
            }
        }
        final var results = new ArrayList<ShardDrain>(shards.size());
        for (Shard shard : shards) {
            final ShardDrain prior = completed.get(shard.shardId());
            final ShardDrain result = prior != null ? prior : drainOne(shard, request, sourceBudget, ownerClock);
            if (result.complete()) {
                completed.put(shard.shardId(), result);
            }
            results.add(result);
        }
        return new Result(results);
    }

    /**
     * Withdraws one Shard from future source/GC selection, waits for its selected turn to exit,
     * then drains it while other Shards remain live. A pending result is retried with the same
     * withdrawn Shard identity; the maintenance loop keeps serving the rest of the fleet.
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
