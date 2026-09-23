package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Owns whole-fleet source admission, maintenance ticks and ordered local shutdown. */
public final class TargetWorkerHostRuntime {
    interface Shard {
        ShardId shardId();

        Optional<SourceReplayEntry> pendingSourceEntry();

        Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
                SchedulerBudget budget, LongSupplier ownerClock);

        TargetOwnerDrainCoordinator.Result drain(TargetOwnerDrainCoordinator.Request request, LongSupplier clock);
    }

    public enum Status {
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
            WorkClassTask pendingGcTask,
            RuntimeException failure) {
        public ShardDrain {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(status, "status");
            if ((status == Status.FAILED) != (failure != null)
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
        this.shards = List.copyOf(Objects.requireNonNull(shards, "shards"));
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

    /**
     * Stops all maintenance ticks before touching any Store. Each call attempts every Shard;
     * pending source/GC and ordinary failures remain visible for a same-host retry.
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
        final var results = new ArrayList<ShardDrain>(shards.size());
        for (Shard shard : shards) {
            SourceApplyCoordinator.TurnResult sourceTurn = null;
            try {
                if (shard.pendingSourceEntry().isPresent()) {
                    sourceTurn = shard.settlePendingSourceTurn(sourceBudget, ownerClock)
                            .orElse(null);
                    if (shard.pendingSourceEntry().isPresent()) {
                        results.add(new ShardDrain(shard.shardId(), Status.PENDING_SOURCE, sourceTurn, null, null));
                        continue;
                    }
                }
                final TargetOwnerDrainCoordinator.Result drained = shard.drain(request, ownerClock);
                results.add(new ShardDrain(
                        shard.shardId(), map(drained.status()), sourceTurn, drained.pendingGcTask(), null));
            } catch (RuntimeException failure) {
                results.add(new ShardDrain(shard.shardId(), Status.FAILED, sourceTurn, null, failure));
            }
        }
        return new Result(results);
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
