package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

/** Gives every admitted Target shard a bounded source and reservation GC turn in rotation. */
public final class TargetWorkerShardFleetRuntime {
    interface ShardTurns {
        ShardId shardId();

        void requireFleetComposition(WorkClassExecutionRegistry workClasses, SharedRocksDbResources resources);

        SourceApplyCoordinator.TurnResult runSourceTurn(SchedulerBudget budget, LongSupplier ownerClock);

        TargetReservationGcRuntime.Turn runMaintenanceTurn(SchedulerBudget budget);
    }

    public record SourceTurn(ShardId shardId, SourceApplyCoordinator.TurnResult result) {
        public SourceTurn {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(result, "result");
        }
    }

    public record MaintenanceTurn(ShardId shardId, TargetReservationGcRuntime.Turn result) {
        public MaintenanceTurn {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(result, "result");
        }
    }

    /** The selected shard is context, not proof that its action caused a shared dispatcher failure. */
    public static final class MaintenanceDispatchFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final byte[] routeIncarnation;
        private final int partition;

        private MaintenanceDispatchFailure(final ShardId selectedShardId, final RuntimeException cause) {
            super("Target maintenance dispatch failed while selecting shard " + selectedShardId, cause);
            final var selected = Objects.requireNonNull(selectedShardId, "selectedShardId");
            routeIncarnation = selected.routeIncarnation().bytes();
            partition = selected.partition();
        }

        public ShardId selectedShardId() {
            return new ShardId(new RouteIncarnation(routeIncarnation), partition);
        }
    }

    private final List<ShardTurns> shards;
    private final WorkClassExecutionRegistry workClasses;
    private final SharedRocksDbResources resources;
    private int sourceCursor;
    private int maintenanceCursor;
    private Thread activeTurnThread;

    public TargetWorkerShardFleetRuntime(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shardRuntimes) {
        this(workClasses, resources, (Iterable<? extends ShardTurns>) Objects.requireNonNull(shardRuntimes, "shards"));
    }

    /** Test seam for fleet ordering without creating native Stores for every scheduling assertion. */
    TargetWorkerShardFleetRuntime(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final ShardTurns... shardRuntimes) {
        this(workClasses, resources, Arrays.asList(Objects.requireNonNull(shardRuntimes, "shards")));
    }

    private TargetWorkerShardFleetRuntime(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final Iterable<? extends ShardTurns> shardRuntimes) {
        final var exactClasses = Objects.requireNonNull(workClasses, "workClasses");
        final var exactResources = Objects.requireNonNull(resources, "resources");
        final var admitted = new ArrayList<ShardTurns>();
        final Set<ShardId> identities = new HashSet<>();
        for (ShardTurns runtime : shardRuntimes) {
            final var candidate = Objects.requireNonNull(runtime, "shard runtime");
            candidate.requireFleetComposition(exactClasses, exactResources);
            if (!identities.add(candidate.shardId())) {
                throw new IllegalArgumentException("Target Worker fleet contains a duplicate shard");
            }
            admitted.add(candidate);
        }
        if (admitted.isEmpty()) {
            throw new IllegalArgumentException("Target Worker fleet requires at least one shard");
        }
        this.workClasses = exactClasses;
        this.resources = exactResources;
        shards = new ArrayList<>(admitted);
    }

    public synchronized List<ShardId> shardIds() {
        return shards.stream().map(ShardTurns::shardId).toList();
    }

    /** Admits one exact Worker-graph instance under the same lock as both dispatch cursors. */
    synchronized void admit(final ShardTurns runtime) {
        requireNotInSelectedTurn();
        final var candidate = Objects.requireNonNull(runtime, "shard runtime");
        candidate.requireFleetComposition(workClasses, resources);
        if (shards.stream().anyMatch(shard -> shard.shardId().equals(candidate.shardId()))) {
            throw new IllegalArgumentException("Target Worker fleet contains a duplicate shard");
        }
        shards.add(candidate);
    }

    /**
     * Removes one Shard under the same lock as source and GC dispatch. The return boundary proves
     * its previously selected turn has exited; later turns cannot select it again.
     */
    synchronized void withdraw(final ShardId shardId) {
        if (activeTurnThread == Thread.currentThread()) {
            throw new IllegalStateException("cannot withdraw a Target shard from its selected turn");
        }
        final ShardId requested = Objects.requireNonNull(shardId, "shardId");
        for (int index = 0; index < shards.size(); index++) {
            if (requested.equals(shards.get(index).shardId())) {
                shards.remove(index);
                sourceCursor = afterRemoval(sourceCursor, index, shards.size());
                maintenanceCursor = afterRemoval(maintenanceCursor, index, shards.size());
                return;
            }
        }
        throw new IllegalArgumentException("Target Worker fleet does not contain shard " + requested);
    }

    private static int afterRemoval(final int cursor, final int removed, final int remaining) {
        if (remaining == 0) {
            return 0;
        }
        final int next = removed < cursor ? cursor - 1 : cursor;
        return next == remaining ? 0 : next;
    }

    public synchronized SourceTurn runNextSourceTurn(final SchedulerBudget budget, final LongSupplier ownerClock) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        requireNotInSelectedTurn();
        if (shards.isEmpty()) {
            throw new IllegalStateException("Target Worker fleet has no active source shard");
        }
        final var selected = shards.get(sourceCursor);
        sourceCursor = sourceCursor == shards.size() - 1 ? 0 : sourceCursor + 1;
        activeTurnThread = Thread.currentThread();
        try {
            return new SourceTurn(selected.shardId(), selected.runSourceTurn(budget, ownerClock));
        } finally {
            activeTurnThread = null;
        }
    }

    public synchronized MaintenanceTurn runNextMaintenanceTurn(final SchedulerBudget budget) {
        return runNextMaintenanceTurnIfPresent(budget)
                .orElseThrow(() -> new IllegalStateException("Target Worker fleet has no active maintenance shard"));
    }

    /** An empty fleet gives the maintenance timer a quiet turn after its final Shard withdraws. */
    synchronized Optional<MaintenanceTurn> runNextMaintenanceTurnIfPresent(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        requireNotInSelectedTurn();
        if (shards.isEmpty()) {
            return Optional.empty();
        }
        final var selected = shards.get(maintenanceCursor);
        maintenanceCursor = maintenanceCursor == shards.size() - 1 ? 0 : maintenanceCursor + 1;
        activeTurnThread = Thread.currentThread();
        try {
            return Optional.of(new MaintenanceTurn(selected.shardId(), selected.runMaintenanceTurn(budget)));
        } catch (RuntimeException failure) {
            throw new MaintenanceDispatchFailure(selected.shardId(), failure);
        } finally {
            activeTurnThread = null;
        }
    }

    private void requireNotInSelectedTurn() {
        if (activeTurnThread == Thread.currentThread()) {
            throw new IllegalStateException("cannot reenter Target fleet dispatch from its selected turn");
        }
    }
}
