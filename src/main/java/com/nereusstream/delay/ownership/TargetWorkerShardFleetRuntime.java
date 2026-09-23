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
    private int sourceCursor;
    private int maintenanceCursor;

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
        shards = List.copyOf(admitted);
    }

    public synchronized List<ShardId> shardIds() {
        return shards.stream().map(ShardTurns::shardId).toList();
    }

    public synchronized SourceTurn runNextSourceTurn(final SchedulerBudget budget, final LongSupplier ownerClock) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        final var selected = shards.get(sourceCursor);
        sourceCursor = sourceCursor == shards.size() - 1 ? 0 : sourceCursor + 1;
        return new SourceTurn(selected.shardId(), selected.runSourceTurn(budget, ownerClock));
    }

    public synchronized MaintenanceTurn runNextMaintenanceTurn(final SchedulerBudget budget) {
        Objects.requireNonNull(budget, "budget");
        final var selected = shards.get(maintenanceCursor);
        maintenanceCursor = maintenanceCursor == shards.size() - 1 ? 0 : maintenanceCursor + 1;
        try {
            return new MaintenanceTurn(selected.shardId(), selected.runMaintenanceTurn(budget));
        } catch (RuntimeException failure) {
            throw new MaintenanceDispatchFailure(selected.shardId(), failure);
        }
    }
}
