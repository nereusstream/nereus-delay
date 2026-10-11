package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerCapacityAdmission;
import com.nereusstream.delay.store.WorkerResourceEnvelope;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** One explicit Target Worker configuration; no guessed resource defaults or publication/placement authority. */
public record TargetWorkerRuntimeConfig(
        ShardStoreConfig store,
        WorkerResourceEnvelope resources,
        CapacityVector hardCaps,
        WorkClassRuntimeConfig workClasses,
        TargetSourceApplyRuntime.Limits source,
        long maximumSourceFrameBytes,
        TargetWorkerTargetInventory.Limits inventory,
        TargetWorkerOrdinaryDrr.Limits ordinary,
        SchedulerBudget ordinaryTurn,
        Duration ordinaryRecheckInterval,
        SchedulerBudget maintenanceTurn,
        Duration maintenanceInterval) {
    public TargetWorkerRuntimeConfig {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(hardCaps, "hardCaps");
        Objects.requireNonNull(workClasses, "workClasses");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(inventory, "inventory");
        Objects.requireNonNull(ordinary, "ordinary");
        Objects.requireNonNull(ordinaryTurn, "ordinaryTurn");
        Objects.requireNonNull(ordinaryRecheckInterval, "ordinaryRecheckInterval");
        Objects.requireNonNull(maintenanceTurn, "maintenanceTurn");
        Objects.requireNonNull(maintenanceInterval, "maintenanceInterval");
        resources.validate(store);
        WorkerCapacityAdmission.requireFits(resources, hardCaps, List.of(),
                CapacityVector.empty(), CapacityVector.empty());
        if (inventory.maximumShards() != store.maxOwnedShards()) {
            throw new IllegalArgumentException("Target inventory Shard limit must equal the Worker ownership limit");
        }
        if (inventory.pageTargets() > inventory.maximumTargets()
                || (long) inventory.pageTargets() * inventory.maximumPagesPerShard() < inventory.maximumTargets()) {
            throw new IllegalArgumentException("Target inventory/page limits cannot cover the admitted Target ring");
        }
        if (ordinaryTurn.maxBytes() < ordinary.sendEnvelopeBytes()
                || ordinaryTurn.maxElapsedNanos() < ordinary.readElapsedNanos()
                || ordinaryRecheckInterval.isZero() || ordinaryRecheckInterval.isNegative()
                || maintenanceInterval.isZero() || maintenanceInterval.isNegative()) {
            throw new IllegalArgumentException("ordinary turn/recheck cannot serve its configured envelope");
        }
        try {
            ordinaryRecheckInterval.toNanos();
            maintenanceInterval.toNanos();
            final long creditRounds = 1 + (ordinary.maximumCostBytes() - 1) / ordinary.quantumBytes();
            Math.multiplyExact((long) inventory.maximumTargets(), creditRounds);
            final long summaries = Math.multiplyExact((long) store.maxOwnedShards(), inventory.maximumTargets());
            Math.multiplyExact(Math.multiplyExact(2L, source.domains()), summaries);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Target service/recheck bounds overflow", overflow);
        }
        final var sourcePolicy = workClasses.policies().get(WorkClass.SOURCE_APPLY);
        if (maximumSourceFrameBytes <= 0 || maximumSourceFrameBytes == Long.MAX_VALUE
                || sourcePolicy.maxBytesPerTurn() < maximumSourceFrameBytes
                || sourcePolicy.maxTimePerTurnNanos() < source.elapsedNanos()) {
            throw new IllegalArgumentException("SOURCE_APPLY policy is below the source action envelope");
        }
        final var duePolicy = workClasses.policies().get(WorkClass.DUE_SCHEDULER);
        if (duePolicy.maxBytesPerTurn() < ordinary.sendEnvelopeBytes()
                || duePolicy.maxTimePerTurnNanos() < ordinary.readElapsedNanos()) {
            throw new IllegalArgumentException("DUE_SCHEDULER policy cannot serve the configured ordinary envelope");
        }
    }

    /** One checked process-level admission over actual external Shard envelopes; never reconstruct per message. */
    public void requireCapacity(List<ShardCapacityEnvelope> shards, CapacityVector fixedCost,
            CapacityVector transitionDemand) {
        if (Objects.requireNonNull(shards, "shards").size() > store.maxOwnedShards()) {
            throw new IllegalArgumentException("committed Shard envelopes exceed Worker ownership limit");
        }
        WorkerCapacityAdmission.requireFits(resources, hardCaps, shards, fixedCost, transitionDemand);
    }

    /** Real Linux process/container probe is required before allocating native shared resources. */
    public SharedRocksDbResources openResources() {
        return SharedRocksDbResources.withRuntimeProbe(store, resources);
    }

    public WorkClassExecutionRegistry newWorkClasses(LongSupplier clock) {
        return new WorkClassExecutionRegistry(workClasses, Objects.requireNonNull(clock, "clock"));
    }

    public SchedulerBudget sourceTurn() {
        final var policy = workClasses.policies().get(WorkClass.SOURCE_APPLY);
        return new SchedulerBudget(policy.maxRecordsPerTurn(), policy.maxBytesPerTurn(), policy.maxTimePerTurnNanos());
    }

    /** Rejects mixed configurations before Host schedulers or queue wakeups are admitted. */
    public void requireRuntime(WorkClassExecutionRegistry classes, SharedRocksDbResources sharedResources) {
        Objects.requireNonNull(classes, "classes").requireConfiguration(workClasses);
        Objects.requireNonNull(sharedResources, "sharedResources").requireWorkerConfiguration(store, resources);
    }

    /** Upper bounds on retained per-Shard summaries and ordinary/native domain heads; not a measured cost claim. */
    public long maximumTargetSummaries() {
        return Math.multiplyExact((long) store.maxOwnedShards(), inventory.maximumTargets());
    }

    public long maximumDomainHeads() {
        return Math.multiplyExact(Math.multiplyExact(2L, source.domains()), maximumTargetSummaries());
    }
}
