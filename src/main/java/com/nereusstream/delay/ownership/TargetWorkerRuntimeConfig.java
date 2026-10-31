package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerCapacityAdmission;
import com.nereusstream.delay.store.WorkerResourceEnvelope;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** One explicit Target Worker configuration; no guessed resource defaults or publication/placement authority. */
public record TargetWorkerRuntimeConfig(
        ShardStoreConfig store,
        WorkerResourceEnvelope resources,
        CapacityVector hardCaps,
        WorkClassRuntimeConfig workClasses,
        TargetSourceApplyRuntime.Limits source,
        long maximumSourceFrameBytes,
        SystemWriterAppendBudget.Limits systemWriter,
        TargetSourceBacklog.Limits backlogRead,
        TargetSourceBacklogAdmission.Limits backlogGrowth,
        TargetWorkerTargetInventory.Limits inventory,
        TargetWorkerOrdinaryDrr.Limits ordinary,
        SchedulerBudget ordinaryTurn,
        Duration ordinaryRecheckInterval,
        SchedulerBudget maintenanceTurn,
        Duration maintenanceInterval,
        TargetCheckpointRuntimeConfig checkpoint) {
    public TargetWorkerRuntimeConfig {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(hardCaps, "hardCaps");
        Objects.requireNonNull(workClasses, "workClasses");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(systemWriter, "systemWriter");
        Objects.requireNonNull(backlogRead, "backlogRead");
        Objects.requireNonNull(backlogGrowth, "backlogGrowth");
        Objects.requireNonNull(inventory, "inventory");
        Objects.requireNonNull(ordinary, "ordinary");
        Objects.requireNonNull(ordinaryTurn, "ordinaryTurn");
        Objects.requireNonNull(ordinaryRecheckInterval, "ordinaryRecheckInterval");
        Objects.requireNonNull(maintenanceTurn, "maintenanceTurn");
        Objects.requireNonNull(maintenanceInterval, "maintenanceInterval");
        Objects.requireNonNull(checkpoint, "checkpoint").requireWorkClasses(workClasses);
        resources.validate(store);
        WorkerCapacityAdmission.requireFits(resources, hardCaps, List.of(),
                CapacityVector.empty(), CapacityVector.empty());
        if (backlogRead.maximumFrameBytes() != maximumSourceFrameBytes
                || backlogGrowth.bytes() < maximumSourceFrameBytes) {
            throw new IllegalArgumentException("Source backlog configuration cannot serve the declared frame");
        }
        if (systemWriter.maximumFrameBytes() != maximumSourceFrameBytes
                || systemWriter.writers() > hardCaps.amount(CapacityDimension.ADAPTER_PRODUCERS)) {
            throw new IllegalArgumentException("system writer limits differ from configured Source/producer envelope");
        }
        for (var dimension : List.of(CapacityDimension.SYSTEM_WRITER_RESERVED_RECORDS,
                CapacityDimension.PHYSICAL_REQUESTS, CapacityDimension.ZOMBIE_REQUESTS)) {
            if (systemWriter.records() > hardCaps.amount(dimension)) {
                throw new IllegalArgumentException("system writer records exceed " + dimension);
            }
        }
        for (var dimension : List.of(CapacityDimension.SYSTEM_WRITER_RESERVED_BYTES,
                CapacityDimension.PHYSICAL_BYTES, CapacityDimension.ZOMBIE_BYTES)) {
            if (systemWriter.bytes() > hardCaps.amount(dimension)) {
                throw new IllegalArgumentException("system writer bytes exceed " + dimension);
            }
        }
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
        requireFiniteCounts(source.records(), source.counters(), inventory.maximumShards(), inventory.maximumTargets(),
                inventory.pageTargets(), inventory.maximumPagesPerShard(), inventory.readRecords(),
                ordinary.maximumVisitsPerTurn(), ordinary.readRecords(), ordinaryTurn.maxMessages(),
                maintenanceTurn.maxMessages(), checkpoint.settlementTurn().maxMessages());
        requireFiniteAmounts(source.bytes(), source.elapsedNanos(), inventory.readBytes(), inventory.readElapsedNanos(),
                ordinary.quantumBytes(), ordinary.maximumCostBytes(), ordinary.sendEnvelopeBytes(),
                ordinary.readBytes(), ordinary.readElapsedNanos(), ordinaryTurn.maxBytes(), ordinaryTurn.maxElapsedNanos(),
                maintenanceTurn.maxBytes(), maintenanceTurn.maxElapsedNanos(), ordinaryRecheckInterval.toNanos(),
                maintenanceInterval.toNanos(), checkpoint.settlementTurn().maxBytes(),
                checkpoint.settlementTurn().maxElapsedNanos(), workClasses.maxEventLoopClassDelayNanos(),
                workClasses.maxBorrowedResourceHoldNanos(), workClasses.totalResourceRecords(), workClasses.totalResourceBytes());
        for (var policy : workClasses.policies().values()) {
            requireFiniteCounts(policy.weight(), policy.maxQueueRecords(), policy.maxRecordsPerTurn());
            requireFiniteAmounts(policy.maxQueueBytes(), policy.maxBytesPerTurn(), policy.maxTimePerTurnNanos(),
                    policy.nonBorrowableMinimumRecords(), policy.nonBorrowableMinimumBytes());
        }
        final var sourcePolicy = workClasses.policies().get(WorkClass.SOURCE_APPLY);
        if (maximumSourceFrameBytes <= 0 || maximumSourceFrameBytes >= Integer.MAX_VALUE
                || sourcePolicy.maxBytesPerTurn() < maximumSourceApplyTaskBytes(maximumSourceFrameBytes)
                || sourcePolicy.maxTimePerTurnNanos() < source.elapsedNanos()
                || workClasses.maxBorrowedResourceHoldNanos() < source.elapsedNanos()) {
            throw new IllegalArgumentException("SOURCE_APPLY policy is below the source action envelope");
        }
        workClasses.requireTaskServiceable(new WorkClassTask(WorkClass.SOURCE_APPLY,
                "target-configured-source-apply", maximumSourceApplyTaskBytes(maximumSourceFrameBytes)));
        workClasses.requireTaskServiceable(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "target-configured-system-frame", maximumSourceFrameBytes));
        final var duePolicy = workClasses.policies().get(WorkClass.DUE_SCHEDULER);
        if (duePolicy.maxBytesPerTurn() < ordinary.sendEnvelopeBytes()
                || duePolicy.maxTimePerTurnNanos() < ordinary.readElapsedNanos()) {
            throw new IllegalArgumentException("DUE_SCHEDULER policy cannot serve the configured ordinary envelope");
        }
        workClasses.requireTaskServiceable(new WorkClassTask(WorkClass.DUE_SCHEDULER,
                "target-configured-ordinary-send", ordinary.sendEnvelopeBytes()));
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

    public SystemWriterAppendBudget newSystemWriterBudget(
            WorkClassExecutionRegistry classes, SharedRocksDbResources sharedResources) {
        requireRuntime(classes, sharedResources);
        return new SystemWriterAppendBudget(systemWriter, classes, sharedResources);
    }

    public TargetPublishAdmissionMutationFactory newPublishAdmissionFactory(
            final int keyVersion, final java.security.PrivateKey key) {
        return new TargetPublishAdmissionMutationFactory(keyVersion, key, maximumSourceFrameBytes);
    }

    public TargetChannelControlMutationFactory newChannelControlFactory(final int keyVersion,
            final java.security.PrivateKey key, final long queryPolicyVersion) {
        return new TargetChannelControlMutationFactory(keyVersion, key, queryPolicyVersion, maximumSourceFrameBytes);
    }

    public TargetSourceBacklogAdmission newSourceIngressAdmission(
            Function<ShardId, TargetSourceBacklog.Snapshot> snapshots, TrustedClock clock, LongSupplier nanos,
            BiConsumer<TargetSourceBacklog.Snapshot, Integer> protection) {
        return new TargetSourceBacklogAdmission(backlogGrowth, snapshots, clock, nanos, protection);
    }

    public TargetSourceBacklogCache newSourceBacklogCache(ThreadPoolExecutor executor, LongSupplier nanos) {
        return new TargetSourceBacklogCache(store.maxOwnedShards(), backlogGrowth.maximumObservationAgeNanos(),
                executor, nanos);
    }

    public SchedulerBudget sourceTurn() {
        final var policy = workClasses.policies().get(WorkClass.SOURCE_APPLY);
        return new SchedulerBudget(policy.maxRecordsPerTurn(), policy.maxBytesPerTurn(), policy.maxTimePerTurnNanos());
    }

    /** SourceApplyWorkClassExecutor charges the full Source Position as well as the retained frame. */
    public long maximumSourceApplyTaskBytes() {
        return maximumSourceApplyTaskBytes(maximumSourceFrameBytes);
    }

    private static long maximumSourceApplyTaskBytes(long maximumFrameBytes) {
        return Math.addExact(maximumFrameBytes, TargetSourcePosition.MAX_CANONICAL_BYTES);
    }

    private static void requireFiniteCounts(int... values) {
        for (int value : values) {
            if (value == Integer.MAX_VALUE) throw new IllegalArgumentException("Target scheduling/read count is unbounded");
        }
    }

    private static void requireFiniteAmounts(long... values) {
        for (long value : values) {
            if (value == Long.MAX_VALUE) throw new IllegalArgumentException("Target scheduling/read byte or time bound is unbounded");
        }
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
