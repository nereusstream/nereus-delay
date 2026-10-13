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
    /** Process-local close permission. Only this Host can create it after fencing its admitted Stores. */
    public static final class BackendClosePermit {
        private final TargetWorkerHostRuntime host;
        private final TargetOwnedBackendGroup backend;
        private final long generation;
        private final List<Object> registrations;

        private BackendClosePermit(TargetWorkerHostRuntime host, TargetOwnedBackendGroup backend,
                long generation, List<Object> registrations) {
            this.host = host;
            this.backend = backend;
            this.generation = generation;
            this.registrations = List.copyOf(registrations);
        }

        public void requireOriginalGroup(TargetOwnedBackendGroup expected, List<Object> frozen) {
            synchronized (host) {
                if (backend != expected || host.quarantineGeneration != generation || !host.backendBarrierEstablished
                        || !host.ownedBackends.contains(expected) || registrations.size() != frozen.size()) {
                    throw new IllegalStateException("backend close lacks its original Host quarantine barrier");
                }
                for (int i = 0; i < registrations.size(); i++) {
                    if (registrations.get(i) != frozen.get(i)) {
                        throw new IllegalStateException("backend close registration set changed");
                    }
                }
            }
        }
    }

    public enum BackendQuarantineStatus { QUARANTINED, BACKEND_TERMINATED }

    public record BackendQuarantineResult(BackendQuarantineStatus status, List<Throwable> failures) {
        public BackendQuarantineResult { failures = List.copyOf(failures); }
    }

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

        default void requestCheckpointRegistrationStop() {}

        default void requestCheckpointPreparationStop() { requestCheckpointRegistrationStop(); }

        default Optional<WorkClassTask> settlePendingGcForDrain(SchedulerBudget budget) { return Optional.empty(); }

        default Optional<WorkClassTask> settlePendingChannelControlForDrain(SchedulerBudget workBudget,
                SchedulerBudget sourceBudget, LongSupplier clock) { return Optional.empty(); }

        default Optional<WorkClassTask> settlePendingPublicationForDrain(SchedulerBudget workBudget,
                SchedulerBudget sourceBudget, LongSupplier clock) { return Optional.empty(); }

        default Optional<WorkClassTask> settlePendingPublicationResourcesForDrain(){return Optional.empty();}

        default boolean canPublishFinalCheckpoint() { return true; }

        default long checkpointStoreSequence() { return 0; }

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
        PENDING_SOURCE_OBSERVATION,
        PENDING_CHECKPOINT,
        PENDING_SOURCE,
        PENDING_GC,
        PENDING_MESSAGE_EXPIRY,
        PENDING_CHANNEL_CONTROL,
        PENDING_PUBLICATION,
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
            WorkClassTask pendingMessageExpiryTask,
            WorkClassTask pendingChannelControlTask,
            WorkClassTask pendingPublicationTask) {
        public ShardDrain(final ShardId shardId, final Status status,
                final SourceApplyCoordinator.TurnResult sourceTurn, final WorkClassTask pendingCheckpointTask,
                final WorkClassTask pendingGcTask, final RuntimeException failure,
                final WorkClassTask pendingMessageExpiryTask, final WorkClassTask pendingChannelControlTask) {
            this(shardId, status, sourceTurn, pendingCheckpointTask, pendingGcTask, failure,
                    pendingMessageExpiryTask, pendingChannelControlTask, null);
        }
        public ShardDrain(final ShardId shardId, final Status status,
                final SourceApplyCoordinator.TurnResult sourceTurn, final WorkClassTask pendingCheckpointTask,
                final WorkClassTask pendingGcTask, final RuntimeException failure,
                final WorkClassTask pendingMessageExpiryTask) {
            this(shardId, status, sourceTurn, pendingCheckpointTask, pendingGcTask, failure,
                    pendingMessageExpiryTask, null);
        }
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
                    || (status == Status.PENDING_MESSAGE_EXPIRY) != (pendingMessageExpiryTask != null)
                    || (status == Status.PENDING_CHANNEL_CONTROL) != (pendingChannelControlTask != null)
                    || (status == Status.PENDING_PUBLICATION) != (pendingPublicationTask != null)) {
                throw new IllegalArgumentException("Target host drain result has inconsistent evidence");
            }
        }

        public boolean complete() {
            return status == Status.RELEASED
                    || status == Status.UNCERTAIN_RELEASED
                    || status == Status.OWNER_LOST_CLOSED;
        }
    }

    public record Result(List<ShardDrain> shards, boolean ownedBackendsTerminated, boolean oxiaTerminated) {
        public Result(List<ShardDrain> shards) { this(shards, true, true); }
        public Result(List<ShardDrain> shards, boolean ownedBackendsTerminated) { this(shards, ownedBackendsTerminated, true); }
        public Result {
            shards = List.copyOf(Objects.requireNonNull(shards, "shards"));
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("Target host drain requires shards");
            }
        }

        public boolean complete() {
            return ownedBackendsTerminated && oxiaTerminated && shards.stream().allMatch(ShardDrain::complete);
        }
    }

    private final TargetWorkerShardFleetRuntime fleet;
    private final TargetWorkerMaintenanceLoop maintenanceLoop;
    private final List<Shard> shards;
    private final TargetWorkerRuntimeConfig runtimeConfig;
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
    private boolean maintenanceStarted;
    private boolean sourceProcessingConfigured;
    private Thread serviceActivationThread;
    private boolean serviceActivated;
    private volatile boolean backendQuarantined;
    private boolean quarantineActive;
    private boolean faultExitActive;
    private TargetWorkerOxiaRuntime oxiaRuntime;
    private OxiaClosePermit oxiaClosePermit;
    private final Map<ShardId, TargetOwnerDrainCoordinator.ReplayExit> faultExited = new HashMap<>();
    private boolean backendBarrierEstablished;
    private long quarantineGeneration;
    private final List<TargetOwnedBackendGroup> ownedBackends = new ArrayList<>();
    private final Map<TargetOwnedBackendGroup, BackendClosePermit> backendClosePermits = new java.util.IdentityHashMap<>();
    private TargetSourceBacklogCache sourceObservationCache;
    private java.util.function.BiFunction<TargetWorkerShardRuntime, TargetSourceBacklog.Cut,
            TargetSourceBacklogRead> sourceObservationFactory;
    private LongSupplier sourceObservationOwnerClock;
    private int sourceObservationCursor;

    private TargetCheckpointCandidateSchedule checkpointSchedule;
    private LongSupplier checkpointClock;
    private SchedulerBudget checkpointBudget;
    private int checkpointCursor;
    private boolean finalCheckpointOnDrain;

    /** Registers the admitted fleet once and uses the existing maintenance loop for bounded claim/settlement. */
    public synchronized TargetCheckpointCandidateSchedule configurePeriodicSemanticCheckpoints(
            TargetCheckpointCandidateSchedule.SemanticRequestFactory factory, LongSupplier clock) {
        if (runtimeConfig == null) {
            throw new IllegalStateException("Target host has no unified runtime configuration");
        }
        final var checkpoint = runtimeConfig.checkpoint();
        return configurePeriodicSemanticCheckpoints(checkpoint.newScheduler(runtimeConfig.store().maxOwnedShards()),
                factory, clock, checkpoint.settlementTurn());
    }

    /** Registers the admitted fleet once and uses the existing maintenance loop for bounded claim/settlement. */
    public synchronized TargetCheckpointCandidateSchedule configurePeriodicSemanticCheckpoints(
            com.nereusstream.delay.store.CheckpointScheduler scheduler,
            TargetCheckpointCandidateSchedule.SemanticRequestFactory factory, LongSupplier clock,
            SchedulerBudget budget) {
        final var supplied = Objects.requireNonNull(factory, "factory");
        if (runtimeConfig != null) {
            runtimeConfig.checkpoint().requireSchedule(scheduler, runtimeConfig.store().maxOwnedShards(), budget);
        }
        final TargetCheckpointCandidateSchedule.SemanticRequestFactory checked = (worker, claim, cut) -> {
            final var request = supplied.create(worker, claim, cut);
            if (runtimeConfig != null) { runtimeConfig.checkpoint().requireRequest(request); }
            return request;
        };
        return registerPeriodicCheckpointSchedule(
                TargetCheckpointCandidateSchedule.semantic(this, scheduler, checked, budget), clock, budget);
    }

    /** Uses the same maintenance loop while original history is collected in bounded CHECKPOINT steps. */
    public synchronized TargetCheckpointCandidateSchedule configurePeriodicCollectedSemanticCheckpoints(
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory factory, LongSupplier clock) {
        if (runtimeConfig == null) {
            throw new IllegalStateException("Target host has no unified runtime configuration");
        }
        final var checkpoint = runtimeConfig.checkpoint();
        return configurePeriodicCollectedSemanticCheckpoints(
                checkpoint.newScheduler(runtimeConfig.store().maxOwnedShards()), factory, clock,
                checkpoint.settlementTurn());
    }

    /** Uses the same maintenance loop while original history is collected in bounded CHECKPOINT steps. */
    public synchronized TargetCheckpointCandidateSchedule configurePeriodicCollectedSemanticCheckpoints(
            com.nereusstream.delay.store.CheckpointScheduler scheduler,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory factory, LongSupplier clock,
            SchedulerBudget budget) {
        final var supplied = Objects.requireNonNull(factory, "factory");
        if (runtimeConfig != null) {
            runtimeConfig.checkpoint().requireSchedule(scheduler, runtimeConfig.store().maxOwnedShards(), budget);
        }
        final TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checked = (worker, claim, cut) -> {
            final var request = supplied.create(worker, claim, cut);
            if (runtimeConfig != null) { runtimeConfig.checkpoint().requireRequest(request); }
            return request;
        };
        return registerPeriodicCheckpointSchedule(
                TargetCheckpointCandidateSchedule.collectedSemantic(this, scheduler, checked, budget), clock, budget);
    }

    private TargetCheckpointCandidateSchedule registerPeriodicCheckpointSchedule(
            TargetCheckpointCandidateSchedule schedule, LongSupplier clock, SchedulerBudget budget) {
        requireServiceActivationCaller();
        if (stopping || checkpointSchedule != null) {
            throw new IllegalStateException("Target Host checkpoint scheduling is stopping or already configured");
        }
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(budget, "budget");
        final var admitted = new ArrayList<ShardId>();
        try {
            final long now = clock.getAsLong();
            for (var shard : shards) {
                if (!(shard instanceof TargetWorkerShardRuntime worker)) {
                    throw new IllegalArgumentException("periodic semantic scheduling requires actual Target Workers");
                }
                schedule.register(worker, now);
                admitted.add(worker.shardId());
            }
            configurePeriodicCheckpoints(schedule, clock, budget, true);
            return schedule;
        } catch (RuntimeException | Error failure) {
            for (var shard : admitted) {
                try { schedule.unregister(shard); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            }
            throw failure;
        }
    }

    /** Deterministic lifecycle seam; callers register the exact admitted test Shards first. */
    synchronized void configurePeriodicCheckpoints(TargetCheckpointCandidateSchedule schedule, LongSupplier clock,
            SchedulerBudget budget) {
        configurePeriodicCheckpoints(schedule, clock, budget, false);
    }

    synchronized void configurePeriodicCheckpoints(TargetCheckpointCandidateSchedule schedule, LongSupplier clock,
            SchedulerBudget budget, boolean finalPublication) {
        if (stopping || checkpointSchedule != null) {
            throw new IllegalStateException("Target Host checkpoint scheduling is stopping or already configured");
        }
        Objects.requireNonNull(schedule, "schedule").requireHost(this);
        final var selectedClock = Objects.requireNonNull(clock, "clock");
        final var selectedBudget = Objects.requireNonNull(budget, "budget");
        maintenanceLoop.configureCheckpointTurn(this::runNextPeriodicCheckpointTurn);
        checkpointSchedule = schedule;
        checkpointClock = selectedClock;
        checkpointBudget = selectedBudget;
        finalCheckpointOnDrain = finalPublication;
    }

    /** At most one exact Shard action per tick; provider/WorkClass I/O stays outside the Host monitor. */
    public boolean runNextPeriodicCheckpointTurn() {
        final TargetCheckpointCandidateSchedule schedule;
        final LongSupplier clock;
        final SchedulerBudget budget;
        final int count;
        synchronized (this) {
            if (stopping || checkpointSchedule == null) { return false; }
            schedule = checkpointSchedule;
            clock = checkpointClock;
            budget = checkpointBudget;
            count = shards.size();
        }
        final long now = clock.getAsLong();
        for (int visited = 0; visited < count; visited++) {
            final Shard shard;
            final boolean withdrawnShard;
            synchronized (this) {
                if (stopping) { return false; }
                if (checkpointCursor >= shards.size()) { checkpointCursor = 0; }
                shard = shards.get(checkpointCursor++);
                if (completed.containsKey(shard.shardId()) || draining.contains(shard.shardId())) { continue; }
                withdrawnShard = withdrawn.contains(shard.shardId());
            }
            if (!withdrawnShard) { schedule.ensureRegistered(shard, now, budget); }
            if (schedule.hasFinalDrainFlight(shard)) { continue; }
            if (schedule.pendingTask(shard.shardId()).isPresent()) {
                final var settled = schedule.settle(shard.shardId(), budget, now);
                if (settled.isPresent() && settled.orElseThrow().failure() != null) {
                    throw new IllegalStateException("periodic Target checkpoint failed",
                            settled.orElseThrow().failure());
                }
                return true;
            }
            if (!withdrawnShard && shard.pendingSourceEntry().isEmpty()
                    && !schedule.claimDueForShard(shard.shardId(), now, 1).isEmpty()) { return true; }
        }
        return false;
    }

    /** One shared cache/provider for the admitted fleet; maintenance only queues one bounded refresh per tick. */
    public synchronized void configureSourceObservations(final TargetSourceBacklogCache cache,
            final java.util.function.BiFunction<TargetWorkerShardRuntime, TargetSourceBacklog.Cut,
                    TargetSourceBacklogRead> factory, final LongSupplier ownerClock) {
        requireServiceActivationCaller();
        if (stopping || sourceObservationCache != null) {
            throw new IllegalStateException("Target Host Source observer is stopping or already configured");
        }
        final var selected = Objects.requireNonNull(cache, "cache");
        if (runtimeConfig != null) {
            selected.requireBounds(runtimeConfig.store().maxOwnedShards(),
                    runtimeConfig.backlogGrowth().maximumObservationAgeNanos());
        }
        final var selectedFactory = Objects.requireNonNull(factory, "factory");
        final var selectedClock = Objects.requireNonNull(ownerClock, "ownerClock");
        selected.bindHost(this);
        maintenanceLoop.configureSourceObservationTurn(this::runNextSourceObservationTurn);
        sourceObservationCache = selected;
        sourceObservationFactory = selectedFactory;
        sourceObservationOwnerClock = selectedClock;
    }

    public boolean runNextSourceObservationTurn() {
        final TargetSourceBacklogCache cache;
        final java.util.function.BiFunction<TargetWorkerShardRuntime, TargetSourceBacklog.Cut,
                TargetSourceBacklogRead> factory;
        final LongSupplier clock;
        final int count;
        synchronized (this) {
            if (stopping || sourceObservationCache == null) { return false; }
            cache = sourceObservationCache;
            factory = sourceObservationFactory;
            clock = sourceObservationOwnerClock;
            count = shards.size();
        }
        for (int visited = 0; visited < count; visited++) {
            final Shard shard;
            synchronized (this) {
                if (stopping) { return false; }
                if (sourceObservationCursor >= shards.size()) { sourceObservationCursor = 0; }
                shard = shards.get(sourceObservationCursor++);
                if (withdrawn.contains(shard.shardId()) || completed.containsKey(shard.shardId())
                        || draining.contains(shard.shardId())) { continue; }
            }
            if (shard instanceof TargetWorkerShardRuntime worker) {
                return tryWithShardAdmission(worker, () -> worker.pendingCheckpointTask().isEmpty()
                        && cache.refresh(worker, clock, cut -> factory.apply(worker, cut))).orElse(false);
            }
        }
        return false;
    }

    public synchronized Optional<TargetSourceBacklogRead> pendingSourceObservation(final ShardId shard) {
        return sourceObservationCache == null ? Optional.empty() : sourceObservationCache.pendingOperation(shard);
    }

    public synchronized void retrySourceObservationCleanup(final ShardId shard) {
        if (sourceObservationCache == null) { throw new IllegalStateException("Target Host has no Source observer"); }
        sourceObservationCache.retryCleanup(shard);
    }

    private ShardDrain stopSourceObservationForDrain(final Shard shard) {
        final TargetSourceBacklogCache cache;
        synchronized (this) { cache = sourceObservationCache; }
        if (cache == null) { return null; }
        cache.withdraw(shard.shardId());
        if (!cache.cleanupComplete(shard.shardId())) {
            return new ShardDrain(shard.shardId(), Status.PENDING_SOURCE_OBSERVATION, null, null, null, null);
        }
        return null;
    }

    /** Starts bounded reservation and message-expiry ticks for one exact Worker graph. */
    public static TargetWorkerHostRuntime start(
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shards,
            final SchedulerBudget maintenanceBudget,
            final Duration maintenanceInterval,
            final Consumer<Throwable> failureConsumer) {
        return start(null, workClasses, resources, shards, maintenanceBudget, maintenanceInterval, failureConsumer,
                null, null);
    }

    /** Starts the configured Host after checking the exact resource, dispatcher and Source limits. */
    public static TargetWorkerHostRuntime start(
            final TargetWorkerRuntimeConfig config,
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shards,
            final Consumer<Throwable> failureConsumer) {
        final var selected = Objects.requireNonNull(config, "config");
        return start(selected, workClasses, resources, shards,
                selected.maintenanceTurn(), selected.maintenanceInterval(), failureConsumer, null, null);
    }

    /** Installs the configured formal checkpoint schedule before admitting the first maintenance tick. */
    public static TargetWorkerHostRuntime startWithCollectedCheckpoints(
            TargetWorkerRuntimeConfig config, WorkClassExecutionRegistry workClasses, SharedRocksDbResources resources,
            List<TargetWorkerShardRuntime> shards,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory factory, LongSupplier checkpointClock,
            Consumer<Throwable> failureConsumer) {
        final var selected = Objects.requireNonNull(config, "config");
        return start(selected, workClasses, resources, shards, selected.maintenanceTurn(),
                selected.maintenanceInterval(), failureConsumer, Objects.requireNonNull(factory, "factory"),
                Objects.requireNonNull(checkpointClock, "checkpointClock"));
    }

    /** Compose the actual Host and bind Source backends without admitting any background tick. */
    public static TargetWorkerHostRuntime prepareWithCollectedCheckpoints(
            TargetWorkerRuntimeConfig config, WorkClassExecutionRegistry classes, SharedRocksDbResources resources,
            List<TargetWorkerShardRuntime> shards,
            TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints, LongSupplier checkpointClock,
            Consumer<Throwable> failureConsumer) {
        final var selected = Objects.requireNonNull(config, "config");
        return compose(selected, classes, resources, shards, selected.maintenanceTurn(), selected.maintenanceInterval(),
                failureConsumer, Objects.requireNonNull(checkpoints, "checkpoints"),
                Objects.requireNonNull(checkpointClock, "checkpointClock"), false);
    }

    /** Activate the existing maintenance loop only after service-specific backends/providers have been installed. */
    public synchronized void startMaintenance() {
        requireServiceActivationCaller();
        if (stopping || backendQuarantined || maintenanceStarted)
            throw new IllegalStateException("Target Host maintenance is stopping or already started");
        maintenanceLoop.start();
        maintenanceStarted = true;
    }

    private static TargetWorkerHostRuntime start(
            final TargetWorkerRuntimeConfig config,
            final WorkClassExecutionRegistry classes, final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shards, final SchedulerBudget budget, final Duration interval,
            final Consumer<Throwable> failures,
            final TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpoints,
            final LongSupplier checkpointClock) {
        return compose(config, classes, resources, shards, budget, interval, failures, checkpoints, checkpointClock, true);
    }

    private static TargetWorkerHostRuntime compose(
            final TargetWorkerRuntimeConfig config,
            final WorkClassExecutionRegistry workClasses,
            final SharedRocksDbResources resources,
            final List<TargetWorkerShardRuntime> shards,
            final SchedulerBudget maintenanceBudget,
            final Duration maintenanceInterval,
            final Consumer<Throwable> failureConsumer,
            final TargetCheckpointCandidateSchedule.CollectedSemanticRequestFactory checkpointFactory,
            final LongSupplier checkpointClock, final boolean activateMaintenance) {
        if (config != null) {
            config.requireRuntime(workClasses, resources);
        }
        final var exactShards = List.copyOf(Objects.requireNonNull(shards, "shards"));
        if (config != null) {
            if (exactShards.size() > config.store().maxOwnedShards()) {
                throw new IllegalArgumentException("configured Host exceeds its Shard ownership bound");
            }
            exactShards.forEach(worker -> worker.requireRuntimeConfiguration(config));
        }
        exactShards.forEach(TargetWorkerShardRuntime::requireMessageExpiryMaintenanceConfigured);
        final var fleet = new TargetWorkerShardFleetRuntime(workClasses, resources, exactShards);
        final var loop =
                TargetWorkerMaintenanceLoop.create(fleet, maintenanceBudget, maintenanceInterval, failureConsumer);
        TargetWorkerHostRuntime host = null;
        try {
            host = new TargetWorkerHostRuntime(fleet, loop, exactShards, config);
            if (checkpointFactory != null) {
                host.configurePeriodicCollectedSemanticCheckpoints(checkpointFactory, checkpointClock);
            }
            final var backends = new ArrayList<TargetOwnedBackendGroup>();
            for (var worker : exactShards) {
                final var backend = worker.sourceBackendGroup();
                if (backend != null && backends.stream().noneMatch(existing -> existing == backend)) {
                    backends.add(backend);
                }
                for (var publication : worker.publicationBackendGroups()) {
                    if (backends.stream().noneMatch(existing -> existing == publication)) { backends.add(publication); }
                }
            }
            // Reject a borrowed/unready group before any other group transfers to this Host.
            for (var backend : backends) { backend.requireHostComposition(host, exactShards); }
            for (var backend : backends) { host.registerOwnedBackend(backend); }
            if (activateMaintenance) { host.startMaintenance(); }
            return host;
        } catch (RuntimeException | Error failure) {
            if (host != null) { synchronized (host) { host.stopping = true; } }
            if (host != null && host.ownedBackends.isEmpty()) {
                host.rollbackTargetQueueChangeSignalBindings(host.shards, failure);
            }
            try {
                loop.close();
            } catch (RuntimeException | Error closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (host != null && !host.ownedBackends.isEmpty()) {
                // Keep the original Store bindings exclusive until this failed Host completes fault exit.
                // Unbinding here would let another Host reuse a Worker that our quarantine still fences.
                synchronized (host) { host.stopping = true; }
                for (var backend : host.ownedBackends) {
                    try { backend.freezeRegistrations(); }
                    catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                }
                throw new CompositionFailure(failure, host);
            }
            throw failure;
        }
    }

    /** Keeps the exact partially composed Host and its bound SDK owners available for explicit fault cleanup. */
    public static final class CompositionFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerHostRuntime failedHost;
        private CompositionFailure(Throwable original, TargetWorkerHostRuntime failedHost) {
            super("Target Host composition failed after backend ownership transfer", original);
            this.failedHost = failedHost;
        }
        public TargetWorkerHostRuntime failedHost() { return failedHost; }
    }

    /** Test seam; production creates the fleet and scheduler from the same exact shards. */
    TargetWorkerHostRuntime(
            final TargetWorkerShardFleetRuntime fleet,
            final TargetWorkerMaintenanceLoop maintenanceLoop,
            final List<? extends Shard> shards) {
        this(fleet, maintenanceLoop, shards, null);
    }

    private TargetWorkerHostRuntime(
            final TargetWorkerShardFleetRuntime fleet,
            final TargetWorkerMaintenanceLoop maintenanceLoop,
            final List<? extends Shard> shards,
            final TargetWorkerRuntimeConfig runtimeConfig) {
        this.fleet = Objects.requireNonNull(fleet, "fleet");
        this.maintenanceLoop = Objects.requireNonNull(maintenanceLoop, "maintenanceLoop");
        this.runtimeConfig = runtimeConfig;
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
            // Publish the Host only after every Store signal has established its exclusive binding.
            for (Shard shard : this.shards) {
                if (shard instanceof TargetWorkerShardRuntime worker) { worker.bindPublicationHost(this); }
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

    /** Configured ordinary/Native Claim consumption and per-Shard backpressure for actual publisher runtimes. */
    public synchronized TargetWorkerOrdinaryLoop startPulsarPublishing(
            final TargetWorkerOrdinaryDrr.Requests requests, final LongSupplier ownerClock,
            final LongSupplier schedulerClock, final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics, final Consumer<Throwable> failureConsumer) {
        currentTargetWorkers().forEach(TargetWorkerShardRuntime::requirePulsarStartupClosure);
        return startPublishing(requests, ownerClock, schedulerClock, monotonicClock, metrics, failureConsumer);
    }

    /** Starts the configured ordinary baseline and Managed Journal endpoints on the same finite Host loop. */
    public synchronized TargetWorkerOrdinaryLoop startPublishing(
            final TargetWorkerOrdinaryDrr.Requests requests, final LongSupplier ownerClock,
            final LongSupplier schedulerClock, final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics, final Consumer<Throwable> failureConsumer) {
        requireServiceActivationCaller();
        final var delegate = Objects.requireNonNull(requests, "requests");
        final var workers = currentTargetWorkers();
        workers.forEach(TargetWorkerShardRuntime::requirePublishingStartupClosure);
        if (workers.stream().anyMatch(worker -> !worker.publisherAvailable())) {
            throw new IllegalStateException("configure every admitted Target publisher before starting its Claim loop");
        }
        final var guarded = new TargetWorkerOrdinaryDrr.Requests() {
            @Override
            public Optional<TargetWorkerOrdinaryDrr.Request> resolve(TargetWorkerShardRuntime worker,
                    TargetHeadCostProbe.Cost cost) {
                return worker.publisherAvailable() ? delegate.resolve(worker, cost) : Optional.empty();
            }
            @Override
            public Optional<TargetWorkerOrdinaryDrr.NativePolicyContext> resolveNativePolicyContext(
                    TargetWorkerShardRuntime worker, TargetHeadCostProbe.Cost cost) {
                return worker.publisherAvailable() && worker.publisherSupportsNative()
                        ? delegate.resolveNativePolicyContext(worker, cost) : Optional.empty();
            }
            @Override
            public java.io.Closeable subscribeNativePolicyChanges(Runnable listener) {
                return delegate.subscribeNativePolicyChanges(listener);
            }
        };
        return startOrdinaryScheduling(guarded, claim -> {
            final var shard = claim.work().locator().messageId().routingId().shardId();
            final var worker = workers.stream().filter(candidate -> candidate.shardId().equals(shard))
                    .findFirst().orElseThrow(() -> new IllegalStateException("Claim belongs to an unconfigured Worker"));
            worker.acceptPublicationClaim(claim);
        }, ownerClock, schedulerClock, monotonicClock, metrics, failureConsumer);
    }

    /** Starts the single bounded ordinary-first Claim loop for this Host. */
    public synchronized TargetWorkerOrdinaryLoop startOrdinaryScheduling(
            final TargetWorkerOrdinaryDrr.Requests requests,
            final TargetWorkerOrdinaryLoop.ClaimConsumer claimConsumer,
            final LongSupplier ownerClock,
            final LongSupplier schedulerClock,
            final LongSupplier monotonicClock,
            final BoundedAsyncMetricExporter metrics,
            final Consumer<Throwable> failureConsumer) {
        if (runtimeConfig == null) {
            throw new IllegalStateException("Target host has no unified runtime configuration");
        }
        return startOrdinaryScheduling(runtimeConfig.inventory(), runtimeConfig.ordinary(),
                runtimeConfig.ordinaryTurn(), runtimeConfig.ordinaryRecheckInterval(), requests, claimConsumer,
                ownerClock, schedulerClock, monotonicClock, metrics, failureConsumer);
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
        requireServiceActivationCaller();
        if (runtimeConfig != null && (!runtimeConfig.inventory().equals(inventoryLimits)
                || !runtimeConfig.ordinary().equals(drrLimits)
                || !runtimeConfig.ordinaryTurn().equals(turnBudget)
                || !runtimeConfig.ordinaryRecheckInterval().equals(recheckInterval))) {
            throw new IllegalArgumentException("ordinary scheduler limits differ from the configured Target Host");
        }
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
            worker.unbindPublicationHost(this);
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
    public TargetWorkerShardFleetRuntime.SourceTurn runNextSourceTurn(final LongSupplier ownerClock) {
        if (runtimeConfig == null) {
            throw new IllegalStateException("Target host has no unified runtime configuration");
        }
        return runNextSourceTurn(runtimeConfig.sourceTurn(), ownerClock);
    }

    /** Calls at most one Shard source turn while whole-fleet admission is open. */
    public TargetWorkerShardFleetRuntime.SourceTurn runNextSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        synchronized (this) {
            requireServiceActivationCaller();
            if (stopping) { throw new IllegalStateException("Target host source admission is stopping"); }
        }
        return runNextSourceTurnIfPresent(budget, ownerClock)
                .orElseThrow(() -> new IllegalStateException("Target host has no admitted Source turn"));
    }

    private Optional<TargetWorkerShardFleetRuntime.SourceTurn> runNextSourceTurnIfPresent(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(ownerClock, "ownerClock");
        if (runtimeConfig != null && !runtimeConfig.sourceTurn().equals(budget)) {
            throw new IllegalArgumentException("Source turn budget differs from the configured Target Host");
        }
        synchronized (this) {
            if (stopping || serviceActivationThread != null) { return Optional.empty(); }
        }
        return fleet.runNextSourceTurnIfPresent(selected -> {
            if (!(selected instanceof Shard shard)) {
                throw new IllegalStateException("Target Source selection lacks the exact Host Shard");
            }
            return tryWithShardAdmission(shard, () -> selected.pendingCheckpointTask().isPresent()
                    ? Optional.<SourceApplyCoordinator.TurnResult>empty()
                    : Optional.of(selected.runSourceTurn(budget, ownerClock))).flatMap(result -> result);
        });
    }

    /** Installs actual Source poll/apply/ACK rotation on the existing maintenance thread before activation. */
    public synchronized void configureSourceProcessing(final LongSupplier ownerClock) {
        requireServiceActivationCaller();
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (runtimeConfig == null || stopping || maintenanceStarted) {
            throw new IllegalStateException("Source processing requires a configured, prepared Target Host");
        }
        final var budget = runtimeConfig.sourceTurn();
        maintenanceLoop.configureSourceApplyTurn(() -> {
            final var selected = runNextSourceTurnIfPresent(budget, clock);
            if (selected.isEmpty()) { return; }
            final var result = selected.orElseThrow().result();
            if (result.failure() != null && result.status() != SourceApplyCoordinator.TurnStatus.READ_INCOMPLETE
                    && result.status() != SourceApplyCoordinator.TurnStatus.AUTHORITY_RETRY) {
                if (result.failure() instanceof Error fatal) { throw fatal; }
                throw new IllegalStateException("Target Source turn failed for " + selected.orElseThrow().shardId()
                        + " at " + result.status(), result.failure());
            }
        });
        sourceProcessingConfigured = true;
    }

    /** Enables the original configured Source, observer, checkpoint and Claim/publisher graph in one startup turn. */
    public TargetWorkerOrdinaryLoop activateConfiguredService(
            TargetSourceBacklogCache observations,
            java.util.function.BiFunction<TargetWorkerShardRuntime, TargetSourceBacklog.Cut,
                    TargetSourceBacklogRead> observationFactory,
            TargetWorkerOrdinaryDrr.Requests requests, LongSupplier ownerClock, LongSupplier schedulerClock,
            LongSupplier monotonicClock, BoundedAsyncMetricExporter metrics, Consumer<Throwable> failures) {
        Objects.requireNonNull(observations, "observations");
        Objects.requireNonNull(observationFactory, "actual Source observation factory");
        Objects.requireNonNull(requests, "actual Claim requests");
        Objects.requireNonNull(ownerClock, "ownerClock");
        Objects.requireNonNull(schedulerClock, "schedulerClock");
        Objects.requireNonNull(monotonicClock, "monotonicClock");
        Objects.requireNonNull(failures, "failureConsumer");
        final List<TargetWorkerShardRuntime> workers;
        synchronized (this) {
            if (runtimeConfig == null || stopping || backendQuarantined || serviceActivationThread != null
                    || serviceActivated || oxiaRuntime == null || ordinaryLoop != null || maintenanceStarted || sourceProcessingConfigured
                    || sourceObservationCache != null || checkpointSchedule == null || !finalCheckpointOnDrain
                    || !draining.isEmpty() || !withdrawn.isEmpty()) {
                throw new IllegalStateException("configured service requires an idle prepared Host with formal checkpoints");
            }
            workers = currentTargetWorkers();
            serviceActivationThread = Thread.currentThread();
        }
        boolean installing = false;
        try {
            // These checks may await Worker/authority reads; keep them outside the Host monitor.
            observations.requireBounds(runtimeConfig.store().maxOwnedShards(),
                    runtimeConfig.backlogGrowth().maximumObservationAgeNanos());
            for (var worker : workers) { worker.requireConfiguredServiceReady(this, ownerClock); }
            installing = true;
            configureSourceObservations(observations, observationFactory, ownerClock);
            configureSourceProcessing(ownerClock);
            final var publishing = startPublishing(requests, ownerClock, schedulerClock, monotonicClock, metrics, failures);
            startMaintenance();
            synchronized (this) {
                if (stopping || backendQuarantined) {
                    throw new IllegalStateException("configured service was stopped during activation");
                }
                serviceActivated = true;
            }
            return publishing;
        } catch (RuntimeException | Error failure) {
            if (installing) {
                try { quarantineOwnedBackends(); }
                catch (RuntimeException | Error cleanup) { if (cleanup != failure) { failure.addSuppressed(cleanup); } }
            }
            throw failure;
        } finally {
            synchronized (this) { serviceActivationThread = null; notifyAll(); }
        }
    }

    /** Registers the original actual authority graph while prepared, including a failed Host retained for fault exit. */
    public void registerOxiaRuntime(TargetWorkerOxiaRuntime root) {
        final List<TargetWorkerShardRuntime> workers;
        synchronized (this) {
            requireServiceActivationCaller();
            if (oxiaRuntime == root) return;
            if (oxiaRuntime != null || maintenanceStarted || serviceActivated || sourceProcessingConfigured
                    || ordinaryLoop != null || drainAllActive || faultExitActive || !draining.isEmpty()) {
                throw new IllegalStateException("Oxia root must transfer before Host execution/drain");
            }
            workers = currentTargetWorkers();
        }
        for (var worker : workers) root.requireWorker(worker);
        synchronized (this) {
            if (oxiaRuntime != null || maintenanceStarted || serviceActivated || sourceProcessingConfigured
                    || ordinaryLoop != null || drainAllActive || faultExitActive || !draining.isEmpty()
                    || !workers.equals(currentTargetWorkers())) {
                throw new IllegalStateException("Host changed during original Oxia root transfer");
            }
            root.bindHost(this); oxiaRuntime = root;
        }
    }

    /** Private-constructor capability; public Result/ReplayExit records do not authorize SDK authority close. */
    public static final class OxiaClosePermit {
        private final TargetWorkerHostRuntime host;
        private final TargetWorkerOxiaRuntime root;
        private OxiaClosePermit(TargetWorkerHostRuntime host, TargetWorkerOxiaRuntime root) {
            this.host = host; this.root = root;
        }
        void requireOriginalRoot(TargetWorkerOxiaRuntime expected) {
            synchronized (host) {
                if (root != expected || host.oxiaRuntime != expected || host.oxiaClosePermit != this || !host.stopping) {
                    throw new IllegalStateException("Oxia close lacks its original stopped Host permit");
                }
            }
        }
    }

    private boolean closeOxiaAfterDependencies(boolean fault) {
        final TargetWorkerOxiaRuntime root;
        final OxiaClosePermit permit;
        synchronized (this) {
            root = oxiaRuntime;
            if (root == null) return true;
            if (!stopping || ownedBackends.stream().anyMatch(backend -> !backend.resourcesTerminated())
                    || shards.stream().anyMatch(shard -> fault ? !faultExited.containsKey(shard.shardId())
                            : !completed.containsKey(shard.shardId()))) {
                return false;
            }
            if (fault != backendQuarantined) throw new IllegalStateException("Oxia close normal/fault lineage changed");
            if (oxiaClosePermit == null) oxiaClosePermit = new OxiaClosePermit(this, root);
            permit = oxiaClosePermit;
        }
        root.closeForHost(permit);
        return root.cleanupComplete();
    }

    public boolean oxiaCleanupComplete() {
        final TargetWorkerOxiaRuntime root;
        synchronized (this) { root = oxiaRuntime; }
        return root == null || root.cleanupComplete();
    }

    /** Configuration changes and manual Source turns must not race the single service startup turn. */
    private void requireServiceActivationCaller() {
        if (serviceActivationThread != null && serviceActivationThread != Thread.currentThread()) {
            throw new IllegalStateException("Target service activation owns this Host configuration");
        }
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
        return claim(expectedShard, budget, selected, owner, nowEpochMs, deadlineEpochMs, executionBytes, operationDigest,
                quota, physicalWrites, ownerClock, null);
    }

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
            final LongSupplier ownerClock,
            final com.nereusstream.delay.protocol.HandoffPolicyHeadRef policyHead) {
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
                        ownerClock, policyHead));
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

    /** Background selection quietly skips a Shard already reserved by Claim, checkpoint, drain or stop. */
    private <T> Optional<T> tryWithShardAdmission(final Shard expectedShard, final Supplier<T> admission) {
        Objects.requireNonNull(expectedShard, "shard");
        Objects.requireNonNull(admission, "admission");
        final var id = expectedShard.shardId();
        synchronized (this) {
            final int index = indexOfShard(id);
            if (stopping || serviceActivationThread != null && serviceActivationThread != Thread.currentThread()
                    || index < 0 || shards.get(index) != expectedShard || withdrawn.contains(id)
                    || completed.containsKey(id) || !draining.add(id)) { return Optional.empty(); }
        }
        try {
            synchronized (expectedShard) { return Optional.of(admission.get()); }
        } finally {
            synchronized (this) { draining.remove(id); notifyAll(); }
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
    void admitShard(final Shard shard, final TargetWorkerShardFleetRuntime.ShardTurns turns) {
        // Wait for any selected native turn without blocking Host quarantine/stop admission.
        synchronized (fleet) {
            synchronized (this) { admitShardAfterFleetTurn(shard, turns); }
        }
    }

    private void admitShardAfterFleetTurn(final Shard shard, final TargetWorkerShardFleetRuntime.ShardTurns turns) {
        requireServiceActivationCaller();
        if (stopping) {
            throw new IllegalStateException("Target host admission is stopping");
        }
        if (Objects.requireNonNull(shard, "shard") != Objects.requireNonNull(turns, "turns")) {
            throw new IllegalArgumentException("Target host source and maintenance instance differ");
        }
        if (runtimeConfig != null) {
            if (!(shard instanceof TargetWorkerShardRuntime worker)) {
                throw new IllegalArgumentException("configured Target Host requires a Target Worker Shard");
            }
            worker.requireRuntimeConfiguration(runtimeConfig);
            if (oxiaRuntime != null) oxiaRuntime.requireWorker(worker);
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
        if (sourceObservationCache != null) { sourceObservationCache.remove(shardId); }
        bindTargetQueueChangeSignal(shard);
        if (shard instanceof TargetWorkerShardRuntime worker && ordinaryTargetCacheLimit > 0) {
            worker.configureTargetQueueHeadCache(ordinaryTargetCacheLimit);
        }
        if (shard instanceof TargetWorkerShardRuntime worker) { worker.bindPublicationHost(this); }
        fleet.admit(turns, () -> {
            if (shard instanceof TargetWorkerShardRuntime worker && worker.sourceBackendGroup() != null) {
                final var backend = worker.sourceBackendGroup();
                if (!ownedBackends.contains(backend)) {
                    final var next = new ArrayList<TargetWorkerShardRuntime>();
                    for (var existing : shards) {
                        if (!existing.shardId().equals(shardId)) { next.add((TargetWorkerShardRuntime) existing); }
                    }
                    next.add(worker);
                    backend.bindHost(this, List.copyOf(next));
                    ownedBackends.add(backend);
                } else { backend.admitWorker(this, worker); }
            }
        });
        if (previousIndex < 0) {
            shards.add(shard);
        } else {
            shards.set(previousIndex, shard);
            withdrawn.remove(shardId);
            completed.remove(shardId);
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
            if (backendQuarantined) {
                throw new IllegalStateException("quarantined Host requires fault exit, not normal final-checkpoint drain");
            }
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
            if (sourceObservationCache != null) { sourceObservationCache.stop(); }
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
                final ShardDrain observation = prior == null ? stopSourceObservationForDrain(shard) : null;
                final ShardDrain result = prior != null ? prior : observation != null ? observation
                        : drainOne(shard, request, sourceBudget, ownerClock);
                if (result.complete()) {
                    synchronized (this) {
                        completed.put(shard.shardId(), result);
                    }
                }
                results.add(result);
            }
            final boolean backendsClosed = results.stream().allMatch(ShardDrain::complete)
                    && closeOwnedBackendsAfterNormalDrain();
            final boolean authorityClosed = backendsClosed && closeOxiaAfterDependencies(false);
            return new Result(results, backendsClosed, authorityClosed);
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
        // The fleet lock is the selected-turn exit boundary; do not wait for it under the Host lock.
        synchronized (fleet) {
            synchronized (this) {
                requireServiceActivationCaller();
                shard = requireShard(Objects.requireNonNull(expectedShard, "shard").shardId());
                if (shard != expectedShard) {
                    throw new IllegalArgumentException("Target host shard instance has been replaced");
                }
                final ShardDrain prior = completed.get(shard.shardId());
                if (prior != null) { return prior; }
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
        }
        ShardDrain result = null;
        try {
            result = stopSourceObservationForDrain(shard);
            if (result != null) { return result; }
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

    private ShardDrain drainOne(
            final Shard shard,
            final TargetOwnerDrainCoordinator.Request request,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
        synchronized (shard) {
            SourceApplyCoordinator.TurnResult sourceTurn = null;
            try {
                requireNormalDrain();
                final var schedule = checkpointSchedule;
                final boolean finalFlight = schedule != null && schedule.hasFinalDrainFlight(shard);
                if (!finalFlight) { shard.requestCheckpointPreparationStop(); }
                final Optional<WorkClassTask> checkpoint = shard.pendingCheckpointTask();
                if (finalFlight) {
                    final var settled = schedule.settleForDrain(shard, checkpointBudget, checkpointClock.getAsLong());
                    if (settled.isEmpty()) {
                        return new ShardDrain(shard.shardId(), Status.PENDING_CHECKPOINT, null,
                                schedule.pendingTask(shard.shardId()).orElseThrow(), null, null);
                    }
                    if (settled.orElseThrow().failure() != null) {
                        throw new IllegalStateException("final Target checkpoint failed",
                                settled.orElseThrow().failure());
                    }
                } else if (checkpoint.isPresent()) {
                    return new ShardDrain(
                            shard.shardId(), Status.PENDING_CHECKPOINT, null, checkpoint.orElseThrow(), null, null);
                }
                if (schedule != null && schedule.pendingTask(shard.shardId()).isPresent()) {
                    // A separately settled old task still owns its process-local claim.
                    schedule.settleForDrain(shard, checkpointBudget, checkpointClock.getAsLong());
                }
                final Optional<WorkClassTask> channel = shard.settlePendingChannelControlForDrain(
                        request.gcBudget(), sourceBudget, ownerClock);
                if (channel.isPresent()) {
                    return new ShardDrain(shard.shardId(), Status.PENDING_CHANNEL_CONTROL, null,
                            null, null, null, null, channel.orElseThrow());
                }
                final Optional<WorkClassTask> publication = shard.settlePendingPublicationForDrain(
                        request.gcBudget(), sourceBudget, ownerClock);
                if (publication.isPresent()) {
                    return new ShardDrain(shard.shardId(), Status.PENDING_PUBLICATION, null,
                            null, null, null, null, null, publication.orElseThrow());
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
                requireNormalDrain();
                final var resources=shard.settlePendingPublicationResourcesForDrain();
                if(resources.isPresent())return new ShardDrain(shard.shardId(),Status.PENDING_PUBLICATION,sourceTurn,
                        null,null,null,null,null,resources.orElseThrow());
                if (finalCheckpointOnDrain && shard.canPublishFinalCheckpoint()) {
                    final var gc = shard.settlePendingGcForDrain(request.gcBudget());
                    if (gc.isPresent()) {
                        return new ShardDrain(shard.shardId(), Status.PENDING_GC, sourceTurn, null,
                                gc.orElseThrow(), null);
                    }
                    final var settledJournal = shard.settlePendingPublicationForDrain(request.gcBudget(), sourceBudget, ownerClock);
                    if (settledJournal.isPresent()) {
                        return new ShardDrain(shard.shardId(), Status.PENDING_PUBLICATION, sourceTurn,
                                null, null, null, null, null, settledJournal.orElseThrow());
                    }
                    if (!schedule.hasCurrentPublishedCut(shard)) {
                        final var task = schedule.submitFinalForDrain(shard, checkpointClock.getAsLong());
                        return new ShardDrain(shard.shardId(), Status.PENDING_CHECKPOINT, sourceTurn, task, null, null);
                    }
                }
                requireNormalDrain();
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

    private void requireNormalDrain() {
        if (backendQuarantined) { throw new IllegalStateException("normal drain was fenced by backend quarantine"); }
    }

    /** Normal final-checkpoint/Source/Owner drain has already completed for the exact admitted fleet. */
    private boolean closeOwnedBackendsAfterNormalDrain() {
        final List<TargetOwnedBackendGroup> backends;
        synchronized (this) {
            requireNormalDrain();
            if (!stopping || shards.stream().anyMatch(shard -> !completed.containsKey(shard.shardId()))) {
                throw new IllegalStateException("owned SDK close requires the completed normal Host drain");
            }
            backends = List.copyOf(ownedBackends);
            if (backends.isEmpty()) { return true; }
            if (quarantineGeneration == 0) { quarantineGeneration++; }
        }
        for (var backend : backends) {
            final var frozen = backend.freezeRegistrations();
            final BackendClosePermit closing;
            synchronized (this) {
                backendBarrierEstablished = true;
                closing = backendClosePermits.computeIfAbsent(backend, ignored ->
                        new BackendClosePermit(this, backend, quarantineGeneration, frozen));
            }
            backend.closeOwnedBackend(closing);
        }
        return backends.stream().allMatch(TargetOwnedBackendGroup::resourcesTerminated);
    }

    /** Binds an actual backend once to the admitted Worker graph, before its first application use. */
    public void registerOwnedBackend(TargetOwnedBackendGroup backend) {
        final List<TargetWorkerShardRuntime> workers;
        synchronized (this) {
            requireServiceActivationCaller();
            if (stopping || backendQuarantined) { throw new IllegalStateException("Host backend registration is closed"); }
            if (ownedBackends.stream().anyMatch(existing -> existing == backend)) { return; }
            workers = shards.stream().map(shard -> {
                if (!(shard instanceof TargetWorkerShardRuntime worker)) {
                    throw new IllegalStateException("owned backend requires actual Worker Shards");
                }
                return worker;
            }).toList();
            backend.bindHost(this, workers);
            ownedBackends.add(backend);
        }
    }

    /** Publication ownership transfers before its asynchronous SDK opening may start. */
    synchronized void registerPublicationBackend(TargetWorkerShardRuntime expected, TargetOwnedBackendGroup backend) {
        if (requireShard(expected.shardId()) != expected || withdrawn.contains(expected.shardId())
                || completed.containsKey(expected.shardId())) {
            throw new IllegalStateException("publication backend lacks its exact admitted Host Worker");
        }
        registerOwnedBackend(Objects.requireNonNull(backend, "actual publication backend"));
    }

    /**
     * Fences local side-effect sections first, then closes owned SDK backends without waiting for their futures.
     * The returned result is unrelated to normal drain/final-checkpoint/Owner release. Repeat calls retain permits.
     */
    public BackendQuarantineResult quarantineOwnedBackends() {
        final List<Shard> frozenShards;
        final List<TargetOwnedBackendGroup> backends;
        final TargetWorkerOrdinaryLoop ordinary;
        synchronized (this) {
            if (quarantineActive || faultExitActive) { return backendQuarantineStatus(); }
            if (ownedBackends.isEmpty()) { throw new IllegalStateException("Host has no actually owned backend group"); }
            quarantineActive = true;
            if (!backendQuarantined && backendClosePermits.isEmpty()) { quarantineGeneration++; }
            backendQuarantined = true;
            stopping = true;
            ordinary = ordinaryLoop;
            frozenShards = List.copyOf(shards);
            backends = List.copyOf(ownedBackends);
        }
        try {
            final Map<TargetOwnedBackendGroup, List<Object>> frozen = new java.util.IdentityHashMap<>();
            for (var backend : backends) { frozen.put(backend, backend.freezeRegistrations()); }
            // Store and runtime fences are independent of Worker monitors held by a native receive/ACK wait.
            for (Shard shard : frozenShards) {
                if (!(shard instanceof TargetWorkerShardRuntime worker)) {
                    throw new IllegalStateException("quarantine requires actual Store/runtime barriers");
                }
                worker.quarantineBackendWrites();
            }
            if (ordinary != null) { ordinary.requestStop(); }
            maintenanceLoop.requestStop();
            if (sourceObservationCache != null) { sourceObservationCache.stop(); }
            synchronized (this) {
                backendBarrierEstablished = true;
                for (var backend : backends) {
                    backendClosePermits.computeIfAbsent(backend, ignored ->
                            new BackendClosePermit(this, backend, quarantineGeneration, frozen.get(backend)));
                }
            }
            for (var backend : backends) {
                final BackendClosePermit permit;
                synchronized (this) { permit = backendClosePermits.get(backend); }
                backend.closeOwnedBackend(permit);
            }
            return backendQuarantineStatus();
        } finally {
            synchronized (this) { quarantineActive = false; notifyAll(); }
        }
    }

    public synchronized BackendQuarantineResult backendQuarantineStatus() {
        final var failures = new ArrayList<Throwable>();
        boolean terminated = backendBarrierEstablished;
        for (var backend : ownedBackends) {
            terminated &= backend.resourcesTerminated();
            if (backend.cleanupFailure() != null) { failures.add(backend.cleanupFailure()); }
        }
        return new BackendQuarantineResult(terminated ? BackendQuarantineStatus.BACKEND_TERMINATED
                : BackendQuarantineStatus.QUARANTINED, failures);
    }

    public record QuarantinedShardExit(ShardId shard, TargetOwnerDrainCoordinator.ReplayExit exited,
            RuntimeException failure) {}

    /** Retries only the quarantined old process exit; pending ACK/UNKNOWN and protected candidates remain for replay. */
    public List<QuarantinedShardExit> exitQuarantinedForReplay(TargetOwnerDrainCoordinator.Request request,
            LongSupplier clock) {
        final List<Shard> current;
        final TargetWorkerOrdinaryLoop ordinary;
        synchronized (this) {
            if (!backendQuarantined || quarantineActive || faultExitActive || drainAllActive || !draining.isEmpty()) {
                throw new IllegalStateException("quarantined fault exit still has a concurrent Host lifecycle owner");
            }
            current = List.copyOf(shards); ordinary = ordinaryLoop;
            faultExitActive = true;
        }
        try {
            if (backendQuarantineStatus().status() != BackendQuarantineStatus.BACKEND_TERMINATED) {
                return current.stream().map(shard -> new QuarantinedShardExit(shard.shardId(), null, null)).toList();
            }
            if (ordinary != null) { ordinary.close(); }
            maintenanceLoop.close();
            final var result = new ArrayList<QuarantinedShardExit>();
            for (var shard : current) {
                if (!(shard instanceof TargetWorkerShardRuntime worker)) {
                    throw new IllegalStateException("fault exit requires actual Worker/Store ownership");
                }
                try {
                    final TargetOwnerDrainCoordinator.ReplayExit prior;
                    synchronized (this) { prior = faultExited.get(shard.shardId()); }
                    final var exited = prior != null ? prior : worker.exitQuarantinedForReplay(request, clock).orElse(null);
                    if (exited != null) { synchronized (this) { faultExited.put(shard.shardId(), exited); } }
                    result.add(new QuarantinedShardExit(shard.shardId(), exited, null));
                } catch (RuntimeException failure) {
                    result.add(new QuarantinedShardExit(shard.shardId(), null, failure));
                }
            }
            if (result.stream().allMatch(exit -> exit.exited() != null && exit.failure() == null)) {
                closeOxiaAfterDependencies(true);
            }
            return List.copyOf(result);
        } finally {
            synchronized (this) { faultExitActive = false; notifyAll(); }
        }
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
