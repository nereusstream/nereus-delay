package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.AdmissionGate;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.RuntimeReadiness;
import com.nereusstream.delay.scheduler.PersistentLaneScheduler;
import com.nereusstream.delay.scheduler.ScheduleWorkItem;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ShardStore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Production composition for the active-owner scheduling boundary of one
 * Delay Shard.
 *
 * <p>Activation supplies the exact active Lane projection. This class
 * restores the persisted Lane fairness state, rebuilds the READY ring from
 * the authoritative Store, and routes due discovery through the bounded
 * {@code DUE_SCHEDULER} work class. READY polling is a separate strict
 * Owner/Store action; it deliberately does not manufacture a Claim,
 * materialization, publish descriptor or external-time proof.</p>
 *
 * <p>Claim handoff, Publish Admission and checkpoint publication remain
 * explicit injected work-class boundaries. Keeping those inputs explicit
 * prevents a local scheduler from becoming an authority for Profile,
 * Object Store, Broker or Oxia state.</p>
 */
public final class WorkerSchedulingRuntime {
    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final PersistentLaneScheduler scheduler;
    private final DueSchedulerWorkClassExecutor dueExecutor;

    /** Creates a runtime over an already-constructed active-owner scheduler. */
    public WorkerSchedulingRuntime(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.ownedShard.requireDueSchedulerSubmission(this.authority, this.scheduler);
        this.dueExecutor =
                new DueSchedulerWorkClassExecutor(this.workClasses, this.ownedShard, this.authority, this.scheduler);
    }

    /** Requires the Worker wrapper to run the registry used by due discovery. */
    void requireWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        if (workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalArgumentException("scheduling runtime uses another work-class registry");
        }
    }

    /**
     * Opens the scheduling graph after strict activation and restores the
     * exact READY/Lane projection for the accepted active-owner assignment.
     */
    public static WorkerSchedulingRuntime openForActiveOwner(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final com.nereusstream.delay.protocol.OwnerIdentity owner,
            final List<LaneRecord> activeLanes,
            final int maxReadyEntries) {
        final PersistentLaneScheduler scheduler = PersistentLaneScheduler.forActiveOwner(
                Objects.requireNonNull(store, "store"),
                Objects.requireNonNull(owner, "owner"),
                Objects.requireNonNull(activeLanes, "activeLanes"));
        final WorkerSchedulingRuntime runtime =
                new WorkerSchedulingRuntime(workClasses, ownedShard, authority, scheduler);
        scheduler.rebuildAuthoritativeReady(maxReadyEntries);
        return runtime;
    }

    /**
     * Opens the production-shaped scheduling graph from the exact typed Lane
     * identities persisted by activation. Callers cannot supply a guessed
     * LaneRecord or a legacy readiness projection to this path.
     */
    public static WorkerSchedulingRuntime openForActiveOwnerFromTypedLanes(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final ShardStore store,
            final com.nereusstream.delay.protocol.OwnerIdentity owner,
            final List<DestinationLaneId> laneIds,
            final int maxReadyEntries) {
        final OwnedDelayShard exactOwned = Objects.requireNonNull(ownedShard, "ownedShard");
        final List<DestinationLaneId> exactLaneIds = List.copyOf(Objects.requireNonNull(laneIds, "laneIds"));
        final Set<DestinationLaneId> seen = new HashSet<>();
        final List<LaneRecord> activeLanes = new ArrayList<>(exactLaneIds.size());
        for (DestinationLaneId laneId : exactLaneIds) {
            final DestinationLaneId exactLaneId = Objects.requireNonNull(laneId, "laneId");
            if (!seen.add(exactLaneId)) {
                throw new IllegalArgumentException("typed Lane bootstrap contains a duplicate Lane");
            }
            final ActiveLaneState typed = exactOwned.shard().getActiveLaneState(exactLaneId);
            if (typed == null) {
                throw new IllegalStateException("typed Lane bootstrap requires an ActiveLaneState");
            }
            if (typed.admissionGate() != AdmissionGate.OPEN
                    || typed.runtimeReadiness() != RuntimeReadiness.READY
                    || typed.readyCertificate() == null) {
                throw new IllegalStateException("typed Lane bootstrap requires certificate-backed READY state");
            }
            final LaneRecord lane = exactOwned.shard().getLane(exactLaneId);
            if (lane == null
                    || !lane.schedulable()
                    || !java.util.Arrays.equals(lane.laneIncarnation(), typed.laneIncarnation())
                    || lane.laneVersion() != typed.laneVersion()) {
                throw new IllegalStateException("typed Lane bootstrap projection differs from Lane record");
            }
            activeLanes.add(lane);
        }
        return openForActiveOwner(
                workClasses,
                exactOwned,
                authority,
                store,
                Objects.requireNonNull(owner, "owner"),
                activeLanes,
                maxReadyEntries);
    }

    /** Runs one arbitrary bounded WorkClass turn from the shared Worker graph. */
    public List<WorkClassTask> runTurn(final SchedulerBudget budget) {
        return workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    /**
     * Submits and executes one exact bounded due-discovery action. The
     * action is absent from the result until the shared work-class turn has
     * actually selected it.
     */
    public DueTurn runDueTurn(
            final TrustedUtcIntervalEvidence evidence,
            final SchedulerBudget discoveryBudget,
            final LongSupplier ownerClock) {
        final DueSchedulerWorkClassExecutor.Submission submission = dueExecutor.submit(
                Objects.requireNonNull(evidence, "evidence"),
                Objects.requireNonNull(discoveryBudget, "discoveryBudget"),
                Objects.requireNonNull(ownerClock, "ownerClock"));
        final List<WorkClassTask> completed =
                workClasses.runTurn(new SchedulerBudget(1, submission.task().bytes(), Long.MAX_VALUE));
        return new DueTurn(submission.task(), completed, submission.discovered().orElse(List.of()));
    }

    /**
     * Polls one due READY slice only after rereading the strict active Owner
     * boundary. The returned item remains a scheduler Claim candidate; the
     * caller must pass it to {@code ClaimHandoffWorkClassExecutor} with its
     * exact materialization, charge and prerequisite gate.
     */
    public List<ScheduleWorkItem> pollReady(
            final TrustedUtcIntervalEvidence evidence, final SchedulerBudget budget, final LongSupplier ownerClock) {
        return ownedShard.pollReadyAuthoritativelyStrict(
                authority,
                scheduler,
                Objects.requireNonNull(evidence, "evidence"),
                Objects.requireNonNull(budget, "budget"),
                Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    public PersistentLaneScheduler scheduler() {
        return scheduler;
    }

    public record DueTurn(
            WorkClassTask task, List<WorkClassTask> completedTasks, List<ScheduleWorkItem> discoveredItems) {
        public DueTurn {
            Objects.requireNonNull(task, "task");
            completedTasks = List.copyOf(Objects.requireNonNull(completedTasks, "completedTasks"));
            discoveredItems = List.copyOf(Objects.requireNonNull(discoveredItems, "discoveredItems"));
        }
    }
}
