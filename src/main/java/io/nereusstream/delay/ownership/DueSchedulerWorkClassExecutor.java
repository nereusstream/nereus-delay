package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.scheduler.PersistentLaneScheduler;
import io.nereusstream.delay.scheduler.ScheduleWorkItem;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Active-owner entrypoint for one bounded persistent READY-discovery turn.
 *
 * <p>The task identity binds the exact Shard, canonical trusted-time evidence
 * and all scan-budget fields.  Its byte charge reserves the canonical request
 * bytes plus the complete configured scan-byte envelope.  Queue rejection is
 * side-effect free: the local strict-owner preflight reads neither Oxia nor
 * RocksDB, and the persistent scheduler is invoked only by the selected
 * {@link WorkClass#DUE_SCHEDULER} action.</p>
 *
 * <p>The action rereads the exact Owner Lease/session after any queue wait and
 * uses the evidence's earliest UTC bound as the inclusive due-through fence.
 * An ordinary or fatal discovery failure remains a generic failed action for
 * explicit exact retry; the READY index and scheduler rollback remain the
 * durable/process recovery authorities.  Successful discovery removes the
 * process action and exposes only the newly promoted due heads.  Claim and
 * Publish Admission remain a later bounded handoff and are deliberately not
 * performed by this discovery-only bridge.</p>
 */
public final class DueSchedulerWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-due-discovery-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final PersistentLaneScheduler scheduler;

    public DueSchedulerWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                         final OwnedDelayShard ownedShard,
                                         final OxiaOwnerLeaseStore authority,
                                         final PersistentLaneScheduler scheduler) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers one exact bounded discovery action without touching scheduler state. */
    public Submission submit(final TrustedUtcIntervalEvidence evidence,
                             final SchedulerBudget budget,
                             final LongSupplier ownerClock) {
        final TrustedUtcIntervalEvidence submittedEvidence = Objects.requireNonNull(evidence, "evidence");
        final SchedulerBudget submittedBudget = Objects.requireNonNull(budget, "budget");
        final LongSupplier submittedClock = Objects.requireNonNull(ownerClock, "ownerClock");
        ownedShard.requireDueSchedulerSubmission(authority, scheduler);
        final byte[] evidenceBytes = submittedEvidence.canonicalBytes();
        final long chargedBytes = Math.addExact(requestBytes(evidenceBytes), submittedBudget.maxBytes());
        final WorkClassTask task = new WorkClassTask(WorkClass.DUE_SCHEDULER,
                taskId(scheduler.shardId(), evidenceBytes, submittedBudget), chargedBytes);
        final Submission submission = new Submission(task);
        workClasses.submit(task, () -> submission.complete(
                ownedShard.discoverReadyAuthoritativelyStrict(authority, scheduler,
                        submittedEvidence, submittedBudget, submittedClock)));
        return submission;
    }

    private static String taskId(final ShardId shard, final byte[] evidence,
                                 final SchedulerBudget budget) {
        final byte[] digest = Bytes.sha256(TASK_ID_DOMAIN,
                shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition()),
                Bytes.lp32(evidence), Bytes.u32be(budget.maxMessages()),
                Bytes.u64be(budget.maxBytes()), Bytes.u64be(budget.maxElapsedNanos()));
        return "due-discovery/" + Bytes.hex(digest);
    }

    private static long requestBytes(final byte[] evidence) {
        return Math.addExact(44L, evidence.length);
    }

    /** Read-only process-local handle for one queued discovery action. */
    public static final class Submission {
        private final WorkClassTask task;
        private volatile List<ScheduleWorkItem> discovered;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<List<ScheduleWorkItem>> discovered() {
            return Optional.ofNullable(discovered);
        }

        private synchronized void complete(final List<ScheduleWorkItem> result) {
            if (discovered != null) {
                throw new IllegalStateException("due scheduler discovery already completed");
            }
            discovered = List.copyOf(Objects.requireNonNull(result, "result"));
        }
    }
}
