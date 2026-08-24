package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ClaimMaterializationV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import com.nereusstream.delay.scheduler.PersistentLaneScheduler;
import com.nereusstream.delay.scheduler.ScheduleWorkItem;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded active-owner handoff from one polled READY head to a reversible Claim.
 *
 * <p>The caller may supply an exact typed materialization or ask this
 * executor to derive the local V1 projection from the durable Message and
 * binding.  In both forms the executor repeats every local fence after the
 * queue wait.  The injected prerequisite gate is the production integration
 * point for immutable Profile/Object Store/Adapter serialization and current
 * channel/evidence/credential generations; this class does not invent those
 * external authorities.</p>
 */
public final class ClaimHandoffWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-claim-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final PersistentLaneScheduler scheduler;
    private final ClaimExecutionAdmission permits;
    private final ClaimPrerequisiteGate prerequisiteGate;
    private final Map<String, Submission> submissions = new HashMap<>();

    public ClaimHandoffWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final PersistentLaneScheduler scheduler,
            final ClaimExecutionAdmission permits,
            final ClaimPrerequisiteGate prerequisiteGate) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.permits = Objects.requireNonNull(permits, "permits");
        this.prerequisiteGate = Objects.requireNonNull(prerequisiteGate, "prerequisiteGate");
        this.workClasses.bindClaimExecutionAdmission(this.permits);
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Requires callers to use the registry that owns this executor's queue. */
    void requireWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        if (workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalArgumentException("Claim handoff executor uses another work-class registry");
        }
    }

    /**
     * Derives the accepted V1 binding projection before queueing the exact
     * Claim handoff.  The charge, deadline, and live prerequisite gate remain
     * explicit caller inputs.
     */
    public Submission submit(
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence,
            final long claimDeadlineEpochMs,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        final ClaimMaterializationV1 materialization = ownedShard.resolveClaimMaterializationAuthoritativelyStrict(
                authority, scheduler, item, evidence, ownerClock);
        return submit(item, evidence, claimDeadlineEpochMs, materialization, claimedCharge, ownerClock);
    }

    /**
     * Binds and queues one exact already-polled Claim action. Queue rejection
     * restores the head and persisted fairness projection before returning.
     */
    public Submission submit(
            final ScheduleWorkItem item,
            final TrustedUtcIntervalEvidence evidence,
            final long claimDeadlineEpochMs,
            final ClaimMaterializationV1 materialization,
            final byte[] claimedCharge,
            final LongSupplier ownerClock) {
        final ClaimRequest request =
                ClaimRequest.create(item, evidence, claimDeadlineEpochMs, materialization, claimedCharge, ownerClock);
        final byte[] requestBytes = request.canonicalTaskBytes(scheduler);
        final String taskId = "claim-handoff/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, requestBytes));
        final WorkClassTask task = new WorkClassTask(WorkClass.DUE_SCHEDULER, taskId, requestBytes.length);
        final Submission submission = new Submission(task);
        boolean selectedForHandoff = false;
        synchronized (this) {
            if (submissions.containsKey(taskId)) {
                throw new IllegalStateException("Claim handoff task identity is already registered");
            }
            submissions.put(taskId, submission);
        }
        try {
            final PersistentLaneScheduler.ClaimCandidate candidate =
                    ownedShard.requireClaimSubmission(authority, scheduler, request.item, request.evidence);
            selectedForHandoff = true;
            request.requireCandidate(candidate);
            workClasses.submit(task, () -> execute(submission, request));
            return submission;
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                submissions.remove(taskId, submission);
            }
            if (selectedForHandoff) {
                try {
                    scheduler.requeueFailedClaim(request.item);
                } catch (RuntimeException | Error rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            } else {
                ownedShard.fence();
            }
            throw failure;
        }
    }

    private void execute(final Submission submission, final ClaimRequest request) {
        ClaimExecutionAdmission.Reservation reservation = null;
        try {
            final PersistentLaneScheduler.ClaimCandidate candidate =
                    ownedShard.requireClaimSubmission(authority, scheduler, request.item, request.evidence);
            request.requireCandidate(candidate);
            final PrerequisiteDecision prerequisite = Objects.requireNonNull(
                    prerequisiteGate.validate(new ClaimPrerequisite(
                            candidate, request.evidence, request.materialization, request.claimDeadlineEpochMs)),
                    "Claim prerequisite decision");
            if (!prerequisite.ready()) {
                scheduler.requeueFailedClaim(request.item);
                submission.complete(ClaimHandoffResult.prerequisiteUnavailable(prerequisite.reason()));
                removeCompleted(submission);
                return;
            }
            final ClaimExecutionAdmission.AdmissionDecision permit = permits.tryAcquire(
                    scheduler.shardId(),
                    request.item.laneId(),
                    candidate.laneIncarnation(),
                    request.item.messageId(),
                    Integer.toUnsignedLong(request.item.generation()),
                    request.item.accountedBytes());
            if (!permit.granted()) {
                scheduler.requeueFailedClaim(request.item);
                submission.complete(ClaimHandoffResult.permitUnavailable(permit.rejection()));
                removeCompleted(submission);
                return;
            }
            reservation = permit.reservation();
            final ClaimRecord claim = ownedShard.claimAuthoritativelyStrict(
                    authority,
                    scheduler,
                    request.item,
                    request.evidence,
                    request.claimDeadlineEpochMs,
                    request.materialization,
                    request.claimedCharge,
                    request.ownerClock);
            submission.complete(ClaimHandoffResult.claimed(claim, reservation));
            reservation = null;
            removeCompleted(submission);
        } catch (RuntimeException | Error failure) {
            if (reservation != null) {
                try {
                    reservation.release();
                } catch (RuntimeException | Error releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            // Expected deferral paths return above. Any exception reaching
            // here leaves the selected head or Claim boundary uncertain;
            // fencing is safer than manufacturing a second local retry.
            ownedShard.fence();
            throw failure;
        }
    }

    private synchronized void removeCompleted(final Submission submission) {
        if (!submissions.remove(submission.task.taskId(), submission)) {
            throw new IllegalStateException("Claim handoff registration changed while running");
        }
    }

    /** External live prerequisites that cannot be inferred from shard-local bytes. */
    @FunctionalInterface
    public interface ClaimPrerequisiteGate {
        PrerequisiteDecision validate(ClaimPrerequisite prerequisite);
    }

    /** Exact context presented to the external live-prerequisite implementation. */
    public record ClaimPrerequisite(
            PersistentLaneScheduler.ClaimCandidate candidate,
            TrustedUtcIntervalEvidence evidence,
            ClaimMaterializationV1 materialization,
            long claimDeadlineEpochMs) {
        public ClaimPrerequisite {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(evidence, "evidence");
            Objects.requireNonNull(materialization, "materialization");
            if (claimDeadlineEpochMs < 0) {
                throw new IllegalArgumentException("Claim deadline must be non-negative");
            }
        }
    }

    /** Closed local result of the injected live-prerequisite check. */
    public record PrerequisiteDecision(boolean ready, PrerequisiteRejection reason) {
        public PrerequisiteDecision {
            if (ready == (reason != null)) {
                throw new IllegalArgumentException("Claim prerequisite must be ready or rejected");
            }
        }

        public static PrerequisiteDecision available() {
            return new PrerequisiteDecision(true, null);
        }

        public static PrerequisiteDecision unavailable(final PrerequisiteRejection reason) {
            return new PrerequisiteDecision(false, Objects.requireNonNull(reason, "reason"));
        }
    }

    public enum PrerequisiteRejection {
        PROFILE_OR_CATALOG_UNAVAILABLE,
        PAYLOAD_OR_SERIALIZATION_UNAVAILABLE,
        CHANNEL_OR_CREDENTIAL_UNAVAILABLE
    }

    public enum ResultKind {
        CLAIMED,
        PREREQUISITE_UNAVAILABLE,
        PERMIT_UNAVAILABLE
    }

    /** Completed local handoff result; the Claim permit stays active on success. */
    public record ClaimHandoffResult(
            ResultKind kind,
            ClaimRecord claim,
            ClaimExecutionAdmission.Reservation reservation,
            PrerequisiteRejection prerequisiteRejection,
            ClaimExecutionAdmission.Rejection permitRejection) {
        public ClaimHandoffResult {
            Objects.requireNonNull(kind, "kind");
            final boolean claimed = kind == ResultKind.CLAIMED;
            if (claimed != (claim != null && reservation != null)
                    || claimed == (prerequisiteRejection != null || permitRejection != null)
                    || (kind == ResultKind.PREREQUISITE_UNAVAILABLE) != (prerequisiteRejection != null)
                    || (kind == ResultKind.PERMIT_UNAVAILABLE) != (permitRejection != null)) {
                throw new IllegalArgumentException("invalid Claim handoff result");
            }
        }

        private static ClaimHandoffResult claimed(
                final ClaimRecord claim, final ClaimExecutionAdmission.Reservation reservation) {
            return new ClaimHandoffResult(
                    ResultKind.CLAIMED,
                    Objects.requireNonNull(claim, "claim"),
                    Objects.requireNonNull(reservation, "reservation"),
                    null,
                    null);
        }

        private static ClaimHandoffResult prerequisiteUnavailable(final PrerequisiteRejection rejection) {
            return new ClaimHandoffResult(
                    ResultKind.PREREQUISITE_UNAVAILABLE,
                    null,
                    null,
                    Objects.requireNonNull(rejection, "rejection"),
                    null);
        }

        private static ClaimHandoffResult permitUnavailable(final ClaimExecutionAdmission.Rejection rejection) {
            return new ClaimHandoffResult(
                    ResultKind.PERMIT_UNAVAILABLE, null, null, null, Objects.requireNonNull(rejection, "rejection"));
        }
    }

    /** Read-only process handle for one exact queued handoff. */
    public static final class Submission {
        private final WorkClassTask task;
        private volatile ClaimHandoffResult result;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<ClaimHandoffResult> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final ClaimHandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("Claim handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }

    private static final class ClaimRequest {
        private final ScheduleWorkItem item;
        private final TrustedUtcIntervalEvidence evidence;
        private final long claimDeadlineEpochMs;
        private final ClaimMaterializationV1 materialization;
        private final byte[] claimedCharge;
        private final LongSupplier ownerClock;

        private ClaimRequest(
                final ScheduleWorkItem item,
                final TrustedUtcIntervalEvidence evidence,
                final long claimDeadlineEpochMs,
                final ClaimMaterializationV1 materialization,
                final byte[] claimedCharge,
                final LongSupplier ownerClock) {
            this.item = item;
            this.evidence = evidence;
            this.claimDeadlineEpochMs = claimDeadlineEpochMs;
            this.materialization = materialization;
            this.claimedCharge = Bytes.copy(claimedCharge);
            this.ownerClock = ownerClock;
        }

        private static ClaimRequest create(
                final ScheduleWorkItem item,
                final TrustedUtcIntervalEvidence evidence,
                final long claimDeadlineEpochMs,
                final ClaimMaterializationV1 materialization,
                final byte[] claimedCharge,
                final LongSupplier ownerClock) {
            final ScheduleWorkItem selected = Objects.requireNonNull(item, "item");
            final TrustedUtcIntervalEvidence trusted = Objects.requireNonNull(evidence, "evidence");
            final ClaimMaterializationV1 prepared = Objects.requireNonNull(materialization, "materialization");
            final byte[] chargeBytes = Objects.requireNonNull(claimedCharge, "claimedCharge");
            final PublishAdmissionBody.ChargeVector charge =
                    PublishAdmissionBody.ChargeVector.decodeCanonical(chargeBytes);
            if (claimDeadlineEpochMs <= trusted.latestEpochMs() || claimDeadlineEpochMs > prepared.expireAtEpochMs()) {
                throw new IllegalArgumentException("Claim deadline is outside the live materialization window");
            }
            if (!prepared.messageId().equals(selected.messageId())
                    || prepared.generation() != Integer.toUnsignedLong(selected.generation())) {
                throw new IllegalArgumentException("Claim materialization differs from selected work identity");
            }
            if (charge.inflightMessages() != 1 || charge.inflightBytes() < selected.accountedBytes()) {
                throw new IllegalArgumentException("Claim charge does not cover selected work");
            }
            return new ClaimRequest(
                    selected,
                    trusted,
                    claimDeadlineEpochMs,
                    prepared,
                    chargeBytes,
                    Objects.requireNonNull(ownerClock, "ownerClock"));
        }

        private void requireCandidate(final PersistentLaneScheduler.ClaimCandidate candidate) {
            if (!candidate.item().equals(item)) {
                throw new IllegalStateException("Claim scheduler candidate changed");
            }
        }

        private byte[] canonicalTaskBytes(final PersistentLaneScheduler scheduler) {
            return Bytes.concat(
                    scheduler.shardId().routeIncarnation().bytes(),
                    Bytes.u32beBits(scheduler.shardId().partition()),
                    item.laneId().bytes(),
                    item.messageId().bytes(),
                    Bytes.u32beBits(item.generation()),
                    Bytes.i64be(item.eligibleAtEpochMs()),
                    Bytes.u64be(item.accountedBytes()),
                    Bytes.lp32(evidence.canonicalBytes()),
                    Bytes.i64be(claimDeadlineEpochMs),
                    Bytes.lp32(materialization.canonicalBytes()),
                    Bytes.lp32(claimedCharge));
        }
    }
}
