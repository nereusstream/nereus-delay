package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Bounded recovery handoff for one retained Admission; it has no Journal or physical publication dependency. */
public final class TargetPublishRecoveryExecutor {
    private final TargetWorkerShardRuntime worker;
    private final TargetOutcomeWorkClassExecutor source;
    private final TargetPublishOutcomeMutationFactory outcomes;
    private Submission pending;

    public TargetPublishRecoveryExecutor(
            TargetWorkerShardRuntime worker, TargetOutcomeWorkClassExecutor source,
            TargetPublishOutcomeMutationFactory outcomes) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.source = Objects.requireNonNull(source, "source");
        this.source.requireWorker(this.worker);
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
    }

    /** Reads exact current admitted work once, signs the hold once, and retains it before queue submission. */
    public synchronized Submission submit(
            final BoundedReadBudget budget, final SystemMutation admission, final OwnerIdentity recoveryOwner,
            final WorkerPublishOutcomeMutationFactory.OutcomeContext context, final LongSupplier ownerClock) {
        Objects.requireNonNull(admission, "admission");
        Objects.requireNonNull(recoveryOwner, "recoveryOwner");
        if (pending != null) {
            if (!Arrays.equals(pending.admission.canonicalEnvelope(), admission.canonicalEnvelope())
                    || !pending.owner.equals(recoveryOwner)) {
                throw new IllegalStateException("Target recovery already retains another exact Admission/Owner");
            }
            return pending;
        }
        final var proof = worker.readRecoveryAdmission(budget, admission, ownerClock);
        final var submission = new Submission(admission, recoveryOwner,
                outcomes.createRecoveryUnknown(proof, recoveryOwner, context));
        pending = submission;
        handoff(submission, ownerClock);
        return submission;
    }

    /** Reuses the original signed bytes even if the Source append or queue acceptance was uncertain. */
    public synchronized void retryHandoff(final LongSupplier ownerClock) {
        handoff(Objects.requireNonNull(pending, "pending recovery"), ownerClock);
    }

    public synchronized boolean settleApplied(final LongSupplier ownerClock) {
        final var submission = Objects.requireNonNull(pending, "pending recovery");
        final var result = source.readApplied(submission.mutation, ownerClock).orElse(null);
        if (result == null) {
            return false;
        }
        if (result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                || !Bytes.constantTimeEquals(result.mutationHash(), submission.mutation.mutationHash())) {
            throw new IllegalStateException("Target recovery hold lacks its exact applied Source first result");
        }
        pending = null;
        return true;
    }

    private void handoff(final Submission submission, final LongSupplier ownerClock) {
        try {
            submission.handoff = source.submit(submission.mutation, ownerClock);
            submission.failure = null;
        } catch (RuntimeException failure) {
            submission.failure = failure;
        }
    }

    public static final class Submission {
        private final SystemMutation admission;
        private final OwnerIdentity owner;
        private final SystemMutation mutation;
        private volatile TargetOutcomeWorkClassExecutor.Submission handoff;
        private volatile Throwable failure;

        private Submission(SystemMutation admission, OwnerIdentity owner, SystemMutation mutation) {
            this.admission = admission;
            this.owner = owner;
            this.mutation = mutation;
        }

        public SystemMutation mutation() { return mutation; }
        public Optional<TargetOutcomeWorkClassExecutor.Submission> handoff() { return Optional.ofNullable(handoff); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
    }
}
