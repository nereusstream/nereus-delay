package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
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

    void requireWorker(final TargetWorkerShardRuntime expected) {
        if (worker != expected) {
            throw new IllegalArgumentException("Target publish recovery belongs to another Worker instance");
        }
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
                outcomes.createRecoveryUnknown(proof, recoveryOwner, context), false,null);
        pending = submission;
        handoff(submission, ownerClock);
        return submission;
    }

    public synchronized Submission submitResolution(BoundedReadBudget reads, SystemMutation admission,
            OwnerIdentity owner, WorkerPublishOutcomeMutationFactory.OutcomeContext context,
            TargetPublishRecoveryMaintenance.Resolution resolution, LongSupplier clock) {
        if (pending != null) {
            if (!Arrays.equals(pending.admission.canonicalEnvelope(), admission.canonicalEnvelope())
                    || !pending.owner.equals(owner) || !pending.resolution) {
                throw new IllegalStateException("Target recovery retains a different exact resolution");
            }
            return pending;
        }
        final var proof = worker.readKafkaUnknown(reads, admission, clock);
        final SystemMutation mutation;
        if(resolution.archived()!=null){
            resolution.archived().requireUnknown(proof);mutation=resolution.archived().mutation();
        }else{
            resolution.authority().requireAuthenticated(proof, resolution.evidence());
            mutation=resolution.signer().createKafkaResolution(proof,resolution.evidence(),resolution.service(),resolution.stableCode(),context);
        }
        // External authority ran outside the Store view; re-read exact pending facts before accepting the image.
        proof.requireSame(worker.readKafkaUnknown(reads, admission, clock));
        final var submission = new Submission(admission, owner, mutation, true,resolution.archived());
        pending = submission;
        handoff(submission, clock);
        return submission;
    }

    /** Reuses the original signed bytes even if the Source append or queue acceptance was uncertain. */
    public synchronized void retryHandoff(final LongSupplier ownerClock) {
        final var submission=Objects.requireNonNull(pending,"pending recovery");
        if(submission.observed==null)handoff(submission,ownerClock);
    }

    public synchronized boolean settleApplied(final LongSupplier ownerClock) {
        final var submission = Objects.requireNonNull(pending, "pending recovery");
        final var result = submission.observed!=null?submission.observed:source.readApplied(submission.mutation, ownerClock).orElse(null);
        if (result == null) {
            return false;
        }
        if (result.applyStatus() != ApplyStatus.APPLIED&&submission.archived==null
                || result.mutationType() != submission.mutation.type()
                ||!Bytes.constantTimeEquals(result.mutationId(),submission.mutation.systemMutationId())
                || !submission.resolution && result.stableCode() != StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                || !Bytes.constantTimeEquals(result.mutationHash(), submission.mutation.mutationHash())) {
            throw new IllegalStateException("Target recovery hold lacks its exact applied Source first result");
        }
        submission.observed=result;
        if(submission.archived!=null){
            try{
                if(submission.completion==null)submission.completion=submission.archived.completeSource();
                CheckpointInputFuture.retryCleanup(submission.completion);
                if(!CheckpointInputFuture.ioExited(submission.completion))return false;
                final var adopted=submission.completion.join();
                if(result.applyStatus()==ApplyStatus.APPLIED){
                    submission.archived.requireAdoption(adopted.orElseThrow(()->new IllegalStateException("APPLIED recovery lacks actual Native adoption")));
                }else if(adopted.isPresent())throw new IllegalStateException("REJECTED recovery cannot adopt external proof");
            }catch(RuntimeException failure){
                submission.failure=failure.getCause()==null?failure:failure.getCause();
                if(CheckpointInputFuture.ioExited(submission.completion))submission.completion=null;
                return false;
            }
        }
        source.releaseTerminal(submission.mutation,ownerClock);
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
        private final boolean resolution;
        private final TargetArchivedKafkaResolution archived;
        private SystemMutationResult observed;
        private java.util.concurrent.CompletableFuture<Optional<com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption>> completion;
        private volatile TargetOutcomeWorkClassExecutor.Submission handoff;
        private volatile Throwable failure;

        private Submission(SystemMutation admission, OwnerIdentity owner, SystemMutation mutation, boolean resolution,TargetArchivedKafkaResolution archived) {
            this.admission = admission;
            this.owner = owner;
            this.mutation = mutation;
            this.resolution = resolution;
            this.archived=archived;
        }

        public SystemMutation mutation() { return mutation; }
        public Optional<TargetOutcomeWorkClassExecutor.Submission> handoff() { return Optional.ofNullable(handoff); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
        public boolean awaitingCompletion(){return observed!=null&&archived!=null;}
        public boolean rejected(){return observed!=null&&observed.applyStatus()==ApplyStatus.REJECTED;}
    }
}
