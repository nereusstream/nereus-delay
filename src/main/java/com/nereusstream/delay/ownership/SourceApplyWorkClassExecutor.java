package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Cross-package production entrypoint for one active Shard Log record.
 *
 * <p>The exact canonical Source Position plus NDL1 frame size is charged to
 * the bounded {@link WorkClass#SOURCE_APPLY} queue. The task identity binds
 * both byte sequences, so a same-position/different-record substitution
 * cannot reuse an existing action. Queue rejection has no Store, lease or
 * source-ack side effect.</p>
 *
 * <p>An ordinary apply failure is captured in the returned
 * {@link Submission}; {@link OwnedDelayShard} has already fenced the local
 * owner when the WriteBatch/authority result is unproven, and the physical
 * Broker record remains the retry authority. The generic work registry must
 * not create a second retry stream. A fatal {@link Error} is recorded and
 * rethrown into the event-loop fatal-stop path.</p>
 */
public final class SourceApplyWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN = Bytes.utf8("nereus-delay-source-apply-task\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final PublicKey verificationKey;

    public SourceApplyWorkClassExecutor(
            final WorkClassExecutionRegistry workClasses,
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final PublicKey verificationKey) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.verificationKey = Objects.requireNonNull(verificationKey, "verificationKey");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Registers one exact source action; the WriteBatch starts only when its bounded turn runs. */
    public Submission submit(final SourceReplayEntry entry, final LongSupplier clock) {
        return submit(entry, clock, false);
    }

    /**
     * Registers one exact recovery replay action in the same SOURCE_APPLY
     * queue. Recovery differs only in its local lifecycle (CATCHING_UP);
     * it never acknowledges a broker record and the caller retains the
     * source cursor until the returned outcome is proven.
     */
    Submission submitRecovery(final SourceReplayEntry entry, final LongSupplier clock) {
        return submit(entry, clock, true);
    }

    private Submission submit(final SourceReplayEntry entry, final LongSupplier clock, final boolean recovery) {
        final SourceReplayEntry submitted = Objects.requireNonNull(entry, "entry");
        final LongSupplier submittedClock = Objects.requireNonNull(clock, "clock");
        final byte[] positionBytes = submitted.position().canonicalBytes();
        final byte[] frameBytes = frame(submitted);
        final long chargedBytes = Math.addExact((long) positionBytes.length, frameBytes.length);
        if (recovery) {
            ownedShard.requireRecoverySourceApplySubmission(authority, submitted, verificationKey);
        } else {
            ownedShard.requireSourceApplySubmission(authority, submitted, verificationKey);
        }
        final String taskId = "source-apply/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, positionBytes, frameBytes));
        final WorkClassTask task = new WorkClassTask(WorkClass.SOURCE_APPLY, taskId, chargedBytes);
        final Submission result = new Submission(task);
        workClasses.submit(task, () -> execute(submitted, submittedClock, result, recovery));
        return result;
    }

    private void execute(
            final SourceReplayEntry entry,
            final LongSupplier clock,
            final Submission submission,
            final boolean recovery) {
        if (submission.outcome().isPresent()) {
            throw new IllegalStateException("source apply work-class action already completed");
        }
        try {
            final SourceReplayOutcome outcome = recovery
                    ? ownedShard.applyRecoverySourceEntryAuthoritativelyStrict(authority, entry, verificationKey, clock)
                    : ownedShard.applySourceEntryAuthoritativelyStrict(authority, entry, verificationKey, clock);
            submission.complete(ApplyOutcome.succeeded(outcome));
        } catch (RuntimeException failure) {
            submission.complete(ApplyOutcome.failed(failure));
        } catch (Error failure) {
            submission.complete(ApplyOutcome.failed(failure));
            throw failure;
        }
    }

    private static byte[] frame(final SourceReplayEntry entry) {
        if (entry instanceof SourceReplayRecord commandRecord) {
            return CommandCodec.encodeFrame(commandRecord.command());
        }
        return ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    /** Read-only process-local handle for one queued source action. */
    public static final class Submission {
        private final WorkClassTask task;
        private volatile ApplyOutcome outcome;

        private Submission(final WorkClassTask task) {
            this.task = Objects.requireNonNull(task, "task");
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<ApplyOutcome> outcome() {
            return Optional.ofNullable(outcome);
        }

        private synchronized void complete(final ApplyOutcome completed) {
            if (outcome != null) {
                throw new IllegalStateException("source apply work-class outcome is already complete");
            }
            outcome = Objects.requireNonNull(completed, "completed");
        }
    }

    /** Exactly one of result/failure is present. */
    public record ApplyOutcome(SourceReplayOutcome result, Throwable failure) {
        public ApplyOutcome {
            if ((result == null) == (failure == null)) {
                throw new IllegalArgumentException("source apply outcome must contain one branch");
            }
        }

        private static ApplyOutcome succeeded(final SourceReplayOutcome result) {
            return new ApplyOutcome(Objects.requireNonNull(result, "result"), null);
        }

        private static ApplyOutcome failed(final Throwable failure) {
            return new ApplyOutcome(null, Objects.requireNonNull(failure, "failure"));
        }
    }
}
