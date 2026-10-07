package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Bounded Target source handoff. It retains exact mutation bytes; append success is not local application. */
public final class TargetOutcomeWorkClassExecutor {
    private final TargetWorkerShardRuntime worker;
    private final ShardLogMutationAppender appender;
    private Submission pending;

    public TargetOutcomeWorkClassExecutor(TargetWorkerShardRuntime worker, ShardLogMutationAppender appender) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.appender = Objects.requireNonNull(appender, "appender");
    }

    void requireWorker(final TargetWorkerShardRuntime expected) {
        if (worker != expected) {
            throw new IllegalArgumentException("Target Outcome handoff belongs to another Worker instance");
        }
    }

    public synchronized Submission submit(final SystemMutation mutation, final LongSupplier ownerClock) {
        final var image = Objects.requireNonNull(mutation, "mutation");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (pending != null) {
            final var applied = worker.outcomeMutationResult(pending.mutation, clock).orElse(null);
            if (applied != null) {
                if (applied.applyStatus() != ApplyStatus.APPLIED) {
                    throw new IllegalStateException("Target pending Outcome has a rejected Source first result");
                }
                if (Arrays.equals(pending.mutation.encodeFrame(), image.encodeFrame())) {
                    return pending;
                }
                pending = null;
            } else {
                if (!Arrays.equals(pending.mutation.encodeFrame(), image.encodeFrame())) {
                    throw new IllegalStateException("Target Outcome handoff already retains another exact mutation");
                }
                if (pending.append == null
                        || pending.append.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                    return pending;
                }
                final var retained = pending;
                enqueue(retained, clock);
                return retained;
            }
        }
        final byte[] frame = image.encodeFrame();
        final var task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "target-outcome/" + Bytes.hex(Bytes.sha256(frame)), frame.length);
        final var submission = new Submission(image, task);
        pending = submission;
        try {
            enqueue(submission, clock);
        } catch (RuntimeException | Error rejectedQueue) {
            pending = null;
            throw rejectedQueue;
        }
        return submission;
    }

    private void enqueue(final Submission submission, final LongSupplier clock) {
        final var priorAppend = submission.append;
        final var priorFailure = submission.failure;
        submission.append = null;
        submission.failure = null;
        try {
            worker.submitOutcomeAction(submission.task, submission.mutation, clock, () -> {
                try {
                    submission.append = worker.appendOutcome(submission.mutation, appender, clock);
                } catch (RuntimeException failure) {
                    worker.fenceMessageExpiry();
                    submission.failure = failure;
                    submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
                } catch (Error fatal) {
                    worker.fenceMessageExpiry();
                    submission.failure = fatal;
                    submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
                    throw fatal;
                }
            });
        } catch (RuntimeException | Error rejectedRetry) {
            submission.append = priorAppend;
            submission.failure = priorFailure;
            throw rejectedRetry;
        }
    }

    public Optional<SystemMutationResult> readApplied(final SystemMutation mutation, final LongSupplier ownerClock) {
        return worker.outcomeMutationResult(mutation, ownerClock);
    }

    public static final class Submission {
        private final SystemMutation mutation;
        private final WorkClassTask task;
        private volatile ShardLogMutationAppender.AppendOutcome append;
        private volatile Throwable failure;

        private Submission(SystemMutation mutation, WorkClassTask task) {
            this.mutation = mutation;
            this.task = task;
        }

        public SystemMutation mutation() { return mutation; }
        public Optional<ShardLogMutationAppender.AppendOutcome> append() { return Optional.ofNullable(append); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
    }
}
