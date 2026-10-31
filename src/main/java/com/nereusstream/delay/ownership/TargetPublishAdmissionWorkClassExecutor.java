package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** One exact signed Target Admission through the existing bounded Source queue; it never applies or sends locally. */
public final class TargetPublishAdmissionWorkClassExecutor {
    private final TargetWorkerShardRuntime worker;
    private final ShardLogMutationAppender appender;
    private Submission pending;

    public TargetPublishAdmissionWorkClassExecutor(
            final TargetWorkerShardRuntime worker, final ShardLogMutationAppender appender) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.appender = Objects.requireNonNull(appender, "appender");
    }

    void requireWorker(final TargetWorkerShardRuntime expected) {
        if (worker != expected) {
            throw new IllegalArgumentException("Target Admission handoff belongs to another Worker instance");
        }
    }

    /**
     * Retains signed bytes before queue admission. A failed or uncertain append retries those same bytes only;
     * neither a Broker receipt nor an in-process result makes the Claim eligible for physical submission.
     */
    public synchronized Submission submit(final SystemMutation mutation, final LongSupplier ownerClock) {
        final var image = Objects.requireNonNull(mutation, "mutation");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (image.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION) {
            throw new IllegalArgumentException("Target Admission handoff requires its independent mutation type");
        }
        final byte[] frame = image.encodeFrame();
        if (pending != null) {
            final boolean same = Arrays.equals(pending.mutation.encodeFrame(), frame);
            final var result = readFirstResult(pending, clock);
            if (result.isPresent()) {
                if (same) {
                    return pending;
                }
                if (pending.actionOutstanding) {
                    throw new IllegalStateException("Target Admission action has not exited its retained submission");
                }
                pending = null;
            } else {
                if (!same) {
                    throw new IllegalStateException("Target Admission already retains another exact signed image");
                }
                if (!pending.actionOutstanding
                        && (pending.append == null
                                || pending.append.disposition()
                                        != ShardLogMutationAppender.AppendDisposition.PERSISTED)) {
                    enqueue(pending, clock);
                }
                return pending;
            }
        }
        final var task = new WorkClassTask(
                WorkClass.OUTCOME_AND_CONTROL, "target-admission/" + Bytes.hex(Bytes.sha256(frame)), frame.length);
        final var submission = new Submission(image, task);
        pending = submission;
        if (readFirstResult(submission, clock).isPresent()) {
            return submission;
        }
        try {
            enqueue(submission, clock);
        } catch (RuntimeException | Error rejectedQueue) {
            // Retain the exact image for an explicit retry; no Source result or Claim release was observed.
            submission.failure = rejectedQueue;
            throw rejectedQueue;
        }
        return submission;
    }

    private void enqueue(final Submission submission, final LongSupplier clock) {
        final var priorAppend = submission.append;
        final var priorFailure = submission.failure;
        submission.append = null;
        submission.failure = null;
        submission.actionOutstanding = true;
        try {
            worker.submitAdmissionAction(submission.task, submission.mutation, clock, () -> {
                try {
                    final var result = worker.appendAdmission(submission.mutation, appender, clock);
                    submission.first = result.firstResult();
                    submission.append = result.append();
                } catch (ReadIncompleteException incomplete) {
                    submission.failure = incomplete;
                } catch (RuntimeException failure) {
                    submission.failure = failure;
                    submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
                } catch (Error fatal) {
                    submission.failure = fatal;
                    submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
                    throw fatal;
                } finally {
                    submission.actionOutstanding = false;
                }
            });
        } catch (RuntimeException | Error rejectedRetry) {
            submission.append = priorAppend;
            submission.failure = priorFailure;
            submission.actionOutstanding = false;
            throw rejectedRetry;
        }
    }

    /** APPLIED and REJECTED are both real terminal first results; only the separate Applied Store proof permits SEND. */
    public synchronized Optional<SystemMutationResult> settle(final LongSupplier ownerClock) {
        final var submission = Objects.requireNonNull(pending, "pending Admission");
        final var result = readFirstResult(submission, Objects.requireNonNull(ownerClock, "ownerClock"));
        if (result.isPresent() && !submission.actionOutstanding) {
            pending = null;
            return result;
        }
        return Optional.empty();
    }

    public synchronized Optional<Submission> pending() {
        return Optional.ofNullable(pending);
    }

    private Optional<SystemMutationResult> readFirstResult(final Submission submission, final LongSupplier clock) {
        final var result = worker.admissionMutationResult(submission.mutation, clock);
        if (result.isPresent()) {
            submission.first = result.get();
        }
        return result;
    }

    public static final class Submission {
        private final SystemMutation mutation;
        private final WorkClassTask task;
        private volatile ShardLogMutationAppender.AppendOutcome append;
        private volatile SystemMutationResult first;
        private volatile Throwable failure;
        private volatile boolean actionOutstanding;

        private Submission(final SystemMutation mutation, final WorkClassTask task) {
            this.mutation = mutation;
            this.task = task;
        }

        public SystemMutation mutation() {
            return mutation;
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<ShardLogMutationAppender.AppendOutcome> append() {
            return Optional.ofNullable(append);
        }

        public Optional<SystemMutationResult> firstResult() {
            return Optional.ofNullable(first);
        }

        public Optional<Throwable> failure() {
            return Optional.ofNullable(failure);
        }
    }
}
