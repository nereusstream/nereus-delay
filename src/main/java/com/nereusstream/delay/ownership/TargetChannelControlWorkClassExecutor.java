package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Registers and appends one exact channel lifecycle through the actual bounded Source queue. */
public final class TargetChannelControlWorkClassExecutor {
    private final TargetWorkerShardRuntime worker;
    private final ShardLogMutationAppender appender;
    private final ControlTargetRegistrationAuthority registrations;
    private final Runnable protection;
    private final java.util.concurrent.atomic.AtomicReference<Submission> pendingRef =
            new java.util.concurrent.atomic.AtomicReference<>();

    public TargetChannelControlWorkClassExecutor(
            final TargetWorkerShardRuntime worker, final ShardLogMutationAppender appender,
            final ControlTargetRegistrationAuthority registrations, final Runnable protection) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.registrations = Objects.requireNonNull(registrations, "registrations");
        this.protection = Objects.requireNonNull(protection, "protection");
    }

    void requireWorker(final TargetWorkerShardRuntime expected) {
        if (worker != expected) {
            throw new IllegalArgumentException("Target channel lifecycle handoff belongs to another Worker instance");
        }
    }

    /**
     * Retains signed bytes before queue admission. A failed or uncertain append retries those same bytes only;
     * neither a Broker receipt nor an in-process result makes the Claim eligible for physical submission.
     */
    public synchronized Submission submit(final TargetChannelControlMutationFactory.Prepared prepared,
            final LongSupplier ownerClock) {
        Submission pending = pendingRef.get();
        final var exact = Objects.requireNonNull(prepared, "prepared");
        final var image = exact.mutation();
        final var control = exact.control();
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        protection.run();
        ControlTargetMutationBinding.validate(control, control.targets().getFirst(), image);
        final byte[] frame = image.encodeFrame();
        if (pending != null) {
            final boolean same = Arrays.equals(pending.mutation.encodeFrame(), frame)
                    && Arrays.equals(pending.control.canonicalBytes(), control.canonicalBytes());
            final var result = readFirstResult(pending, clock);
            if (result.isPresent()) {
                if (same) {
                    return pending;
                }
                if (pending.actionOutstanding.get()) {
                    throw new IllegalStateException("Target channel lifecycle action has not exited its retained submission");
                }
                pendingRef.compareAndSet(pending, null);
                pending = null;
            } else {
                if (!same) {
                    throw new IllegalStateException("Target channel lifecycle already retains another exact signed image");
                }
                if (!pending.actionOutstanding.get() && (pending.append == null
                        || pending.append.disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED)) {
                    enqueue(pending, clock);
                }
                return pending;
            }
        }
        final var task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "target-channel-control/" + Bytes.hex(Bytes.sha256(frame, control.preparedDigest())),
                Math.addExact((long) frame.length, control.canonicalBytes().length));
        final var submission = new Submission(control, image, task);
        pendingRef.set(submission);
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
        if (!submission.actionOutstanding.compareAndSet(false, true)) { return; }
        if (pendingRef.get() != submission) {
            submission.actionOutstanding.set(false);
            return;
        }
        final var priorAppend = submission.append;
        final var priorFailure = submission.failure;
        submission.append = null;
        submission.failure = null;
        try {
            worker.submitChannelControlAction(submission.task, submission.mutation, clock, () -> {
                try {
                    protection.run();
                    // Actual Oxia registration classifies response loss only by rereading these exact Prepared bytes.
                    registrations.register(submission.control);
                    registrations.validateMutation(submission.control, submission.control.targets().getFirst(),
                            submission.mutation);
                    protection.run();
                    final var result = worker.appendChannelControl(submission.mutation, appender, clock);
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
                    submission.actionOutstanding.set(false);
                }
            });
        } catch (RuntimeException | Error rejectedRetry) {
            submission.append = priorAppend;
            submission.failure = priorFailure;
            submission.actionOutstanding.set(false);
            throw rejectedRetry;
        }
    }

    /** One bounded retry of the retained image; safe for a Host holding the Worker monitor during drain. */
    public void retryPending(final LongSupplier clock) {
        final var submission = pendingRef.get();
        if (submission == null || readFirstResult(submission, clock).isPresent()
                || submission.actionOutstanding.get()
                || submission.append != null
                        && submission.append.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            return;
        }
        enqueue(submission, clock);
    }

    /** Actual first-result settlement only; callers must separately read the current Store slot before using it. */
    public Optional<SystemMutationResult> settle(final LongSupplier ownerClock) {
        final var submission = pendingRef.get();
        if (submission == null) { return Optional.empty(); }
        final var result = readFirstResult(submission, Objects.requireNonNull(ownerClock, "ownerClock"));
        if (result.isPresent() && !submission.actionOutstanding.get()) {
            pendingRef.compareAndSet(submission, null);
            return result;
        }
        return Optional.empty();
    }

    public Optional<Submission> pending() {
        return Optional.ofNullable(pendingRef.get());
    }

    private Optional<SystemMutationResult> readFirstResult(final Submission submission, final LongSupplier clock) {
        protection.run();
        final var result = worker.channelControlMutationResult(submission.mutation, clock);
        protection.run();
        if (result.isPresent()) {
            submission.first = result.get();
        }
        return result;
    }

    public static final class Submission {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;
        private final WorkClassTask task;
        private volatile ShardLogMutationAppender.AppendOutcome append;
        private volatile SystemMutationResult first;
        private volatile Throwable failure;
        private final java.util.concurrent.atomic.AtomicBoolean actionOutstanding =
                new java.util.concurrent.atomic.AtomicBoolean();

        private Submission(final PreparedControlOperation control, final SystemMutation mutation,
                final WorkClassTask task) {
            this.control = control;
            this.mutation = mutation;
            this.task = task;
        }

        public PreparedControlOperation control() { return control; }
        public SystemMutation mutation() { return mutation; }
        public WorkClassTask task() { return task; }
        public Optional<ShardLogMutationAppender.AppendOutcome> append() { return Optional.ofNullable(append); }
        public Optional<SystemMutationResult> firstResult() { return Optional.ofNullable(first); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
    }
}
