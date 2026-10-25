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
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** Appends one exact Control or SERVICE observation through the same bounded System Source slot. */
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
    public Submission submit(final TargetChannelControlMutationFactory.Prepared prepared,
            final LongSupplier ownerClock) {
        final var exact = Objects.requireNonNull(prepared, "prepared");
        return submit(exact.control(), exact.mutation(), ownerClock);
    }

    /** Shares the original single retained slot, WorkClass limits, registration and drain settlement. */
    public Submission submitObservation(final TargetSloObservationControlMutationFactory.Prepared prepared,
            final LongSupplier ownerClock) {
        final var exact = Objects.requireNonNull(prepared, "prepared");
        return submit(exact.control(), exact.mutation(), ownerClock);
    }

    public Submission submitFinalEvent(final TargetSloObservationEventMutationFactory.Prepared prepared,
            final LongSupplier ownerClock) {
        return submit(null, Objects.requireNonNull(prepared).mutation(), ownerClock);
    }

    public Submission submitWriter(final TargetSloWriterControlMutationFactory.Prepared prepared,
            final LongSupplier ownerClock) {
        final var exact = Objects.requireNonNull(prepared, "prepared");
        return submit(exact.control(), exact.mutation(), ownerClock);
    }

    private synchronized Submission submit(final PreparedControlOperation control, final SystemMutation image,
            final LongSupplier ownerClock) {
        Submission pending = pendingRef.get();
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        protection.run();
        if(control!=null)ControlTargetMutationBinding.validate(control, control.targets().getFirst(), image);
        else TargetSloObservationEventMutationFactory.Prepared.restore(image);
        final byte[] frame = image.encodeFrame();
        if (pending != null) {
            final boolean same = Arrays.equals(pending.mutation.encodeFrame(), frame)
                    && Arrays.equals(controlBytes(pending.control), controlBytes(control));
            final var result = readFirstResult(pending, clock);
            if (result.isPresent()) {
                if (!terminalResourcesReleased(pending)) {
                    enqueue(pending, clock);
                    if (!same) throw new IllegalStateException("original Control action or Scope read-loan cleanup is still retained", pending.failure);
                    return pending;
                }
                if (same) {
                    return pending;
                }
                pendingRef.compareAndSet(pending, null);
                pending = null;
            } else {
                if (!same) {
                    throw new IllegalStateException("Target channel lifecycle already retains another exact signed image");
                }
                if (actionExited(pending) && (pending.scopeCleanup != null || pending.append == null
                        || pending.append.disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED)) {
                    enqueue(pending, clock);
                }
                return pending;
            }
        }
        final boolean scoped = control != null && TargetSloControlScopeAuthority.reference(image).isPresent();
        final long scopeBytes = scoped ? registrations.scopedMutationReservationBytes() : 0;
        if (scoped && (scopeBytes <= 0 || scopeBytes == Long.MAX_VALUE)) {
            throw new IllegalStateException("scoped Control requires its complete finite original admission I/O reservation");
        }
        final var task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "target-control/" + Bytes.hex(Bytes.sha256(frame, control==null?new byte[0]:control.preparedDigest())),
                Math.addExact(scopeBytes, Math.addExact((long) frame.length, controlBytes(control).length)));
        final var submission = new Submission(control, image, task, scopeBytes);
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
        if (!actionExited(submission)) { return; }
        final boolean cleanupOnly = submission.first != null || submission.scopeCleanup != null;
        if (submission.first != null && terminalResourcesReleased(submission)) { return; }
        if (!submission.actionOutstanding.compareAndSet(false, true)) { return; }
        if (pendingRef.get() != submission) {
            submission.actionOutstanding.set(false);
            return;
        }
        try {
            final Runnable action = () -> {
                try {
                    if (cleanupOnly) {
                        if (!retryScopeCleanup(submission)) return;
                        final var held = submission.scopeHold;
                        if (submission.first != null && held != null && !submission.scopeHoldClosed) {
                            held.close();
                            submission.scopeHoldClosed = true;
                        }
                        submission.failure = null;
                        return;
                    }
                    protection.run();
                    // An actual Source first precedes all new registration, admission and append decisions.
                    if (readFirstResult(submission, clock).isPresent()) return;
                    if (!retryScopeCleanup(submission)) return;
                    if(submission.control!=null){
                        if (submission.scopeBytes != 0) {
                            var held = submission.scopeHold;
                            if (held == null) {
                                held = Objects.requireNonNull(registrations.admitScopedMutation(submission.control,
                                        submission.control.targets().getFirst(), submission.mutation, submission.scopeBytes,
                                        () -> requireOriginalAction(submission)), "original admitted Scope MEMBER hold");
                                // Keep the real hold before any later validation, guard or Source append can fail.
                                submission.scopeHold = held;
                            }
                            if (held.retainedBytes() < 0 || held.retainedBytes() > submission.scopeBytes) {
                                throw new IllegalStateException("actual Scope MEMBER exceeds its original admission reservation");
                            }
                            held.validate(submission.control, submission.control.targets().getFirst(), submission.mutation);
                            held.requireCurrent();
                        } else {
                            // Non-scoped Controls retain the existing immutable point-registration path.
                            registrations.register(submission.control);
                            registrations.validateMutation(submission.control, submission.control.targets().getFirst(), submission.mutation);
                        }
                    }
                    protection.run();
                    if (submission.scopeHold != null) submission.scopeHold.requireCurrent();
                    submission.failure = null;
                    final var result = worker.appendTargetControl(submission.mutation, appender, clock);
                    if (result.firstResult() != null) submission.first = result.firstResult();
                    submission.append = result.append();
                    if (submission.scopeHold != null) submission.scopeHold.requireCurrent();
                } catch (ReadIncompleteException incomplete) {
                    retainFailure(submission, incomplete);
                } catch (RuntimeException failure) {
                    retainFailure(submission, failure);
                    if (!cleanupOnly) retainUnknown(submission);
                } catch (Error fatal) {
                    retainFailure(submission, fatal);
                    if (!cleanupOnly) retainUnknown(submission);
                    throw fatal;
                } finally {
                    submission.actionOutstanding.set(false);
                }
            };
            if (cleanupOnly) worker.submitTargetControlCleanupAction(submission.task, action);
            else worker.submitTargetControlAction(submission.task, submission.mutation, clock, action);
        } catch (RuntimeException | Error rejectedRetry) {
            submission.actionOutstanding.set(false);
            throw rejectedRetry;
        }
    }

    /** One bounded retry of the retained image; safe for a Host holding the Worker monitor during drain. */
    public void retryPending(final LongSupplier clock) {
        final var submission = pendingRef.get();
        if (submission == null) return;
        final var first = readFirstResult(submission, clock);
        if (!actionExited(submission)) return;
        if (first.isPresent()) {
            if (!terminalResourcesReleased(submission)) enqueue(submission, clock);
            return;
        }
        if (submission.scopeCleanup != null) { enqueue(submission, clock); return; }
        if (submission.append != null
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
        if (result.isPresent() && terminalResourcesReleased(submission)) {
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
        final var result = worker.targetControlMutationResult(submission.mutation, clock);
        if (result.isPresent()) {
            submission.first = result.get();
        }
        protection.run();
        return result;
    }

    private boolean actionExited(Submission submission) {
        return !submission.actionOutstanding.get() && worker.targetControlActionExited(submission.task);
    }

    private boolean terminalResourcesReleased(Submission submission) {
        return actionExited(submission) && scopeCleanupReady(submission)
                && (submission.scopeHold == null || submission.scopeHoldClosed);
    }

    private void requireOriginalAction(Submission submission) {
        protection.run();
        if (pendingRef.get() != submission || !submission.actionOutstanding.get()) {
            throw new IllegalStateException("Scope admission lost its original retained Control action");
        }
    }

    /** A detached/cancelled receipt is not the underlying provider's native cleanup exit. */
    private static boolean cleanupAttemptExited(CompletableFuture<Void> attempt) {
        return attempt == null || attempt.isDone() && CheckpointInputFuture.ioExited(attempt)
                && (!attempt.isCancelled() || attempt instanceof CheckpointInputFuture<?>);
    }

    private static boolean scopeCleanupReady(Submission submission) {
        synchronized (submission) {
            while (submission.scopeCleanup != null) {
                final var original = submission.scopeCleanup;
                if (!original.cleanup.cleanupComplete() || !cleanupAttemptExited(original.attempt)) return false;
                submission.scopeCleanup = original.next;
            }
            return true;
        }
    }

    /** One retry of the same opaque acquisition/close cleanup; no new admission or append while it is pending. */
    private static boolean retryScopeCleanup(Submission submission) {
        if (scopeCleanupReady(submission)) return true;
        final ScopeCleanup original;
        synchronized (submission) {
            original = submission.scopeCleanup;
            if (original == null) return true;
            if (!cleanupAttemptExited(original.attempt)) return false;
        }
        final var attempt = Objects.requireNonNull(original.cleanup.retryCleanup(), "original Scope cleanup future");
        synchronized (submission) { original.attempt = attempt; }
        return scopeCleanupReady(submission);
    }

    private static void retainUnknown(Submission submission) {
        final var prior = submission.append;
        if (prior == null || prior.disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
        }
    }

    private static void retainFailure(Submission submission, Throwable failure) {
        submission.failure = failure;
        Throwable original = failure;
        while (original instanceof java.util.concurrent.CompletionException && original.getCause() != null) {
            original = original.getCause();
        }
        if (original instanceof CheckpointInputFuture.Cleanup cleanup) {
            synchronized (submission) {
                if (submission.scopeCleanup == null) {
                    submission.scopeCleanup = new ScopeCleanup(cleanup);
                } else {
                    var retained = submission.scopeCleanup;
                    while (retained != null && retained.cleanup != cleanup) {
                        retained = retained.next;
                    }
                    if (retained == null) {
                        final var partial = new ScopeCleanup(cleanup);
                        partial.next = submission.scopeCleanup;
                        submission.scopeCleanup = partial;
                    }
                }
            }
        }
    }

    /** Same original read-loan handles, including an opaque partial cleanup returned while retrying one. */
    private static final class ScopeCleanup {
        private final CheckpointInputFuture.Cleanup cleanup;
        private CompletableFuture<Void> attempt;
        private ScopeCleanup next;
        private ScopeCleanup(CheckpointInputFuture.Cleanup cleanup) { this.cleanup = cleanup; }
    }

    private static byte[] controlBytes(PreparedControlOperation control){return control==null?new byte[0]:control.canonicalBytes();}

    public static final class Submission {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;
        private final WorkClassTask task;
        private final long scopeBytes;
        private volatile TargetSloControlScopeAuthority.MemberHold scopeHold;
        private volatile boolean scopeHoldClosed;
        private volatile ScopeCleanup scopeCleanup;
        private volatile ShardLogMutationAppender.AppendOutcome append;
        private volatile SystemMutationResult first;
        private volatile Throwable failure;
        private final java.util.concurrent.atomic.AtomicBoolean actionOutstanding =
                new java.util.concurrent.atomic.AtomicBoolean();

        private Submission(final PreparedControlOperation control, final SystemMutation mutation,
                final WorkClassTask task, final long scopeBytes) {
            this.control = control;
            this.mutation = mutation;
            this.task = task;
            this.scopeBytes = scopeBytes;
        }

        /** Absent for SERVICE observations, which carry no Control registration. */
        public PreparedControlOperation control() { return control; }
        public SystemMutation mutation() { return mutation; }
        public WorkClassTask task() { return task; }
        public Optional<ShardLogMutationAppender.AppendOutcome> append() { return Optional.ofNullable(append); }
        public Optional<SystemMutationResult> firstResult() { return Optional.ofNullable(first); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
    }
}
