package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** One retained retirement frame through the existing shared Source queue; no additional queue, SDK or thread. */
public final class TargetEvidenceRetirementWorkClassExecutor {
    private final TargetWorkerShardRuntime worker;
    private final OwnedSourceSystemWriter appender;
    private final AtomicReference<Submission> pending = new AtomicReference<>();

    TargetEvidenceRetirementWorkClassExecutor(TargetWorkerShardRuntime worker, OwnedSourceSystemWriter appender) {
        this.worker = Objects.requireNonNull(worker); this.appender = Objects.requireNonNull(appender);
    }

    void requireAppender(OwnedSourceSystemWriter expected) {
        if (appender != expected) throw new IllegalArgumentException("retirement handoff changed its actual Source writer");
    }

    synchronized Submission submit(SystemMutation image, Runnable protection, LongSupplier clock,
            java.util.function.Consumer<Submission> retained) {
        TargetEvidenceRetireIntentBody.decode(Objects.requireNonNull(image).canonicalBody());
        if(image.type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_RETIRE_INTENT)
            throw new IllegalArgumentException("retirement handoff requires its exact Source mutation type");
        Objects.requireNonNull(protection); Objects.requireNonNull(clock); Objects.requireNonNull(retained);
        final var prior = pending.get();
        if (prior != null) {
            final boolean same = Arrays.equals(prior.image.canonicalEnvelope(), image.canonicalEnvelope());
            if(same)retained.accept(prior);
            final var first = observe(prior, clock);
            if (first.isEmpty() || !prior.ioExited()) {
                if (!same) throw new IllegalStateException("original retirement Source handoff remains owned");
                if (first.isEmpty()) enqueue(prior, clock);
                return prior;
            }
            if (same) return prior;
            pending.compareAndSet(prior, null);
        }
        final byte[] frame = image.encodeFrame();
        final var task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "target-evidence-retire/" + Bytes.hex(Bytes.sha256(frame)), frame.length);
        final var next = new Submission(this, image, task, protection);
        pending.set(next);
        retained.accept(next);
        if (observe(next, clock).isEmpty()) {
            try { enqueue(next, clock); }
            catch (RuntimeException | Error rejected) { next.failure = rejected; throw rejected; }
        }
        return next;
    }

    private void enqueue(Submission submission, LongSupplier clock) {
        // The action's finally block precedes registry removal; never resubmit inside that small exit window.
        if (!worker.retirementActionExited(submission.task)) return;
        synchronized (submission) {
            if (submission.stopped || submission.closed || submission.queued || submission.running
                    || submission.append != null && submission.append.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED)
                return;
            submission.queued = true;
        }
        try {
            worker.submitRetirementAction(submission.task, submission.image, clock, () -> {
                synchronized (submission) {
                    submission.queued = false;
                    if (submission.stopped || submission.closed) return;
                    submission.running = true;
                }
                try {
                    final var result = worker.appendRetirement(submission.image, appender, submission.protection, clock);
                    if(result.firstResult()!=null)submission.first = result.firstResult();
                    submission.append = result.append(); submission.failure = null;
                } catch (ReadIncompleteException incomplete) {
                    submission.failure = incomplete;
                } catch (RuntimeException failure) {
                    submission.failure = failure; submission.append = ShardLogMutationAppender.AppendOutcome.unknown();
                } catch (Error fatal) {
                    submission.failure = fatal; submission.append = ShardLogMutationAppender.AppendOutcome.unknown(); throw fatal;
                } finally { synchronized (submission) { submission.running = false; } }
            });
        } catch (RuntimeException | Error rejected) {
            synchronized (submission) { submission.queued = false; }
            // Preserve every earlier append disposition, including UNKNOWN; failed admission changes no physical fact.
            throw rejected;
        }
    }

    public void retryPending(LongSupplier clock) {
        final var selected = pending.get();
        if (selected != null && observe(selected, clock).isEmpty()) enqueue(selected, clock);
    }

    public Optional<SystemMutationResult> settle(LongSupplier clock) {
        final var selected = pending.get();
        if (selected == null) return Optional.empty();
        final var first = observe(selected, clock);
        if (first.isPresent() && selected.closeForRelease()) {
            pending.compareAndSet(selected, null); return first;
        }
        return Optional.empty();
    }
    public Optional<SystemMutationResult> settle(SystemMutation image,LongSupplier clock){
        final var selected=pending.get();
        if(selected!=null&&!Arrays.equals(selected.image.canonicalEnvelope(),image.canonicalEnvelope()))
            throw new IllegalArgumentException("retirement reconciliation selected another exact frame");
        return selected==null?worker.retirementMutationResult(image,clock):settle(clock);
    }

    public Optional<Submission> pending() { return Optional.ofNullable(pending.get()); }

    private Optional<SystemMutationResult> observe(Submission selected, LongSupplier clock) {
        final var first = worker.retirementMutationResult(selected.image, clock);
        first.ifPresent(value -> selected.first = value); return first;
    }

    public static final class Submission {
        private final TargetEvidenceRetirementWorkClassExecutor owner;
        private final SystemMutation image;
        private final WorkClassTask task;
        private final Runnable protection;
        private volatile ShardLogMutationAppender.AppendOutcome append;
        private volatile SystemMutationResult first;
        private volatile Throwable failure;
        private boolean queued, running, stopped, closed;
        private Submission(TargetEvidenceRetirementWorkClassExecutor owner, SystemMutation image,
                WorkClassTask task, Runnable protection) {
            this.owner = owner; this.image = image; this.task = task; this.protection = protection;
        }
        public SystemMutation mutation() { return image; }
        public WorkClassTask task() { return task; }
        public Optional<ShardLogMutationAppender.AppendOutcome> append() { return Optional.ofNullable(append); }
        public Optional<SystemMutationResult> firstResult() { return Optional.ofNullable(first); }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
        public boolean ioExited() {
            synchronized (this) { if (running || !stopped && queued) return false; if (stopped) return true; }
            return owner.worker.retirementActionExited(task);
        }
        /** Requires an actual Native first recorded only by this executor, plus action/registry exit. */
        public boolean closeForRelease() {
            if (!owner.worker.retirementActionExited(task)) return false;
            synchronized (this) {
                if (first == null || queued || running) return false;
                closed = true; return true;
            }
        }
        /** Cancels unstarted append work under the same gate used by action entry; running I/O must still exit. */
        public synchronized void requestStop() { stopped = true; }
    }
}
