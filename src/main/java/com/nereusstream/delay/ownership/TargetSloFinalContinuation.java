package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetSloFinalCandidates;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;

/** One business Source entry's bounded post-sync observations; completion never waits for the next Source apply. */
public final class TargetSloFinalContinuation implements SourceApplyTarget.FinalContinuation {
    /** Original post-read UTC, SERVICE signer and complete reservation/retention custody; all I/O stays on the caller pool. */
    @FunctionalInterface
    public interface Provider {
        Held prepare(List<TargetSloFinalCandidates.Candidate> candidates, TargetQuotaMutation inspectedAt,
                SourceReplayEntry entry, SourceReplayOutcome outcome, Runnable guard);
    }

    public interface Held extends AutoCloseable {
        /** Original qualified interval sampled after this continuation's actual guarded candidate read. */
        TrustedUtcIntervalEvidence observedAt();
        TargetSloObservationEventMutationFactory signer(TargetSloFinalCandidates.Candidate candidate);
        long retryUntilEpochMs(TargetSloFinalCandidates.Candidate candidate);
        /** Complete original time, signer and reservation material; generated frames are charged independently. */
        long retainedBytes();
        /** Local checks of the original held qualification only; no remote I/O. */
        void requireCurrent();
        /** Same-handle, retryable resource release; called only by owned caller-pool I/O. */
        @Override void close();
    }

    private record Step(Throwable failure) {}
    private final TargetWorkerShardRuntime worker;
    private final ShardLogMutationAppender appender;
    private final ThreadPoolExecutor executor;
    private final List<TargetSloFinalCandidates.Candidate> candidates;
    private final TargetQuotaMutation inspectedAt;
    private final SourceReplayEntry entry;
    private final SourceReplayOutcome outcome;
    private final Provider provider;
    private final long maximumBytes, capturedBytes;
    private final WorkClassTask task;
    private OwnedCheckpointIo<Step> operation;
    private OwnedCheckpointIo<Boolean> closing;
    private Held held;
    private TrustedUtcIntervalEvidence observedAt;
    private List<TargetSloObservationEventMutationFactory.Prepared> frames;
    private int persisted;
    private SourcePosition persistedThrough;
    private boolean actionOutstanding, stopping, heldClosed, framesValidated, cleanupOutstanding;
    private Throwable lastFailure;

    public TargetSloFinalContinuation(TargetWorkerShardRuntime worker, ShardLogMutationAppender appender,
            ThreadPoolExecutor executor, List<TargetSloFinalCandidates.Candidate> candidates,
            TargetQuotaMutation inspectedAt, SourceReplayEntry entry, SourceReplayOutcome outcome,
            Provider provider, long maximumBytes) {
        this.worker = Objects.requireNonNull(worker); this.appender = Objects.requireNonNull(appender);
        this.executor = Objects.requireNonNull(executor); this.candidates = List.copyOf(candidates);
        this.inspectedAt = Objects.requireNonNull(inspectedAt); this.entry = Objects.requireNonNull(entry);
        this.outcome = Objects.requireNonNull(outcome); this.provider = Objects.requireNonNull(provider);
        if (maximumBytes <= 0 || maximumBytes == Long.MAX_VALUE || this.candidates.isEmpty()
                || this.candidates.size() > TargetSloObservationPolicy.MAX_DECLARATIONS
                || executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || executor.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("SLO Final continuation requires its finite original task and caller I/O envelope");
        }
        inspectedAt.requireSourceApplied();
        if (!Arrays.equals(entry.position().canonicalBytes(), outcome.position().canonicalBytes())
                || !Arrays.equals(entry.position().canonicalBytes(), inspectedAt.source().canonicalBytes())) {
            throw new IllegalArgumentException("SLO continuation changed original business Source/read cut");
        }
        final var sourceFrame = entry instanceof SourceReplayRecord command
                ? CommandCodec.encodeFrame(command.command()) : ((SourceReplayMutation) entry).mutation().encodeFrame();
        long bytes = Math.addExact((long) sourceFrame.length, entry.position().canonicalBytes().length);
        bytes = Math.addExact(bytes, inspectedAt.canonicalBytes().length);
        bytes = Math.addExact(bytes, outcome.isCommand() ? outcome.commandResult().encode().length
                : outcome.systemMutationResult().encode().length);
        byte[] identities = new byte[0];
        final var sampleIds = new HashSet<String>();
        for (var candidate : this.candidates) {
            if (candidate.sample().observation().finalObservation() != null
                    || !Arrays.equals(candidate.syncedFirst().mutation().source().canonicalBytes(), entry.position().canonicalBytes())
                    || !sampleIds.add(Bytes.hex(candidate.sample().observation().sampleId()))) {
                throw new IllegalArgumentException("SLO continuation cannot repeat a Final or manufacture another sync");
            }
            candidate.syncedFirst().mutation().requireAtOrBefore(inspectedAt);
            bytes = Math.addExact(bytes, candidate.sample().canonicalBytes().length);
            bytes = Math.addExact(bytes, candidate.generation().canonicalBytes().length);
            bytes = Math.addExact(bytes, candidate.syncedFirst().canonicalBytes().length);
            identities = Bytes.concat(identities, candidate.sample().observation().sampleId());
        }
        if (bytes >= maximumBytes) throw new IllegalArgumentException("SLO captured inputs leave no qualified Final frame envelope");
        this.maximumBytes = maximumBytes; this.capturedBytes = bytes;
        task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "target-slo-final-source/" + Bytes.hex(Bytes.sha256(
                Bytes.utf8("nereus-delay-target-slo-final-continuation\0"), sourceFrame,
                entry.position().canonicalBytes(), inspectedAt.canonicalBytes(), identities)), maximumBytes);
    }

    @Override public boolean complete() {
        harvest();
        synchronized (this) {
            if (stopping || frames == null || persisted != frames.size() || !heldClosed
                    || actionOutstanding || operation != null || closing != null || cleanupOutstanding
                    || lastFailure != null) return false;
        }
        return worker.sloFinalContinuationActionExited(task);
    }

    /** WorkClass owns only a nonblocking submit/poll; provider, append and release run on the caller executor. */
    @Override public void schedule(LongSupplier clock) {
        Objects.requireNonNull(clock);
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release;
        synchronized (this) { current = operation; release = closing; }
        if (current != null) current.retryCleanup();
        if (release != null) release.retryCleanup();
        harvest();
        synchronized (this) {
            if (stopping || actionOutstanding || operation != null || closing != null) return;
            if (frames != null && persisted == frames.size() && heldClosed && lastFailure == null) return;
        }
        if (!worker.sloFinalContinuationActionExited(task)) return;
        synchronized (this) {
            if (stopping || actionOutstanding || operation != null || closing != null) return;
            actionOutstanding = true;
        }
        try { worker.submitSloFinalContinuationAction(task, () -> launch(clock)); }
        catch (RuntimeException | Error failure) {
            synchronized (this) { actionOutstanding = false; lastFailure = failure; }
            throw failure;
        }
    }

    private void launch(LongSupplier clock) {
        try {
            synchronized (this) { if (stopping) return; }
            final var next = OwnedCheckpointIo.submit(guard -> step(clock, guard), () -> requireCurrent(clock), executor);
            synchronized (this) { operation = next; if (stopping) next.requestStop(); }
        } catch (RuntimeException | Error failure) {
            synchronized (this) { lastFailure = failure; }
        } finally { synchronized (this) { actionOutstanding = false; } }
    }

    private Step step(LongSupplier clock, Runnable guard) {
        guard.run();
        Held original;
        synchronized (this) { original = held; }
        if (original == null) {
            original = Objects.requireNonNull(provider.prepare(candidates, inspectedAt, entry, outcome, guard), "original Final inputs");
            // Custody precedes any later guard, getter, validation, stop or append failure.
            synchronized (this) { held = original; }
        }
        final var exact = prepareFrames(original, guard);
        final boolean validated;
        synchronized (this) { validated = framesValidated; }
        if (!validated) {
            validateFrames(original, exact);
            synchronized (this) { framesValidated = true; }
        }
        guard.run();
        final int index; final boolean closed;
        synchronized (this) { index = persisted; closed = heldClosed; }
        if (index == exact.size()) {
            if (!closed) release(original);
            return new Step(null);
        }
        original.requireCurrent();
        final var image = exact.get(index).mutation();
        final var first = worker.sloFinalMutationResult(image, clock);
        if(first.isPresent())rememberPersisted(SourcePositionCodec.decode(first.orElseThrow().appliedSourcePosition()));
        guard.run();
        if (first.isPresent()) {
            // This settles exact Source durability only; its actual applied/rejected result remains authoritative.
            synchronized (this) { persisted++; }
            return new Step(null);
        }
        worker.requireSloFinalWriter(image, clock); guard.run(); original.requireCurrent();
        final ShardLogMutationAppender.AppendOutcome appended;
        try { appended = Objects.requireNonNull(appender.append(image), "original Final Source append"); }
        catch (RuntimeException uncertain) { guard.run(); return new Step(uncertain); }
        guard.run(); original.requireCurrent();
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            worker.requireSloFinalAppend(image, appended, inspectedAt, clock);
            rememberPersisted(appended.sourcePosition());
            guard.run(); original.requireCurrent();
            synchronized (this) { persisted++; }
        }
        return new Step(null); // UNKNOWN/definite non-persistence keeps the same index and exact frame.
    }

    /** A real append/first-result barrier, retained independently of later provider/cleanup failures. */
    public synchronized SourcePosition persistedThrough(){return persistedThrough;}

    private synchronized void rememberPersisted(SourcePosition position){
        TargetSourcePosition.requireBounded(Objects.requireNonNull(position));
        if(!position.shardId().equals(inspectedAt.source().shardId())||!position.sameSourceIdentity(inspectedAt.source()))
            throw new IllegalStateException("Final settlement changed its original physical Source");
        if(persistedThrough==null||position.compareTo(persistedThrough)>0)persistedThrough=position;
    }

    private List<TargetSloObservationEventMutationFactory.Prepared> prepareFrames(Held original, Runnable guard) {
        TrustedUtcIntervalEvidence time;
        synchronized (this) { time = observedAt; }
        if (time == null) {
            guard.run(); original.requireCurrent();
            final var sampled = Objects.requireNonNull(original.observedAt(), "original post-sync UTC observation");
            TargetNativePolicySnapshot.requireBoundedTime(sampled);
            // Keep this one observation even if the next signer/guard fails after producing an earlier frame.
            synchronized (this) { observedAt = sampled; time = sampled; }
        }
        for (;;) {
            final int next;
            synchronized (this) { next = frames == null ? 0 : frames.size(); }
            if (next == candidates.size()) { synchronized (this) { return frames; } }
            guard.run(); original.requireCurrent();
            final var candidate = candidates.get(next);
            final var evidence = entry instanceof SourceReplayRecord
                    ? new TargetSloFinalEvidence(TargetSloFinalEvidence.Kind.COMMAND_SYNC, time, inspectedAt, null, null)
                    : new TargetSloFinalEvidence(TargetSloFinalEvidence.Kind.ADMISSION_SYNC, time, inspectedAt,
                            ((SourceReplayMutation) entry).mutation(), candidate.syncedFirst().mutation());
            final var prepared = Objects.requireNonNull(original.signer(candidate), "original qualified SERVICE signer")
                    .prepareObserved(candidate.generation(), candidate.sample(), evidence, original.retryUntilEpochMs(candidate));
            synchronized (this) {
                final var retained = new java.util.ArrayList<TargetSloObservationEventMutationFactory.Prepared>();
                if (frames != null) retained.addAll(frames);
                retained.add(prepared); frames = List.copyOf(retained);
            }
            guard.run(); // The exact new frame is already retained before any post-signing loss of authority.
        }
    }

    private void validateFrames(Held original, List<TargetSloObservationEventMutationFactory.Prepared> exact) {
        if (exact.size() != candidates.size()) throw new IllegalArgumentException("Final provider omitted or added a required sample");
        long bytes = 0;
        for (int index = 0; index < exact.size(); index++) {
            final var candidate = candidates.get(index); final var sample = candidate.sample(); final var generation = candidate.generation();
            final var input = exact.get(index).mutation(); final var body = TargetSloObservationEventBody.decode(input.canonicalBody());
            final var evidence = Objects.requireNonNull(body.evidence(), "fresh observation requires complete body4 post-sync evidence");
            if (!sample.scope().equals(body.scope()) || !Arrays.equals(sample.lineage(), body.lineage())
                    || body.generation() != sample.generation() || generation.generation() != sample.generation()
                    || !Arrays.equals(body.sampleId(), sample.observation().sampleId())
                    || !Arrays.equals(body.bindingDigest(), sample.bindingDigest())
                    || !Arrays.equals(input.authorIdentity(), generation.policy().service().canonicalBytes())
                    || input.signingKeyVersion() != generation.policy().keyVersion()
                    || !evidence.inspectedAt().equals(inspectedAt) || !body.isSourceAssigned()) {
                throw new IllegalArgumentException("Final provider changed original sample/binding/author/read cut or Source-assigned observation");
            }
            evidence.requireSample(sample);
            if (entry instanceof SourceReplayRecord) {
                if (evidence.kind() != TargetSloFinalEvidence.Kind.COMMAND_SYNC) throw new IllegalArgumentException("Command sync cannot complete due eligibility");
            } else if (evidence.kind() != TargetSloFinalEvidence.Kind.ADMISSION_SYNC
                    || !Arrays.equals(evidence.admission().canonicalEnvelope(), ((SourceReplayMutation) entry).mutation().canonicalEnvelope())
                    || !evidence.admissionAt().equals(candidate.syncedFirst().mutation())) {
                throw new IllegalArgumentException("Final provider changed original Admission sync input/stamp");
            }
            bytes = Math.addExact(bytes, input.encodeFrame().length);
        }
        final long retained = original.retainedBytes();
        if (retained < 0 || Math.addExact(Math.addExact(capturedBytes, retained), bytes) > maximumBytes) {
            throw new IllegalArgumentException("complete original Final custody exceeds its qualified task byte envelope");
        }
    }

    private void requireCurrent(LongSupplier clock) {
        final Held original; final boolean closed;
        synchronized (this) {
            if (stopping) throw new IllegalStateException("original Final continuation is stopping");
            original = held; closed = heldClosed;
        }
        worker.requireSloFinalContinuation(entry, outcome, inspectedAt, clock);
        if (original != null && !closed) original.requireCurrent();
        synchronized (this) { if (stopping) throw new IllegalStateException("original Final continuation is stopping"); }
    }

    private void harvest() {
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release;
        synchronized (this) { current = operation; release = closing; }
        if (current != null && current.ioExited()) {
            final var failure = CheckpointInputFuture.ownedFailure(current.receipt());
            final var result = current.completedValue().orElse(null);
            synchronized (this) {
                if (operation == current) {
                    lastFailure = resolved(failure) ? result == null ? null : result.failure() : failure;
                    operation = null;
                }
            }
        }
        if (release != null && release.ioExited()) {
            final var failure = CheckpointInputFuture.ownedFailure(release.receipt());
            synchronized (this) {
                if (closing == release) { lastFailure = resolved(failure) ? null : failure; closing = null; }
            }
        }
    }

    private static boolean resolved(Throwable failure) {
        return failure == null || failure instanceof CheckpointInputFuture.Cleanup cleanup && cleanup.cleanupComplete();
    }

    @Override public Throwable failure() {
        harvest();
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release; final Throwable last;
        synchronized (this) { current = operation; release = closing; last = lastFailure; }
        final var closeFailure = release == null ? null : CheckpointInputFuture.ownedFailure(release.receipt());
        if (!resolved(closeFailure)) return closeFailure;
        final var ioFailure = current == null ? null : CheckpointInputFuture.ownedFailure(current.receipt());
        return resolved(ioFailure) ? last : ioFailure;
    }

    public void requestStop() {
        final OwnedCheckpointIo<Step> current;
        synchronized (this) { stopping = true; current = operation; }
        if (current != null) current.requestStop();
        // Source fencing may hold its monitor. Worker queue withdrawal runs later, outside that monitor.
    }

    /** Keeps the original handles; stop/cancel never proves an in-flight SDK method or native cleanup has exited. */
    public void retryCleanup() {
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release;
        final boolean stopped;
        synchronized (this) { stopped = stopping; }
        if (stopped && worker.withdrawSloFinalContinuationAction(task)) synchronized (this) { actionOutstanding = false; }
        synchronized (this) { current = operation; release = closing; }
        if (current != null) current.retryCleanup();
        if (release != null) release.retryCleanup();
        harvest();
        final Held original;
        synchronized (this) {
            if (!stopping || actionOutstanding || operation != null || closing != null || cleanupOutstanding
                    || held == null || heldClosed) return;
            original = held;
        }
        if (!worker.sloFinalContinuationActionExited(task)) return;
        synchronized (this) {
            if (operation != null || closing != null || cleanupOutstanding || heldClosed) return;
            cleanupOutstanding = true;
        }
        try {
            final var next = OwnedCheckpointIo.submit(ignored -> { release(original); return true; }, () -> {}, executor);
            synchronized (this) { closing = next; }
        } catch (RejectedExecutionException full) { synchronized (this) { lastFailure = full; } }
        finally { synchronized (this) { cleanupOutstanding = false; } }
    }

    public boolean ioExited() {
        harvest();
        synchronized (this) {
            if (actionOutstanding || operation != null || closing != null || cleanupOutstanding || held != null && !heldClosed) return false;
        }
        return worker.sloFinalContinuationActionExited(task);
    }

    public Throwable pendingIoFailure() { return failure(); }

    private void release(Held original) {
        try { original.close(); synchronized (this) { heldClosed = true; } }
        catch (RuntimeException | Error failure) { throw new CleanupFailure(original, failure); }
    }

    private final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held original;
        private boolean complete;
        private CleanupFailure(Held original, Throwable failure) { super("original Final custody cleanup is not confirmed", failure); this.original = original; }
        @Override public synchronized boolean cleanupComplete() { return complete; }
        @Override public synchronized CompletableFuture<Void> retryCleanup() {
            if (complete) return CompletableFuture.completedFuture(null);
            try { original.close(); synchronized (TargetSloFinalContinuation.this) { heldClosed = true; } complete = true; return CompletableFuture.completedFuture(null); }
            catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
        }
    }
}
