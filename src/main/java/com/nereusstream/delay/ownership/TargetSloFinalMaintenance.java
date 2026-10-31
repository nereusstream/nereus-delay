package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardLogFrame;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloFinalEvidence;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.TargetSloTimeoutCandidates;
import com.nereusstream.delay.runtime.TargetSloWriterEpochStore;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One retained timeout or independently confirmed gap flight, separate from a business Source continuation. */
public final class TargetSloFinalMaintenance {
    public static final long MAXIMUM_FRAME_BYTES = (long) TargetSloObservationEventRecord.MAX_INPUT_BYTES
            + ShardLogFrame.HEADER_LENGTH
            + ShardLogFrame.TRAILER_LENGTH;

    public record Limits(int maximumRows, long maximumCanonicalBytes) {
        public Limits {
            if (maximumRows < 1
                    || maximumRows == Integer.MAX_VALUE
                    || maximumCanonicalBytes < 1
                    || maximumCanonicalBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("SLO Final discovery requires finite row and retained-byte bounds");
            }
        }
    }

    /** The sweep binds a physical Source and key order; each candidate has its own actual Native cut. */
    public record Cursor(TargetQuotaScope scope, byte[] lineage, SourcePosition sweepSource, byte[] afterSampleKey) {
        public Cursor {
            Objects.requireNonNull(scope);
            TargetSourcePosition.requireBounded(sweepSource);
            Bytes.requireLength(lineage, 16, "SLO Final discovery lineage");
            lineage = Bytes.copy(lineage);
            afterSampleKey = Bytes.copy(Objects.requireNonNull(afterSampleKey));
            if (scope.target() != null
                    || !scope.shard().equals(sweepSource.shardId())
                    || afterSampleKey.length != 34
                    || afterSampleKey[0] != TargetKeyCodec.SLO_SAMPLE_TAG
                    || afterSampleKey[1] != TargetKeyCodec.KEY_FORMAT) {
                throw new IllegalArgumentException("SLO Final cursor changed its Shard or primary sample key");
            }
        }

        @Override
        public byte[] lineage() {
            return Bytes.copy(lineage);
        }

        @Override
        public byte[] afterSampleKey() {
            return Bytes.copy(afterSampleKey);
        }

        public long retainedBytes() {
            return Math.addExact(
                    32L,
                    Math.addExact(
                            scope.canonicalBytes().length,
                            Math.addExact(
                                    lineage.length,
                                    Math.addExact(sweepSource.canonicalBytes().length, afterSampleKey.length))));
        }
    }

    /** Native classification is a candidate only; it grants neither UTC nor a recovery-gap confirmation. */
    public record Candidate(
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            TargetQuotaMutation inspectedAt,
            TargetSloTimeoutCandidates.Result classification) {
        public Candidate {
            Objects.requireNonNull(generation);
            Objects.requireNonNull(sample);
            Objects.requireNonNull(inspectedAt).requireSourceApplied();
            Objects.requireNonNull(classification);
            if (classification == TargetSloTimeoutCandidates.Result.CURRENT_FINAL
                    || sample.observation().finalObservation() != null
                    || generation.generation() != sample.generation()
                    || !generation.policy().scope().equals(sample.scope())
                    || !Arrays.equals(generation.policy().lineage(), sample.lineage())
                    || !Arrays.equals(generation.policy().digest(), sample.policyDigest())) {
                throw new IllegalArgumentException("SLO Final candidate changed its open original generation/sample");
            }
            sample.mutation().requireAtOrBefore(inspectedAt);
            generation.mutation().requireAtOrBefore(inspectedAt);
            generation.policy().requireSource(inspectedAt.source());
        }

        public long retainedBytes() {
            return Math.addExact(
                    64L,
                    Math.addExact(
                            generation.canonicalBytes().length,
                            Math.addExact(sample.canonicalBytes().length, inspectedAt.canonicalBytes().length)));
        }
    }

    /** At most one complete candidate is retained; sweep completion is not whole-Scope coverage. */
    public record Page(Candidate candidate, Cursor nextCursor, boolean sweepComplete, long canonicalBytes) {
        public Page {
            if (sweepComplete != (nextCursor == null)
                    || canonicalBytes < minimumCanonicalBytes(candidate, nextCursor)) {
                throw new IllegalArgumentException("SLO Final page changed its finite cursor/candidate accounting");
            }
            if (candidate != null
                    && nextCursor != null
                    && (!candidate.sample().scope().equals(nextCursor.scope())
                            || !Arrays.equals(candidate.sample().lineage(), nextCursor.lineage())
                            || !candidate.inspectedAt().source().sameSourceIdentity(nextCursor.sweepSource())
                            || nextCursor
                                            .sweepSource()
                                            .compareTo(candidate.inspectedAt().source())
                                    > 0)) {
                throw new IllegalArgumentException("SLO Final page crossed its actual physical Source");
            }
        }

        public static long minimumCanonicalBytes(Candidate candidate, Cursor cursor) {
            return Math.addExact(
                    32L,
                    Math.addExact(
                            candidate == null ? 0 : candidate.retainedBytes(),
                            cursor == null ? 0 : cursor.retainedBytes()));
        }
    }

    @FunctionalInterface
    public interface Provider {
        /** Acquire within the supplied reservation; partial failure must retain its original Cleanup handle. */
        Held load(Candidate candidate, long maximumRetainedBytes, Runnable guard);
    }

    public interface Held extends AutoCloseable {
        /** Empty defers this sample until a later sweep; it records no positive Native decision. */
        Optional<TargetSloFinalEvidence> evidence();

        TargetSloObservationEventMutationFactory signer();
        /** Actual guarded Native62 token; a declaration or reference DTO cannot mint the new frame. */
        TargetSloWriterEpochStore.Applied writerEpoch();

        long retryUntilEpochMs();
        /** All original evidence, signer and writer-token material; the exact frame is separately reserved. */
        long retainedBytes();

        void requireCurrent();
        /** Read-loan release only; never drops UNKNOWN, producer ownership or durable retention commitments. */
        @Override
        void close();
    }

    private record Step(Throwable failure) {}

    private final TargetWorkerShardRuntime worker;
    private final OwnedSourceSystemWriter writer;
    private final ThreadPoolExecutor executor;
    private final Supplier<BoundedReadBudget> reads;
    private final LongSupplier clock;
    private final Limits limits;
    private final Provider provider;
    private final long maximumBytes;
    private final WorkClassTask task;
    private Cursor cursor, nextCursor;
    private Candidate candidate;
    private long capturedBytes;
    private Held held;
    private long heldBytes;
    private boolean heldClosed, evidenceSelected, appendAttempted, flightSettled;
    private TargetSloFinalEvidence evidence;
    private TargetSloObservationEventMutationFactory.Prepared frame;
    private volatile SourcePosition persistedThrough;
    private OwnedCheckpointIo<Step> operation;
    private boolean actionOutstanding, cleanupOutstanding, stopNew, stopping;
    private Throwable lastFailure;

    public TargetSloFinalMaintenance(
            TargetWorkerShardRuntime worker,
            OwnedSourceSystemWriter writer,
            ThreadPoolExecutor executor,
            Supplier<BoundedReadBudget> reads,
            LongSupplier clock,
            Limits limits,
            Provider provider,
            long maximumBytes) {
        this.worker = Objects.requireNonNull(worker);
        this.writer = Objects.requireNonNull(writer);
        this.executor = Objects.requireNonNull(executor);
        this.reads = Objects.requireNonNull(reads);
        this.clock = Objects.requireNonNull(clock);
        this.limits = Objects.requireNonNull(limits);
        this.provider = Objects.requireNonNull(provider);
        this.maximumBytes = maximumBytes;
        if (writer.sourceWriterWorker() != worker
                || maximumBytes < 1
                || maximumBytes == Long.MAX_VALUE
                || Math.addExact(Math.multiplyExact(2L, limits.maximumCanonicalBytes()), MAXIMUM_FRAME_BYTES)
                        >= maximumBytes
                || executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || executor.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException(
                    "SLO Final maintenance requires its original finite Worker/I/O/frame envelope");
        }
        task = new WorkClassTask(
                WorkClass.OUTCOME_AND_CONTROL, "target-slo-final-maintenance/" + worker.shardId(), maximumBytes);
    }

    public Optional<WorkClassTask> runTurn() {
        retryOwnedCleanup();
        harvest();
        synchronized (this) {
            if (stopping) {
                return hasRetainedState() ? Optional.of(task) : Optional.empty();
            }
            if (actionOutstanding || operation != null || cleanupOutstanding) {
                return Optional.of(task);
            }
            if (stopNew && candidate == null) {
                return Optional.empty();
            }
        }
        if (!worker.sloFinalMaintenanceActionExited(task)) {
            return Optional.of(task);
        }
        synchronized (this) {
            if (stopping || actionOutstanding || operation != null || cleanupOutstanding) {
                return Optional.of(task);
            }
            actionOutstanding = true;
        }
        try {
            worker.submitSloFinalMaintenanceAction(task, this::launch);
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                actionOutstanding = false;
                lastFailure = failure;
            }
            throw failure;
        }
        return Optional.of(task);
    }

    private void launch() {
        try {
            synchronized (this) {
                if (stopping) {
                    return;
                }
            }
            final var next = OwnedCheckpointIo.submit(this::step, this::requireCurrent, executor);
            synchronized (this) {
                operation = next;
                if (stopping) {
                    next.requestStop();
                }
            }
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                lastFailure = failure;
            }
        } finally {
            synchronized (this) {
                actionOutstanding = false;
            }
        }
    }

    private Step step(Runnable guard) {
        guard.run();
        Candidate selected;
        boolean settled;
        synchronized (this) {
            selected = candidate;
            settled = flightSettled;
        }
        if (settled) {
            finishFlight();
            return new Step(null);
        }
        if (selected == null) {
            final Cursor after;
            synchronized (this) {
                if (stopNew) {
                    return new Step(null);
                }
                after = cursor;
            }
            final var page = Objects.requireNonNull(worker.captureSloFinalPage(
                    Objects.requireNonNull(reads.get(), "original SLO discovery read budget"), after, limits, clock));
            if (page.canonicalBytes() > limits.maximumCanonicalBytes()) {
                throw new IllegalArgumentException("SLO Final page exceeds its original scan envelope");
            }
            synchronized (this) {
                if (page.candidate() == null) {
                    cursor = page.nextCursor();
                    return new Step(null);
                }
                candidate = page.candidate();
                selected = candidate;
                nextCursor = page.nextCursor();
                capturedBytes = Math.addExact(page.canonicalBytes(), after == null ? 0 : after.retainedBytes());
            }
            guard.run();
        }
        final TargetSloObservationEventMutationFactory.Prepared retainedFrame;
        synchronized (this) {
            retainedFrame = frame;
        }
        if (retainedFrame != null) {
            // Exact first-result reconciliation needs no renewed signing time or open writer epoch.
            final var first = worker.sloFinalMutationResult(retainedFrame.mutation(), clock);
            if (first.isPresent()) {
                rememberPersisted(SourcePositionCodec.decode(first.orElseThrow().appliedSourcePosition()));
                synchronized (this) {
                    flightSettled = true;
                }
                guard.run();
                finishFlight();
                return new Step(null);
            }
        }
        Held original;
        synchronized (this) {
            original = held;
        }
        if (original == null) {
            final long available;
            synchronized (this) {
                available = Math.subtractExact(maximumBytes, Math.addExact(capturedBytes, MAXIMUM_FRAME_BYTES));
            }
            if (available <= 0) {
                throw new IllegalArgumentException("SLO Final candidate leaves no original provider reservation");
            }
            original = Objects.requireNonNull(
                    provider.load(selected, available, guard), "original SLO deadline/gap inputs");
            synchronized (this) {
                held = original;
            }
            final long retained = original.retainedBytes();
            synchronized (this) {
                heldBytes = retained;
            }
            if (retained < 0 || retained > available) {
                throw new IllegalArgumentException("original SLO Final inputs exceed their acquisition reservation");
            }
        }
        guard.run();
        original.requireCurrent();
        if (!selectEvidence(original, selected)) {
            synchronized (this) {
                flightSettled = true;
            }
            finishFlight();
            return new Step(null);
        }
        final var exact = prepareFrame(original, selected, guard);
        final Optional<com.nereusstream.delay.runtime.SystemMutationResult> first =
                retainedFrame == null ? worker.sloFinalMutationResult(exact.mutation(), clock) : Optional.empty();
        if (first.isPresent()) {
            rememberPersisted(SourcePositionCodec.decode(first.orElseThrow().appliedSourcePosition()));
            synchronized (this) {
                flightSettled = true;
            }
        }
        guard.run();
        if (first.isPresent()) {
            finishFlight();
            return new Step(null);
        }
        final boolean attempted;
        synchronized (this) {
            attempted = appendAttempted;
        }
        if (!attempted && !worker.sloFinalCandidateCurrent(selected, exact, clock)) {
            synchronized (this) {
                flightSettled = true;
            }
            finishFlight();
            return new Step(null);
        }
        TargetSloObservationEventMutationFactory.requirePreparedWriter(exact, selected.generation());
        worker.requireSloFinalWriter(exact.mutation(), clock);
        guard.run();
        original.requireCurrent();
        synchronized (this) {
            appendAttempted = true;
        }
        final ShardLogMutationAppender.AppendOutcome appended;
        try {
            appended = Objects.requireNonNull(writer.append(exact.mutation()), "original SLO Final append");
        } catch (RuntimeException uncertain) {
            guard.run();
            return new Step(uncertain);
        }
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            worker.requireSloFinalAppend(exact.mutation(), appended, selected.inspectedAt(), clock);
            rememberPersisted(appended.sourcePosition());
            synchronized (this) {
                flightSettled = true;
            }
        }
        guard.run();
        original.requireCurrent();
        synchronized (this) {
            settled = flightSettled;
        }
        if (settled) {
            finishFlight();
        }
        return new Step(null);
    }

    private boolean selectEvidence(Held original, Candidate selected) {
        synchronized (this) {
            if (evidenceSelected) {
                return evidence != null;
            }
        }
        final var chosen = Objects.requireNonNull(original.evidence(), "original SLO Final evidence decision");
        if (chosen.isPresent()) {
            final var value = chosen.orElseThrow();
            if (value.kind() != TargetSloFinalEvidence.Kind.TIMEOUT
                            && value.kind() != TargetSloFinalEvidence.Kind.RECOVERY_GAP
                    || !value.inspectedAt().equals(selected.inspectedAt())
                    || value.kind() == TargetSloFinalEvidence.Kind.TIMEOUT
                            && selected.classification()
                                    != TargetSloTimeoutCandidates.Result.NO_KNOWN_ADMISSION_AT_CUT) {
                throw new IllegalArgumentException(
                        "SLO maintenance cannot promote a current hint to original timeout/gap authority");
            }
            value.requireSample(selected.sample());
            synchronized (this) {
                evidence = value;
            }
        }
        synchronized (this) {
            evidenceSelected = true;
            return evidence != null;
        }
    }

    private TargetSloObservationEventMutationFactory.Prepared prepareFrame(
            Held original, Candidate selected, Runnable guard) {
        final TargetSloObservationEventMutationFactory.Prepared existing;
        synchronized (this) {
            existing = frame;
        }
        if (existing != null) {
            validateFrame(original, selected, existing);
            return existing;
        }
        original.requireCurrent();
        final var signer = Objects.requireNonNull(original.signer(), "original SERVICE Final signer");
        final var epoch = Objects.requireNonNull(original.writerEpoch(), "actual Native62 writer token");
        final TargetSloFinalEvidence chosen;
        synchronized (this) {
            chosen = Objects.requireNonNull(evidence);
        }
        final var prepared = signer.prepareObserved(
                selected.generation(), selected.sample(), chosen, original.retryUntilEpochMs(), epoch);
        synchronized (this) {
            frame = prepared;
        }
        guard.run();
        validateFrame(original, selected, prepared);
        return prepared;
    }

    private void validateFrame(
            Held original, Candidate selected, TargetSloObservationEventMutationFactory.Prepared prepared) {
        final TargetSloFinalEvidence chosen;
        synchronized (this) {
            chosen = Objects.requireNonNull(evidence);
        }
        final var body =
                TargetSloObservationEventBody.decode(prepared.mutation().canonicalBody());
        if (!body.isSourceAssigned()
                || body.isCustody()
                || body.evidence() == null
                || !Arrays.equals(body.evidence().canonicalBytes(), chosen.canonicalBytes())
                || !body.evidence().inspectedAt().equals(selected.inspectedAt())
                || !body.scope().equals(selected.sample().scope())
                || !Arrays.equals(body.lineage(), selected.sample().lineage())
                || body.generation() != selected.sample().generation()
                || !Arrays.equals(
                        body.sampleId(), selected.sample().observation().sampleId())
                || !Arrays.equals(body.bindingDigest(), selected.sample().bindingDigest())) {
            throw new IllegalArgumentException("SLO Final signer changed its original evidence/read cut");
        }
        TargetSloObservationEventMutationFactory.requirePreparedWriter(prepared, selected.generation());
        final long bytes = prepared.mutation().encodeFrame().length;
        if (bytes > MAXIMUM_FRAME_BYTES
                || original.retainedBytes() != heldBytes
                || Math.addExact(Math.addExact(capturedBytes, heldBytes), bytes) > maximumBytes) {
            throw new IllegalArgumentException(
                    "complete SLO candidate/evidence/frame exceeds its original task envelope");
        }
    }

    /** Lock-free: the actual Native Source view reads this barrier while holding its own Store monitor. */
    public SourcePosition persistedThrough() {
        return persistedThrough;
    }

    private synchronized void rememberPersisted(SourcePosition position) {
        TargetSourcePosition.requireBounded(Objects.requireNonNull(position));
        final var at = Objects.requireNonNull(candidate).inspectedAt().source();
        if (!at.sameSourceIdentity(position) || !at.shardId().equals(position.shardId())) {
            throw new IllegalStateException("SLO Final settlement changed its original physical Source");
        }
        if (persistedThrough != null
                && position.compareTo(persistedThrough) == 0
                && !Arrays.equals(position.canonicalBytes(), persistedThrough.canonicalBytes())) {
            throw new IllegalStateException(
                    "SLO Final settlement changed complete metadata at the same Source position");
        }
        if (persistedThrough == null || position.compareTo(persistedThrough) > 0) {
            persistedThrough = position;
        }
    }

    private void finishFlight() {
        final Held original;
        final boolean closed;
        synchronized (this) {
            original = held;
            closed = heldClosed;
        }
        if (original != null && !closed) {
            release(original);
        }
        synchronized (this) {
            cursor = nextCursor;
            nextCursor = null;
            candidate = null;
            capturedBytes = 0;
            held = null;
            heldClosed = false;
            heldBytes = 0;
            evidence = null;
            evidenceSelected = false;
            frame = null;
            appendAttempted = false;
            flightSettled = false;
        }
    }

    private void requireCurrent() {
        final Candidate selected;
        synchronized (this) {
            if (stopping) {
                throw new IllegalStateException("SLO Final maintenance is stopping");
            }
            selected = candidate;
        }
        worker.requireSloFinalMaintenanceContext(selected == null ? null : selected.inspectedAt(), clock);
        synchronized (this) {
            if (stopping) {
                throw new IllegalStateException("SLO Final maintenance is stopping");
            }
        }
    }

    public synchronized void stopNew() {
        stopNew = true;
    }

    public void requestStop() {
        final OwnedCheckpointIo<Step> current;
        synchronized (this) {
            stopping = true;
            stopNew = true;
            current = operation;
        }
        if (current != null) {
            current.requestStop();
        }
    }

    private void retryOwnedCleanup() {
        final OwnedCheckpointIo<Step> current;
        synchronized (this) {
            current = operation;
        }
        if (current != null) {
            current.retryCleanup();
        }
    }

    public void retryCleanup() {
        final boolean stopped;
        synchronized (this) {
            stopped = stopping;
        }
        if (stopped && worker.withdrawSloFinalMaintenanceAction(task)) {
            synchronized (this) {
                actionOutstanding = false;
            }
        }
        retryOwnedCleanup();
        harvest();
        final Held original;
        synchronized (this) {
            if (!stopping
                    || actionOutstanding
                    || operation != null
                    || cleanupOutstanding
                    || held == null
                    || heldClosed) {
                return;
            }
            original = held;
        }
        if (!worker.sloFinalMaintenanceActionExited(task)) {
            return;
        }
        synchronized (this) {
            if (operation != null || cleanupOutstanding || heldClosed) {
                return;
            }
            cleanupOutstanding = true;
        }
        try {
            final var next = OwnedCheckpointIo.submit(
                    ignored -> {
                        release(original);
                        return new Step(null);
                    },
                    () -> {},
                    executor);
            synchronized (this) {
                operation = next;
            }
        } catch (RejectedExecutionException full) {
            synchronized (this) {
                lastFailure = full;
            }
        } finally {
            synchronized (this) {
                cleanupOutstanding = false;
            }
        }
    }

    private void harvest() {
        final OwnedCheckpointIo<Step> current;
        synchronized (this) {
            current = operation;
        }
        if (current == null || !current.ioExited()) {
            return;
        }
        final var failure = CheckpointInputFuture.ownedFailure(current.receipt());
        final var value = current.completedValue().orElse(null);
        synchronized (this) {
            if (operation == current) {
                lastFailure = resolved(failure) ? value == null ? null : value.failure() : failure;
                operation = null;
            }
        }
    }

    private static boolean resolved(Throwable failure) {
        return failure == null || failure instanceof CheckpointInputFuture.Cleanup cleanup && cleanup.cleanupComplete();
    }

    private boolean hasRetainedState() {
        return actionOutstanding || operation != null || cleanupOutstanding || held != null && !heldClosed;
    }

    public boolean hasPending() {
        harvest();
        synchronized (this) {
            return hasRetainedState() || !stopping && candidate != null;
        }
    }

    public boolean ioExited() {
        harvest();
        synchronized (this) {
            if (hasRetainedState() || !stopping && candidate != null) {
                return false;
            }
        }
        return worker.sloFinalMaintenanceActionExited(task);
    }

    public Throwable pendingIoFailure() {
        harvest();
        final OwnedCheckpointIo<Step> current;
        final Throwable last;
        synchronized (this) {
            current = operation;
            last = lastFailure;
        }
        final var failure = current == null ? null : CheckpointInputFuture.ownedFailure(current.receipt());
        return resolved(failure) ? last : failure;
    }

    private void release(Held original) {
        try {
            original.close();
            synchronized (this) {
                heldClosed = true;
            }
        } catch (RuntimeException | Error failure) {
            throw new CleanupFailure(original, failure);
        }
    }

    private final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held original;
        private boolean complete;

        private CleanupFailure(Held original, Throwable cause) {
            super("original SLO Final input cleanup is unconfirmed", cause);
            this.original = original;
        }

        public synchronized boolean cleanupComplete() {
            return complete;
        }

        public synchronized CompletableFuture<Void> retryCleanup() {
            if (complete) {
                return CompletableFuture.completedFuture(null);
            }
            try {
                original.close();
                synchronized (TargetSloFinalMaintenance.this) {
                    heldClosed = true;
                }
                complete = true;
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException | Error failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }
    }
}
