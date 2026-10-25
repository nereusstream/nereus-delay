package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetSloExportPageReader;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.PersistentSloObservationCollector;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One bounded Target delivery flight, retaining exact collector custody and Source bytes across uncertainty. */
public final class TargetSloCustodyExportMaintenance {
    @FunctionalInterface public interface Provider {
        Held load(TargetSloObservationExport exported, Runnable guard);
    }
    public interface Held extends AutoCloseable {
        PersistentSloObservationCollector collector();
        PersistentSloObservationCollector.TargetLimits collectorLimits();
        PersistentSloObservationCollector.OriginalAuthority authority();
        PersistentSloObservationCollector.ReceiptSigner collectorSigner();
        TargetSloObservationEventMutationFactory sourceSigner();
        long retryUntilEpochMs();
        /** Complete original export, generated receipt/frame, collector I/O and credential/retention material. */
        long retainedBytes();
        /** Held local qualification only; blocking work belongs to this flight's original owned I/O. */
        void requireCurrent();
        @Override void close();
    }
    private record Step(Throwable failure) {}

    private final TargetWorkerShardRuntime worker;
    private final OwnedSourceSystemWriter writer;
    private final ThreadPoolExecutor executor;
    private final Supplier<BoundedReadBudget> reads;
    private final LongSupplier clock;
    private final TargetSloExportPageReader.Limits pageLimits;
    private final Provider provider;
    private final long maximumBytes;
    private final WorkClassTask task;
    private TargetSloExportPageReader.Cursor cursor;
    private TargetSloExportPageReader.Page page;
    private TargetQuotaMutation cut;
    private int row;
    private Held held;
    private TargetSloObservationExport fixedExport;
    private TargetSloCustodyReceipt receipt;
    private TargetSloObservationEventMutationFactory.Prepared frame;
    private boolean appendPersisted, rowSettled, heldClosed;
    private boolean actionOutstanding, cleanupOutstanding, stopNew, stopping;
    private OwnedCheckpointIo<Step> operation;
    private OwnedCheckpointIo<Boolean> closing;
    private Throwable lastFailure;

    public TargetSloCustodyExportMaintenance(TargetWorkerShardRuntime worker, OwnedSourceSystemWriter writer,
            ThreadPoolExecutor executor, Supplier<BoundedReadBudget> reads, LongSupplier clock,
            TargetSloExportPageReader.Limits pageLimits, Provider provider, long maximumBytes) {
        this.worker = Objects.requireNonNull(worker); this.writer = Objects.requireNonNull(writer);
        this.executor = Objects.requireNonNull(executor); this.reads = Objects.requireNonNull(reads);
        this.clock = Objects.requireNonNull(clock); this.pageLimits = Objects.requireNonNull(pageLimits);
        this.provider = Objects.requireNonNull(provider);
        if (writer.sourceWriterWorker() != worker || maximumBytes <= 0 || maximumBytes == Long.MAX_VALUE
                || pageLimits.maximumCanonicalBytes() >= maximumBytes || executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || executor.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("Target custody export needs its original finite Worker/I/O/task envelope");
        }
        this.maximumBytes = maximumBytes;
        task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, "target-slo-custody-export/" + worker.shardId(), maximumBytes);
    }

    /** Schedules or polls one nonblocking step; the existing shared WorkClass dispatcher executes the action. */
    public Optional<WorkClassTask> runTurn() {
        retryOwnedCleanup(); harvest();
        synchronized (this) {
            if (stopping) return hasRetainedState() ? Optional.of(task) : Optional.empty();
            if (actionOutstanding || operation != null || closing != null || cleanupOutstanding) return Optional.of(task);
            if (stopNew && page == null && held == null) return Optional.empty();
        }
        if (!worker.sloExportActionExited(task)) return Optional.of(task);
        synchronized (this) {
            if (stopping || actionOutstanding || operation != null || closing != null || cleanupOutstanding) return Optional.of(task);
            actionOutstanding = true;
        }
        try { worker.submitSloExportAction(task, this::launch); }
        catch (RuntimeException | Error failure) {
            synchronized (this) { actionOutstanding = false; lastFailure = failure; }
            throw failure;
        }
        return Optional.of(task);
    }

    private void launch() {
        try {
            synchronized (this) { if (stopping) return; }
            final var next = OwnedCheckpointIo.submit(this::step, this::requireCurrent, executor);
            synchronized (this) { operation = next; if (stopping) next.requestStop(); }
        } catch (RuntimeException | Error failure) { synchronized (this) { lastFailure = failure; } }
        finally { synchronized (this) { actionOutstanding = false; } }
    }

    private Step step(Runnable guard) {
        guard.run();
        TargetSloExportPageReader.Page originalPage;
        synchronized (this) { originalPage = page; if (originalPage == null && stopNew) return new Step(null); }
        if (originalPage == null) {
            final TargetSloExportPageReader.Cursor after; synchronized (this) { after = cursor; }
            final var captured = worker.captureSloExportPage(Objects.requireNonNull(reads.get(), "original export read budget"), after, pageLimits, clock);
            synchronized (this) { page = captured; cut = captured.cut(); row = 0; originalPage = captured; }
            if (captured.canonicalBytes() > maximumBytes) throw new IllegalArgumentException("Target export capture exceeds its task envelope");
            guard.run();
            if (captured.exports().isEmpty()) {
                synchronized (this) { cursor = captured.nextCursor(); page = null; }
                return new Step(null);
            }
        }
        final TargetSloObservationExport exported; final boolean settled;
        synchronized (this) { exported = originalPage.exports().get(row); settled = rowSettled; }
        if (settled) { finishRow(); return new Step(null); }
        if (worker.hasSloCustody(exported, clock)) {
            guard.run(); synchronized (this) { rowSettled = true; }
            finishRow(); return new Step(null);
        }

        Held original;
        synchronized (this) { original = held; }
        if (original == null) {
            original = Objects.requireNonNull(provider.load(exported, guard), "original Target collector contract/transport custody");
            synchronized (this) { held = original; }
        }
        guard.run(); original.requireCurrent();
        final var authority = Objects.requireNonNull(original.authority(), "original collector authority");
        TargetSloCustodyReceipt exactReceipt;
        synchronized (this) { exactReceipt = receipt; }
        if (exactReceipt == null) {
            final var merged = Objects.requireNonNull(original.collector(), "actual Target collector").mergeTarget(exported,
                    Objects.requireNonNull(original.collectorLimits()), authority,
                    Objects.requireNonNull(original.collectorSigner(), "original locally prepared collector signer"));
            synchronized (this) { receipt = merged; exactReceipt = merged; }
            guard.run(); original.requireCurrent();
        }
        TargetSloObservationExport originalExport;
        synchronized (this) { originalExport = fixedExport; }
        if (originalExport == null) {
            final var stored = Objects.requireNonNull(original.collector().targetExport(exported.sample().digest(),
                    original.collectorLimits(), authority), "collector lost its first complete imported version");
            synchronized (this) { fixedExport = stored; originalExport = stored; }
        }
        requireReceipt(original, originalExport, exported, exactReceipt);
        guard.run(); original.requireCurrent();

        TargetSloObservationEventMutationFactory.Prepared exact;
        synchronized (this) { exact = frame; }
        if (exact == null) {
            final var prepared = Objects.requireNonNull(original.sourceSigner(), "original SERVICE Source signer")
                    .prepareCustody(originalExport.generation(), originalExport.sample(), exactReceipt, original.retryUntilEpochMs());
            synchronized (this) { frame = prepared; exact = prepared; }
        }
        requireFrame(original, originalPage, exported, originalExport, exactReceipt, exact);
        guard.run(); original.requireCurrent();
        final var first = worker.sloCustodyMutationResult(exact.mutation(), clock);
        guard.run();
        if (first.isPresent()) {
            if (!worker.hasSloCustody(exported, clock)) {
                throw new IllegalStateException("custody Source first has no exact authenticated Native custody record");
            }
            synchronized (this) { rowSettled = true; }
            finishRow(); return new Step(null);
        }
        final boolean persisted; synchronized (this) { persisted = appendPersisted; }
        if (persisted) return new Step(null); // Source continues independently; this does not occupy its Control slot.
        worker.requireSloCustodyWriter(exact.mutation(), clock); guard.run(); original.requireCurrent();
        final ShardLogMutationAppender.AppendOutcome appended;
        try { appended = Objects.requireNonNull(writer.append(exact.mutation()), "original custody Source append"); }
        catch (RuntimeException unknown) { guard.run(); return new Step(unknown); }
        guard.run(); original.requireCurrent();
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            worker.requireSloCustodyAppend(exact.mutation(), appended, originalPage.cut(), clock); guard.run(); original.requireCurrent();
            synchronized (this) { appendPersisted = true; }
        }
        return new Step(null); // Definite non-persistence/UNKNOWN retries these exact receipt/frame bytes.
    }

    private void requireReceipt(Held original, TargetSloObservationExport stored, TargetSloObservationExport requested,
            TargetSloCustodyReceipt exact) {
        if (!Arrays.equals(stored.sample().canonicalBytes(), requested.sample().canonicalBytes())
                || !Arrays.equals(stored.digest(), exact.exportDigest())) {
            throw new IllegalStateException("Target collector receipt changed its first fixed export or exact sample version");
        }
        exact.requireSample(stored.generation(), stored.sample());
        final var authority = original.authority(); authority.requireCurrent(); authority.requireOriginal(stored); authority.requireCurrent();
        final var key = authority.receiptKey(stored, exact);
        if (key == null || !exact.verifySignature(key)) throw new IllegalStateException("Target custody receipt lacks its original collector contract signature");
        original.requireCurrent();
    }

    private void requireFrame(Held original, TargetSloExportPageReader.Page originalPage, TargetSloObservationExport requested,
            TargetSloObservationExport stored, TargetSloCustodyReceipt exact, TargetSloObservationEventMutationFactory.Prepared prepared) {
        final var image = prepared.mutation(); final var body = TargetSloObservationEventBody.decode(image.canonicalBody());
        if (!body.isCustody() || !Arrays.equals(body.exportedSample().canonicalBytes(), requested.sample().canonicalBytes())
                || !Arrays.equals(body.custody().canonicalBytes(), exact.canonicalBytes())
                || !Arrays.equals(image.authorIdentity(), stored.generation().policy().service().canonicalBytes())
                || image.signingKeyVersion() != stored.generation().policy().keyVersion()) {
            throw new IllegalStateException("Target custody Source frame changed original receipt/binding/SERVICE author");
        }
        long known = Math.addExact(stored.canonicalBytes().length, exact.canonicalBytes().length);
        known = Math.addExact(known, image.encodeFrame().length);
        if (!Arrays.equals(stored.canonicalBytes(), requested.canonicalBytes())) known = Math.addExact(known, requested.canonicalBytes().length);
        final long retained = original.retainedBytes();
        final long otherRows = Math.subtractExact(originalPage.canonicalBytes(), requested.canonicalBytes().length);
        if (retained < known || Math.addExact(otherRows, retained) > maximumBytes) {
            throw new IllegalArgumentException("complete Target export/collector receipt/Source frame exceeds its original task custody envelope");
        }
    }

    private void finishRow() {
        final Held original; final boolean closed;
        synchronized (this) { original = held; closed = heldClosed; }
        if (original != null && !closed) release(original);
        synchronized (this) {
            held = null; heldClosed = false; receipt = null; fixedExport = null; frame = null;
            appendPersisted = false; rowSettled = false; row++;
            if (row == page.exports().size()) { cursor = page.nextCursor(); page = null; row = 0; }
        }
    }

    private void requireCurrent() {
        final TargetQuotaMutation originalCut; final Held original; final boolean closed;
        synchronized (this) {
            if (stopping) throw new IllegalStateException("Target custody export is stopping");
            originalCut = cut; original = held; closed = heldClosed;
        }
        worker.requireSloExportContext(originalCut, clock);
        if (original != null && !closed) original.requireCurrent();
        synchronized (this) { if (stopping) throw new IllegalStateException("Target custody export is stopping"); }
    }

    /** Stops new pages, while a previously captured page/receipt continues on its original resources. */
    public synchronized void stopNew() { stopNew = true; }

    /** Signal only: this path may run while Source.fence holds its monitor, so it never calls back into Worker. */
    public void requestStop() {
        final OwnedCheckpointIo<Step> current;
        synchronized (this) { stopping = true; stopNew = true; current = operation; }
        if (current != null) current.requestStop();
    }

    public void retryCleanup() {
        final boolean stopped; synchronized (this) { stopped = stopping; }
        if (stopped && worker.withdrawSloExportAction(task)) synchronized (this) { actionOutstanding = false; }
        retryOwnedCleanup(); harvest();
        final Held original;
        synchronized (this) {
            if (!stopping || actionOutstanding || operation != null || closing != null || cleanupOutstanding || held == null || heldClosed) return;
            original = held;
        }
        if (!worker.sloExportActionExited(task)) return;
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

    private void retryOwnedCleanup() {
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release;
        synchronized (this) { current = operation; release = closing; }
        if (current != null) current.retryCleanup();
        if (release != null) release.retryCleanup();
    }

    private void harvest() {
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release;
        synchronized (this) { current = operation; release = closing; }
        if (current != null && current.ioExited()) {
            final var failure = CheckpointInputFuture.ownedFailure(current.receipt());
            final var value = current.completedValue().orElse(null);
            synchronized (this) {
                if (operation == current) { lastFailure = resolved(failure) ? value == null ? null : value.failure() : failure; operation = null; }
            }
        }
        if (release != null && release.ioExited()) {
            final var failure = CheckpointInputFuture.ownedFailure(release.receipt());
            synchronized (this) { if (closing == release) { lastFailure = resolved(failure) ? null : failure; closing = null; } }
        }
    }

    private static boolean resolved(Throwable failure) {
        return failure == null || failure instanceof CheckpointInputFuture.Cleanup cleanup && cleanup.cleanupComplete();
    }

    private boolean hasRetainedState() {
        return actionOutstanding || cleanupOutstanding || operation != null || closing != null || held != null && !heldClosed;
    }

    public boolean hasPending() {
        harvest(); synchronized (this) { return hasRetainedState() || page != null; }
    }

    public boolean ioExited() {
        harvest();
        synchronized (this) { if (hasRetainedState() || !stopping && page != null) return false; }
        return worker.sloExportActionExited(task);
    }

    public Throwable pendingIoFailure() {
        harvest();
        final OwnedCheckpointIo<Step> current; final OwnedCheckpointIo<Boolean> release; final Throwable last;
        synchronized (this) { current = operation; release = closing; last = lastFailure; }
        final var closeFailure = release == null ? null : CheckpointInputFuture.ownedFailure(release.receipt());
        if (!resolved(closeFailure)) return closeFailure;
        final var ioFailure = current == null ? null : CheckpointInputFuture.ownedFailure(current.receipt());
        return resolved(ioFailure) ? last : ioFailure;
    }

    private void release(Held original) {
        try { original.close(); synchronized (this) { heldClosed = true; } }
        catch (RuntimeException | Error failure) { throw new CleanupFailure(original, failure); }
    }

    private final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held original;
        private boolean complete;
        private CleanupFailure(Held original, Throwable failure) { super("original Target collector custody cleanup remains", failure); this.original = original; }
        @Override public synchronized boolean cleanupComplete() { return complete; }
        @Override public synchronized CompletableFuture<Void> retryCleanup() {
            if (complete) return CompletableFuture.completedFuture(null);
            try { original.close(); synchronized (TargetSloCustodyExportMaintenance.this) { heldClosed = true; } complete = true; return CompletableFuture.completedFuture(null); }
            catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
        }
    }
}
