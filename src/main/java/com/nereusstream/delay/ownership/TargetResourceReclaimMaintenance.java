package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One Native GC preparation/Source handoff, driven by the existing maintenance tick and process I/O. */
public final class TargetResourceReclaimMaintenance {
    public record ReadLimits(int records, long bytes, long nanos) {
        public ReadLimits {
            if (records < 1 || records == Integer.MAX_VALUE || bytes < 1 || bytes == Long.MAX_VALUE
                    || nanos < 1 || nanos == Long.MAX_VALUE) throw new IllegalArgumentException("finite Native GC read bounds required");
        }
        BoundedReadBudget budget(LongSupplier clock) { return new BoundedReadBudget(records, bytes, nanos, clock); }
    }

    public record Configuration(Path signingConfiguration, byte[] deploymentRoot, int maximumRows,
            ReadLimits discovery, ReadLimits keys, ReadLimits recovery, ReadLimits nativeReads,
            long maximumInputBytes, long preparationTimeoutNanos, long retryWindowMs) {
        public Configuration {
            Objects.requireNonNull(signingConfiguration); Objects.requireNonNull(discovery); Objects.requireNonNull(keys);
            Objects.requireNonNull(recovery); Objects.requireNonNull(nativeReads);
            Bytes.requireLength(deploymentRoot, 32, "Native GC deployment Root"); deploymentRoot = Bytes.copy(deploymentRoot);
            if (!signingConfiguration.isAbsolute() || maximumRows < 1 || maximumRows > TargetResourceReclaimBody.MAX_OPERATIONS
                    || maximumInputBytes <= minimumWindow(discovery, recovery, nativeReads) || maximumInputBytes == Long.MAX_VALUE
                    || preparationTimeoutNanos < 1 || preparationTimeoutNanos == Long.MAX_VALUE || retryWindowMs < 1 || retryWindowMs == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Native GC requires explicit finite original configuration/input/time bounds");
            }
        }
        @Override public byte[] deploymentRoot() { return Bytes.copy(deploymentRoot); }
        private static long scratch(ReadLimits discovery, ReadLimits recovery, ReadLimits nativeReads) {
            return Math.multiplyExact(4L, Math.addExact(discovery.bytes(), Math.addExact(recovery.bytes(), nativeReads.bytes())));
        }
        private static long minimumWindow(ReadLimits discovery, ReadLimits recovery, ReadLimits nativeReads) {
            return Math.addExact(scratch(discovery, recovery, nativeReads), Math.addExact(TargetResourceReclaimSigner.CONFIGURATION_WINDOW_BYTES,
                    Math.multiplyExact(4L, TargetResourceReclaimBody.MAX_CANONICAL_BYTES + 4096L)));
        }
        long signingWindow() { return maximumInputBytes - scratch(discovery, recovery, nativeReads); }
        long heldWindow() { return maximumInputBytes - minimumWindow(discovery, recovery, nativeReads); }
    }

    /** Original deployment admission, never a pair of metadata observations or caller approval booleans. */
    public interface InputProvider {
        /**
         * Acquires this preparation's real original loan outside Store/Worker locks. INLINE uses the separate
         * PROTECT_NATIVE_INLINE_PAYLOAD_RECLAIM purpose, never Catalog-object rights or Source Action3.
         * Bind the actual Source/Shard/tenant/lineage, Native Store incarnation/current Owner, exact frozen
         * candidates and the original publication namespace/images/cut. The held image authority also binds
         * original policy/SourceFacts and each actual Pin as preparation freezes them. No future Source point
         * is predicted. Failures retain/clean partial acquisition; the finite bound is already reserved.
         */
        Held load(List<TargetResourceReclaimDiscovery.Candidate> candidates, long maximumBytes, Runnable operationGuard);
    }
    public interface Held extends AutoCloseable {
        /** Complete stable original image set, retained under the same admission hold. */
        List<TargetCheckpointRootVerifier.AuthenticatedImage> images();
        long retainedBytes();
        /** Held future-reader/Pin/image/Source admission checks only; no remote I/O at Source commit. */
        void requireCurrent();
        /**
         * Only local revalidation of that actual inline loan/owner/epoch. It covers all legal primary
         * read/replay/export admission, prior read exits or separately charged independent copies, and
         * old image activation excluded or forced through the real closed safe cut before service.
         * Unknown readers or uncovered old deployments fail. An identity snapshot/empty inventory is not it.
         * close() below releases only this preparation's acquisition, retaining a failed close for cleanup.
         */
        default void requireInlinePayloadIndependent(){throw new IllegalStateException("GC input hold has no real inline reader/admission protection");}
        /** This preparation's actual record/query/Source/Pin admission, bound to exact original values and safe cut. */
        default void requireNativeRetainedIndependent(){throw new IllegalStateException("GC hold lacks actual Native record/query/recovery admission");}
        /** Exact Native OBJECT read/replay/export/upload/commit/Pin purpose, never inline/Catalog/Action3 rights. */
        default void requireObjectPayloadIndependent(){throw new IllegalStateException("actual Native OBJECT admission unavailable");}
        /** The original Profile/namespace adapter; a local content-hash backend is not a production delete provider. */
        default com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore objectPayloadStore(){throw new IllegalStateException("actual OBJECT provider unavailable");}
        /** A bounded already loaded, independently authenticated original provider operation; absence is not evidence. */
        default Optional<com.nereusstream.delay.protocol.TargetResourceReclaimBody.ObjectPayloadConfirm> objectPayloadHistoricalConfirmation(
                com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord intent){return Optional.empty();}
        /**
         * The original load authenticates the durable exact-operation-history guarantee outside locks; recheck it locally before I/O.
         * It survives response loss, process/Owner/epoch loss and this preparation's close. Native56 and original
         * Source/Pin/reader protections remain durable until confirmation. No current Profile flag/empty GET or
         * in-memory receipt is that guarantee. Missing deployment integration rejects before DELETE.
         */
        default void requireRecoverableObjectProviderOperation(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord intent){
            throw new IllegalStateException("durable original OBJECT provider operation history is unqualified");
        }
        /**
         * Idempotent release of this preparation's loans/admission acquisition only. Does not remove
         * authoritative RecoveryPins, retained images, Source/history or other durable recovery protection.
         * A failure retains this exact original release state for retry.
         */
        @Override void close();
    }
    public enum Status { IDLE, PREPARING, SOURCE_PENDING, CLOSING, SETTLED, STOPPED, STOPPED_FOR_REPLAY, FAILED }
    public record Turn(Status status, Optional<SystemMutationResult> result, Throwable failure) {}
    /** Only an actual provider response minted after the real durable-history qualification enters this owner. */
    private record ObjectDebt(TargetResourceReclaimDiscovery.Candidate candidate,
            com.nereusstream.delay.adapter.PayloadObjectDelete.Observation observation,TargetResourceReclaimDiscovery.Page page){
        long retainedBytes(){final var request=observation.request();return Math.addExact(candidate.operation().canonicalBytes().length,
                request.expectedOwner().canonicalBytes().length+request.originBytes()+request.intentAt().canonicalBytes().length+512L);}
    }
    private volatile ObjectDebt objectDebt;
    /** Inspectable only for the selected actual Source input; this receipt grants no Source/admission permission. */
    Optional<com.nereusstream.delay.adapter.PayloadObjectDelete.Observation> objectDeletionFor(com.nereusstream.delay.protocol.SystemMutation input){
        final var debt=objectDebt;
        if(debt==null)return Optional.empty();
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation)
                ||!java.util.Arrays.equals(confirmation.intent().canonicalBytes(),((TargetResourceReclaimBody.ObjectPayloadConfirm)debt.candidate().operation()).intent().canonicalBytes()))return Optional.empty();
        debt.observation().requireConfirmation(body);return Optional.of(debt.observation());
    }
    private record Opened(TargetResourceReclaimDiscovery.Page page, Held held, TargetResourceReclaimSigner.Prepared prepared, Throwable failure) {}

    private final TargetWorkerShardRuntime worker;
    private final OwnedSourceSystemWriter writer;
    private final Configuration configuration;
    private final InputProvider provider;
    private final TargetWorkerProcessRuntime.ReclaimInputs reservation;
    private final ThreadPoolExecutor executor;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final LongSupplier clock, nanos;
    private final Runnable originalSource;
    private final AtomicBoolean turnActive = new AtomicBoolean();
    private TargetResourceReclaimDiscovery.Cursor cursor;
    private volatile OwnedCheckpointIo<Opened> opening;
    private OwnedCheckpointIo<Boolean> closing;
    private Opened selected;
    private volatile TargetEvidenceRetirementWorkClassExecutor.Submission submission;
    private SystemMutationResult result;
    private long started;
    private volatile boolean stopping, exited;
    private boolean replayStopping;

    TargetResourceReclaimMaintenance(TargetWorkerShardRuntime worker, OwnedSourceSystemWriter writer,
            Configuration configuration, InputProvider provider, TargetWorkerProcessRuntime.ReclaimInputs reservation,
            ThreadPoolExecutor executor, Supplier<TrustedUtcIntervalEvidence> time, LongSupplier clock, LongSupplier nanos, Runnable originalSource) {
        this.worker = Objects.requireNonNull(worker); this.writer = Objects.requireNonNull(writer);
        this.configuration = Objects.requireNonNull(configuration); this.provider = Objects.requireNonNull(provider);
        this.reservation = Objects.requireNonNull(reservation); this.executor = Objects.requireNonNull(executor);
        this.time = Objects.requireNonNull(time); this.clock = Objects.requireNonNull(clock); this.nanos = Objects.requireNonNull(nanos);
        this.originalSource = Objects.requireNonNull(originalSource);
        if (reservation.bytes() != configuration.maximumInputBytes()) throw new IllegalArgumentException("Native GC changed original reservation");
    }

    /** Short local state-machine step; all file/Catalog/image/provider work runs on original owned I/O. */
    public Turn runTurn() {
        if (!turnActive.compareAndSet(false, true)) throw new IllegalStateException("concurrent Native GC maintenance turn");
        try { return advance(true); }
        finally { turnActive.set(false); }
    }
    /** Quarantine may advance owned cleanup, but cannot borrow an unavailable Native read authority. */
    void runQuarantinedTurn() {
        requestStop();
        if (!turnActive.compareAndSet(false, true)) return;
        try { if (!exited) replayStopping = true; advance(false); }
        finally { turnActive.set(false); }
    }
    /** Exact original objects only; never a caller-supplied outcome or release approval. */
    void requireReplayPreparation(TargetWorkerShardRuntime actual, OwnedSourceSystemWriter originalWriter,
            TargetEvidenceRetirementWorkClassExecutor.Submission originalSubmission) {
        if (worker != actual || writer != originalWriter || submission != originalSubmission || selected == null
                || !java.util.Arrays.equals(selected.prepared().mutation().canonicalEnvelope(), originalSubmission.mutation().canonicalEnvelope())) {
            throw new IllegalArgumentException("GC replay exit changed its original preparation, writer or Source frame");
        }
    }
    private Turn advance(boolean sourceReads) {
        if (exited) return turn(stoppedStatus(), null);
        if (opening != null) {
            final long elapsed = nanos.getAsLong() - started;
            if (stopping || elapsed < 0 || elapsed >= configuration.preparationTimeoutNanos()) opening.requestStop();
            opening.retryCleanup();
            if (!opening.ioExited() || !opening.receipt().isDone()) return turn(Status.PREPARING, ownedFailure(opening));
            final var loaded = opening.completedValue().orElse(null);
            final var failure = ownedFailure(opening); opening = null;
            if (loaded == null) {
                if (failure instanceof TargetResourceReclaimDiscovery.StaleCursor) cursor = null;
                if (stopping) return finishStop();
                return turn(Status.FAILED, failure);
            }
            cursor = loaded.page().complete() ? null : loaded.page().continuation();
            selected = loaded;
            if (loaded.prepared() == null) {
                selected = null;
                if (stopping) return finishStop();
                return turn(loaded.failure() == null ? Status.IDLE : Status.FAILED, loaded.failure());
            }
        }
        if (selected != null) {
            if (closing != null) {
                closing.retryCleanup();
                if (!closing.ioExited() || !closing.receipt().isDone()) return turn(Status.CLOSING, ownedFailure(closing));
                final var failure = ownedFailure(closing);
                if (!closing.completedValue().orElse(false)
                        && !(failure instanceof CleanupFailure cleanup && cleanup.cleanupComplete())) return turn(Status.CLOSING, failure);
                if(objectDebt!=null&&result!=null&&result.applyStatus()==com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                        &&result.stableCode()==com.nereusstream.delay.protocol.StableCode.OK
                        &&TargetResourceReclaimBody.decode(selected.prepared().mutation().canonicalBody()).operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm){
                    objectDebt.observation().requireConfirmation(TargetResourceReclaimBody.decode(selected.prepared().mutation().canonicalBody()));objectDebt=null;
                }
                closing = null; selected = null; submission = null;
                if (stopping) return finishStop();
                final var settled = new Turn(result == null ? Status.IDLE : Status.SETTLED, Optional.ofNullable(result), null); result = null; return settled;
            }
            if (submission != null) {
                if (stopping || submission.failure().isPresent()) submission.stopBeforeAppend();
                final var knownFirst = submission.firstResult();
                if (knownFirst.isPresent() && submission.closeForRelease()) { result = knownFirst.orElseThrow(); return closeSelected(); }
                if (!submission.releaseIfNeverAppended()) {
                    if (!sourceReads) {
                        if (submission.releasePreparationForReplay(this)) return closeSelected();
                        return turn(Status.SOURCE_PENDING, submission.failure().orElse(null));
                    }
                    final var settled = submission.settle(clock);
                    if (settled.isEmpty()) {
                        if (!stopping || !submission.stopBeforeAppend()) submission.retry(clock);
                        return turn(Status.SOURCE_PENDING, submission.failure().orElse(null));
                    }
                    result = settled.orElseThrow();
                }
                return closeSelected();
            }
            if (stopping) return closeSelected();
            try {
                final var knownFirst = worker.retirementMutationResult(selected.prepared().mutation(), clock);
                if (knownFirst.isPresent()) { result = knownFirst.orElseThrow(); return closeSelected(); }
                selected.prepared().requireCurrent();
                worker.submitResourceReclaim(selected.prepared(), writer, clock, retained -> submission = retained);
                // A concurrent stop cannot discard a request after the original writer-entry gate.
                if (stopping) submission.stopBeforeAppend();
                return turn(Status.SOURCE_PENDING, null);
            } catch (RuntimeException failure) {
                // The callback precedes enqueue; an exception may still own the original Source handoff.
                if (submission != null) return turn(Status.SOURCE_PENDING, failure);
                return closeSelected(failure);
            }
        }
        if (stopping) return finishStop();
        final var after = cursor; started = nanos.getAsLong();
        try { opening = OwnedCheckpointIo.submitResource(guard -> prepare(after, guard), this::requirePreparing, this::discard, executor); }
        catch (RejectedExecutionException full) { return turn(Status.IDLE, full); }
        return turn(Status.PREPARING, null);
    }

    private Opened prepare(TargetResourceReclaimDiscovery.Cursor after, Runnable guard) {
        guard.run();
        final var debt=objectDebt;
        final var page=debt==null?worker.discoverResourceReclaim(configuration.discovery().budget(nanos),after,configuration.maximumRows(),clock):debt.page();
        if (page.candidates().isEmpty()) return new Opened(page, null, null, null);
        Held held = null;
        try {
            guard.run(); held = Objects.requireNonNull(provider.load(page.candidates(), configuration.heldWindow(), guard), "original held Native GC inputs");
            held.requireCurrent(); guard.run();
            if (held.retainedBytes() < 0 || held.retainedBytes() > configuration.heldWindow()) throw new IllegalArgumentException("held Native GC inputs exceed original reservation");
            final Held original = held;
            // No transient OwnedCheckpointIo stop guard escapes into a submitted Source frame.
            final Runnable current=()->{
                reservation.requireCurrent();original.requireCurrent();
                if(page.candidates().getFirst().objectPayload())original.requireObjectPayloadIndependent();
                else if(page.candidates().getFirst().nativeRetained())original.requireNativeRetainedIndependent();
                else if(page.candidates().getFirst().inlinePayload())original.requireInlinePayloadIndependent();
                original.requireCurrent();
            };
            current.run();
            var candidates=debt==null?page.candidates():List.of(debt.candidate());
            if(debt==null&&candidates.getFirst().objectPayload()){
                final var selected=candidates.getFirst();
                if(selected.pendingObject()){
                    final var intent=selected.objectIntent();
                    final var historical=Objects.requireNonNull(original.objectPayloadHistoricalConfirmation(intent),"bounded original provider history selection");
                    if(historical.isPresent())candidates=List.of(selected.withObjectConfirmation(historical.orElseThrow()));
                    else{
                        original.requireRecoverableObjectProviderOperation(intent);current.run();guard.run();
                        final var operation=intent.operation();
                        final long debtWindow=intent.canonicalBytes().length+operation.expected().canonicalBytes().length+(operation.nativeOrigin()==null?operation.proof().canonicalBytes().length:operation.nativeOrigin().canonicalBytes().length)+intent.appliedAt().canonicalBytes().length+1024L;
                        final long available=Math.subtractExact(configuration.signingWindow(),Math.addExact(original.retainedBytes(),debtWindow));
                        final var observation=worker.deletePendingObjectPayload(selected,original.objectPayloadStore(),original.images(),
                                configuration.recovery().budget(nanos),configuration.nativeReads().budget(nanos),available,
                                ()->{current.run();guard.run();original.requireRecoverableObjectProviderOperation(intent);},clock);
                        final var ready=selected.withObjectConfirmation(new TargetResourceReclaimBody.ObjectPayloadConfirm(intent,observation.providerRequestIdHash(),observation.providerResponseHash()));
                        // Publish custody before a stop, signer failure or final OwnedCheckpointIo guard can discard it.
                        objectDebt=new ObjectDebt(ready,observation,new TargetResourceReclaimDiscovery.Page(List.of(ready),page.continuation(),page.stop()));
                        candidates=List.of(ready);
                    }
                }
            }
            current.run();guard.run();
            final long heldBytes=original.retainedBytes();
            if(heldBytes<0||heldBytes>configuration.heldWindow())throw new IllegalStateException("complete OBJECT proof/history inputs exceed original held reservation");
            long signingWindow=configuration.signingWindow();
            if(candidates.getFirst().objectPayload())signingWindow=Math.subtractExact(signingWindow,Math.addExact(heldBytes,objectDebt==null?0:objectDebt.retainedBytes()));
            final long retryUntil=Math.addExact(Objects.requireNonNull(time.get()).latestEpochMs(),configuration.retryWindowMs());
            final var prepared=worker.prepareConfiguredResourceReclaim(candidates,configuration.signingConfiguration(),configuration.deploymentRoot(),
                    original.images(),configuration.keys().budget(nanos),configuration.recovery().budget(nanos),configuration.nativeReads().budget(nanos),
                    time,retryUntil,signingWindow,current,clock);
            guard.run(); return new Opened(page, held, prepared, null);
        } catch (RuntimeException | IOException failure) {
            if (held != null) release(held, failure);
            // A provider may retain partial I/O before it can return Held. Keep that exact cleanup owner.
            if (failure instanceof RuntimeException runtime && failure instanceof CheckpointInputFuture.Cleanup) throw runtime;
            return new Opened(page, null, null, failure);
        } catch (Error fatal) {
            if (held != null) release(held, fatal);
            throw fatal;
        }
    }
    private void requirePreparing() {
        if (stopping) throw new IllegalStateException("Native GC preparation stopped");
        reservation.requireCurrent(); originalSource.run();
        if (stopping) throw new IllegalStateException("Native GC preparation stopped");
    }
    private Turn closeSelected() { return closeSelected(null); }
    private Turn closeSelected(Throwable cause) {
        if (closing == null) {
            final var original = selected.held();
            try { closing = OwnedCheckpointIo.submit(ignored -> { release(original, null); return true; }, () -> {}, executor); }
            catch (RejectedExecutionException full) { return turn(Status.CLOSING, full); }
        }
        return turn(Status.CLOSING, cause);
    }
    private Turn finishStop() {
        // Original local I/O has exited. Late/UNKNOWN provider work remains covered by the qualified durable
        // operation history and Native56 protections. Source UNKNOWN follows the original submission custody above.
        cursor = null;objectDebt=null;reservation.release(); exited = true; return turn(stoppedStatus(), null);
    }
    private Status stoppedStatus() { return replayStopping ? Status.STOPPED_FOR_REPLAY : Status.STOPPED; }
    /** Stops acquisition; a submitted UNKNOWN keeps its exact frame, input reservation and original protections. */
    public void requestStop() {
        stopping = true;
        final var pending = opening; if (pending != null) pending.requestStop();
        final var sent = submission; if (sent != null) sent.stopBeforeAppend();
    }
    public boolean ioExited() { return exited; }
    private Turn turn(Status status, Throwable failure) { return new Turn(status, Optional.ofNullable(result), failure); }
    private static Throwable ownedFailure(OwnedCheckpointIo<?> io) { return CheckpointInputFuture.ownedFailure(io.receipt()); }
    private void discard(Opened loaded) { if (loaded.held() != null) release(loaded.held(), null); }
    private static void release(Held original, Throwable cause) {
        try { original.close(); }
        catch (RuntimeException | Error failure) {
            final var retained = new CleanupFailure(original, cause == null ? failure : cause);
            if (cause != null && cause != failure) retained.addSuppressed(failure);
            throw retained;
        }
    }
    private static final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held original;
        private final transient CheckpointInputFuture.Cleanup prior;
        private volatile boolean complete;
        private final AtomicBoolean closing = new AtomicBoolean();
        CleanupFailure(Held original, Throwable failure) {
            super("original Native GC input cleanup is unconfirmed", failure); this.original = original;
            prior = failure instanceof CheckpointInputFuture.Cleanup cleanup ? cleanup : null;
        }
        @Override public boolean cleanupComplete() { return complete && (prior == null || prior.cleanupComplete()); }
        @Override public CompletableFuture<Void> retryCleanup() {
            if (cleanupComplete()) return CompletableFuture.completedFuture(null);
            if (!closing.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("original Native GC close is running"));
            try {
                if (!complete) { original.close(); complete = true; }
                return prior == null || prior.cleanupComplete() ? CompletableFuture.completedFuture(null)
                        : Objects.requireNonNull(prior.retryCleanup(), "original provider cleanup future");
            }
            catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
            finally { closing.set(false); }
        }
    }
}
