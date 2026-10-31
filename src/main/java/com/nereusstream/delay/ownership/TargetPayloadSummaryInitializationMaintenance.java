package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import com.nereusstream.delay.store.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** One protected old-owner upgrade on the original Process WorkClass/I/O and independently paid management Source. */
public final class TargetPayloadSummaryInitializationMaintenance implements CheckpointInputFuture.Cleanup {
    private static byte[] assigned(byte[] value, int length, String name) {
        Bytes.requireLength(value, length, name);
        if (Arrays.equals(value, new byte[length])) throw new IllegalArgumentException(name + " is unassigned");
        return Bytes.copy(value);
    }
    public enum State { OPENING, FOLD, ARCHIVE, REGISTER, APPEND, OBSERVE, CLOSING, COMPLETE, STOPPED }
    public record Turn(State state, boolean blocked, Throwable failure) {}
    public record Configuration(TargetQuotaScope scope, byte[] lineage, DelayMessageId messageId,
            TargetPayloadObligationSummaryBootstrap.Limits fold, int pageRows, int readRecords,
            long readBytes, long turnNanos, long totalReadRecords, long totalReadBytes,
            long maximumRetainedBytes, int maximumArchiveBytes, long taskBytes, long maximumHoldingNanos,
            int maximumNativeAttempts) {
        public Configuration {
            Objects.requireNonNull(scope); Objects.requireNonNull(messageId); Objects.requireNonNull(fold);
            lineage = assigned(lineage, 16, "summary maintenance lineage");
            if (scope.target() != null || !scope.shard().equals(messageId.routingId().shardId())
                    || pageRows < 1 || pageRows > fold.maximumRecords() || readRecords < 1 || readRecords == Integer.MAX_VALUE
                    || readBytes < 1 || readBytes == Long.MAX_VALUE || turnNanos < 1 || turnNanos == Long.MAX_VALUE
                    || totalReadRecords < readRecords || totalReadRecords == Long.MAX_VALUE
                    || totalReadBytes < readBytes || totalReadBytes == Long.MAX_VALUE
                    || maximumRetainedBytes < fold.maximumRetainedBytes() || maximumRetainedBytes == Long.MAX_VALUE
                    || maximumArchiveBytes < 1 || maximumArchiveBytes == Integer.MAX_VALUE || maximumArchiveBytes > maximumRetainedBytes
                    || taskBytes < 1 || taskBytes == Long.MAX_VALUE || taskBytes > maximumRetainedBytes
                    || maximumHoldingNanos < turnNanos || maximumHoldingNanos == Long.MAX_VALUE
                    || maximumNativeAttempts < 1 || maximumNativeAttempts == Integer.MAX_VALUE)
                throw new IllegalArgumentException("summary maintenance needs the original finite cumulative operation envelope");
        }
        @Override public byte[] lineage() { return Bytes.copy(lineage); }
        public static Configuration load(java.nio.file.Path file) throws java.io.IOException {
            final var fields = TargetWorkerRuntimeConfigLoader.loadFields(file);
            final var result = new Configuration(TargetQuotaScope.decode(fields.image("summary.scopeFile", TargetQuotaScope.MAX_CANONICAL_BYTES)),
                    fields.hex("summary.lineage", 16, 16), new DelayMessageId(fields.hex("summary.messageId", DelayMessageId.LENGTH, DelayMessageId.LENGTH)),
                    new TargetPayloadObligationSummaryBootstrap.Limits(fields.integer("summary.fold.maximumRecords"), fields.number("summary.fold.maximumRetainedBytes")),
                    fields.integer("summary.pageRows"), fields.integer("summary.readRecords"), fields.number("summary.readBytes"), fields.number("summary.turnNanos"),
                    fields.number("summary.totalReadRecords"), fields.number("summary.totalReadBytes"), fields.number("summary.maximumRetainedBytes"),
                    fields.integer("summary.maximumArchiveBytes"), fields.number("summary.taskBytes"), fields.number("summary.maximumHoldingNanos"),
                    fields.integer("summary.maximumNativeAttempts"));
            fields.requireComplete(); return result;
        }
    }

    /** Implemented only by the actual Process/Startup composition holding the frozen upgrade root. */
    interface Admission {
        TargetWorkerProcessRuntime.Assembly graph();
        TargetWorkerShardRuntime worker();
        ShardStore store();
        TargetStoreBackend backend();
        TargetStoreBackend.ReadAuthority reads();
        OwnedSourceSystemWriter writer();
        OwnerIdentity ownerIdentity();
        void requireCurrent(TargetPayloadSummaryInitializationMaintenance actual);
        void requireLocalCurrent(TargetPayloadSummaryInitializationMaintenance actual);
        long reservedBytes();
        void release(TargetPayloadSummaryInitializationMaintenance actual);
    }

    public record ControlPreparation(byte[] operationId, TargetPayloadSummaryControlMutationFactory factory,
            ControlAuthorizationContext authenticated, ControlOperationAuthorization.TargetScopeProof exactScope,
            long registrationRetryUntil, long sourceRetryUntil) {
        public ControlPreparation {
            operationId = assigned(operationId, 32, "original summary operation ID");
            Objects.requireNonNull(factory); Objects.requireNonNull(authenticated); Objects.requireNonNull(exactScope);
        }
        @Override public byte[] operationId() { return Bytes.copy(operationId); }
    }

    public interface Inputs extends CheckpointInputFuture.Cleanup, AutoCloseable {
        interface Provider {
            /** Retain every partial acquisition on opening before original native I/O starts. */
            Inputs open(Opening opening);
            /** Actual archived raw frame/original key/registration/protected-image reacquisition for Source replay. */
            TargetSourceAuthorityLoader.HeldInputs loadSource(SourceReplayMutation original, Runnable current);
        }
        void requireCurrent();
        void requireLocalCurrent();
        long retainedBytes();
        TargetPayloadObligationSummaryBootstrap.Protection protection();
        CheckpointResource protectedImage();
        ManagementHandle originalManagementSource();
        TargetScopeDeploymentAllocation.RegistryHandle originalManagementRegistry();
        ControlTargetRegistrationAuthority registration();
        ControlPreparation controlPreparation(TargetPayloadSummaryInitializationRequest request);
        @Override void close();
    }

    /** Additional original-owner capability; an existing Worker producer or migration grant never supplies it. */
    public interface ManagementHandle extends TargetScopeDeploymentAllocation.SourceHandle {
        /** Pure local validation of this already authenticated original Root/actor/purpose/payment loan. */
        void requireSummaryManagement(ExposureRequest request);
        TargetPayloadSummaryPreparedCustody.Backing preparedBacking(PreparedRequest request);
        Debit reserveSummaryControl(ExposureRequest request, Runnable current);
    }
    public record PreparedRequest(Opening opening, TargetPayloadSummaryInitializationRequest request) {
        public PreparedRequest { Objects.requireNonNull(opening); Objects.requireNonNull(request); opening.requireRequestScope(request); }
        public byte[] candidateId() {
            return Bytes.sha256(Bytes.utf8("nereus-delay-payload-summary-candidate-v1\0"), request.expectedOwner().key(),
                    request.expectedOwner().canonicalBytes(), request.auditedCut().canonicalBytes(), request.protectedImage().canonicalBytes());
        }
    }
    public record ExposureRequest(Opening opening, TargetPayloadSummaryControlMutationFactory.Prepared exact,
            OwnedSourceSystemWriter writer, TargetScopeExposureSource.CopyContract copies,
            TargetScopeDeploymentAllocation.RegistryHandle registry) {
        public ExposureRequest {
            Objects.requireNonNull(opening); Objects.requireNonNull(exact); Objects.requireNonNull(writer);
            Objects.requireNonNull(copies); Objects.requireNonNull(registry);
            final var body = TargetPayloadSummaryInitializationBody.decode(exact.mutation().canonicalBody()); body.requireInput(exact.mutation());
            opening.requireRequestScope(body.request());
        }
    }
    /** Actual prepaid original operation, including all copies/UNKNOWN/history; close only returns this local loan. */
    public interface Debit extends CheckpointInputFuture.Cleanup, AutoCloseable {
        /** Same original loan and exact frame/copies, without I/O or reacquisition. */
        void requireBound(ExposureRequest request);
        /** Pure local revocation/ownership check retained through the actual native operation. */
        void requireCurrent();
        /** Pure local consumption of the already durable original debit; no allocation, RPC or Store read. */
        void claimNative(ExposureRequest request, boolean retry);
        long retainedBytes();
        boolean ioExited();
        void requestStop();
        @Override void close();
    }

    public static final class Opening {
        private final TargetPayloadSummaryInitializationMaintenance owner;
        private Opening(TargetPayloadSummaryInitializationMaintenance owner) { this.owner = owner; }
        public Configuration configuration() { return owner.configuration; }
        public TargetWorkerProcessRuntime.Assembly graph() { return owner.admission.graph(); }
        public OwnerIdentity ownerIdentity() { return owner.admission.ownerIdentity(); }
        public OwnedSourceSystemWriter writer() { return owner.admission.writer(); }
        public ShardStore store() { return owner.admission.store(); }
        public void requireCurrent() { owner.requireCurrent(); }
        public void requireLocalCurrent() { owner.requireLocalCurrent(); }
        public synchronized void retainInputs(Inputs actual) {
            if (owner.inputs != null && owner.inputs != actual) throw new IllegalStateException("summary opening already retained original inputs");
            owner.inputs = Objects.requireNonNull(actual);
        }
        public void retainCleanup(CheckpointInputFuture.Cleanup actual) { owner.retainCleanup(actual); }
        public long remainingRetainedBytes() { return Math.subtractExact(owner.configuration.maximumRetainedBytes(), owner.retainedBytes()); }
        private void requireRequestScope(TargetPayloadSummaryInitializationRequest request) {
            if (!request.scope().equals(owner.configuration.scope()) || !Arrays.equals(request.lineage(), owner.configuration.lineage())
                    || !request.expectedOwner().messageId().equals(owner.configuration.messageId()))
                throw new IllegalArgumentException("summary management changed its actual frozen root/owner");
        }
    }

    private final Admission admission;
    private final Configuration configuration;
    private final Inputs.Provider provider;
    private final LongSupplier nanos;
    private final TargetMigrationMaintenanceWorkClassExecutor executor;
    private final Opening opening = new Opening(this);
    private final List<CheckpointInputFuture.Cleanup> partial = new ArrayList<>();
    private volatile Inputs inputs;
    private volatile State state = State.OPENING;
    private volatile boolean stopping, exited, blocked;
    private volatile Throwable failure;
    private volatile StableCode terminalCode;
    private final long started;
    private TargetQuotaPayloadOwner originalOwner;
    private TargetPayloadObligationSummaryBootstrap.Cursor cursor;
    private TargetPayloadObligationSummaryBootstrap.Fold fold;
    private TargetPayloadSummaryInitializationRequest request;
    private TargetPayloadSummaryPreparedCustody custody;
    private TargetPayloadSummaryControlMutationFactory.Prepared exact;
    private ExposureRequest exposure;
    private Debit debit;
    private volatile AppendPermit permit;
    private SystemMutationResult first;
    private long actualReadRecords, actualReadBytes;
    private int attempts;
    private boolean released;
    private OwnedCheckpointIo<Boolean> cleanup;

    TargetPayloadSummaryInitializationMaintenance(Admission admission, Configuration configuration, Inputs.Provider provider, LongSupplier nanos) {
        this.admission = Objects.requireNonNull(admission); this.configuration = Objects.requireNonNull(configuration);
        this.provider = Objects.requireNonNull(provider); this.nanos = Objects.requireNonNull(nanos); started = nanos.getAsLong();
        if (admission.reservedBytes() != configuration.maximumRetainedBytes()
                || configuration.taskBytes() < admission.graph().config().maximumSourceApplyTaskBytes())
            throw new IllegalArgumentException("summary maintenance differs from original Process I/O reservation");
        admission.backend().requireStore(admission.store());
        admission.writer().ownedBackendGroup().requireWorkerGraph(admission.graph().classes(), admission.graph().resources());
        if (admission.writer().sourceWriterWorker() != admission.worker())
            throw new IllegalArgumentException("summary must use this actual Process Source producer");
        executor = new TargetMigrationMaintenanceWorkClassExecutor(admission.graph(),
                "target-payload-summary-" + Bytes.hex(configuration.messageId().bytes()), configuration.taskBytes());
    }

    public State state() { return state; }
    public boolean ioExited() { return exited; }
    public Optional<SystemMutationResult> first() { return Optional.ofNullable(first); }
    /** Result reporting only; actual service qualification remains the Native32/Source proof. */
    public Optional<StableCode> terminalCode() { return Optional.ofNullable(terminalCode); }
    public synchronized Turn runTurn() {
        if (exited) return new Turn(state, blocked, failure);
        if (stopping || state == State.CLOSING) { retryCleanup(); return new Turn(state, blocked, failure); }
        try {
            if (!executor.idle()) return new Turn(state, true, failure);
            requireLocalCurrent();
            executor.submit(() -> {
                try { blocked = false; failure = null; step(); }
                catch (RuntimeException | Error problem) { retainFailure(problem); blocked = true; failure = problem; }
            }, this::requireCurrent);
        } catch (RuntimeException | Error problem) { retainFailure(problem); blocked = true; failure = problem; }
        return new Turn(state, blocked, failure);
    }

    private void step() {
        requireCurrent();
        if (!retryFailures()) { blocked = true; return; }
        if (nanos.getAsLong() - started >= configuration.maximumHoldingNanos())
            throw new IllegalStateException("original summary operation exhausted its total protected holding window");
        switch (state) {
            case OPENING -> open();
            case FOLD -> fold();
            case ARCHIVE -> archive();
            case REGISTER -> register();
            case APPEND -> append();
            case OBSERVE -> { if (observe()) state = State.CLOSING; else if (admission.writer().payloadSummaryOperationsExited()
                    && attempts < configuration.maximumNativeAttempts()) state = State.APPEND; else blocked = true; }
            default -> throw new IllegalStateException("summary step is not admitted in " + state);
        }
    }
    private void open() {
        final var acquired = Objects.requireNonNull(provider.open(opening), "original platform summary inputs");
        if (inputs != acquired) throw new IllegalStateException("summary inputs were not retained before acquisition");
        acquired.requireCurrent();
        originalOwner = read(budget -> admission.backend().guardedRead(budget, reader -> {
            final var key = Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT}, configuration.messageId().bytes());
            return TargetQuotaPayloadOwner.decodeForStore(key, TargetValueEnvelope.decode(Objects.requireNonNull(reader.get(ColumnFamily.META, key)),
                    TargetQuotaPayloadOwner.VALUE_TYPE).payload(), configuration.scope().shard(), configuration.scope().tenantScope());
        }, admission.reads()));
        if (originalOwner.summaryQualified()) { terminalCode = StableCode.OK; state = State.CLOSING; return; }
        cursor = read(budget -> TargetPayloadObligationSummaryBootstrap.begin(admission.store(), originalOwner, inputs.protection(), configuration.fold(), budget));
        state = State.FOLD;
    }
    private void fold() {
        fold = read(budget -> cursor.advance(budget, configuration.pageRows())).orElse(null);
        if (fold == null) return;
        request = new TargetPayloadSummaryInitializationRequest(configuration.scope(), configuration.lineage(), fold.owner(), fold.cut(),
                inputs.protectedImage(), fold.digest(), fold.unresolvedCount());
        fold.requireRequest(request); state = State.ARCHIVE;
    }
    private void archive() {
        final var original = Objects.requireNonNull(inputs.controlPreparation(request));
        final var candidate = new PreparedRequest(opening, request);
        if (custody == null) custody = TargetPayloadSummaryPreparedCustody.retainOrRecover(inputs.originalManagementSource(), candidate,
                () -> original.factory().prepare(original.operationId(), request, original.authenticated(), original.exactScope(),
                        original.registrationRetryUntil(), original.sourceRetryUntil()));
        exact = custody.exact();
        if (!Arrays.equals(exact.control().operationId(), original.operationId()))
            throw new IllegalStateException("summary durable winner changed original management operation ID");
        ControlOperationAuthorization.authorize(exact.control(), original.authenticated(), original.exactScope());
        if (observe()) { state = State.CLOSING; return; }
        fold.requireArchivedRequest(request);
        final var source = inputs.originalManagementSource(); final var registry = inputs.originalManagementRegistry();
        exposure = new ExposureRequest(opening, exact, admission.writer(), admission.writer().payloadSummaryCopyContract(), registry);
        final var actor = AuthorIdentity.decode(exact.mutation().authorIdentity());
        source.requireManagementPurpose(TargetScopeDeploymentAllocation.ManagerPurpose.PAYLOAD_SUMMARY_INITIALIZE, actor);
        registry.requireManagementPurpose(TargetScopeDeploymentAllocation.ManagerPurpose.PAYLOAD_SUMMARY_INITIALIZE, actor);
        final var allocation = Objects.requireNonNull(source.readAuthenticated()); Objects.requireNonNull(registry.readAuthenticated());
        if (!allocation.source().sameSourceIdentity(request.auditedCut().source())
                || exact.mutation().encodeFrame().length > allocation.maximumSourceFrameBytes())
            throw new IllegalArgumentException("summary exceeds its original physical Source allocation");
        source.requireSdkContract(exposure.copies()); source.requireSummaryManagement(exposure);
        debit = Objects.requireNonNull(source.reserveSummaryControl(exposure, this::requireCurrent), "original prepaid platform summary Source debit");
        opening.retainCleanup(debit); debit.requireBound(exposure); debit.requireCurrent();
        if (debit.retainedBytes() < 0 || retainedBytes() > configuration.maximumRetainedBytes())
            throw new IllegalStateException("summary management inputs exceed original retained reservation");
        permit = new AppendPermit(this, false); state = State.REGISTER;
    }
    private void register() {
        custody.requireCurrent(); debit.requireCurrent(); fold.requireArchivedRequest(request);
        inputs.registration().register(exact.control());
        inputs.registration().validateMutation(exact.control(), exact.control().targets().getFirst(), exact.mutation());
        state = State.APPEND;
    }
    private void append() {
        if (observe()) { state = State.CLOSING; return; }
        custody.requireCurrent(); debit.requireCurrent();
        if (!admission.writer().payloadSummaryOperationsExited()) { blocked = true; return; }
        if (attempts >= configuration.maximumNativeAttempts()) { blocked = true; return; }
        fold.requireArchivedRequest(request);
        if (permit.exposed) { permit.disabled = true; permit = new AppendPermit(this, true); }
        try { admission.writer().appendPayloadSummary(exact.mutation(), permit); }
        finally { if (permit.exposed) attempts = Math.incrementExact(attempts); }
        state = State.OBSERVE;
    }
    private boolean observe() {
        if (exact == null) return false;
        first = read(budget -> new TargetSystemReplayStore(admission.backend(), configuration.scope(), configuration.lineage(), 2, 1)
                .appliedResult(budget, exact.mutation(), admission.reads()).orElse(null));
        if (first == null) return false;
        read(budget -> admission.backend().guardedRead(budget, reader -> {
            final byte[] key = Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, exact.mutation().systemMutationId());
            final var stored = TargetResultRecord.decode(TargetValueEnvelope.decode(Objects.requireNonNull(reader.get(ColumnFamily.DEDUPE, key)), TargetResultRecord.VALUE_TYPE).payload());
            stored.requireStored(key, TargetResultRecord.VALUE_TYPE, stored.canonicalBytes());
            final var root = TargetParentControlCreditVerifier.readView(reader, configuration.scope(), configuration.lineage()).actualRoot();
            stored.requireOwner(root);
            if (!Arrays.equals(stored.typedPayload(), first.encode()) || !Arrays.equals(stored.mutation().mutationDigest(), Bytes.sha256(exact.mutation().canonicalEnvelope())))
                throw new IllegalStateException("summary observation changed actual Source first");
            final byte[] posKey = Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, stored.mutation().source().canonicalBytes());
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(Objects.requireNonNull(reader.get(ColumnFamily.DEDUPE, posKey)), TargetResultRecord.VALUE_TYPE).payload());
            position.requireStored(posKey, TargetResultRecord.VALUE_TYPE, position.canonicalBytes()); position.requireFirst(stored);
            position.requireOwner(root);
            if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM || !position.mutation().equals(stored.mutation()))
                throw new IllegalStateException("summary observation lost original actual Source POS");
            return Boolean.TRUE;
        }, admission.reads()));
        if (first.applyStatus() == ApplyStatus.APPLIED && first.stableCode() == StableCode.OK) {
            read(budget -> admission.backend().guardedRead(budget, reader -> {
                final var owner = TargetQuotaPayloadOwner.decodeForStore(originalOwner.key(), TargetValueEnvelope.decode(
                        Objects.requireNonNull(reader.get(ColumnFamily.META, originalOwner.key())), TargetQuotaPayloadOwner.VALUE_TYPE).payload(),
                        configuration.scope().shard(), configuration.scope().tenantScope());
                if (!owner.summaryQualified() || owner.summaryOrigin() != TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP
                        || !Arrays.equals(owner.initializationSource().source().canonicalBytes(), first.appliedSourcePosition())
                        || !Arrays.equals(owner.initializationSource().mutationDigest(), Bytes.sha256(exact.mutation().canonicalEnvelope()))
                        || !Arrays.equals(owner.summaryOriginDigest(), request.foldDigest()))
                    throw new IllegalStateException("summary actual first lacks its exact Native32 v2 initialization");
                final var root = TargetParentControlCreditVerifier.readView(reader, configuration.scope(), configuration.lineage()).actualRoot();
                TargetMigrationStore.requireApplied(reader, List.of(), root, exact.mutation(), owner.initializationSource(), reader.aggregate().mutation());
                return Boolean.TRUE;
            }, admission.reads()));
        } else if (first.applyStatus() != ApplyStatus.REJECTED || first.stableCode() != StableCode.STALE_SYSTEM_MUTATION
                && first.stableCode() != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                && first.stableCode() != StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION)
            throw new IllegalStateException("summary original Source has an unexpected terminal first");
        if (permit != null) permit.disabled = true;
        terminalCode = first.stableCode();
        return true;
    }

    private <T> T read(java.util.function.Function<BoundedReadBudget, T> action) {
        final int records = Math.toIntExact(Math.min(configuration.readRecords(), configuration.totalReadRecords() - actualReadRecords));
        final long bytes = Math.min(configuration.readBytes(), configuration.totalReadBytes() - actualReadBytes);
        if (records < 1 || bytes < 1) throw new IllegalStateException("summary exhausted its original cumulative read reservation");
        final var budget = new BoundedReadBudget(records, bytes, configuration.turnNanos(), nanos);
        try { return action.apply(budget); }
        finally { actualReadRecords = Math.addExact(actualReadRecords, budget.actualRecords()); actualReadBytes = Math.addExact(actualReadBytes, budget.actualBytes()); }
    }
    private void requireCurrent() { admission.requireCurrent(this); if (inputs != null) inputs.requireCurrent(); }
    private void requireLocalCurrent() { admission.requireLocalCurrent(this); if (inputs != null) inputs.requireLocalCurrent(); }
    private synchronized void retainCleanup(CheckpointInputFuture.Cleanup actual) { if (!partial.contains(actual)) partial.add(Objects.requireNonNull(actual)); }
    private void retainFailure(Throwable problem) {
        Throwable actual = problem;
        while (actual instanceof java.util.concurrent.CompletionException && actual.getCause() != null && actual.getCause() != actual) actual = actual.getCause();
        if (actual instanceof CheckpointInputFuture.Cleanup cleanup) retainCleanup(cleanup);
    }
    private synchronized boolean retryFailures() {
        for (var cleanup : partial) if (cleanup != debit && cleanup != custody && !cleanup.cleanupComplete()) {
            cleanup.retryCleanup(); if (!cleanup.cleanupComplete()) return false;
        }
        partial.removeIf(cleanup -> cleanup != debit && cleanup != custody && cleanup.cleanupComplete());
        return true;
    }
    private long retainedBytes() {
        long bytes = inputs == null ? 0 : inputs.retainedBytes();
        if (custody != null) bytes = Math.addExact(bytes, custody.retainedBytes());
        if (debit != null) bytes = Math.addExact(bytes, debit.retainedBytes());
        return bytes;
    }
    public synchronized void requestStop() { stopping = true; if (permit != null) permit.disabled = true; executor.requestStop(); state = State.CLOSING; }
    /** Host may now close this same shared producer; UNKNOWN loans remain owned until its real callbacks exit. */
    synchronized boolean prepareForHostDrain(OwnedSourceSystemWriter actual) {
        if (actual != admission.writer()) throw new IllegalArgumentException("summary drain changed its original shared Source producer");
        requestStop();
        return executor.ioExited();
    }
    @Override public boolean cleanupComplete() { return exited; }
    @Override public synchronized CompletableFuture<Void> retryCleanup() {
        if (exited) return CompletableFuture.completedFuture(null);
        if (permit != null) permit.disabled = true;
        executor.requestStop();
        if (!executor.ioExited() || !admission.writer().payloadSummaryOperationsExited()) return CompletableFuture.completedFuture(null);
        if (cleanup != null) {
            if (!cleanup.ioExited()) return CompletableFuture.completedFuture(null);
            final var problem = CheckpointInputFuture.ownedFailure(cleanup.receipt());
            if (problem != null) { retainFailure(problem); failure = problem; }
            if (cleanup.completedValue().orElse(false)) {
                cleanup = null;
                dropExitedReferences();
                if (!released) { admission.release(this); released = true; }
                state = stopping ? State.STOPPED : State.COMPLETE; exited = true;
                return CompletableFuture.completedFuture(null);
            }
            cleanup = null;
        }
        cleanup = OwnedCheckpointIo.submit(guard -> cleanupStep(), () -> {}, admission.graph().io());
        return cleanup.receipt().thenApply(ignored -> null);
    }
    private boolean cleanupStep() {
        if (debit != null) { debit.requestStop(); debit.close(); debit.retryCleanup(); if (!debit.ioExited() || !debit.cleanupComplete()) return false; }
        if (custody != null) { custody.close(); custody.retryCleanup(); if (!custody.cleanupComplete()) return false; }
        for (var cleanup : partial) { cleanup.retryCleanup(); if (!cleanup.cleanupComplete()) return false; }
        if (inputs != null) { inputs.close(); inputs.retryCleanup(); if (!inputs.cleanupComplete()) return false; }
        return true;
    }
    private void dropExitedReferences() {
        fold = null; cursor = null; originalOwner = null; exact = null; request = null; first = null;
        custody = null; exposure = null; debit = null; permit = null; inputs = null; failure = null; partial.clear();
    }

    public static final class AppendPermit implements TargetPayloadSummaryInitializationAppendPermit {
        private final TargetPayloadSummaryInitializationMaintenance owner;
        private final boolean retry;
        private volatile boolean exposed, disabled;
        private AppendPermit(TargetPayloadSummaryInitializationMaintenance owner, boolean retry) { this.owner = owner; this.retry = retry; }
        @Override public synchronized void beforeSdk(OwnedSourceSystemWriter actual, SystemMutation exact, TargetScopeExposureSource.CopyContract copies) {
            owner.requireLocalCurrent();
            if (disabled || owner.stopping || owner.permit != this || owner.state != State.APPEND
                    || actual != owner.admission.writer() || exact != owner.exact.mutation() || !copies.equals(owner.exposure.copies()))
                throw new IllegalStateException("summary SDK handoff changed original private operation/producer/copy contract");
            owner.custody.requireCurrent(); owner.fold.requireLocalCurrent();
            final var source = owner.inputs.originalManagementSource(); final var registry = owner.inputs.originalManagementRegistry();
            source.requireLocalCurrent(); registry.requireLocalCurrent();
            source.requireManagementPurpose(TargetScopeDeploymentAllocation.ManagerPurpose.PAYLOAD_SUMMARY_INITIALIZE, AuthorIdentity.decode(exact.authorIdentity()));
            registry.requireManagementPurpose(TargetScopeDeploymentAllocation.ManagerPurpose.PAYLOAD_SUMMARY_INITIALIZE, AuthorIdentity.decode(exact.authorIdentity()));
            source.requireSdkContract(copies); source.requireSummaryManagement(owner.exposure);
            owner.debit.requireBound(owner.exposure); owner.debit.requireCurrent();
            if (!exposed) { owner.debit.claimNative(owner.exposure, retry); exposed = true; }
            owner.requireLocalCurrent();
        }
    }
}
