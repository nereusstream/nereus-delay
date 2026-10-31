package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.RecoveryInstallPhase;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.RecoveryCatalogAuthority;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Protected bounded Source recovery of an already authenticated Target checkpoint/local recovery Store.
 * The caller owns the accepted no-ACK Source cursor and all Catalog/Pin, original image/semantic/evidence,
 * assignment, physical and session protections. This class neither creates nor releases those authorities.
 */
public final class TargetOwnerRecoveryCoordinator {
    private final ShardStore store;
    private final TargetStoreBootstrap.Reopened initial;
    private final SourceAssignment assignment;
    private final TargetSourceApplyRuntime.Authorities authorities;
    private final TargetSourceApplyRuntime.Limits limits;
    private final SourceReplayCursor<? extends SourceReplayEntry> source;
    private final RecoveryCatalogAuthority catalog;
    private final RecoveryPin pin;
    private final CheckpointManifest manifest;
    private final Runnable protection;
    private final Runnable completionProtection;
    private final ReplayTurnBudget turnBudget;
    private final WorkClassExecutionRegistry workClasses;
    private final LongSupplier clock;
    private final LongSupplier monotonic;
    private final Supplier<BoundedReadBudget> auditBudgets;
    private final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits;
    private volatile OwnerLease lease;
    private volatile LocalRecoveryProtection localRecoveryProtection;
    private volatile boolean localRecoveryStopped;
    private TargetSourceApplyRuntime replay;
    private TargetScopeResourceManager scopeResources;
    private TargetMigrationCompatibilityRuntime migrationCompatibility;
    private TargetPayloadSummaryLegacyPrefix payloadSummaryLegacyPrefix;
    private com.nereusstream.delay.runtime.TargetSloWriterOrigin.Limits writerOriginLimits;
    private com.nereusstream.delay.protocol.OwnerIdentity writerOriginOwner;
    private LongSupplier writerOriginClock;
    private com.nereusstream.delay.protocol.SourcePosition requiredReplayCut;
    private SourceApplyWorkClassExecutor sourceApply;
    private SourceReplayEntry pendingEntry;
    private SourceApplyWorkClassExecutor.Submission pending;
    private TargetClaimTakeoverCoordinator claimTakeover;
    private com.nereusstream.delay.protocol.OwnerIdentity recoveryOwner;
    private com.nereusstream.delay.adapter.PulsarAttemptJournal recoveryJournal;
    private Runnable recoveryJournalProtection;
    private TargetStoreBootstrap.Reopened completedRoot;
    private boolean complete;
    private int turnNumber;
    private volatile long lastNow = -1;
    private TargetSourceAuthorityPreparation authorityPreparation;
    private RecoveryCatalogAuthority evidenceCatalog;
    private TargetWorkerRecoverySource boundReader;
    private SourcePosition boundReaderCut;
    private boolean deferForSourceReactivation;
    private boolean sourceReadyForReactivation;
    private boolean reactivationReaderReleased;
    private com.nereusstream.delay.scheduler.WorkClassTask lastSourceAction;
    private TargetWorkerRecoverySource replayTailReader;
    private SourcePosition replayTailAppliedAt;
    private boolean replayTailCaptured;
    private TargetStoreBootstrap.SloRecoveryAudit sloRecoveryOriginal;
    private TargetStoreBootstrap.SloRecoveryAudit sloRecoveryReplayed;
    private TargetStoreBootstrap.SloRecoveryAudit sloRecoveryCompleted;
    private TargetTimeFenceMutationFactory.Prepared sloRecoveryFence;
    private TargetSourceRecoveryInputs sloRecoveryInput;
    private boolean sloRecoveryBound;
    private boolean sloRecoveryQualificationRequired;
    private boolean sloRecoveryPhaseComplete;
    private SloRecoveryQualification sloRecoveryQualification;

    /**
     * Qualification of one actual original recovery hold, not a caller assertion or a Native census.
     * Only the strict completion callback can create this proof. Ordinary recovery remains unqualified.
     */
    public static final class SloRecoveryQualification {
        private final ShardStore store;
        private final TargetStoreBootstrap.SloRecoveryAudit original;
        private final TargetStoreBootstrap.SloRecoveryAudit replayed;
        private final TargetTimeFenceMutationFactory.Prepared exactFence;
        private final TargetSourceRecoveryInputs input;
        private final SourceAssignment assignment;
        private final OwnerLease owner;
        private TargetStoreBootstrap.Reopened runtimeRoot;

        private SloRecoveryQualification(ShardStore store, TargetStoreBootstrap.SloRecoveryAudit original,
                TargetStoreBootstrap.SloRecoveryAudit replayed, TargetTimeFenceMutationFactory.Prepared exactFence,
                TargetSourceRecoveryInputs input, SourceAssignment assignment, OwnerLease owner) {
            this.store = store; this.original = original; this.replayed = replayed; this.exactFence = exactFence;
            this.input = input; this.assignment = assignment; this.owner = owner;
        }

        private synchronized void bindRuntimeRoot(TargetStoreBootstrap.Reopened root) {
            if (runtimeRoot != null && runtimeRoot != root) {
                throw new IllegalStateException("SLO qualification cannot move to another actual recovery root");
            }
            root.backend().requireStore(store);
            if (!root.root().identity().equals(original.root().identity())
                    || !Arrays.equals(root.root().recoveryLineage(), original.lineage())) {
                throw new IllegalStateException("SLO qualification changed its original Root incarnation/lineage");
            }
            runtimeRoot = root;
        }

        /** Exact final audited root and physical Store/Source/Owner binding; lease renewal preserves identity. */
        public synchronized void requireRuntimeBinding(TargetStoreBootstrap.Reopened root, ShardStore actualStore,
                SourceAssignment actualAssignment, OwnerLease actualOwner) {
            if (runtimeRoot == null || runtimeRoot != root) {
                throw new IllegalStateException("SLO qualification lacks its actual completed recovery root");
            }
            requireScope(actualStore, root.root().scope(), root.root().recoveryLineage(), actualAssignment, actualOwner);
        }

        /** Retained identity checks only; no replacement input, authoritative reread or Store scan. */
        public void requireScope(ShardStore actualStore, TargetQuotaScope scope, byte[] lineage,
                SourceAssignment actualAssignment, OwnerLease actualOwner) {
            if (store != actualStore || !original.scope().equals(scope.shardScope())
                    || !Arrays.equals(original.lineage(), lineage)
                    || !assignment.sameIdentity(actualAssignment) || !owner.sameIdentity(actualOwner)
                    || !original.root().identity().equals(replayed.root().identity())
                    || exactFence != null && !exactFence.body().shard().equals(scope.shard())) {
                throw new IllegalStateException("SLO qualification changed its original Store/Scope/lineage/Source/Owner");
            }
        }

        /** Only this hold's already-retained local version/pin/Scope protections remain valid at apply. */
        public void requireCurrent() { input.requireSourceCurrent(); }
    }
    /** Same original actual reader and retained input; no full cursor/Owner lookup is reachable here. */
    static final class LocalRecoveryProtection{
        private final TargetOwnerRecoveryCoordinator coordinator;
        private final TargetWorkerRecoverySource reader;
        private final TargetSourceRecoveryInputs input;
        private final OwnerLease owner;
        private LocalRecoveryProtection(TargetOwnerRecoveryCoordinator coordinator,TargetWorkerRecoverySource reader,
                TargetSourceRecoveryInputs input,OwnerLease owner){
            this.coordinator=coordinator;this.reader=reader;this.input=input;this.owner=owner;
        }
        void requireBinding(ShardStore store,com.nereusstream.delay.protocol.TargetQuotaScope scope,byte[] lineage,
                SourceAssignment assignment,OwnerLease actual){
            if(coordinator.store!=store||!coordinator.initial.root().scope().equals(scope)
                    ||!java.util.Arrays.equals(coordinator.initial.root().recoveryLineage(),lineage)
                    ||!coordinator.assignment.sameIdentity(assignment)||!owner.sameIdentity(actual))
                throw new IllegalStateException("local recovery protection changed actual Store/Root/lineage/Source/Owner");
        }
        void requireCurrent(){
            try{requireAdmitted();reader.requireLocalCurrent();input.requireSourceCurrent();reader.requireLocalCurrent();requireAdmitted();}
            catch(RuntimeException|Error failure){coordinator.localRecoveryStopped=true;throw failure;}
        }
        private void requireAdmitted(){
            final var current=coordinator.lease;final long now=coordinator.clock.getAsLong();
            if(coordinator.localRecoveryStopped||coordinator.localRecoveryProtection!=this
                    ||current==null||!current.sameIdentity(owner)||!current.validAt(now)||now<coordinator.lastNow
                    ||coordinator.store.nativeExposureFenceRaised())
                throw new IllegalStateException("same held recovery local protection is revoked");
        }
    }
    void configureLocalRecoveryProtection(TargetWorkerRecoverySource actual,TargetSourceRecoveryInputs input){
        Objects.requireNonNull(actual);Objects.requireNonNull(input);
        source.requireLocalReader(actual);
        final var identity=input.owner();
        final LocalRecoveryProtection captured;
        synchronized(this){
            if(replay!=null||turnNumber!=0||localRecoveryProtection!=null||localRecoveryStopped)
                throw new IllegalStateException("local reader/input protection must bind before the original recovery action");
            if(identity.ownerEpoch()!=lease.ownerEpoch()||!java.util.Arrays.equals(identity.leaseFencingDigest(),lease.leaseToken()))
                throw new IllegalStateException("local recovery input changed its actual Owner");
            captured=new LocalRecoveryProtection(this,actual,input,lease);localRecoveryProtection=captured;
        }
        try{captured.requireCurrent();}
        catch(RuntimeException|Error failure){localRecoveryStopped=true;throw failure;}
    }

    synchronized void bindReplayTail(TargetWorkerRecoverySource reader){
        if(turnNumber!=0||replay!=null||replayTailReader!=null)throw new IllegalStateException("finite replay tail must bind before recovery starts");
        replayTailReader=Objects.requireNonNull(reader);
    }

    synchronized boolean awaitingReplayTailCapture(){
        return replayTailReader!=null&&!replayTailCaptured&&pending==null&&pendingEntry==null
                &&(lastSourceAction==null||workClasses.state(lastSourceAction).isEmpty())
                &&assignment.activationBarrier().reachedBy(store.appliedShardLogPosition());
    }

    synchronized void replayTailCaptured(TargetWorkerRecoverySource reader){
        if(replayTailReader!=reader)throw new IllegalArgumentException("captured tail changed its original recovery reader");
        if(replayTailCaptured)return;
        if(!awaitingReplayTailCapture()||!sloRecoveryBound)throw new IllegalStateException("tail captured before original Source barrier/SLO preparation completion");
        replayTailCaptured=true;
    }

    synchronized OwnerRecoveryTurn replayTailCaptureTurn(){
        if(!awaitingReplayTailCapture())throw new IllegalStateException("original Source is not paused for tail capture");
        return new OwnerRecoveryTurn(List.of(),false,turnNumber);
    }

    synchronized boolean needsSloRecoveryPreparation(){return awaitingReplayTailCapture()&&!sloRecoveryBound;}

    synchronized boolean awaitingSloRecoveryCompletion(){
        return replayTailReader!=null&&sloRecoveryBound&&!sloRecoveryPhaseComplete&&replayTailCaptured
                &&pending==null&&pendingEntry==null
                &&(lastSourceAction==null||workClasses.state(lastSourceAction).isEmpty())&&replayTailReached();
    }

    synchronized OwnerRecoveryTurn sloRecoveryCompletionTurn(){
        if(!awaitingSloRecoveryCompletion())throw new IllegalStateException("original Source is not paused for SLO recovery authentication");
        return new OwnerRecoveryTurn(List.of(),false,turnNumber);
    }

    /** Complete local facts only. Independent old-writer/range/time authority is acquired after this view exits. */
    synchronized TargetStoreBootstrap.SloRecoveryAudit captureSloRecoveryAudit(long maximumRetainedBytes,boolean afterTail){
        requireSloAuditPhase(afterTail);beginReplay();requireLease(false);
        final var audit=TargetStoreBootstrap.reopenForSloRecovery(store,initial.root().scope(),
                new TargetStoreBackend.WriteLimits(limits.records(),limits.bytes()),
                Objects.requireNonNull(auditBudgets.get(),"SLO recovery audit budget"),sloRecoveryReads(afterTail),ledgerLimits,maximumRetainedBytes);
        requireLease(false);requireSloAuditPhase(afterTail);
        if(afterTail)sloRecoveryReplayed=audit;else sloRecoveryOriginal=audit;
        return audit;
    }

    private TargetStoreBackend.ReadAuthority sloRecoveryReads(boolean afterTail){
        return (metadata,scope)->{
            requireLease(false);requireSloAuditPhase(afterTail);
            if(!scope.equals(initial.root().scope())||!Arrays.equals(metadata.encode(),store.metadata().encode()))
                throw new IllegalStateException("SLO recovery read changed its original Store/scope");
            final var captured=lease;
            final var original=Objects.requireNonNull(authorities.reads().acquire(metadata,scope));
            return new TargetStoreBackend.CommitGuard(){
                public void requireCurrent(){
                    // No authoritative lease I/O while the native Store view is held.
                    protection.run();requireSloAuditPhase(afterTail);
                    final long now=clock.getAsLong();
                    if(!lease.sameIdentity(captured)||lease.state()!=ShardLifecycleState.CATCHING_UP
                            ||store.isCloseStarted()||store.runtimeMetadata().lastOpenedOwnerEpoch()!=captured.ownerEpoch()
                            ||now<lastNow||!lease.validAt(now))throw new IllegalStateException("SLO recovery read lost its original local Owner");
                    original.requireCurrent();
                }
                public void close(){original.close();}
            };
        };
    }

    private void requireSloAuditPhase(boolean afterTail){
        if(afterTail?!awaitingSloRecoveryCompletion():!needsSloRecoveryPreparation())
            throw new IllegalStateException("SLO recovery audit crossed its original paused Source phase");
    }

    synchronized void bindSloRecoveryPreparation(TargetStoreBootstrap.SloRecoveryAudit audit,
            TargetTimeFenceMutationFactory.Prepared exactFence,TargetSourceRecoveryInputs originalInput,
            boolean qualificationRequired){
        requireSloAuditPhase(false);requireLease(false);
        if(sloRecoveryOriginal!=audit)throw new IllegalStateException("SLO recovery changed its actual captured original census");
        if(!qualificationRequired&&(hasSlo(audit)||exactFence!=null))
            throw new IllegalStateException("ordinary recovery cannot prepare an E4 domain or Fence");
        if(qualificationRequired&&originalInput==null)
            throw new IllegalStateException("strict SLO recovery requires its original protected input hold");
        requireSloAudit(audit);requireFenceCovers(audit,exactFence);
        requireSloInput(originalInput);
        store.withReadView(audit.reopened().view(),()->null);
        sloRecoveryFence=exactFence;sloRecoveryInput=originalInput;
        sloRecoveryQualificationRequired=qualificationRequired;sloRecoveryBound=true;
    }

    /** Existing signed Fence frames require actual original-cut APPLIED first/POS before skipping a new SDK producer. */
    synchronized void requireSloRecoveryFencePreviouslyApplied(TargetStoreBootstrap.SloRecoveryAudit original,
            TargetTimeFenceMutationFactory.Prepared exactFence){
        requireSloAuditPhase(false);requireLease(false);requireSloAudit(original);
        if(sloRecoveryOriginal!=original)throw new IllegalStateException("existing Fence changed its original actual recovery audit");
        requireFenceCovers(original,Objects.requireNonNull(exactFence,"original already-applied Fence"));
        store.withReadView(original.reopened().view(),()->null);
        original.reopened().backend().guardedRead(Objects.requireNonNull(auditBudgets.get()),reader->{
            com.nereusstream.delay.runtime.TargetSloRecoveryFenceVerifier.requireApplied(reader,original.scope(),original.lineage(),
                    exactFence.mutation(),original.cut());return Boolean.TRUE;
        },sloRecoveryReads(false));
        requireLease(false);requireSloAuditPhase(false);
    }

    /** Exact APPLIED first/POS, not append return or an empty finite prefix, closes the expected Fence dependency. */
    synchronized void requireSloRecoveryFenceApplied(TargetStoreBootstrap.SloRecoveryAudit replayed){
        requireSloAuditPhase(true);requireLease(false);requireSloAudit(replayed);requireFenceCovers(replayed,sloRecoveryFence);
        if(sloRecoveryReplayed!=replayed)throw new IllegalStateException("SLO recovery changed its actual captured post-prefix census");
        store.withReadView(replayed.reopened().view(),()->null);
        if(sloRecoveryFence!=null){
            replayed.reopened().backend().guardedRead(Objects.requireNonNull(auditBudgets.get()),reader->{
                com.nereusstream.delay.runtime.TargetSloRecoveryFenceVerifier.requireApplied(reader,replayed.scope(),replayed.lineage(),
                        sloRecoveryFence.mutation(),replayed.cut());return Boolean.TRUE;
            },sloRecoveryReads(true));
        }
        requireLease(false);requireSloAuditPhase(true);
    }

    /**
     * Runs on the original I/O task. The independent original-history callback is deliberately outside
     * Startup, Coordinator and Store monitors. Its success, including a strict null-Fence result, is
     * the only path that can mint an E4 proof; empty censuses complete only the ordinary recovery phase.
     */
    void completeSloRecovery(TargetStoreBootstrap.SloRecoveryAudit replayed,
            TargetSourceRecoveryInputs originalInput,Runnable guard){
        final TargetStoreBootstrap.SloRecoveryAudit original;
        final TargetTimeFenceMutationFactory.Prepared exactFence;
        final boolean qualificationRequired;
        final OwnerLease originalOwner;
        synchronized(this){
            requireSloRecoveryFenceApplied(replayed);requireSloCompletionInput(originalInput);
            if(sloRecoveryCompleted!=null)throw new IllegalStateException("SLO recovery completion was already authenticated");
            original=Objects.requireNonNull(sloRecoveryOriginal,"original SLO recovery census");
            exactFence=sloRecoveryFence;qualificationRequired=sloRecoveryQualificationRequired;originalOwner=lease;
            if(!qualificationRequired&&(hasSlo(original)||hasSlo(replayed)||exactFence!=null))
                throw new IllegalStateException("ordinary recovery observed an E4 obligation without original qualification");
        }
        final var actualGuard=Objects.requireNonNull(guard,"original SLO recovery I/O guard");
        actualGuard.run();
        if(originalInput!=null){
            originalInput.requireSourceCurrent();actualGuard.run();
            if(!qualificationRequired&&originalInput.requiresSloRecoveryQualification())
                throw new IllegalStateException("original recovery hold now requires strict E4 qualification");
        }
        if(qualificationRequired){
            originalInput.requireSloRecoveryComplete(original,replayed,exactFence,actualGuard);
            originalInput.requireSourceCurrent();
        }
        actualGuard.run();
        synchronized(this){
            requireSloAuditPhase(true);requireLease(false);requireSloCompletionInput(originalInput);
            if(sloRecoveryOriginal!=original||sloRecoveryReplayed!=replayed||sloRecoveryFence!=exactFence
                    ||sloRecoveryQualificationRequired!=qualificationRequired||!originalOwner.sameIdentity(lease))
                throw new IllegalStateException("SLO recovery changed its actual held authentication inputs");
            requireSloAudit(replayed);store.withReadView(replayed.reopened().view(),()->null);
            if(qualificationRequired)sloRecoveryQualification=new SloRecoveryQualification(
                    store,original,replayed,exactFence,originalInput,assignment,originalOwner);
            sloRecoveryCompleted=replayed;
        }
    }

    /** I/O receipt and native cleanup must have exited before Startup binds this already-authenticated phase. */
    synchronized void bindSloRecoveryCompletion(TargetStoreBootstrap.SloRecoveryAudit replayed,
            TargetSourceRecoveryInputs originalInput){
        requireSloAuditPhase(true);requireLease(false);requireSloCompletionInput(originalInput);
        if(sloRecoveryCompleted!=replayed||sloRecoveryReplayed!=replayed
                ||sloRecoveryQualificationRequired!=(sloRecoveryQualification!=null))
            throw new IllegalStateException("SLO recovery has no actual original completion receipt");
        requireSloAudit(replayed);store.withReadView(replayed.reopened().view(),()->null);
        sloRecoveryPhaseComplete=true;
    }

    private void requireSloCompletionInput(TargetSourceRecoveryInputs originalInput){
        if(sloRecoveryInput!=originalInput)throw new IllegalStateException("SLO recovery changed its original input hold");
        requireSloInput(originalInput);
    }

    private void requireSloInput(TargetSourceRecoveryInputs input){
        if(input!=null&&(recoveryOwner==null||!recoveryOwner.equals(input.owner())
                ||input.owner().ownerEpoch()!=lease.ownerEpoch()
                ||!Arrays.equals(input.owner().leaseFencingDigest(),lease.leaseToken())))
            throw new IllegalStateException("SLO recovery input changed its original actual Owner");
    }

    private static boolean hasSlo(TargetStoreBootstrap.SloRecoveryAudit audit){
        return !audit.generations().isEmpty()||!audit.writers().isEmpty();
    }

    private void requireSloAudit(TargetStoreBootstrap.SloRecoveryAudit audit){
        Objects.requireNonNull(audit);
        audit.reopened().backend().requireStore(store);
        if(!initial.root().scope().equals(audit.scope())||!initial.root().identity().equals(audit.root().identity())
                ||!Arrays.equals(initial.root().recoveryLineage(),audit.lineage())
                ||!Arrays.equals(store.appliedShardLogPosition().canonicalBytes(),audit.cut().source().canonicalBytes()))
            throw new IllegalStateException("SLO recovery audit changed its actual Root/lineage/Source cut");
    }

    private static void requireFenceCovers(TargetStoreBootstrap.SloRecoveryAudit audit,TargetTimeFenceMutationFactory.Prepared fence){
        if(fence==null){
            if(!audit.generations().isEmpty()||!audit.writers().isEmpty())
                throw new IllegalStateException("retained or newly replayed SLO writers require complete original range/Fence qualification");
            return;
        }
        if(!audit.scope().shard().equals(fence.body().shard()))throw new IllegalStateException("recovery Fence changed its original Shard");
        for(var writer:audit.writers())if(writer.contract().maximumRetryUntil()>fence.body().closeThrough())
            throw new IllegalStateException("recovery Fence omits a restored writer deadline; observed maxima do not prove the old domain complete");
    }

    /** A real consumed prefix, distinct from the immutable assignment and from old-writer settlement. */
    private boolean replayTailReached(){
        if(replayTailReader==null)return true; // Original component-only recovery keeps its original barrier contract.
        if(!replayTailCaptured)return false;
        final var applied=Objects.requireNonNull(store.appliedShardLogPosition(),"actual Source tail apply cut");
        if(replayTailAppliedAt!=null){
            if(!applied.sameSourceIdentity(replayTailAppliedAt)||applied.compareTo(replayTailAppliedAt)<0
                    ||applied.compareTo(replayTailAppliedAt)==0&&!Arrays.equals(applied.canonicalBytes(),replayTailAppliedAt.canonicalBytes()))
                throw new IllegalStateException("recovery lost its actual consumed tail cut");
            return true;
        }
        if(!replayTailReader.replayTailReached(applied))return false;
        replayTailAppliedAt=applied;return true;
    }

    synchronized void releaseReaderForSourceReactivation(TargetWorkerRecoverySource reader) {
        if (!sourceReadyForReactivation || reader != boundReader || !reader.cleanupComplete()) {
            throw new IllegalStateException("original paused reader has not actually exited");
        }
        reactivationReaderReleased = true;
    }

    synchronized void deferForSourceReactivation() {
        if (turnNumber != 0 || replay != null || boundReader == null
                || !(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.PulsarActivationBarrier)) {
            throw new IllegalStateException("P1 reactivation pause requires its original bound startup reader");
        }
        deferForSourceReactivation = true;
    }

    synchronized boolean sourceReadyForReactivation() {
        if (!sourceReadyForReactivation) return false;
        requireCatalog(); requireLease(false);
        if (lastSourceAction != null && workClasses.state(lastSourceAction).isPresent()) return false;
        if (pending != null || pendingEntry != null || completedRoot == null
                || !assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())||!replayTailReached()) {
            throw new IllegalStateException("Source reactivation pause lost its actual audited cut");
        }
        return true;
    }

    synchronized OwnerLease sourcePreparationLease() {
        if (!sourceReadyForReactivation()) throw new IllegalStateException("Source replay is not paused for reactivation");
        return lease;
    }

    /** Retained read protections only; the caller must separately hold the private frozen Owner/Store receipt. */
    synchronized void requireSourcePreparationProtection() {
        if (!sourceReadyForReactivation || !reactivationReaderReleased || pending != null || pendingEntry != null
                || lastSourceAction != null && workClasses.state(lastSourceAction).isPresent()) {
            throw new IllegalStateException("original Source action/reader has not actually left its pause");
        }
        completionProtection.run();
        if (catalog != null) catalog.validatePublishedRestoreCandidate(manifest);
    }

    synchronized OwnerRecoveryTurn pausedSourceReactivationTurn() {
        requireSourcePreparationProtection();
        return new OwnerRecoveryTurn(List.of(), false, turnNumber);
    }

    synchronized void bindReader(TargetWorkerRecoverySource reader, SourcePosition cut) {
        if (turnNumber != 0 || replay != null || boundReader != null) {
            throw new IllegalStateException("Target reader must bind before recovery begins");
        }
        reader.requireReaderBinding(assignment, cut);
        boundReader = reader;
        boundReaderCut = Objects.requireNonNull(cut);
    }

    /** Same bounded content providers as active Source; this instance is exclusively bound to replay. */
    public synchronized void configureAuthorityPreparation(TargetSourceAuthorityPreparation selected) {
        configureAuthorityPreparation(selected,catalog);
    }
    public synchronized void configureAuthorityPreparation(TargetSourceAuthorityPreparation selected,RecoveryCatalogAuthority originalEvidenceCatalog){
        if (turnNumber != 0 || replay != null || authorityPreparation != null) {
            throw new IllegalStateException("recovery content preparation must be configured before replay");
        }
        authorityPreparation = Objects.requireNonNull(selected);
        evidenceCatalog=originalEvidenceCatalog;
    }

    synchronized void configureScopeResources(TargetScopeResourceManager original) {
        if (turnNumber != 0 || replay != null || scopeResources != null)
            throw new IllegalStateException("Scope manager must bind before original Source recovery starts");
        scopeResources = Objects.requireNonNull(original);
    }

    synchronized void configurePayloadSummaryLegacyPrefix(TargetPayloadSummaryLegacyPrefix original) {
        if (turnNumber != 0 || replay != null || payloadSummaryLegacyPrefix != null)
            throw new IllegalStateException("old payload prefix must bind before the actual recovery Source action");
        payloadSummaryLegacyPrefix = Objects.requireNonNull(original);
    }
    /** Only the actual startup binds limits/physical Owner; each prepared prefix still needs its independent paid loan. */
    synchronized void configureWriterOrigin(com.nereusstream.delay.runtime.TargetSloWriterOrigin.Limits originalLimits,
            com.nereusstream.delay.protocol.OwnerIdentity actualOwner, LongSupplier originalClock) {
        if (turnNumber != 0 || replay != null || writerOriginLimits != null)
            throw new IllegalStateException("writer history preparation must bind before original Source recovery starts");
        Objects.requireNonNull(actualOwner); Objects.requireNonNull(originalLimits); Objects.requireNonNull(originalClock);
        if (actualOwner.ownerEpoch() != lease.ownerEpoch() || !Arrays.equals(actualOwner.leaseFencingDigest(), lease.leaseToken()))
            throw new IllegalArgumentException("writer origin changed its actual recovery Source Owner");
        writerOriginLimits = originalLimits; writerOriginOwner = actualOwner; writerOriginClock = originalClock;
    }

    synchronized void configureMigration(TargetMigrationCompatibilityRuntime original,
            com.nereusstream.delay.protocol.SourcePosition minimum) {
        if (turnNumber != 0 || replay != null || migrationCompatibility != null)
            throw new IllegalStateException("migration compatibility must bind before actual Source recovery");
        migrationCompatibility = Objects.requireNonNull(original);
        if (minimum != null) {
            assignment.activationBarrier().validatePosition(minimum);
            requiredReplayCut = minimum;
        }
    }

    public synchronized void stopAuthorityPreparation() {
        localRecoveryStopped=true;
        if (authorityPreparation != null) authorityPreparation.requestStop();
        if (replay != null) replay.stopAuthorityPreparation();
    }
    public synchronized void retryAuthorityCleanup() {
        if (authorityPreparation != null) authorityPreparation.retryCleanup();
        if (replay != null) replay.retryAuthorityCleanup();
    }
    public synchronized boolean authorityIoExited() { return (authorityPreparation == null || authorityPreparation.ioExited())
            && (replay == null || replay.authorityIoExited()); }
    synchronized boolean nativeReadLoansExited() { return authorityPreparation == null || authorityPreparation.nativeReadLoansExited(); }

    /** Protection rechecks held snapshots at native guards; provider acquisition/full reads remain outside monitors. */
    public TargetOwnerRecoveryCoordinator(ShardStore store, TargetStoreBootstrap.Reopened reopened,
            SourceAssignment assignment, OwnerLease lease, TargetSourceApplyRuntime.Authorities authorities,
            TargetSourceApplyRuntime.Limits limits, SourceReplayCursor<? extends SourceReplayEntry> source,
            RecoveryCatalogAuthority catalog, RecoveryPin pin, Runnable protection, ReplayTurnBudget turnBudget,
            WorkClassExecutionRegistry workClasses, LongSupplier clock, LongSupplier monotonic,
            Supplier<BoundedReadBudget> auditBudgets, TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        this(store, reopened, assignment, lease, authorities, limits, source, catalog, pin, protection,
                turnBudget, workClasses, clock, monotonic, auditBudgets, ledgerLimits, true);
    }

    /** Same-DB restart recovery also replays its accepted barrier and drains unadmitted Claims before exposure. */
    public TargetOwnerRecoveryCoordinator(ShardStore store, TargetStoreBootstrap.Reopened reopened,
            SourceAssignment assignment, OwnerLease lease, TargetSourceApplyRuntime.Authorities authorities,
            TargetSourceApplyRuntime.Limits limits, SourceReplayCursor<? extends SourceReplayEntry> source,
            Runnable protection, ReplayTurnBudget turnBudget, WorkClassExecutionRegistry workClasses,
            LongSupplier clock, LongSupplier monotonic, Supplier<BoundedReadBudget> auditBudgets,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        this(store, reopened, assignment, lease, authorities, limits, source, null, null, protection,
                turnBudget, workClasses, clock, monotonic, auditBudgets, ledgerLimits, false);
    }

    private TargetOwnerRecoveryCoordinator(ShardStore store, TargetStoreBootstrap.Reopened reopened,
            SourceAssignment assignment, OwnerLease lease, TargetSourceApplyRuntime.Authorities authorities,
            TargetSourceApplyRuntime.Limits limits, SourceReplayCursor<? extends SourceReplayEntry> source,
            RecoveryCatalogAuthority catalog, RecoveryPin pin, Runnable protection, ReplayTurnBudget turnBudget,
            WorkClassExecutionRegistry workClasses, LongSupplier clock, LongSupplier monotonic,
            Supplier<BoundedReadBudget> auditBudgets, TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            boolean checkpointRestore) {
        this.store = Objects.requireNonNull(store, "store");
        initial = Objects.requireNonNull(reopened, "reopened");
        initial.backend().requireStore(store);
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.authorities = Objects.requireNonNull(authorities, "authorities");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.source = Objects.requireNonNull(source, "source");
        this.source.requireGuarded();
        this.catalog = checkpointRestore ? Objects.requireNonNull(catalog, "catalog") : null;
        this.pin = checkpointRestore ? Objects.requireNonNull(pin, "pin") : null;
        final var heldProtection = Objects.requireNonNull(protection, "protection");
        completionProtection = heldProtection;
        this.protection = () -> {
            this.source.requireCurrent();
            heldProtection.run();
            this.source.requireCurrent();
        };
        this.turnBudget = Objects.requireNonNull(turnBudget, "turnBudget");
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.monotonic = Objects.requireNonNull(monotonic, "monotonic");
        this.auditBudgets = Objects.requireNonNull(auditBudgets, "auditBudgets");
        this.ledgerLimits = Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        manifest = checkpointRestore ? this.catalog.manifest(this.pin.candidate().checkpointId())
                .orElseThrow(() -> new IllegalStateException("Target recovery candidate is not published")) : null;
        final var base = store.recoveryMetadata().lineageBase();
        if (store.metadata().storeFormatVersion() != 2
                || !store.shardId().equals(assignment.shardId()) || !store.shardId().equals(lease.shardId())
                || lease.context() == null || !Arrays.equals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()) {
            throw new IllegalArgumentException("Target recovery lacks exact Store/assignment/Owner identity");
        }
        if (checkpointRestore) {
            if (base == null || store.recoveryMetadata().installState() == null
                    || pin.owner().ownerEpoch() != lease.ownerEpoch()
                    || !pin.shard().shardId().equals(store.shardId())
                    || !Arrays.equals(base.storeIncarnation(), store.metadata().storeIncarnation())
                    || !Arrays.equals(base.checkpointId(), pin.candidate().checkpointId())
                    || !Arrays.equals(base.manifestSha256(), pin.candidate().manifestSha256())
                    || !Arrays.equals(base.recoveryLineageId(), initial.root().recoveryLineage())
                    || !Arrays.equals(base.recoveryLineageId(), pin.candidate().recoveryLineageId())
                    || !Arrays.equals(manifest.manifestSha256(), pin.candidate().manifestSha256())) {
                throw new IllegalArgumentException("Target recovery lacks exact Store/base/assignment/Owner/Pin identity");
            }
        } else {
            final var install = store.recoveryMetadata().installState();
            if (install != null && (install.phase() == RecoveryInstallPhase.STAGED
                    || install.phase() == RecoveryInstallPhase.INSTALLED)) {
                throw new IllegalArgumentException("an incomplete checkpoint install requires published Catalog/Pin recovery");
            }
        }
        assignment.activationBarrier().validatePosition(store.appliedShardLogPosition());
    }

    /** Install actual authenticated identity and retained replayed Journal rights before starting recovery. */
    public synchronized void configureClaimTakeover(com.nereusstream.delay.protocol.OwnerIdentity owner,
            com.nereusstream.delay.adapter.PulsarAttemptJournal journal, Runnable journalProtection) {
        if (turnNumber != 0 || claimTakeover != null || recoveryOwner != null) {
            throw new IllegalStateException("Claim takeover must be configured before recovery begins");
        }
        final var current = requireLease(true);
        if (owner.ownerEpoch() != current.ownerEpoch()
                || !Arrays.equals(owner.leaseFencingDigest(), current.leaseToken())
                || (journal == null) != (journalProtection == null)) {
            throw new IllegalArgumentException("Claim takeover identity/Journal protection does not match current recovery");
        }
        recoveryOwner = owner;
        recoveryJournal = journal;
        recoveryJournalProtection = journalProtection;
    }

    /** One retained Source action; the original barrier and any privately bound captured prefix must be applied. */
    public synchronized OwnerRecoveryTurn runTurn() {
        final var outcomes = new ArrayList<SourceReplayOutcome>();
        final int turn = Math.incrementExact(turnNumber);
        final long started = monotonic.getAsLong();
        long bytes = 0;
        try {
            if (complete) {
                requireLease(true);
                return new OwnerRecoveryTurn(List.of(), true, turnNumber);
            }
            if (sourceReadyForReactivation) {
                requireCatalog(); requireLease(false);
                return new OwnerRecoveryTurn(List.of(), false, turnNumber);
            }
            requireCatalog();
            while (true) {
                if (pending == null && pendingEntry==null&&assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())) {
                    if(replayTailReader!=null&&!replayTailCaptured){
                        turnNumber=turn;return new OwnerRecoveryTurn(outcomes,false,turn);
                    }
                    if(replayTailReached()){
                        if(replayTailReader!=null&&!sloRecoveryPhaseComplete){turnNumber=turn;return new OwnerRecoveryTurn(outcomes,false,turn);}
                        final boolean finished = finish();
                        complete = finished;
                        turnNumber = turn;
                        return new OwnerRecoveryTurn(outcomes, finished, turn);
                    }
                }
                beginReplay();
                replay.requireRecoveryOwner(clock);
                if (outcomes.size() >= turnBudget.maxRecords() || bytes >= turnBudget.maxCanonicalBytes()
                        || elapsed(started) >= turnBudget.maxElapsedNanos()) {
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn);
                }
                try {
                if (pendingEntry == null) {
                    if (!source.hasNext()) {
                        if(assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())&&replayTailReached()){
                            if(replayTailReader!=null&&!sloRecoveryPhaseComplete){turnNumber=turn;return new OwnerRecoveryTurn(outcomes,false,turn);}
                            final boolean finished=finish();complete=finished;turnNumber=turn;
                            return new OwnerRecoveryTurn(outcomes,finished,turn);
                        }
                        throw new SourceReplayGapException("Target Source ended before its barrier");
                    }
                    pendingEntry = Objects.requireNonNull(source.peek(), "source entry");
                } else if (!sameEntry(pendingEntry, source.peek())) {
                    throw new IllegalStateException("Target recovery retained Source entry changed");
                }
                } catch(TargetWorkerSourceStartup.FrameCapacityUnavailable unavailable) {
                    turnNumber=turn;
                    return new OwnerRecoveryTurn(outcomes,false,turn,false,null,null,unavailable);
                }
                final long charged = Math.addExact(pendingEntry.position().canonicalBytes().length,
                        frame(pendingEntry).length);
                if (charged > turnBudget.maxCanonicalBytes()) {
                    throw new IllegalArgumentException("Target recovery entry exceeds its finite byte budget");
                }
                if (charged > turnBudget.maxCanonicalBytes() - bytes) {
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn);
                }
                if (pending == null) {
                    if (!replay.prepareAuthorities(pendingEntry, clock, true)) {
                        turnNumber = turn; return new OwnerRecoveryTurn(outcomes, false, turn);
                    }
                    pending = sourceApply.submitRecovery(pendingEntry, clock);
                    lastSourceAction = pending.task();
                }
                Throwable dispatchFailure = null;
                try {
                    workClasses.runTurn(new SchedulerBudget(1, pending.task().bytes(),
                            Math.max(1, turnBudget.maxElapsedNanos() - elapsed(started))));
                } catch (RuntimeException | Error failure) {
                    if (pending.outcome().isEmpty()) { throw failure; }
                    dispatchFailure = failure;
                }
                final var applied = pending.outcome().orElse(null);
                if (applied == null) {
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn, true, pending.task());
                }
                final var incomplete = replay.sourceReadIncompleteReason(applied.failure());
                if (incomplete != null) {
                    pending = null;
                    if (dispatchFailure != null) { throwUnchecked(dispatchFailure); }
                    turnNumber = turn;
                    return new OwnerRecoveryTurn(outcomes, false, turn, false, null, incomplete);
                }
                if (replay.isAuthorityPreparationRequired(applied.failure())) {
                    pending = null;
                    if (dispatchFailure != null) throwUnchecked(dispatchFailure);
                    turnNumber = turn; return new OwnerRecoveryTurn(outcomes, false, turn);
                }
                if (applied.failure() != null) { throwUnchecked(applied.failure()); }
                if (!replay.completeAuthorities(pendingEntry)) {
                    turnNumber = turn; return new OwnerRecoveryTurn(outcomes, false, turn);
                }
                try {
                if (!sameEntry(pendingEntry, source.peek())) {
                    throw new IllegalStateException("Target recovery Source changed before cursor advance");
                }
                source.next();
                } catch(TargetWorkerSourceStartup.FrameCapacityUnavailable unavailable) {
                    turnNumber=turn;
                    return new OwnerRecoveryTurn(outcomes,false,turn,false,null,null,unavailable);
                }
                outcomes.add(Objects.requireNonNull(applied.result(), "applied result"));
                pendingEntry = null;
                pending = null;
                bytes = Math.addExact(bytes, charged);
                if (dispatchFailure != null) { throwUnchecked(dispatchFailure); }
            }
        } catch (ReadIncompleteException incomplete) {
            turnNumber = turn;
            return new OwnerRecoveryTurn(outcomes, false, turn, false, null, incomplete.reason());
        } catch (RuntimeException | Error failure) {
            if (replay != null && replay.isReadIncomplete(failure)) {
                turnNumber = turn;
                return new OwnerRecoveryTurn(outcomes, false, turn, false, null, replay.sourceReadIncompleteReason(failure));
            }
            localRecoveryStopped=true;
            if (replay != null) { replay.fence(); }
            throw failure;
        }
    }

    private void beginReplay() {
        if (replay != null) { return; }
        final var current = requireLease(false);
        if (current.state() != ShardLifecycleState.CATCHING_UP) {
            lease = authorities.leases().transitionOrRead(current, ShardLifecycleState.CATCHING_UP)
                    .orElseThrow(() -> new IllegalStateException("Target recovery CATCHING_UP CAS was not observed"));
        }
        requireLease(false);
        if (Long.compareUnsigned(store.runtimeMetadata().lastOpenedOwnerEpoch(), lease.ownerEpoch()) > 0) {
            throw new IllegalStateException("Target recovery Store already has a newer Owner epoch");
        }
        protection.run();
        if (store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()) {
            store.recordOpenedOwnerEpoch(lease.ownerEpoch());
        }
        requireLease(false);
        replay = TargetSourceApplyRuntime.recovery(initial, store, assignment, lease, authorities, limits,
                monotonic, protection);
        if(localRecoveryProtection!=null)replay.configureRecoveryLocalProtection(localRecoveryProtection);
        if (writerOriginLimits != null) replay.configureWriterOrigin(writerOriginLimits, writerOriginOwner, writerOriginClock);
        if (migrationCompatibility != null) replay.configureMigration(migrationCompatibility);
        if (payloadSummaryLegacyPrefix != null) replay.configurePayloadSummaryLegacyPrefix(payloadSummaryLegacyPrefix);
        if (boundReader != null) replay.bindRecoveryReader(boundReader, boundReaderCut);
        if (authorityPreparation != null) replay.configureAuthorityPreparation(authorityPreparation, clock,evidenceCatalog);
        if (scopeResources != null) scopeResources.bindSource(replay, clock);
        sourceApply = new SourceApplyWorkClassExecutor(workClasses, replay);
    }

    private boolean finish() {
        if (requiredReplayCut != null) {
            final var actual = Objects.requireNonNull(store.appliedShardLogPosition());
            if (!actual.sameSourceIdentity(requiredReplayCut) || actual.compareTo(requiredReplayCut) < 0
                    || actual.compareTo(requiredReplayCut) == 0 && !Arrays.equals(actual.canonicalBytes(), requiredReplayCut.canonicalBytes()))
                throw new SourceReplayGapException("actual Source recovery has not applied the original captured migration prefix");
        }
        if(replayTailReader!=null&&!sloRecoveryPhaseComplete)throw new IllegalStateException("bounded pre/post recovery census phase must precede Claim takeover/Source freeze");
        if (deferForSourceReactivation) {
            beginReplay();
            requireCatalog();
            completedRoot = reopenAtCurrentCut();
            requireLease(false);
            if (replay != null) replay.fence();
            sourceReadyForReactivation = true;
            return false;
        }
        if (claimTakeover == null) {
            final var current = requireLease(true);
            if (current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) { beginReplay(); }
            requireCatalog();
            final var audited = reopenAtCurrentCut();
            final var install = store.recoveryMetadata().installState();
            if (install != null && install.phase() == RecoveryInstallPhase.INSTALLED) {
                try (var guard = heldGuard()) {
                    store.completeTargetRecovery(audited.view(), store.recoveryMetadata().lineageBase(), guard);
                }
            }
            requireCatalog();
            lease = TargetWorkerOwnerActivation.activate(audited, store, assignment, lease, authorities.leases(), clock);
            requireLease(true);
            if (lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                throw new IllegalStateException("Target recovery activation did not retain ACTIVE Owner");
            }
            // Owner/install metadata writes can invalidate the prior physical audit cut.
            final var activated = reopenAtCurrentCut();
            claimTakeover = new TargetClaimTakeoverCoordinator(store, activated, assignment, recoveryOwner, lease,
                    authorities.leases(), protection, recoveryJournal, recoveryJournalProtection,
                    auditBudgets, ledgerLimits, limits, clock);
        }
        requireCatalog();
        if (!claimTakeover.runTurn()) { return false; }
        completedRoot = claimTakeover.completedRoot();
        requireLease(true);
        if (replay != null) { replay.fence(); }
        return true;
    }

    private TargetStoreBootstrap.Reopened reopenAtCurrentCut() {
        return TargetStoreBootstrap.reopen(store, initial.root().scope(),
                new TargetStoreBackend.WriteLimits(limits.records(), limits.bytes()),
                Objects.requireNonNull(auditBudgets.get(), "recovery audit budget"),
                (metadata, scope) -> heldGuard(), ledgerLimits);
    }

    private TargetStoreBackend.CommitGuard heldGuard() {
        protection.run();
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                protection.run();
                requireLease(true);
                if (!assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())||!replayTailReached()) {
                    throw new IllegalStateException("Target recovery completion lost its Source barrier");
                }
            }

            @Override
            public void close() {}
        };
    }

    private OwnerLease requireLease(boolean allowActive) {
        protection.run();
        final long now = clock.getAsLong();
        final var current = authorities.leases().current(store.shardId())
                .orElseThrow(() -> new IllegalStateException("Target recovery Owner disappeared"));
        if (now < 0 || now < lastNow || !lease.sameIdentity(current) || !current.validAt(now)
                || current.state() != ShardLifecycleState.ACQUIRING && current.state() != ShardLifecycleState.RESTORING
                        && current.state() != ShardLifecycleState.CATCHING_UP
                        && !(allowActive && current.state() == ShardLifecycleState.ACTIVE_FOR_COMMANDS)) {
            throw new IllegalStateException("Target recovery Owner/session/time changed");
        }
        lastNow = now;
        lease = current;
        return current;
    }

    private void requireCatalog() {
        final var held = complete || reactivationReaderReleased ? completionProtection : protection;
        held.run();
        if (catalog == null) { return; }
        catalog.validatePublishedRestoreCandidate(manifest);
        if (!catalog.activeRecoveryPin()
                .orElseThrow(() -> new IllegalStateException("Target recovery Pin is no longer active")).equals(pin)) {
            throw new IllegalStateException("Target recovery exact RecoveryPin changed");
        }
        held.run();
    }

    private long elapsed(long started) {
        final long now = monotonic.getAsLong();
        if (now < started) { throw new IllegalStateException("Target recovery monotonic clock regressed"); }
        return Math.subtractExact(now, started);
    }

    public synchronized OwnerLease activeLease() {
        if (!complete) { throw new IllegalStateException("Target recovery has not completed"); }
        return requireLease(true);
    }

    /** Exact final audit after all local Claim ordinals, ready for active Source composition. */
    public synchronized TargetStoreBootstrap.Reopened completedRoot() {
        if (!complete || completedRoot == null) { throw new IllegalStateException("Target recovery is incomplete"); }
        requireLease(true);
        store.withReadView(completedRoot.view(), () -> null);
        return completedRoot;
    }

    /** Full recovery/Claim completion, distinct from the first paused Source reactivation cut. */
    synchronized boolean readyForJournalHandoff() {
        if (!complete || sourceReadyForReactivation || pending != null || pendingEntry != null
                || lastSourceAction != null && workClasses.state(lastSourceAction).isPresent()) return false;
        completedRoot(); requireCatalog();
        return authorityIoExited();
    }

    /** Builds an active Source runtime only from the final, still-current recovery audit and held authorities. */
    public synchronized TargetSourceApplyRuntime activeRuntime() {
        // The legacy branch keeps its original reader-bound successor and therefore still requires that reader.
        source.requireCurrent();
        return activeRuntime(authorities.successor());
    }

    /** Active consumption uses its own adapter proof; a closed recovery reader cannot authorize later Source delivery. */
    public synchronized TargetSourceApplyRuntime activeRuntime(SourceReplaySuccessor activeSuccessor) {
        Objects.requireNonNull(activeSuccessor, "actual active Source successor");
        final var root = completedRoot();
        requireCatalog();
        final var active = activeLease();
        if (replay != null && !replay.authorityIoExited())
            throw new IllegalStateException("original recovery writer/history/Source loans remain owned before active handoff");
        final TargetSourceApplyRuntime runtime;
        if (authorityPreparation == null) {
            runtime = new TargetSourceApplyRuntime(root, store, assignment, active,
                    authorities.withSuccessor(activeSuccessor), limits, monotonic);
        } else {
            if (!authorityPreparation.ioExited()) throw new IllegalStateException("recovery content I/O is still live");
            final var preparation = authorityPreparation.fork();
            final var selected = preparation.authorities(authorities.leases(), activeSuccessor,
                    authorities.duplicateWrites(), authorities.reads());
            runtime = new TargetSourceApplyRuntime(root, store, assignment, active, selected, limits, monotonic);
            runtime.configureAuthorityPreparation(preparation, clock,evidenceCatalog);
        }
        if(sloRecoveryQualification!=null){
            if(!sloRecoveryPhaseComplete)throw new IllegalStateException("E4 qualification has not completed its original I/O exit");
            sloRecoveryQualification.bindRuntimeRoot(root);
        }
        runtime.bindRecoveredSloQualification(sloRecoveryQualification);
        if (writerOriginLimits != null) runtime.configureWriterOrigin(writerOriginLimits,
                new com.nereusstream.delay.protocol.OwnerIdentity(writerOriginOwner.deploymentId(), writerOriginOwner.workerRunId(),
                        active.ownerEpoch(), active.leaseToken()), writerOriginClock);
        if (migrationCompatibility != null) runtime.configureMigration(migrationCompatibility);
        if (scopeResources != null) scopeResources.bindSource(runtime, clock);
        return runtime;
    }

    private static boolean sameEntry(SourceReplayEntry first, SourceReplayEntry second) {
        return second != null && Arrays.equals(first.position().canonicalBytes(), second.position().canonicalBytes())
                && Arrays.equals(frame(first), frame(second))
                && Arrays.equals(first.guardAttestationDigest(), second.guardAttestationDigest())
                && Objects.equals(first.sourceConnectionGeneration(), second.sourceConnectionGeneration());
    }

    private static byte[] frame(SourceReplayEntry entry) {
        return entry instanceof SourceReplayRecord command ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) entry).mutation().encodeFrame();
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException runtime) { throw runtime; }
        if (failure instanceof Error error) { throw error; }
        throw new IllegalStateException("Target recovery failed", failure);
    }
}
