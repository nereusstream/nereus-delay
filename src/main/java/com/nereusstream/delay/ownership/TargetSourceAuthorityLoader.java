package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Repository collector for the original deployment-owned, protected content/history inputs. */
public final class TargetSourceAuthorityLoader implements TargetSourceAuthorityPreparation.Loader {
    /**
     * Loads and authenticates original issuer/role/Route/history/materialization data at this Source point.
     * Except the Control keys/registration explicitly collected below, returned authority callbacks must
     * consume only this hold's immutable local snapshots. No current-role substitution or remote I/O at commit.
     */
    @FunctionalInterface public interface Provider {
        HeldInputs load(SourceReplayEntry entry, TargetSourceAuthorityPreparation.Family family, Runnable guard);
        default HeldInputs load(SourceReplayEntry entry,TargetSourceAuthorityPreparation.Family family,Runnable guard,
                TargetSourceApplyRuntime.ConfirmationRead source){return load(entry,family,guard);}
        default HeldInputs load(SourceReplayEntry entry,TargetSourceAuthorityPreparation.Family family,Runnable guard,
                TargetSourceApplyRuntime.ConfirmationRead source,TargetSourceApplyRuntime.ReclaimRead reclaim){return load(entry,family,guard,source);}
        default HeldInputs loadWriterPrelude(SourceReplayEntry entry,TargetSloWriterOrigin.PreparationRequired request,Runnable guard,
                TargetSourceApplyRuntime.ConfirmationRead source,TargetSourceApplyRuntime.ReclaimRead reclaim){
            return load(entry,TargetSourceAuthorityPreparation.Family.SCOPE_FEES,guard,source,reclaim);
        }
    }

    public interface HeldInputs extends AutoCloseable {
        SourceReplayEntry entry();
        Inputs inputs();
        long retainedBytes();
        /**
         * Original registry/exposure/Native allocation/accounting/retention material acquired by this load.
         * All receipt/frame payload bytes are included in retainedBytes; methods consume local held data only.
         * requireCurrent keeps these protections valid through the same Source commit.
         */
        default TargetScopeFeeAuthority scopeFees() { return TargetScopeFeeAuthority.UNAVAILABLE; }
        /** The actual whole-flight Root/Owner/Source/Pin/history/payment admission, never a limit inferred from this frame. */
        default TargetRetirementPreparation.Protection retirementPreparationProtection(TargetSourceApplyRuntime.ReclaimRead source) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original retirement cumulative admission/retention contract unavailable");
        }
        /** Transfers a separately paid original Root/Owner/history/payment/retention loan to the actual writer kernel.
         * Closing this entry must not close the transferred loan; requestRelease/released retain actual cleanup debt. */
        default TargetSloWriterOrigin.Protection detachWriterOrigin(TargetSloWriterOrigin.PreparationRequired request) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original independent writer history/retention subloan unavailable");
        }
        /** Materializes the original historical approval only after the actual complete same-view Native proof exists. */
        default TargetEvidenceRetirementStore.ConfirmationApproval confirmationAfterPreparation(PendingConfirmation input,
                TargetSourceApplyRuntime.ConfirmationRead source,TargetRetirementPreparation.Proof prepared) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original historical confirmation approval material unavailable");
        }
        default ArchiveTailInputs archiveTailInputs(TargetMigrationArchivePreparation.TailPreparationRequired request) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original archive opening/paid receiver loan unavailable");
        }
        /**
         * Local held checks only. RECLAIM must retain the original Source/Worker/Store and complete
         * Catalog/Floor/Pin/image/future-reader admission protections through the Native commit.
         * Two observations, an empty caller inventory, or signatures alone are not that held protocol.
         * PARENT_CONTROL_CREDIT independently retains the original tenant, placement, physical Source,
         * exclusive funding slice and DataDefinition/Native grant protections acquired during Provider.load;
         * its capacity callbacks use only those bounded held snapshots, never Scope membership or remote I/O.
         * SCOPE_OBLIGATION separately retains original registry/physical Source/retention rights and paid-parent
         * funding protection; its actual Native parent/view/complete plan is a read set, not authorization.
         */
        void requireCurrent();
        /**
         * Actual original read/replay/export and writer/alias admission hold for this private Source context.
         * The original Provider.load RECLAIM acquisition owns this separately from any GC preparation loan;
         * bind the exact signed body11/Source point, actual Native Store/current Owner/incarnation, original
         * publication record/namespace, frozen Message values, policy/SourceFacts/cut and real Pin/images.
         * Its purpose is PROTECT_NATIVE_INLINE_PAYLOAD_RECLAIM, separate from Action3 or Catalog object rights.
         * Must keep the full qualified Floor closure valid for the exact terminal values through commit;
         * no untracked dependency may arise after that safe cut. Old images must be excluded from service
         * or actually replay through the closed cut before service; previous read loans must have exited
         * or be carried by separately charged independent copies. Unknown readers fail acquisition/recheck.
         * Local actual owner/epoch/loan checks only, never remote I/O. Original close retains failed cleanup.
         */
        default void requireInlinePayloadIndependent(TargetSourceApplyRuntime.ReclaimRead source){
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original Source hold has no inline payload reader/admission protection");
        }
        /** This Source load independently retains the actual Control/Registry lifecycle decision and exact owner/epoch,
         * complete ledgers including live zero-fee references, old writers/late I/O and all reader/Pin protections. */
        default void requireIncarnationIndependent(TargetSourceApplyRuntime.ReclaimRead source){
            throw new TargetSourceAuthorityPreparation.InputUnavailable("actual Source incarnation lifecycle protection unavailable");
        }
        /** This RECLAIM load owns a separate actual record/query/Source/Pin admission loan through the exact Native commit. */
        default void requireNativeRetainedIndependent(TargetSourceApplyRuntime.ReclaimRead source){
            throw new TargetSourceAuthorityPreparation.InputUnavailable("actual Native record/query/recovery admission protection unavailable");
        }
        /**
         * This Source load independently owns the exact Native OBJECT read/replay/upload/commit/Pin loan.
         * Intent must close future use durably; confirmation requires the actual matching hot S3 observation
         * or independently pinned provider operation history, including exact owner/proof/intent and response hashes.
         * Acquire/verify remotely outside locks; this recheck is local and remains held through commit/UNKNOWN.
         * Neither credential/Profile flags nor serialized receipt hashes alone prove deletion or no resurrection.
         */
        default void requireUncommittedObjectIndependent(TargetSourceApplyRuntime.ReclaimRead source){
            throw new IllegalStateException("Source lacks original uncommitted reader/writer/provider admission");
        }
        default void requireUncommittedObjectHistoricalConfirmation(TargetSourceApplyRuntime.ReclaimRead source){
            throw new IllegalStateException("Source lacks original complete closed upload/provider terminal history");
        }
        default void requireObjectPayloadIndependent(TargetSourceApplyRuntime.ReclaimRead source){
            throw new TargetSourceAuthorityPreparation.InputUnavailable("actual Native OBJECT/provider admission unavailable");
        }
        /** Independently authenticate an earlier exact provider operation when no original hot observation survives. */
        default void requireObjectPayloadHistoricalConfirmation(TargetSourceApplyRuntime.ReclaimRead source){
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original OBJECT provider operation history unavailable");
        }
        /** Original hold release; a failed close must retain the original release state for retry. */
        @Override void close();
    }

    public sealed interface Inputs permits Command, Grant, Fence, Expiry, Close, Membership, NativePolicy, Admission, Outcome, Channel, Retirement, Confirmation, PendingConfirmation, Activation, Reclaim, SloMode, SloEvent, SloWriter, ParentCredit, ScopeObligation, ScopeFees, Migration, PayloadSummary {}
    public record ArchiveTailInputs(TargetMigrationArchivePreparation.Held opening,
            com.nereusstream.delay.store.TargetMigrationArchiveCustody receiver) {
        public ArchiveTailInputs { Objects.requireNonNull(opening);Objects.requireNonNull(receiver); }
    }
    public record PayloadSummary(PreparedControlOperation prepared, TargetPayloadSummaryInitializationStore.Authority authority,
            TargetStoreBackend.CommitAuthority commit) implements Inputs {
        public PayloadSummary { Objects.requireNonNull(prepared); Objects.requireNonNull(authority); Objects.requireNonNull(commit); }
    }
    public record Migration(PreparedControlOperation prepared,
            com.nereusstream.delay.runtime.TargetMigrationStore.Authority authority,
            com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.SettlementAuthority settlement,
            com.nereusstream.delay.runtime.TargetMigrationImporter.Limits importLimits,
            TargetStoreBackend.CommitAuthority commit,
            com.nereusstream.delay.runtime.TargetMigrationRetirementStore.OriginalInputs retirement,
            TargetSourceApplyRuntime.MigrationArchiveInputs archive) implements Inputs {
        public Migration {
            Objects.requireNonNull(prepared); Objects.requireNonNull(authority);
            Objects.requireNonNull(importLimits); Objects.requireNonNull(commit);
            if (prepared.request().branch() instanceof com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest
                    || prepared.request().branch() instanceof com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest)
                Objects.requireNonNull(retirement, "actual original old-domain retirement protections");
            else if(prepared.request().branch() instanceof com.nereusstream.delay.protocol.TargetMigrationArchiveRequest)
                Objects.requireNonNull(archive,"actual original archive copy/takeover materials");
            else Objects.requireNonNull(settlement);
        }
        public Migration(PreparedControlOperation prepared,
                com.nereusstream.delay.runtime.TargetMigrationStore.Authority authority,
                com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.SettlementAuthority settlement,
                com.nereusstream.delay.runtime.TargetMigrationImporter.Limits importLimits,
                TargetStoreBackend.CommitAuthority commit,
                com.nereusstream.delay.runtime.TargetMigrationRetirementStore.OriginalInputs retirement) {
            this(prepared,authority,settlement,importLimits,commit,retirement,null);
        }
        public Migration(PreparedControlOperation prepared,
                com.nereusstream.delay.runtime.TargetMigrationStore.Authority authority,
                com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.SettlementAuthority settlement,
                com.nereusstream.delay.runtime.TargetMigrationImporter.Limits importLimits,
                TargetStoreBackend.CommitAuthority commit) {
            this(prepared, authority, settlement, importLimits, commit, null, null);
        }
    }
    /** Payment of this exact Source occurrence only; it grants no current business role or write authority. */
    public record ScopeFees(TargetScopeFeeAuthority fees) implements Inputs {
        public ScopeFees { Objects.requireNonNull(fees); }
    }
    public record ParentCredit(PreparedControlOperation prepared, TargetParentControlCreditVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) implements Inputs {
        public ParentCredit { Objects.requireNonNull(prepared); Objects.requireNonNull(authority); Objects.requireNonNull(commit); }
    }
    public record ScopeObligation(PreparedControlOperation prepared, TargetScopeControlObligationVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) implements Inputs {
        public ScopeObligation { Objects.requireNonNull(prepared); Objects.requireNonNull(authority); Objects.requireNonNull(commit); }
    }
    public record SloWriter(PreparedControlOperation prepared,com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.Authority authority) implements Inputs {}
    public record SloEvent(com.nereusstream.delay.runtime.TargetSloObservationEventStore.Authority authority) implements Inputs {}
    public record SloMode(PreparedControlOperation prepared, TargetSloObservationVerifier.Authority authority) implements Inputs {}
    public record Activation(PreparedControlOperation prepared,TargetProtocolActivationStore.Authority authority) implements Inputs {}
    public record Command(TargetCommandStore.Policy policy, TargetCommandStore.CancellationControls cancellations,
            TargetReservationControls.Authority reservations, TargetCommandStore.Schedules schedules,
            TargetCommandStore.PayloadProofControls payloadProofs, TargetSloObservationSamples.Authority samples) implements Inputs {
        public Command(TargetCommandStore.Policy policy,TargetCommandStore.CancellationControls cancellations,
                TargetReservationControls.Authority reservations,TargetCommandStore.Schedules schedules,TargetCommandStore.PayloadProofControls payloadProofs){
            this(policy,cancellations,reservations,schedules,payloadProofs,TargetSloObservationSamples.UNAVAILABLE);
        }
        public Command { Objects.requireNonNull(samples); }
    }
    public record Grant(PreparedControlOperation prepared, TargetQuotaGrantControlVerifier.Authority authority,
            TargetQuotaCapacitySnapshot.Provider capacity, TargetQuotaCapacitySnapshot.Limits capacityLimits) implements Inputs {}
    public record Fence(TargetTimeFenceVerifier.Authority authority) implements Inputs {}
    public record Expiry(TargetExpireGenerationVerifier.Authority authority) implements Inputs {}
    public record Close(PreparedControlOperation prepared, TargetCloseVerifier.Authority authority) implements Inputs {}
    public record Membership(PreparedControlOperation prepared, CanonicalTargetPartition physical,
            TargetMembershipControlVerifier.Authority authority) implements Inputs {}
    public record NativePolicy(PreparedControlOperation prepared, TargetNativePolicyControlVerifier.Authority authority) implements Inputs {}
    public record Admission(TargetPublishAdmissionVerifier.Authority authority,TargetSloObservationSamples.Authority samples) implements Inputs {
        public Admission(TargetPublishAdmissionVerifier.Authority authority){this(authority,TargetSloObservationSamples.UNAVAILABLE);}
        public Admission{Objects.requireNonNull(samples);}
    }
    public record Outcome(TargetPublishOutcomeVerifier.Authority authority) implements Inputs {}
    public record Channel(PreparedControlOperation prepared, TargetChannelControlVerifier.Authority authority) implements Inputs {}
    /** Provider-approved original Source purpose/Root/capacity and actual Catalog copy; null approval is a proved denial. */
    public record Retirement(TargetQuotaScope scope, TargetEvidenceRetirementStore.Approval approval,
            TargetEvidenceRetirementStore.SourceHistory history) implements Inputs {
        public Retirement(TargetQuotaScope scope,TargetEvidenceRetirementStore.Approval approval){this(scope,approval,null);}
        public Retirement { Objects.requireNonNull(scope); if (scope.target() != null) throw new IllegalArgumentException("retirement needs Source scope"); }
    }
    public record Confirmation(TargetQuotaScope scope,TargetEvidenceRetirementStore.ConfirmationApproval approval,
            TargetEvidenceRetirementStore.SourceHistory history) implements Inputs {
        public Confirmation{Objects.requireNonNull(scope);Objects.requireNonNull(history);if(scope.target()!=null)throw new IllegalArgumentException("confirmation requires original Source scope");}
    }
    /** Captured original history/hold without a prematurely read Native intent or ready approval. */
    public static final class PendingConfirmation implements Inputs {
        private final TargetQuotaScope scope;
        private final TargetEvidenceRetirementStore.SourceHistory history;
        private PendingConfirmation(TargetQuotaScope scope,TargetEvidenceRetirementStore.SourceHistory history) {
            this.scope=Objects.requireNonNull(scope);this.history=Objects.requireNonNull(history);
            if(scope.target()!=null)throw new IllegalArgumentException("historical confirmation requires original Source scope");
        }
        public TargetQuotaScope scope(){return scope;}
        public TargetEvidenceRetirementStore.SourceHistory history(){return history;}
    }
    public static PendingConfirmation pendingHistoricalConfirmation(TargetQuotaScope scope,TargetEvidenceRetirementStore.SourceHistory history) {
        return new PendingConfirmation(scope,history);
    }

    /** The original hold protects actual Catalog/Pin/future-reader admission and complete immutable images through commit. */
    public record Reclaim(TargetQuotaScope scope,TargetResourceReclaimStore.SourceHistory history,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> recoveryImages,
            com.nereusstream.delay.adapter.PayloadObjectDelete.Observation objectDeletion,
            com.nereusstream.delay.adapter.PayloadUncommittedDelete.Observation uncommittedDeletion) implements Inputs {
        public Reclaim(TargetQuotaScope scope,TargetResourceReclaimStore.SourceHistory history,
                java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
                com.nereusstream.delay.adapter.PayloadObjectDelete.Observation observation){this(scope,history,images,observation,null);}
        public Reclaim(TargetQuotaScope scope,TargetResourceReclaimStore.SourceHistory history,
                java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images){this(scope,history,images,null);}
        public Reclaim{Objects.requireNonNull(scope);Objects.requireNonNull(history);recoveryImages=java.util.List.copyOf(recoveryImages);
            if(scope.target()!=null)throw new IllegalArgumentException("reclaim requires original Source scope");}
    }

    private final Provider provider;
    private final TargetStoreBackend.CommitAuthority originalCommit;
    private final long maximumBytes;

    public TargetSourceAuthorityLoader(Provider provider, TargetStoreBackend.CommitAuthority originalCommit, long maximumBytes) {
        this.provider = Objects.requireNonNull(provider, "original content/history provider");
        this.originalCommit = Objects.requireNonNull(originalCommit, "root's actual physical guard");
        if (maximumBytes < 1 || maximumBytes == Long.MAX_VALUE) throw new IllegalArgumentException("finite content bytes required");
        this.maximumBytes = maximumBytes;
    }

    @Override public Set<TargetSourceAuthorityPreparation.Family> families() {
        return Set.copyOf(EnumSet.allOf(TargetSourceAuthorityPreparation.Family.class));
    }
    @Override public TargetSourceAuthorityPreparation.Held loadWriterPrelude(SourceReplayEntry entry,
            TargetSloWriterOrigin.PreparationRequired request,Runnable guard,TargetSourceApplyRuntime.ConfirmationRead source,
            TargetSourceApplyRuntime.ReclaimRead reclaim){
        final var prelude=new WriterPrelude(entry);
        try{
            guard.run();
            final var original=Objects.requireNonNull(provider.loadWriterPrelude(entry,request,guard,source,reclaim),"original fee-only writer hold");
            prelude.original=new Held(entry,original);
            if(!SourceApplyCoordinator.sameEntry(entry,original.entry())||!(original.inputs() instanceof ScopeFees)
                    ||original.retainedBytes()<0||original.retainedBytes()>maximumBytes)
                throw new IllegalStateException("writer prelude changed its exact fee-only Source input or reservation");
            original.requireCurrent();guard.run();return prelude;
        }catch(RuntimeException|Error failure){
            if(failure instanceof CheckpointInputFuture.Cleanup cleanup)prelude.pending=cleanup;
            try{prelude.close();}catch(RuntimeException|Error cleanup){
                final var retained=new TargetSourceAuthorityPreparation.CleanupFailure(prelude,failure);
                if(cleanup!=failure)retained.addSuppressed(cleanup);throw retained;
            }
            throw failure;
        }
    }

    @Override public TargetSourceAuthorityPreparation.Held load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family, Runnable guard) {
        return load(entry,family,guard,null);
    }
    @Override public TargetSourceAuthorityPreparation.Held load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family,Runnable guard,TargetSourceApplyRuntime.ConfirmationRead source){
        return load(entry,family,guard,source,null);
    }
    @Override public TargetSourceAuthorityPreparation.Held load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family,Runnable guard,TargetSourceApplyRuntime.ConfirmationRead source,
            TargetSourceApplyRuntime.ReclaimRead reclaim){
        guard.run();
        var original = Objects.requireNonNull(provider.load(entry, family, guard,source,reclaim), "protected original inputs");
        var held = new Held(entry, original);
        try {
            guard.run(); original.requireCurrent();
            if (!SourceApplyCoordinator.sameEntry(entry, original.entry()) || original.retainedBytes() < 0
                    || original.retainedBytes() > maximumBytes) {
                throw new IllegalStateException("content provider changes original frame/proof or exceeds its byte bound");
            }
            final Inputs inputs = Objects.requireNonNull(original.inputs(), "typed original content");
            if (family(inputs) != family) throw new IllegalStateException("original Source content family differs");
            held.scopeFees = scopeFeeHold(inputs instanceof ScopeFees fees ? fees.fees() : original.scopeFees(), held);
            held.control = collect(entry, inputs, held, guard,source,reclaim);
            original.requireCurrent(); guard.run();
            if (held.bytes > maximumBytes) throw new IllegalStateException("complete content snapshot exceeds byte bound");
            return held;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof CheckpointInputFuture.Cleanup pending) held.pendingCapacityCleanup = pending;
            try { held.close(); }
            catch (RuntimeException | Error cleanup) {
                var retained = new TargetSourceAuthorityPreparation.CleanupFailure(held, failure);
                if (cleanup != failure) retained.addSuppressed(cleanup);
                throw retained;
            }
            throw failure;
        }
    }

    private Object collect(SourceReplayEntry entry, Inputs inputs, Held held, Runnable guard,
            TargetSourceApplyRuntime.ConfirmationRead confirmation,TargetSourceApplyRuntime.ReclaimRead reclaim) {
        var commit = held.commit(originalCommit);
        return switch (inputs) {
            case Migration value -> {
                final var a = value.authority();
                final var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                final var original = (SourceReplayMutation) entry;
                final var body = com.nereusstream.delay.protocol.TargetMigrationControlBody.decode(original.mutation().canonicalBody());
                final var retained = Objects.requireNonNull(a.inputs().authenticate(value.prepared(), original.mutation(),
                        entry.position(), body.request()), "original migration settlement protections");
                retained.run(); held.requireCurrent(); guard.run();
                final var captured = new com.nereusstream.delay.runtime.TargetMigrationStore.Authority(frozen,
                        (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof(),
                        (prepared, input, source, request) -> {
                            held.requireCurrent(); requireSource(entry, source);
                            if (!Arrays.equals(prepared.canonicalBytes(), value.prepared().canonicalBytes())
                                    || !Arrays.equals(input.canonicalEnvelope(), original.mutation().canonicalEnvelope())
                                    || !Arrays.equals(request.canonicalBytes(), body.request().canonicalBytes()))
                                throw new IllegalStateException("migration Source hold changed its original exact CONTROL input");
                            retained.run(); held.requireCurrent(); return () -> { held.requireCurrent(); retained.run(); };
                        });
                yield new TargetSourceApplyRuntime.MigrationControl(value.prepared(), captured,
                        (prior, request, actualSource) -> {
                            held.requireCurrent(); requireSource(entry, actualSource.source());
                            value.settlement().requireAuthorized(prior, request, actualSource); retained.run(); held.requireCurrent();
                        }, value.importLimits(), held.commit(value.commit()), retirementHold(value.retirement(), held, original),
                        archiveHold(value.archive(),value.prepared(),held,original));
            }
            case Command value -> {
                yield new TargetSourceApplyRuntime.CommandControl(value.policy(),value.cancellations(),value.reservations(),value.schedules(),value.payloadProofs(),sampleHold(value.samples(),held),commit);
            }
            case Fence value -> new TargetSourceApplyRuntime.FenceControl(value.authority(), commit);
            case Expiry value -> new TargetSourceApplyRuntime.ExpiryControl(value.authority(), commit);
            case Admission value -> new TargetSourceApplyRuntime.AdmissionControl(value.authority(),sampleHold(value.samples(),held),commit);
            case Outcome value -> new TargetSourceApplyRuntime.OutcomeControl(value.authority(), commit);
            case ScopeFees value -> new TargetSourceApplyRuntime.ScopeFeeControl(held.scopeFees());
            case PayloadSummary value -> {
                final var original = (SourceReplayMutation) entry;
                final var a = value.authority();
                final var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.PayloadSummaryControl(value.prepared(),
                        new TargetPayloadSummaryInitializationStore.Authority(frozen,
                                (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof(),
                                (prepared, input, source, request) -> {
                                    held.requireCurrent(); requireSource(entry, source);
                                    if (!Arrays.equals(prepared.canonicalBytes(), value.prepared().canonicalBytes())
                                            || !Arrays.equals(input.canonicalEnvelope(), original.mutation().canonicalEnvelope()))
                                        throw new IllegalStateException("summary hold changed its original Prepared/Source frame");
                                    final var fold = Objects.requireNonNull(a.inputs().requireActual(prepared, input, source, request));
                                    fold.requireRequest(request); held.requireCurrent(); return fold;
                                }), held.commit(value.commit()));
            }
            case Retirement value -> retirement(entry, value, held, commit, guard,reclaim);
            case Confirmation value -> confirmation(entry,value,held,commit,guard,reclaim);
            case PendingConfirmation value -> confirmation(entry,new Confirmation(value.scope(),null,value.history()),held,
                    commit,guard,reclaim,value,confirmation);
            case Reclaim value -> reclaim(entry,value,held,commit,guard,reclaim);
            case Activation value -> {
                final var original=(SourceReplayMutation)entry;final var body=TargetProtocolActivationBody.decode(original.mutation().canonicalBody());
                final var a=value.authority();final var frozen=freeze(value.prepared(),entry,a.registrations(),v->a.keys().resolve(v,entry.position()),held,guard);
                final var captured=Objects.requireNonNull(a.inputs().capture(value.prepared(),original.mutation(),entry.position(),body),"original Target activation snapshot");
                captured.current().run();held.requireCurrent();guard.run();
                held.bytes=Math.addExact(held.bytes,captured.retainedBytes());
                if(held.bytes>maximumBytes)throw new IllegalStateException("complete Target activation inputs exceed Source snapshot bound");
                final var retained=new TargetProtocolActivationStore.Inputs(captured.readers(),captured.reset(),captured.resetIssuer(),()->{
                    held.requireCurrent();captured.current().run();held.requireCurrent();
                },captured.rootEvidence(),captured.rootIssuer());
                final var authority=new TargetProtocolActivationStore.Authority(frozen,(v,s)->frozen.key(v,s),a.actor(),a.scopeProof(),(prepared,marker,source,actual)->{
                    held.requireCurrent();requireSource(entry,source);
                    if(!Arrays.equals(prepared.canonicalBytes(),value.prepared().canonicalBytes())||!marker.equals(original.mutation())
                            ||!Arrays.equals(actual.canonicalBytes(),body.canonicalBytes()))throw new IllegalStateException("Target activation snapshot changed original marker");
                    retained.current().run();return retained;
                });
                yield new TargetSourceApplyRuntime.ProtocolActivationControl(value.prepared(),authority,commit);
            }
            case Grant value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve((int) version, entry.position()), held, guard);
                var captured = new TargetQuotaGrantControlVerifier.Authority(frozen, (version, source) -> frozen.key(version, source),
                        a.routes(), a.capacity(), a.actor(), a.scopeProof());
                held.capacity = TargetQuotaCapacitySnapshot.collect(value.capacity(), (SourceReplayMutation) entry, value.capacityLimits());
                // Retain the concrete collector's actual canonical-input accounting, not its configured maximum.
                held.bytes = Math.addExact(held.bytes, held.capacity.retainedBytes());
                yield held.capacity.grantControl(value.prepared(), captured, commit);
            }
            case Close value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve((int) version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.CloseControl(value.prepared(), new TargetCloseVerifier.Authority(
                        frozen, (version, source) -> frozen.key(version, source), a.coverage(), a.actor(), a.scopeProof()), commit);
            }
            case Membership value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve((int) version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.MembershipControl(value.prepared(), value.physical(),
                        new TargetMembershipControlVerifier.Authority(frozen, a.policies(),
                                (version, source) -> frozen.key(version, source), a.profiles(), a.actor(), a.scopeProof()), commit);
            }
            case NativePolicy value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.NativePolicyControl(value.prepared(), new TargetNativePolicyControlVerifier.Authority(
                        frozen, (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof()), commit);
            }
            case Channel value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.ChannelControl(value.prepared(), new TargetChannelControlVerifier.Authority(
                        frozen, (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof(), a.materialization(),
                        a.activatedChannelSlots(), a.currentOwner()), commit);
            }
            case SloEvent value -> {
                final var original = ((SourceReplayMutation) entry).mutation();
                final var a = value.authority();
                final long version = Integer.toUnsignedLong(original.signingKeyVersion());
                final var key = copyKey(a.keys().resolve(version, entry.position()));
                final var body=TargetSloObservationEventBody.decode(original.canonicalBody());
                final var collectorKey=body.isCustody()?copyKey(a.observation().collectorKey(body.custody(),entry.position())):null;
                guard.run(); held.requireCurrent();
                held.bytes = Math.addExact(held.bytes, Math.addExact(key == null ? 0 : key.getEncoded().length,collectorKey==null?0:collectorKey.getEncoded().length));
                final var observation = new com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority() {
                    public void requireAuthorized(TargetSloObservationGeneration generation, TargetSloObservationSample sample,
                            SystemMutation input, SourcePosition source) {
                        requireCurrent(); requireSource(entry, source);
                        if (!Arrays.equals(input.canonicalEnvelope(), original.canonicalEnvelope())) throw new IllegalStateException("SLO event snapshot changed exact original frame");
                        a.observation().requireAuthorized(generation, sample, input, source); requireCurrent();
                    }
                    public void requireRecoveryGap(TargetSloObservationGeneration generation,TargetSloObservationSample sample,
                            SystemMutation input,SourcePosition source){
                        requireCurrent();requireSource(entry,source);
                        if(!Arrays.equals(input.canonicalEnvelope(),original.canonicalEnvelope()))throw new IllegalStateException("SLO recovery snapshot changed its original frame");
                        a.observation().requireRecoveryGap(generation,sample,input,source);requireCurrent();
                    }
                    public PublicKey collectorKey(TargetSloCustodyReceipt receipt,SourcePosition source){
                        requireCurrent();requireSource(entry,source);
                        if(!body.isCustody()||!Arrays.equals(receipt.canonicalBytes(),body.custody().canonicalBytes()))throw new IllegalStateException("collector key changed exact original receipt");
                        return collectorKey;
                    }
                    public void requireCurrent() { held.requireCurrent(); a.observation().requireCurrent(); held.requireCurrent(); }
                    public TargetScopeFeeAuthority scopeFees() { return held.scopeFees(); }
                };
                yield new TargetSourceApplyRuntime.SloEventControl(
                        new com.nereusstream.delay.runtime.TargetSloObservationEventStore.Authority((actual, source) -> {
                            held.requireCurrent(); requireSource(entry, source);
                            if (actual != version) throw new IllegalStateException("SLO event signing key changed after capture");
                            return key;
                        }, observation), commit);
            }
            case SloWriter value -> {
                final var a=value.authority();
                final var frozen=freeze(value.prepared(),entry,a.registrations(),
                        version->a.keys().resolve(version,entry.position()),held,guard);
                final var material=new com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority(){
                    public void requireAuthorized(TargetSloWriterControlRequest request,TargetSloWriterEpoch retained,SourcePosition source){
                        requireCurrent();a.materialization().requireAuthorized(request,retained,source);requireCurrent();
                    }
                    public void requireFenceAuthorized(SystemMutation originalInput,TargetQuotaMutation fenceAt){
                        requireCurrent();a.materialization().requireFenceAuthorized(originalInput,fenceAt);requireCurrent();
                    }
                    public void requireCurrent(){held.requireCurrent();a.materialization().requireCurrent();held.requireCurrent();}
                    public TargetScopeFeeAuthority scopeFees(){return held.scopeFees();}
                };
                yield new TargetSourceApplyRuntime.SloWriterControl(value.prepared(),
                        new com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.Authority(frozen,
                                (version,source)->frozen.key(version,source),a.actor(),a.scopeProof(),material),commit);
            }
            case ParentCredit value -> {
                final var a = value.authority();
                final var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                final var capacity = new TargetParentControlCreditVerifier.CapacityAuthority() {
                    public void requireAuthorized(TargetParentControlCredit.Definition definition,
                            TargetParentControlCreditVerifier.View actual, SourcePosition source,
                            TargetStoreBackend.Mutation fullPlan) {
                        requireCurrent(); requireSource(entry, source);
                        a.capacity().requireAuthorized(definition, actual, source, fullPlan);
                        requireCurrent();
                    }
                    public void requireCurrent() { held.requireCurrent(); a.capacity().requireCurrent(); held.requireCurrent(); }
                };
                yield new TargetSourceApplyRuntime.ParentCreditControl(value.prepared(),
                        new TargetParentControlCreditVerifier.Authority(frozen,
                                (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof(), capacity),
                        held.commit(value.commit()));
            }
            case ScopeObligation value -> {
                final var a = value.authority();
                final var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                final var capacity = new TargetScopeControlObligationVerifier.CapacityAuthority() {
                    public void requireAuthorized(TargetScopeControlObligation.Definition definition,
                            TargetParentControlCreditVerifier.View actual, TargetParentControlCredit parent,
                            SourcePosition source, TargetStoreBackend.Mutation fullPlan) {
                        requireCurrent(); requireSource(entry, source);
                        a.capacity().requireAuthorized(definition, actual, parent, source, fullPlan);
                        requireCurrent();
                    }
                    public void requireCurrent() { held.requireCurrent(); a.capacity().requireCurrent(); held.requireCurrent(); }
                };
                yield new TargetSourceApplyRuntime.ScopeObligationControl(value.prepared(),
                        new TargetScopeControlObligationVerifier.Authority(frozen,
                                (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof(), capacity),
                        held.commit(value.commit()));
            }
            case SloMode value -> {
                final var a = value.authority();
                final var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                final var material = new TargetSloObservationVerifier.MaterializationAuthority() {
                    public void requireAuthorized(TargetSloObservationControlRequest request, TargetSloObservationState prior,
                            TargetSloObservationGeneration retained, SourcePosition source) {
                        requireCurrent(); a.materialization().requireAuthorized(request, prior, retained, source); requireCurrent();
                    }
                    public void requireCurrent() { held.requireCurrent(); a.materialization().requireCurrent(); held.requireCurrent(); }
                    public TargetScopeFeeAuthority scopeFees() { return held.scopeFees(); }
                };
                yield new TargetSourceApplyRuntime.SloObservationControl(value.prepared(),
                        new TargetSloObservationVerifier.Authority(frozen, (version, source) -> frozen.key(version, source),
                                a.actor(), a.scopeProof(), material), commit);
            }
        };
    }

    /** The provider has already acquired all external Catalog/reader/resource/management materials on owned I/O.
     * These methods consume only its immutable local snapshot and retain that same acquisition through commit. */
    private static com.nereusstream.delay.runtime.TargetMigrationRetirementStore.OriginalInputs retirementHold(
            com.nereusstream.delay.runtime.TargetMigrationRetirementStore.OriginalInputs original, Held held,
            SourceReplayMutation entry) {
        if (original == null) return null;
        return new com.nereusstream.delay.runtime.TargetMigrationRetirementStore.OriginalInputs() {
            private void requireInput(SystemMutation input, SourcePosition source) {
                held.requireCurrent(); requireSource(entry, source);
                if (!Arrays.equals(input.canonicalEnvelope(), entry.mutation().canonicalEnvelope()))
                    throw new IllegalStateException("retirement content hold changed its exact actual Source frame");
            }
            public void requireIntent(com.nereusstream.delay.protocol.TargetMigrationRoot root,
                    com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest intent,
                    SystemMutation input, SourcePosition source) {
                requireInput(input, source); original.requireIntent(root, intent, input, source); requireCurrent();
            }
            public void requireConfirmation(com.nereusstream.delay.protocol.TargetMigrationRoot root,
                    com.nereusstream.delay.protocol.TargetMigrationRetirementRecord prior,
                    com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest request,
                    SystemMutation input, SourcePosition source) {
                requireInput(input, source); original.requireConfirmation(root, prior, request, input, source); requireCurrent();
            }
            public void requireManagementPayment(com.nereusstream.delay.protocol.TargetMigrationRoot root,
                    PreparedControlOperation control, SystemMutation input, SourcePosition source,
                    com.nereusstream.delay.protocol.TargetMigrationRetirementRecord next) {
                requireInput(input, source); original.requireManagementPayment(root, control, input, source, next); requireCurrent();
            }
            public void requireCurrent() { held.requireCurrent(); original.requireCurrent(); held.requireCurrent(); }
        };
    }

    private static TargetSourceApplyRuntime.MigrationArchiveInputs archiveHold(TargetSourceApplyRuntime.MigrationArchiveInputs archive,
            PreparedControlOperation prepared,Held held,SourceReplayMutation entry){
        if(archive==null)return null;
        final var original=archive.original();
        return new TargetSourceApplyRuntime.MigrationArchiveInputs(archive.copy(),archive.takeover(),archive.received(),
                new TargetMigrationArchiveStore.OriginalInputs(){
                    public void requireManagementPayment(TargetMigrationRoot root,TargetMigrationRetirementRecord retirement,
                            TargetMigrationArchiveState prior,TargetMigrationArchiveState next,java.util.List<TargetMigrationArchiveRow> rows,
                            PreparedControlOperation control,SystemMutation input,SourcePosition source){
                        requireCurrent();requireSource(entry,source);
                        if(!Arrays.equals(prepared.canonicalBytes(),control.canonicalBytes())
                                ||!Arrays.equals(entry.mutation().canonicalEnvelope(),input.canonicalEnvelope()))
                            throw new IllegalStateException("archive payment changed its original exact CONTROL/Source frame");
                        original.requireManagementPayment(root,retirement,prior,next,rows,control,input,source);requireCurrent();
                    }
                    public void requireCurrent(){held.requireCurrent();original.requireCurrent();held.requireCurrent();}
                });
    }

    private static TargetSloObservationSamples.Authority sampleHold(TargetSloObservationSamples.Authority original,Held held){
        return new TargetSloObservationSamples.Authority(){
            public void requireSample(TargetSloObservationGeneration generation,TargetSloObservationSample sample,long bytes){
                requireCurrent();original.requireSample(generation,sample,bytes);requireCurrent();
            }
            public void requireCurrent(){held.requireCurrent();original.requireCurrent();held.requireCurrent();}
            public TargetScopeFeeAuthority scopeFees(){return held.scopeFees();}
        };
    }

    private static TargetScopeFeeAuthority scopeFeeHold(TargetScopeFeeAuthority original,Held held){
        Objects.requireNonNull(original,"original Scope fee authority");
        return new TargetScopeFeeAuthority(){
            public SystemMutation occurrenceInput(SystemMutation firstInput,TargetResultRecord first,TargetResultRecord position){
                requireCurrent();
                requireSource(held.entry,position.mutation().source());
                final var actual=Objects.requireNonNull(original.occurrenceInput(firstInput,first,position),"original occurrence frame");
                requireCurrent();return actual;
            }
            public void requireScope(TargetScopeControlObligation scope){
                requireCurrent();original.requireScope(scope);requireCurrent();
            }
            public Payment requireOccurrence(Occurrence occurrence){
                requireCurrent();requireSource(held.entry,occurrence.position().mutation().source());
                if(!Arrays.equals(held.digest,occurrence.position().mutation().mutationDigest()))
                    throw new IllegalStateException("Scope fee hold changes its exact original Source frame");
                final var payment=Objects.requireNonNull(original.requireOccurrence(occurrence));
                requireCurrent();return payment;
            }
            public void requireCurrent(){held.requireCurrent();original.requireCurrent();held.requireCurrent();}
        };
    }

    private static TargetSourceAuthorityPreparation.Family family(Inputs inputs) {
        return switch (inputs) {
            case Migration ignored -> TargetSourceAuthorityPreparation.Family.MIGRATION;
            case PayloadSummary ignored -> TargetSourceAuthorityPreparation.Family.PAYLOAD_SUMMARY;
            case Command ignored -> TargetSourceAuthorityPreparation.Family.COMMAND;
            case Grant ignored -> TargetSourceAuthorityPreparation.Family.GRANT;
            case Fence ignored -> TargetSourceAuthorityPreparation.Family.FENCE;
            case Expiry ignored -> TargetSourceAuthorityPreparation.Family.EXPIRY;
            case Close ignored -> TargetSourceAuthorityPreparation.Family.CLOSE;
            case Membership ignored -> TargetSourceAuthorityPreparation.Family.MEMBERSHIP;
            case NativePolicy ignored -> TargetSourceAuthorityPreparation.Family.NATIVE_POLICY;
            case Admission ignored -> TargetSourceAuthorityPreparation.Family.ADMISSION;
            case Outcome ignored -> TargetSourceAuthorityPreparation.Family.OUTCOME;
            case Channel ignored -> TargetSourceAuthorityPreparation.Family.CHANNEL;
            case SloMode ignored -> TargetSourceAuthorityPreparation.Family.SLO_MODE;
            case SloEvent ignored -> TargetSourceAuthorityPreparation.Family.SLO_EVENT;
            case SloWriter ignored -> TargetSourceAuthorityPreparation.Family.SLO_WRITER;
            case ParentCredit ignored -> TargetSourceAuthorityPreparation.Family.PARENT_CONTROL_CREDIT;
            case ScopeObligation ignored -> TargetSourceAuthorityPreparation.Family.SCOPE_OBLIGATION;
            case ScopeFees ignored -> TargetSourceAuthorityPreparation.Family.SCOPE_FEES;
            case Retirement ignored -> TargetSourceAuthorityPreparation.Family.RETIREMENT;
            case Confirmation ignored -> TargetSourceAuthorityPreparation.Family.RETIREMENT;
            case PendingConfirmation ignored -> TargetSourceAuthorityPreparation.Family.RETIREMENT;
            case Reclaim ignored -> TargetSourceAuthorityPreparation.Family.RECLAIM;
            case Activation ignored -> TargetSourceAuthorityPreparation.Family.ACTIVATION;
        };
    }

    private TargetSourceApplyRuntime.ReclaimControl reclaim(SourceReplayEntry entry,Reclaim input,
            Held held,TargetStoreBackend.CommitAuthority commit,Runnable guard,TargetSourceApplyRuntime.ReclaimRead source){
        if(!(entry instanceof SourceReplayMutation original)||original.mutation().type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM)
            throw new IllegalArgumentException("reclaim snapshot changed original Source operation");
        final var mutation=original.mutation();final var body=TargetResourceReclaimBody.decode(mutation.canonicalBody());
        if(!input.scope().equals(body.scope()))throw new IllegalArgumentException("reclaim snapshot changed original Source scope");
        final var prior=input.history();final var selected=prior.select(body,mutation,entry.position()).orElse(null);
        held.requireCurrent();guard.run();
        final var pin=prior.pin();
        held.bytes=Math.addExact(held.bytes,Math.addExact(prior.proof().canonicalBytes().length,
                Math.addExact(pin.authority().canonicalBytes().length,pin.key().getEncoded().length+256L)));
        final var history=new TargetResourceReclaimStore.SourceHistory(prior.proof(),new TargetRetirementAuthorizationHistory.Pin(
                pin.authority(),pin.keyVersion(),pin.key(),pin.baselineDigest(),pin.root(),pin.namespace()),()->{
            held.requireCurrent();prior.current().run();held.requireCurrent();
        });
        final boolean authorized=selected!=null&&mutation.verifySignature(selected.writerKey());
        if(authorized&&source==null)throw new TargetSourceAuthorityPreparation.InputUnavailable("original Native reclaim Source/Store context is unavailable");
        final var objectDeletion=input.objectDeletion()!=null?input.objectDeletion():
                source!=null&&body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm?source.objectDeletion().orElse(null):null;
        if(objectDeletion!=null){
            if(!(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm))throw new IllegalArgumentException("hot OBJECT observation supplied for another Source action");
            objectDeletion.requireConfirmation(body);
            final var request=objectDeletion.request();held.bytes=Math.addExact(held.bytes,request.expectedOwner().canonicalBytes().length+request.originBytes()+request.intentAt().canonicalBytes().length+256L);
        }
        final var uncommittedDeletion=input.uncommittedDeletion()!=null?input.uncommittedDeletion():
                source!=null&&body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm?source.uncommittedDeletion().orElse(null):null;
        if(uncommittedDeletion!=null){uncommittedDeletion.requireConfirmation(body);held.bytes=Math.addExact(held.bytes,uncommittedDeletion.retainedBytes());}
        final Runnable reclaimHeld=()->{
            held.requireCurrent();
            if(body.uncommittedObjectReclaim()){
                held.original.requireUncommittedObjectIndependent(source);
                if(body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm){
                    if(uncommittedDeletion!=null)uncommittedDeletion.requireConfirmation(body);
                    else held.original.requireUncommittedObjectHistoricalConfirmation(source);
                }
            }else if(body.objectPayloadReclaim()){
                held.original.requireObjectPayloadIndependent(source);
                if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm){
                    if(objectDeletion!=null)objectDeletion.requireConfirmation(body);
                    else held.original.requireObjectPayloadHistoricalConfirmation(source);
                }
            }else if(body.incarnationReclaim())held.original.requireIncarnationIndependent(source);
            else if(body.nativeRetainedReclaim())held.original.requireNativeRetainedIndependent(source);
            else if(body.inlinePayloadRelease())held.original.requireInlinePayloadIndependent(source);
            held.requireCurrent();
        };
        final var protection=authorized&&source.matchesExpected()?source.protectReclaim(input.recoveryImages(),maximumBytes-held.bytes,reclaimHeld):null;
        if(protection!=null){
            protection.requireSource(body.scope(),body.lineage(),mutation,entry.position());protection.requireCurrent();
            final long bytes=protection.retainedBytes();
            if(bytes<0||bytes>maximumBytes-held.bytes)throw new IllegalStateException("reclaim protections exceed complete Source input byte bound");
            held.bytes=Math.addExact(held.bytes,bytes);
            beginRetirement(held, source);
            held.retirement.recovery(source.catalog(),protection.recoveryProtection());
            held.retirement.debitReads(protection.acquisitionRows(),protection.acquisitionBytes());
        }
        if(held.bytes>maximumBytes)throw new IllegalStateException("reclaim history exceeds complete Source input byte bound");
        return new TargetSourceApplyRuntime.ReclaimControl(new TargetResourceReclaimStore.Authority(){
            public TargetRetirementPreparation.Proof preparation(){return held.retirementProof();}
            private void requireEntry(TargetQuotaScope scope,SystemMutation actual,SourcePosition source){
                held.requireCurrent();requireSource(entry,source);
                if(!input.scope().equals(scope)||!Arrays.equals(held.digest,Bytes.sha256(actual.canonicalEnvelope())))
                    throw new IllegalStateException("reclaim snapshot changed original Source frame/scope");
            }
            public TargetResourceReclaimStore.SourceHistory history(TargetQuotaScope scope,SystemMutation actual,SourcePosition source){
                requireEntry(scope,actual,source);history.current().run();return history;
            }
            public TargetResourceReclaimStore.Protection protect(TargetQuotaScope scope,SystemMutation actual,SourcePosition source,
                    TargetRetirementAuthorizationHistory.Grant grant){
                requireEntry(scope,actual,source);
                if(!authorized||!Arrays.equals(selected.canonicalBytes(),grant.canonicalBytes()))
                    throw new IllegalStateException("reclaim changed original purpose-signed grant");
                if(protection==null)throw new TargetSourceAuthorityPreparation.Required();
                protection.requireCurrent();return protection;
            }
        },commit);
    }

    private TargetSourceApplyRuntime.RetirementControl retirement(SourceReplayEntry entry, Retirement input,
            Held held, TargetStoreBackend.CommitAuthority commit, Runnable guard,TargetSourceApplyRuntime.ReclaimRead source) {
        if (!(entry instanceof SourceReplayMutation original) || original.mutation().type() != SystemMutationType.RESOURCE_RETIRE_INTENT)
            throw new IllegalArgumentException("retirement snapshot changed its original Source operation");
        final var mutation = original.mutation();
        final var body = TargetEvidenceRetireIntentBody.decode(mutation.canonicalBody());
        input.scope().requireRoute(body.shard(), body.resource().tenantScope());
        final var originalHistory=Objects.requireNonNull(input.history(),"complete original retirement Source proof/pin");
        final var selectedGrant=originalHistory.select(body.resource(),mutation,entry.position()).orElse(null);held.requireCurrent();guard.run();
        final var pin=originalHistory.pin();
        final long historyBytes=Math.addExact(originalHistory.proof().canonicalBytes().length,
                Math.addExact(pin.authority().canonicalBytes().length,pin.key().getEncoded().length+256L));
        held.bytes=Math.addExact(held.bytes,historyBytes);
        if(held.bytes>maximumBytes)throw new IllegalStateException("original retirement history exceeds complete Source input bound");
        final var history=new TargetEvidenceRetirementStore.SourceHistory(originalHistory.proof(),new TargetRetirementAuthorizationHistory.Pin(
                pin.authority(),pin.keyVersion(),pin.key(),pin.baselineDigest(),pin.root(),pin.namespace()),()->{
            held.requireCurrent();originalHistory.current().run();held.requireCurrent();
        });
        final var approved = input.approval();
        final TargetEvidenceRetirementStore.Approval frozen;
        if (approved == null) frozen = null;
        else {
            approved.current().run(); held.requireCurrent(); guard.run();
            final var copy = approved.copy().image();
            // Preflight using actual held lengths, before serializing/hashing the complete copy.
            final byte[] keyBytes = approved.writerKey().getEncoded();
            if (keyBytes == null || keyBytes.length != 44) throw new IllegalStateException("invalid original retirement Source public key");
            final long upper = Math.addExact(copy.estimatedBytes(), keyBytes.length);
            if (upper > maximumBytes - held.bytes) throw new IllegalStateException("complete retirement Source input exceeds byte bound");
            if (!Arrays.equals(copy.resource().canonicalBytes(), body.resource().canonicalBytes()))
                throw new IllegalStateException("retirement Source snapshot replaced the exact Root object");
            copy.requireContext(body.context());
            final PublicKey key;
            try { key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(keyBytes)); }
            catch (java.security.GeneralSecurityException invalid) { throw new IllegalStateException("invalid original retirement Source public key", invalid); }
            held.bytes = Math.addExact(held.bytes, Math.addExact(body.context().copyManifestBytes(), keyBytes.length));
            frozen = new TargetEvidenceRetirementStore.Approval(key, approved.copy(), () -> {
                held.requireCurrent(); approved.current().run(); held.requireCurrent();
            },approved.activatedTuple(),approved.historyGrant());
        }
        held.requireCurrent(); guard.run();
        if(frozen!=null&&selectedGrant!=null&&mutation.verifySignature(selectedGrant.writerKey()))beginRetirement(held,source);
        return new TargetSourceApplyRuntime.RetirementControl(new TargetEvidenceRetirementStore.Authority(){
            public TargetRetirementPreparation.Proof preparation(){return held.retirementProof();}
            private void requireEntry(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                held.requireCurrent();requireSource(entry,source);
                if(!scope.equals(input.scope())||!Arrays.equals(actor.canonicalBytes(),mutation.authorIdentity())
                        ||actual!=mutation&&!Arrays.equals(held.digest,Bytes.sha256(actual.canonicalEnvelope())))
                    throw new IllegalStateException("retirement snapshot changed original scope/actor/Source frame");
            }
            public TargetEvidenceRetirementStore.Approval resolve(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                requireEntry(scope,actor,actual,source);if(frozen!=null)frozen.current().run();return frozen;
            }
            public TargetEvidenceRetirementStore.SourceHistory history(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                requireEntry(scope,actor,actual,source);history.current().run();return history;
            }
        }, commit);
    }

    private TargetSourceApplyRuntime.RetirementControl confirmation(SourceReplayEntry entry,Confirmation input,
            Held held,TargetStoreBackend.CommitAuthority commit,Runnable guard,TargetSourceApplyRuntime.ReclaimRead source){
        return confirmation(entry,input,held,commit,guard,source,null,null);
    }
    private TargetSourceApplyRuntime.RetirementControl confirmation(SourceReplayEntry entry,Confirmation input,
            Held held,TargetStoreBackend.CommitAuthority commit,Runnable guard,TargetSourceApplyRuntime.ReclaimRead source,
            PendingConfirmation pending,TargetSourceApplyRuntime.ConfirmationRead confirmation){
        if(!(entry instanceof SourceReplayMutation original)||original.mutation().type()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED)
            throw new IllegalArgumentException("confirmation snapshot changed its original Source operation");
        final var mutation=original.mutation();final var body=TargetEvidenceDeleteConfirmedBody.decode(mutation.canonicalBody());
        input.scope().requireRoute(body.shard(),body.resource().tenantScope());
        final var prior=Objects.requireNonNull(input.history());final var selectedGrant=prior.select(body.resource(),mutation,entry.position()).orElse(null);
        held.requireCurrent();guard.run();final var pin=prior.pin();
        held.bytes=Math.addExact(held.bytes,Math.addExact(prior.proof().canonicalBytes().length,
                Math.addExact(pin.authority().canonicalBytes().length,pin.key().getEncoded().length+256L)));
        if(held.bytes>maximumBytes)throw new IllegalStateException("complete confirmation history exceeds Source input bound");
        final var history=new TargetEvidenceRetirementStore.SourceHistory(prior.proof(),new TargetRetirementAuthorizationHistory.Pin(
                pin.authority(),pin.keyVersion(),pin.key(),pin.baselineDigest(),pin.root(),pin.namespace()),()->{
            held.requireCurrent();prior.current().run();held.requireCurrent();
        });
        final var frozen=new TargetEvidenceRetirementStore.ConfirmationApproval[]{freezeConfirmation(input.approval(),original,held,guard)};
        held.requireCurrent();guard.run();
        if(selectedGrant!=null&&mutation.verifySignature(selectedGrant.writerKey())&&(frozen[0]!=null||pending!=null)){
            beginRetirement(held,source);
            if(pending!=null){
                if(confirmation==null)throw new TargetSourceAuthorityPreparation.InputUnavailable("original historical confirmation Native reader unavailable");
                held.finishRetirement=()->{
                    final var prepared=Objects.requireNonNull(held.retirementProof());
                    prepared.requireInput(mutation,entry.position());prepared.requireCurrent();held.requireCurrent();
                    final var approved=held.original.confirmationAfterPreparation(pending,confirmation,prepared);
                    held.requireCurrent();prepared.requireCurrent();
                    if(approved!=null&&approved.historical()==null)
                        throw new IllegalStateException("pending historical confirmation returned a different hot observation path");
                    frozen[0]=freezeConfirmation(approved,original,held,guard);
                };
            }
        }
        return new TargetSourceApplyRuntime.RetirementControl(new TargetEvidenceRetirementStore.Authority(){
            public TargetRetirementPreparation.Proof preparation(){return held.retirementProof();}
            private void requireEntry(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                held.requireCurrent();requireSource(entry,source);
                if(!scope.equals(input.scope())||!Arrays.equals(actor.canonicalBytes(),mutation.authorIdentity())
                        ||actual!=mutation&&!Arrays.equals(held.digest,Bytes.sha256(actual.canonicalEnvelope())))
                    throw new IllegalStateException("confirmation snapshot changed original Source/subject/frame");
            }
            public TargetEvidenceRetirementStore.Approval resolve(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                throw new IllegalArgumentException("confirmation snapshot cannot approve a retirement intent");
            }
            public TargetEvidenceRetirementStore.ConfirmationApproval resolveConfirmation(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                requireEntry(scope,actor,actual,source);if(frozen[0]!=null)frozen[0].current().run();return frozen[0];
            }
            public TargetEvidenceRetirementStore.SourceHistory history(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                requireEntry(scope,actor,actual,source);history.current().run();return history;
            }
        },commit);
    }

    private TargetEvidenceRetirementStore.ConfirmationApproval freezeConfirmation(
            TargetEvidenceRetirementStore.ConfirmationApproval approved,SourceReplayMutation original,Held held,Runnable guard){
        if(approved==null)return null;
        approved.current().run();held.requireCurrent();guard.run();
        final var observed=approved.observation();final var historical=approved.historical();final byte[] encoded=approved.writerKey().getEncoded();
        if(encoded==null||encoded.length!=44)throw new IllegalStateException("confirmation writer requires original Ed25519 key");
        final long upper=Math.addExact(observed!=null?observed.retainedBytes():historical.retainedBytes(),encoded.length);
        if(upper>maximumBytes-held.retainedBytes())throw new IllegalStateException("complete confirmation receipt exceeds reserved Source inputs");
        approved.requireSource(original.mutation(),original.position());
        approved.requireConfirmation(TargetEvidenceDeleteConfirmedBody.decode(original.mutation().canonicalBody()));
        if(historical!=null)historical.requireEntry(original);final PublicKey key;
        try{key=KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));}
        catch(java.security.GeneralSecurityException invalid){throw new IllegalStateException("invalid original confirmation Source key",invalid);}
        held.bytes=Math.addExact(held.bytes,upper);
        return new TargetEvidenceRetirementStore.ConfirmationApproval(key,observed,historical,()->{
            held.requireCurrent();approved.current().run();held.requireCurrent();
        },approved.activatedTuple(),approved.historyGrant());
    }

    private void beginRetirement(Held held,TargetSourceApplyRuntime.ReclaimRead source) {
        if(source==null)throw new TargetSourceAuthorityPreparation.InputUnavailable("original selected retirement Source/Store context unavailable");
        final var original=Objects.requireNonNull(held.original.retirementPreparationProtection(source),"actual whole-flight retirement admission");
        original.requireCurrent();held.requireCurrent();
        held.retirementSource=source;
        held.retirement=source.beginPreparation(new TargetRetirementPreparation.Protection(){
            public TargetRetirementPreparation.Capacity capacity(){return original.capacity();}
            public void requireCurrent(){held.requireCurrent();original.requireCurrent();held.requireCurrent();}
            public void requireBinding(com.nereusstream.delay.store.StoreMetadata metadata,TargetQuotaIncarnation root,
                    TargetQuotaMutation cut,SystemMutation input,SourcePosition at){
                held.requireCurrent();original.requireBinding(metadata,root,cut,input,at);held.requireCurrent();
            }
        });
    }

    @FunctionalInterface private interface Keys { PublicKey resolve(long version); }

    private FrozenControl freeze(PreparedControlOperation prepared, SourceReplayEntry entry,
            ControlTargetRegistrationAuthority registrations, Keys keys, Held held, Runnable guard) {
        var mutation = ((SourceReplayMutation) entry).mutation();
        var registered = registrations.find(prepared.operationId()).orElse(null);
        guard.run(); held.requireCurrent();
        var preparedKey = copyKey(keys.resolve(prepared.signingKeyVersion()));
        var mutationKey = copyKey(keys.resolve(mutation.signingKeyVersion()));
        boolean bound = false;
        if (registered != null && Arrays.equals(registered.canonicalBytes(), prepared.canonicalBytes()) && !prepared.targets().isEmpty()) {
            // A local semantic denial stays with the existing verifier; external registry failures never become denials.
            try { ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), mutation); bound = true; }
            catch (IllegalArgumentException rejected) { bound = false; }
            if (bound) registrations.validateMutation(prepared, prepared.targets().getFirst(), mutation);
        }
        guard.run(); held.requireCurrent();
        final long frozenBytes=(long) prepared.canonicalBytes().length
                + (registered == null ? 0 : registered.canonicalBytes().length)
                + (preparedKey == null ? 0 : preparedKey.getEncoded().length)
                + (mutationKey == null ? 0 : mutationKey.getEncoded().length);
        if(frozenBytes>maximumBytes-held.bytes)throw new IllegalStateException("original Control keys/registration exceed the content reservation");
        held.bytes=Math.addExact(held.bytes,frozenBytes);
        if(bound&&TargetSloControlScopeAuthority.reference(mutation).isPresent()){
            final var scoped=Objects.requireNonNull(registrations.captureScopedMutation(prepared,prepared.targets().getFirst(),mutation,
                    maximumBytes-held.bytes,guard),"original Scope membership hold");
            held.scopedControl=scoped;
            if(scoped.retainedBytes()>maximumBytes-held.bytes)throw new IllegalStateException("original Scope membership exceeds the content reservation");
            held.bytes=Math.addExact(held.bytes,scoped.retainedBytes());
            guard.run();held.requireCurrent();
        }
        return new FrozenControl(held, prepared, registered, preparedKey, mutationKey, mutation, bound);
    }

    private static PublicKey copyKey(PublicKey key) {
        if (key == null) return null; // Proved denial from the original provider; incomplete reads must throw there.
        try { return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(key.getEncoded())); }
        catch (java.security.GeneralSecurityException invalid) { throw new IllegalStateException("invalid original Control signing key", invalid); }
    }

    private static final class FrozenControl implements ControlTargetRegistrationAuthority {
        private final Held held;
        private final PreparedControlOperation prepared, registered;
        private final PublicKey preparedKey, mutationKey;
        private final SystemMutation mutation;
        private final boolean registryBound;
        FrozenControl(Held held, PreparedControlOperation prepared, PreparedControlOperation registered,
                PublicKey preparedKey, PublicKey mutationKey, SystemMutation mutation, boolean registryBound) {
            this.held = held; this.prepared = prepared; this.registered = registered;
            this.preparedKey = preparedKey; this.mutationKey = mutationKey; this.mutation = mutation; this.registryBound = registryBound;
        }
        PublicKey key(long version, SourcePosition source) {
            held.requireCurrent(); requireSource(held.entry, source);
            if (version == prepared.signingKeyVersion() || version == (int) prepared.signingKeyVersion()) return preparedKey;
            if (version == mutation.signingKeyVersion()) return mutationKey;
            throw new IllegalStateException("Control key not collected for this exact source use");
        }
        @Override public RegistrationResult register(PreparedControlOperation value) {
            throw new IllegalStateException("Source content is a read-only original registration snapshot");
        }
        @Override public Optional<PreparedControlOperation> find(byte[] operation) {
            held.requireCurrent();
            if (!Arrays.equals(operation, prepared.operationId())) throw new IllegalStateException("Control operation not collected");
            return Optional.ofNullable(registered);
        }
        @Override public void validateMutation(PreparedControlOperation value, ControlTargetRef target, SystemMutation actual) {
            held.requireCurrent();
            if (!registryBound || !Arrays.equals(value.canonicalBytes(), prepared.canonicalBytes())
                    || !Arrays.equals(actual.canonicalEnvelope(), mutation.canonicalEnvelope())) {
                throw new IllegalStateException("Control registry snapshot is not this exact mutation");
            }
            ControlTargetMutationBinding.validate(value, target, actual);
        }
        @Override public void validateScopedMutation(PreparedControlOperation value,ControlTargetRef target,SystemMutation actual){
            validateMutation(value,target,actual);
            if(held.scopedControl==null)throw new IllegalStateException("original Scope membership was not collected");
            held.scopedControl.validate(value,target,actual);
        }
    }

    private static void requireSource(SourceReplayEntry entry, SourcePosition source) {
        if (!Arrays.equals(entry.position().canonicalBytes(), source.canonicalBytes())) {
            throw new IllegalStateException("content snapshot changes its original Source point");
        }
    }

    /** Deliberately has no Native business control or fee-plan authority. */
    private static final class WriterPrelude implements TargetSourceAuthorityPreparation.Held {
        private final SourceReplayEntry entry;
        private Held original;
        private CheckpointInputFuture.Cleanup pending;
        private boolean closed;
        private WriterPrelude(SourceReplayEntry entry){this.entry=entry;}
        public SourceReplayEntry entry(){return entry;}
        public Object control(){throw new TargetSourceAuthorityPreparation.InputUnavailable("writer prelude cannot authorize a Source Native plan");}
        public long retainedBytes(){return original==null?0:original.retainedBytes();}
        public boolean preparationReady(){return false;}
        public void requireCurrent(){
            if(closed||original==null)throw new IllegalStateException("original writer prelude is not held");
            original.requireCurrent();
        }
        public TargetSloWriterOrigin.Protection detachWriterOrigin(TargetSloWriterOrigin.PreparationRequired request){
            requireCurrent();return original.detachWriterOrigin(request);
        }
        public void close(){
            if(closed)return;
            if(pending!=null&&!pending.cleanupComplete()){
                final var retry=pending.retryCleanup();
                if(!retry.isDone()||!pending.cleanupComplete())
                    throw new IllegalStateException("original partial writer prelude cleanup has not exited");
            }
            if(original!=null)original.close();closed=true;
        }
    }

    private static final class Held implements TargetSourceAuthorityPreparation.Held {
        private final SourceReplayEntry entry;
        private final HeldInputs original;
        private final byte[] digest;
        private Object control;
        private TargetQuotaCapacitySnapshot.Verified capacity;
        private CheckpointInputFuture.Cleanup pendingCapacityCleanup;
        private TargetScopeFeeAuthority scopeFees=TargetScopeFeeAuthority.UNAVAILABLE;
        private TargetSloControlScopeAuthority.MemberHold scopedControl;
        private TargetRetirementPreparation retirement;
        private TargetRetirementPreparation.Proof retirementProof;
        private TargetSourceApplyRuntime.ReclaimRead retirementSource;
        private boolean retirementClosed;
        private Runnable finishRetirement;
        private boolean scopedControlClosed;
        private long bytes;
        private int commits;
        private boolean originalClosed, capacityClosed;
        Held(SourceReplayEntry entry, HeldInputs original) {
            this.entry = entry; this.original = original; bytes = original.retainedBytes();
            digest = entry instanceof SourceReplayRecord command ? command.command().canonicalFrameDigest()
                    : Bytes.sha256(((SourceReplayMutation) entry).mutation().canonicalEnvelope());
        }
        @Override public SourceReplayEntry entry() { return entry; }
        @Override public Object control() { return control; }
        @Override public long retainedBytes() { return Math.addExact(bytes,retirement==null?0:retirement.retainedBytes()); }
        @Override public TargetSloWriterOrigin.Protection detachWriterOrigin(TargetSloWriterOrigin.PreparationRequired request) {
            requireCurrent();
            final var independent=Objects.requireNonNull(original.detachWriterOrigin(request),"transferred original writer subloan");
            independent.requireCurrent();requireCurrent();return independent;
        }
        @Override public ArchiveTailInputs archiveTailInputs(TargetMigrationArchivePreparation.TailPreparationRequired request) {
            requireCurrent();final var actual=Objects.requireNonNull(original.archiveTailInputs(request),"actual protected archive receiver inputs");
            actual.opening().requireCurrent();requireCurrent();return actual;
        }
        @Override public boolean preparationReady() { return (retirement==null||retirementProof!=null)&&finishRetirement==null; }
        @Override public void invalidateRetirement(TargetRetirementPreparation.ViewChanged changed) {
            requireCurrent();if(retirement==null)throw changed;
            retirement.invalidate();retirementProof=null;
        }
        @Override public boolean advancePreparation() {
            requireCurrent();
            if(retirement!=null&&retirementProof==null){
                if(!retirementSource.advancePreparation(retirement))return false;
                retirementProof=retirement.proof();
            }
            if(finishRetirement!=null){finishRetirement.run();finishRetirement=null;}
            requireCurrent();return true;
        }
        private TargetRetirementPreparation.Proof retirementProof() {
            requireCurrent();
            if(retirement!=null&&retirementProof==null)throw new TargetSourceAuthorityPreparation.Required();
            if(retirementProof!=null)retirementProof.requireCurrent();
            return retirementProof;
        }
        @Override public TargetScopeFeeAuthority scopeFees() { requireCurrent(); return scopeFees; }
        @Override public TargetStoreBackend.CommitAuthority protectScopeFees(TargetStoreBackend.CommitAuthority physical) {
            return commit(physical);
        }
        @Override public synchronized void requireCurrent() {
            if (originalClosed || capacityClosed) throw new IllegalStateException("original content hold is closing");
            original.requireCurrent();
            if(scopedControl!=null&&!scopedControlClosed)scopedControl.requireCurrent();
        }
        TargetStoreBackend.CommitAuthority commit(TargetStoreBackend.CommitAuthority physical) {
            return (store, scope, mutation) -> {
                synchronized (this) {
                    requireCurrent(); var stamp = mutation.quota().counters().mutation();
                    stamp.requireSourceApplied(); requireSource(entry, stamp.source());
                    if (!scope.shard().equals(entry.position().shardId()) || !Arrays.equals(digest, stamp.mutationDigest())) {
                        throw new IllegalStateException("content commit changes its original Shard/frame");
                    }
                    commits++;
                }
                final TargetStoreBackend.CommitGuard acquired;
                try { acquired = physical.acquire(store, scope, mutation); }
                catch (RuntimeException | Error failure) { synchronized (this) { commits--; } throw failure; }
                return new TargetStoreBackend.CommitGuard() {
                    private boolean released;
                    @Override public void requireCurrent() { synchronized (Held.this) {
                        if (released) throw new IllegalStateException("content commit guard closed");
                        Held.this.requireCurrent(); acquired.requireCurrent(); Held.this.requireCurrent();
                    } }
                    @Override public void close() { synchronized (Held.this) {
                        if (!released) { acquired.close(); released = true; commits--; }
                    } }
                };
            };
        }
        @Override public synchronized void close() {
            if (commits != 0) throw new IllegalStateException("original content still protects a commit");
            if(retirement!=null&&!retirementClosed){retirement.close();retirementClosed=true;}
            if(scopedControl!=null&&!scopedControlClosed){scopedControl.close();scopedControlClosed=true;}
            Throwable failure = null;
            if (!capacityClosed) {
                try {
                    if (pendingCapacityCleanup != null && !pendingCapacityCleanup.cleanupComplete()) {
                        var retry = pendingCapacityCleanup.retryCleanup();
                        if (!retry.isDone() || !pendingCapacityCleanup.cleanupComplete()) {
                            throw new IllegalStateException("original failed capacity collection cleanup remains pending");
                        }
                    }
                    if (capacity != null) capacity.close(); capacityClosed = true;
                }
                catch (RuntimeException | Error rejected) { failure = rejected; }
            }
            if (!originalClosed) {
                try { original.close(); originalClosed = true; }
                catch (RuntimeException | Error rejected) { if (failure == null) failure = rejected; else if (failure != rejected) failure.addSuppressed(rejected); }
            }
            if (failure != null) throw new IllegalStateException("original content/capacity release remains pending", failure);
        }
    }
}
