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
    }

    public interface HeldInputs extends AutoCloseable {
        SourceReplayEntry entry();
        Inputs inputs();
        long retainedBytes();
        /**
         * Local held checks only. RECLAIM must retain the original Source/Worker/Store and complete
         * Catalog/Floor/Pin/image/future-reader admission protections through the Native commit.
         * Two observations, an empty caller inventory, or signatures alone are not that held protocol.
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

    public sealed interface Inputs permits Command, Grant, Fence, Expiry, Close, Membership, NativePolicy, Admission, Outcome, Channel, Retirement, Confirmation, Activation, Reclaim, SloMode, SloEvent, SloWriter {}
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
            held.control = collect(entry, inputs, held, guard,reclaim);
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

    private Object collect(SourceReplayEntry entry, Inputs inputs, Held held, Runnable guard,TargetSourceApplyRuntime.ReclaimRead reclaim) {
        var commit = held.commit(originalCommit);
        return switch (inputs) {
            case Command value -> {
                yield new TargetSourceApplyRuntime.CommandControl(value.policy(),value.cancellations(),value.reservations(),value.schedules(),value.payloadProofs(),sampleHold(value.samples(),held),commit);
            }
            case Fence value -> new TargetSourceApplyRuntime.FenceControl(value.authority(), commit);
            case Expiry value -> new TargetSourceApplyRuntime.ExpiryControl(value.authority(), commit);
            case Admission value -> new TargetSourceApplyRuntime.AdmissionControl(value.authority(),sampleHold(value.samples(),held),commit);
            case Outcome value -> new TargetSourceApplyRuntime.OutcomeControl(value.authority(), commit);
            case Retirement value -> retirement(entry, value, held, commit, guard);
            case Confirmation value -> confirmation(entry,value,held,commit,guard);
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
                });
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
                };
                yield new TargetSourceApplyRuntime.SloWriterControl(value.prepared(),
                        new com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.Authority(frozen,
                                (version,source)->frozen.key(version,source),a.actor(),a.scopeProof(),material),commit);
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
                };
                yield new TargetSourceApplyRuntime.SloObservationControl(value.prepared(),
                        new TargetSloObservationVerifier.Authority(frozen, (version, source) -> frozen.key(version, source),
                                a.actor(), a.scopeProof(), material), commit);
            }
        };
    }

    private static TargetSloObservationSamples.Authority sampleHold(TargetSloObservationSamples.Authority original,Held held){
        return new TargetSloObservationSamples.Authority(){
            public void requireSample(TargetSloObservationGeneration generation,TargetSloObservationSample sample,long bytes){
                requireCurrent();original.requireSample(generation,sample,bytes);requireCurrent();
            }
            public void requireCurrent(){held.requireCurrent();original.requireCurrent();held.requireCurrent();}
        };
    }

    private static TargetSourceAuthorityPreparation.Family family(Inputs inputs) {
        return switch (inputs) {
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
            case Retirement ignored -> TargetSourceAuthorityPreparation.Family.RETIREMENT;
            case Confirmation ignored -> TargetSourceAuthorityPreparation.Family.RETIREMENT;
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
        }
        if(held.bytes>maximumBytes)throw new IllegalStateException("reclaim history exceeds complete Source input byte bound");
        return new TargetSourceApplyRuntime.ReclaimControl(new TargetResourceReclaimStore.Authority(){
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
            Held held, TargetStoreBackend.CommitAuthority commit, Runnable guard) {
        if (!(entry instanceof SourceReplayMutation original) || original.mutation().type() != SystemMutationType.RESOURCE_RETIRE_INTENT)
            throw new IllegalArgumentException("retirement snapshot changed its original Source operation");
        final var mutation = original.mutation();
        final var body = TargetEvidenceRetireIntentBody.decode(mutation.canonicalBody());
        input.scope().requireRoute(body.shard(), body.resource().tenantScope());
        final var originalHistory=Objects.requireNonNull(input.history(),"complete original retirement Source proof/pin");
        originalHistory.select(body.resource(),mutation,entry.position());held.requireCurrent();guard.run();
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
        return new TargetSourceApplyRuntime.RetirementControl(new TargetEvidenceRetirementStore.Authority(){
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
            Held held,TargetStoreBackend.CommitAuthority commit,Runnable guard){
        if(!(entry instanceof SourceReplayMutation original)||original.mutation().type()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED)
            throw new IllegalArgumentException("confirmation snapshot changed its original Source operation");
        final var mutation=original.mutation();final var body=TargetEvidenceDeleteConfirmedBody.decode(mutation.canonicalBody());
        input.scope().requireRoute(body.shard(),body.resource().tenantScope());
        final var prior=Objects.requireNonNull(input.history());prior.select(body.resource(),mutation,entry.position());
        held.requireCurrent();guard.run();final var pin=prior.pin();
        held.bytes=Math.addExact(held.bytes,Math.addExact(prior.proof().canonicalBytes().length,
                Math.addExact(pin.authority().canonicalBytes().length,pin.key().getEncoded().length+256L)));
        if(held.bytes>maximumBytes)throw new IllegalStateException("complete confirmation history exceeds Source input bound");
        final var history=new TargetEvidenceRetirementStore.SourceHistory(prior.proof(),new TargetRetirementAuthorizationHistory.Pin(
                pin.authority(),pin.keyVersion(),pin.key(),pin.baselineDigest(),pin.root(),pin.namespace()),()->{
            held.requireCurrent();prior.current().run();held.requireCurrent();
        });
        final var approved=input.approval();final TargetEvidenceRetirementStore.ConfirmationApproval frozen;
        if(approved==null)frozen=null;
        else{
            approved.current().run();held.requireCurrent();guard.run();
            final var observed=approved.observation();final var historical=approved.historical();final byte[] encoded=approved.writerKey().getEncoded();
            if(encoded==null||encoded.length!=44)throw new IllegalStateException("confirmation writer requires original Ed25519 key");
            final long upper=Math.addExact(observed!=null?observed.retainedBytes():historical.retainedBytes(),encoded.length);
            if(upper>maximumBytes-held.bytes)throw new IllegalStateException("complete confirmation receipt exceeds reserved Source inputs");
            approved.requireSource(mutation,entry.position());approved.requireConfirmation(body);
            if(historical!=null)historical.requireEntry(original);final PublicKey key;
            try{key=KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));}
            catch(java.security.GeneralSecurityException invalid){throw new IllegalStateException("invalid original confirmation Source key",invalid);}
            held.bytes=Math.addExact(held.bytes,upper);
            frozen=new TargetEvidenceRetirementStore.ConfirmationApproval(key,observed,historical,()->{
                held.requireCurrent();approved.current().run();held.requireCurrent();
            },approved.activatedTuple(),approved.historyGrant());
        }
        held.requireCurrent();guard.run();
        return new TargetSourceApplyRuntime.RetirementControl(new TargetEvidenceRetirementStore.Authority(){
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
                requireEntry(scope,actor,actual,source);if(frozen!=null)frozen.current().run();return frozen;
            }
            public TargetEvidenceRetirementStore.SourceHistory history(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation actual,SourcePosition source){
                requireEntry(scope,actor,actual,source);history.current().run();return history;
            }
        },commit);
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

    private static final class Held implements TargetSourceAuthorityPreparation.Held {
        private final SourceReplayEntry entry;
        private final HeldInputs original;
        private final byte[] digest;
        private Object control;
        private TargetQuotaCapacitySnapshot.Verified capacity;
        private CheckpointInputFuture.Cleanup pendingCapacityCleanup;
        private TargetSloControlScopeAuthority.MemberHold scopedControl;
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
        @Override public long retainedBytes() { return bytes; }
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
