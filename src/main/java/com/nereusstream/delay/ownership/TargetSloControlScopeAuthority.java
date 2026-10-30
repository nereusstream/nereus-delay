package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.security.SecureRandom;
import com.nereusstream.delay.runtime.TrustedUtcClock;
import com.nereusstream.delay.runtime.TrustedUtcInterval;

/** Bounded E4 admission on the original registry. Only an observed MEMBER CAS admits an exact Source frame. */
public final class TargetSloControlScopeAuthority {
    @FunctionalInterface public interface Protections {
        /** Original namespace/history/resource material; acquire within this bound and retain partial Cleanup. */
        Protection acquire(TargetSloControlScopeHeader.Reference reference,long maximumRetainedBytes,Runnable guard);
    }
    public interface Protection extends AutoCloseable {
        // Acquire all material first; subsequent methods are local and retain the same fixed byte envelope.
        TargetSloControlScopeHeader header();
        long retainedBytes();
        /** Already-held local checks only. Neither a root nor its digest grants namespace or retention authority. */
        void requireCurrent();
        /** Independent original manager and first-birth history; never inferred from missing registry records. */
        default void requireBirth(){throw new IllegalStateException("original Scope birth authority is unavailable");}
        /** Independent manager; SERVICE, Owner exit and TIME_FENCE authority cannot seal a registry Scope. */
        default void requireSeal(TargetSloControlScopeRoot original){throw new IllegalStateException("original Scope seal authority is unavailable");}
        /** Independent manager authority. Closing issuance neither exits old permits nor releases unused resources. */
        default void requireCloseIssuance(TargetSloControlScopeRoot original){throw new IllegalStateException("original exposure issuance authority is unavailable");}
        /** Actual SDK-frame proof is borrowed from the joint loan owner, never reconstructed from caller DTOs. */
        default void requireNativeExposure(TargetSourceApplyRuntime.NativeExposureHold actual){
            throw new IllegalStateException("actual Native SDK exposure frame authority is unavailable");
        }
        /**
         * Already-held actual Native64 payment, original input/writer, SDK expansion, physical Source,
         * retention and closure margins. A policy or SDK contract alone cannot implement this authority.
         * Returned UTC intervals must come from the original qualified clock and require no remote I/O.
         */
        default ExposureQualification requireExposure(TargetSloControlScopeRoot original,SystemMutation exact,
                TargetScopeExposureDebit.Origin origin,TargetScopeExposureSource.CopyContract sdk){
            throw new IllegalStateException("actual paid Source exposure qualification is unavailable");
        }
        /** Original signatures, authorized alias/protocol range and prepaid storage/retention, including orphans. */
        void requireInput(TargetSloControlRegisteredInput original);
        /** Resolve all SERVICE ranges introduced or referenced by this original input; empty preserves UNKNOWN. */
        default OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput original){return OptionalLong.empty();}
        /** Local read-loan release only; no physical UNKNOWN, pending slot or durable resource refund. */
        @Override void close();
    }

    public interface ExposureQualification {
        TargetScopeExposurePolicy.Kind kind();
        /** Local live projection; returning old evidence as current time is not permitted. */
        TrustedUtcInterval currentUtcInterval();
        /** Recheck the same held origin, paid plan and actual SDK/physical expansion contract locally. */
        void requireCurrent();
        /** Recheck the actual SDK invocation against the independently granted frozen expansion contract locally. */
        default void requireContract(TargetScopeExposureSource.CopyContract sdk){
            throw new IllegalStateException("actual SDK exposure expansion contract authority is unavailable");
        }
        /** Validate the complete proposed transition under the same actual Native/Source coverage. The root view is borrowed. */
        default void requireDebitTransition(RootHold expected,TargetSloControlScopeRoot next,TargetScopeExposureDebit debit){
            throw new IllegalStateException("actual Source coverage for the original exposure debit transition is unavailable");
        }
        /** Requalify the actual observed own debit and remaining coverage; an old proposal cannot stand in for this proof. */
        default void requireCommittedDebit(RootHold expected,RootHold observed,TargetScopeExposureDebit debit){
            throw new IllegalStateException("actual committed Source exposure debit coverage is unavailable");
        }
        /**
         * Local capture from the actual clock approved by this independently held resource authority.
         * Raw interval DTOs cannot mint its custody continuity token or supply time-source authority.
         */
        default TrustedUtcClock.CustodyTime captureHandoffTime(){
            throw new IllegalStateException("actual qualified SDK custody clock authority is unavailable");
        }
        /**
         * Compatibility hook. Actual custody is bracketed by captures around the native SDK's
         * ownership transition; this method cannot promise an unobserved future handoff window.
         */
        default void requireHandoffWindow(TargetScopeExposurePolicy policy,long chargedWindow){
            throw new IllegalStateException("actual SDK handoff window protection is unavailable");
        }
    }

    record RootRecord(TargetSloControlScopeRoot value,long versionId,Long birthVersionId) {
        RootRecord(TargetSloControlScopeRoot value,long versionId){this(value,versionId,null);}
        RootRecord {Objects.requireNonNull(value);if(versionId<0||versionId==Long.MAX_VALUE
                ||birthVersionId!=null&&(birthVersionId<0||birthVersionId==Long.MAX_VALUE))throw new IllegalArgumentException("actual root/birth versions required");}
    }
    record InputRecord(TargetSloControlRegisteredInput value,long versionId) {
        InputRecord {Objects.requireNonNull(value);if(versionId<0||versionId==Long.MAX_VALUE)throw new IllegalArgumentException("actual entry version required");}
    }
    interface Storage {
        byte[] namespaceBinding();
        String keyPrefix();
        RootRecord readRoot(TargetSloControlScopeHeader.Reference reference);
        RootRecord birth(TargetSloControlScopeHeader header);
        RootRecord compareRoot(RootRecord expected,TargetSloControlScopeRoot next);
        InputRecord readInput(byte[] operationId);
        InputRecord putInput(TargetSloControlRegisteredInput exact);
        void requireCurrent();
    }
    private final Storage storage;
    private final Protections protections;
    private final SecureRandom attempts=new SecureRandom();
    TargetSloControlScopeAuthority(Storage storage,Protections protections){
        this.storage=Objects.requireNonNull(storage);this.protections=Objects.requireNonNull(protections);
    }
    public static Optional<TargetSloControlScopeHeader.Reference> reference(SystemMutation mutation){
        if(TargetSloWriterControlBody.isControl(mutation))return Optional.ofNullable(TargetSloWriterControlBody.decode(mutation.canonicalBody()).admissionScope());
        if(TargetSloObservationControlBody.isControl(mutation))return Optional.ofNullable(TargetSloObservationControlBody.decode(mutation.canonicalBody()).admissionScope());
        return Optional.empty();
    }

    public RootHold birth(TargetSloControlScopeHeader header,long maximumBytes,Runnable guard){
        final var protection=acquire(header.reference(),maximumBytes,guard,false);
        try{
            requireHeader(protection,header);protection.requireBirth();check(protection,guard);
            final var root=Objects.requireNonNull(storage.birth(header),"actual Scope birth");
            requireRoot(root,header);check(protection,guard);
            return new RootHold(root,protection,rootBytes(root,protection));
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    /** Bounded original registry snapshot, including its actual Root2 birth marker; no issuance or mutation. */
    public RootHold captureRoot(TargetSloControlScopeHeader.Reference reference,long maximumBytes,Runnable guard){
        final var protection=acquire(reference,maximumBytes,guard,false);
        try{
            final var root=requireRoot(storage.readRoot(reference),protection.header());
            check(protection,guard);
            final long bytes=rootBytes(root,protection);
            if(bytes>maximumBytes)throw new IllegalStateException("original Scope root exceeds its bounded snapshot hold");
            return new RootHold(root,protection,bytes);
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    public RootHold seal(TargetSloControlScopeHeader.Reference reference,long maximumBytes,Runnable guard){
        final var protection=acquire(reference,maximumBytes,guard,false);
        try{
            var root=requireRoot(storage.readRoot(reference),protection.header());
            protection.requireSeal(root.value());check(protection,guard);
            if(!root.value().sealed()){
                root=Objects.requireNonNull(storage.compareRoot(root,root.value().seal()),"actual Scope seal CAS");
                requireRoot(root,protection.header());
                if(!root.value().sealed())throw new ConcurrentModificationException("original Scope seal must retry the same root");
            }
            check(protection,guard);return new RootHold(root,protection,rootBytes(root,protection));
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    public RootHold closeExposureIssuance(TargetSloControlScopeHeader.Reference reference,long maximumBytes,Runnable guard){
        final var protection=acquire(reference,maximumBytes,guard,false);
        try{
            var root=requireRoot(storage.readRoot(reference),protection.header());
            protection.requireCloseIssuance(root.value());check(protection,guard);
            if(!root.value().issuanceClosed()){
                root=requireRoot(storage.compareRoot(root,root.value().closeIssuance()),protection.header());
                if(!root.value().issuanceClosed())throw new ConcurrentModificationException("original issuance close must retry the same root");
            }
            check(protection,guard);return new RootHold(root,protection,rootBytes(root,protection));
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    /**
     * One new append invocation, one privately assigned durable debit. A response loss cannot be
     * repaired by restoring a permit from an old slot. Callers retain the charged UNKNOWN instead.
     */
    public ExposurePermit acquireExposure(TargetSourceApplyRuntime.NativeExposureHold nativeInputs,
            TargetScopeExposureSource.CopyContract sdk,long maximumBytes,Runnable guard){
        final var protection=new NativeExposureProtection(Objects.requireNonNull(nativeInputs,"actual Native exposure inputs"));
        try{
            Objects.requireNonNull(sdk);Objects.requireNonNull(guard);
            if(maximumBytes<1||maximumBytes==Long.MAX_VALUE)
                throw new IllegalArgumentException("finite original exposure input reservation required");
            nativeInputs.requireLocalCurrent();
            nativeInputs.requireExposureFrame();
            if(nativeInputs.legacyUnscoped())
                throw new TargetSourceAuthorityPreparation.InputUnavailable("actual legacy-unscoped writer requires its original legacy exposure authority");
            final var reference=nativeInputs.reference();
            final var paid=nativeInputs.paidScope();
            if(reference==null||paid==null)
                throw new TargetSourceAuthorityPreparation.InputUnavailable("actual Native exposure Scope payment is unavailable");
            final var header=paid.definition().header();
            if(header.schemaVersion()!=TargetSloControlScopeHeader.VERSION
                    ||paid.definition().plan().schemaVersion()!=TargetSloScopeNativePlan.EXPOSURE_VERSION
                    ||!reference.equals(paid.reference())||!reference.equals(header.reference())
                    ||!nativeInputs.scope().equals(header.scope())||!Arrays.equals(nativeInputs.lineage(),header.lineage()))
                throw new IllegalArgumentException("actual Native exposure changed its original Header2/Plan3 Scope payment");
            final var exact=nativeInputs.exactInput();
            if(!Arrays.equals(exact.encodeFrame(),nativeInputs.exactFrame()))
                throw new IllegalArgumentException("actual Native exposure changed its complete original frame");
            final long nativeBytes=nativeInputs.retainedBytes();
            if(nativeBytes<0||nativeBytes>=maximumBytes)
                throw new IllegalArgumentException("actual Native exposure cannot fit its original acquisition bound");
            protection.attach(acquire(reference,Math.subtractExact(maximumBytes,nativeBytes),guard,false));
            requireHeader(protection,header);paid.definition().plan().requireHeader(protection.header());
            final var root=requireRoot(storage.readRoot(reference),header);
            long retained=rootBytes(root,protection);
            final TargetScopeExposureDebit.Origin origin;
            if(exact.type()==SystemMutationType.TARGET_SLO_OBSERVATION){
                final var writer=Objects.requireNonNull(nativeInputs.writerEpoch(),"actual original Native writer epoch");
                final var body=TargetSloObservationEventBody.decode(exact.canonicalBody());
                if(!reference.equals(writer.register().body().admissionScope())||!writer.reference().equals(body.writerEpoch()))
                    throw new IllegalArgumentException("SERVICE exposure changed its actual original REGISTER/Scope reference");
                origin=TargetScopeExposureDebit.Origin.serviceWriter(writer.reference());
            }else{
                if(!reference.equals(reference(exact).orElse(null)))
                    throw new IllegalArgumentException("Control exposure changed its exact Native admission Scope");
                final byte[] operationId;
                if(TargetSloWriterControlBody.isControl(exact))
                    operationId=TargetSloWriterControlBody.decode(exact.canonicalBody()).controlRef().operationId();
                else if(TargetSloObservationControlBody.isControl(exact))
                    operationId=TargetSloObservationControlBody.decode(exact.canonicalBody()).controlRef().operationId();
                else throw new IllegalArgumentException("actual Native exposure is not an original SLO Control frame");
                final var stored=Objects.requireNonNull(storage.readInput(operationId),"exposure lost its exact immutable Control MEMBER");
                final var input=stored.value();input.requireHeader(header);
                root.value().requireMember(input,stored.versionId());
                if(!Arrays.equals(operationId,input.operationId())||!Arrays.equals(exact.encodeFrame(),input.mutation().encodeFrame()))
                    throw new IllegalArgumentException("exposure changed its exact original Control MEMBER/frame");
                protection.requireInput(input);retained=Math.addExact(retained,input.entryBytes());
                origin=TargetScopeExposureDebit.Origin.controlMember(input.slot(),input.digest());
            }
            return issueExposure(root,protection,exact,origin,sdk,maximumBytes,guard,retained);
        }catch(RuntimeException|Error failure){
            protection.retainAcquisitionFailure(failure);
            throw retainFailure(protection,failure);
        }
    }

    /** Compatibility entry point; production issuance consumes the private actual Native hold above. */
    public ExposurePermit acquireExposure(TargetSloControlScopeHeader.Reference reference,SystemMutation exact,
            TargetScopeExposureDebit.Origin origin,TargetScopeExposureSource.CopyContract sdk,long maximumBytes,Runnable guard){
        Objects.requireNonNull(exact);Objects.requireNonNull(origin);Objects.requireNonNull(sdk);
        final var protection=acquire(reference,maximumBytes,guard,false);
        try{
            final var root=requireRoot(storage.readRoot(reference),protection.header());
            if(root.value().schemaVersion()!=TargetSloControlScopeRoot.VERSION||root.value().issuanceClosed())
                throw new IllegalStateException("original Scope has no open paid exposure issuance");
            long retained=rootBytes(root,protection);
            if(origin instanceof TargetScopeExposureDebit.ControlMember member){
                final var slot=root.value().member(member.memberSlot());
                final var stored=Objects.requireNonNull(storage.readInput(slot.operationId()),"exposure lost its exact immutable Control MEMBER");
                root.value().requireMember(stored.value(),stored.versionId());
                if(!Arrays.equals(member.registeredInputDigest(),stored.value().digest())
                        ||!Arrays.equals(exact.encodeFrame(),stored.value().mutation().encodeFrame()))
                    throw new IllegalArgumentException("exposure changed its exact original Control MEMBER/frame");
                protection.requireInput(stored.value());retained=Math.addExact(retained,stored.value().entryBytes());
            }else{
                if(exact.type()!=SystemMutationType.TARGET_SLO_OBSERVATION
                        ||!Objects.equals(((TargetScopeExposureDebit.ServiceWriter)origin).exactWriter(),
                        TargetSloObservationEventBody.decode(exact.canonicalBody()).writerEpoch()))
                    throw new IllegalArgumentException("SERVICE exposure changed its exact original writer reference");
            }
            return issueExposure(root,protection,exact,origin,sdk,maximumBytes,guard,retained);
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    private ExposurePermit issueExposure(RootRecord root,Protection protection,SystemMutation exact,
            TargetScopeExposureDebit.Origin origin,TargetScopeExposureSource.CopyContract sdk,long maximumBytes,Runnable guard,long retained){
        if(root.value().schemaVersion()!=TargetSloControlScopeRoot.VERSION||root.value().issuanceClosed())
            throw new IllegalStateException("original Scope has no open paid exposure issuance");
        final var qualification=Objects.requireNonNull(protection.requireExposure(root.value(),exact,origin,sdk));
        qualification.requireCurrent();check(protection,guard);
        final var policy=Objects.requireNonNull(root.value().header().exposurePolicy());
        final var interval=Objects.requireNonNull(qualification.currentUtcInterval());
        final long window=qualifiedWindow(policy,interval);
        final var frame=exact.encodeFrame();final byte[] attemptId=new byte[32];attempts.nextBytes(attemptId);
        final var debit=new TargetScopeExposureDebit(root.value().exposureDebits().size(),attemptId,Bytes.sha256(frame),
                origin,Objects.requireNonNull(qualification.kind()),sdk.maximumCopies(),frame.length,window,
                interval.earliestEpochMs(),sdk.digest());
        final var next=root.value().appendExposureDebit(debit);
        if(Math.addExact(retained,next.canonicalBytes().length)>maximumBytes)
            throw new IllegalStateException("exposure material exceeds its original bounded hold before debit CAS");
        final var expectedProof=new RootHold(root,protection,retained,true);
        qualification.requireDebitTransition(expectedProof,next,debit);
        qualification.requireCurrent();check(protection,guard);
        final var observed=requireRoot(storage.compareRoot(root,next),protection.header());
        if(!Objects.equals(observed.birthVersionId(),root.birthVersionId())
                ||observed.value().exposureDebits().size()<=debit.index()
                ||!observed.value().debit(debit.index()).equals(debit))
            throw new ConcurrentModificationException("own exact exposure debit was not confirmed; retain UNKNOWN without an SDK permit");
        final long bytes=Math.addExact(retained,observed.value().canonicalBytes().length);
        if(bytes>maximumBytes)throw new IllegalStateException("exposure material exceeds its original bounded hold");
        qualification.requireCommittedDebit(expectedProof,new RootHold(observed,protection,bytes,true),debit);
        qualification.requireCurrent();check(protection,guard);
        return new ExposurePermit(observed,protection,bytes,debit,sdk,qualification);
    }

    /** The actual Native loan and the independently acquired resource loan have one local SDK exit. */
    private static final class NativeExposureProtection implements Protection,CheckpointInputFuture.Cleanup {
        private final TargetSourceApplyRuntime.NativeExposureHold nativeInputs;
        private Protection resourceProtection;
        private CheckpointInputFuture.Cleanup resourceCleanup;
        private CompletableFuture<Void> resourceCleanupRetry;
        private CompletableFuture<Void> nativeCleanupRetry;
        private boolean nativeClosed,resourceClosed;
        private NativeExposureProtection(TargetSourceApplyRuntime.NativeExposureHold nativeInputs){this.nativeInputs=nativeInputs;}
        private void attach(Protection protection){resourceProtection=Objects.requireNonNull(protection);}
        private void retainAcquisitionFailure(Throwable failure){
            if(resourceProtection==null&&failure instanceof CheckpointInputFuture.Cleanup cleanup)resourceCleanup=cleanup;
        }
        @Override public TargetSloControlScopeHeader header(){return resourceProtection.header();}
        @Override public long retainedBytes(){return Math.addExact(nativeInputs.retainedBytes(),resourceProtection.retainedBytes());}
        @Override public synchronized void requireCurrent(){
            if(nativeClosed||resourceClosed||resourceProtection==null)
                throw new IllegalStateException("actual Native/resource exposure loan is unavailable or released");
            nativeInputs.requireLocalCurrent();resourceProtection.requireCurrent();nativeInputs.requireLocalCurrent();
        }
        @Override public void requireInput(TargetSloControlRegisteredInput original){resourceProtection.requireInput(original);}
        @Override public ExposureQualification requireExposure(TargetSloControlScopeRoot original,SystemMutation exact,
                TargetScopeExposureDebit.Origin origin,TargetScopeExposureSource.CopyContract sdk){
            resourceProtection.requireNativeExposure(nativeInputs);
            return resourceProtection.requireExposure(original,exact,origin,sdk);
        }
        @Override public synchronized void close(){
            Throwable failure=null;
            if(resourceProtection!=null&&!resourceClosed&&(resourceCleanup==null||resourceCleanup.cleanupComplete())){
                try{resourceProtection.close();resourceClosed=true;}
                catch(RuntimeException|Error close){
                    failure=close;
                    if(close instanceof CheckpointInputFuture.Cleanup cleanup)resourceCleanup=cleanup;
                }
            }
            if(!nativeClosed){
                try{nativeInputs.close();nativeClosed=true;}
                catch(RuntimeException|Error close){if(failure==null)failure=close;else if(failure!=close)failure.addSuppressed(close);}
            }
            if(resourceCleanup!=null&&!resourceCleanup.cleanupComplete()){
                final var pending=new IllegalStateException("original exposure resource cleanup remains retained");
                if(failure==null)failure=pending;else failure.addSuppressed(pending);
            }
            if(failure instanceof Error fatal)throw fatal;
            if(failure!=null)throw (RuntimeException)failure;
        }
        @Override public synchronized boolean cleanupComplete(){
            return nativeClosed&&(resourceProtection==null||resourceClosed)
                    &&(resourceCleanup==null||resourceCleanup.cleanupComplete());
        }
        @Override public synchronized CompletableFuture<Void> retryCleanup(){
            if(cleanupComplete())return CompletableFuture.completedFuture(null);
            final Throwable initialFailure;
            try{close();return CompletableFuture.completedFuture(null);}
            catch(RuntimeException|Error failure){initialFailure=failure;}
            boolean childRetry=false;
            CompletableFuture<Void> resourceRetry=CompletableFuture.completedFuture(null);
            if(resourceCleanup!=null&&!resourceCleanup.cleanupComplete()){
                childRetry=true;
                try{
                    if(resourceCleanupRetry==null||resourceCleanupRetry.isDone())
                        resourceCleanupRetry=Objects.requireNonNull(resourceCleanup.retryCleanup(),"original resource cleanup retry");
                }catch(RuntimeException|Error failure){resourceCleanupRetry=CompletableFuture.failedFuture(failure);}
                resourceRetry=resourceCleanupRetry;
            }
            CompletableFuture<Void> nativeRetry=CompletableFuture.completedFuture(null);
            if(!nativeClosed){
                childRetry=true;
                try{
                    if(nativeCleanupRetry==null||nativeCleanupRetry.isDone())
                        nativeCleanupRetry=Objects.requireNonNull(nativeInputs.retryCleanup(),"original Native cleanup retry");
                }catch(RuntimeException|Error failure){nativeCleanupRetry=CompletableFuture.failedFuture(failure);}
                nativeRetry=nativeCleanupRetry;
            }
            if(!childRetry)return CompletableFuture.failedFuture(initialFailure);
            return CompletableFuture.allOf(resourceRetry,nativeRetry).handle((ignored,retryFailure)->{
                try{close();return null;}
                catch(RuntimeException|Error close){
                    if(initialFailure!=close)close.addSuppressed(initialFailure);
                    if(retryFailure!=null&&retryFailure!=close&&retryFailure!=initialFailure)close.addSuppressed(retryFailure);
                    throw close;
                }
            });
        }
    }

    private static long qualifiedWindow(TargetScopeExposurePolicy policy,TrustedUtcInterval interval){
        if(!interval.qualified()||interval.latestEpochMs()==Long.MAX_VALUE)
            throw new IllegalStateException("exposure requires an actual qualified finite handoff interval");
        final long first=policy.windowIndex(interval.earliestEpochMs());
        if(first!=policy.windowIndex(interval.latestEpochMs()))
            throw new IllegalStateException("qualified handoff interval straddles exposure windows");
        return first;
    }

    /** Reservation CAS is not admission. Entry put alone cannot return this private MEMBER hold. */
    public MemberHold admit(TargetSloControlScopeHeader.Reference reference,PreparedControlOperation prepared,
            SystemMutation exactFrame,long maximumBytes,Runnable guard){
        final var protection=acquire(reference,maximumBytes,guard,false);
        try{
            var root=requireRoot(storage.readRoot(reference),protection.header());
            var slot=root.value().find(prepared.operationId()).orElse(null);
            final int index=slot==null?root.value().slots().size():slot.slotIndex();
            final var input=new TargetSloControlRegisteredInput(index,reference,prepared,exactFrame);
            input.requireHeader(protection.header());protection.requireInput(input);check(protection,guard);
            if(slot==null){
                final var reserved=root.value().reserve(input);
                root=requireRoot(storage.compareRoot(root,reserved),protection.header());
                slot=root.value().find(input.operationId()).orElseThrow(()->new ConcurrentModificationException("Scope slot reservation was not observed; retain original frame"));
            }
            slot.requireInput(input);
            final var stored=Objects.requireNonNull(storage.putInput(input),"original full signed registration");
            requireInput(stored,input);check(protection,guard);
            // A seal may have won after the reservation or immutable put. Never promote that PENDING slot.
            root=requireRoot(storage.readRoot(reference),protection.header());
            slot=root.value().slot(index);slot.requireInput(input);
            if(slot.state()!=TargetSloControlScopeRoot.SlotState.MEMBER){
                if(root.value().sealed())throw new IllegalStateException("sealed Scope retained a pending/orphan input without admitting it");
                root=requireRoot(storage.compareRoot(root,root.value().admit(index,stored.versionId())),protection.header());
            }
            root.value().requireMember(input,stored.versionId());check(protection,guard);
            return new MemberHold(root,protection,stored,Math.addExact(rootBytes(root,protection),input.entryBytes()),true);
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    /** Capture outside Store locks; the returned hold performs only local exact-frame validation at Source apply. */
    public MemberHold captureMember(TargetSloControlScopeHeader.Reference reference,PreparedControlOperation prepared,
            ControlTargetRef target,SystemMutation exactFrame,long maximumBytes,Runnable guard){
        final var protection=acquire(reference,maximumBytes,guard,false);
        try{
            final var root=requireRoot(storage.readRoot(reference),protection.header());
            final var stored=Objects.requireNonNull(storage.readInput(prepared.operationId()),"original immutable scoped registration");
            final var input=stored.value();input.requireHeader(protection.header());
            final var slot=root.value().slot(input.slot());slot.requireInput(input);
            final boolean admitted=slot.state()==TargetSloControlScopeRoot.SlotState.MEMBER;
            if(admitted)root.value().requireMember(input,stored.versionId());
            else if(!root.value().sealed())throw new IllegalStateException("OPEN Scope membership is still uncertain");
            requireExact(input,prepared,target,exactFrame);protection.requireInput(input);check(protection,guard);
            return new MemberHold(root,protection,stored,Math.addExact(rootBytes(root,protection),input.entryBytes()),admitted);
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    /** All MEMBER entries must be read. Missing material, partial work and PENDING slots never prove completion. */
    public SealedInputs readSealed(TargetSloControlScopeHeader.Reference reference,long maximumBytes,Runnable guard){
        final var protection=acquire(reference,maximumBytes,guard,true);
        try{
            final var root=requireRoot(storage.readRoot(reference),protection.header());
            if(!root.value().sealed())throw new IllegalStateException("complete Scope inputs require the actual sealed root");
            final var inputs=new ArrayList<TargetSloControlRegisteredInput>();
            long bytes=rootBytes(root,protection),deadline=0;boolean known=true;
            for(var slot:root.value().members()){
                check(protection,guard);
                final var stored=Objects.requireNonNull(storage.readInput(slot.operationId()),"sealed Scope lost an immutable MEMBER");
                final var input=stored.value();root.value().requireMember(input,stored.versionId());
                protection.requireInput(input);check(protection,guard);
                bytes=Math.addExact(bytes,input.entryBytes());
                if(bytes>maximumBytes)throw new IllegalStateException("complete sealed Scope exceeds its original reservation");
                inputs.add(input);deadline=Math.max(deadline,input.sourceRetryUntil());
                final var dependency=Objects.requireNonNull(protection.requiredServiceRetryUntil(input));
                if(dependency.isEmpty())known=false;
                else{
                    if(dependency.getAsLong()<0||dependency.getAsLong()==Long.MAX_VALUE)throw new IllegalArgumentException("finite original SERVICE deadline required");
                    deadline=Math.max(deadline,dependency.getAsLong());
                }
                if(input.prepared().request().branch() instanceof TargetSloWriterControlRequest writer&&writer.contract()!=null)
                    deadline=Math.max(deadline,writer.contract().maximumRetryUntil());
            }
            final var after=requireRoot(storage.readRoot(reference),protection.header());
            if(root.value().schemaVersion()==TargetSloControlScopeRoot.LEGACY_VERSION){
                if(after.versionId()!=root.versionId()||!Arrays.equals(after.value().canonicalBytes(),root.value().canonicalBytes()))
                    throw new IllegalStateException("sealed Scope root/version changed during complete original input read");
            }else{
                if(!Objects.equals(root.birthVersionId(),after.birthVersionId()))
                    throw new IllegalStateException("sealed Scope original birth changed during complete member read");
                root.value().requireSameSealedMembership(after.value());
            }
            check(protection,guard);
            return new SealedInputs(root,protection,bytes,inputs,known?OptionalLong.of(deadline):OptionalLong.empty());
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }

    private Protection acquire(TargetSloControlScopeHeader.Reference reference,long maximumBytes,Runnable guard,boolean complete){
        if(maximumBytes<1||maximumBytes==Long.MAX_VALUE)throw new IllegalArgumentException("finite original Scope input reservation required");
        Objects.requireNonNull(guard).run();storage.requireCurrent();
        final var protection=Objects.requireNonNull(protections.acquire(Objects.requireNonNull(reference),maximumBytes,guard));
        try{
            final var header=Objects.requireNonNull(protection.header());
            if(!reference.equals(header.reference())||!Arrays.equals(header.registryNamespaceBinding(),storage.namespaceBinding())
                    ||!header.registryKeyPrefix().equals(storage.keyPrefix()))throw new IllegalStateException("Scope material changed original registry namespace/header");
            final long retained=protection.retainedBytes();
            final long entries=complete?header.limits().maximumEntriesBytes():header.limits().maximumEntryBytes();
            final long required=Math.addExact(512L,Math.addExact(Math.multiplyExact(4L,header.limits().maximumRootBytes()),
                    Math.addExact(Math.multiplyExact(2L,entries),retained)));
            if(retained<0||required>maximumBytes)throw new IllegalArgumentException("Scope root/entry/protection material cannot fit its original acquisition bound");
            check(protection,guard);return protection;
        }catch(RuntimeException|Error failure){throw retainFailure(protection,failure);}
    }
    private void check(Protection protection,Runnable guard){guard.run();storage.requireCurrent();protection.requireCurrent();guard.run();}
    private static void requireHeader(Protection protection,TargetSloControlScopeHeader expected){
        if(!Arrays.equals(protection.header().canonicalBytes(),expected.canonicalBytes()))throw new IllegalStateException("Scope original header changed");
    }
    private static RootRecord requireRoot(RootRecord root,TargetSloControlScopeHeader expected){
        if(root==null)throw new IllegalStateException("original Scope root is unavailable; never recreate it during admission or recovery");
        if(!Arrays.equals(root.value().header().canonicalBytes(),expected.canonicalBytes()))throw new IllegalStateException("Scope root changed its immutable original header");
        if(root.value().schemaVersion()!=TargetSloControlScopeRoot.LEGACY_VERSION&&root.birthVersionId()==null)
            throw new IllegalStateException("Root2 lost its actual immutable birth identity");
        return root;
    }
    private static void requireInput(InputRecord stored,TargetSloControlRegisteredInput expected){
        if(!Arrays.equals(stored.value().canonicalBytes(),expected.canonicalBytes()))throw new IllegalStateException("immutable Scope entry changed original Prepared/frame");
    }
    private static void requireExact(TargetSloControlRegisteredInput original,PreparedControlOperation prepared,
            ControlTargetRef target,SystemMutation mutation){
        if(!Arrays.equals(original.prepared().canonicalBytes(),prepared.canonicalBytes())
                ||!Arrays.equals(original.mutation().encodeFrame(),mutation.encodeFrame()))throw new IllegalArgumentException("Scope admission is not this exact original registration/Source frame");
        ControlTargetMutationBinding.validate(prepared,target,mutation);
    }
    private static long rootBytes(RootRecord root,Protection protection){
        return Math.addExact(128L,Math.addExact(root.value().canonicalBytes().length,protection.retainedBytes()));
    }

    public static class RootHold implements AutoCloseable {
        private final RootRecord root;
        private final Protection protection;
        private final long bytes;
        private final long protectionBytes;
        private final boolean borrowed;
        private boolean closed;
        private RootHold(RootRecord root,Protection protection,long bytes){this(root,protection,bytes,false);}
        private RootHold(RootRecord root,Protection protection,long bytes,boolean borrowed){
            this.root=root;this.protection=protection;this.bytes=bytes;this.protectionBytes=protection.retainedBytes();this.borrowed=borrowed;
        }
        public final TargetSloControlScopeRoot root(){requireCurrent();return root.value();}
        /** Actual observed physical Root version, never an immutable Root2 seal version or current exposure balance. */
        public final long rootVersion(){requireCurrent();return root.versionId();}
        /** Actual immutable Root2 birth-marker version; the original Root1 domain has no marker. */
        public final OptionalLong birthVersion(){
            requireCurrent();return root.birthVersionId()==null?OptionalLong.empty():OptionalLong.of(root.birthVersionId());
        }
        public final long retainedBytes(){return bytes;}
        public synchronized void requireCurrent(){
            if(closed)throw new IllegalStateException("original Scope input hold is closed");
            if(protection.retainedBytes()!=protectionBytes||!root.value().header().reference().equals(protection.header().reference()))
                throw new IllegalStateException("original Scope protection changed its frozen header/byte envelope");
            protection.requireCurrent();
        }
        @Override public synchronized void close(){
            if(borrowed)throw new IllegalStateException("borrowed actual Scope root proof does not own its original protection");
            if(!closed){protection.close();closed=true;}
        }
    }
    public static final class ExposurePermit extends RootHold implements CheckpointInputFuture.Cleanup {
        private final Protection exposureProtection;
        private final TargetScopeExposureDebit debit;
        private final TargetScopeExposureSource.CopyContract sdk;
        private final ExposureQualification qualification;
        private TrustedUtcClock.CustodyTime beforeCustodyTime;
        private boolean claimAttempted,claimed,nativeCustodyStarted,nativeCustodyAfterAttempted,nativeCustodyQualified;
        private boolean localExited,cleanupComplete;
        private ExposurePermit(RootRecord root,Protection protection,long bytes,TargetScopeExposureDebit debit,
                TargetScopeExposureSource.CopyContract sdk,ExposureQualification qualification){
            super(root,protection,bytes);this.exposureProtection=protection;this.debit=debit;this.sdk=sdk;this.qualification=qualification;
        }
        public TargetScopeExposureDebit debit(){return debit;}
        /** One-shot pre-SDK validation only; the native SDK brackets its actual ownership separately. */
        public synchronized void claim(byte[] frame,TargetScopeExposureSource.CopyContract actual){
            if(claimAttempted||localExited)throw new IllegalStateException("exposure permit is one-shot for its original SDK invocation");
            claimAttempted=true;requireCurrent();qualification.requireCurrent();debit.requireFrame(frame);
            if(actual==null||sdk.maximumCopies()!=actual.maximumCopies()||!Arrays.equals(sdk.digest(),actual.digest())
                    ||qualification.kind()!=debit.kind()
                    ||qualifiedWindow(root().header().exposurePolicy(),Objects.requireNonNull(qualification.currentUtcInterval()))!=debit.windowIndex())
                throw new IllegalStateException("exposure SDK contract or actual handoff window changed after its debit");
            qualification.requireContract(actual);
            qualification.requireCurrent();requireCurrent();
            claimed=true;
        }
        /** Local callback immediately before the native SDK takes custody of this complete serialized record. */
        public synchronized void beforeSdkCustody(byte[] actualSerializedFrame){
            if(!claimed||nativeCustodyStarted||localExited)
                throw new IllegalStateException("native SDK custody requires its original successfully claimed one-shot permit");
            nativeCustodyStarted=true;requireCurrent();qualification.requireCurrent();debit.requireFrame(actualSerializedFrame);
            final var before=Objects.requireNonNull(qualification.captureHandoffTime(),"actual native SDK custody clock");
            if(qualification.kind()!=debit.kind()
                    ||qualifiedWindow(root().header().exposurePolicy(),before.interval())!=debit.windowIndex())
                throw new IllegalStateException("native SDK custody started outside its original charged exposure window");
            qualification.requireCurrent();requireCurrent();beforeCustodyTime=before;
        }
        /** Local callback after actual SDK custody, before the operation becomes visible to send/reconnect. */
        public synchronized void afterSdkCustody(){
            if(!claimed||beforeCustodyTime==null||nativeCustodyAfterAttempted||nativeCustodyQualified||localExited)
                throw new IllegalStateException("native SDK custody completion requires its original unconsumed before-custody capture");
            nativeCustodyAfterAttempted=true;requireCurrent();qualification.requireCurrent();
            final var after=Objects.requireNonNull(qualification.captureHandoffTime(),"actual native SDK custody clock");
            final var policy=root().header().exposurePolicy();
            if(qualification.kind()!=debit.kind()||qualifiedWindow(policy,beforeCustodyTime.interval())!=debit.windowIndex()
                    ||qualifiedWindow(policy,after.interval())!=debit.windowIndex())
                throw new IllegalStateException("native SDK custody crossed its original charged exposure window");
            qualification.requireCurrent();requireCurrent();beforeCustodyTime.requireContinuousTo(after);nativeCustodyQualified=true;
        }
        /** Only local SDK non-entry/exit releases this loan. Durable debit and physical obligations remain. */
        public synchronized void notInvoked(){
            if(nativeCustodyStarted)throw new IllegalStateException("native SDK custody has started; only actual SDK cleanup completion can release its loan");
            localExited=true;close();
        }
        public synchronized void sdkCompleted(){localExited=true;close();}
        @Override public synchronized void close(){
            if(!localExited)throw new IllegalStateException("original exposure SDK operation has not locally exited");
            try{super.close();cleanupComplete=true;}
            catch(RuntimeException|Error failure){throw new ExposureCleanupFailure(this,failure);}
        }
        @Override public synchronized boolean cleanupComplete(){return cleanupComplete;}
        @Override public synchronized CompletableFuture<Void> retryCleanup(){
            if(localExited&&exposureProtection instanceof NativeExposureProtection original){
                try{return original.retryCleanup().thenRun(this::close);}
                catch(RuntimeException|Error failure){return CompletableFuture.failedFuture(failure);}
            }
            try{close();return CompletableFuture.completedFuture(null);}
            catch(RuntimeException|Error failure){return CompletableFuture.failedFuture(failure);}
        }
    }
    private static final class ExposureCleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID=1L;
        private final transient ExposurePermit permit;
        private ExposureCleanupFailure(ExposurePermit permit,Throwable failure){super("original exposure read-loan cleanup retained",failure);this.permit=permit;}
        public boolean cleanupComplete(){return permit.cleanupComplete();}
        public CompletableFuture<Void> retryCleanup(){return permit.retryCleanup();}
    }
    public static final class MemberHold extends RootHold {
        private final InputRecord input;
        private final boolean admitted;
        private MemberHold(RootRecord root,Protection protection,InputRecord input,long bytes,boolean admitted){super(root,protection,bytes);this.input=input;this.admitted=admitted;}
        public TargetSloControlRegisteredInput input(){requireCurrent();return input.value();}
        public void validate(PreparedControlOperation prepared,ControlTargetRef target,SystemMutation mutation){
            requireCurrent();requireExact(input.value(),prepared,target,mutation);
            if(!admitted)throw new com.nereusstream.delay.runtime.CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                    "the actual sealed Scope never admitted this retained pending input");
            root().requireMember(input.value(),input.versionId());requireCurrent();
        }
    }
    public static final class SealedInputs extends RootHold {
        private final List<TargetSloControlRegisteredInput> inputs;
        private final OptionalLong maximumRetryUntil;
        private SealedInputs(RootRecord root,Protection protection,long bytes,List<TargetSloControlRegisteredInput> inputs,OptionalLong maximumRetryUntil){
            super(root,protection,bytes);this.inputs=List.copyOf(inputs);this.maximumRetryUntil=maximumRetryUntil;
        }
        public List<TargetSloControlRegisteredInput> inputs(){requireCurrent();return inputs;}
        public long membershipRevision(){return root().membershipRevision();}
        /** This root's complete MEMBER range only; no claim about older/unscoped domains or physical UNKNOWN. */
        public OptionalLong maximumRetryUntil(){requireCurrent();return maximumRetryUntil;}
    }

    private static RuntimeException retainFailure(Protection original,Throwable failure){
        try{original.close();}catch(RuntimeException|Error close){if(failure!=close)failure.addSuppressed(close);return new OpeningFailure(original,failure,close);}
        if(failure instanceof Error fatal)throw fatal;
        return (RuntimeException)failure;
    }
    private static final class OpeningFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID=1L;
        private final transient Protection original;
        private transient CheckpointInputFuture.Cleanup resourceCleanup;
        private transient CompletableFuture<Void> resourceCleanupRetry;
        private boolean complete;
        private OpeningFailure(Protection original,Throwable failure,Throwable close){
            super("original Scope read-loan cleanup retained",failure);this.original=original;
            if(close instanceof CheckpointInputFuture.Cleanup cleanup)resourceCleanup=cleanup;
        }
        public synchronized boolean cleanupComplete(){return complete;}
        public synchronized CompletableFuture<Void> retryCleanup(){
            if(complete)return CompletableFuture.completedFuture(null);
            if(original instanceof NativeExposureProtection cleanup){
                try{
                    return cleanup.retryCleanup().thenRun(()->{
                        synchronized(this){
                            if(!cleanup.cleanupComplete())throw new IllegalStateException("original Scope read-loan cleanup remains retained");
                            complete=true;
                        }
                    });
                }catch(RuntimeException|Error failure){return CompletableFuture.failedFuture(failure);}
            }
            if(resourceCleanup!=null&&!resourceCleanup.cleanupComplete()){
                try{
                    if(resourceCleanupRetry==null||resourceCleanupRetry.isDone())
                        resourceCleanupRetry=Objects.requireNonNull(resourceCleanup.retryCleanup(),"original Scope resource cleanup retry");
                    return resourceCleanupRetry.handle((ignored,retryFailure)->{
                        synchronized(this){
                            if(!resourceCleanup.cleanupComplete()){
                                final var pending=new IllegalStateException("original Scope resource cleanup remains retained");
                                if(retryFailure!=null)pending.addSuppressed(retryFailure);
                                throw pending;
                            }
                            closeOriginal();return null;
                        }
                    });
                }catch(RuntimeException|Error failure){return CompletableFuture.failedFuture(failure);}
            }
            try{closeOriginal();return CompletableFuture.completedFuture(null);}
            catch(RuntimeException|Error failure){return CompletableFuture.failedFuture(failure);}
        }
        private void closeOriginal(){
            try{original.close();complete=true;}
            catch(RuntimeException|Error failure){
                if(failure instanceof CheckpointInputFuture.Cleanup cleanup){resourceCleanup=cleanup;resourceCleanupRetry=null;}
                throw failure;
            }
        }
    }
}
