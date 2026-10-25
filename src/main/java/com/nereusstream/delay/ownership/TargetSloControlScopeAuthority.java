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
        /** Original signatures, authorized alias/protocol range and prepaid storage/retention, including orphans. */
        void requireInput(TargetSloControlRegisteredInput original);
        /** Resolve all SERVICE ranges introduced or referenced by this original input; empty preserves UNKNOWN. */
        default OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput original){return OptionalLong.empty();}
        /** Local read-loan release only; no physical UNKNOWN, pending slot or durable resource refund. */
        @Override void close();
    }

    record RootRecord(TargetSloControlScopeRoot value,long versionId) {
        RootRecord {Objects.requireNonNull(value);if(versionId<0||versionId==Long.MAX_VALUE)throw new IllegalArgumentException("actual root version required");}
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
            if(after.versionId()!=root.versionId()||!Arrays.equals(after.value().canonicalBytes(),root.value().canonicalBytes()))
                throw new IllegalStateException("sealed Scope root/version changed during complete original input read");
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
        private boolean closed;
        private RootHold(RootRecord root,Protection protection,long bytes){this.root=root;this.protection=protection;this.bytes=bytes;this.protectionBytes=protection.retainedBytes();}
        public final TargetSloControlScopeRoot root(){requireCurrent();return root.value();}
        public final long rootVersion(){requireCurrent();return root.versionId();}
        public final long retainedBytes(){return bytes;}
        public synchronized void requireCurrent(){
            if(closed)throw new IllegalStateException("original Scope input hold is closed");
            if(protection.retainedBytes()!=protectionBytes||!root.value().header().reference().equals(protection.header().reference()))
                throw new IllegalStateException("original Scope protection changed its frozen header/byte envelope");
            protection.requireCurrent();
        }
        @Override public synchronized void close(){if(!closed){protection.close();closed=true;}}
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
        /** This root's complete MEMBER range only; no claim about older/unscoped domains or physical UNKNOWN. */
        public OptionalLong maximumRetryUntil(){requireCurrent();return maximumRetryUntil;}
    }

    private static RuntimeException retainFailure(Protection original,Throwable failure){
        try{original.close();}catch(RuntimeException|Error close){failure.addSuppressed(close);return new OpeningFailure(original,failure);}
        if(failure instanceof Error fatal)throw fatal;
        return (RuntimeException)failure;
    }
    private static final class OpeningFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID=1L;
        private final transient Protection original;
        private boolean complete;
        private OpeningFailure(Protection original,Throwable failure){super("original Scope read-loan cleanup retained",failure);this.original=original;}
        public synchronized boolean cleanupComplete(){return complete;}
        public synchronized CompletableFuture<Void> retryCleanup(){
            if(complete)return CompletableFuture.completedFuture(null);
            try{original.close();complete=true;return CompletableFuture.completedFuture(null);}
            catch(RuntimeException|Error failure){return CompletableFuture.failedFuture(failure);}
        }
    }
}
