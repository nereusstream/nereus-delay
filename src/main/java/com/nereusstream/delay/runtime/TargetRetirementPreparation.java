package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.*;
import java.util.function.LongSupplier;

/** One original retirement input's protected Native inventory. This is neither an authority nor a durable summary. */
public final class TargetRetirementPreparation {
    /** The original admission must reserve the whole flight, including unsuccessful reads and retained copies. */
    public record Capacity(long rows,long readBytes,long copyBytes,long retainedBytes,long holdNanos) {
        public Capacity {
            if(rows<1||rows==Long.MAX_VALUE||readBytes<1||readBytes==Long.MAX_VALUE
                    ||copyBytes<1||copyBytes==Long.MAX_VALUE||retainedBytes<1||retainedBytes==Long.MAX_VALUE
                    ||holdNanos<1||holdNanos==Long.MAX_VALUE)
                throw new IllegalArgumentException("original finite cumulative retirement reservation required");
        }
    }

    /** Deployment-owned original Root/Owner/reader/Pin/payment/retention admission, held through Source commit. */
    public interface Protection {
        Capacity capacity();
        void requireCurrent();
        /** Pure local check of this original loan's actual Store/Root/accounting/cut and exact Source request. */
        void requireBinding(StoreMetadata actual,TargetQuotaIncarnation actualRoot,TargetQuotaMutation actualCut,
                SystemMutation exact,SourcePosition selectedSource);
    }

    public static final class ViewChanged extends IllegalStateException {
        private static final long serialVersionUID=1L;
        public ViewChanged(){super("original retirement Native view changed; discard and prepare the exact input again");}
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage,inputBytes,sourceBytes;
    private final SystemMutation input;
    private final TargetStoreBackend.ReadAuthority authority;
    private final Protection protection;
    private final Capacity capacity;
    private final LongSupplier clock;
    private final long started;
    private final boolean allowApplied;
    private TargetPayloadObjectRetiringRecord pendingObject;
    private TargetRetirementCursor cursor;
    private final boolean needsLedger;
    private TargetRetirementLedgerCursor ledger;
    private OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery;
    private OxiaSyncCheckpointPublicationBackend catalog;
    private TargetInlinePayloadReclaim.RetirementCursor inlineClosure;
    private boolean ledgerComplete,closureComplete;
    private byte[] metadata,cut,aggregate,root;
    private long nativeSequence,sourceSequence,rows,readBytes,copyBytes,retainedBytes;
    private long generation;
    private boolean captured,closed,advancing,exhausted;
    private Proof proof;

    private TargetRetirementPreparation(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,
            SystemMutation input,SourcePosition source,TargetStoreBackend.ReadAuthority authority,Protection protection,LongSupplier clock,boolean allowApplied){
        this.backend=Objects.requireNonNull(backend);this.scope=Objects.requireNonNull(scope);
        Bytes.requireLength(lineage,16,"retirement preparation lineage");this.lineage=Bytes.copy(lineage);
        this.input=Objects.requireNonNull(input);inputBytes=input.canonicalEnvelope();sourceBytes=source==null?null:source.canonicalBytes();
        this.authority=Objects.requireNonNull(authority);this.protection=Objects.requireNonNull(protection);this.allowApplied=allowApplied;
        protection.requireCurrent();capacity=Objects.requireNonNull(protection.capacity());this.clock=Objects.requireNonNull(clock);started=clock.getAsLong();
        if(scope.target()!=null||!input.shardId().equals(scope.shard())||source!=null&&!source.shardId().equals(scope.shard()))
            throw new IllegalArgumentException("retirement preparation changed actual Source scope");
        cursor=TargetRetirementCursor.forInput(scope,this.lineage,input);
        final var body=input.type()==SystemMutationType.TARGET_RESOURCE_RECLAIM?TargetResourceReclaimBody.decode(input.canonicalBody()):null;
        needsLedger=body!=null&&(body.incarnationReclaim()||source==null&&(body.objectPayloadReclaim()||body.inlinePayloadRelease()||body.nativeRetainedReclaim()));
        final long inputWindow=Math.addExact(inputBytes.length,sourceBytes==null?0:sourceBytes.length);retain(inputWindow);copy(inputWindow);
    }

    /** Called only after retained-first and bounded rejection probes, from the original owned I/O action. */
    public static TargetRetirementPreparation begin(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,
            SystemMutation input,SourcePosition source,TargetStoreBackend.ReadAuthority authority,Protection protection,LongSupplier clock){
        return new TargetRetirementPreparation(backend,scope,lineage,input,source,authority,protection,clock,false);
    }
    public static TargetRetirementPreparation beginPending(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,
            TargetPayloadObjectRetiringRecord pending,TargetStoreBackend.ReadAuthority authority,Protection protection,LongSupplier clock,
            OxiaSyncCheckpointPublicationBackend catalog,OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery){
        Objects.requireNonNull(recovery).requirePendingObject(Objects.requireNonNull(pending));
        final var prepared=new TargetRetirementPreparation(backend,scope,lineage,pending.intent(),null,authority,protection,clock,true);
        prepared.pendingObject=pending;return prepared.recovery(catalog,recovery);
    }
    /** The original Catalog's actual exact-input recovery receipt, acquired before this flight starts reading Native pages. */
    public TargetRetirementPreparation recovery(OxiaSyncCheckpointPublicationBackend catalog,OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery){
        requireCurrent();if(captured||advancing||this.recovery!=null)throw new IllegalStateException("retirement recovery receipt is already selected or preparation started");
        this.catalog=Objects.requireNonNull(catalog);this.recovery=Objects.requireNonNull(recovery);recovery.requireHeldFor(catalog,input);return this;
    }

    /** Advances actual rows, not matches. The original cumulative reservation is never reset between turns. */
    public boolean advance(BoundedReadBudget turn,int maximumRows){
        Objects.requireNonNull(turn);
        if(maximumRows<1||maximumRows==Integer.MAX_VALUE)throw new IllegalArgumentException("finite retirement page rows required");
        synchronized(this){
            if(closed||advancing)throw new IllegalStateException("retirement preparation is closed or already advancing");
            if(proof!=null){requireCurrent();return true;}advancing=true;
        }
        final long beforeRows=turn.actualRecords(),beforeBytes=turn.actualBytes();
        try{
            requireCurrent();
            final boolean complete=backend.guardedRead(turn,reader->{
                requireCurrent();captureOrRequire(reader);
                final boolean inventoryDone=cursor.advance(reader,maximumRows,this::retain,this::copy);
                final boolean done=inventoryDone&&advanceLedger(reader,maximumRows);
                reader.requireWithinElapsedBudget();captureOrRequire(reader);requireCurrent();return done;
            },authority);
            if(complete){requireCurrent();proof=new Proof(this);return true;}
            return false;
        }catch(ReadIncompleteException incomplete){
            requireCurrent();return false;
        }catch(ViewChanged changed){
            // Relevant writes revoke every partial conclusion. The same original flight keeps its cumulative debit/deadline.
            invalidateInventory();return false;
        }finally{
            try{charge(turn.actualRecords()-beforeRows,turn.actualBytes()-beforeBytes);}
            finally{synchronized(this){advancing=false;}}
        }
    }

    public Proof proof(){requireCurrent();if(proof==null)throw new IllegalStateException("retirement inventory has not reached every original range EOF");return proof;}
    public long retainedBytes(){requireCurrent();return retainedBytes;}
    /** Actual original acquisition/final point or Catalog rereads outside advance, including unsuccessful I/O. */
    public void debitReads(long actualRows,long actualBytes){charge(actualRows,actualBytes);}
    public void invalidate(){synchronized(this){if(advancing)throw new IllegalStateException("retirement I/O has not exited");requireCurrent();invalidateInventory();}}
    private void invalidateInventory(){
        generation=Math.incrementExact(generation);cursor.clear();cursor=TargetRetirementCursor.forInput(scope,lineage,input);
        if(ledger!=null)ledger.clear();ledger=null;ledgerComplete=false;closureComplete=false;inlineClosure=null;captured=false;proof=null;
    }
    public void close(){synchronized(this){if(advancing)throw new IllegalStateException("retirement I/O has not exited");closed=true;}cursor.clear();if(ledger!=null)ledger.clear();}

    private boolean advanceLedger(TargetStoreBackend.Reader reader,int maximumRows){
        if(!needsLedger)return true;
        if(!ledgerComplete){
            if(ledger==null){
                if(capacity.rows()>=Integer.MAX_VALUE)throw new IllegalArgumentException("original retirement ledger row contract exceeds its supported finite fold bound");
                ledger=new TargetRetirementLedgerCursor(reader,new TargetCheckpointRootVerifier.LedgerAuditLimits((int)capacity.rows(),capacity.readBytes(),(int)capacity.rows(),capacity.readBytes()));
            }
            ledgerComplete=ledger.advance(reader,maximumRows,this::retain,this::copy);
            return false;
        }
        if(closureComplete)return true;
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        final Runnable current=()->{reader.requireWithinElapsedBudget();requireCurrent();};
        if(body.nativeRetainedReclaim()){
            TargetNativeRetainedReclaim.requirePreparedClosed(ledger,body,sourceBytes==null?null:input,allowApplied,current);closureComplete=true;return true;
        }
        if(recovery==null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("draft retirement preparation lacks its actual original Catalog/Floor recovery receipt");
        recovery.requireHeldFor(catalog,input);
        if(body.uncommittedObjectReclaim()){
            TargetUncommittedPayloadReclaim.requirePreparedClosed(ledger,input,allowApplied,current);closureComplete=true;return true;
        }
        if(inlineClosure==null){
            final var policies=body.objectPayloadReclaim()?recovery.objectPolicies():recovery.inlinePolicies();
            final var facts=body.objectPayloadReclaim()?TargetInlinePayloadReclaim.SourceFacts.from(body.payloadFacts()):recovery.inlineSourceFacts();
            inlineClosure=new TargetInlinePayloadReclaim.RetirementCursor(ledger,body,policies,facts,allowApplied);
        }
        if(!inlineClosure.advance(maximumRows,current,this::retain,this::copy))return false;
        if(body.objectPayloadReclaim())TargetObjectPayloadReclaim.requirePreparedClosed(ledger,input,allowApplied,current);
        closureComplete=true;return true;
    }

    private void charge(long actualRows,long actualBytes){
        if(actualRows<0||actualBytes<0)throw new IllegalStateException("retirement page budget regressed");
        rows=Math.addExact(rows,actualRows);readBytes=Math.addExact(readBytes,actualBytes);
        // Every actual Store projection is a copied byte window, including a read that did not fit this turn.
        copy(actualBytes);
        if(rows>capacity.rows()||readBytes>capacity.readBytes()){exhausted=true;throw new IllegalStateException("original cumulative retirement I/O reservation exhausted");}
    }
    private void copy(long bytes){copyBytes=Math.addExact(copyBytes,bytes);if(copyBytes>capacity.copyBytes()){exhausted=true;throw new IllegalStateException("original cumulative retirement copy reservation exhausted");}}
    private void retain(long bytes){
        retainedBytes=Math.addExact(retainedBytes,bytes);
        if(retainedBytes>capacity.retainedBytes()){exhausted=true;throw new IllegalStateException("original retirement retained window exhausted");}
    }
    private void requireCurrent(){
        if(closed||exhausted)throw new IllegalStateException("retirement preparation is closed or its original cumulative reservation is exhausted");
        final long elapsed=clock.getAsLong()-started;
        if(elapsed<0||elapsed>=capacity.holdNanos())throw new IllegalStateException("original retirement preparation hold expired");
        protection.requireCurrent();
    }
    private void captureOrRequire(TargetStoreBackend.Reader reader){
        reader.requireBackend(backend);
        final byte[] selectedMetadata=reader.metadata().encode();
        final byte[] selectedCut=Objects.requireNonNull(reader.source(),"retirement requires established Source").canonicalBytes();
        final byte[] selectedAggregate=reader.aggregate().canonicalBytes();
        final var rootId=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final byte[] key=rootId.key();key[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] selectedRoot=Objects.requireNonNull(reader.get(ColumnFamily.META,key),"retirement original root");
        final var owner=TargetQuotaIncarnation.decodeForStore(key,TargetValueEnvelope.decode(selectedRoot,TargetQuotaIncarnation.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope());
        if(!owner.identity().equals(rootId)||!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("retirement root lineage changed");
        protection.requireBinding(reader.metadata(),owner,reader.aggregate().mutation(),input,sourceBytes==null?null:TargetSourcePosition.decode(sourceBytes));
        if(!captured){
            metadata=selectedMetadata;cut=selectedCut;aggregate=selectedAggregate;root=selectedRoot;
            nativeSequence=reader.nativeSequence();sourceSequence=reader.sourceSequence();captured=true;
            final long capturedBytes=metadata.length+cut.length+aggregate.length+root.length;retain(capturedBytes);copy(capturedBytes);
        }else if(nativeSequence!=reader.nativeSequence()||sourceSequence!=reader.sourceSequence()
                ||!Arrays.equals(metadata,selectedMetadata)||!Arrays.equals(cut,selectedCut)
                ||!Arrays.equals(aggregate,selectedAggregate)||!Arrays.equals(root,selectedRoot))throw new ViewChanged();
    }

    /** Privately minted only by this repository's actual complete scanner; cannot be populated by a provider DTO. */
    public static final class Proof {
        private final TargetRetirementPreparation owner;
        private final long generation;
        private Proof(TargetRetirementPreparation owner){this.owner=owner;generation=owner.generation;}
        private void requireValid(){owner.requireCurrent();if(owner.generation!=generation||owner.proof!=this)throw new ViewChanged();}
        public void requireReader(TargetStoreBackend.Reader reader,TargetStoreBackend backend){
            requireValid();if(owner.backend!=backend)throw new IllegalArgumentException("retirement proof belongs to another backend");
            owner.captureOrRequire(reader);owner.requireCurrent();
        }
        public void requireInput(SystemMutation input,SourcePosition source){
            requireValid();if(!Arrays.equals(owner.inputBytes,input.canonicalEnvelope())
                    ||!Arrays.equals(owner.sourceBytes,source==null?null:source.canonicalBytes()))
                throw new IllegalArgumentException("retirement proof changed exact original input/Source");
        }
        public void requireCurrent(){requireValid();}
        /** Accounting only: cannot qualify or restore an invalidated proof. */
        public void debitReads(long actualRows,long actualBytes){owner.debitReads(actualRows,actualBytes);}
        public long retainedBytes(){requireValid();return owner.retainedBytes();}
        public void requireDraft(TargetStoreBackend.Reader reader,SystemMutation input){
            requireInput(input,null);requireReader(reader,owner.backend);
        }
        public void requirePending(TargetStoreBackend.Reader reader,TargetPayloadObjectRetiringRecord pending){
            requireDraft(reader,pending.intent());requireClosure(reader,pending.intent());
            if(!owner.allowApplied||owner.pendingObject==null||!Arrays.equals(owner.pendingObject.canonicalBytes(),pending.canonicalBytes()))
                throw new IllegalStateException("pending OBJECT changed the original actual Native56 preparation");
        }
        public void requireClosure(TargetStoreBackend.Reader reader,SystemMutation input){
            requireReader(reader,owner.backend);
            if(!Arrays.equals(owner.inputBytes,input.canonicalEnvelope())||!owner.needsLedger||!owner.ledgerComplete||!owner.closureComplete)
                throw new IllegalStateException("exact original complete retirement closure is unavailable");
        }
        boolean provesUnused(TargetStoreBackend.Reader reader,TargetResourceReclaimBody body){
            requireReader(reader,owner.backend);
            return owner.cursor.unused()&&Arrays.equals(body.canonicalBytes(),owner.input.canonicalBody());
        }
        TargetRecordAccounting.View evidenceView(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
                TargetExternalEvidenceObjectResource resource){
            return evidenceView(view,overlay,resource.subtype().wireValue(),resource.objectDigest(),resource.exactKey());
        }
        TargetRecordAccounting.View evidenceView(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
                int subtype,byte[] digest,String exactKey){
            requireValid();if(!owner.cursor.evidence(subtype,digest,exactKey))return view;
            if(!(view instanceof TargetStoreBackend.Reader reader))throw new IllegalStateException("retirement proof requires its actual Native reader");
            requireReader(reader,owner.backend);return owner.cursor.view(view,overlay);
        }
    }
}
