package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One actual Source batch transfers paid parent capacity to the complete immutable child obligation. */
public final class TargetScopeControlObligationStore {
    public static final class Prepared {
        private final TargetScopeControlObligationStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Runnable current;
        private Prepared(TargetScopeControlObligationStore owner,TargetStoreBackend.Prepared batch,SystemMutationResult result,Runnable current){
            this.owner=owner;this.batch=batch;this.result=result;this.current=current;
        }
    }
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters,maximumDomains;
    public TargetScopeControlObligationStore(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,int maximumCounters,int maximumDomains){
        this.backend=Objects.requireNonNull(backend);this.scope=Objects.requireNonNull(scope);Bytes.requireLength(lineage,16,"Scope obligation lineage");
        if(scope.target()!=null||Arrays.equals(lineage,new byte[16])||maximumCounters<2||maximumDomains<1||maximumDomains>TargetQueueState.MAX_DOMAIN_SLOTS)
            throw new IllegalArgumentException("Scope obligation needs its finite original Source accounting limits");
        this.lineage=Bytes.copy(lineage);this.maximumCounters=maximumCounters;this.maximumDomains=maximumDomains;
    }
    public Prepared prepareFirst(BoundedReadBudget budget,PreparedControlOperation control,SystemMutation input,SourcePosition source,
            TargetScopeControlObligationVerifier.Authority authority){
        Objects.requireNonNull(control);Objects.requireNonNull(authority);TargetSourcePosition.requireBounded(source);
        if(!scope.shard().equals(source.shardId())||!scope.shard().equals(input.shardId())||!TargetScopeControlObligationBody.isControl(input))
            throw new IllegalArgumentException("Scope obligation changed its actual Source branch");
        final var result=new SystemMutationResult[1];final boolean[] paid={false};
        final var batch=backend.prepare(budget,reader->{
            if(reader.source()==null||source.compareTo(reader.source())<=0)throw new IllegalStateException("Scope obligation requires a later actual Source position");
            final var firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},input.systemMutationId());
            final var posKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},source.canonicalBytes());
            absent(reader,firstKey);absent(reader,posKey);
            final var stamp=new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()),source,Bytes.sha256(input.canonicalEnvelope()));
            final var view=TargetParentControlCreditVerifier.readView(reader,scope,lineage);final var root=view.actualRoot();stamp.requireAfter(view.priorCut());
            TargetScopeControlObligationVerifier.Change change=null;StableCode rejected=null;
            if(reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs())rejected=StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
            else try{change=TargetScopeControlObligationVerifier.verifyFirstApplication(reader,scope,lineage,control,input,source,authority);}
            catch(CommandResolutionException denial){if(!knownDenial(denial.stableCode()))throw denial;rejected=denial.stableCode();}
            result[0]=SystemMutationResult.from(input,rejected==null?ApplyStatus.APPLIED:ApplyStatus.REJECTED,rejected==null?StableCode.OK:rejected,source.canonicalBytes());
            final TargetResultRecord.CreationAuthority creation=(record,original)->{
                record.requireOwner(root);
                if(!record.mutation().equals(stamp)||record.allocation()!=null)throw new IllegalStateException("Scope result changed its actual Root/stamp");
                absent(reader,record.key());
                if(record.kind()==TargetResultRecord.Kind.SYSTEM){
                    if(original!=null||!Arrays.equals(record.key(),firstKey)||!Arrays.equals(record.typedPayload(),result[0].encode()))throw new IllegalStateException("Scope result changed its original first");
                }else if(record.kind()==TargetResultRecord.Kind.POSITION_SYSTEM){
                    if(original==null||!Arrays.equals(record.key(),posKey)||!Arrays.equals(original.key(),firstKey))throw new IllegalStateException("Scope POSITION lost its original first");
                    record.requireFirst(original);
                }else throw new IllegalStateException("Scope allocation cannot create another result kind");
            };
            var first=TargetResultRecord.system(root,result[0],stamp,null,creation);var position=TargetResultRecord.position(root,first,stamp,creation);
            final var edits=new ArrayList<TargetStoreBackend.Edit>();
            if(rejected==null){
                final var selected=Objects.requireNonNull(change);
                final var obligation=TargetScopeControlObligation.fund(selected.body().request().definition(),input,stamp,first.recordCharge().add(position.recordCharge()));
                if(!selected.parent().remaining().covers(obligation.parentCommitment())){
                    result[0]=SystemMutationResult.from(input,ApplyStatus.REJECTED,StableCode.HARD_QUOTA_EXCEEDED,source.canonicalBytes());
                    first=TargetResultRecord.system(root,result[0],stamp,null,creation);position=TargetResultRecord.position(root,first,stamp,creation);
                }else{
                    final var allocation=new TargetParentControlCredit.Allocation(obligation.headerReference(),obligation.allocationGrantDigest(),obligation.parentCommitment(),stamp);
                    final var parent=selected.parent().allocate(allocation);
                    edits.add(reader.replace(ColumnFamily.META,parent.key(),TargetParentControlCredit.VALUE_TYPE,parent.canonicalBytes()));
                    edits.add(reader.replace(ColumnFamily.META,obligation.key(),TargetScopeControlObligation.VALUE_TYPE,obligation.canonicalBytes()));
                    paid[0]=true;
                }
            }
            edits.add(reader.replace(ColumnFamily.DEDUPE,first.key(),TargetResultRecord.VALUE_TYPE,first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE,position.key(),TargetResultRecord.VALUE_TYPE,position.canonicalBytes()));
            var complete=new TargetSourceAccounting(scope,lineage,source,stamp.mutationDigest(),maximumCounters,1,maximumDomains).assemble(reader,edits);
            if(paid[0])try{TargetScopeControlObligationVerifier.requireComplete(Objects.requireNonNull(change),complete,source,authority);}
            catch(CommandResolutionException denial){
                if(denial.stableCode()!=StableCode.HARD_QUOTA_EXCEEDED)throw denial;
                paid[0]=false;edits.clear();result[0]=SystemMutationResult.from(input,ApplyStatus.REJECTED,denial.stableCode(),source.canonicalBytes());
                first=TargetResultRecord.system(root,result[0],stamp,null,creation);position=TargetResultRecord.position(root,first,stamp,creation);
                edits.add(reader.replace(ColumnFamily.DEDUPE,first.key(),TargetResultRecord.VALUE_TYPE,first.canonicalBytes()));
                edits.add(reader.replace(ColumnFamily.DEDUPE,position.key(),TargetResultRecord.VALUE_TYPE,position.canonicalBytes()));
                complete=new TargetSourceAccounting(scope,lineage,source,stamp.mutationDigest(),maximumCounters,1,maximumDomains).assemble(reader,edits);
            }
            return complete;
        });
        return new Prepared(this,batch,result[0],paid[0]?authority.capacity()::requireCurrent:()->{});
    }
    public SystemMutationResult commit(Prepared prepared,TargetStoreBackend.CommitAuthority authority){
        if(Objects.requireNonNull(prepared).owner!=this)throw new IllegalArgumentException("foreign Scope allocation Source plan");
        prepared.current.run();backend.commit(prepared.batch,(metadata,actualScope,mutation)->{
            final var held=Objects.requireNonNull(authority.acquire(metadata,actualScope,mutation));
            return new TargetStoreBackend.CommitGuard(){public void requireCurrent(){prepared.current.run();held.requireCurrent();prepared.current.run();}
                public void close(){held.close();}};
        });return prepared.result;
    }
    /** Guarded original Source payment only; no Scope birth, exposure or physical resource qualification. */
    public static TargetScopeControlObligation requirePaidScope(TargetStoreBackend.Reader reader,TargetQuotaScope scope,
            byte[] lineage,TargetSloControlScopeHeader.Reference reference){
        Objects.requireNonNull(reader);Objects.requireNonNull(scope);Objects.requireNonNull(reference);
        Bytes.requireLength(lineage,16,"paid Scope lineage");
        if(scope.target()!=null||!scope.shard().equals(reader.shardId()))
            throw new IllegalArgumentException("paid Scope requires its actual SHARD Store");
        reader.requireWithinElapsedBudget();
        final var aggregate=reader.aggregate();final var cut=aggregate.mutation();
        if(cut==null||reader.source()==null)
            throw new TargetSourceAuthorityPreparation.InputUnavailable("paid Scope lacks its actual Source accounting cut");
        if(cut.sequence()!=reader.sourceSequence()||!Arrays.equals(cut.source().canonicalBytes(),reader.source().canonicalBytes()))
            throw new IllegalStateException("paid Scope aggregate differs from the actual Source frontier");
        // Every causal dependency is read from this guard; absent retained evidence cannot become a known denial.
        final TargetRecordAccounting.View evidence=new TargetRecordAccounting.View(){
            public ShardId shardId(){return reader.shardId();}
            public byte[] projected(ColumnFamily family,byte[] key,List<TargetStoreBackend.Edit> overlay){
                final var raw=reader.projected(family,key,overlay);
                if(raw==null)throw new TargetSourceAuthorityPreparation.InputUnavailable("paid Scope lacks an actual retained Source payment dependency");
                return raw;
            }
            public ShardStore.KeyValue first(ColumnFamily family,byte[] lower,byte[] upper,List<TargetStoreBackend.Edit> overlay){
                return reader.first(family,lower,upper,overlay);
            }
        };
        final var key=Bytes.concat(new byte[]{TargetScopeControlObligation.KEY_TAG,TargetKeyCodec.KEY_FORMAT},reference.scopeId());
        final var obligation=TargetScopeControlObligation.decodeForStore(key,
                payload(evidence,List.of(),ColumnFamily.META,key,TargetScopeControlObligation.VALUE_TYPE),scope,lineage);
        obligation.requireReference(reference);
        final var owner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),aggregate.accountingIncarnation(),null,null);
        final byte[] rootKey=owner.key();rootKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(rootKey,
                payload(evidence,List.of(),ColumnFamily.META,rootKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        final var bookkeeping=TargetQuotaBookkeeping.decodeForStore(TargetQuotaBookkeeping.keyFor(scope.shard()),
                payload(evidence,List.of(),ColumnFamily.META,TargetQuotaBookkeeping.keyFor(scope.shard()),TargetQuotaBookkeeping.VALUE_TYPE),owner,scope.tenantScope());
        if(!root.identity().equals(owner)||!root.scope().equals(scope)||!Arrays.equals(root.recoveryLineage(),lineage)
                ||!Arrays.equals(root.accounting().canonicalBytes(),bookkeeping.accounting().canonicalBytes()))
            throw new IllegalStateException("paid Scope changed actual Root/bookkeeping tenant/accounting/lineage");
        root.latestMutation().requireAtOrBefore(cut);bookkeeping.requireRoot(aggregate);
        obligation.requireRoot(root);obligation.requireSource(cut.source());
        requireOrigin(evidence,List.of(),scope,lineage,root,obligation,cut);
        reader.requireWithinElapsedBudget();return obligation;
    }
    public static void requireOrigin(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,
            byte[] lineage,TargetQuotaIncarnation owner,TargetScopeControlObligation obligation,TargetQuotaMutation frontier){
        requireLocalOrigin(view,overlay,scope,lineage,owner,obligation,frontier);
        final var key=obligation.parentReference().key();
        final var parent=TargetParentControlCredit.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetParentControlCredit.VALUE_TYPE),scope,lineage);
        TargetParentControlCreditStore.requireOrigin(view,overlay,scope,lineage,owner,parent,frontier);
        requireParentAllocation(parent,obligation);
    }
    /** Local child proof avoids recursive parent/child accounting calls; full parent linkage is checked separately. */
    static void requireLocalOrigin(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,
            byte[] lineage,TargetQuotaIncarnation owner,TargetScopeControlObligation obligation,TargetQuotaMutation frontier){
        obligation.requireRoot(owner);obligation.mutation().requireAtOrBefore(frontier);
        if(!scope.equals(obligation.scope())||!Arrays.equals(lineage,obligation.lineage()))throw new IllegalStateException("Scope obligation changed actual Source/lineage");
        final var input=obligation.funding().input();
        final var first=result(view,overlay,Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},input.systemMutationId()));
        first.requireOwner(owner);
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.mutation().equals(obligation.fundedAt())
                ||!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,ApplyStatus.APPLIED,StableCode.OK,obligation.fundedAt().source().canonicalBytes()).encode()))
            throw new IllegalStateException("Scope obligation lacks its exact original APPLIED first");
        final var position=result(view,overlay,Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},obligation.fundedAt().source().canonicalBytes()));
        position.requireOwner(owner);position.requireFirst(first);
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(obligation.fundedAt())
                ||!obligation.bootstrapResultsCharge().equals(first.recordCharge().add(position.recordCharge())))
            throw new IllegalStateException("Scope obligation changed its original POSITION/bootstrap payment");
        final var key=TargetProtocolActivationRecord.key(obligation.funding().body().tuple());
        final var activation=TargetProtocolActivationRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        TargetProtocolActivationStore.requireApplied(view,overlay,scope,lineage,owner.identity(),activation,frontier);
        obligation.fundedAt().requireAfter(activation.appliedAt());
    }
    public static void requireParentAllocation(TargetParentControlCredit parent,TargetScopeControlObligation obligation){
        parent.requireReference(obligation.parentReference());parent.requireRoot(obligation.definition().plan().originalRoot());
        final var allocation=parent.allocations().stream().filter(a->Arrays.equals(a.scope().scopeId(),obligation.headerReference().scopeId())).findFirst().orElseThrow(
                ()->new IllegalStateException("Scope obligation lacks its actual parent allocation"));
        if(!allocation.scope().equals(obligation.headerReference())||!Arrays.equals(allocation.allocationGrantDigest(),obligation.allocationGrantDigest())
                ||!allocation.commitment().equals(obligation.parentCommitment())||!allocation.allocatedAt().equals(obligation.fundedAt()))
            throw new IllegalStateException("Scope obligation changed its original parent Scope/digest/vector/Source");
    }
    private static boolean knownDenial(StableCode code){return code==StableCode.UNAUTHORIZED_SYSTEM_MUTATION||code==StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
            ||code==StableCode.STALE_SYSTEM_MUTATION||code==StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION||code==StableCode.HARD_QUOTA_EXCEEDED;}
    private static TargetResultRecord result(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key){
        final var bytes=payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE);final var value=TargetResultRecord.decode(bytes);
        value.requireStored(key,TargetResultRecord.VALUE_TYPE,bytes);return value;
    }
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"original Scope obligation dependency"),type).payload();
    }
    private static void absent(TargetStoreBackend.Reader reader,byte[] key){if(reader.get(ColumnFamily.DEDUPE,key)!=null)throw new IllegalStateException("Scope allocation already has a first/POS");}
}
