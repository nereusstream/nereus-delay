package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Explicit Source-paid parent credit; a declaration or ordinary grant update never creates this balance. */
public final class TargetParentControlCreditStore {
    public static final class Prepared {
        private final TargetParentControlCreditStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Runnable current;
        private Prepared(TargetParentControlCreditStore owner,TargetStoreBackend.Prepared batch,SystemMutationResult result,Runnable current){
            this.owner=owner;this.batch=batch;this.result=result;this.current=current;
        }
    }
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters,maximumDomains;
    public TargetParentControlCreditStore(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,int maximumCounters,int maximumDomains){
        this.backend=Objects.requireNonNull(backend);this.scope=Objects.requireNonNull(scope);
        Bytes.requireLength(lineage,16,"parent credit Source lineage");
        if(Arrays.equals(lineage,new byte[16]))throw new IllegalArgumentException("parent credit Source lineage is unassigned");
        this.lineage=Bytes.copy(lineage);
        if(scope.target()!=null||maximumCounters<2||maximumDomains<1||maximumDomains>TargetQueueState.MAX_DOMAIN_SLOTS)
            throw new IllegalArgumentException("parent credit requires its finite actual Source accounting scope");
        this.maximumCounters=maximumCounters;this.maximumDomains=maximumDomains;
    }

    public Prepared prepareFirst(BoundedReadBudget budget,PreparedControlOperation control,SystemMutation input,
            SourcePosition source,TargetParentControlCreditVerifier.Authority authority){
        Objects.requireNonNull(control);Objects.requireNonNull(authority);TargetSourcePosition.requireBounded(source);
        if(!scope.shard().equals(source.shardId())||!scope.shard().equals(input.shardId())||!TargetParentControlCreditBody.isControl(input))
            throw new IllegalArgumentException("parent credit changed its actual Source/Control branch");
        final var result=new SystemMutationResult[1];final boolean[] paid={false};
        final var batch=backend.prepare(budget,reader->{
            if(reader.source()==null||source.compareTo(reader.source())<=0)throw new IllegalStateException("parent credit needs a later actual Source position");
            final var firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},input.systemMutationId());
            final var posKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},source.canonicalBytes());
            absent(reader,ColumnFamily.DEDUPE,firstKey);absent(reader,ColumnFamily.DEDUPE,posKey);
            final var stamp=new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()),source,Bytes.sha256(input.canonicalEnvelope()));
            final var view=TargetParentControlCreditVerifier.readView(reader,scope,lineage);
            final var root=view.actualRoot();stamp.requireAfter(reader.aggregate().mutation());
            final var edits=new ArrayList<TargetStoreBackend.Edit>();
            TargetParentControlCreditVerifier.Change change=null;StableCode rejected=null;
            if(reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs())rejected=StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
            else try{change=TargetParentControlCreditVerifier.verifyFirstApplication(reader,scope,lineage,control,input,source,authority);}
            catch(CommandResolutionException denial){
                if(denial.stableCode()!=StableCode.UNAUTHORIZED_SYSTEM_MUTATION&&denial.stableCode()!=StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                        &&denial.stableCode()!=StableCode.STALE_SYSTEM_MUTATION&&denial.stableCode()!=StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION
                        &&denial.stableCode()!=StableCode.HARD_QUOTA_EXCEEDED)throw denial;
                rejected=denial.stableCode();
            }
            if(rejected==null){
                final var selected=Objects.requireNonNull(change);
                final var credit=TargetParentControlCredit.fund(selected.body().request().definition(),input,selected.view().actualGrant(),stamp);
                absent(reader,ColumnFamily.META,credit.key());
                edits.add(reader.replace(ColumnFamily.META,credit.key(),TargetParentControlCredit.VALUE_TYPE,credit.canonicalBytes()));
                paid[0]=true;
            }
            result[0]=SystemMutationResult.from(input,rejected==null?ApplyStatus.APPLIED:ApplyStatus.REJECTED,
                    rejected==null?StableCode.OK:rejected,source.canonicalBytes());
            final TargetResultRecord.CreationAuthority creation=(record,original)->{
                record.requireOwner(root);
                if(!record.mutation().equals(stamp)||record.allocation()!=null)throw new IllegalStateException("parent credit result changed actual Root/stamp");
                absent(reader,ColumnFamily.DEDUPE,record.key());
                if(record.kind()==TargetResultRecord.Kind.SYSTEM){
                    if(original!=null||!Arrays.equals(record.key(),firstKey)||!Arrays.equals(record.typedPayload(),result[0].encode()))throw new IllegalStateException("parent credit changed first result");
                }else if(record.kind()==TargetResultRecord.Kind.POSITION_SYSTEM){
                    if(original==null||!Arrays.equals(record.key(),posKey)||!Arrays.equals(original.key(),firstKey))throw new IllegalStateException("parent credit POSITION lost its first");
                    record.requireFirst(original);
                }else throw new IllegalStateException("parent credit cannot create another result kind");
            };
            final var first=TargetResultRecord.system(root,result[0],stamp,null,creation);
            final var position=TargetResultRecord.position(root,first,stamp,creation);
            edits.add(reader.replace(ColumnFamily.DEDUPE,first.key(),TargetResultRecord.VALUE_TYPE,first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE,position.key(),TargetResultRecord.VALUE_TYPE,position.canonicalBytes()));
            var complete=new TargetSourceAccounting(scope,lineage,source,stamp.mutationDigest(),maximumCounters,1,maximumDomains).assemble(reader,edits);
            if(paid[0]){
                try{TargetParentControlCreditVerifier.requireComplete(Objects.requireNonNull(change),complete,source,authority);}
                catch(CommandResolutionException denied){
                    if(denied.stableCode()!=StableCode.HARD_QUOTA_EXCEEDED)throw denied;
                    // A rejected new allocation cannot wait behind itself for a later Source grant increase.
                    paid[0]=false;edits.clear();
                    result[0]=SystemMutationResult.from(input,ApplyStatus.REJECTED,denied.stableCode(),source.canonicalBytes());
                    final var rejectedFirst=TargetResultRecord.system(root,result[0],stamp,null,creation);
                    final var rejectedPosition=TargetResultRecord.position(root,rejectedFirst,stamp,creation);
                    edits.add(reader.replace(ColumnFamily.DEDUPE,rejectedFirst.key(),TargetResultRecord.VALUE_TYPE,rejectedFirst.canonicalBytes()));
                    edits.add(reader.replace(ColumnFamily.DEDUPE,rejectedPosition.key(),TargetResultRecord.VALUE_TYPE,rejectedPosition.canonicalBytes()));
                    complete=new TargetSourceAccounting(scope,lineage,source,stamp.mutationDigest(),maximumCounters,1,maximumDomains).assemble(reader,edits);
                }
            }
            return complete;
        });
        return new Prepared(this,batch,result[0],paid[0]?authority.capacity()::requireCurrent:()->{});
    }
    public SystemMutationResult commit(Prepared prepared,TargetStoreBackend.CommitAuthority authority){
        if(Objects.requireNonNull(prepared).owner!=this)throw new IllegalArgumentException("foreign parent credit Source plan");
        prepared.current.run();
        backend.commit(prepared.batch,(metadata,actualScope,mutation)->{
            final var held=Objects.requireNonNull(authority.acquire(metadata,actualScope,mutation));
            return new TargetStoreBackend.CommitGuard(){
                public void requireCurrent(){prepared.current.run();held.requireCurrent();prepared.current.run();}
                public void close(){held.close();}
            };
        });
        return prepared.result;
    }

    /** Independent local linkage for the common hot/cold accounting fold. External authorization stays separate. */
    public static void requireOrigin(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIncarnation owner,TargetParentControlCredit credit,TargetQuotaMutation frontier){
        credit.requireRoot(owner);credit.mutation().requireAtOrBefore(frontier);
        if(!scope.equals(credit.scope())||!Arrays.equals(lineage,credit.lineage()))throw new IllegalStateException("parent credit changed Source scope/lineage");
        final var input=credit.funding().input();
        final var first=result(view,overlay,Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},input.systemMutationId()));
        final var outcome=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!owner.identity().equals(first.primaryIdentity())
                ||!first.mutation().equals(credit.fundedAt())||outcome.applyStatus()!=ApplyStatus.APPLIED||outcome.stableCode()!=StableCode.OK
                ||!Arrays.equals(outcome.mutationHash(),input.mutationHash())||!Arrays.equals(outcome.authorIdentity(),input.authorIdentity())
                ||!Arrays.equals(outcome.mutationId(),input.systemMutationId())||outcome.mutationType()!=input.type()
                ||!Arrays.equals(outcome.appliedSourcePosition(),credit.fundedAt().source().canonicalBytes())
                ||outcome.retryUntilEpochMs()!=input.retryUntilEpochMs()||!Arrays.equals(first.recoveryLineage(),lineage)
                ||!Arrays.equals(first.tenantScope(),scope.tenantScope()))throw new IllegalStateException("parent credit lacks its exact original APPLIED first");
        final var position=result(view,overlay,Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},credit.fundedAt().source().canonicalBytes()));
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(credit.fundedAt())
                ||!owner.identity().equals(position.primaryIdentity())||!Arrays.equals(position.recoveryLineage(),lineage)
                ||!Arrays.equals(position.tenantScope(),scope.tenantScope()))throw new IllegalStateException("parent credit lacks its exact original Source POSITION");
        position.requireFirst(first);
        final var activationKey=TargetProtocolActivationRecord.key(ProtocolTuple.targetParentControlCredit());
        final var activation=TargetProtocolActivationRecord.decodeForStore(activationKey,payload(view,overlay,ColumnFamily.META,activationKey,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        TargetProtocolActivationStore.requireApplied(view,overlay,scope,lineage,owner.identity(),activation,frontier);
        credit.fundedAt().requireAfter(activation.appliedAt());
        // Rebuild each immutable prefix: another operation cannot spend a stale parent revision or rewrite a child.
        var prefix=TargetParentControlCredit.fund(credit.definition(),input,credit.actualPayerGrant(),credit.fundedAt());
        for(var allocation:credit.allocations()){
            final var key=Bytes.concat(new byte[]{TargetScopeControlObligation.KEY_TAG,TargetKeyCodec.KEY_FORMAT},allocation.scope().scopeId());
            final var child=TargetScopeControlObligation.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetScopeControlObligation.VALUE_TYPE),scope,lineage);
            TargetScopeControlObligationStore.requireLocalOrigin(view,overlay,scope,lineage,owner,child,frontier);
            TargetScopeControlObligationStore.requireParentAllocation(credit,child);
            final var request=child.funding().body().request();
            if(!child.parentReference().equals(prefix.reference())||request.expectedParentRevision()!=prefix.revision()
                    ||!Arrays.equals(request.expectedParentDigest(),prefix.digest()))
                throw new IllegalStateException("Scope allocation changed its original paid parent prefix");
            prefix=prefix.allocate(allocation);
        }
    }
    private static TargetResultRecord result(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key){
        final var bytes=payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE);
        final var result=TargetResultRecord.decode(bytes);result.requireStored(key,TargetResultRecord.VALUE_TYPE,bytes);return result;
    }
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"original parent credit dependency"),type).payload();
    }
    private static void absent(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key){
        if(reader.get(family,key)!=null)throw new IllegalStateException("parent credit first apply encountered an existing record");
    }
}
