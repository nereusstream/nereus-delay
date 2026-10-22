package com.nereusstream.delay.runtime;

import com.nereusstream.delay.assessment.DataResetManifest;
import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Actual first Source marker, immutable Target projection and SHARD accounting in one Native batch. */
public final class TargetProtocolActivationStore {
    @FunctionalInterface public interface Keys { PublicKey resolve(long version,SourcePosition source); }
    /**
     * Trusted original Root must authenticate complete eligible membership/revisions/sessions, exact binary/SDK support,
     * established base environment/reset scope and independent issuer pin at this Source point. It retains every pin.
     * Historical collection must not resample today's readers or reactivate the old reset's cutover window.
     * Incomplete inputs throw; no caller boolean or data-record constructor grants this permission.
     */
    @FunctionalInterface public interface OriginalInputs {
        Inputs capture(PreparedControlOperation prepared,SystemMutation marker,SourcePosition source,TargetProtocolActivationBody body);
    }
    public record Inputs(TargetProtocolReaderSet readers,DataResetManifest reset,PublicKey resetIssuer,Runnable current) {
        public Inputs { Objects.requireNonNull(readers);Objects.requireNonNull(reset);Objects.requireNonNull(resetIssuer);Objects.requireNonNull(current); }
        public long retainedBytes(){return Math.addExact(readers.canonicalBytes().length,Math.addExact(reset.canonicalBytes().length,resetIssuer.getEncoded().length));}
    }
    /** Source keys/null denials and scope proofs must come from complete protected original history; unknown throws. */
    public record Authority(ControlTargetRegistrationAuthority registrations,Keys keys,ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof,OriginalInputs inputs) {
        public Authority { Objects.requireNonNull(registrations);Objects.requireNonNull(keys);Objects.requireNonNull(actor);Objects.requireNonNull(scopeProof);Objects.requireNonNull(inputs); }
    }
    public static final class Prepared {
        private final TargetProtocolActivationStore owner;private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;private final Inputs inputs;
        private Prepared(TargetProtocolActivationStore owner,TargetStoreBackend.Prepared batch,SystemMutationResult result,Inputs inputs){this.owner=owner;this.batch=batch;this.result=result;this.inputs=inputs;}
    }
    private final TargetStoreBackend backend;private final TargetQuotaScope scope;private final byte[] lineage;private final int counters,domains;
    public TargetProtocolActivationStore(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,int counters,int domains){
        this.backend=Objects.requireNonNull(backend);this.scope=Objects.requireNonNull(scope);Bytes.requireLength(lineage,16,"activation lineage");
        if(scope.target()!=null||counters<2||domains<1||domains>64)throw new IllegalArgumentException("Target activation requires finite Shard accounting");
        this.lineage=Bytes.copy(lineage);this.counters=counters;this.domains=domains;
    }
    public Prepared prepareFirst(BoundedReadBudget budget,PreparedControlOperation control,SystemMutation marker,SourcePosition source,Authority authority){
        Objects.requireNonNull(authority);TargetSourcePosition.requireBounded(source);
        final var body=TargetProtocolActivationBody.decode(marker.canonicalBody());
        if(!scope.equals(body.payload().scope())||!Arrays.equals(lineage,body.payload().lineage())||!scope.shard().equals(source.shardId())
                ||!scope.shard().equals(marker.shardId())||marker.type()!=SystemMutationType.APPLY_SHARD_CONTROL)
            throw new IllegalArgumentException("Target activation changed actual Source scope/lineage");
        final boolean expired=source.brokerPersistenceTimeEpochMs()>marker.retryUntilEpochMs()
                ||backend.prepareRead(budget,r->r.closedIngressDeadlineThrough()>=marker.retryUntilEpochMs()).value();
        PublicKey preparedKey=null,markerKey=null;boolean authorized=false;
        if(!expired){
            preparedKey=authority.keys.resolve(control.signingKeyVersion(),source);markerKey=authority.keys.resolve(Integer.toUnsignedLong(marker.signingKeyVersion()),source);
            if(preparedKey!=null&&markerKey!=null&&control.verifySignature(preparedKey)&&marker.verifySignature(markerKey)){
                final boolean scopeCovered=authority.scopeProof.covers(control);
                final var original=authority.registrations.find(control.operationId()).orElse(null);
                try{
                    if(control.kind()!=ControlOperationKind.ACTIVATE_TARGET_PROTOCOL||!control.request().equals(body.payload().operationRequest()))throw new IllegalArgumentException("activation differs from original request");
                    final var actor=AuthorIdentity.decode(marker.authorIdentity());final var author=control.author();
                    if(actor.kind()!=AuthorIdentity.Kind.CONTROL||!Arrays.equals(actor.first(),author.operationActorIdHash())
                            ||!Arrays.equals(actor.second(),author.authenticatedRoleSetHash())||!Arrays.equals(actor.digest(),author.tenantResourceScopeHash()))
                        throw new IllegalArgumentException("activation marker changed original CONTROL author");
                    ControlOperationAuthorization.authorize(control,authority.actor,ignored->scopeCovered);
                    if(original!=null&&Arrays.equals(original.canonicalBytes(),control.canonicalBytes())){
                        ControlTargetMutationBinding.validate(control,control.targets().getFirst(),marker);
                        authorized=true;
                    }
                }catch(IllegalArgumentException denied){authorized=false;}
                if(authorized)authority.registrations.validateMutation(control,control.targets().getFirst(),marker);
            }
        }
        requireLocalCompanion(authorized);
        final Inputs selected=authorized?Objects.requireNonNull(authority.inputs.capture(control,marker,source,body),"original Target activation inputs"):null;
        final TargetProtocolActivationRecord.PublicEvidence publicEvidence;
        if(selected!=null){
            selected.current.run();publicEvidence=new TargetProtocolActivationRecord.PublicEvidence(authority.actor,preparedKey.getEncoded(),markerKey.getEncoded(),selected.resetIssuer.getEncoded(),selected.reset);
            if(!selected.readers.descriptor().equals(body.payload().descriptor())||!Arrays.equals(selected.readers.digest(),body.payload().readerEvidenceHash()))
                throw new IllegalStateException("original Target activation inputs changed signed reader/descriptor evidence");
        }else publicEvidence=null;
        final boolean allowed=authorized;final var result=new SystemMutationResult[1];
        final var batch=backend.prepare(budget,reader->{
            if(reader.source()==null||source.compareTo(reader.source())<=0)throw new IllegalStateException("first Target activation needs earlier actual Source frontier");
            final byte[] firstKey=resultKey(marker.systemMutationId()),positionKey=positionKey(source);
            absent(reader,ColumnFamily.DEDUPE,firstKey);absent(reader,ColumnFamily.DEDUPE,positionKey);
            final var stamp=new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()),source,Bytes.sha256(marker.canonicalEnvelope()));
            final var root=root(reader,stamp);final var edits=new ArrayList<TargetStoreBackend.Edit>();
            StableCode rejection=expired||reader.closedIngressDeadlineThrough()>=marker.retryUntilEpochMs()?StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                    :!allowed?StableCode.UNAUTHORIZED_SYSTEM_MUTATION:null;
            if(rejection==null){
                selected.current.run();final var payload=body.payload();
                final var initial=TargetInitialControlSnapshot.decode(payload(reader,ColumnFamily.META,KeyCodec.metaFixed(10),TargetInitialControlSnapshot.VALUE_TYPE));
                initial.mutation().requireAtOrBefore(reader.aggregate().mutation());
                if(!initial.root().equals(root.identity())||!initial.input().scope().equals(scope)||!Arrays.equals(initial.input().lineage(),lineage)
                        ||!initial.input().protocolTuples().contains(payload.descriptor().tuple())
                        ||!initial.input().protocolTuples().contains(payload.descriptor().base().systemMutationTuple()))
                    rejection=StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION;
                else{
                    final byte[] key=TargetProtocolActivationRecord.key(payload.descriptor().tuple());final byte[] raw=reader.get(ColumnFamily.META,key);
                    if(raw!=null){
                        final var old=TargetProtocolActivationRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
                        requireApplied(reader,List.of(),scope,lineage,root.identity(),old,reader.aggregate().mutation());rejection=StableCode.STALE_SYSTEM_MUTATION;
                    }else{
                        final var record=new TargetProtocolActivationRecord(marker,control,stamp,selected.readers,publicEvidence);
                        edits.add(reader.replace(ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE,record.canonicalBytes()));
                    }
                }
            }
            final var outcome=SystemMutationResult.from(marker,rejection==null?ApplyStatus.APPLIED:ApplyStatus.REJECTED,rejection==null?StableCode.OK:rejection,source.canonicalBytes());
            final TargetResultRecord.CreationAuthority results=(record,first)->{
                record.requireOwner(root);absent(reader,ColumnFamily.DEDUPE,record.key());
                if(!record.mutation().equals(stamp)||record.allocation()!=null||!Arrays.equals(record.typedPayload(),outcome.encode()))throw new IllegalStateException("activation result changed actual Source decision");
                if(record.kind()==TargetResultRecord.Kind.SYSTEM){if(first!=null||!Arrays.equals(record.key(),firstKey))throw new IllegalStateException("activation first changed key");}
                else if(record.kind()==TargetResultRecord.Kind.POSITION_SYSTEM){if(first==null||!Arrays.equals(first.key(),firstKey)||!Arrays.equals(record.key(),positionKey))throw new IllegalStateException("activation POSITION changed first");record.requireFirst(first);}
                else throw new IllegalStateException("invalid activation result kind");
            };
            final var first=TargetResultRecord.system(root,outcome,stamp,null,results);final var position=TargetResultRecord.position(root,first,stamp,results);
            edits.add(reader.replace(ColumnFamily.DEDUPE,first.key(),TargetResultRecord.VALUE_TYPE,first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE,position.key(),TargetResultRecord.VALUE_TYPE,position.canonicalBytes()));result[0]=outcome;
            return new TargetSourceAccounting(scope,lineage,source,stamp.mutationDigest(),counters,1,domains).assemble(reader,edits);
        });
        return new Prepared(this,batch,result[0],selected);
    }
    public SystemMutationResult commit(Prepared prepared,TargetStoreBackend.CommitAuthority authority){
        if(Objects.requireNonNull(prepared).owner!=this)throw new IllegalArgumentException("foreign Target activation plan");
        backend.commit(prepared.batch,(store,actualScope,mutation)->{
            final var guard=Objects.requireNonNull(authority.acquire(store,actualScope,mutation));return new TargetStoreBackend.CommitGuard(){
                public void requireCurrent(){guard.requireCurrent();if(prepared.inputs!=null)prepared.inputs.current.run();guard.requireCurrent();}
                public void close(){guard.close();}
            };
        });return prepared.result;
    }
    private TargetQuotaIncarnation root(TargetStoreBackend.Reader reader,TargetQuotaMutation stamp){
        final var id=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final byte[] key=id.key();key[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(key,payload(reader,ColumnFamily.META,key,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!root.identity().equals(id)||!Arrays.equals(root.recoveryLineage(),lineage))throw new IllegalStateException("activation lost actual Source root");
        root.latestMutation().requireAtOrBefore(reader.aggregate().mutation());stamp.requireAfter(reader.aggregate().mutation());return root;
    }
    public static void requireApplied(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,byte[] lineage,
            TargetQuotaIdentity sourceOwner,TargetProtocolActivationRecord record,TargetQuotaMutation frontier){
        record.appliedAt().requireAtOrBefore(frontier);
        if(sourceOwner.kind()!=TargetQuotaIdentity.Kind.SHARD||!record.body().payload().scope().equals(scope)||!Arrays.equals(record.body().payload().lineage(),lineage))
            throw new IllegalStateException("activation changed actual Source owner/scope/lineage");
        final var marker=record.marker();final byte[] firstKey=resultKey(marker.systemMutationId());
        final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,firstKey,TargetResultRecord.VALUE_TYPE));first.requireStored(firstKey,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
        final var result=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.primaryIdentity().equals(sourceOwner)||!first.mutation().equals(record.appliedAt())
                ||result.mutationType()!=SystemMutationType.APPLY_SHARD_CONTROL||result.applyStatus()!=ApplyStatus.APPLIED||result.stableCode()!=StableCode.OK
                ||!Arrays.equals(result.mutationId(),marker.systemMutationId())||!Arrays.equals(result.mutationHash(),marker.mutationHash())
                ||result.retryUntilEpochMs()!=marker.retryUntilEpochMs()||!Arrays.equals(result.authorIdentity(),marker.authorIdentity())
                ||!Arrays.equals(result.appliedSourcePosition(),record.appliedAt().source().canonicalBytes()))throw new IllegalStateException("activation lost exact APPLIED first");
        final byte[] ownerKey=sourceOwner.key();ownerKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner=TargetQuotaIncarnation.decodeForStore(ownerKey,payload(view,overlay,ColumnFamily.META,ownerKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("activation first changed Source lineage");owner.latestMutation().requireAtOrBefore(frontier);first.requireOwner(owner);
        final byte[] positionKey=positionKey(record.appliedAt().source());final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(owner);
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(record.appliedAt()))throw new IllegalStateException("activation lost original Source POSITION");
    }
    /** Read-only ledger dependency; no local runtime capability or current reader/Owner is substituted for historical bytes. */
    public static void requirePriorActivation(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,byte[] lineage,
            TargetQuotaIdentity sourceOwner,TargetQuotaMutation business,TargetQuotaMutation frontier){
        final byte[] key=TargetProtocolActivationRecord.key(ProtocolTuple.targetEvidenceRetirement());
        final var activation=TargetProtocolActivationRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        requireApplied(view,overlay,scope,lineage,sourceOwner,activation,frontier);business.requireAfter(activation.appliedAt());
    }
    public static boolean isActivated(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,ProtocolTuple tuple,SourcePosition source){
        final var record=applied(reader,scope,lineage,tuple);return record!=null&&record.body().payload().descriptor().supportsAcceptedHistoryTip()&&record.appliedAt().source().compareTo(source)<0;
    }
    public static boolean isActivatedForRetirement(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            ProtocolTuple tuple,SourcePosition source,TargetExternalEvidenceObjectResource resource){
        final var record=applied(reader,scope,lineage,tuple);
        return record!=null&&record.body().payload().descriptor().supportsAcceptedHistoryTip()&&record.appliedAt().source().compareTo(source)<0
                &&(!(resource.provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging)
                    ||record.body().payload().descriptor().supportsCapacityOwnerStaging());
    }
    public static void requireCapacityOwnerStaging(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,TargetQuotaMutation business,TargetQuotaMutation frontier){
        final byte[] key=TargetProtocolActivationRecord.key(ProtocolTuple.targetEvidenceRetirement());
        final var record=TargetProtocolActivationRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        requireApplied(view,overlay,scope,lineage,sourceOwner,record,frontier);business.requireAfter(record.appliedAt());
        if(!record.body().payload().descriptor().supportsCapacityOwnerStaging())
            throw new IllegalStateException("capacity-root staging requires original descriptor4 activation");
    }
    public static TargetProtocolActivationRecord applied(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,ProtocolTuple tuple){
        final byte[] key=TargetProtocolActivationRecord.key(tuple),raw=reader.get(ColumnFamily.META,key);if(raw==null)return null;
        requireLocalCompanion(true);
        final var record=TargetProtocolActivationRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
        final var owner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        requireApplied(reader,List.of(),scope,lineage,owner,record,reader.aggregate().mutation());return record;
    }
    public static void requireRetainedMarkerConfirmation(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,TargetQuotaMutation business,TargetQuotaMutation frontier){
        final byte[] key=TargetProtocolActivationRecord.key(ProtocolTuple.targetEvidenceRetirement());
        final var record=TargetProtocolActivationRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        requireApplied(view,overlay,scope,lineage,sourceOwner,record,frontier);business.requireAfter(record.appliedAt());
        if(!record.body().payload().descriptor().supportsRetainedMarkerConfirmation())
            throw new IllegalStateException("retained marker confirmation requires actual descriptor5 activation");
    }
    public static void requireHistoryBinding(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            TargetRetirementAuthorizationHistory.Grant grant,SourcePosition source){
        final var record=applied(reader,scope,lineage,grant.activation().tuple());
        if(record==null)throw new IllegalStateException("original history activation projection is unavailable");
        final var payload=record.body().payload();final var reference=grant.activation();
        if(record.appliedAt().source().compareTo(source)>=0||!Arrays.equals(record.digest(),grant.targetActivationDigest())
                ||!Arrays.equals(payload.descriptor().digest(),grant.targetDescriptorDigest())||!Arrays.equals(record.marker().mutationHash(),grant.markerHash())
                ||!Arrays.equals(reference.mutationId(),record.marker().systemMutationId())||!Arrays.equals(reference.sourcePosition().canonicalBytes(),record.appliedAt().source().canonicalBytes())
                ||!Arrays.equals(reference.canonicalSchemaHash(),payload.descriptor().targetSchemaHash())
                ||!Arrays.equals(reference.compatibleReaderSetEvidenceHash(),payload.readerEvidenceHash()))
            throw new CommandResolutionException(StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION,"original Source history mismatches actual prior Target activation");
    }
    private static byte[] resultKey(byte[] id){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},id);}
    private static void requireLocalCompanion(boolean needed){
        if(needed&&TargetCheckpointSemanticSnapshot.MAX_SCHEMA_VERSION<TargetProtocolActivationDescriptor.TARGET_CHECKPOINT_SCHEMA)
            throw new IllegalStateException("Target activation requires the independently implemented checkpoint companion8");
    }
    private static byte[] positionKey(SourcePosition source){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},source.canonicalBytes());}
    private static void absent(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key){if(reader.get(family,key)!=null)throw new IllegalStateException("first activation encountered existing Source result");}
    private static byte[] payload(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key,int type){return payload(reader,List.of(),family,key,type);}
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"actual activation dependency"),type).payload();}
}
