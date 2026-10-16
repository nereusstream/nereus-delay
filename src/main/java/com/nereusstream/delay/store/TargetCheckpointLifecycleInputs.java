package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

/** Complete public g8 checkpoint inputs; independent original authority and protection are mandatory. */
public record TargetCheckpointLifecycleInputs(List<TargetProtocolActivationRecord> activations,
        List<TargetEvidenceRetiringRecord> retirements,List<TargetEvidenceConfirmedRecord> confirmations,List<HistoryImage> histories){
    public record HistoryImage(SourcePosition source,SystemMutation image,TargetRetirementAuthorizationHistory.PublicProof proof){
        public HistoryImage{
            TargetSourcePosition.requireBounded(source);Objects.requireNonNull(image);
            if(!source.shardId().equals(image.shardId()))throw new IllegalArgumentException("retirement history crossed Source Shard");
            resource(image); // Closed body8 only, including durable rejected inputs.
        }
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.bytes(out,1,source.canonicalBytes());CanonicalProtobuf.bytes(out,2,image.canonicalEnvelope());
            if(proof!=null)CanonicalProtobuf.bytes(out,3,proof.canonicalBytes());
        });}
        static HistoryImage decode(byte[] encoded,int maximumBytes){
            final var f=read(encoded,maximumBytes,3,false);
            QueryCodecSupport.requireNumbers(f,f.size()==3?new int[]{1,2,3}:new int[]{1,2},"retirement history image");
            final var value=new HistoryImage(TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(0),1)),
                    SystemMutation.decodeEnvelope(QueryCodecSupport.bytes(f.get(1),2)),
                    f.size()==3?TargetRetirementAuthorizationHistory.PublicProof.decode(QueryCodecSupport.bytes(f.get(2),3)):null);
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"retirement history image");return value;
        }
    }
    /**
     * Original Root providers run outside Store locks on the existing CHECKPOINT turn and retain all input loans.
     * Pins must be independently trusted at original Source. requireOriginal also authenticates a rejected
     * first's actual reason; a positive purpose grant alone cannot override another independently proved denial.
     * No old Owner/session must remain currently alive.
     * Activation includes original CONTROL/registration/keys, complete reader membership/revisions/sessions,
     * base environment/reset and actual binary/SDK locks. Retiring/confirmed checks include original purpose,
     * protection/capacity/copy and authenticated exact provider deletion, respectively; DTOs never suffice.
     */
    public interface Authority{
        void requireActivation(TargetProtocolActivationRecord record);
        HistoryImage resolve(TargetResultRecord originalFirst);
        TargetRetirementAuthorizationHistory.Pin requireOriginal(HistoryImage input,TargetResultRecord originalFirst);
        void requireRetiring(TargetEvidenceRetiringRecord record,TargetRetirementAuthorizationHistory.Grant grant);
        void requireConfirmed(TargetEvidenceConfirmedRecord record,HistoryImage originalIntent,
                TargetRetirementAuthorizationHistory.Grant intentGrant,TargetRetirementAuthorizationHistory.Grant confirmationGrant);
    }
    public TargetCheckpointLifecycleInputs{
        activations=ordered(activations,TargetProtocolActivationRecord::key);retirements=ordered(retirements,TargetEvidenceRetiringRecord::key);
        confirmations=ordered(confirmations,TargetEvidenceConfirmedRecord::key);histories=ordered(histories,v->v.image().systemMutationId());
    }
    public boolean requiresRetainedMarkerCapability(){
        return activations.stream().anyMatch(v->v.body().payload().descriptor().supportsRetainedMarkerConfirmation())
                ||confirmations.stream().anyMatch(v->v.body().markerRetained())
                ||histories.stream().anyMatch(v->v.image().type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED
                    &&TargetEvidenceDeleteConfirmedBody.decode(v.image().canonicalBody()).markerRetained());
    }
    public int recordCount(){return Math.addExact(Math.addExact(activations.size(),retirements.size()),Math.addExact(confirmations.size(),histories.size()));}
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);
        for(var v:activations)CanonicalProtobuf.bytes(out,2,v.canonicalBytes());
        for(var v:retirements)CanonicalProtobuf.bytes(out,3,v.canonicalBytes());
        for(var v:confirmations)CanonicalProtobuf.bytes(out,4,v.canonicalBytes());
        for(var v:histories)CanonicalProtobuf.bytes(out,5,v.canonicalBytes());
    });}
    public static TargetCheckpointLifecycleInputs collect(TargetCheckpointDependencies d,Authority authority,
            BoundedReadBudget budget,TargetCheckpointSemanticSnapshot.Limits limits,Runnable guard){
        if(!d.hasLifecycleInputs())return new TargetCheckpointLifecycleInputs(List.of(),List.of(),List.of(),List.of());
        Objects.requireNonNull(authority,"complete original Target lifecycle authority");
        final long records=(long)d.protocolActivations().size()+d.retirements().size()+d.evidenceConfirmations().size()+d.retirementResults().size();
        if(records>limits.maximumRecords())throw new IllegalArgumentException("lifecycle inputs exceed checkpoint record bound");
        final var images=new ArrayList<HistoryImage>();long bytes=2;
        for(var v:d.protocolActivations())bytes=Math.addExact(bytes,v.canonicalBytes().length+8L);
        for(var v:d.retirements())bytes=Math.addExact(bytes,v.canonicalBytes().length+8L);
        for(var v:d.evidenceConfirmations())bytes=Math.addExact(bytes,v.canonicalBytes().length+8L);
        if(bytes>limits.maximumBytes())throw new IllegalArgumentException("native lifecycle rows exceed checkpoint byte bound");
        final var stored=new TreeMap<String,TargetRetirementSourceHistoryRecord>();for(var value:d.sourceHistories())stored.put(Bytes.hex(value.input().systemMutationId()),value);
        for(var first:d.retirementResults()){
            before(budget,guard);final var retained=stored.get(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()));
            final var image=retained==null?Objects.requireNonNull(authority.resolve(first),"original retirement frame/proof")
                    :new HistoryImage(retained.decidedAt().source(),retained.input(),retained.proof());
            guard.run();final int retainedBytes=image.canonicalBytes().length;bytes=Math.addExact(bytes,retainedBytes+8L);
            if(bytes>limits.maximumBytes())throw new IllegalArgumentException("complete lifecycle inputs exceed checkpoint byte bound");
            charge(budget,first.key().length,retainedBytes);after(budget,guard);images.add(image);
        }
        final var value=new TargetCheckpointLifecycleInputs(d.protocolActivations(),d.retirements(),d.evidenceConfirmations(),images);
        value.authenticate(d,authority,budget,limits,guard);return value;
    }
    public void validateAgainst(TargetCheckpointDependencies d,TargetCheckpointSemanticSnapshot.Limits limits){
        if(recordCount()>limits.maximumRecords()||canonicalBytes().length>limits.maximumBytes())throw new IllegalArgumentException("lifecycle companion exceeds finite bounds");
        same(activations,d.protocolActivations(),TargetProtocolActivationRecord::canonicalBytes);
        same(retirements,d.retirements(),TargetEvidenceRetiringRecord::canonicalBytes);
        same(confirmations,d.evidenceConfirmations(),TargetEvidenceConfirmedRecord::canonicalBytes);
        final var actualFirsts=firsts(d);final var pending=new TreeMap<>(actualFirsts);if(histories.size()!=pending.size())throw new IllegalArgumentException("checkpoint omits original retirement first frames");
        for(var input:histories){
            final var first=pending.remove(Bytes.hex(input.image().systemMutationId()));if(first==null)throw new IllegalArgumentException("extra retirement history input");
            correlate(input.source(),input.image(),first);
            if((applied(first)||SystemMutationResult.decode(first.typedPayload()).stableCode()==StableCode.UNAUTHORIZED_SYSTEM_MUTATION)&&input.proof()==null)
                throw new IllegalStateException("APPLIED or history-denied retirement has no complete original public history proof");
        }
        final var byId=images();
        for(var retained:d.sourceHistories()){
            final var input=Objects.requireNonNull(byId.get(Bytes.hex(retained.input().systemMutationId())),"checkpoint omitted actual Native Source history");
            final var first=Objects.requireNonNull(actualFirsts.get(Bytes.hex(retained.input().systemMutationId())),"retained Source history has no actual first");
            if(!retained.decidedAt().equals(first.mutation())||!Arrays.equals(input.image().canonicalEnvelope(),retained.input().canonicalEnvelope())
                    ||input.proof()==null||!Arrays.equals(input.proof().canonicalBytes(),retained.proof().canonicalBytes()))
                throw new IllegalArgumentException("checkpoint replaced original atomic Source history");
        }
        final var nativeHistories=new TreeMap<String,TargetRetirementSourceHistoryRecord>();
        for(var retained:d.sourceHistories())nativeHistories.put(Bytes.hex(retained.input().systemMutationId()),retained);
        for(var tip:d.historyTips()){
            final var retained=Objects.requireNonNull(nativeHistories.get(Bytes.hex(tip.mutationId())),"accepted tip lost original Native history");
            tip.requireDecision(retained);tip.decidedAt().requireAtOrBefore(d.root().aggregate().mutation());
            final var input=Objects.requireNonNull(byId.get(Bytes.hex(tip.mutationId())),"checkpoint omitted accepted tip Source frame");
            if(input.proof()==null||!Arrays.equals(input.proof().canonicalBytes(),tip.proof().canonicalBytes()))throw new IllegalArgumentException("checkpoint changed actual persisted accepted tip");
        }
        final var controls=new TreeMap<String,TargetResultRecord>();
        for(var first:d.controlResults())controls.put(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()),first);
        for(var value:activations){
            final var first=Objects.requireNonNull(controls.get(Bytes.hex(value.marker().systemMutationId())),"actual activation first");
            correlate(value.appliedAt().source(),value.marker(),first);
            if(!applied(first)||!first.mutation().equals(value.appliedAt()))throw new IllegalArgumentException("activation differs from original APPLIED first");
        }
        final var images=images();final var states=new java.util.HashSet<String>();
        for(var value:retirements){
            final var input=requireImage(images,value.intent().systemMutationId(),value.appliedAt());
            if(!actualFirsts.get(Bytes.hex(input.image().systemMutationId())).mutation().equals(value.appliedAt())
                    ||!Arrays.equals(input.image().canonicalEnvelope(),value.intent().canonicalEnvelope())||!applied(actualFirsts.get(Bytes.hex(input.image().systemMutationId()))))throw new IllegalArgumentException("retiring row changed original intent");
            states.add(Bytes.hex(value.intent().systemMutationId()));
        }
        for(var value:confirmations){
            final var confirmed=requireImage(images,value.confirmation().systemMutationId(),value.appliedAt());
            final var intent=requireImage(images,value.body().intent().mutationId(),value.intentAppliedAt());
            final var original=TargetEvidenceRetireIntentBody.decode(intent.image().canonicalBody());
            if(!actualFirsts.get(Bytes.hex(intent.image().systemMutationId())).mutation().equals(value.intentAppliedAt())
                    ||!actualFirsts.get(Bytes.hex(confirmed.image().systemMutationId())).mutation().equals(value.appliedAt())
                    ||!applied(actualFirsts.get(Bytes.hex(intent.image().systemMutationId())))||!applied(actualFirsts.get(Bytes.hex(confirmed.image().systemMutationId())))||!Arrays.equals(confirmed.image().canonicalEnvelope(),value.confirmation().canonicalEnvelope())
                    ||!Arrays.equals(value.body().intent().mutationHash(),intent.image().mutationHash())
                    ||!Arrays.equals(value.body().resource().canonicalBytes(),original.resource().canonicalBytes())
                    ||!Arrays.equals(value.body().context().canonicalBytes(),original.context().canonicalBytes()))throw new IllegalArgumentException("confirmed row lost original intent/frame/context");
            if(!states.add(Bytes.hex(intent.image().systemMutationId())))throw new IllegalArgumentException("intent has both retiring and confirmed state");
            states.add(Bytes.hex(value.confirmation().systemMutationId()));
        }
        for(var input:histories)if(applied(actualFirsts.get(Bytes.hex(input.image().systemMutationId())))&&!states.contains(Bytes.hex(input.image().systemMutationId())))throw new IllegalStateException("APPLIED retirement lacks its actual state1/2 row");
    }
    public void authenticate(TargetCheckpointDependencies d,Authority authority,BoundedReadBudget budget,
            TargetCheckpointSemanticSnapshot.Limits limits,Runnable guard){
        validateAgainst(d,limits);if(!d.hasLifecycleInputs()){guard.run();return;}
        Objects.requireNonNull(authority,"independent original lifecycle authority");
        for(var value:activations){before(budget,guard);charge(budget,value.key().length,value.canonicalBytes().length);authority.requireActivation(value);after(budget,guard);}
        final var actualFirsts=firsts(d);final var grants=new TreeMap<String,TargetRetirementAuthorizationHistory.Grant>();
        for(var input:histories){
            before(budget,guard);charge(budget,input.source().canonicalBytes().length,input.canonicalBytes().length);
            final var first=actualFirsts.get(Bytes.hex(input.image().systemMutationId()));final var pin=authority.requireOriginal(input,first);after(budget,guard);
            if(input.proof()==null)continue;
            final var verified=input.proof().authenticate(Objects.requireNonNull(pin,"independent original purpose pin"));
            final var selected=verified.select(resource(input.image()),input.source(),AuthorIdentity.decode(input.image().authorIdentity()),
                    input.image().signingKeyVersion(),TargetRetirementAuthorizationHistory.Action.of(input.image().type()));
            if(!applied(first)){
                after(budget,guard);continue;
            }
            final var grant=selected.orElseThrow(()->new IllegalArgumentException("original public history denies APPLIED retirement"));
            if(!input.image().verifySignature(grant.writerKey()))throw new IllegalArgumentException("retirement original signature is invalid");
            final var activation=activations.stream().filter(a->a.body().payload().descriptor().tuple().equals(grant.activation().tuple())).findFirst()
                    .orElseThrow(()->new IllegalStateException("retirement proof lacks actual prior activation"));
            grant.requireActivation(activation,input.source());
            if(input.image().type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED
                    &&TargetEvidenceDeleteConfirmedBody.decode(input.image().canonicalBody()).markerRetained()
                    &&!activation.body().payload().descriptor().supportsRetainedMarkerConfirmation())
                throw new IllegalStateException("APPLIED retained marker history lacks actual descriptor5 capability");
            grants.put(Bytes.hex(input.image().systemMutationId()),grant);after(budget,guard);
        }
        for(var value:retirements){before(budget,guard);charge(budget,value.key().length,value.canonicalBytes().length);authority.requireRetiring(value,grants.get(Bytes.hex(value.intent().systemMutationId())));after(budget,guard);}
        final var images=images();
        for(var value:confirmations){
            before(budget,guard);charge(budget,value.key().length,value.canonicalBytes().length);authority.requireConfirmed(value,images.get(Bytes.hex(value.body().intent().mutationId())),
                    grants.get(Bytes.hex(value.body().intent().mutationId())),grants.get(Bytes.hex(value.confirmation().systemMutationId())));after(budget,guard);
        }
    }
    public static TargetCheckpointLifecycleInputs decode(byte[] encoded,TargetCheckpointSemanticSnapshot.Limits limits){
        final var f=read(encoded,limits.maximumBytes(),limits.maximumRecords()+1,true);
        if(f.isEmpty()||QueryCodecSupport.uint32(f.get(0),1)!=1)throw new IllegalArgumentException("unknown lifecycle companion schema");
        final var a=new ArrayList<TargetProtocolActivationRecord>();final var r=new ArrayList<TargetEvidenceRetiringRecord>();
        final var c=new ArrayList<TargetEvidenceConfirmedRecord>();final var h=new ArrayList<HistoryImage>();int i=1;
        while(i<f.size()&&f.get(i).number()==2)a.add(TargetProtocolActivationRecord.decode(QueryCodecSupport.bytes(f.get(i++),2)));
        while(i<f.size()&&f.get(i).number()==3)r.add(TargetEvidenceRetiringRecord.decode(QueryCodecSupport.bytes(f.get(i++),3)));
        while(i<f.size()&&f.get(i).number()==4)c.add(TargetEvidenceConfirmedRecord.decode(QueryCodecSupport.bytes(f.get(i++),4)));
        while(i<f.size()&&f.get(i).number()==5)h.add(HistoryImage.decode(QueryCodecSupport.bytes(f.get(i++),5),limits.maximumBytes()));
        if(i!=f.size())throw new IllegalArgumentException("unknown lifecycle companion fields");
        final var value=new TargetCheckpointLifecycleInputs(a,r,c,h);QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target lifecycle companion");return value;
    }
    private TreeMap<String,HistoryImage> images(){final var result=new TreeMap<String,HistoryImage>();for(var image:histories)result.put(Bytes.hex(image.image().systemMutationId()),image);return result;}
    private static TreeMap<String,TargetResultRecord> firsts(TargetCheckpointDependencies d){final var result=new TreeMap<String,TargetResultRecord>();for(var first:d.retirementResults())result.put(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()),first);return result;}
    private static HistoryImage requireImage(TreeMap<String,HistoryImage> images,byte[] id,TargetQuotaMutation stamp){
        final var value=Objects.requireNonNull(images.get(Bytes.hex(id)),"original signed retirement frame");
        if(!Arrays.equals(value.source().canonicalBytes(),stamp.source().canonicalBytes())||!Arrays.equals(Bytes.sha256(value.image().canonicalEnvelope()),stamp.mutationDigest()))throw new IllegalArgumentException("retirement image changed actual Source stamp");return value;
    }
    private static void correlate(SourcePosition source,SystemMutation image,TargetResultRecord first){
        final var result=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!Arrays.equals(first.mutation().source().canonicalBytes(),source.canonicalBytes())
                ||!Arrays.equals(first.mutation().mutationDigest(),Bytes.sha256(image.canonicalEnvelope()))||result.mutationType()!=image.type()
                ||!Arrays.equals(result.mutationId(),image.systemMutationId())||!Arrays.equals(result.mutationHash(),image.mutationHash())
                ||!Arrays.equals(result.authorIdentity(),image.authorIdentity())||result.retryUntilEpochMs()!=image.retryUntilEpochMs()
                ||!Arrays.equals(result.appliedSourcePosition(),source.canonicalBytes())
                ||result.applyStatus()==ApplyStatus.APPLIED&&(result.stableCode()!=StableCode.OK||source.brokerPersistenceTimeEpochMs()>image.retryUntilEpochMs()))throw new IllegalArgumentException("lifecycle input changed actual original first");
    }
    private static boolean applied(TargetResultRecord first){final var result=SystemMutationResult.decode(first.typedPayload());return result.applyStatus()==ApplyStatus.APPLIED;}
    private static TargetExternalEvidenceObjectResource resource(SystemMutation image){return switch(image.type()){
        case RESOURCE_RETIRE_INTENT->TargetEvidenceRetireIntentBody.decode(image.canonicalBody()).resource();
        case RESOURCE_DELETE_CONFIRMED->TargetEvidenceDeleteConfirmedBody.decode(image.canonicalBody()).resource();
        default->throw new IllegalArgumentException("not a Target retirement frame");};}
    private static void before(BoundedReadBudget budget,Runnable guard){guard.run();if(!budget.beforeRead())throw budget.incomplete();}
    private static void after(BoundedReadBudget budget,Runnable guard){guard.run();if(!budget.beforeTimedWork())throw budget.incomplete();}
    private static void charge(BoundedReadBudget budget,int key,int value){if(!budget.tryCharge(key,value))throw budget.incomplete();}
    private static List<CanonicalProtobuf.Reader.Field> read(byte[] bytes,int maximum,int fields,boolean repeated){
        if(bytes==null||bytes.length==0||bytes.length>maximum)throw new IllegalArgumentException("lifecycle input exceeds finite byte bound");
        final var reader=new CanonicalProtobuf.Reader(bytes,repeated);final var result=new ArrayList<CanonicalProtobuf.Reader.Field>();
        while(reader.hasRemaining()){if(result.size()>=fields)throw new IllegalArgumentException("lifecycle input exceeds field bound");result.add(reader.next());}return result;
    }
    private static <T> List<T> ordered(List<T> values,Function<T,byte[]> key){final var map=new TreeMap<String,T>();for(var value:values)if(map.put(Bytes.hex(key.apply(value)),value)!=null)throw new IllegalArgumentException("duplicate lifecycle identity");return List.copyOf(map.values());}
    private static <T> void same(List<T> actual,List<T> expected,Function<T,byte[]> bytes){
        if(actual.size()!=expected.size())throw new IllegalArgumentException("lifecycle companion changed Native inventory size");
        for(int i=0;i<actual.size();i++)if(!Arrays.equals(bytes.apply(actual.get(i)),bytes.apply(expected.get(i))))
            throw new IllegalArgumentException("lifecycle companion changed actual Native inventory");
    }
}
