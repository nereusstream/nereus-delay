package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Independent bootstrap ApplyShardControl branch; only actual Source apply may allocate its obligation. */
public record TargetScopeControlObligationBody(ShardId shard,long retryUntil,ControlRef controlRef,
        TargetScopeControlObligationRequest request,int expectedRecordState) {
    public static final int BODY_VERSION=24;
    public static final int FROZEN_BODY_VERSION=25;
    public static final int EXPOSURE_BODY_VERSION=26;
    public static final int V2_MAX_CANONICAL_BYTES=3_185_601;
    public static final int MAX_CANONICAL_BYTES=TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES+192;
    public static final int MAX_INPUT_BYTES=MAX_CANONICAL_BYTES+1024;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-scope-control-obligation\0");
    public TargetScopeControlObligationBody {
        Objects.requireNonNull(shard);Objects.requireNonNull(controlRef);Objects.requireNonNull(request);
        request.requireControlRef(controlRef);
        requirePlanSchema(request.definition().plan().schemaVersion());
        if(retryUntil<0||retryUntil==Long.MAX_VALUE||!shard.equals(request.scope().shard())
                ||expectedRecordState!=1||expectedRecordState!=request.expectedRecordState())
            throw new IllegalArgumentException("Scope Control obligation changed its finite Source retry, Shard or absence precondition");
    }
    public byte[] logicalIdentity(){return controlRef.logicalOperationIdentity(TargetScopeControlObligationRequest.CONTROL_KIND);}
    public int schemaVersion(){return requirePlanSchema(request.definition().plan().schemaVersion());}
    public ProtocolTuple tuple(){return switch(schemaVersion()){
        case 1->ProtocolTuple.targetScopeControlObligation();case 2->ProtocolTuple.targetScopeControlObligationFrozen();
        case 3->ProtocolTuple.targetScopeControlObligationExposure();default->throw new IllegalStateException("unknown Scope Control obligation schema");};}
    public byte[] semanticHash(){return Bytes.sha256(DOMAIN,Bytes.u16be(TargetScopeControlObligationRequest.CONTROL_KIND),request.canonicalBytes());}
    public static boolean isControl(SystemMutation input){
        if(input.type()!=SystemMutationType.APPLY_SHARD_CONTROL)return false;
        final var reader=new CanonicalProtobuf.Reader(input.canonicalBody());
        for(int count=0;count<5&&reader.hasRemaining();count++){
            final var field=reader.next();
            if(field.number()==11){
                if(field.wireType()!=0)throw new IllegalArgumentException("Scope Control obligation discriminator is not varint");
                return field.unsignedValue()==TargetScopeControlObligationRequest.CONTROL_KIND;
            }
        }
        return false;
    }
    public byte[] canonicalBytes(){
        final var encoded=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.bytes(out,1,new ShardSubject(shard).canonicalBytes());CanonicalProtobuf.uint32(out,2,SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64Bits(out,3,retryUntil);CanonicalProtobuf.bytes(out,10,controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out,11,TargetScopeControlObligationRequest.CONTROL_KIND);CanonicalProtobuf.uint32(out,12,schemaVersion());
            CanonicalProtobuf.bytes(out,13,semanticHash());CanonicalProtobuf.uint32(out,14,expectedRecordState);
            CanonicalProtobuf.bytes(out,15,CanonicalProtobuf.message(payload->
                    CanonicalProtobuf.bytes(payload,TargetScopeControlObligationRequest.CONTROL_KIND,request.canonicalBytes())));
        });
        if(encoded.length>(schemaVersion()==3?MAX_CANONICAL_BYTES:V2_MAX_CANONICAL_BYTES))
            throw new IllegalArgumentException("Scope Control obligation body exceeds its codec bound");return encoded;
    }
    public static TargetScopeControlObligationBody decode(byte[] encoded){
        final var fields=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,9,false,"Target Scope Control obligation body");
        QueryCodecSupport.requireNumbers(fields,new int[]{1,2,3,10,11,12,13,14,15},"Target Scope Control obligation body");
        final int schema=QueryCodecSupport.uint32(fields.get(5),12);
        if(QueryCodecSupport.uint32(fields.get(1),2)!=SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                ||QueryCodecSupport.uint32(fields.get(4),11)!=TargetScopeControlObligationRequest.CONTROL_KIND
                ||(schema!=1&&schema!=2&&schema!=3))
            throw new IllegalArgumentException("unknown Scope Control obligation type/kind/schema");
        final var shard=QueryCodecSupport.bytes(fields.getFirst(),1);TargetCompatibilityCodec.read(shard,24,2,false,"Scope Control obligation Shard subject");
        final var ref=QueryCodecSupport.bytes(fields.get(3),10);TargetCompatibilityCodec.read(ref,74,3,false,"Scope Control obligation ControlRef");
        final var payload=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(fields.get(8),15),
                TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES+8,1,false,"Scope Control obligation payload");
        QueryCodecSupport.requireNumbers(payload,new int[]{TargetScopeControlObligationRequest.CONTROL_KIND},"Scope Control obligation payload");
        final var result=new TargetScopeControlObligationBody(ShardSubject.decode(shard).shardId(),QueryCodecSupport.uint64Bits(fields.get(2),3),ControlRef.decode(ref),
                TargetScopeControlObligationRequest.decode(QueryCodecSupport.bytes(payload.getFirst(),TargetScopeControlObligationRequest.CONTROL_KIND)),
                QueryCodecSupport.uint32(fields.get(7),14));
        if(result.schemaVersion()!=schema)throw new IllegalArgumentException("Scope Control obligation schema differs from its original Native plan");
        if(!Arrays.equals(result.semanticHash(),QueryCodecSupport.fixed(fields.get(6),13,32)))
            throw new IllegalArgumentException("Scope Control obligation semantic hash mismatch");
        QueryCodecSupport.requireCanonical(encoded,result.canonicalBytes(),"Target Scope Control obligation body");return result;
    }
    private static int requirePlanSchema(int schema){
        if(schema!=1&&schema!=2&&schema!=3)throw new IllegalArgumentException("unknown Scope Control obligation Native plan schema");
        return schema;
    }
}
