package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Original registered ApplyShardControl subset for a writer epoch. No SERVICE contract is needed to issue Control. */
public record TargetSloWriterControlBody(ShardId shard,long retryUntil,ControlRef controlRef,
        TargetSloWriterControlRequest request,int expectedRecordState,TargetSloControlScopeHeader.Reference admissionScope) {
    public static final int MAX_CANONICAL_BYTES=TargetSloWriterControlRequest.MAX_CANONICAL_BYTES
            +192+TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES+8;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-writer-control\0");
    public TargetSloWriterControlBody(ShardId shard,long retryUntil,ControlRef controlRef,
            TargetSloWriterControlRequest request,int expectedRecordState){
        this(shard,retryUntil,controlRef,request,expectedRecordState,null);
    }
    public TargetSloWriterControlBody {
        Objects.requireNonNull(shard);Objects.requireNonNull(controlRef);Objects.requireNonNull(request);
        TargetCompatibilityCodec.assigned(controlRef.operationId(),32,"SLO writer Control operation ID");
        final int expected=request.operationKind()==ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH?1:0;
        if(retryUntil<0||controlRef.targetIndex()!=0||!shard.equals(request.scope().shard())||expectedRecordState!=expected
                ||!Arrays.equals(controlRef.requestHash(),PreparedControlOperation.requestHash(request.operationKind(),request.operationRequest())))
            throw new IllegalArgumentException("writer Control changed original request/Source/row precondition");
    }
    public byte[] logicalIdentity(){return controlRef.logicalOperationIdentity(request.controlKind());}
    public int schemaVersion(){return admissionScope==null?1:2;}
    public ProtocolTuple tuple(){return admissionScope==null?ProtocolTuple.targetSloWriterEpoch():ProtocolTuple.targetSloScopedWriterEpoch();}
    public byte[] semanticHash(){return admissionScope==null?Bytes.sha256(DOMAIN,Bytes.u16be(request.controlKind()),request.canonicalBytes())
            :Bytes.sha256(DOMAIN,Bytes.u16be(2),Bytes.u16be(request.controlKind()),request.canonicalBytes(),admissionScope.canonicalBytes());}
    public static boolean isControl(SystemMutation input){
        if(input.type()!=SystemMutationType.APPLY_SHARD_CONTROL)return false;
        final var reader=new CanonicalProtobuf.Reader(input.canonicalBody());
        for(int count=0;count<5&&reader.hasRemaining();count++){
            final var field=reader.next();
            if(field.number()==11){
                if(field.wireType()!=0)throw new IllegalArgumentException("writer Control discriminator is not varint");
                return field.unsignedValue()==TargetSloWriterControlRequest.REGISTER_CONTROL_KIND||field.unsignedValue()==TargetSloWriterControlRequest.CLOSE_CONTROL_KIND;
            }
        }
        return false;
    }
    public byte[] canonicalBytes(){
        final var encoded=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.bytes(out,1,new ShardSubject(shard).canonicalBytes());CanonicalProtobuf.uint32(out,2,SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64Bits(out,3,retryUntil);CanonicalProtobuf.bytes(out,10,controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out,11,request.controlKind());CanonicalProtobuf.uint32(out,12,schemaVersion());CanonicalProtobuf.bytes(out,13,semanticHash());
            CanonicalProtobuf.uint32(out,14,expectedRecordState);
            CanonicalProtobuf.bytes(out,15,CanonicalProtobuf.message(payload->CanonicalProtobuf.bytes(payload,request.controlKind(),request.canonicalBytes())));
            if(admissionScope!=null)CanonicalProtobuf.bytes(out,16,admissionScope.canonicalBytes());
        });
        if(encoded.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("SLO writer Control body exceeds its codec bound");return encoded;
    }
    public static TargetSloWriterControlBody decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,10,false,"Target SLO writer Control body");
        if(f.size()!=9&&f.size()!=10)throw new IllegalArgumentException("unknown writer Control field count");
        QueryCodecSupport.requireNumbers(f,f.size()==9?new int[]{1,2,3,10,11,12,13,14,15}:new int[]{1,2,3,10,11,12,13,14,15,16},"Target SLO writer Control body");
        final int kind=QueryCodecSupport.uint32(f.get(4),11);
        final var operation=switch(kind){
            case TargetSloWriterControlRequest.REGISTER_CONTROL_KIND->ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH;
            case TargetSloWriterControlRequest.CLOSE_CONTROL_KIND->ControlOperationKind.CLOSE_TARGET_SLO_WRITER_EPOCH;
            default->throw new IllegalArgumentException("unknown SLO writer Control kind");
        };
        if(QueryCodecSupport.uint32(f.get(1),2)!=SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                ||QueryCodecSupport.uint32(f.get(5),12)!=(f.size()==9?1:2))throw new IllegalArgumentException("unknown SLO writer Control type/schema");
        final var shard=QueryCodecSupport.bytes(f.getFirst(),1);TargetCompatibilityCodec.read(shard,24,2,false,"writer Control Shard subject");
        final var ref=QueryCodecSupport.bytes(f.get(3),10);TargetCompatibilityCodec.read(ref,74,3,false,"writer ControlRef");
        final var payload=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(8),15),TargetSloWriterControlRequest.MAX_CANONICAL_BYTES+8,1,false,"writer Control payload");
        QueryCodecSupport.requireNumbers(payload,new int[]{kind},"writer Control payload");
        final var value=new TargetSloWriterControlBody(ShardSubject.decode(shard).shardId(),QueryCodecSupport.uint64Bits(f.get(2),3),ControlRef.decode(ref),
                TargetSloWriterControlRequest.decode(operation,QueryCodecSupport.bytes(payload.getFirst(),kind)),QueryCodecSupport.uint32(f.get(7),14),
                f.size()==9?null:TargetSloControlScopeHeader.Reference.decode(QueryCodecSupport.bytes(f.get(9),16)));
        if(!Arrays.equals(value.semanticHash(),QueryCodecSupport.fixed(f.get(6),13,32)))throw new IllegalArgumentException("writer Control semantic hash mismatch");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target SLO writer Control body");return value;
    }
}
