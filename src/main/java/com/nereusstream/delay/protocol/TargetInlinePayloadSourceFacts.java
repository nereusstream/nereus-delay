package com.nereusstream.delay.protocol;

import java.util.*;

/** Closed original Source frames. Authentication comes from retained Native first/POS and the original trust chain. */
public record TargetInlinePayloadSourceFacts(Frame fence,List<Retained> retained) {
    public static final int SCHEMA_VERSION=1;
    public static final int MAX_BYTES=TargetResourceReclaimBody.MAX_CANONICAL_BYTES;
    public static final int MAX_FRAME_BYTES=MAX_BYTES+4096;
    /** Source stamp includes the exact canonical broker persistence timestamp; connection loans are not persisted. */
    public record Frame(int kind,TargetQuotaMutation appliedAt,byte[] frame) {
        public Frame {
            Objects.requireNonNull(appliedAt).requireSourceApplied();TargetSourcePosition.requireBounded(appliedAt.source());
            if(frame==null||frame.length==0||frame.length>MAX_FRAME_BYTES||kind<1||kind>2)
                throw new IllegalArgumentException("bounded original Command/System frame required");
            frame=Bytes.copy(frame);
            byte[] digest;
            ShardId shard;
            if(kind==1){final var command=CommandCodec.decodeFrame(frame);digest=command.canonicalFrameDigest();shard=command.shardId();}
            else {
                // Reject nested reclaim capsules before recursive semantic decoding.
                final var outer=TargetCompatibilityCodec.read(frame,MAX_FRAME_BYTES,2,false,"original System envelope");
                QueryCodecSupport.requireNumbers(outer,new int[]{1,3},"original System envelope");
                final var inner=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(outer.get(1),3),MAX_FRAME_BYTES,11,false,"original System input");
                if(inner.size()!=11||QueryCodecSupport.uint32(inner.get(3),4)==SystemMutationType.TARGET_RESOURCE_RECLAIM.wireValue())
                    throw new IllegalArgumentException("reclaim cannot be an original RETAINED or fence transition");
                final var mutation=SystemMutation.decodeEnvelope(frame);digest=Bytes.sha256(mutation.canonicalEnvelope());shard=mutation.shardId();
            }
            if(!shard.equals(appliedAt.source().shardId())||!Arrays.equals(digest,appliedAt.mutationDigest()))
                throw new IllegalArgumentException("original Source frame differs from its actual APPLIED stamp");
        }
        @Override public byte[] frame(){return Bytes.copy(frame);}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,kind);CanonicalProtobuf.bytes(out,2,appliedAt.canonicalBytes());CanonicalProtobuf.bytes(out,3,frame);
        });}
        static Frame decode(byte[] encoded){
            final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,3,false,"inline original Source frame");
            QueryCodecSupport.requireNumbers(f,new int[]{1,2,3},"inline original Source frame");
            final var value=new Frame(QueryCodecSupport.uint32(f.get(0),1),TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(1),2)),QueryCodecSupport.bytes(f.get(2),3));
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"inline original Source frame");return value;
        }
    }
    public record Retained(DelayMessageId message,Frame input) {
        public Retained {Objects.requireNonNull(message);Objects.requireNonNull(input);
            if(!message.routingId().shardId().equals(input.appliedAt().source().shardId())
                    ||input.kind()==1&&!CommandCodec.decodeFrame(input.frame()).delayMessageId().equals(message))
                throw new IllegalArgumentException("original retained input changed Message/Source");
        }
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.bytes(out,1,message.bytes());CanonicalProtobuf.bytes(out,2,input.canonicalBytes());});}
        static Retained decode(byte[] encoded){
            final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,2,false,"inline retained Source input");
            QueryCodecSupport.requireNumbers(f,new int[]{1,2},"inline retained Source input");
            return new Retained(new DelayMessageId(QueryCodecSupport.bytes(f.get(0),1)),Frame.decode(QueryCodecSupport.bytes(f.get(1),2)));
        }
    }
    public TargetInlinePayloadSourceFacts {
        Objects.requireNonNull(fence);
        if(fence.kind()!=2||SystemMutation.decodeEnvelope(fence.frame()).type()!=SystemMutationType.TIME_FENCE
                ||retained==null||retained.isEmpty()||retained.size()>TargetResourceReclaimBody.MAX_OPERATIONS)
            throw new IllegalArgumentException("bounded original closure TimeFence and retained group required");
        TargetTimeFenceBody.decode(SystemMutation.decodeEnvelope(fence.frame()).canonicalBody());
        final var sorted=new TreeMap<String,Retained>();long bytes=fence.canonicalBytes().length+16L;
        for(var input:retained){
            if(sorted.put(Bytes.hex(input.message().bytes()),input)!=null)throw new IllegalArgumentException("duplicate retained Source Message");
            input.input().appliedAt().requireAtOrBefore(fence.appliedAt());
            bytes=Math.addExact(bytes,input.canonicalBytes().length+8L);
            if(bytes>MAX_BYTES)throw new IllegalArgumentException("inline SourceFacts exceeds total byte bound");
        }
        retained=List.copyOf(sorted.values());
    }
    public void requireFor(TargetResourceReclaimBody body){
        requireFor(body.objectPayloadReclaim()?List.of(body.objectOperation()):body.operations(),body.floor());
    }
    void requireFor(List<TargetResourceReclaimBody.Operation> operations,RecoveryFloorRef floor){
        if(operations.isEmpty()||!(operations.getFirst() instanceof TargetResourceReclaimBody.RetainedPayload)||retained.size()!=operations.size())
            throw new IllegalArgumentException("SourceFacts changed exact inline group");
        fence.appliedAt().requireCoveredByFloor(floor);
        for(int i=0;i<retained.size();i++){
            final var input=retained.get(i);final var operation=(TargetResourceReclaimBody.RetainedPayload)operations.get(i);
            if(!input.message().equals(operation.expected().messageId())||!input.input().appliedAt().equals(operation.expected().mutation()))
                throw new IllegalArgumentException("SourceFacts changed original frozen RETAINED owner stamp");
        }
    }
    public byte[] canonicalBytes(){
        final byte[] encoded=CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,SCHEMA_VERSION);CanonicalProtobuf.bytes(out,2,fence.canonicalBytes());
            for(var input:retained)CanonicalProtobuf.bytes(out,3,input.canonicalBytes());});
        if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("inline SourceFacts exceeds encoded byte bound");return encoded;
    }
    public static TargetInlinePayloadSourceFacts decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,TargetResourceReclaimBody.MAX_OPERATIONS+2,true,"inline SourceFacts");
        if(f.size()<3||QueryCodecSupport.uint32(f.get(0),1)!=SCHEMA_VERSION)throw new IllegalArgumentException("unknown inline SourceFacts schema");
        final var values=new ArrayList<Retained>();for(int i=2;i<f.size();i++)values.add(Retained.decode(QueryCodecSupport.bytes(f.get(i),3)));
        final var value=new TargetInlinePayloadSourceFacts(Frame.decode(QueryCodecSupport.bytes(f.get(1),2)),values);
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"inline SourceFacts");return value;
    }
}
