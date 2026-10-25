package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Bounded, signed Source observation. This schema only carries an unchanged v1 Final, never business state. */
public record TargetSloObservationEventBody(TargetQuotaScope scope,byte[] lineage,long retryUntil,long generation,
        byte[] sampleId,byte[] bindingDigest,SloSampleFinal finalObservation) {
    public static final int VERSION=1;
    public static final int MAX_CANONICAL_BYTES=TargetQuotaScope.MAX_CANONICAL_BYTES+TargetSloObservationSample.MAX_FINAL_BYTES+256;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-final-source-event\0");
    public TargetSloObservationEventBody {
        Objects.requireNonNull(scope);Objects.requireNonNull(finalObservation);
        lineage=TargetCompatibilityCodec.assigned(lineage,16,"original SLO event lineage");
        sampleId=TargetCompatibilityCodec.assigned(sampleId,32,"original SLO sample ID");
        bindingDigest=TargetCompatibilityCodec.assigned(bindingDigest,32,"original SLO event binding");
        if(scope.target()!=null||retryUntil<0||generation==0||!Arrays.equals(sampleId,finalObservation.sampleId())
                ||finalObservation.canonicalBytes().length>TargetSloObservationSample.MAX_FINAL_BYTES)throw new IllegalArgumentException("SLO Final event changed original scope/sample or finite bound");
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] sampleId(){return Bytes.copy(sampleId);}
    @Override public byte[] bindingDigest(){return Bytes.copy(bindingDigest);}
    public byte[] logicalIdentity(){return Bytes.sha256(DOMAIN,scope.canonicalBytes(),lineage,Bytes.u64beBits(generation),sampleId,bindingDigest,finalObservation.canonicalBytes());}
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.bytes(out,1,new ShardSubject(scope.shard()).canonicalBytes());
        CanonicalProtobuf.uint32(out,2,SystemMutationType.TARGET_SLO_OBSERVATION.wireValue());CanonicalProtobuf.uint64Bits(out,3,retryUntil);
        CanonicalProtobuf.uint32(out,10,VERSION);CanonicalProtobuf.uint32(out,11,1);CanonicalProtobuf.bytes(out,12,scope.canonicalBytes());
        CanonicalProtobuf.bytes(out,13,lineage);CanonicalProtobuf.uint64Bits(out,14,generation);CanonicalProtobuf.bytes(out,15,sampleId);
        CanonicalProtobuf.bytes(out,16,bindingDigest);CanonicalProtobuf.bytes(out,17,finalObservation.canonicalBytes());
    });}
    public static TargetSloObservationEventBody decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,11,false,"Target SLO Source event");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,10,11,12,13,14,15,16,17},"Target SLO Source event");
        if(QueryCodecSupport.uint32(f.get(1),2)!=SystemMutationType.TARGET_SLO_OBSERVATION.wireValue()
                ||QueryCodecSupport.uint32(f.get(3),10)!=VERSION||QueryCodecSupport.uint32(f.get(4),11)!=1)
            throw new IllegalArgumentException("unsupported Target observation Source type/schema/kind");
        final var value=new TargetSloObservationEventBody(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(5),12)),QueryCodecSupport.fixed(f.get(6),13,16),
                QueryCodecSupport.uint64Bits(f.get(2),3),QueryCodecSupport.uint64Bits(f.get(7),14),QueryCodecSupport.fixed(f.get(8),15,32),
                QueryCodecSupport.fixed(f.get(9),16,32),SloSampleFinal.decode(QueryCodecSupport.bytes(f.get(10),17)));
        if(!ShardSubject.decode(QueryCodecSupport.bytes(f.getFirst(),1)).shardId().equals(value.scope().shard()))throw new IllegalArgumentException("SLO event changed original Source Shard");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target SLO Source event");return value;
    }
}
