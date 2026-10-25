package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Bounded original Source observations: unchanged v1 Finals or exact independent collector custody. */
public record TargetSloObservationEventBody(TargetQuotaScope scope,byte[] lineage,long retryUntil,long generation,
        byte[] sampleId,byte[] bindingDigest,SloSampleFinal finalObservation,TargetSloFinalEvidence evidence,
        TargetSloCustodyReceipt custody,TargetSloObservationSample exportedSample) {
    public static final int VERSION=1;
    public static final int MAX_V1_CANONICAL_BYTES=TargetQuotaScope.MAX_CANONICAL_BYTES+TargetSloObservationSample.MAX_FINAL_BYTES+256;
    public static final int MAX_FINAL_CANONICAL_BYTES=MAX_V1_CANONICAL_BYTES+TargetSloFinalEvidence.MAX_CANONICAL_BYTES+8;
    public static final int MAX_CUSTODY_CANONICAL_BYTES=TargetQuotaScope.MAX_CANONICAL_BYTES+TargetSloCustodyReceipt.MAX_CANONICAL_BYTES
            +TargetSloObservationSample.MAX_CANONICAL_BYTES+256;
    public static final int MAX_CANONICAL_BYTES=Math.max(MAX_FINAL_CANONICAL_BYTES,MAX_CUSTODY_CANONICAL_BYTES);
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-final-source-event\0");
    private static final byte[] CUSTODY_DOMAIN=Bytes.utf8("nereus-delay-target-slo-custody-source-event\0");
    public TargetSloObservationEventBody(TargetQuotaScope scope,byte[] lineage,long retryUntil,long generation,
            byte[] sampleId,byte[] bindingDigest,SloSampleFinal observation){this(scope,lineage,retryUntil,generation,sampleId,bindingDigest,observation,null,null,null);}
    public TargetSloObservationEventBody(TargetQuotaScope scope,byte[] lineage,long retryUntil,long generation,
            byte[] sampleId,byte[] bindingDigest,SloSampleFinal observation,TargetSloFinalEvidence evidence){this(scope,lineage,retryUntil,generation,sampleId,bindingDigest,observation,evidence,null,null);}
    public TargetSloObservationEventBody {
        Objects.requireNonNull(scope);lineage=TargetCompatibilityCodec.assigned(lineage,16,"original SLO event lineage");
        sampleId=TargetCompatibilityCodec.assigned(sampleId,32,"original SLO sample ID");
        bindingDigest=TargetCompatibilityCodec.assigned(bindingDigest,32,"original SLO event binding");
        if(scope.target()!=null||retryUntil<0||generation==0)throw new IllegalArgumentException("SLO Source changed assigned scope/generation");
        if(custody==null){
            Objects.requireNonNull(finalObservation);
            if(exportedSample!=null||!Arrays.equals(sampleId,finalObservation.sampleId())
                    ||finalObservation.canonicalBytes().length>TargetSloObservationSample.MAX_FINAL_BYTES)throw new IllegalArgumentException("SLO Final changed original sample or bounds");
        }else{
            Objects.requireNonNull(exportedSample);
            if(finalObservation!=null||evidence!=null||!scope.equals(exportedSample.scope())||!Arrays.equals(lineage,exportedSample.lineage())
                    ||generation!=exportedSample.generation()||!Arrays.equals(sampleId,exportedSample.observation().sampleId())
                    ||!Arrays.equals(bindingDigest,exportedSample.bindingDigest())||!Arrays.equals(custody.sampleDigest(),exportedSample.digest()))
                throw new IllegalArgumentException("SLO custody changed exact original exported image");
        }
    }
    public static TargetSloObservationEventBody custody(TargetSloObservationSample sample,TargetSloCustodyReceipt receipt,long retryUntil){
        return new TargetSloObservationEventBody(sample.scope(),sample.lineage(),retryUntil,sample.generation(),sample.observation().sampleId(),sample.bindingDigest(),null,null,receipt,sample);
    }
    public boolean isCustody(){return custody!=null;}
    public ProtocolTuple tuple(){return isCustody()?ProtocolTuple.targetSloObservationCustody():evidence==null?ProtocolTuple.targetSloObservationEvent():ProtocolTuple.targetSloObservationEventEvidence();}
    public int schemaVersion(){return isCustody()?3:evidence==null?1:2;}
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] sampleId(){return Bytes.copy(sampleId);}
    @Override public byte[] bindingDigest(){return Bytes.copy(bindingDigest);}
    public byte[] logicalIdentity(){return isCustody()?Bytes.sha256(CUSTODY_DOMAIN,scope.canonicalBytes(),lineage,Bytes.u64beBits(generation),sampleId,bindingDigest,custody.receiptDigest(),exportedSample.digest())
            :Bytes.sha256(DOMAIN,scope.canonicalBytes(),lineage,Bytes.u64beBits(generation),sampleId,bindingDigest,finalObservation.canonicalBytes(),evidence==null?new byte[0]:evidence.digest());}
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.bytes(out,1,new ShardSubject(scope.shard()).canonicalBytes());CanonicalProtobuf.uint32(out,2,SystemMutationType.TARGET_SLO_OBSERVATION.wireValue());
        CanonicalProtobuf.uint64Bits(out,3,retryUntil);CanonicalProtobuf.uint32(out,10,schemaVersion());CanonicalProtobuf.uint32(out,11,isCustody()?2:1);
        CanonicalProtobuf.bytes(out,12,scope.canonicalBytes());CanonicalProtobuf.bytes(out,13,lineage);CanonicalProtobuf.uint64Bits(out,14,generation);
        CanonicalProtobuf.bytes(out,15,sampleId);CanonicalProtobuf.bytes(out,16,bindingDigest);
        if(isCustody()){CanonicalProtobuf.bytes(out,19,custody.canonicalBytes());CanonicalProtobuf.bytes(out,20,exportedSample.canonicalBytes());}
        else{CanonicalProtobuf.bytes(out,17,finalObservation.canonicalBytes());if(evidence!=null)CanonicalProtobuf.bytes(out,18,evidence.canonicalBytes());}
    });}
    public static TargetSloObservationEventBody decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,12,false,"Target SLO Source event");
        final int version=QueryCodecSupport.uint32(f.get(3),10);
        if(version<1||version>3)throw new IllegalArgumentException("unsupported Target SLO event schema");
        QueryCodecSupport.requireNumbers(f,version==1?new int[]{1,2,3,10,11,12,13,14,15,16,17}:version==2?new int[]{1,2,3,10,11,12,13,14,15,16,17,18}
                :new int[]{1,2,3,10,11,12,13,14,15,16,19,20},"Target SLO Source event");
        if(QueryCodecSupport.uint32(f.get(1),2)!=SystemMutationType.TARGET_SLO_OBSERVATION.wireValue()
                ||QueryCodecSupport.uint32(f.get(4),11)!=(version==3?2:1))throw new IllegalArgumentException("unsupported Target observation Source type/kind");
        final var value=new TargetSloObservationEventBody(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(5),12)),QueryCodecSupport.fixed(f.get(6),13,16),
                QueryCodecSupport.uint64Bits(f.get(2),3),QueryCodecSupport.uint64Bits(f.get(7),14),QueryCodecSupport.fixed(f.get(8),15,32),QueryCodecSupport.fixed(f.get(9),16,32),
                version==3?null:SloSampleFinal.decode(QueryCodecSupport.bytes(f.get(10),17)),version==2?TargetSloFinalEvidence.decode(QueryCodecSupport.bytes(f.get(11),18)):null,
                version==3?TargetSloCustodyReceipt.decode(QueryCodecSupport.bytes(f.get(10),19)):null,version==3?TargetSloObservationSample.decode(QueryCodecSupport.bytes(f.get(11),20)):null);
        if(!ShardSubject.decode(QueryCodecSupport.bytes(f.getFirst(),1)).shardId().equals(value.scope().shard()))throw new IllegalArgumentException("SLO Source event changed Shard");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target SLO Source event");return value;
    }
}
