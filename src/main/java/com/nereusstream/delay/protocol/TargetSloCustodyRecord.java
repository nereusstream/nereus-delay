package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/** Immutable Source custody of one exact exported sample version; it does not modify or delete that sample. */
public record TargetSloCustodyRecord(SystemMutation input,TargetQuotaMutation appliedAt) {
    public static final int VALUE_TYPE=61;
    public static final int MAX_INPUT_BYTES=TargetSloObservationEventBody.MAX_CUSTODY_CANONICAL_BYTES+TargetSloObservationPolicy.MAX_SERVICE_BYTES+512;
    public static final int MAX_CANONICAL_BYTES=MAX_INPUT_BYTES+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+64;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-custody-source-record\0");
    public TargetSloCustodyRecord {
        Objects.requireNonNull(input);Objects.requireNonNull(appliedAt).requireSourceApplied();
        final var body=TargetSloObservationEventBody.decode(input.canonicalBody());
        if(input.type()!=SystemMutationType.TARGET_SLO_OBSERVATION||!body.isCustody()||!input.shardId().equals(body.scope().shard())
                ||!input.shardId().equals(appliedAt.source().shardId())||input.retryUntilEpochMs()!=body.retryUntil()
                ||input.canonicalEnvelope().length>MAX_INPUT_BYTES||!Arrays.equals(input.logicalOperationIdentity(),body.logicalIdentity())
                ||!Arrays.equals(appliedAt.mutationDigest(),Bytes.sha256(input.canonicalEnvelope()))
                ||appliedAt.source().brokerPersistenceTimeEpochMs()>input.retryUntilEpochMs())throw new IllegalArgumentException("custody lost exact original Source input/stamp");
        body.exportedSample().mutation().requireAtOrBefore(appliedAt);
    }
    public TargetSloObservationEventBody body(){return TargetSloObservationEventBody.decode(input.canonicalBody());}
    public TargetSloCustodyReceipt receipt(){return body().custody();}
    public TargetSloObservationSample sample(){return body().exportedSample();}
    public byte[] key(){return key(receipt().sampleId(),receipt().sampleDigest());}
    public static byte[] key(byte[] sampleId,byte[] exportedSampleDigest){
        Bytes.requireLength(sampleId,32,"custody sample ID");Bytes.requireLength(exportedSampleDigest,32,"custody exact sample digest");
        return Bytes.concat(new byte[]{TargetKeyCodec.SLO_CUSTODY_TAG,TargetKeyCodec.KEY_FORMAT},sampleId,exportedSampleDigest);
    }
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,input.canonicalEnvelope());CanonicalProtobuf.bytes(out,3,appliedAt.canonicalBytes());
    });}
    public byte[] canonicalBytes(){return Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,4,Bytes.sha256(DOMAIN,fields()))));}
    public long storedBytes(){return Math.addExact((long)key().length,TargetValueEnvelope.encode(VALUE_TYPE,canonicalBytes()).length);}
    /** Complete key/NV upper bound for these exact bytes and the frozen fixed-width physical Source codec. */
    public static long maximumStoredBytes(SystemMutation input,SourcePosition sourceAnchor){
        final long stamp=11+delimited(sourceAnchor.canonicalBytes().length)+34;
        return 66+12+2+delimited(input.canonicalEnvelope().length)+delimited(stamp)+34;
    }
    private static long delimited(long bytes){long value=bytes;int width=1;while((value>>>=7)!=0)width++;return 1L+width+bytes;}
    public static TargetSloCustodyRecord decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,4,false,"Target Source custody record");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4},"Target Source custody record");
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown Target Source custody schema");
        final var value=new TargetSloCustodyRecord(SystemMutation.decodeTargetEnvelope(QueryCodecSupport.bytes(f.get(1),2),SystemMutationType.TARGET_SLO_OBSERVATION),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2),3)));
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target Source custody record");return value;
    }
    public static TargetSloCustodyRecord decodeForStore(byte[] key,byte[] encoded,TargetQuotaScope scope,byte[] lineage){
        final var value=decode(encoded);final var body=value.body();
        if(!Arrays.equals(key,value.key())||!scope.equals(body.scope())||!Arrays.equals(lineage,body.lineage()))
            throw new IllegalArgumentException("custody record changed exact key/scope/lineage");return value;
    }
}
