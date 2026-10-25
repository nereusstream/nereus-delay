package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/** Immutable original Source observation and actual conservative merge edge; not collector custody. */
public record TargetSloObservationEventRecord(SystemMutation input,TargetQuotaMutation appliedAt,byte[] beforeDigest,SloObservationOutbox after) {
    public static final int VALUE_TYPE=60;
    public static final int MAX_INPUT_BYTES=TargetSloObservationEventBody.MAX_CANONICAL_BYTES+TargetSloObservationPolicy.MAX_SERVICE_BYTES+512;
    public static final int MAX_CANONICAL_BYTES=MAX_INPUT_BYTES+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            +TargetSloObservationSample.MAX_START_BYTES+TargetSloObservationSample.MAX_FINAL_BYTES+192;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-observation-source-record\0");
    public TargetSloObservationEventRecord {
        Objects.requireNonNull(input);Objects.requireNonNull(appliedAt).requireSourceApplied();Objects.requireNonNull(after);
        beforeDigest=TargetCompatibilityCodec.assigned(beforeDigest,32,"actual original outbox digest");
        final var body=TargetSloObservationEventBody.decode(input.canonicalBody());
        if(input.type()!=SystemMutationType.TARGET_SLO_OBSERVATION||!input.shardId().equals(body.scope().shard())||!input.shardId().equals(appliedAt.source().shardId())
                ||input.retryUntilEpochMs()!=body.retryUntil()||!Arrays.equals(input.logicalOperationIdentity(),body.logicalIdentity())
                ||input.canonicalEnvelope().length>MAX_INPUT_BYTES||!Arrays.equals(appliedAt.mutationDigest(),Bytes.sha256(input.canonicalEnvelope()))
                ||!Arrays.equals(body.sampleId(),after.sampleId())||after.finalObservation()==null||appliedAt.source().brokerPersistenceTimeEpochMs()>input.retryUntilEpochMs())
            throw new IllegalArgumentException("SLO event record lost exact applied Source input");
        body.finalObservation().validateAgainst(after.start());
    }
    @Override public byte[] beforeDigest(){return Bytes.copy(beforeDigest);}
    public TargetSloObservationEventBody body(){return TargetSloObservationEventBody.decode(input.canonicalBody());}
    public byte[] key(){return key(body().sampleId(),appliedAt.sequence());}
    public static byte[] key(byte[] sampleId,long sequence){
        Bytes.requireLength(sampleId,32,"original SLO sample ID");if(sequence==0)throw new IllegalArgumentException("SLO event requires actual Source sequence");
        return Bytes.concat(new byte[]{TargetKeyCodec.SLO_EVENT_TAG,TargetKeyCodec.KEY_FORMAT},sampleId,Bytes.u64beBits(sequence));
    }
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,input.canonicalEnvelope());CanonicalProtobuf.bytes(out,3,appliedAt.canonicalBytes());
        CanonicalProtobuf.bytes(out,4,beforeDigest);CanonicalProtobuf.bytes(out,5,after.canonicalBytes());
    });}
    public byte[] canonicalBytes(){
        final var bytes=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,6,Bytes.sha256(DOMAIN,fields()))));
        if(bytes.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("SLO event history exceeds finite bound");return bytes;
    }
    public long storedBytes(){return Math.addExact((long)key().length,TargetValueEnvelope.encode(VALUE_TYPE,canonicalBytes()).length);}
    public static TargetSloObservationEventRecord decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,6,false,"Target original SLO Source event");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6},"Target original SLO Source event");
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown SLO event history schema");
        final var input=QueryCodecSupport.bytes(f.get(1),2);if(input.length>MAX_INPUT_BYTES)throw new IllegalArgumentException("SLO event input exceeds bound");
        final var value=new TargetSloObservationEventRecord(SystemMutation.decodeTargetEnvelope(input),TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2),3)),
                QueryCodecSupport.fixed(f.get(3),4,32),SloObservationOutbox.decode(QueryCodecSupport.bytes(f.get(4),5)));
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target original SLO Source event");return value;
    }
    public static TargetSloObservationEventRecord decodeForStore(byte[] key,byte[] encoded,TargetQuotaScope scope,byte[] lineage){
        final var value=decode(encoded);
        if(!Arrays.equals(key,value.key())||!scope.equals(value.body().scope())||!Arrays.equals(lineage,value.body().lineage()))throw new IllegalArgumentException("SLO history changed key/scope/lineage");return value;
    }
}
