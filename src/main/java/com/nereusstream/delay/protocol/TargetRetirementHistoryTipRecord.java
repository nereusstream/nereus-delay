package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** One monotone, charged accepted chain per exact Root/namespace/Shard/tenant/lineage/physical Source. */
public record TargetRetirementHistoryTipRecord(TargetRetirementAuthorizationHistory.PublicProof proof,byte[] mutationId,TargetQuotaMutation decidedAt){
    public static final int VALUE_TYPE=55;
    public static final int MAX_BYTES=TargetRetirementAuthorizationHistory.PublicProof.MAX_BYTES+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+256;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-retirement-accepted-history-tip\0");
    public TargetRetirementHistoryTipRecord{
        Objects.requireNonNull(proof);mutationId=TargetCompatibilityCodec.assigned(mutationId,32,"accepted history Source mutation");
        Objects.requireNonNull(decidedAt).requireSourceApplied();final var last=proof.chain().getLast();final var source=decidedAt.source();
        if(!last.scope().shard().equals(source.shardId())||!last.afterExclusive().sameSourceIdentity(source)
                ||source.compareTo(last.afterExclusive())<=0||source.compareTo(last.throughInclusive())>0)
            throw new IllegalArgumentException("accepted tip changed actual decision Source/coverage");
    }
    public TargetRetirementHistoryTipRecord(TargetRetirementSourceHistoryRecord decision){this(decision.proof(),decision.input().systemMutationId(),decision.decidedAt());}
    @Override public byte[] mutationId(){return Bytes.copy(mutationId);}
    public byte[] key(){return key(proof);}
    public static byte[] key(TargetRetirementAuthorizationHistory.PublicProof proof){
        final var snapshot=proof.chain().getLast();
        return Bytes.concat(new byte[]{TargetKeyCodec.RETIREMENT_HISTORY_TIP_TAG,TargetKeyCodec.KEY_FORMAT},Bytes.sha256(
                Bytes.utf8("nereus-delay-target-retirement-accepted-history-tip-key\0"),snapshot.deploymentRoot(),snapshot.namespace(),
                Bytes.lp32(snapshot.scope().canonicalBytes()),snapshot.lineage(),Bytes.lp32(sourceIdentity(snapshot.afterExclusive()))));
    }
    private static byte[] sourceIdentity(SourcePosition source){return switch(source){
        case KafkaSourcePosition value->new KafkaSourcePosition(value.shardId(),value.authenticatedClusterId(),value.nativeTopicUuid(),0,null,0).canonicalBytes();
        case PulsarSourcePosition value->new PulsarSourcePosition(value.shardId(),value.brokerResourceIncarnation(),value.physicalTopic(),0,0,0,1,PulsarSourcePosition.EntryKind.NON_BATCH,0).canonicalBytes();
    };}
    public void requireDecision(TargetRetirementSourceHistoryRecord original){
        if(!Arrays.equals(mutationId,original.input().systemMutationId())||!decidedAt.equals(original.decidedAt())
                ||!Arrays.equals(proof.canonicalBytes(),original.proof().canonicalBytes()))throw new IllegalStateException("accepted history tip lost exact actual Source decision");
    }
    public void requireCovers(TargetRetirementSourceHistoryRecord previous){
        if(!Arrays.equals(key(),key(previous.proof())))throw new IllegalStateException("accepted tip crossed history scope");
        previous.decidedAt().requireAtOrBefore(decidedAt);proof.requireContainsTip(previous.proof());
    }
    public void requireAfter(TargetRetirementHistoryTipRecord previous){
        if(!Arrays.equals(key(),previous.key()))throw new IllegalStateException("accepted tip changed exact history scope");
        decidedAt.requireAfter(previous.decidedAt);proof.requireContainsTip(previous.proof);
    }
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,proof.canonicalBytes());
        CanonicalProtobuf.bytes(out,3,mutationId);CanonicalProtobuf.bytes(out,4,decidedAt.canonicalBytes());
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){
        final byte[] encoded=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,5,digest())));
        if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("accepted history tip exceeds finite bound");return encoded;
    }
    public static TargetRetirementHistoryTipRecord decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,5,false,"accepted retirement history tip");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5},"accepted retirement history tip");
        if(QueryCodecSupport.uint32(f.get(0),1)!=1)throw new IllegalArgumentException("unknown accepted history tip schema");
        final var value=new TargetRetirementHistoryTipRecord(TargetRetirementAuthorizationHistory.PublicProof.decode(QueryCodecSupport.bytes(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(2),3,32),TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3),4)));
        if(!Arrays.equals(value.digest(),QueryCodecSupport.fixed(f.get(4),5,32)))throw new IllegalArgumentException("accepted history tip checksum mismatch");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"accepted retirement history tip");return value;
    }
    public static TargetRetirementHistoryTipRecord decodeForStore(byte[] key,byte[] encoded,TargetQuotaScope scope,byte[] lineage){
        final var value=decode(encoded);final var snapshot=value.proof().chain().getLast();
        if(!Arrays.equals(key,value.key())||!scope.equals(snapshot.scope())||!Arrays.equals(lineage,snapshot.lineage()))
            throw new IllegalArgumentException("accepted history tip key/scope/lineage mismatch");return value;
    }
}
