package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Actual Source decision's complete immutable public history; embedded keys never provide independent trust. */
public record TargetRetirementSourceHistoryRecord(SystemMutation input,TargetQuotaMutation decidedAt,
        TargetRetirementAuthorizationHistory.PublicProof proof){
    public static final int VALUE_TYPE=54;
    public static final int MAX_ENVELOPE_BYTES=Math.max(TargetEvidenceRetiringRecord.MAX_INTENT_BYTES,TargetEvidenceConfirmedRecord.MAX_CONFIRMATION_BYTES);
    public static final int MAX_BYTES=MAX_ENVELOPE_BYTES+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            +TargetRetirementAuthorizationHistory.PublicProof.MAX_BYTES+256;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-retirement-source-history-record\0");
    public TargetRetirementSourceHistoryRecord{
        Objects.requireNonNull(input);Objects.requireNonNull(decidedAt).requireSourceApplied();Objects.requireNonNull(proof);
        final var resource=resource(input);final var source=decidedAt.source();final var last=proof.chain().getLast();
        if(input.canonicalEnvelope().length>MAX_ENVELOPE_BYTES||!input.shardId().equals(source.shardId())
                ||!Arrays.equals(Bytes.sha256(input.canonicalEnvelope()),decidedAt.mutationDigest())
                ||!last.scope().shard().equals(resource.shard())||!Arrays.equals(last.scope().tenantScope(),resource.tenantScope())
                ||!Arrays.equals(last.lineage(),resource.recoveryLineage())||!Arrays.equals(last.deploymentRoot(),resource.deploymentRootDigest())
                ||!Arrays.equals(last.namespace(),resource.catalogNamespaceId())||!last.afterExclusive().sameSourceIdentity(source)
                ||source.compareTo(last.afterExclusive())<=0||source.compareTo(last.throughInclusive())>0)
            throw new IllegalArgumentException("retained Source history changed exact resource/Source/coverage");
    }
    public TargetExternalEvidenceObjectResource resource(){return resource(input);}
    public byte[] key(){return key(input.systemMutationId());}
    public static byte[] key(byte[] id){Bytes.requireLength(id,32,"retirement Source mutation ID");return Bytes.concat(new byte[]{TargetKeyCodec.RETIREMENT_SOURCE_HISTORY_TAG,TargetKeyCodec.KEY_FORMAT},id);}
    /** Local data consistency only. The original independent purpose pin is still required for authentication. */
    public Optional<TargetRetirementAuthorizationHistory.Grant> declaredGrant(){
        if(input.authorIdentityLength()>1024)return Optional.empty();
        final byte[] actor=input.authorIdentity();
        final var policy=proof.chain().getLast().intervals().stream().filter(i->decidedAt.source().compareTo(i.throughInclusive())<=0).findFirst().orElseThrow();
        return policy.grants().stream().filter(g->g.keyVersion()==input.signingKeyVersion()
                &&g.action()==TargetRetirementAuthorizationHistory.Action.of(input.type())&&Arrays.equals(g.service().canonicalBytes(),actor)).findFirst();
    }
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,input.canonicalEnvelope());
        CanonicalProtobuf.bytes(out,3,decidedAt.canonicalBytes());CanonicalProtobuf.bytes(out,4,proof.canonicalBytes());
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){
        final byte[] value=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,5,digest())));
        if(value.length>MAX_BYTES)throw new IllegalArgumentException("Source history record exceeds finite bound");return value;
    }
    public static TargetRetirementSourceHistoryRecord decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,5,false,"retirement Source history record");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5},"retirement Source history record");
        if(QueryCodecSupport.uint32(f.get(0),1)!=1)throw new IllegalArgumentException("unknown retirement Source history schema");
        final byte[] frame=QueryCodecSupport.bytes(f.get(1),2);if(frame.length>MAX_ENVELOPE_BYTES)throw new IllegalArgumentException("retirement history frame exceeds bound");
        final var value=new TargetRetirementSourceHistoryRecord(SystemMutation.decodeEnvelope(frame),TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2),3)),
                TargetRetirementAuthorizationHistory.PublicProof.decode(QueryCodecSupport.bytes(f.get(3),4)));
        if(!Arrays.equals(value.digest(),QueryCodecSupport.fixed(f.get(4),5,32)))throw new IllegalArgumentException("Source history checksum mismatch");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"retirement Source history record");return value;
    }
    public static TargetRetirementSourceHistoryRecord decodeForStore(byte[] key,byte[] payload,TargetQuotaScope scope,byte[] lineage){
        final var value=decode(payload);final var resource=value.resource();
        if(!Arrays.equals(key,value.key())||scope.target()!=null||!scope.shard().equals(resource.shard())
                ||!Arrays.equals(scope.tenantScope(),resource.tenantScope())||!Arrays.equals(lineage,resource.recoveryLineage()))
            throw new IllegalArgumentException("retirement Source history key/scope/lineage mismatch");return value;
    }
    private static TargetExternalEvidenceObjectResource resource(SystemMutation input){return switch(input.type()){
        case RESOURCE_RETIRE_INTENT->TargetEvidenceRetireIntentBody.decode(input.canonicalBody()).resource();
        case RESOURCE_DELETE_CONFIRMED->TargetEvidenceDeleteConfirmedBody.decode(input.canonicalBody()).resource();
        default->throw new IllegalArgumentException("not a body8 retirement action");};}
}
