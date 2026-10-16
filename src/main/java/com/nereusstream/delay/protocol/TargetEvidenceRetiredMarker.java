package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Persistent value at the original key. It blocks old conditional puts; it does not prove their completion or permit key GC. */
public record TargetEvidenceRetiredMarker(TargetCatalogCapacityOwnerRef capacityOwner,byte[] deploymentRootDigest,
        byte[] publicationId,byte[] publicationOriginDigest,String exactKey,byte[] resourceIdentityHash,
        byte[] objectDigest,byte[] payloadDigest,long payloadBytes,byte[] payloadVersion,long closureRevision,
        byte[] closureDigest,byte[] intentId,byte[] intentHash,TargetQuotaMutation intentAppliedAt){
    public static final int MAX_BYTES=TargetCatalogCapacityOwnerRef.MAX_BYTES+TargetRecoveryProofRef.MAX_KEY_BYTES
            +TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+1024;
    public TargetEvidenceRetiredMarker{
        Objects.requireNonNull(capacityOwner);deploymentRootDigest=hash(deploymentRootDigest);publicationId=hash(publicationId);
        publicationOriginDigest=hash(publicationOriginDigest);resourceIdentityHash=hash(resourceIdentityHash);
        objectDigest=hash(objectDigest);payloadDigest=hash(payloadDigest);closureDigest=hash(closureDigest);
        intentId=hash(intentId);intentHash=hash(intentHash);payloadVersion=Bytes.copy(Objects.requireNonNull(payloadVersion));
        Objects.requireNonNull(intentAppliedAt).requireSourceApplied();
        if(Objects.requireNonNull(exactKey).isBlank()||exactKey.indexOf('\0')>=0||!Normalizer.isNormalized(exactKey,Normalizer.Form.NFC)
                ||Bytes.utf8(exactKey).length>TargetRecoveryProofRef.MAX_KEY_BYTES||payloadBytes<1
                ||payloadBytes>TargetRecoveryProofRef.MAX_PROOF_BYTES||payloadVersion.length!=8||closureRevision==0)
            throw new IllegalArgumentException("invalid bounded retired-payload marker");
    }
    @Override public byte[] deploymentRootDigest(){return Bytes.copy(deploymentRootDigest);}
    @Override public byte[] publicationId(){return Bytes.copy(publicationId);}
    @Override public byte[] publicationOriginDigest(){return Bytes.copy(publicationOriginDigest);}
    @Override public byte[] resourceIdentityHash(){return Bytes.copy(resourceIdentityHash);}
    @Override public byte[] objectDigest(){return Bytes.copy(objectDigest);}
    @Override public byte[] payloadDigest(){return Bytes.copy(payloadDigest);}
    @Override public byte[] payloadVersion(){return Bytes.copy(payloadVersion);}
    @Override public byte[] closureDigest(){return Bytes.copy(closureDigest);}
    @Override public byte[] intentId(){return Bytes.copy(intentId);}
    @Override public byte[] intentHash(){return Bytes.copy(intentHash);}
    public void requireBindings(TargetExternalEvidenceObjectResource resource,TargetEvidenceRetirementContext context,
            ResourceDeleteConfirmedBody.RetireIntentRef intent){
        context.requireResource(resource);intentAppliedAt.requireAfter(context.preparedCut());
        if(!Arrays.equals(capacityOwner.catalogNamespaceId(),resource.catalogNamespaceId())
                ||!Arrays.equals(deploymentRootDigest,resource.deploymentRootDigest())||!exactKey.equals(resource.exactKey())
                ||!Arrays.equals(resourceIdentityHash,resource.exactIdentity().identityHash())||!Arrays.equals(resourceIdentityHash,intent.resourceIdentityHash())
                ||!Arrays.equals(objectDigest,resource.objectDigest())||!Arrays.equals(payloadDigest,resource.valueDigest())
                ||payloadBytes!=resource.canonicalValueBytes()||!Arrays.equals(payloadVersion,resource.immutableVersion())
                ||closureRevision!=context.closureRevision()||!Arrays.equals(closureDigest,context.closureDigest())
                ||!Arrays.equals(intentId,intent.mutationId())||!Arrays.equals(intentHash,intent.mutationHash())
                ||intent.expectedResourceStateVersion()!=0||!intentAppliedAt.source().shardId().equals(resource.shard()))
            throw new IllegalArgumentException("retired marker changed original payload/Root/closure/Source intent");
        if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging original
                &&(!Arrays.equals(capacityOwner.canonicalBytes(),original.capacityOwner().canonicalBytes())
                    ||!Arrays.equals(publicationId,original.publicationIntentId())||!Arrays.equals(publicationOriginDigest,original.publicationOriginDigest())))
            throw new IllegalArgumentException("retired marker changed original staging reservation");
    }
    public byte[] canonicalBytes(){
        final byte[] encoded=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,capacityOwner.canonicalBytes());
            CanonicalProtobuf.bytes(out,3,deploymentRootDigest);CanonicalProtobuf.bytes(out,4,publicationId);
            CanonicalProtobuf.bytes(out,5,publicationOriginDigest);CanonicalProtobuf.bytes(out,6,Bytes.utf8(exactKey));
            CanonicalProtobuf.bytes(out,7,resourceIdentityHash);CanonicalProtobuf.bytes(out,8,objectDigest);
            CanonicalProtobuf.bytes(out,9,payloadDigest);CanonicalProtobuf.uint64(out,10,payloadBytes);
            CanonicalProtobuf.bytes(out,11,payloadVersion);CanonicalProtobuf.uint64Bits(out,12,closureRevision);
            CanonicalProtobuf.bytes(out,13,closureDigest);CanonicalProtobuf.bytes(out,14,intentId);
            CanonicalProtobuf.bytes(out,15,intentHash);CanonicalProtobuf.bytes(out,16,intentAppliedAt.canonicalBytes());
        });
        if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("retired marker exceeds finite bound");return encoded;
    }
    public static TargetEvidenceRetiredMarker decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,16,false,"retired payload marker");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16},"retired payload marker");
        if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown retired marker schema");
        final var marker=new TargetEvidenceRetiredMarker(TargetCatalogCapacityOwnerRef.decode(QueryCodecSupport.nested(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(2),3,32),QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32),
                new String(QueryCodecSupport.bytes(f.get(5),6),StandardCharsets.UTF_8),QueryCodecSupport.fixed(f.get(6),7,32),
                QueryCodecSupport.fixed(f.get(7),8,32),QueryCodecSupport.fixed(f.get(8),9,32),QueryCodecSupport.uint(f.get(9),10),
                QueryCodecSupport.fixed(f.get(10),11,8),QueryCodecSupport.uint64Bits(f.get(11),12),QueryCodecSupport.fixed(f.get(12),13,32),
                QueryCodecSupport.fixed(f.get(13),14,32),QueryCodecSupport.fixed(f.get(14),15,32),TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(15),16)));
        QueryCodecSupport.requireCanonical(encoded,marker.canonicalBytes(),"retired payload marker");return marker;
    }
    private static byte[] hash(byte[] value){return TargetCompatibilityCodec.assigned(value,32,"retired marker identity");}
}
