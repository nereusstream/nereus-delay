package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Stable data identity of the original persistent capacity root; not proof of its ownership or deletion permission. */
public record TargetCatalogCapacityOwnerRef(byte[] catalogNamespaceId,String exactRootKey,
        byte[] storageNamespaceBinding,byte[] capacitySourceDigest) {
    public static final int MAX_BYTES=4096;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-catalog-capacity-owner\0");
    public TargetCatalogCapacityOwnerRef {
        catalogNamespaceId=TargetCompatibilityCodec.assigned(catalogNamespaceId,32,"original Catalog namespace");
        storageNamespaceBinding=TargetCompatibilityCodec.assigned(storageNamespaceBinding,32,"original storage namespace binding");
        capacitySourceDigest=TargetCompatibilityCodec.assigned(capacitySourceDigest,32,"original capacity source");
        Objects.requireNonNull(exactRootKey);
        if(exactRootKey.isBlank()||!exactRootKey.equals(exactRootKey.trim())||exactRootKey.indexOf('\0')>=0
                ||Bytes.utf8(exactRootKey).length>2048||!Normalizer.isNormalized(exactRootKey,Normalizer.Form.NFC)
                ||!exactRootKey.equals(new String(Bytes.utf8(exactRootKey),StandardCharsets.UTF_8)))
            throw new IllegalArgumentException("capacity owner requires its exact bounded persistent root key");
    }
    @Override public byte[] catalogNamespaceId(){return Bytes.copy(catalogNamespaceId);}
    @Override public byte[] storageNamespaceBinding(){return Bytes.copy(storageNamespaceBinding);}
    @Override public byte[] capacitySourceDigest(){return Bytes.copy(capacitySourceDigest);}
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,catalogNamespaceId);
        CanonicalProtobuf.bytes(out,3,Bytes.utf8(exactRootKey));CanonicalProtobuf.bytes(out,4,storageNamespaceBinding);
        CanonicalProtobuf.bytes(out,5,capacitySourceDigest);
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,canonicalBytes());}
    public static TargetCatalogCapacityOwnerRef decode(byte[] bytes){
        final var f=TargetCompatibilityCodec.read(bytes,MAX_BYTES,5,false,"Catalog capacity owner");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5},"Catalog capacity owner");
        if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown capacity owner schema");
        final var result=new TargetCatalogCapacityOwnerRef(QueryCodecSupport.fixed(f.get(1),2,32),
                new String(QueryCodecSupport.bytes(f.get(2),3),StandardCharsets.UTF_8),QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32));
        QueryCodecSupport.requireCanonical(bytes,result.canonicalBytes(),"Catalog capacity owner");return result;
    }
    @Override public boolean equals(Object other){return other instanceof TargetCatalogCapacityOwnerRef ref&&Arrays.equals(canonicalBytes(),ref.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
}
