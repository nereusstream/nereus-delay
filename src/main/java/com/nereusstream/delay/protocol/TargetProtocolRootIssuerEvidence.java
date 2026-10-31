package com.nereusstream.delay.protocol;

import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

/** Public original Root inputs; issuer trust is independently pinned at original Source, never granted by these bytes. */
public record TargetProtocolRootIssuerEvidence(TargetInitialControlSnapshot initial,byte[] issuerKey,TargetMigrationRoot migration,byte[] activationAttemptDigest) {
    public static final int MAX_BYTES=Math.max(TargetInitialControlSnapshot.MAX_CANONICAL_BYTES,TargetMigrationRoot.MAX_CANONICAL_BYTES)+112;
    public TargetProtocolRootIssuerEvidence(TargetInitialControlSnapshot initial,byte[] issuerKey){this(initial,issuerKey,null,null);}
    public TargetProtocolRootIssuerEvidence {issuerKey=Bytes.copy(issuerKey);key(issuerKey);
        if((initial==null)==(migration==null))throw new IllegalArgumentException("one exact original Root authority origin required");
        if(initial!=null){if(activationAttemptDigest!=null||!initial.input().verifySignature(key(issuerKey)))throw new IllegalArgumentException("original Root issuer evidence changed signed META10 input");}
        else{if(!migration.activated())throw new IllegalArgumentException("reader extension requires actual A");activationAttemptDigest=TargetCompatibilityCodec.assigned(activationAttemptDigest,32,"original durable ACTIVATE Prepared intent");}}
    public static TargetProtocolRootIssuerEvidence forMigration(TargetMigrationRoot root,TargetMigrationActivationAttempt attempt,PublicKey independentlyPinnedManagementIssuer){
        attempt.requirePlan(root.plan());if(!Arrays.equals(attempt.input().canonicalEnvelope(),root.activation().canonicalEnvelope()))throw new IllegalArgumentException("migration issuer evidence changed actual original Prepared/A");
        return new TargetProtocolRootIssuerEvidence(null,independentlyPinnedManagementIssuer.getEncoded(),root,attempt.digest());}
    public boolean migrated(){return migration!=null;}
    @Override public byte[] issuerKey(){return Bytes.copy(issuerKey);}@Override public byte[] activationAttemptDigest(){return activationAttemptDigest==null?null:Bytes.copy(activationAttemptDigest);}
    public PublicKey publicKey(){return key(issuerKey);}
    public void require(TargetProtocolActivatePayload payload){final var authorization=Objects.requireNonNull(payload.rootAuthorization());
        if(migrated())authorization.requireMigration(migration,payload.descriptor());else authorization.require(initial,payload.descriptor());
        if(migrated()&&migration.rootIssuerAuthorization()!=null&&!migration.rootIssuerAuthorization().verifySignature(publicKey()))throw new IllegalArgumentException("Root tuple key differs from the independently authorized signed A2 issuer statement");
        if(!authorization.verifySignature(publicKey()))throw new IllegalArgumentException("tuple extension was not signed by the original independently pinned Root issuer");}
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,migrated()?2:1);
        if(initial!=null)CanonicalProtobuf.bytes(out,2,initial.canonicalBytes());CanonicalProtobuf.bytes(out,3,issuerKey);
        if(migrated()){CanonicalProtobuf.bytes(out,4,migration.canonicalBytes());CanonicalProtobuf.bytes(out,5,activationAttemptDigest);}});}
    public static TargetProtocolRootIssuerEvidence decode(byte[] bytes){final var f=TargetCompatibilityCodec.read(bytes,MAX_BYTES,4,false,"Root tuple issuer public evidence");final boolean migrated=QueryCodecSupport.uint32(f.get(0),1)==2;
        QueryCodecSupport.requireNumbers(f,migrated?new int[]{1,3,4,5}:new int[]{1,2,3},"Root tuple issuer public evidence");if(!migrated&&QueryCodecSupport.uint32(f.get(0),1)!=1)throw new IllegalArgumentException("unknown Root issuer evidence schema");
        final var value=migrated?new TargetProtocolRootIssuerEvidence(null,QueryCodecSupport.fixed(f.get(1),3,44),TargetMigrationRoot.decode(QueryCodecSupport.bytes(f.get(2),4)),QueryCodecSupport.fixed(f.get(3),5,32)):
                new TargetProtocolRootIssuerEvidence(TargetInitialControlSnapshot.decode(QueryCodecSupport.bytes(f.get(1),2)),QueryCodecSupport.fixed(f.get(2),3,44));
        QueryCodecSupport.requireCanonical(bytes,value.canonicalBytes(),"Root tuple issuer public evidence");return value;}
    @Override public boolean equals(Object other){return other instanceof TargetProtocolRootIssuerEvidence value&&Arrays.equals(canonicalBytes(),value.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
    private static PublicKey key(byte[] bytes){if(bytes==null||bytes.length!=44)throw new IllegalArgumentException("exact original Ed25519 issuer key required");
        try{return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(bytes));}catch(GeneralSecurityException failure){throw new IllegalArgumentException("invalid original Root issuer key",failure);}}
}
