package com.nereusstream.delay.protocol;

import java.security.*;
import java.util.*;

/** Pre-A independent management authority permits one new Root to request only these reader extensions. */
public record TargetMigrationRootIssuerAuthorization(byte[] deploymentAuthority,byte[] managementAuthorityDigest,
        byte[] migrationId,byte[] planDigest,TargetQuotaScope scope,SourcePosition baselineSource,
        byte[] newStore,byte[] newDbIdentity,byte[] newLineage,byte[] issuerId,int issuerKeyVersion,
        List<ProtocolTuple> allowedTuples,byte[] originalAuthorityHistory,byte[] signature) {
    public static final int MAX_HISTORY_BYTES=1<<20;
    public static final int MAX_BYTES=MAX_HISTORY_BYTES+TargetSourcePosition.MAX_CANONICAL_BYTES+TargetQuotaScope.MAX_CANONICAL_BYTES+1024;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-migration-root-issuer-authorization-v1\0");
    private static final byte[] SIGN=Bytes.utf8("nereus-delay-migration-root-issuer-authorization-signature-v1\0");
    public TargetMigrationRootIssuerAuthorization {
        deploymentAuthority=fixed(deploymentAuthority,32,"original deployment authority");managementAuthorityDigest=fixed(managementAuthorityDigest,32,"original management authority anchor");
        migrationId=fixed(migrationId,32,"migration ID");planDigest=fixed(planDigest,32,"exact pre-A plan");Objects.requireNonNull(scope);baselineSource=TargetSourcePosition.requireBounded(baselineSource);
        newStore=fixed(newStore,16,"new Store");newDbIdentity=fixed(newDbIdentity,32,"new DB");newLineage=fixed(newLineage,16,"new lineage");issuerId=fixed(issuerId,32,"original root issuer identity");
        if(scope.target()!=null||!scope.shard().equals(baselineSource.shardId())||issuerKeyVersion==0)throw new IllegalArgumentException("root issuer statement changed exact Source/Shard/key scope");
        final var sorted=new TreeMap<String,ProtocolTuple>();for(var tuple:allowedTuples){if(!allowed(tuple)||sorted.put(Bytes.hex(tuple.canonicalBytes()),tuple)!=null)throw new IllegalArgumentException("unregistered/duplicate Root extension purpose");}
        if(sorted.isEmpty()||sorted.size()>9)throw new IllegalArgumentException("Root issuer permits a finite explicit reader extension subset");allowedTuples=List.copyOf(sorted.values());
        originalAuthorityHistory=Bytes.copy(Objects.requireNonNull(originalAuthorityHistory));if(originalAuthorityHistory.length==0||originalAuthorityHistory.length>MAX_HISTORY_BYTES)throw new IllegalArgumentException("complete bounded original authority/succession history required");
        if(signature!=null){Bytes.requireLength(signature,64,"independent management issuer signature");signature=Bytes.copy(signature);}
    }
    private static byte[] fixed(byte[] value,int bytes,String name){return TargetCompatibilityCodec.assigned(value,bytes,name);}
    private static boolean allowed(ProtocolTuple tuple){return tuple.equals(ProtocolTuple.targetObjectPayloadReclaim())||tuple.equals(ProtocolTuple.targetUncommittedObjectReclaim())
        ||tuple.equals(ProtocolTuple.targetMigrationObjectPayloadReclaim())||tuple.equals(ProtocolTuple.targetMigrationUncommittedObjectReclaim())||tuple.equals(ProtocolTuple.targetMigrationRetireIntent())
        ||tuple.equals(ProtocolTuple.targetMigrationRetireConfirm())||tuple.equals(ProtocolTuple.targetPayloadObligationSummary())||tuple.equals(ProtocolTuple.targetMigrationLogicalFirstArchive())||tuple.equals(ProtocolTuple.targetMigrationFullSourceArchive());}
    @Override public byte[] deploymentAuthority(){return Bytes.copy(deploymentAuthority);}@Override public byte[] managementAuthorityDigest(){return Bytes.copy(managementAuthorityDigest);}
    @Override public byte[] migrationId(){return Bytes.copy(migrationId);}@Override public byte[] planDigest(){return Bytes.copy(planDigest);}@Override public byte[] newStore(){return Bytes.copy(newStore);}
    @Override public byte[] newDbIdentity(){return Bytes.copy(newDbIdentity);}@Override public byte[] newLineage(){return Bytes.copy(newLineage);}@Override public byte[] issuerId(){return Bytes.copy(issuerId);}
    @Override public byte[] originalAuthorityHistory(){return Bytes.copy(originalAuthorityHistory);}@Override public byte[] signature(){return signature==null?null:Bytes.copy(signature);}
    public static TargetMigrationRootIssuerAuthorization signed(TargetMigrationPlan plan,byte[] deploymentAuthority,byte[] issuerId,int issuerVersion,List<ProtocolTuple> tuples,byte[] completeAuthorityHistory,PrivateKey managementIssuer){
        final var draft=new TargetMigrationRootIssuerAuthorization(deploymentAuthority,plan.managementAuthorityDigest(),plan.migrationId(),plan.digest(),plan.scope(),plan.baseline().source(),plan.newStore(),plan.newDbIdentity(),plan.newLineage(),issuerId,issuerVersion,tuples,completeAuthorityHistory,null);
        try{final var signer=Signature.getInstance("Ed25519");signer.initSign(managementIssuer);signer.update(Bytes.concat(SIGN,draft.digest()));
            return new TargetMigrationRootIssuerAuthorization(draft.deploymentAuthority,draft.managementAuthorityDigest,draft.migrationId,draft.planDigest,draft.scope,draft.baselineSource,draft.newStore,draft.newDbIdentity,draft.newLineage,draft.issuerId,draft.issuerKeyVersion,draft.allowedTuples,draft.originalAuthorityHistory,signer.sign());
        }catch(GeneralSecurityException failure){throw new IllegalArgumentException("cannot sign original migration Root issuer policy",failure);}}
    public void requirePlan(TargetMigrationPlan plan){if(!Arrays.equals(managementAuthorityDigest,plan.managementAuthorityDigest())||!Arrays.equals(migrationId,plan.migrationId())||!Arrays.equals(planDigest,plan.digest())||!scope.equals(plan.scope())
            ||!Arrays.equals(baselineSource.canonicalBytes(),plan.baseline().source().canonicalBytes())||!Arrays.equals(newStore,plan.newStore())||!Arrays.equals(newDbIdentity,plan.newDbIdentity())||!Arrays.equals(newLineage,plan.newLineage()))throw new IllegalArgumentException("Root issuer policy changed exact pre-A plan/Source/new Root identity");}
    public void requireExtension(TargetProtocolTupleAuthorization extension){if(!extension.migrated()||!scope.equals(extension.scope())||!Arrays.equals(newLineage,extension.lineage())||!Arrays.equals(planDigest,extension.migrationPlanDigest())
            ||!Arrays.equals(managementAuthorityDigest,extension.migrationManagementDigest())||issuerKeyVersion!=extension.issuerKeyVersion()||!allowedTuples.contains(extension.tuple()))throw new IllegalArgumentException("reader tuple extension exceeds original signed Root issuer policy");}
    public boolean verifySignature(PublicKey independentlyPinnedIssuer){if(signature==null)return false;try{final var verifier=Signature.getInstance("Ed25519");verifier.initVerify(independentlyPinnedIssuer);verifier.update(Bytes.concat(SIGN,digest()));return verifier.verify(signature);}catch(GeneralSecurityException failure){throw new IllegalArgumentException("cannot verify original Root issuer policy",failure);}}
    private byte[] fields(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,deploymentAuthority);CanonicalProtobuf.bytes(out,3,managementAuthorityDigest);CanonicalProtobuf.bytes(out,4,migrationId);CanonicalProtobuf.bytes(out,5,planDigest);
        CanonicalProtobuf.bytes(out,6,scope.canonicalBytes());CanonicalProtobuf.bytes(out,7,baselineSource.canonicalBytes());CanonicalProtobuf.bytes(out,8,newStore);CanonicalProtobuf.bytes(out,9,newDbIdentity);CanonicalProtobuf.bytes(out,10,newLineage);
        CanonicalProtobuf.bytes(out,11,issuerId);CanonicalProtobuf.uint32Bits(out,12,issuerKeyVersion);CanonicalProtobuf.uint32(out,13,1);for(var tuple:allowedTuples)CanonicalProtobuf.bytes(out,14,tuple.canonicalBytes());CanonicalProtobuf.bytes(out,15,originalAuthorityHistory);});}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){if(signature==null)throw new IllegalStateException("migration Root issuer policy is unsigned");final byte[] bytes=Bytes.concat(fields(),CanonicalProtobuf.message(out->{CanonicalProtobuf.bytes(out,16,digest());CanonicalProtobuf.bytes(out,17,signature);}));if(bytes.length>MAX_BYTES)throw new IllegalArgumentException("Root issuer policy exceeds existing bounded history/source fields");return bytes;}
    public static TargetMigrationRootIssuerAuthorization decode(byte[] bytes){final var f=TargetCompatibilityCodec.read(bytes,MAX_BYTES,25,true,"migration Root issuer policy");
        if(f.size()<17||QueryCodecSupport.uint32(f.get(0),1)!=1||QueryCodecSupport.uint32(f.get(12),13)!=1)throw new IllegalArgumentException("unknown migration Root issuer policy schema/purpose");
        final var tuples=new ArrayList<ProtocolTuple>();int index=13;while(index<f.size()&&f.get(index).number()==14)tuples.add(ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(index++),14)));
        if(index+3!=f.size())throw new IllegalArgumentException("incomplete Root issuer policy closure");
        final var value=new TargetMigrationRootIssuerAuthorization(QueryCodecSupport.fixed(f.get(1),2,32),QueryCodecSupport.fixed(f.get(2),3,32),QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32),TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(5),6)),TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(6),7)),
            QueryCodecSupport.fixed(f.get(7),8,16),QueryCodecSupport.fixed(f.get(8),9,32),QueryCodecSupport.fixed(f.get(9),10,16),QueryCodecSupport.fixed(f.get(10),11,32),QueryCodecSupport.uint32Bits(f.get(11),12),tuples,QueryCodecSupport.bytes(f.get(index),15),QueryCodecSupport.fixed(f.get(index+2),17,64));
        if(!Arrays.equals(value.digest(),QueryCodecSupport.fixed(f.get(index+1),16,32)))throw new IllegalArgumentException("Root issuer policy digest changed");QueryCodecSupport.requireCanonical(bytes,value.canonicalBytes(),"migration Root issuer policy");return value;}
    @Override public boolean equals(Object other){return other instanceof TargetMigrationRootIssuerAuthorization value&&Arrays.equals(canonicalBytes(),value.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(digest());}

}
