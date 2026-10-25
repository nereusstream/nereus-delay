package com.nereusstream.delay.protocol;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Objects;

/** Independent collector signature for the exact Target import, never a deletion or coverage certificate. */
public record TargetSloCustodyReceipt(TargetQuotaScope scope,byte[] lineage,long generation,byte[] collectorContractDigest,
        byte[] sampleId,byte[] bindingDigest,byte[] sampleDigest,byte[] outboxDigest,long observationRevision,
        byte[] exportDigest,AuthorIdentity collector,int keyVersion,TrustedUtcIntervalEvidence storedAt,long retainedUntil,
        byte[] signature) {
    public static final int MAX_CANONICAL_BYTES=TargetQuotaScope.MAX_CANONICAL_BYTES+TargetSloObservationPolicy.MAX_SERVICE_BYTES
            +TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES+9*34+4*11+128;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-custody-receipt\0");
    public TargetSloCustodyReceipt {
        Objects.requireNonNull(scope);Objects.requireNonNull(collector);Objects.requireNonNull(storedAt);
        lineage=TargetCompatibilityCodec.assigned(lineage,16,"custody lineage");
        collectorContractDigest=TargetCompatibilityCodec.assigned(collectorContractDigest,32,"original collector contract");
        sampleId=TargetCompatibilityCodec.assigned(sampleId,32,"custody sample ID");
        bindingDigest=TargetCompatibilityCodec.assigned(bindingDigest,32,"custody binding");
        sampleDigest=TargetCompatibilityCodec.assigned(sampleDigest,32,"exact imported Target sample");
        outboxDigest=TargetCompatibilityCodec.assigned(outboxDigest,32,"exact imported v1 content");
        exportDigest=TargetCompatibilityCodec.assigned(exportDigest,32,"complete retained Target export");
        signature=Bytes.copy(Objects.requireNonNull(signature));Bytes.requireLength(signature,64,"collector custody signature");
        TargetNativePolicySnapshot.requireBoundedTime(storedAt);
        if(scope.target()!=null||generation==0||keyVersion==0||collector.kind()!=AuthorIdentity.Kind.SERVICE
                ||collector.canonicalBytes().length>TargetSloObservationPolicy.MAX_SERVICE_BYTES||retainedUntil<storedAt.latestEpochMs())
            throw new IllegalArgumentException("collector custody changed assigned scope/author/retention");
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] collectorContractDigest(){return Bytes.copy(collectorContractDigest);}
    @Override public byte[] sampleId(){return Bytes.copy(sampleId);}
    @Override public byte[] bindingDigest(){return Bytes.copy(bindingDigest);}
    @Override public byte[] sampleDigest(){return Bytes.copy(sampleDigest);}
    @Override public byte[] outboxDigest(){return Bytes.copy(outboxDigest);}
    @Override public byte[] exportDigest(){return Bytes.copy(exportDigest);}
    @Override public byte[] signature(){return Bytes.copy(signature);}
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,scope.canonicalBytes());CanonicalProtobuf.bytes(out,3,lineage);
        CanonicalProtobuf.uint64Bits(out,4,generation);CanonicalProtobuf.bytes(out,5,collectorContractDigest);
        CanonicalProtobuf.bytes(out,6,sampleId);CanonicalProtobuf.bytes(out,7,bindingDigest);CanonicalProtobuf.bytes(out,8,sampleDigest);
        CanonicalProtobuf.bytes(out,9,outboxDigest);CanonicalProtobuf.uint64Bits(out,10,observationRevision);
        CanonicalProtobuf.bytes(out,11,exportDigest);CanonicalProtobuf.bytes(out,12,collector.canonicalBytes());
        CanonicalProtobuf.uint32Bits(out,13,keyVersion);CanonicalProtobuf.bytes(out,14,storedAt.canonicalBytes());
        CanonicalProtobuf.uint64Bits(out,15,retainedUntil);
    });}
    public byte[] receiptDigest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){return Bytes.concat(fields(),CanonicalProtobuf.message(out->{
        CanonicalProtobuf.bytes(out,16,receiptDigest());CanonicalProtobuf.bytes(out,17,signature);
    }));}
    public void requireSample(TargetSloObservationGeneration original,TargetSloObservationSample sample){
        final var finalObservation=sample.observation().finalObservation();
        if(!scope.equals(sample.scope())||!Arrays.equals(lineage,sample.lineage())||generation!=sample.generation()
                ||original.generation()!=generation||!Arrays.equals(original.policy().digest(),sample.policyDigest())
                ||!Arrays.equals(collectorContractDigest,original.policy().collectorContractDigest())
                ||!Arrays.equals(sampleId,sample.observation().sampleId())||!Arrays.equals(bindingDigest,sample.bindingDigest())
                ||!Arrays.equals(sampleDigest,sample.digest())||!Arrays.equals(outboxDigest,sample.observation().recordDigest())
                ||observationRevision!=(finalObservation==null?0:finalObservation.observationRevision())
                ||retainedUntil<Math.addExact(storedAt.latestEpochMs(),original.policy().limits().retentionMs()))
            throw new IllegalArgumentException("custody receipt changed exact original Target import or retention obligation");
    }
    public boolean verifySignature(PublicKey key){
        try{final var verifier=Signature.getInstance("Ed25519");verifier.initVerify(Objects.requireNonNull(key));verifier.update(receiptDigest());return verifier.verify(signature);}
        catch(GeneralSecurityException failure){throw new IllegalStateException("collector custody verification unavailable",failure);}
    }
    public static TargetSloCustodyReceipt signed(TargetSloObservationGeneration generation,TargetSloObservationSample sample,
            byte[] exportDigest,AuthorIdentity collector,int keyVersion,TrustedUtcIntervalEvidence storedAt,long retainedUntil,PrivateKey key){
        final var observation=sample.observation().finalObservation();
        final var unsigned=new TargetSloCustodyReceipt(sample.scope(),sample.lineage(),sample.generation(),generation.policy().collectorContractDigest(),
                sample.observation().sampleId(),sample.bindingDigest(),sample.digest(),sample.observation().recordDigest(),
                observation==null?0:observation.observationRevision(),exportDigest,collector,keyVersion,storedAt,retainedUntil,new byte[64]);
        unsigned.requireSample(generation,sample);
        try{final var signer=Signature.getInstance("Ed25519");signer.initSign(Objects.requireNonNull(key));signer.update(unsigned.receiptDigest());
            return new TargetSloCustodyReceipt(unsigned.scope,unsigned.lineage,unsigned.generation,unsigned.collectorContractDigest,unsigned.sampleId,
                    unsigned.bindingDigest,unsigned.sampleDigest,unsigned.outboxDigest,unsigned.observationRevision,unsigned.exportDigest,
                    collector,keyVersion,storedAt,retainedUntil,signer.sign());
        }catch(GeneralSecurityException failure){throw new IllegalStateException("collector custody signing unavailable",failure);}
    }
    public static TargetSloCustodyReceipt decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,17,false,"Target custody receipt");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17},"Target custody receipt");
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown Target custody receipt schema");
        final var value=new TargetSloCustodyReceipt(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1),2)),QueryCodecSupport.fixed(f.get(2),3,16),
                QueryCodecSupport.uint64Bits(f.get(3),4),QueryCodecSupport.fixed(f.get(4),5,32),QueryCodecSupport.fixed(f.get(5),6,32),
                QueryCodecSupport.fixed(f.get(6),7,32),QueryCodecSupport.fixed(f.get(7),8,32),QueryCodecSupport.fixed(f.get(8),9,32),
                QueryCodecSupport.uint64Bits(f.get(9),10),QueryCodecSupport.fixed(f.get(10),11,32),AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(11),12)),
                QueryCodecSupport.uint32Bits(f.get(12),13),TrustedUtcIntervalEvidence.decode(QueryCodecSupport.bytes(f.get(13),14)),
                QueryCodecSupport.uint64Bits(f.get(14),15),QueryCodecSupport.fixed(f.get(16),17,64));
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target custody receipt");return value;
    }
}
