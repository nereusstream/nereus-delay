package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** One actual Source-adopted recovery object, charged to the original Attempt's frozen Target. */
public record TargetRecoveryEvidenceOwner(TargetRecoveryProofRef reference,TargetQuotaIncarnation primaryOwner,
        TargetQuotaMutation admissionMutation,byte[] admissionHash,byte[] observerDigest,TrustedUtcIntervalEvidence observedAt,
        SystemMutationType outcomeType,byte[] outcomeId,byte[] outcomeHash,TargetQuotaMutation adoptedAt) {
    public static final int VALUE_TYPE=49;
    public static final int MAX_CANONICAL_BYTES=TargetRecoveryProofRef.MAX_CANONICAL_BYTES
            +TargetQuotaIncarnation.MAX_CANONICAL_BYTES+2*TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+512;
    public TargetRecoveryEvidenceOwner {
        Objects.requireNonNull(reference);Objects.requireNonNull(primaryOwner);Objects.requireNonNull(observedAt);
        admissionHash=TargetCompatibilityCodec.assigned(admissionHash,32,"original Admission body hash");
        observerDigest=TargetCompatibilityCodec.assigned(observerDigest,32,"original observer identity digest");
        outcomeId=TargetCompatibilityCodec.assigned(outcomeId,32,"adopting Outcome ID");
        outcomeHash=TargetCompatibilityCodec.assigned(outcomeHash,32,"adopting Outcome body hash");
        Objects.requireNonNull(admissionMutation).requireSourceApplied();Objects.requireNonNull(adoptedAt).requireSourceApplied();
        if(primaryOwner.identity().kind()!=TargetQuotaIdentity.Kind.TARGET
                ||!primaryOwner.identity().shard().equals(adoptedAt.source().shardId())
                ||outcomeType!=SystemMutationType.PUBLISH_OUTCOME&&outcomeType!=SystemMutationType.EVIDENCE_RESOLUTION)
            throw new IllegalArgumentException("invalid recovery evidence owner provenance");
        primaryOwner.latestMutation().requireAtOrBefore(admissionMutation);adoptedAt.requireAfter(admissionMutation);
    }
    @Override public byte[] admissionHash(){return Bytes.copy(admissionHash);}
    @Override public byte[] observerDigest(){return Bytes.copy(observerDigest);}
    @Override public byte[] outcomeId(){return Bytes.copy(outcomeId);}
    @Override public byte[] outcomeHash(){return Bytes.copy(outcomeHash);}
    public byte[] key(){return key(reference.proofDigest());}
    public static byte[] key(byte[] digest){
        return Bytes.concat(new byte[]{TargetKeyCodec.RECOVERY_EVIDENCE_OWNER_TAG,TargetKeyCodec.KEY_FORMAT},
                TargetCompatibilityCodec.assigned(digest,32,"recovery proof digest"));
    }
    public CapacityVector blobCharge(){
        return primaryOwner.accounting().externalEvidenceCharge(Bytes.utf8(reference.exactKey()).length,reference.canonicalValueBytes());
    }
    public TargetRecoveryEvidenceReference referenceRecord(){
        return new TargetRecoveryEvidenceReference(reference.proofDigest(),reference.mappingId(),reference.evidenceId(),reference.attemptId(),
                primaryOwner.identity(),primaryOwner.accounting(),adoptedAt,outcomeId,outcomeHash,primaryOwner.recoveryLineage());
    }
    /** Full share transferred from this Attempt reserve to the independently charged three actual native rows/blob. */
    public CapacityVector adoptedCharge(){
        final var ref=referenceRecord();final var accounting=primaryOwner.accounting();
        return blobCharge().add(accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE,key().length,canonicalBytes().length))
                .add(accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE,ref.key().length,ref.canonicalBytes().length))
                .add(accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE,ref.attemptKey().length,ref.canonicalBytes().length));
    }
    /** Finite future Source positions/records, reserved before the adopting Source position exists. */
    public static CapacityVector maximumStateCharge(TargetQuotaAccounting accounting){
        return accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE,34,MAX_CANONICAL_BYTES)
                .add(accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE,67,TargetRecoveryEvidenceReference.MAX_CANONICAL_BYTES))
                .add(accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE,35,TargetRecoveryEvidenceReference.MAX_CANONICAL_BYTES));
    }
    public static CapacityVector maximumRecoveryCharge(TargetQuotaAccounting accounting){
        return accounting.externalEvidenceCharge(TargetRecoveryProofRef.MAX_KEY_BYTES,TargetRecoveryProofRef.MAX_PROOF_BYTES)
                .add(maximumStateCharge(accounting));
    }
    public void requireProof(TargetRecoveryProof proof){
        reference.requireMapping(proof.mapping(),proof.evidence());reference.requireObject(proof.digest(),proof.canonicalBytes());
        if(!Arrays.equals(primaryOwner.canonicalBytes(),proof.frozenOwner().canonicalBytes())
                ||!admissionMutation.equals(proof.mapping().mutation())||!Arrays.equals(admissionHash,proof.mapping().mutationHash())
                ||!Arrays.equals(observerDigest,Bytes.sha256(proof.observer().canonicalBytes()))
                ||!Arrays.equals(observedAt.canonicalBytes(),proof.observedAt().canonicalBytes()))
            throw new IllegalStateException("recovery owner changed original proof/observer/frozen origin");
    }
    public void requireSource(SystemMutation outcome,SourcePosition source){
        if(outcome.type()!=outcomeType||!Arrays.equals(outcome.systemMutationId(),outcomeId)||!Arrays.equals(outcome.mutationHash(),outcomeHash)
                ||!Arrays.equals(adoptedAt.mutationDigest(),Bytes.sha256(outcome.canonicalEnvelope()))
                ||!Arrays.equals(adoptedAt.source().canonicalBytes(),source.canonicalBytes()))
            throw new IllegalStateException("recovery owner changed exact adopting Source frame");
    }
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,reference.canonicalBytes());
        CanonicalProtobuf.bytes(out,3,primaryOwner.canonicalBytes());CanonicalProtobuf.bytes(out,4,admissionMutation.canonicalBytes());
        CanonicalProtobuf.bytes(out,5,admissionHash);CanonicalProtobuf.bytes(out,6,observerDigest);
        CanonicalProtobuf.bytes(out,7,observedAt.canonicalBytes());CanonicalProtobuf.uint32(out,8,outcomeType.wireValue());
        CanonicalProtobuf.bytes(out,9,outcomeId);CanonicalProtobuf.bytes(out,10,outcomeHash);CanonicalProtobuf.bytes(out,11,adoptedAt.canonicalBytes());
    });}
    public static TargetRecoveryEvidenceOwner decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,11,false,"recovery evidence owner");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9,10,11},"recovery evidence owner");
        if(QueryCodecSupport.uint(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown recovery owner version");
        final var result=new TargetRecoveryEvidenceOwner(TargetRecoveryProofRef.decode(QueryCodecSupport.nested(f.get(1),2)),
                TargetQuotaIncarnation.decode(QueryCodecSupport.nested(f.get(2),3)),TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(3),4)),
                QueryCodecSupport.fixed(f.get(4),5,32),QueryCodecSupport.fixed(f.get(5),6,32),TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(f.get(6),7)),
                SystemMutationType.fromWire(QueryCodecSupport.uint32(f.get(7),8)),QueryCodecSupport.fixed(f.get(8),9,32),QueryCodecSupport.fixed(f.get(9),10,32),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(10),11)));
        QueryCodecSupport.requireCanonical(encoded,result.canonicalBytes(),"recovery evidence owner");return result;
    }
    public static TargetRecoveryEvidenceOwner decodeForStore(byte[] key,byte[] encoded,ShardId shard,byte[] tenant,byte[] lineage){
        final var owner=decode(encoded);
        if(!Arrays.equals(key,owner.key())||!owner.primaryOwner.identity().shard().equals(shard)
                ||!Arrays.equals(owner.primaryOwner.tenantScope(),tenant)||!Arrays.equals(owner.primaryOwner.recoveryLineage(),lineage))
            throw new IllegalArgumentException("recovery owner key/Shard/tenant/lineage differs");return owner;
    }
}
