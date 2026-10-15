package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Exact adopting Source reference and its separately charged Attempt alias; independent of CONTROL/NV48. */
public record TargetRecoveryEvidenceReference(byte[] proofDigest,byte[] mappingId,byte[] evidenceId,byte[] attemptId,
        TargetQuotaIdentity referenceOwner,TargetQuotaAccounting accounting,TargetQuotaMutation adoptedAt,
        byte[] outcomeId,byte[] outcomeHash,byte[] lineage) {
    public static final int VALUE_TYPE=50;
    public static final int MAX_CANONICAL_BYTES=TargetQuotaIdentity.MAX_CANONICAL_BYTES+TargetQuotaAccounting.MAX_CANONICAL_BYTES
            +TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+320;
    public TargetRecoveryEvidenceReference {
        proofDigest=TargetCompatibilityCodec.assigned(proofDigest,32,"recovery proof digest");
        mappingId=TargetCompatibilityCodec.assigned(mappingId,32,"recovery mapping ID");evidenceId=TargetCompatibilityCodec.assigned(evidenceId,32,"recovery evidence ID");
        attemptId=TargetCompatibilityCodec.assigned(attemptId,32,"recovery Attempt ID");outcomeId=TargetCompatibilityCodec.assigned(outcomeId,32,"adopting Outcome ID");
        outcomeHash=TargetCompatibilityCodec.assigned(outcomeHash,32,"adopting Outcome body hash");lineage=TargetCompatibilityCodec.assigned(lineage,16,"recovery lineage");
        Objects.requireNonNull(referenceOwner);Objects.requireNonNull(accounting);Objects.requireNonNull(adoptedAt).requireSourceApplied();
        if(referenceOwner.kind()!=TargetQuotaIdentity.Kind.TARGET||!referenceOwner.shard().equals(adoptedAt.source().shardId()))
            throw new IllegalArgumentException("recovery reference crosses frozen Target/Source");
    }
    @Override public byte[] proofDigest(){return Bytes.copy(proofDigest);}
    @Override public byte[] mappingId(){return Bytes.copy(mappingId);}
    @Override public byte[] evidenceId(){return Bytes.copy(evidenceId);}
    @Override public byte[] attemptId(){return Bytes.copy(attemptId);}
    @Override public byte[] outcomeId(){return Bytes.copy(outcomeId);}
    @Override public byte[] outcomeHash(){return Bytes.copy(outcomeHash);}
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    public byte[] key(){return key(mappingId,evidenceId);}
    public static byte[] key(byte[] mapping,byte[] evidence){
        return Bytes.concat(new byte[]{TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG,TargetKeyCodec.KEY_FORMAT,1},
                TargetCompatibilityCodec.assigned(mapping,32,"mapping ID"),TargetCompatibilityCodec.assigned(evidence,32,"evidence ID"));
    }
    public byte[] attemptKey(){return attemptKey(attemptId);}
    public static byte[] attemptKey(byte[] attempt){
        return Bytes.concat(new byte[]{TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG,TargetKeyCodec.KEY_FORMAT,2},
                TargetCompatibilityCodec.assigned(attempt,32,"Attempt ID"));
    }
    public void requireOwner(TargetRecoveryEvidenceOwner owner){
        if(!Arrays.equals(canonicalBytes(),owner.referenceRecord().canonicalBytes()))throw new IllegalStateException("recovery reference differs from its original owner/adopting Source");
    }
    public void requireAttempt(TargetQuotaAttemptBudget budget){
        if(!Arrays.equals(attemptId,budget.publishAttemptId())||!referenceOwner.equals(budget.primaryIdentity())
                ||!accounting.equals(budget.accounting())||!Arrays.equals(lineage,budget.recoveryLineage()))
            throw new IllegalStateException("recovery reference changed frozen Attempt accounting");
    }
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,proofDigest);CanonicalProtobuf.bytes(out,3,mappingId);CanonicalProtobuf.bytes(out,4,evidenceId);
        CanonicalProtobuf.bytes(out,5,attemptId);CanonicalProtobuf.bytes(out,6,referenceOwner.canonicalBytes());CanonicalProtobuf.bytes(out,7,accounting.canonicalBytes());
        CanonicalProtobuf.bytes(out,8,adoptedAt.canonicalBytes());CanonicalProtobuf.bytes(out,9,outcomeId);CanonicalProtobuf.bytes(out,10,outcomeHash);CanonicalProtobuf.bytes(out,11,lineage);
    });}
    public static TargetRecoveryEvidenceReference decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,11,false,"recovery evidence reference");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9,10,11},"recovery evidence reference");
        if(QueryCodecSupport.uint(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown recovery reference version");
        final var result=new TargetRecoveryEvidenceReference(QueryCodecSupport.fixed(f.get(1),2,32),QueryCodecSupport.fixed(f.get(2),3,32),
                QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32),TargetQuotaIdentity.decode(QueryCodecSupport.nested(f.get(5),6)),
                TargetQuotaAccounting.decode(QueryCodecSupport.nested(f.get(6),7)),TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(7),8)),
                QueryCodecSupport.fixed(f.get(8),9,32),QueryCodecSupport.fixed(f.get(9),10,32),QueryCodecSupport.fixed(f.get(10),11,16));
        QueryCodecSupport.requireCanonical(encoded,result.canonicalBytes(),"recovery evidence reference");return result;
    }
    public static TargetRecoveryEvidenceReference decodeForStore(byte[] key,byte[] encoded,ShardId shard,byte[] lineage){
        final var ref=decode(encoded);
        if(!Arrays.equals(key,ref.key())&&!Arrays.equals(key,ref.attemptKey())||!ref.referenceOwner.shard().equals(shard)||!Arrays.equals(ref.lineage,lineage))
            throw new IllegalArgumentException("recovery reference key/Shard/lineage differs");return ref;
    }
}
