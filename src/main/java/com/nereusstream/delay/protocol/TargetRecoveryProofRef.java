package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Exact immutable recovery object selected before signing Source; never an evidenceId-to-current-object lookup. */
public record TargetRecoveryProofRef(String exactKey, byte[] proofDigest, byte[] valueDigest,
        long canonicalValueBytes, byte[] mappingId, byte[] evidenceId, byte[] attemptId) {
    public static final int VERSION = 1;
    public static final int TYPE = 1; // Dedicated RECOVERY_PROOF namespace, unrelated to CONTROL artifact kinds.
    public static final int MAX_KEY_BYTES = 2048;
    public static final int MAX_PROOF_BYTES = 4 * 1024 * 1024;
    public static final int MAX_CANONICAL_BYTES = MAX_KEY_BYTES + 256;

    public TargetRecoveryProofRef {
        Objects.requireNonNull(exactKey,"exact external recovery object key");
        if(exactKey.isBlank()||!exactKey.equals(exactKey.trim())||exactKey.indexOf('\0')>=0
                ||exactKey.getBytes(StandardCharsets.UTF_8).length>MAX_KEY_BYTES
                ||!Normalizer.isNormalized(exactKey,Normalizer.Form.NFC)
                ||!exactKey.equals(new String(exactKey.getBytes(StandardCharsets.UTF_8),StandardCharsets.UTF_8))
                ||canonicalValueBytes<1||canonicalValueBytes>MAX_PROOF_BYTES)
            throw new IllegalArgumentException("recovery proof reference requires finite canonical key/value bounds");
        proofDigest=TargetCompatibilityCodec.assigned(proofDigest,32,"recovery proof digest");
        valueDigest=TargetCompatibilityCodec.assigned(valueDigest,32,"signed recovery value digest");
        mappingId=TargetCompatibilityCodec.assigned(mappingId,32,"original mapping ID");
        evidenceId=TargetCompatibilityCodec.assigned(evidenceId,32,"original evidence ID");
        attemptId=TargetCompatibilityCodec.assigned(attemptId,32,"original attempt ID");
    }
    @Override public byte[] proofDigest(){return Bytes.copy(proofDigest);}
    @Override public byte[] valueDigest(){return Bytes.copy(valueDigest);}
    @Override public byte[] mappingId(){return Bytes.copy(mappingId);}
    @Override public byte[] evidenceId(){return Bytes.copy(evidenceId);}
    @Override public byte[] attemptId(){return Bytes.copy(attemptId);}
    public byte[] canonicalBytes(){
        return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,VERSION);CanonicalProtobuf.uint32(out,2,TYPE);
            CanonicalProtobuf.bytes(out,3,Bytes.utf8(exactKey));CanonicalProtobuf.bytes(out,4,proofDigest);
            CanonicalProtobuf.bytes(out,5,valueDigest);CanonicalProtobuf.uint64(out,6,canonicalValueBytes);
            CanonicalProtobuf.bytes(out,7,mappingId);CanonicalProtobuf.bytes(out,8,evidenceId);CanonicalProtobuf.bytes(out,9,attemptId);
        });
    }
    public static TargetRecoveryProofRef decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,9,false,"recovery proof reference");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9},"recovery proof reference");
        if(QueryCodecSupport.uint(f.get(0),1)!=VERSION||QueryCodecSupport.uint(f.get(1),2)!=TYPE)
            throw new IllegalArgumentException("unknown recovery proof reference type/version");
        final var ref=new TargetRecoveryProofRef(new String(QueryCodecSupport.bytes(f.get(2),3),StandardCharsets.UTF_8),
                QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32),QueryCodecSupport.uint(f.get(5),6),
                QueryCodecSupport.fixed(f.get(6),7,32),QueryCodecSupport.fixed(f.get(7),8,32),QueryCodecSupport.fixed(f.get(8),9,32));
        QueryCodecSupport.requireCanonical(encoded,ref.canonicalBytes(),"recovery proof reference");return ref;
    }
    public void requireEvidence(PublishEvidence evidence,byte[] expectedAttempt){
        if(!Arrays.equals(attemptId,expectedAttempt)||!Arrays.equals(evidenceId,evidence.evidenceId())
                ||evidence.evidenceKind()!=PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT
                    &&evidence.evidenceKind()!=PublishEvidenceKind.KAFKA_RECEIPT_ABSENCE)
            throw new IllegalArgumentException("recovery proof reference changes original K2 attempt/evidence");
        evidence.requireBusinessMutation(attemptId,evidence.verificationStatus()==EvidenceVerificationStatus.VERIFIED_PUBLISHED);
    }
    public void requireMapping(TargetKafkaReceiptMapping mapping,PublishEvidence evidence){
        requireEvidence(evidence,mapping.admission().publishAttemptId());
        if(!Arrays.equals(mappingId,mapping.mappingId()))throw new IllegalArgumentException("recovery proof refers to another original mapping");
        if(evidence.verificationStatus()==EvidenceVerificationStatus.VERIFIED_PUBLISHED)TargetKafkaReceiptWire.requirePublished(mapping,evidence);
        else TargetKafkaReceiptWire.requireNotPublished(mapping,evidence);
    }
    public void requireObject(byte[] originalProofDigest,byte[] signedCanonicalValue){
        if(!Arrays.equals(proofDigest,originalProofDigest)||canonicalValueBytes!=signedCanonicalValue.length
                ||!Arrays.equals(valueDigest,Bytes.sha256(signedCanonicalValue)))
            throw new IllegalArgumentException("recovery object differs from signed Source reference");
    }
    @Override public boolean equals(Object other){return other instanceof TargetRecoveryProofRef ref&&Arrays.equals(canonicalBytes(),ref.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
}
