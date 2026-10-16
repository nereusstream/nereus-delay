package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Explicit Target branch at existing ControlPayload field1; its varint discriminator rejects legacy decoders. */
public record TargetProtocolActivatePayload(TargetQuotaScope scope,byte[] lineage,
        TargetProtocolActivationDescriptor descriptor,byte[] readerEvidenceHash) implements ControlOperationRequestBranch {
    public static final int CONTROL_KIND=1;
    public static final int MAX_BYTES=TargetProtocolActivationDescriptor.MAX_BYTES+TargetQuotaScope.MAX_CANONICAL_BYTES+256;
    public TargetProtocolActivatePayload {
        Objects.requireNonNull(scope);Objects.requireNonNull(descriptor);
        lineage=TargetCompatibilityCodec.assigned(lineage,16,"Target activation lineage");
        readerEvidenceHash=TargetCompatibilityCodec.assigned(readerEvidenceHash,32,"exact Target reader evidence");
        if(scope.target()!=null)throw new IllegalArgumentException("Target protocol activation requires Source Shard scope");
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] readerEvidenceHash(){return Bytes.copy(readerEvidenceHash);}
    public ControlOperationRequest operationRequest(){return new ControlOperationRequest(ControlOperationKind.ACTIVATE_TARGET_PROTOCOL,this);}
    @Override public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,scope.canonicalBytes());CanonicalProtobuf.bytes(out,3,lineage);
        CanonicalProtobuf.bytes(out,4,descriptor.canonicalBytes());CanonicalProtobuf.bytes(out,5,readerEvidenceHash);
    });}
    public static TargetProtocolActivatePayload decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,5,false,"Target protocol activation payload");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5},"Target protocol activation payload");
        if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unsupported Target activation payload version");
        final var payload=new TargetProtocolActivatePayload(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(2),3,16),TargetProtocolActivationDescriptor.decode(QueryCodecSupport.bytes(f.get(3),4)),
                QueryCodecSupport.fixed(f.get(4),5,32));
        QueryCodecSupport.requireCanonical(encoded,payload.canonicalBytes(),"Target protocol activation payload");return payload;
    }
    @Override public boolean equals(Object other){return other instanceof TargetProtocolActivatePayload that&&Arrays.equals(canonicalBytes(),that.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
}
