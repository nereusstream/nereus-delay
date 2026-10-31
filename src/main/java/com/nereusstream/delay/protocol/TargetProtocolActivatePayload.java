package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Explicit Target branch at existing ControlPayload field1; its varint discriminator rejects legacy decoders. */
public record TargetProtocolActivatePayload(TargetQuotaScope scope,byte[] lineage,
        TargetProtocolActivationDescriptor descriptor,byte[] readerEvidenceHash,TargetProtocolTupleAuthorization rootAuthorization) implements ControlOperationRequestBranch {
    public TargetProtocolActivatePayload(TargetQuotaScope scope,byte[] lineage,TargetProtocolActivationDescriptor descriptor,byte[] readers){this(scope,lineage,descriptor,readers,null);}
    public static final int CONTROL_KIND=1;
    public static final int V1_MAX_BYTES=TargetProtocolActivationDescriptor.MAX_BYTES+TargetQuotaScope.MAX_CANONICAL_BYTES+256;
    public static final int MAX_BYTES=TargetProtocolActivationDescriptor.MAX_BYTES+TargetQuotaScope.MAX_CANONICAL_BYTES+TargetProtocolTupleAuthorization.MAX_BYTES+264;
    public TargetProtocolActivatePayload {
        Objects.requireNonNull(scope);Objects.requireNonNull(descriptor);
        lineage=TargetCompatibilityCodec.assigned(lineage,16,"Target activation lineage");
        readerEvidenceHash=TargetCompatibilityCodec.assigned(readerEvidenceHash,32,"exact Target reader evidence");
        if(rootAuthorization==null?descriptor.extendedRootAuthorization():!descriptor.extendedRootAuthorization()&&!(rootAuthorization.migrated()&&(descriptor.schemaVersion()==10||descriptor.schemaVersion()==11)))throw new IllegalArgumentException("new reader descriptor requires the original Root issuer authorization; old payload stays unchanged");
        if(rootAuthorization!=null&&(!scope.equals(rootAuthorization.scope())||!Arrays.equals(lineage,rootAuthorization.lineage())||!Arrays.equals(descriptor.digest(),rootAuthorization.descriptorDigest())||!descriptor.tuple().equals(rootAuthorization.tuple())))throw new IllegalArgumentException("Root tuple authorization changed exact payload scope/lineage/descriptor");
        if(scope.target()!=null)throw new IllegalArgumentException("Target protocol activation requires Source Shard scope");
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] readerEvidenceHash(){return Bytes.copy(readerEvidenceHash);}
    public static TargetProtocolActivatePayload forRoot(TargetInitialControlSnapshot initial,TargetProtocolActivationDescriptor descriptor,byte[] readerEvidenceHash,java.security.PrivateKey originalIssuer){
        return new TargetProtocolActivatePayload(initial.input().scope(),initial.input().lineage(),descriptor,readerEvidenceHash,TargetProtocolTupleAuthorization.signed(initial,descriptor,originalIssuer));
    }
    public static TargetProtocolActivatePayload forMigrationRoot(TargetMigrationRoot root,TargetProtocolActivationDescriptor descriptor,byte[] readerEvidenceHash,int originalManagementIssuerVersion,java.security.PrivateKey originalManagementIssuer){
        return new TargetProtocolActivatePayload(root.plan().scope(),root.plan().newLineage(),descriptor,readerEvidenceHash,TargetProtocolTupleAuthorization.signedForMigration(root,descriptor,originalManagementIssuerVersion,originalManagementIssuer));
    }
    public int schemaVersion(){return rootAuthorization==null?1:2;}
    public ControlOperationRequest operationRequest(){return new ControlOperationRequest(ControlOperationKind.ACTIVATE_TARGET_PROTOCOL,this);}
    @Override public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,schemaVersion());CanonicalProtobuf.bytes(out,2,scope.canonicalBytes());CanonicalProtobuf.bytes(out,3,lineage);
        CanonicalProtobuf.bytes(out,4,descriptor.canonicalBytes());CanonicalProtobuf.bytes(out,5,readerEvidenceHash);if(rootAuthorization!=null)CanonicalProtobuf.bytes(out,6,rootAuthorization.canonicalBytes());
    });}
    public static TargetProtocolActivatePayload decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,6,false,"Target protocol activation payload");
        QueryCodecSupport.requireNumbers(f,f.size()==5?new int[]{1,2,3,4,5}:new int[]{1,2,3,4,5,6},"Target protocol activation payload");
        if(QueryCodecSupport.uint(f.get(0),1)!=(f.size()==5?1:2))throw new IllegalArgumentException("unsupported Target activation payload version");
        if(f.size()==5&&encoded.length>V1_MAX_BYTES)throw new IllegalArgumentException("legacy activation payload exceeds its original bound");
        final var payload=new TargetProtocolActivatePayload(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(2),3,16),TargetProtocolActivationDescriptor.decode(QueryCodecSupport.bytes(f.get(3),4)),
                QueryCodecSupport.fixed(f.get(4),5,32),f.size()==6?TargetProtocolTupleAuthorization.decode(QueryCodecSupport.bytes(f.get(5),6)):null);
        QueryCodecSupport.requireCanonical(encoded,payload.canonicalBytes(),"Target protocol activation payload");return payload;
    }
    @Override public boolean equals(Object other){return other instanceof TargetProtocolActivatePayload that&&Arrays.equals(canonicalBytes(),that.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
}
