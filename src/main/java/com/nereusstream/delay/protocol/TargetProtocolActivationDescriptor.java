package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Exact Target extension of an unchanged base Artifact/reset generation; data, not deployment authority. */
public record TargetProtocolActivationDescriptor(ArtifactGenerationSet base,byte[] baseResetManifest,
        byte[] targetSchemaHash,String workerSourceCommit,String kafkaSourceCommit,byte[] kafkaSdkJar,byte[] pulsarSdkJar) {
    public static final int MAX_BYTES=8192;
    public static final int TARGET_CHECKPOINT_SCHEMA=6;
    public static final int TARGET_NATIVE_MAX=53;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-protocol-activation-descriptor\0");
    public TargetProtocolActivationDescriptor {
        Objects.requireNonNull(base);
        baseResetManifest=TargetCompatibilityCodec.assigned(baseResetManifest,32,"original base reset manifest");
        targetSchemaHash=TargetCompatibilityCodec.assigned(targetSchemaHash,32,"independent Target schema hash");
        kafkaSdkJar=TargetCompatibilityCodec.assigned(kafkaSdkJar,32,"actual K1 SDK JAR");
        pulsarSdkJar=TargetCompatibilityCodec.assigned(pulsarSdkJar,32,"actual P1 SDK JAR");
        if(!Objects.requireNonNull(workerSourceCommit).matches("[0-9a-f]{40}")||!Objects.requireNonNull(kafkaSourceCommit).matches("[0-9a-f]{40}"))
            throw new IllegalArgumentException("Target activation requires exact source locks");
    }
    @Override public byte[] baseResetManifest(){return Bytes.copy(baseResetManifest);}
    @Override public byte[] targetSchemaHash(){return Bytes.copy(targetSchemaHash);}
    @Override public byte[] kafkaSdkJar(){return Bytes.copy(kafkaSdkJar);}
    @Override public byte[] pulsarSdkJar(){return Bytes.copy(pulsarSdkJar);}
    public ProtocolTuple tuple(){return ProtocolTuple.targetEvidenceRetirement();}
    private static byte[] versions(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,2);CanonicalProtobuf.uint32(out,2,TARGET_NATIVE_MAX);
        CanonicalProtobuf.uint32(out,3,TARGET_CHECKPOINT_SCHEMA);CanonicalProtobuf.uint32(out,4,8);
        CanonicalProtobuf.uint32(out,5,1);CanonicalProtobuf.uint32(out,6,1);
        CanonicalProtobuf.uint32(out,7,1);CanonicalProtobuf.uint32(out,8,1);
        CanonicalProtobuf.uint32(out,9,3);CanonicalProtobuf.uint32(out,10,1);CanonicalProtobuf.uint32(out,11,28);
        CanonicalProtobuf.uint32(out,12,1);CanonicalProtobuf.uint32(out,13,1);CanonicalProtobuf.uint32(out,14,1);CanonicalProtobuf.uint32(out,15,1);
    });}
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,base.canonicalBytes());CanonicalProtobuf.bytes(out,3,base.setDigest());
        CanonicalProtobuf.bytes(out,4,baseResetManifest);CanonicalProtobuf.bytes(out,5,tuple().canonicalBytes());
        CanonicalProtobuf.bytes(out,6,targetSchemaHash);CanonicalProtobuf.bytes(out,7,versions());
        CanonicalProtobuf.bytes(out,8,Bytes.utf8(workerSourceCommit));CanonicalProtobuf.bytes(out,9,Bytes.utf8(kafkaSourceCommit));
        CanonicalProtobuf.bytes(out,10,kafkaSdkJar);CanonicalProtobuf.bytes(out,11,pulsarSdkJar);
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    @Override public boolean equals(Object other){return other instanceof TargetProtocolActivationDescriptor that&&Arrays.equals(canonicalBytes(),that.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
    public byte[] canonicalBytes(){
        final byte[] encoded=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,12,digest())));
        if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("Target activation descriptor exceeds finite bound");return encoded;
    }
    public static TargetProtocolActivationDescriptor decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,12,false,"Target activation descriptor");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9,10,11,12},"Target activation descriptor");
        if(QueryCodecSupport.uint(f.get(0),1)!=1||!Arrays.equals(QueryCodecSupport.bytes(f.get(6),7),versions()))
            throw new IllegalArgumentException("unsupported Target activation descriptor/version combination");
        final var descriptor=new TargetProtocolActivationDescriptor(ArtifactGenerationSet.decode(QueryCodecSupport.bytes(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(5),6,32),
                new String(QueryCodecSupport.bytes(f.get(7),8),StandardCharsets.US_ASCII),
                new String(QueryCodecSupport.bytes(f.get(8),9),StandardCharsets.US_ASCII),
                QueryCodecSupport.fixed(f.get(9),10,32),QueryCodecSupport.fixed(f.get(10),11,32));
        if(!Arrays.equals(QueryCodecSupport.fixed(f.get(2),3,32),descriptor.base.setDigest())
                ||!ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(4),5)).equals(descriptor.tuple())
                ||!Arrays.equals(QueryCodecSupport.fixed(f.get(11),12,32),descriptor.digest()))
            throw new IllegalArgumentException("Target descriptor lost exact base/tuple/digest binding");
        QueryCodecSupport.requireCanonical(encoded,descriptor.canonicalBytes(),"Target activation descriptor");return descriptor;
    }
}
