package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Exact Target extension of an unchanged base Artifact/reset generation; data, not deployment authority. */
public record TargetProtocolActivationDescriptor(ArtifactGenerationSet base,byte[] baseResetManifest,
        byte[] targetSchemaHash,String workerSourceCommit,String kafkaSourceCommit,byte[] kafkaSdkJar,byte[] pulsarSdkJar,int schemaVersion) {
    public static final int MAX_BYTES=8192;
    public static final int TARGET_CHECKPOINT_SCHEMA=8;
    public static final int TARGET_NATIVE_MAX=55;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-protocol-activation-descriptor\0");
    public TargetProtocolActivationDescriptor(ArtifactGenerationSet base,byte[] reset,byte[] schema,String worker,String kafka,byte[] kafkaJar,byte[] pulsarJar){
        this(base,reset,schema,worker,kafka,kafkaJar,pulsarJar,5);
    }
    public boolean supportsSourceHistoryRecords(){return schemaVersion>=2;}
    public boolean supportsAcceptedHistoryTip(){return schemaVersion>=3;}
    public boolean supportsCapacityOwnerStaging(){return schemaVersion>=4;}
    public boolean supportsRetainedMarkerConfirmation(){return schemaVersion>=5;}
    public boolean supportsResourceReclaim(){return schemaVersion>=6;}
    public TargetProtocolActivationDescriptor {
        if(schemaVersion<1||schemaVersion>6)throw new IllegalArgumentException("unsupported Target descriptor schema");
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
    public ProtocolTuple tuple(){return supportsResourceReclaim()?ProtocolTuple.targetResourceReclaim():ProtocolTuple.targetEvidenceRetirement();}
    private static byte[] versions(int version){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,2);CanonicalProtobuf.uint32(out,2,version==1?53:version==2?54:TARGET_NATIVE_MAX);
        CanonicalProtobuf.uint32(out,3,version>=6?8:version>=5?7:6);CanonicalProtobuf.uint32(out,4,version>=6?9:8);
        CanonicalProtobuf.uint32(out,5,1);CanonicalProtobuf.uint32(out,6,1);
        CanonicalProtobuf.uint32(out,7,1);CanonicalProtobuf.uint32(out,8,1);
        CanonicalProtobuf.uint32(out,9,3);CanonicalProtobuf.uint32(out,10,1);CanonicalProtobuf.uint32(out,11,28);
        CanonicalProtobuf.uint32(out,12,1);CanonicalProtobuf.uint32(out,13,version>=5?2:1);CanonicalProtobuf.uint32(out,14,1);CanonicalProtobuf.uint32(out,15,1);
        if(version>=2)CanonicalProtobuf.uint32(out,16,version>=6?2:1);
        if(version>=3)CanonicalProtobuf.uint32(out,17,1);
        if(version>=4){CanonicalProtobuf.uint32(out,18,1);CanonicalProtobuf.uint32(out,19,2);}
        if(version>=5){CanonicalProtobuf.uint32(out,20,1);CanonicalProtobuf.uint32(out,21,1);CanonicalProtobuf.uint32(out,22,3);}
        if(version>=6)CanonicalProtobuf.uint32(out,23,1);
    });}
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,schemaVersion);CanonicalProtobuf.bytes(out,2,base.canonicalBytes());CanonicalProtobuf.bytes(out,3,base.setDigest());
        CanonicalProtobuf.bytes(out,4,baseResetManifest);CanonicalProtobuf.bytes(out,5,tuple().canonicalBytes());
        CanonicalProtobuf.bytes(out,6,targetSchemaHash);CanonicalProtobuf.bytes(out,7,versions(schemaVersion));
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
        final int version=QueryCodecSupport.uint32(f.get(0),1);
        if((version<1||version>6)||!Arrays.equals(QueryCodecSupport.bytes(f.get(6),7),versions(version)))
            throw new IllegalArgumentException("unsupported Target activation descriptor/version combination");
        final var descriptor=new TargetProtocolActivationDescriptor(ArtifactGenerationSet.decode(QueryCodecSupport.bytes(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(5),6,32),
                new String(QueryCodecSupport.bytes(f.get(7),8),StandardCharsets.US_ASCII),
                new String(QueryCodecSupport.bytes(f.get(8),9),StandardCharsets.US_ASCII),
                QueryCodecSupport.fixed(f.get(9),10,32),QueryCodecSupport.fixed(f.get(10),11,32),version);
        if(!Arrays.equals(QueryCodecSupport.fixed(f.get(2),3,32),descriptor.base.setDigest())
                ||!ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(4),5)).equals(descriptor.tuple())
                ||!Arrays.equals(QueryCodecSupport.fixed(f.get(11),12,32),descriptor.digest()))
            throw new IllegalArgumentException("Target descriptor lost exact base/tuple/digest binding");
        QueryCodecSupport.requireCanonical(encoded,descriptor.canonicalBytes(),"Target activation descriptor");return descriptor;
    }
}
