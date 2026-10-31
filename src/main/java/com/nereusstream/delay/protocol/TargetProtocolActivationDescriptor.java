package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Exact Target extension of an unchanged base Artifact/reset generation; data, not deployment authority. */
public record TargetProtocolActivationDescriptor(ArtifactGenerationSet base,byte[] baseResetManifest,
        byte[] targetSchemaHash,String workerSourceCommit,String kafkaSourceCommit,byte[] kafkaSdkJar,byte[] pulsarSdkJar,int schemaVersion) {
    public static final int MAX_BYTES=8192;
    public static final int TARGET_CHECKPOINT_SCHEMA=16;
    public static final int TARGET_NATIVE_MAX=56;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-protocol-activation-descriptor\0");
    public TargetProtocolActivationDescriptor(ArtifactGenerationSet base,byte[] reset,byte[] schema,String worker,String kafka,byte[] kafkaJar,byte[] pulsarJar){
        this(base,reset,schema,worker,kafka,kafkaJar,pulsarJar,5);
    }
    public boolean extendedRootAuthorization(){return schemaVersion==15||schemaVersion==16||schemaVersion==17||schemaVersion==18||schemaVersion==19||schemaVersion==20||schemaVersion==21||schemaVersion==22;}
    public boolean supportsParentControlCredit(){return schemaVersion==22;}
    public boolean supportsMigrationObjectPayloadReclaim(){return schemaVersion==15;}
    public boolean supportsMigrationUncommittedObjectReclaim(){return schemaVersion==16;}
    public boolean supportsMigrationRetirementIntent(){return schemaVersion==17;}
    public boolean supportsMigrationRetirementConfirmation(){return schemaVersion==18;}
    public boolean supportsPayloadObligationSummary(){return schemaVersion==19;}
    public boolean supportsMigrationLogicalFirstArchive(){return schemaVersion==20;}
    public boolean supportsMigrationFullSourceArchive(){return schemaVersion==21;}
    public boolean supportsMigrationArchiveRowSchema(int version){return version==1&&(supportsMigrationLogicalFirstArchive()||supportsMigrationFullSourceArchive())||version==2&&supportsMigrationFullSourceArchive();}
    public boolean supportsMigrationArchiveStateSchema(int version){return version==2&&(supportsMigrationLogicalFirstArchive()||supportsMigrationFullSourceArchive())||version==3&&supportsMigrationFullSourceArchive();}
    /** Archive-format representation boundary; actual frozen Source bytes and service reservation are separate. */
    public int maximumMigrationArchiveSourceFrameBytes(){return supportsMigrationFullSourceArchive()?TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES:supportsMigrationLogicalFirstArchive()?32<<20:0;}
    /** Explicit decoder matrix; these are reader formats, not grants for another tuple. */
    public int nativeMaximum(){return supportsParentControlCredit()?63:supportsMigrationLogicalFirstArchive()||supportsMigrationFullSourceArchive()?72:extendedRootAuthorization()?71:schemaVersion>=10?TARGET_NATIVE_MAX:schemaVersion==1?53:schemaVersion==2?54:55;}
    public int checkpointSchema(){return supportsParentControlCredit()?6:extendedRootAuthorization()?16:schemaVersion>=14?16:schemaVersion>=13?15:schemaVersion>=12?14:schemaVersion>=11?13:schemaVersion>=10?12:schemaVersion>=9?11:schemaVersion>=8?10:schemaVersion>=7?9:schemaVersion>=6?8:schemaVersion>=5?7:6;}
    public boolean supportsSourceHistoryRecords(){return schemaVersion<=21&&schemaVersion>=2;}
    public boolean supportsAcceptedHistoryTip(){return schemaVersion<=21&&schemaVersion>=3;}
    public boolean supportsCapacityOwnerStaging(){return schemaVersion<=21&&schemaVersion>=4;}
    public boolean supportsRetainedMarkerConfirmation(){return schemaVersion<=21&&schemaVersion>=5;}
    public boolean supportsResourceReclaim(){return schemaVersion<=21&&schemaVersion>=6;}
    public boolean supportsInlinePayloadReclaim(){return schemaVersion<=21&&schemaVersion>=7;}
    public boolean supportsSelfContainedInlinePayloadReclaim(){return schemaVersion<=21&&schemaVersion>=8;}
    public boolean supportsDuplicatePositionReclaim(){return schemaVersion<=21&&schemaVersion>=14;}
    public boolean supportsIncarnationReclaim(){return schemaVersion<=21&&schemaVersion>=13;}
    public boolean supportsAnchorAllocationReclaim(){return schemaVersion<=21&&schemaVersion>=12;}
    public boolean supportsNativeRetainedReclaim(){return schemaVersion<=21&&schemaVersion>=9;}
    /** Descriptor11 separately admits original uncommitted reservation upload closure/terminal history. */
    public boolean supportsUncommittedObjectPayloadReclaim(){return schemaVersion<=21&&schemaVersion>=11;}
    /** Descriptor10 admits the exact committed OBJECT intent/confirmation contract. */
    public boolean supportsObjectPayloadReclaim(){return schemaVersion<=21&&schemaVersion>=10;}
    public TargetProtocolActivationDescriptor {
        if(schemaVersion<1||schemaVersion>22)throw new IllegalArgumentException("unsupported Target descriptor schema");
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
    public ProtocolTuple tuple(){return switch(schemaVersion){
        case 15 -> ProtocolTuple.targetMigrationObjectPayloadReclaim();case 16 -> ProtocolTuple.targetMigrationUncommittedObjectReclaim();
        case 17 -> ProtocolTuple.targetMigrationRetireIntent();case 18 -> ProtocolTuple.targetMigrationRetireConfirm();case 19 -> ProtocolTuple.targetPayloadObligationSummary();case 20 -> ProtocolTuple.targetMigrationLogicalFirstArchive();case 21 -> ProtocolTuple.targetMigrationFullSourceArchive();case 22 -> ProtocolTuple.targetParentControlCredit();
        default -> legacyTuple();};}
    private ProtocolTuple legacyTuple(){return supportsDuplicatePositionReclaim()?ProtocolTuple.targetDuplicatePositionReclaim():supportsIncarnationReclaim()?ProtocolTuple.targetIncarnationReclaim():supportsAnchorAllocationReclaim()?ProtocolTuple.targetAnchorAllocationReclaim():supportsUncommittedObjectPayloadReclaim()?ProtocolTuple.targetUncommittedObjectReclaim():supportsObjectPayloadReclaim()?ProtocolTuple.targetObjectPayloadReclaim():supportsNativeRetainedReclaim()?ProtocolTuple.targetNativeRetainedReclaim():supportsSelfContainedInlinePayloadReclaim()?ProtocolTuple.targetSelfContainedInlinePayloadReclaim():supportsInlinePayloadReclaim()?ProtocolTuple.targetInlinePayloadReclaim():supportsResourceReclaim()?ProtocolTuple.targetResourceReclaim():ProtocolTuple.targetEvidenceRetirement();}
    private static byte[] versions(int version){
        if(version==22)return parentControlCreditVersions();
        if(version>=15&&version<=21)return extendedVersions(version);
        return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,2);CanonicalProtobuf.uint32(out,2,version>=10?TARGET_NATIVE_MAX:version==1?53:version==2?54:55);
        CanonicalProtobuf.uint32(out,3,version>=14?16:version>=13?15:version>=12?14:version>=11?13:version>=10?12:version>=9?11:version>=8?10:version>=7?9:version>=6?8:version>=5?7:6);CanonicalProtobuf.uint32(out,4,version>=14?17:version>=13?16:version>=12?15:version>=11?14:version>=10?13:version>=9?12:version>=8?11:version>=7?10:version>=6?9:8);
        CanonicalProtobuf.uint32(out,5,1);CanonicalProtobuf.uint32(out,6,1);
        CanonicalProtobuf.uint32(out,7,1);CanonicalProtobuf.uint32(out,8,1);
        CanonicalProtobuf.uint32(out,9,3);CanonicalProtobuf.uint32(out,10,1);CanonicalProtobuf.uint32(out,11,28);
        CanonicalProtobuf.uint32(out,12,1);CanonicalProtobuf.uint32(out,13,version>=5?2:1);CanonicalProtobuf.uint32(out,14,1);CanonicalProtobuf.uint32(out,15,1);
        if(version>=2)CanonicalProtobuf.uint32(out,16,version>=14?10:version>=13?9:version>=12?8:version>=11?7:version>=10?6:version>=9?5:version>=8?4:version>=7?3:version>=6?2:1);
        if(version>=3)CanonicalProtobuf.uint32(out,17,1);
        if(version>=4){CanonicalProtobuf.uint32(out,18,1);CanonicalProtobuf.uint32(out,19,2);}
        if(version>=5){CanonicalProtobuf.uint32(out,20,1);CanonicalProtobuf.uint32(out,21,1);CanonicalProtobuf.uint32(out,22,3);}
        if(version>=6)CanonicalProtobuf.uint32(out,23,1);
        if(version>=7){CanonicalProtobuf.uint32(out,24,TerminalPolicySemantic.SCHEMA_VERSION);
            CanonicalProtobuf.uint32(out,25,2);CanonicalProtobuf.uint32(out,26,1);}
        if(version>=8)CanonicalProtobuf.uint32(out,27,TargetInlinePayloadSourceFacts.SCHEMA_VERSION);
        if(version>=9){CanonicalProtobuf.uint32(out,28,TargetNativeReclaimFacts.SCHEMA_VERSION);CanonicalProtobuf.uint32(out,29,TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES);}
        if(version>=10){CanonicalProtobuf.uint32(out,30,version>=11?2:1);CanonicalProtobuf.uint32(out,31,TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES);
            CanonicalProtobuf.uint32(out,32,2);CanonicalProtobuf.uint32(out,33,TargetObjectPayloadOrigin.SCHEMA_VERSION);}
        if(version>=11){CanonicalProtobuf.uint32(out,34,TargetUncommittedPayloadFacts.SCHEMA_VERSION);CanonicalProtobuf.uint32(out,35,TargetUncommittedObjectClosure.SCHEMA_VERSION);
            CanonicalProtobuf.uint32(out,36,TargetUncommittedObjectClosure.MAX_VERSIONS);CanonicalProtobuf.uint32(out,37,TargetUncommittedObjectClosure.MAX_BYTES);}
        if(version>=12)CanonicalProtobuf.uint32(out,38,2);
        if(version>=13)CanonicalProtobuf.uint32(out,39,1);
        if(version>=14)CanonicalProtobuf.uint32(out,40,6);
    });}
    private static byte[] parentControlCreditVersions(){return CanonicalProtobuf.message(out->{
        // The Parent reader purpose has no migration or reclamation permission.
        CanonicalProtobuf.uint32(out,1,2);CanonicalProtobuf.uint32(out,2,63);CanonicalProtobuf.uint32(out,3,6);
        CanonicalProtobuf.uint32(out,7,2);CanonicalProtobuf.uint32(out,8,2);CanonicalProtobuf.uint32(out,9,6);
        CanonicalProtobuf.uint32(out,10,1);CanonicalProtobuf.uint32(out,11,28);CanonicalProtobuf.uint32(out,15,1);
        CanonicalProtobuf.uint32(out,41,8);CanonicalProtobuf.uint32(out,43,1);
        CanonicalProtobuf.uint32(out,51,1); // Native63 Parent credit
        CanonicalProtobuf.uint32(out,52,1); // immutable initial Root snapshot
        CanonicalProtobuf.uint32(out,53,1); // Native33 quota incarnation
        CanonicalProtobuf.uint32(out,54,1); // Native30 applied quota grant
        CanonicalProtobuf.uint32(out,55,1); // Native35 first/POS result
        CanonicalProtobuf.uint32(out,56,1); // Native31 aggregate
        CanonicalProtobuf.uint32(out,57,1); // quota usage/vector accounting
        CanonicalProtobuf.uint32(out,58,2); // explicit pre-A Root issuer policy reader
    });}
    private static byte[] extendedVersions(int version){return CanonicalProtobuf.message(out->{
        // A separate closed matrix avoids silently inheriting future descriptor privileges.
        CanonicalProtobuf.uint32(out,1,2);CanonicalProtobuf.uint32(out,2,version==20||version==21?72:71);CanonicalProtobuf.uint32(out,3,16);
        CanonicalProtobuf.uint32(out,4,version==15?18:version==16?19:17);
        CanonicalProtobuf.uint32(out,5,1);CanonicalProtobuf.uint32(out,6,1);CanonicalProtobuf.uint32(out,7,1);CanonicalProtobuf.uint32(out,8,1);
        CanonicalProtobuf.uint32(out,9,3);CanonicalProtobuf.uint32(out,10,1);CanonicalProtobuf.uint32(out,11,28);
        CanonicalProtobuf.uint32(out,12,1);CanonicalProtobuf.uint32(out,13,2);CanonicalProtobuf.uint32(out,14,1);CanonicalProtobuf.uint32(out,15,1);
        CanonicalProtobuf.uint32(out,16,12);CanonicalProtobuf.uint32(out,17,1);CanonicalProtobuf.uint32(out,18,1);CanonicalProtobuf.uint32(out,19,2);
        CanonicalProtobuf.uint32(out,20,1);CanonicalProtobuf.uint32(out,21,1);CanonicalProtobuf.uint32(out,22,3);CanonicalProtobuf.uint32(out,23,1);
        CanonicalProtobuf.uint32(out,24,TerminalPolicySemantic.SCHEMA_VERSION);CanonicalProtobuf.uint32(out,25,2);CanonicalProtobuf.uint32(out,26,1);
        CanonicalProtobuf.uint32(out,27,TargetInlinePayloadSourceFacts.SCHEMA_VERSION);CanonicalProtobuf.uint32(out,28,TargetNativeReclaimFacts.SCHEMA_VERSION);
        CanonicalProtobuf.uint32(out,29,TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES);CanonicalProtobuf.uint32(out,30,4);
        CanonicalProtobuf.uint32(out,31,TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES);CanonicalProtobuf.uint32(out,32,2);
        CanonicalProtobuf.uint32(out,33,2);CanonicalProtobuf.uint32(out,34,2);CanonicalProtobuf.uint32(out,35,TargetUncommittedObjectClosure.SCHEMA_VERSION);
        CanonicalProtobuf.uint32(out,36,TargetUncommittedObjectClosure.MAX_VERSIONS);CanonicalProtobuf.uint32(out,37,TargetUncommittedObjectClosure.MAX_BYTES);
        CanonicalProtobuf.uint32(out,38,2);CanonicalProtobuf.uint32(out,39,1);CanonicalProtobuf.uint32(out,40,6);
        CanonicalProtobuf.uint32(out,41,version-14); // one exact new reader purpose, never another permission
        CanonicalProtobuf.uint32(out,42,2); // activation payload and retained record
        CanonicalProtobuf.uint32(out,43,1); // signed Root tuple authorization
        CanonicalProtobuf.uint32(out,44,65);CanonicalProtobuf.uint32(out,45,version==20||version==21?72:71); // bounded migration Native interval
        CanonicalProtobuf.uint32(out,46,2); // PayloadOwner summary reader schema
        if(version==20){
            CanonicalProtobuf.uint32(out,47,1); // Native72 logical-first archive
            CanonicalProtobuf.uint32(out,48,2); // Native71 archive head/history
            CanonicalProtobuf.uint32(out,49,1); // COPY_BATCH / TAKEOVER request
        }
        if(version==21){
            CanonicalProtobuf.uint32(out,47,2); // Native72 complete inline Source archive
            CanonicalProtobuf.uint32(out,48,3); // Native71 complete inline Source archive head/history
            CanonicalProtobuf.uint32(out,49,1); // unchanged COPY_BATCH / TAKEOVER request
            CanonicalProtobuf.uint32(out,50,TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES);
        }
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
        if((version<1||version>22)||!Arrays.equals(QueryCodecSupport.bytes(f.get(6),7),versions(version)))
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
