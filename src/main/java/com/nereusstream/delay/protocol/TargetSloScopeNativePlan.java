package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.Objects;

/** Frozen logical bounds; these bytes do not authorize physical Source or registry funding. */
public final class TargetSloScopeNativePlan {
    public static final int VERSION = 1;
    public static final int SETTLEMENT_VERSION = 2;
    public static final int EXPOSURE_VERSION = 3;
    public static final int V1_MAX_CANONICAL_BYTES = 2_097_908;
    public static final int V2_MAX_CANONICAL_BYTES = 2_098_035;
    public static final int MAX_CANONICAL_BYTES = 2_098_242;
    private static final int EXPOSURE_POLICY_MAX_CANONICAL_BYTES = 172;
    public static final int MAX_REGISTRY_WRAPPER_BYTES = 256;
    static final int V1_SOURCE_BYTES = 1_048_670;
    static final int V1_ROOT_BYTES = 1_049_078;
    static final int V1_VECTOR_BYTES = 960;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-scope-native-plan\0");

    /** Signed numeric format limits, independent of later codec MAX constants. */
    public record Ceilings(int resultBytes, int sampleBytes, int writerEpochBytes, int generationBytes,
            int headBytes, int eventBytes, int custodyBytes, int physicalSourceBytes, int maximumUsageBytes,
            int maximumVectorBytes, int valueEnvelopeBytes, int writerKeyBytes, int generationKeyBytes,
            int headKeyBytes, int registryWrapperBytes, int registryRootFixedBytes, int registrySlotBytes) {
        public static final int MAX_CANONICAL_BYTES = 2 + 17 * 7;
        public Ceilings {
            final int[] values = {resultBytes,sampleBytes,writerEpochBytes,generationBytes,headBytes,eventBytes,custodyBytes,
                    physicalSourceBytes,maximumUsageBytes,maximumVectorBytes,valueEnvelopeBytes,writerKeyBytes,
                    generationKeyBytes,headKeyBytes,registryWrapperBytes,registryRootFixedBytes,registrySlotBytes};
            for (int value : values) if (value < 1 || value == Integer.MAX_VALUE)
                throw new IllegalArgumentException("Scope format ceilings must be finite positive bounds");
            if (maximumVectorBytes != V1_VECTOR_BYTES || valueEnvelopeBytes != 12
                    || maximumUsageBytes != usageMaximum(physicalSourceBytes, maximumVectorBytes))
                throw new IllegalArgumentException("unknown Scope settlement vector/envelope/Usage format");
        }
        public byte[] canonicalBytes() {
            final int[] values = {resultBytes,sampleBytes,writerEpochBytes,generationBytes,headBytes,eventBytes,custodyBytes,
                    physicalSourceBytes,maximumUsageBytes,maximumVectorBytes,valueEnvelopeBytes,writerKeyBytes,
                    generationKeyBytes,headKeyBytes,registryWrapperBytes,registryRootFixedBytes,registrySlotBytes};
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out,1,SETTLEMENT_VERSION);
                for (int i=0;i<values.length;i++) CanonicalProtobuf.uint32(out,i+2,values[i]);
            });
        }
        public static Ceilings decode(byte[] encoded) {
            final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,18,false,"Scope frozen formats");
            final int[] fields=new int[18];for(int i=0;i<fields.length;i++)fields[i]=i+1;
            QueryCodecSupport.requireNumbers(f,fields,"Scope frozen formats");
            if(QueryCodecSupport.uint32(f.getFirst(),1)!=SETTLEMENT_VERSION)throw new IllegalArgumentException("unknown Scope frozen formats");
            final int[] v=new int[17];for(int i=0;i<v.length;i++)v[i]=QueryCodecSupport.uint32(f.get(i+1),i+2);
            final var value=new Ceilings(v[0],v[1],v[2],v[3],v[4],v[5],v[6],v[7],v[8],v[9],v[10],v[11],v[12],v[13],v[14],v[15],v[16]);
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Scope frozen formats");return value;
        }
    }
    /** Schema-3 Root2 reservation coefficients, independent of later registry codec ceilings. */
    public record ExposureFormat3(int registryRootFixedBytes, int registrySlotBytes, int registryDebitBytes) {
        public static final int MAX_CANONICAL_BYTES = 23;
        public ExposureFormat3 {
            if (registryRootFixedBytes != 128 || registrySlotBytes != 110 || registryDebitBytes != 278)
                throw new IllegalArgumentException("unknown Scope exposure Root format");
        }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, EXPOSURE_VERSION);
                CanonicalProtobuf.uint32(out, 2, registryRootFixedBytes);
                CanonicalProtobuf.uint32(out, 3, registrySlotBytes);
                CanonicalProtobuf.uint32(out, 4, registryDebitBytes);
            });
        }
        public static ExposureFormat3 decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "Scope exposure Root format");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4}, "Scope exposure Root format");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != EXPOSURE_VERSION)
                throw new IllegalArgumentException("unknown Scope exposure Root format schema");
            final var value = new ExposureFormat3(QueryCodecSupport.uint32(f.get(1), 2),
                    QueryCodecSupport.uint32(f.get(2), 3), QueryCodecSupport.uint32(f.get(3), 4));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope exposure Root format");
            return value;
        }
        public long maximumRootReservation(TargetSloControlScopeHeader header, TargetScopeExposurePolicy policy) {
            return Math.addExact(Math.addExact(header.canonicalBytes().length, registryRootFixedBytes),
                    Math.addExact(Math.multiplyExact((long) header.limits().maximumSlots(), registrySlotBytes),
                            Math.multiplyExact((long) policy.maximumDebits(), registryDebitBytes)));
        }
    }
    public enum NativeClass { FIRST, POSITION, SAMPLE_PRIMARY, SAMPLE_PROJECTION, WRITER_EPOCH, GENERATION, HEAD, EVENT, CUSTODY }
    private final int schemaVersion;
    private final TargetQuotaIncarnation originalRoot;
    private final SourcePosition sourceAnchor;
    private final long maximumSourceOccurrences, maximumSourceFrameBytes, maximumSamples;
    private final int maximumWriterEpochs, maximumGenerations;
    private final Ceilings ceilings;
    private final ExposureFormat3 exposureFormat;
    private final TargetScopeExposurePolicy exposurePolicy;
    private final byte[] digest;

    /** The original constructor retains schema-1 bytes and its zero-only settlement boundary. */
    public TargetSloScopeNativePlan(TargetQuotaIncarnation root,SourcePosition anchor,long n,long f,long s,int w,int g) {
        this(VERSION,root,anchor,n,f,s,w,g,null,null,null);
    }
    /** Explicit schema-2 creation freezes the complete current codecs, including cross-Scope CLOSE/STOP frames. */
    public static TargetSloScopeNativePlan frozen(TargetQuotaIncarnation root,SourcePosition anchor,long n,long f,long s,int w,int g) {
        Objects.requireNonNull(root);TargetSourcePosition.requireBounded(anchor);
        final int source=TargetQuotaBookkeeping.maximumSourceBytes(anchor);
        final var formats=new Ceilings(TargetResultRecord.MAX_CANONICAL_BYTES,TargetSloObservationSample.MAX_CANONICAL_BYTES,
                TargetSloWriterEpoch.MAX_CANONICAL_BYTES,TargetSloObservationGeneration.MAX_CANONICAL_BYTES,
                TargetSloObservationState.MAX_CANONICAL_BYTES,TargetSloObservationEventRecord.MAX_CANONICAL_BYTES,
                TargetSloCustodyRecord.MAX_CANONICAL_BYTES,source,usageMaximum(source,V1_VECTOR_BYTES),V1_VECTOR_BYTES,
                TargetQuotaAccounting.VALUE_ENVELOPE_BYTES,TargetSloWriterEpoch.key(root.scope(),new byte[32],1).length,
                TargetSloObservationGeneration.key(root.scope(),1).length,TargetSloObservationState.key(root.scope()).length,
                MAX_REGISTRY_WRAPPER_BYTES,TargetSloControlScopeRoot.ROOT_FIXED_RESERVED_BYTES,TargetSloControlScopeRoot.Slot.RESERVED_BYTES);
        return new TargetSloScopeNativePlan(SETTLEMENT_VERSION,root,anchor,n,f,s,w,g,formats,null,null);
    }
    /** Explicit schema-3 creation binds its paid exposure policy and complete worst-case Root2 shape. */
    public static TargetSloScopeNativePlan frozen(TargetQuotaIncarnation root,SourcePosition anchor,long n,long f,long s,int w,int g,
            TargetScopeExposurePolicy policy) {
        final var nativePlan = frozen(root,anchor,n,f,s,w,g);
        return new TargetSloScopeNativePlan(EXPOSURE_VERSION,root,anchor,n,f,s,w,g,nativePlan.ceilings,
                new ExposureFormat3(128,110,278),Objects.requireNonNull(policy));
    }
    private TargetSloScopeNativePlan(int version,TargetQuotaIncarnation root,SourcePosition anchor,long n,long f,long s,int w,int g,
            Ceilings formats,ExposureFormat3 exposureFormat,TargetScopeExposurePolicy exposurePolicy) {
        schemaVersion=version;originalRoot=Objects.requireNonNull(root);sourceAnchor=TargetSourcePosition.requireBounded(anchor);
        if((version!=VERSION&&version!=SETTLEMENT_VERSION&&version!=EXPOSURE_VERSION)||(version==VERSION)!=(formats==null)
                ||(version==EXPOSURE_VERSION)!=(exposureFormat!=null)||(version==EXPOSURE_VERSION)!=(exposurePolicy!=null)
                ||root.identity().kind()!=TargetQuotaIdentity.Kind.SHARD||root.draining()||root.canonicalBytes().length>V1_ROOT_BYTES
                ||anchor.canonicalBytes().length>V1_SOURCE_BYTES||root.allocation().source().canonicalBytes().length>V1_SOURCE_BYTES
                ||n<1||n==Long.MAX_VALUE||f<1||f==Long.MAX_VALUE||s<0||s==Long.MAX_VALUE||w<0||g<0)
            throw new IllegalArgumentException("Scope plan requires its actual OPEN SHARD Root and finite limits");
        maximumSourceOccurrences=n;maximumSourceFrameBytes=f;maximumSamples=s;maximumWriterEpochs=w;maximumGenerations=g;ceilings=formats;
        this.exposureFormat=exposureFormat;this.exposurePolicy=exposurePolicy;
        final long maximumSourceBytes=Math.multiplyExact(n,f);requireSource(root.allocation().source());
        if(anchor.compareTo(root.allocation().source())<0)throw new IllegalArgumentException("Scope anchor precedes Root allocation");
        if(settlementEnabled()&&(formats.physicalSourceBytes()!=legacyMaximumSourceBytes(anchor)
                ||formats.writerKeyBytes()!=2+scope().keySuffix().length+40||formats.generationKeyBytes()!=2+scope().keySuffix().length+8
                ||formats.headKeyBytes()!=2+scope().keySuffix().length))
            throw new IllegalArgumentException("Scope frozen formats changed physical Source or original key shapes");
        if(exposurePolicy!=null&&exposurePolicy.canonicalBytes().length>EXPOSURE_POLICY_MAX_CANONICAL_BYTES)
            throw new IllegalArgumentException("Scope exposure policy exceeds its frozen format bound");
        if(exposurePolicy!=null&&(exposurePolicy.maximumFrameBytes()>f||exposurePolicy.lifetime().records()>n
                ||exposurePolicy.lifetime().bytes()>maximumSourceBytes))
            throw new IllegalArgumentException("Scope exposure exceeds its paid Source occurrence/frame bounds");
        digest=Bytes.sha256(DOMAIN,fields());
        if(canonicalBytes().length>maximumCanonicalBytes(version))throw new IllegalArgumentException("Scope plan exceeds codec bound");
    }
    public int schemaVersion(){return schemaVersion;}
    public boolean settlementEnabled(){return schemaVersion==SETTLEMENT_VERSION||schemaVersion==EXPOSURE_VERSION;}
    public ExposureFormat3 exposureFormat(){return exposureFormat;}
    public TargetScopeExposurePolicy exposurePolicy(){return exposurePolicy;}
    public TargetQuotaIncarnation originalRoot(){return originalRoot;}
    public TargetQuotaScope scope(){return originalRoot.scope();}
    public byte[] lineage(){return originalRoot.recoveryLineage();}
    public TargetQuotaAccounting accounting(){return originalRoot.accounting();}
    public SourcePosition sourceAnchor(){return sourceAnchor;}
    public long maximumSourceOccurrences(){return maximumSourceOccurrences;}
    public long maximumSourceFrameBytes(){return maximumSourceFrameBytes;}
    public long maximumSamples(){return maximumSamples;}
    public int maximumWriterEpochs(){return maximumWriterEpochs;}
    public int maximumGenerations(){return maximumGenerations;}
    public byte[] digest(){return Bytes.copy(digest);}
    public Ceilings ceilings(){if(!settlementEnabled())throw new IllegalStateException("legacy Scope has no settlement formats");return ceilings;}
    private Ceilings effectiveCeilings(){
        if(settlementEnabled())return ceilings;
        final int source=legacyMaximumSourceBytes(sourceAnchor);
        // Schema-1 snapshot: never reprice an old Scope from later codec constants.
        return new Ceilings(4_196_001,31_649_130,4_201_754,5_256_557,1_049_037,36_895_441,32_701_970,
                source,usageMaximum(source,V1_VECTOR_BYTES),V1_VECTOR_BYTES,12,2+scope().keySuffix().length+40,
                2+scope().keySuffix().length+8,2+scope().keySuffix().length,256,64,110);
    }
    static int legacyMaximumSourceBytes(SourcePosition source){
        TargetSourcePosition.requireBounded(source);
        return Math.addExact(source.canonicalBytes().length,source instanceof KafkaSourcePosition k&&k.leaderEpoch()==null?4:0);
    }
    static int usageMaximum(int source,int vector){return Math.addExact(2+6+vector+4*11+6+49,source);}
    public long maximumUsageBytes(){return effectiveCeilings().maximumUsageBytes();}
    public int maximumVectorBytes(){return effectiveCeilings().maximumVectorBytes();}
    public int maximumPhysicalSourceBytes(){return effectiveCeilings().physicalSourceBytes();}
    public void requireRoot(TargetQuotaIncarnation current){
        if(current==null||!originalRoot.identity().equals(current.identity())||!Arrays.equals(originalRoot.tenantScope(),current.tenantScope())
                ||!originalRoot.accounting().equals(current.accounting())||!Arrays.equals(originalRoot.recoveryLineage(),current.recoveryLineage())
                ||!originalRoot.allocation().equals(current.allocation()))throw new IllegalArgumentException("Scope changed frozen Root/tenant/accounting/lineage/origin");
    }
    public void requireSource(SourcePosition source){
        TargetSourcePosition.requireBounded(source);
        if(!scope().shard().equals(source.shardId())||!sourceAnchor.sameSourceIdentity(source)
                ||ceilings!=null&&source.canonicalBytes().length>ceilings.physicalSourceBytes())throw new IllegalArgumentException("Scope changed physical Source or encoding bound");
    }
    public void requireHeader(TargetSloControlScopeHeader header){
        final var exact=Objects.requireNonNull(header);requireSource(exact.sourceAnchor());
        if((schemaVersion==EXPOSURE_VERSION?exact.schemaVersion()!=2||!exposurePolicy.equals(exact.exposurePolicy()):exact.schemaVersion()!=1)
                ||!scope().equals(exact.scope())||!Arrays.equals(lineage(),exact.lineage())||!Arrays.equals(digest,exact.resourceContractDigest())
                ||maximumWriterEpochs>exact.limits().maximumSlots()||maximumGenerations>exact.limits().maximumSlots()
                ||Math.addExact((long)maximumWriterEpochs,maximumGenerations)>exact.limits().maximumSlots())
            throw new IllegalArgumentException("Scope header changed frozen plan/Source/Control slots");
    }
    public CapacityVector metadataCharge(long key,long payload){return recordCharge(TargetQuotaAccounting.RecordClass.STATE,key,payload);}
    private CapacityVector recordCharge(TargetQuotaAccounting.RecordClass kind,long key,long payload){
        if(key<1||payload<0)throw new IllegalArgumentException("invalid Scope record length");
        final long[] amounts=new long[CapacityDimension.COUNT];
        final var bytes=switch(kind){case STATE->CapacityDimension.LOGICAL_STATE_BYTES;case RESULT->CapacityDimension.RESULT_BYTES;
            case EVIDENCE->CapacityDimension.EVIDENCE_BYTES;case SYSTEM_MUTATION->CapacityDimension.SYSTEM_MUTATION_BYTES;};
        amounts[bytes.wireValue()-1]=Math.addExact(Math.addExact(Math.addExact(key,payload),effectiveCeilings().valueEnvelopeBytes()),accounting().recordOverheadBytes());
        if(kind==TargetQuotaAccounting.RecordClass.RESULT)amounts[CapacityDimension.RESULT_RECORDS.wireValue()-1]=1;
        if(kind==TargetQuotaAccounting.RecordClass.EVIDENCE)amounts[CapacityDimension.EVIDENCE_RECORDS.wireValue()-1]=1;
        if(kind==TargetQuotaAccounting.RecordClass.SYSTEM_MUTATION)amounts[CapacityDimension.SYSTEM_MUTATION_RECORDS.wireValue()-1]=1;
        return new CapacityVector(amounts);
    }
    private long payloadCeiling(NativeClass kind){final var c=effectiveCeilings();return switch(kind){
        case FIRST,POSITION->c.resultBytes();case SAMPLE_PRIMARY,SAMPLE_PROJECTION->c.sampleBytes();case WRITER_EPOCH->c.writerEpochBytes();
        case GENERATION->c.generationBytes();case HEAD->c.headBytes();case EVENT->c.eventBytes();case CUSTODY->c.custodyBytes();};}
    private long keyCeiling(NativeClass kind){final var c=effectiveCeilings();return switch(kind){
        case FIRST,SAMPLE_PRIMARY->34;case POSITION->2L+c.physicalSourceBytes();case SAMPLE_PROJECTION,CUSTODY->66;
        case WRITER_EPOCH->c.writerKeyBytes();case GENERATION->c.generationKeyBytes();case HEAD->c.headKeyBytes();case EVENT->42;};}
    public CapacityVector recordCeiling(NativeClass kind){Objects.requireNonNull(kind);return recordCharge(kind==NativeClass.FIRST
            ?TargetQuotaAccounting.RecordClass.RESULT:kind==NativeClass.POSITION?TargetQuotaAccounting.RecordClass.EVIDENCE
            :TargetQuotaAccounting.RecordClass.STATE,keyCeiling(kind),payloadCeiling(kind));}
    public void requireNativeRecord(NativeClass kind,long key,long payload){
        if(!settlementEnabled()||key<1||key>keyCeiling(kind)||payload<0||payload>payloadCeiling(kind))
            throw new IllegalArgumentException("Native record exceeds original Scope format ceiling");
    }
    /** Actual Native KV class ceilings; registry evidence remains held future. */
    public CapacityVector nativeBudget(TargetSloControlScopeHeader header,long n,long s,long w,long g){
        requireHeader(header);
        if(n<0||n>maximumSourceOccurrences||s<0||s>maximumSamples||w<0||w>maximumWriterEpochs||g<0||g>maximumGenerations)
            throw new IllegalArgumentException("Scope lifetime counters exceed frozen limits");
        CapacityVector result=multiply(recordCeiling(NativeClass.FIRST).add(recordCeiling(NativeClass.POSITION)),n);
        result=result.add(multiply(recordCeiling(NativeClass.SAMPLE_PRIMARY).add(multiply(recordCeiling(NativeClass.SAMPLE_PROJECTION),3)),s));
        result=result.add(multiply(recordCeiling(NativeClass.WRITER_EPOCH),w)).add(multiply(recordCeiling(NativeClass.GENERATION),g));
        if(g>0)result=result.add(recordCeiling(NativeClass.HEAD));
        final var event=recordCeiling(NativeClass.EVENT);final var custody=recordCeiling(NativeClass.CUSTODY);
        return requireFiniteNative(result.add(multiply(event.amount(CapacityDimension.LOGICAL_STATE_BYTES)>=custody.amount(CapacityDimension.LOGICAL_STATE_BYTES)?event:custody,n)));
    }
    public void requireConvertedNative(TargetSloControlScopeHeader header,CapacityVector converted,long n,long s,long w,long g){
        if(!settlementEnabled()||!nativeBudget(header,n,s,w,g).covers(requireFiniteNative(converted)))
            throw new IllegalArgumentException("converted Native exceeds frozen lifetime record classes");
    }
    public CapacityVector futureNative(TargetSloControlScopeHeader header){
        CapacityVector future=nativeBudget(header,maximumSourceOccurrences,maximumSamples,maximumWriterEpochs,maximumGenerations);
        final var c=effectiveCeilings();final var a=accounting();final long prefix=Bytes.utf8(header.registryKeyPrefix()).length;
        final long base=Math.addExact(prefix,74);
        final long root=exposureFormat==null
                ?Math.addExact(Math.addExact(c.registryRootFixedBytes(),header.canonicalBytes().length),Math.multiplyExact((long)header.limits().maximumSlots(),c.registrySlotBytes()))
                :exposureFormat.maximumRootReservation(header,exposurePolicy);
        future=future.add(a.externalEvidenceCharge(Math.addExact(base,5),root));
        future=future.add(a.externalEvidenceCharge(Math.addExact(base,6),Math.addExact(header.canonicalBytes().length,c.registryWrapperBytes())));
        future=future.add(multiply(a.externalEvidenceCharge(Math.addExact(prefix,75),c.registryWrapperBytes()),header.limits().maximumSlots()));
        final long[] payload=new long[CapacityDimension.COUNT];payload[CapacityDimension.EVIDENCE_BYTES.wireValue()-1]=header.limits().maximumEntriesBytes();
        return requireFiniteNative(future.add(new CapacityVector(payload)));
    }
    static CapacityVector multiply(CapacityVector vector,long count){
        if(count<0)throw new IllegalArgumentException("negative Scope count");final long[] amounts=vector.amounts();
        for(int i=0;i<amounts.length;i++)amounts[i]=Math.multiplyExact(amounts[i],count);return new CapacityVector(amounts);
    }
    static CapacityVector requireFiniteNative(CapacityVector vector){
        Objects.requireNonNull(vector);new TargetQuotaUsage(vector,0,0,0,0);
        for(var dimension:CapacityDimension.values())if(vector.amount(dimension)==Long.MAX_VALUE)throw new IllegalArgumentException("Scope Native amounts must be finite");return vector;
    }
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,schemaVersion);CanonicalProtobuf.bytes(out,2,originalRoot.canonicalBytes());CanonicalProtobuf.bytes(out,3,sourceAnchor.canonicalBytes());
        CanonicalProtobuf.uint64(out,4,maximumSourceOccurrences);CanonicalProtobuf.uint64(out,5,maximumSourceFrameBytes);CanonicalProtobuf.uint64(out,6,maximumSamples);
        CanonicalProtobuf.uint32(out,7,maximumWriterEpochs);CanonicalProtobuf.uint32(out,8,maximumGenerations);
        if(settlementEnabled())CanonicalProtobuf.bytes(out,9,ceilings.canonicalBytes());
        if(schemaVersion==EXPOSURE_VERSION){
            CanonicalProtobuf.bytes(out,10,exposureFormat.canonicalBytes());CanonicalProtobuf.bytes(out,11,exposurePolicy.canonicalBytes());
        }
    });}
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{out.writeBytes(fields());CanonicalProtobuf.bytes(out,schemaVersion==EXPOSURE_VERSION?12:settlementEnabled()?10:9,digest);});}
    public static TargetSloScopeNativePlan decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,12,false,"Scope Native plan");
        if(f.isEmpty())throw new IllegalArgumentException("missing Scope plan version");final int version=QueryCodecSupport.uint32(f.getFirst(),1);
        QueryCodecSupport.requireNumbers(f,version==VERSION?new int[]{1,2,3,4,5,6,7,8,9}:version==SETTLEMENT_VERSION
                ?new int[]{1,2,3,4,5,6,7,8,9,10}:new int[]{1,2,3,4,5,6,7,8,9,10,11,12},"Scope Native plan");
        if(encoded.length>maximumCanonicalBytes(version))throw new IllegalArgumentException("oversized Scope plan");
        final var root=QueryCodecSupport.bytes(f.get(1),2);if(root.length>V1_ROOT_BYTES)throw new IllegalArgumentException("Scope Root exceeds original bound");
        final var value=new TargetSloScopeNativePlan(version,TargetQuotaIncarnation.decode(root),TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(2),3)),
                QueryCodecSupport.uint64Bits(f.get(3),4),QueryCodecSupport.uint64Bits(f.get(4),5),QueryCodecSupport.uint64Bits(f.get(5),6),
                QueryCodecSupport.uint32(f.get(6),7),QueryCodecSupport.uint32(f.get(7),8),version==VERSION?null:Ceilings.decode(QueryCodecSupport.bytes(f.get(8),9)),
                version==EXPOSURE_VERSION?ExposureFormat3.decode(QueryCodecSupport.bytes(f.get(9),10)):null,
                version==EXPOSURE_VERSION?TargetScopeExposurePolicy.decode(QueryCodecSupport.bytes(f.get(10),11)):null);
        if(!Arrays.equals(value.digest,QueryCodecSupport.fixed(f.getLast(),version==EXPOSURE_VERSION?12:version==VERSION?9:10,32)))throw new IllegalArgumentException("Scope plan digest mismatch");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Scope Native plan");return value;
    }
    private static int maximumCanonicalBytes(int version){return switch(version){
        case VERSION->V1_MAX_CANONICAL_BYTES;case SETTLEMENT_VERSION->V2_MAX_CANONICAL_BYTES;case EXPOSURE_VERSION->MAX_CANONICAL_BYTES;
        default->throw new IllegalArgumentException("unknown Scope plan schema");};}
    @Override public boolean equals(Object other){return other instanceof TargetSloScopeNativePlan that&&Arrays.equals(canonicalBytes(),that.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(digest);}
}
