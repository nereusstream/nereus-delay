package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable purpose-signed recovery witness. Decoding is never signature, Source adoption or deletion permission. */
public final class TargetRecoveryProof {
    public static final int VERSION=1, ALGORITHM_VERSION=1;
    public static final int MAX_CANONICAL_BYTES=TargetRecoveryProofRef.MAX_PROOF_BYTES;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-recovery-proof\0");
    private static final byte[] SIGNATURE_DOMAIN=Bytes.utf8("nereus-delay-target-recovery-proof-signature\0");
    private final List<CanonicalProtobuf.Reader.Field> fields;
    private final byte[] encoded,digest;
    private final TargetKafkaReceiptMapping mapping;
    private final SystemMutation admission;
    private final PublishEvidence evidence;
    private final OwnerIdentity observer;
    private final TrustedUtcIntervalEvidence observedAt;
    private final AuthorIdentity issuer;

    private TargetRecoveryProof(byte[] bytes){
        fields=TargetCompatibilityCodec.read(bytes,MAX_CANONICAL_BYTES,19,false,"signed recovery proof");
        final int[] numbers=new int[19];for(int i=0;i<numbers.length;i++)numbers[i]=i+1;
        QueryCodecSupport.requireNumbers(fields,numbers,"signed recovery proof");
        if(number(1)!=VERSION||number(9)!=ALGORITHM_VERSION||number(14)==0)throw invalid();
        for(int n:new int[]{7,12,18}){Bytes.requireLength(value(n),32,"recovery proof digest");nonzero(value(n));}
        if(value(19).length!=64)throw invalid();
        mapping=TargetKafkaReceiptMapping.decode(value(2));admission=SystemMutation.decodeTargetFrame(value(3));
        mapping.requireOriginal(admission,mapping.mutation().source());
        evidence=PublishEvidence.decode(value(4));
        observer=OwnerIdentity.decode(value(5));observedAt=TrustedUtcIntervalEvidence.decode(value(6));
        issuer=AuthorIdentity.decode(value(13));if(issuer.kind()!=AuthorIdentity.Kind.SERVICE)throw invalid();
        if(!sourceCommit().matches("[0-9a-f]{40}"))throw invalid();
        final var capability=ProfileSemanticEnvelope.decode(value(15));
        if(!capability.ref().equals(mapping.admission().publication().capabilityProfile())
                ||!(capability.body() instanceof DeliveryCapabilitySemantic semantic)
                ||semantic.adapterKind()!=AdapterKind.KAFKA||semantic.outcomeCapability()!=OutcomeCapability.KAFKA_TRANSACTIONAL_RECEIPT)throw invalid();
        final var attempt=attempt();final var frozen=frozenOwner();final var body=mapping.admission();
        if(!attempt.primaryIdentity().equals(frozen.identity())||!attempt.accounting().equals(frozen.accounting())
                ||!Arrays.equals(attempt.tenantScope(),value(12))||!Arrays.equals(frozen.tenantScope(),value(12))
                ||!Arrays.equals(attempt.recoveryLineage(),mapping.recoveryLineage())||!Arrays.equals(frozen.recoveryLineage(),mapping.recoveryLineage())
                ||!Arrays.equals(attempt.publishAttemptId(),body.publishAttemptId())||!Arrays.equals(attempt.admissionDigest(),mapping.mutationHash())
                ||!attempt.locator().equals(body.locator())||!attempt.allocated().equals(body.allocated())||!attempt.commitment().equals(body.commitment())
                ||attempt.executionBytes()!=body.executionBytes()||attempt.resolvedAt()!=null
                ||attempt.phase()!=TargetQuotaAttemptBudget.Phase.ADMITTED&&attempt.phase()!=TargetQuotaAttemptBudget.Phase.UNKNOWN)throw invalid();
        frozen.latestMutation().requireAtOrBefore(mapping.mutation());
        if(evidence.verificationStatus()==EvidenceVerificationStatus.VERIFIED_PUBLISHED)TargetKafkaReceiptWire.requirePublished(mapping,evidence);
        else TargetKafkaReceiptWire.requireNotPublished(mapping,evidence);
        requireObservation();
        digest=Bytes.sha256(DOMAIN,canonical(17));
        if(!Arrays.equals(digest,value(18))||!Arrays.equals(bytes,canonical(19)))throw invalid();
        encoded=Bytes.copy(bytes);
    }
    public static TargetRecoveryProof decode(byte[] bytes){return new TargetRecoveryProof(Objects.requireNonNull(bytes));}

    /** Data encoder only; the privileged signer accepts a private actual native observation, not these caller values. */
    public static byte[] unsigned(TargetKafkaReceiptMapping mapping,SystemMutation admission,PublishEvidence evidence,
            OwnerIdentity observer,TrustedUtcIntervalEvidence at,byte[] sdkJar,String sourceCommit,
            byte[] observation,byte[] publicMaterial,byte[] tenant,AuthorIdentity issuer,int keyVersion,ProfileSemanticEnvelope capability,
            TargetQuotaAttemptBudget attempt,TargetQuotaIncarnation frozenOwner){
        final byte[] core=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,VERSION);CanonicalProtobuf.bytes(out,2,mapping.canonicalBytes());
            CanonicalProtobuf.bytes(out,3,admission.encodeFrame());CanonicalProtobuf.bytes(out,4,evidence.canonicalBytes());
            CanonicalProtobuf.bytes(out,5,observer.canonicalBytes());CanonicalProtobuf.bytes(out,6,at.canonicalBytes());
            CanonicalProtobuf.bytes(out,7,sdkJar);CanonicalProtobuf.bytes(out,8,Bytes.utf8(sourceCommit));
            CanonicalProtobuf.uint32(out,9,ALGORITHM_VERSION);CanonicalProtobuf.bytes(out,10,observation);
            CanonicalProtobuf.bytes(out,11,publicMaterial);CanonicalProtobuf.bytes(out,12,tenant);
            CanonicalProtobuf.bytes(out,13,issuer.canonicalBytes());CanonicalProtobuf.uint32Bits(out,14,keyVersion);
            CanonicalProtobuf.bytes(out,15,capability.canonicalBytes());
            CanonicalProtobuf.bytes(out,16,attempt.canonicalBytes());CanonicalProtobuf.bytes(out,17,frozenOwner.canonicalBytes());
        });
        // A zero signature validates shape only. It is never accepted by the proof trust verifier.
        assemble(core,new byte[64]);return core;
    }
    public static TargetRecoveryProof assemble(byte[] unsigned,byte[] signature){
        if(unsigned.length==0||unsigned.length>MAX_CANONICAL_BYTES-102||signature.length!=64)throw invalid();
        final byte[] result=CanonicalProtobuf.message(out->{out.writeBytes(unsigned);
            CanonicalProtobuf.bytes(out,18,Bytes.sha256(DOMAIN,unsigned));CanonicalProtobuf.bytes(out,19,signature);});
        return decode(result);
    }
    public static byte[] signingDigest(byte[] unsigned){
        final var f=QueryCodecSupport.read(unsigned,"unsigned recovery proof");
        final int[] expected=new int[17];for(int i=0;i<17;i++)expected[i]=i+1;
        QueryCodecSupport.requireNumbers(f,expected,"unsigned recovery proof");
        return Bytes.sha256(SIGNATURE_DOMAIN,Bytes.sha256(DOMAIN,unsigned),Bytes.u32beBits(QueryCodecSupport.uint32(f.get(13),14)));
    }
    public byte[] signingDigest(){return signingDigest(canonical(17));}
    public byte[] signature(){return Bytes.copy(value(19));}
    public int signingKeyVersion(){return QueryCodecSupport.uint32(fields.get(13),14);}
    public AuthorIdentity issuer(){return issuer;}
    public TargetKafkaReceiptMapping mapping(){return mapping;}
    public SystemMutation admission(){return admission;}
    public PublishEvidence evidence(){return evidence;}
    public OwnerIdentity observer(){return observer;}
    public TrustedUtcIntervalEvidence observedAt(){return observedAt;}
    public byte[] tenantScope(){return Bytes.copy(value(12));}
    public byte[] sdkJar(){return Bytes.copy(value(7));}
    public String sourceCommit(){return new String(value(8),StandardCharsets.US_ASCII);}
    public byte[] publicMaterial(){return Bytes.copy(value(11));}
    /** Original credential/material authorization data, not the current observer or retirement purpose grant. */
    public byte[] originalAuthorizationDigest(){
        final var material=QueryCodecSupport.read(value(11),"archived public recovery material");
        QueryCodecSupport.requireNumbers(material,new int[]{1,2,3,4,5,6,7},"archived public recovery material");
        return com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.decode(QueryCodecSupport.nested(material.get(1),2)).digest();
    }
    public TargetQuotaAttemptBudget attempt(){return TargetQuotaAttemptBudget.decode(value(16));}
    public TargetQuotaIncarnation frozenOwner(){return TargetQuotaIncarnation.decode(value(17));}
    public byte[] observation(){return Bytes.copy(value(10));}
    public byte[] digest(){return Bytes.copy(digest);}
    public byte[] canonicalBytes(){return Bytes.copy(encoded);}
    public TargetRecoveryProofRef reference(String exactKey){
        return new TargetRecoveryProofRef(exactKey,digest,Bytes.sha256(encoded),encoded.length,mapping.mappingId(),evidence.evidenceId(),mapping.admission().publishAttemptId());
    }
    private byte[] value(int n){return QueryCodecSupport.bytes(fields.get(n-1),n);}
    private long number(int n){return QueryCodecSupport.uint64Bits(fields.get(n-1),n);}
    private byte[] canonical(int through){
        return CanonicalProtobuf.message(out->{for(int n=1;n<=through;n++){
            if(n==1||n==9||n==14)CanonicalProtobuf.uint32Bits(out,n,QueryCodecSupport.uint32(fields.get(n-1),n));
            else CanonicalProtobuf.bytes(out,n,value(n));
        }});
    }
    private void requireObservation(){
        final var o=QueryCodecSupport.read(value(10),"actual recovery observation");
        final boolean published=evidence.verificationStatus()==EvidenceVerificationStatus.VERIFIED_PUBLISHED;
        final int fixed=published?5:10;
        if(o.size()<fixed)throw invalid();
        final int[] first=new int[fixed];for(int n=0;n<fixed;n++)first[n]=n+1;
        QueryCodecSupport.requireNumbers(o.subList(0,fixed),first,"actual recovery observation");
        if(QueryCodecSupport.uint(o.get(0),1)!=(published?3:4)||!Arrays.equals(QueryCodecSupport.bytes(o.get(1),2),mapping.mappingId()))throw invalid();
        final var fence=QueryCodecSupport.read(QueryCodecSupport.nested(o.get(2),3),"original typed recovery fence");
        QueryCodecSupport.requireNumbers(fence,new int[]{1,2,3,4,5,6,7,8,9},"original typed recovery fence");
        if(QueryCodecSupport.uint(fence.get(0),1)!=2||!Arrays.equals(QueryCodecSupport.bytes(fence.get(1),2),mapping.mappingId())
                ||!Arrays.equals(QueryCodecSupport.bytes(fence.get(2),3),mapping.admission().channel().canonicalBytes())
                ||!Arrays.equals(QueryCodecSupport.bytes(fence.get(3),4),observer.canonicalBytes())
                ||!Arrays.equals(QueryCodecSupport.bytes(fence.get(4),5),value(7))
                ||!Arrays.equals(QueryCodecSupport.bytes(fence.get(5),6),value(8)))throw invalid();
        nonzero(QueryCodecSupport.fixed(fence.get(6),7,16));
        final var context=QueryCodecSupport.read(QueryCodecSupport.nested(fence.get(8),9),"actual pending Store context");
        QueryCodecSupport.requireNumbers(context,new int[]{1,2,3,4,5,6,7},"actual pending Store context");
        final var store=com.nereusstream.delay.store.StoreMetadata.decode(QueryCodecSupport.bytes(context.get(1),2));
        final var cut=SourcePositionCodec.decode(QueryCodecSupport.nested(context.get(2),3));
        if(QueryCodecSupport.uint(context.get(0),1)!=1||store.storeFormatVersion()!=2||!store.shardId().equals(mapping.admission().shard())
                ||!cut.shardId().equals(mapping.admission().shard())
                ||cut.compareTo(mapping.mutation().source())<0||!Arrays.equals(QueryCodecSupport.fixed(context.get(5),6,32),Bytes.sha256(mapping.canonicalBytes())))throw invalid();
        QueryCodecSupport.uint64Bits(context.get(3),4);QueryCodecSupport.uint64Bits(context.get(4),5);QueryCodecSupport.fixed(context.get(6),7,32);
        if(published){
            if(!Arrays.equals(QueryCodecSupport.bytes(o.get(3),4),evidence.canonicalBytes()))throw invalid();
            int index=5;while(index<o.size()&&o.get(index).number()==6)index++;
            if(o.size()!=index+3)throw invalid();
            final var folded=requireTranscript(o.subList(5,index),6);
            final var fetch=requireFetch(QueryCodecSupport.nested(o.get(4),5));
            final var cursor=TargetKafkaReceiptWire.cursor(mapping,evidence);
            final long recordTime=QueryCodecSupport.uint(o.get(index),7);
            final long offset=QueryCodecSupport.uint(QueryCodecSupport.read(evidence.branch(),"published receipt offset").get(1),2);
            final long firstOffset=QueryCodecSupport.uint64Bits(fetch.get(11),12),lastOffset=QueryCodecSupport.uint64Bits(fetch.get(12),13);
            if(QueryCodecSupport.uint(fetch.get(10),11)>folded.next||cursor.nextOffsetExclusive()<=folded.next
                    ||cursor.nextOffsetExclusive()>QueryCodecSupport.uint(fetch.get(14),15)
                    ||cursor.lastObservedLsoExclusive()!=QueryCodecSupport.uint(fetch.get(14),15)
                    ||cursor.nextOffsetExclusive()!=Math.addExact(offset,1)||firstOffset<0||lastOffset<firstOffset||offset<firstOffset||offset>lastOffset
                    ||cursor.maxBrokerPersistedAtThroughCursor()<Math.max(folded.time,recordTime))throw invalid();
            if(!Arrays.equals(QueryCodecSupport.bytes(o.get(index+1),8),TargetKafkaReceiptWire.key(mapping))
                    ||!Arrays.equals(QueryCodecSupport.bytes(o.get(index+2),9),TargetKafkaReceiptWire.value(mapping)))throw invalid();
        }else{
            final long start=QueryCodecSupport.uint(o.get(3),4),upper=QueryCodecSupport.uint(o.get(4),5),next=QueryCodecSupport.uint(o.get(5),6),lso=QueryCodecSupport.uint(o.get(6),7);
            final var cursor=TargetKafkaReceiptWire.cursor(mapping,evidence);
            if(start!=mapping.admission().kafkaReceipt().scanStartOffset()||upper<start||next<upper||lso<next
                    ||cursor.nextOffsetExclusive()!=upper||cursor.lastObservedLsoExclusive()!=lso
                    ||QueryCodecSupport.uint(o.get(8),9)<1||!Arrays.equals(QueryCodecSupport.bytes(o.get(9),10),evidence.canonicalBytes()))throw invalid();
            final var folded=requireTranscript(o.subList(10,o.size()),11);
            if(folded.count!=QueryCodecSupport.uint(o.get(8),9)||folded.next!=next||folded.firstHw!=upper
                    ||folded.lastLso!=lso||folded.time!=cursor.maxBrokerPersistedAtThroughCursor()
                    ||!Arrays.equals(folded.digest,QueryCodecSupport.fixed(o.get(7),8,32)))throw invalid();
        }
        final var material=QueryCodecSupport.read(value(11),"archived public recovery material");
        QueryCodecSupport.requireNumbers(material,new int[]{1,2,3,4,5,6,7},"archived public recovery material");
        if(QueryCodecSupport.uint(material.get(0),1)!=1)throw invalid();
        final var a=com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.decode(QueryCodecSupport.nested(material.get(1),2));
        a.requireMapping(mapping);a.requireRuntimeScope(observer,value(12));a.requireTime(observedAt);
        final var projection=QueryCodecSupport.read(QueryCodecSupport.nested(fence.get(7),8),"fence recovery material scope");
        QueryCodecSupport.requireNumbers(projection,new int[]{1,2,3,4,5,6,7,8,9,10},"fence recovery material scope");
        final byte[] expected=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,a.digest());CanonicalProtobuf.bytes(out,3,a.scopeImageDigest());
            CanonicalProtobuf.bytes(out,4,a.evidenceDigest());CanonicalProtobuf.bytes(out,5,a.bindingId());CanonicalProtobuf.uint64Bits(out,6,a.credentialGeneration());
            CanonicalProtobuf.bytes(out,7,a.referenceDigest());CanonicalProtobuf.bytes(out,8,a.fingerprint());CanonicalProtobuf.bytes(out,9,a.principalHash());CanonicalProtobuf.bytes(out,10,a.policyDigest());
        });
        QueryCodecSupport.requireCanonical(QueryCodecSupport.bytes(fence.get(7),8),expected,"fence recovery material scope");
    }
    private record Fold(long next,long time,long firstHw,long lastLso,int count,byte[] digest){}
    private Fold requireTranscript(List<CanonicalProtobuf.Reader.Field> entries,int number){
        long next=mapping.admission().kafkaReceipt().scanStartOffset(),time=0,firstHw=-1,lastLso=-1;
        int count=0;byte[] digest=Bytes.sha256(Bytes.utf8("nereus-delay-target-receipt-scan\0"));
        for(var entry:entries){
            final byte[] bytes=QueryCodecSupport.nested(entry,number);final var s=QueryCodecSupport.read(bytes,"processed recovery segment");
            QueryCodecSupport.requireNumbers(s,new int[]{1,2,3,4},"processed recovery segment");
            final var fetch=requireFetch(QueryCodecSupport.nested(s.get(2),3));
            final long start=QueryCodecSupport.uint(s.get(0),1),after=QueryCodecSupport.uint(s.get(1),2),foldTime=QueryCodecSupport.uint(s.get(3),4);
            if(start!=next||after<start||QueryCodecSupport.uint(fetch.get(10),11)>start
                    ||after>QueryCodecSupport.uint(fetch.get(14),15)||foldTime<time)throw invalid();
            if(count==0)firstHw=QueryCodecSupport.uint(fetch.get(13),14);
            next=after;time=foldTime;lastLso=QueryCodecSupport.uint(fetch.get(14),15);count++;
            digest=Bytes.sha256(digest,Bytes.lp32(bytes));
        }
        return new Fold(next,time,firstHw,lastLso,count,digest);
    }
    private List<CanonicalProtobuf.Reader.Field> requireFetch(byte[] encoded){
        final var f=QueryCodecSupport.read(encoded,"actual guarded recovery Fetch");
        final int[] numbers=new int[18];for(int i=0;i<18;i++)numbers[i]=i+1;
        QueryCodecSupport.requireNumbers(f,numbers,"actual guarded recovery Fetch");
        final var r=mapping.admission().kafkaReceipt();final var id=r.receiptResource().receiptTopicUuid();
        final byte[] uuid=ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
        if(QueryCodecSupport.uint(f.get(0),1)!=2||!Arrays.equals(QueryCodecSupport.bytes(f.get(1),2),value(8))
                ||!Arrays.equals(QueryCodecSupport.bytes(f.get(2),3),Bytes.utf8(r.receiptResource().authenticatedClusterId()))
                ||!Arrays.equals(QueryCodecSupport.bytes(f.get(3),4),Bytes.utf8(r.receiptPhysicalTopic()))
                ||!Arrays.equals(QueryCodecSupport.fixed(f.get(4),5,16),uuid)||QueryCodecSupport.uint(f.get(5),6)!=r.receiptPartition()
                ||!Arrays.equals(QueryCodecSupport.fixed(f.get(17),18,32),value(7)))throw invalid();
        QueryCodecSupport.fixed(f.get(16),17,32);
        final long hw=QueryCodecSupport.uint(f.get(13),14),lso=QueryCodecSupport.uint(f.get(14),15),logStart=QueryCodecSupport.uint(f.get(15),16);
        if(hw<lso||lso<logStart||logStart>r.scanStartOffset())throw invalid();
        QueryCodecSupport.uint32(f.get(6),7);QueryCodecSupport.uint32(f.get(7),8);
        QueryCodecSupport.uint64Bits(f.get(8),9);QueryCodecSupport.uint64Bits(f.get(9),10);
        QueryCodecSupport.uint64Bits(f.get(11),12);QueryCodecSupport.uint64Bits(f.get(12),13);
        return f;
    }
    private static void nonzero(byte[] bytes){for(byte value:bytes)if(value!=0)return;throw invalid();}
    private static IllegalArgumentException invalid(){return new IllegalArgumentException("invalid bounded exact recovery proof");}
}
