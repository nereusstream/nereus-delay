package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.TargetRecoveryProofTrust;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Action;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.BoundedReadBudget;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.*;
import java.util.function.UnaryOperator;

/** One bounded Registry capacity/management/publication CAS record and immutable external objects; no distributed-atomic claim. */
public final class OxiaSyncTargetKafkaReceiptProtectionCatalog {
    public static final int INSTALLING = 1, ACTIVE = 2, BROKEN = 3;
    private static final int RESERVED = 1, PUBLISHED = 2, POSSIBLY_SOURCE = 3, ADOPTED = 4;
    private static final int MAX_ROOT_BYTES = 16 * 1024 * 1024;
    private static final int MAX_CAS_ATTEMPTS = 16;
    private final SyncOxiaClient client;
    private final Runnable session;
    private final String prefix;
    private final String ledgerKey;
    private final Limits limits;
    private final TargetKafkaReceiptProtectionTrust trust;

    /** Control-storage physical budget, separate from Target logical EVIDENCE and system-writer append reservations. */
    public record Limits(long physicalBytes, int physicalRecords, int maximumPublications, int maximumTopics,
            int maximumPreparedReferences, byte[] capacitySourceDigest) {
        public Limits {
            Bytes.requireLength(capacitySourceDigest, 32, "configured durable control-storage budget source");
            capacitySourceDigest = Bytes.copy(capacitySourceDigest);
            if (physicalBytes <= 0 || physicalBytes == Long.MAX_VALUE || physicalRecords < 2
                    || maximumPublications < 1 || maximumPublications > 4096 || maximumTopics < 1 || maximumTopics > 64
                    || maximumPreparedReferences < 1 || maximumPreparedReferences > 128
                    || rootReserve(maximumPublications, maximumTopics, maximumPreparedReferences) > MAX_ROOT_BYTES
                    || physicalBytes < rootReserve(maximumPublications, maximumTopics, maximumPreparedReferences)) {
                throw new IllegalArgumentException("receipt catalog requires finite control-storage/root bounds");
            }
        }
        @Override public byte[] capacitySourceDigest() { return Bytes.copy(capacitySourceDigest); }
        public long rootReservedBytes() { return rootReserve(maximumPublications, maximumTopics, maximumPreparedReferences); }
        private static long rootReserve(int publications, int topics, int references) {
            return Math.addExact(4096L, Math.addExact(Math.multiplyExact((long) topics,
                    TargetKafkaReceiptProtection.MAX_POLICY_BYTES + TargetKafkaReceiptProtection.MAX_MANAGEMENT_BYTES + 512L),
                    Math.multiplyExact((long) publications, Math.addExact(4096L, Math.multiplyExact((long) references, 128L)))));
        }
        public static Limits load(java.util.Properties properties, String keyPrefix) {
            return new Limits(Long.parseLong(required(properties, keyPrefix + ".physicalBytes")),
                    Integer.parseInt(required(properties, keyPrefix + ".physicalRecords")),
                    Integer.parseInt(required(properties, keyPrefix + ".maximumPublications")),
                    Integer.parseInt(required(properties, keyPrefix + ".maximumTopics")),
                    Integer.parseInt(required(properties, keyPrefix + ".maximumPreparedReferences")),
                    java.util.HexFormat.of().parseHex(required(properties, keyPrefix + ".capacitySourceDigest")));
        }
        private static String required(java.util.Properties properties, String key) {
            return Objects.requireNonNull(properties.getProperty(key), "missing explicit receipt catalog configuration: " + key);
        }
    }
    public record Management(TargetKafkaReceiptControlArtifact artifact, int state, long revision, byte[] policy,
            byte[] failureDigest, byte[] failureSignature, byte[] installationId) {
        public Management {
            Bytes.requireLength(installationId, 16, "original management installation ID");
            installationId = Bytes.copy(installationId);
            if (artifact.kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT || state < INSTALLING || state > BROKEN
                    || revision <= 0 || policy.length > TargetKafkaReceiptProtection.MAX_POLICY_BYTES
                    || state == ACTIVE && policy.length == 0
                    || (state == BROKEN ? failureDigest.length != 32 || failureSignature.length != 64
                            : failureDigest.length != 0 || failureSignature.length != 0)) {
                throw new IllegalArgumentException("invalid persistent receipt management state");
            }
            policy = Bytes.copy(policy); failureDigest = Bytes.copy(failureDigest); failureSignature = Bytes.copy(failureSignature);
        }
        @Override public byte[] installationId() { return Bytes.copy(installationId); }
        @Override public byte[] failureDigest() { return Bytes.copy(failureDigest); }
        @Override public byte[] failureSignature() { return Bytes.copy(failureSignature); }
        @Override public byte[] policy() { return Bytes.copy(policy); }
        public TargetKafkaReceiptManagementImage image() { return TargetKafkaReceiptManagementImage.decode(artifact.image()); }
        String id() { return Bytes.hex(image().topicIdentity()); }
        byte[] bytes() {
            return CanonicalProtobuf.message(o -> {
                CanonicalProtobuf.bytes(o,1,artifact.canonicalBytes()); CanonicalProtobuf.uint32(o,2,state);
                CanonicalProtobuf.uint64(o,3,revision); CanonicalProtobuf.bytes(o,4,policy);
                CanonicalProtobuf.bytes(o,5,failureDigest); CanonicalProtobuf.bytes(o,6,failureSignature); CanonicalProtobuf.bytes(o,7,installationId);
            });
        }
        static Management decode(byte[] bytes) {
            final var f = fields(bytes, new int[] {1,2,3,4,5,6,7}, "receipt management state");
            final var m = new Management(TargetKafkaReceiptControlArtifact.decode(QueryCodecSupport.nested(f.get(0),1)),
                    QueryCodecSupport.uint32(f.get(1),2),QueryCodecSupport.uint(f.get(2),3),QueryCodecSupport.bytes(f.get(3),4),
                    QueryCodecSupport.bytes(f.get(4),5),QueryCodecSupport.bytes(f.get(5),6),QueryCodecSupport.fixed(f.get(6),7,16));
            QueryCodecSupport.requireCanonical(bytes,m.bytes(),"receipt management state"); return m;
        }
    }
    private record PreparedUse(byte[] id, byte[] hash, long retryUntil) {
        PreparedUse { Bytes.requireLength(id,32,"prepared Admission ID"); Bytes.requireLength(hash,32,"prepared Admission hash");
            if(retryUntil<0)throw new IllegalArgumentException("negative Admission retry scope"); id=Bytes.copy(id);hash=Bytes.copy(hash); }
        byte[] bytes(){return CanonicalProtobuf.message(o->{CanonicalProtobuf.bytes(o,1,id);CanonicalProtobuf.bytes(o,2,hash);CanonicalProtobuf.int64(o,3,retryUntil);});}
        static PreparedUse decode(byte[] b){final var f=fields(b,new int[]{1,2,3},"prepared receipt use");return new PreparedUse(QueryCodecSupport.fixed(f.get(0),1,32),QueryCodecSupport.fixed(f.get(1),2,32),QueryCodecSupport.uint(f.get(2),3));}
    }
    /** Immutable copy of the exact durable possible-Source window, not a terminal-result proof. */
    public record ClosedPreparedUse(byte[] id,byte[] hash,long retryUntil){
        public ClosedPreparedUse{Bytes.requireLength(id,32,"sealed Source ID");Bytes.requireLength(hash,32,"sealed Source hash");
            if(retryUntil<0)throw new IllegalArgumentException("negative sealed retry deadline");id=Bytes.copy(id);hash=Bytes.copy(hash);}
        @Override public byte[] id(){return Bytes.copy(id);}
        @Override public byte[] hash(){return Bytes.copy(hash);}
    }
    /** Complete candidate inventory at the closure cut; each candidate still needs actual dependency/lifetime classification. */
    public record CatalogPublication(int subtype,byte[] id){
        public CatalogPublication{if(subtype<1||subtype>3)throw new IllegalArgumentException("unknown catalog publication subtype");
            Bytes.requireLength(id,32,"catalog publication ID");id=Bytes.copy(id);}
        @Override public byte[] id(){return Bytes.copy(id);}
        private String order(){return subtype+":"+Bytes.hex(id);}
        private byte[] encode(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,subtype);CanonicalProtobuf.bytes(out,2,id);});}
        private static CatalogPublication decode(byte[] value){final var f=fields(value,new int[]{1,2},"closure inventory entry");
            return new CatalogPublication(QueryCodecSupport.uint32(f.get(0),1),QueryCodecSupport.fixed(f.get(1),2,32));}
    }
    private record Closure(int subtype,String key,byte[] objectDigest,byte[] valueDigest,long valueBytes,long blobVersion,
            byte[] publicationId,byte[] originDigest,byte[] usesDigest,int useCount,long revision,byte[] namespaceId,
            int capturedPhase,byte[] capturedAdoptedSource,List<CatalogPublication> inventory,byte[] inventoryDigest,byte[] rootCutDigest,byte[] managementTopicId){
        Closure{
            if(subtype<1||subtype>3||key.isBlank()||key.indexOf('\0')>=0||!Normalizer.isNormalized(key,Normalizer.Form.NFC)
                    ||Bytes.utf8(key).length>TargetRecoveryProofRef.MAX_KEY_BYTES||valueBytes<1
                    ||valueBytes>(subtype==3?TargetRecoveryProofRef.MAX_PROOF_BYTES:TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES)
                    ||useCount<0||useCount>128||revision==0||capturedPhase<RESERVED||capturedPhase>ADOPTED
                    ||capturedAdoptedSource.length!=0&&capturedAdoptedSource.length!=32)
                throw new IllegalArgumentException("invalid exact object closure");
            for(byte[] digest:List.of(objectDigest,valueDigest,publicationId,originDigest,usesDigest,namespaceId,inventoryDigest,rootCutDigest,managementTopicId))
                Bytes.requireLength(digest,32,"closure exact digest/identity");
            objectDigest=Bytes.copy(objectDigest);valueDigest=Bytes.copy(valueDigest);publicationId=Bytes.copy(publicationId);
            originDigest=Bytes.copy(originDigest);usesDigest=Bytes.copy(usesDigest);namespaceId=Bytes.copy(namespaceId);
            capturedAdoptedSource=Bytes.copy(capturedAdoptedSource);inventoryDigest=Bytes.copy(inventoryDigest);rootCutDigest=Bytes.copy(rootCutDigest);
            managementTopicId=Bytes.copy(managementTopicId);
            inventory=List.copyOf(inventory);String previous=null;
            for(var entry:inventory){if(previous!=null&&previous.compareTo(entry.order())>=0)throw new IllegalArgumentException("closure inventory is not unique/sorted");previous=entry.order();}
            requireExact(inventoryDigest,OxiaSyncTargetKafkaReceiptProtectionCatalog.inventoryDigest(inventory));
        }
        byte[] encode(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,subtype);CanonicalProtobuf.bytes(out,3,Bytes.utf8(key));
            CanonicalProtobuf.bytes(out,4,objectDigest);CanonicalProtobuf.bytes(out,5,valueDigest);CanonicalProtobuf.uint64(out,6,valueBytes);
            CanonicalProtobuf.uint64Bits(out,7,blobVersion);CanonicalProtobuf.bytes(out,8,publicationId);CanonicalProtobuf.bytes(out,9,originDigest);
            CanonicalProtobuf.bytes(out,10,usesDigest);CanonicalProtobuf.uint32(out,11,useCount);CanonicalProtobuf.uint64Bits(out,12,revision);
            CanonicalProtobuf.bytes(out,13,namespaceId);CanonicalProtobuf.uint32(out,14,capturedPhase);CanonicalProtobuf.bytes(out,15,capturedAdoptedSource);
            for(var entry:inventory)CanonicalProtobuf.bytes(out,16,entry.encode());
            CanonicalProtobuf.bytes(out,17,inventoryDigest);CanonicalProtobuf.bytes(out,18,rootCutDigest);CanonicalProtobuf.bytes(out,19,managementTopicId);
        });}
        byte[] digest(){return Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-reference-closure\0"),encode());}
        void requireFingerprint(String actualKey,byte[] actualDigest,byte[] actualValueDigest,long actualBytes){
            if(!key.equals(actualKey)||valueBytes!=actualBytes)throw new IllegalStateException("closed object changed its exact key/length");
            requireExact(objectDigest,actualDigest);requireExact(valueDigest,actualValueDigest);
        }
        static Closure decode(byte[] value){
            final var f=QueryCodecSupport.read(value,"exact object closure");if(f.size()<18)throw new IllegalArgumentException("incomplete object closure");
            QueryCodecSupport.requireNumbers(f.subList(0,15),new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15},"object closure");
            if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown object closure schema");
            int n=15;final var inventory=new ArrayList<CatalogPublication>();
            while(n<f.size()&&f.get(n).number()==16)inventory.add(CatalogPublication.decode(QueryCodecSupport.nested(f.get(n++),16)));
            if(n+3!=f.size())throw new IllegalArgumentException("unknown object closure fields");
            final var result=new Closure(QueryCodecSupport.uint32(f.get(1),2),new String(QueryCodecSupport.bytes(f.get(2),3),StandardCharsets.UTF_8),
                    QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32),QueryCodecSupport.uint(f.get(5),6),
                    QueryCodecSupport.uint64Bits(f.get(6),7),QueryCodecSupport.fixed(f.get(7),8,32),QueryCodecSupport.fixed(f.get(8),9,32),
                    QueryCodecSupport.fixed(f.get(9),10,32),QueryCodecSupport.uint32(f.get(10),11),QueryCodecSupport.uint64Bits(f.get(11),12),
                    QueryCodecSupport.fixed(f.get(12),13,32),QueryCodecSupport.uint32(f.get(13),14),QueryCodecSupport.bytes(f.get(14),15),inventory,
                    QueryCodecSupport.fixed(f.get(n),17,32),QueryCodecSupport.fixed(f.get(n+1),18,32),QueryCodecSupport.fixed(f.get(n+2),19,32));
            QueryCodecSupport.requireCanonical(value,result.encode(),"object closure");return result;
        }
    }
    private record Intent(byte[] id,int kind,String key,byte[] digest,byte[] valueHash,long bytes,byte[] controlId,
            byte[] controlDigest,byte[] shard,TargetQuotaIdentity controlScope,int phase,List<PreparedUse> uses,byte[] adoptedSource,
            byte[] brokenDigest,byte[] brokenSignature) {
        Intent {
            Bytes.requireLength(id,32,"publication ID");Bytes.requireLength(digest,32,"artifact digest");Bytes.requireLength(valueHash,32,"actual blob hash");
            Bytes.requireLength(controlId,32,"CONTROL operation ID");Bytes.requireLength(controlDigest,32,"original CONTROL digest");Bytes.requireLength(shard,20,"Source Shard");
            if(kind!=1&&kind!=2||bytes<=0||bytes>TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES||phase<RESERVED||phase>ADOPTED
                    ||key.getBytes(StandardCharsets.UTF_8).length>TargetExternalEvidenceOwner.MAX_KEY_BYTES
                    ||controlScope.kind()!=TargetQuotaIdentity.Kind.TARGET||adoptedSource.length!=0&&adoptedSource.length!=32
                    ||brokenDigest.length!=0&&brokenDigest.length!=32||brokenSignature.length!=0&&brokenSignature.length!=64
                    ||(brokenDigest.length==0)!=(brokenSignature.length==0))throw new IllegalArgumentException("invalid bounded receipt publication intent");
            uses=List.copyOf(uses);id=Bytes.copy(id);digest=Bytes.copy(digest);valueHash=Bytes.copy(valueHash);
            String previous=null;for(var use:uses){final String order=Bytes.hex(use.id);if(previous!=null&&previous.compareTo(order)>=0)
                throw new IllegalArgumentException("CONTROL prepared uses are not unique/sorted");previous=order;}
            controlId=Bytes.copy(controlId);controlDigest=Bytes.copy(controlDigest);shard=Bytes.copy(shard);
            adoptedSource=Bytes.copy(adoptedSource);brokenDigest=Bytes.copy(brokenDigest);brokenSignature=Bytes.copy(brokenSignature);
        }
        long physicalBytes(){return Math.addExact(bytes,key.getBytes(StandardCharsets.UTF_8).length);}
        byte[] originDigest(){return Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-publication-origin\0"),CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,kind);CanonicalProtobuf.bytes(out,2,id);CanonicalProtobuf.bytes(out,3,Bytes.utf8(key));
            CanonicalProtobuf.bytes(out,4,digest);CanonicalProtobuf.bytes(out,5,valueHash);CanonicalProtobuf.uint64(out,6,bytes);
            CanonicalProtobuf.bytes(out,7,controlId);CanonicalProtobuf.bytes(out,8,controlDigest);CanonicalProtobuf.bytes(out,9,shard);
            CanonicalProtobuf.bytes(out,10,controlScope.canonicalBytes());
        }));}
        Intent phase(int next){return new Intent(id,kind,key,digest,valueHash,bytes,controlId,controlDigest,shard,controlScope,next,uses,adoptedSource,brokenDigest,brokenSignature);}
        Intent bind(PreparedUse use,int maximum){
            for(var current:uses)if(Arrays.equals(current.id,use.id)){if(!Arrays.equals(current.bytes(),use.bytes()))throw new IllegalStateException("prepared Admission changed exact hash/retry");return this;}
            if(uses.size()>=maximum)throw new IllegalStateException("receipt publication prepared-use bound exhausted");
            final var values=new ArrayList<>(uses);values.add(use);values.sort(Comparator.comparing(u->Bytes.hex(u.id)));
            return new Intent(id,kind,key,digest,valueHash,bytes,controlId,controlDigest,shard,controlScope,phase==ADOPTED?ADOPTED:POSSIBLY_SOURCE,values,adoptedSource,brokenDigest,brokenSignature);
        }
        Intent adopt(byte[] source){return new Intent(id,kind,key,digest,valueHash,bytes,controlId,controlDigest,shard,controlScope,ADOPTED,uses,adoptedSource.length==0?source:adoptedSource,brokenDigest,brokenSignature);}
        byte[] encode(){return CanonicalProtobuf.message(o->{
            CanonicalProtobuf.bytes(o,1,id);CanonicalProtobuf.uint32(o,2,kind);CanonicalProtobuf.bytes(o,3,Bytes.utf8(key));
            CanonicalProtobuf.bytes(o,4,digest);CanonicalProtobuf.bytes(o,5,valueHash);CanonicalProtobuf.uint64(o,6,bytes);
            CanonicalProtobuf.bytes(o,7,controlId);CanonicalProtobuf.bytes(o,8,controlDigest);CanonicalProtobuf.bytes(o,9,shard);
            CanonicalProtobuf.bytes(o,10,controlScope.canonicalBytes());CanonicalProtobuf.uint32(o,11,phase);
            for(var use:uses)CanonicalProtobuf.bytes(o,12,use.bytes());CanonicalProtobuf.bytes(o,13,adoptedSource);
            CanonicalProtobuf.bytes(o,14,brokenDigest);CanonicalProtobuf.bytes(o,15,brokenSignature);
        });}
        static Intent decode(byte[] b){
            final var f=QueryCodecSupport.read(b,"receipt publication intent");if(f.size()<14)throw new IllegalArgumentException("incomplete publication intent");
            final var uses=new ArrayList<PreparedUse>();int n=11;while(n<f.size()&&f.get(n).number()==12)uses.add(PreparedUse.decode(QueryCodecSupport.nested(f.get(n++),12)));
            if(n+3!=f.size())throw new IllegalArgumentException("unknown publication intent fields");
            final var i=new Intent(QueryCodecSupport.fixed(f.get(0),1,32),QueryCodecSupport.uint32(f.get(1),2),new String(QueryCodecSupport.bytes(f.get(2),3),StandardCharsets.UTF_8),
                QueryCodecSupport.fixed(f.get(3),4,32),QueryCodecSupport.fixed(f.get(4),5,32),QueryCodecSupport.uint(f.get(5),6),QueryCodecSupport.fixed(f.get(6),7,32),
                QueryCodecSupport.fixed(f.get(7),8,32),QueryCodecSupport.fixed(f.get(8),9,20),TargetQuotaIdentity.decode(QueryCodecSupport.nested(f.get(9),10)),
                QueryCodecSupport.uint32(f.get(10),11),uses,QueryCodecSupport.bytes(f.get(n),13),QueryCodecSupport.bytes(f.get(n+1),14),QueryCodecSupport.bytes(f.get(n+2),15));
            QueryCodecSupport.requireCanonical(b,i.encode(),"receipt publication intent");return i;
        }
    }
    private record RecoveryIntent(byte[] id,TargetRecoveryProofRef reference,TargetQuotaIncarnation frozenOwner,
            int phase,List<PreparedUse> uses,byte[] adoptedSource){
        RecoveryIntent{
            id=Bytes.copy(id);Bytes.requireLength(id,32,"recovery publication discovery ID");
            if(!Arrays.equals(id,recoveryId(reference.mappingId(),reference.evidenceId()))||phase<RESERVED||phase>ADOPTED
                    ||(phase==ADOPTED?adoptedSource.length!=32:adoptedSource.length!=0)
                    ||(phase<POSSIBLY_SOURCE?!uses.isEmpty():uses.isEmpty())
                    ||frozenOwner.identity().kind()!=TargetQuotaIdentity.Kind.TARGET)throw new IllegalArgumentException("invalid recovery publication intent");
            String previous=null;for(var use:uses){final String key=Bytes.hex(use.id);
                if(previous!=null&&previous.compareTo(key)>=0)throw new IllegalArgumentException("recovery prepared uses are not unique and sorted");previous=key;}
            Objects.requireNonNull(frozenOwner);uses=List.copyOf(uses);adoptedSource=Bytes.copy(adoptedSource);
        }
        byte[] encode(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,id);CanonicalProtobuf.bytes(out,3,reference.canonicalBytes());
            CanonicalProtobuf.bytes(out,4,frozenOwner.canonicalBytes());CanonicalProtobuf.uint32(out,5,phase);
            for(var use:uses)CanonicalProtobuf.bytes(out,6,use.bytes());CanonicalProtobuf.bytes(out,7,adoptedSource);
        });}
        byte[] originDigest(){return Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-publication-origin\0"),CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,3);CanonicalProtobuf.bytes(out,2,id);CanonicalProtobuf.bytes(out,3,reference.canonicalBytes());
            CanonicalProtobuf.bytes(out,4,frozenOwner.canonicalBytes());
        }));}
        long rootReserve(int maximumUses){
            final var empty=new RecoveryIntent(id,reference,frozenOwner,RESERVED,List.of(),new byte[0]);
            // Reserve all future uses and the Source adoption stamp before the first blob write.
            return Math.addExact(empty.encode().length+64L,Math.multiplyExact(128L,maximumUses));
        }
        RecoveryIntent phase(int next){return new RecoveryIntent(id,reference,frozenOwner,next,uses,adoptedSource);}
        RecoveryIntent adopt(byte[] source){
            if(adoptedSource.length!=0)requireExact(adoptedSource,source);
            return new RecoveryIntent(id,reference,frozenOwner,ADOPTED,uses,source);
        }
        RecoveryIntent bind(PreparedUse use,int maximum){
            for(var old:uses)if(Arrays.equals(old.id,use.id)){requireExact(old.bytes(),use.bytes());return this;}
            if(uses.size()>=maximum)throw new IllegalStateException("recovery prepared Source-use window exhausted");
            final var copy=new ArrayList<>(uses);copy.add(use);copy.sort(Comparator.comparing(u->Bytes.hex(u.id)));
            return new RecoveryIntent(id,reference,frozenOwner,phase==ADOPTED?ADOPTED:POSSIBLY_SOURCE,copy,adoptedSource);
        }
        static RecoveryIntent decode(byte[] encoded){
            final var f=QueryCodecSupport.read(encoded,"recovery proof intent");if(f.size()<6)throw new IllegalArgumentException("incomplete recovery intent");
            QueryCodecSupport.requireNumbers(f.subList(0,5),new int[]{1,2,3,4,5},"recovery proof intent");
            if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown recovery intent version");
            int n=5;final var uses=new ArrayList<PreparedUse>();
            while(n<f.size()&&f.get(n).number()==6)uses.add(PreparedUse.decode(QueryCodecSupport.nested(f.get(n++),6)));
            if(n+1!=f.size())throw new IllegalArgumentException("unknown recovery intent fields");
            final var result=new RecoveryIntent(QueryCodecSupport.fixed(f.get(1),2,32),TargetRecoveryProofRef.decode(QueryCodecSupport.nested(f.get(2),3)),
                    TargetQuotaIncarnation.decode(QueryCodecSupport.nested(f.get(3),4)),QueryCodecSupport.uint32(f.get(4),5),uses,QueryCodecSupport.bytes(f.get(n),7));
            QueryCodecSupport.requireCanonical(encoded,result.encode(),"recovery proof intent");return result;
        }
    }
    private record Root(int version,Limits limits,Map<String,Management> topics,Map<String,Intent> publications,Map<String,RecoveryIntent> recoveries,
            Map<String,Closure> closures,long closureSequence) {
        Root(Limits limits,Map<String,Management> topics,Map<String,Intent> publications){this(1,limits,topics,publications,Map.of(),Map.of(),0);}
        Root {topics=Map.copyOf(topics);publications=Map.copyOf(publications);recoveries=Map.copyOf(recoveries);closures=Map.copyOf(closures);
            validate(version,limits,topics,publications,recoveries,closures,closureSequence);}
        Root topic(Management m){final var map=new TreeMap<>(topics);map.put(m.id(),m);return new Root(version,limits,map,publications,recoveries,closures,closureSequence);}
        Root publication(Intent i){final var map=new TreeMap<>(publications);map.put(Bytes.hex(i.id),i);return new Root(version,limits,topics,map,recoveries,closures,closureSequence);}
        Root recovery(RecoveryIntent i){final var map=new TreeMap<>(recoveries);map.put(Bytes.hex(i.id),i);return new Root(Math.max(version,2),limits,topics,publications,map,closures,closureSequence);}
        Root close(Closure closure){final var map=new TreeMap<>(closures);if(map.putIfAbsent(closure.key,closure)!=null)throw new IllegalStateException("object already closed");
            return new Root(3,limits,topics,publications,recoveries,map,closure.revision);}
        long closureReserve(){long bytes=version==3?64:0;for(var closure:closures.values())bytes=Math.addExact(bytes,closure.encode().length+16L);return bytes;}
        long reservedBytes(){long result=limits.rootReservedBytes();for(var i:recoveries.values())
            result=Math.addExact(result,Math.max(0,i.rootReserve(limits.maximumPreparedReferences)-(4096L+128L*limits.maximumPreparedReferences)));return Math.addExact(result,closureReserve());}
        byte[] bytes(){final var b=CanonicalProtobuf.message(o->{
            CanonicalProtobuf.uint32(o,1,version);CanonicalProtobuf.uint64(o,2,limits.physicalBytes);CanonicalProtobuf.uint32(o,3,limits.physicalRecords);
            CanonicalProtobuf.uint32(o,4,limits.maximumPublications);CanonicalProtobuf.uint32(o,5,limits.maximumTopics);CanonicalProtobuf.uint32(o,6,limits.maximumPreparedReferences);
            CanonicalProtobuf.bytes(o,7,limits.capacitySourceDigest);
            for(var m:new TreeMap<>(topics).values())CanonicalProtobuf.bytes(o,8,m.bytes());
            for(var i:new TreeMap<>(publications).values())CanonicalProtobuf.bytes(o,9,i.encode());
            for(var i:new TreeMap<>(recoveries).values())CanonicalProtobuf.bytes(o,10,i.encode());
            if(version==3){for(var closure:new TreeMap<>(closures).values())CanonicalProtobuf.bytes(o,11,closure.encode());CanonicalProtobuf.uint64Bits(o,12,closureSequence);}
        });if(b.length>reservedBytes()||b.length>MAX_ROOT_BYTES)throw new IllegalArgumentException("receipt catalog root exceeds its reserved physical storage");return b;}
        static Root decode(byte[] b){
            if(b.length==0||b.length>MAX_ROOT_BYTES)throw new IllegalArgumentException("invalid receipt capacity root length");
            final var f=QueryCodecSupport.read(b,"receipt capacity root");if(f.size()<7)throw new IllegalArgumentException("incomplete receipt capacity root");
            QueryCodecSupport.requireNumbers(f.subList(0,7),new int[]{1,2,3,4,5,6,7},"receipt capacity root");
            final int version=QueryCodecSupport.uint32(f.get(0),1);if(version<1||version>3)throw new IllegalArgumentException("unknown receipt capacity root version");
            final var l=new Limits(QueryCodecSupport.uint(f.get(1),2),QueryCodecSupport.uint32(f.get(2),3),QueryCodecSupport.uint32(f.get(3),4),QueryCodecSupport.uint32(f.get(4),5),QueryCodecSupport.uint32(f.get(5),6),QueryCodecSupport.fixed(f.get(6),7,32));
            final var topics=new TreeMap<String,Management>();final var intents=new TreeMap<String,Intent>();int n=7;
            while(n<f.size()&&f.get(n).number()==8){var m=Management.decode(QueryCodecSupport.nested(f.get(n++),8));if(topics.put(m.id(),m)!=null)throw new IllegalArgumentException("duplicate managed topic");}
            while(n<f.size()&&f.get(n).number()==9){var i=Intent.decode(QueryCodecSupport.nested(f.get(n++),9));if(intents.put(Bytes.hex(i.id),i)!=null)throw new IllegalArgumentException("duplicate publication ID");}
            final var recovery=new TreeMap<String,RecoveryIntent>();
            if(version>=2)while(n<f.size()&&f.get(n).number()==10){var i=RecoveryIntent.decode(QueryCodecSupport.nested(f.get(n++),10));if(recovery.put(Bytes.hex(i.id),i)!=null)throw new IllegalArgumentException("duplicate recovery discovery ID");}
            final var closures=new TreeMap<String,Closure>();long sequence=0;
            if(version==3){while(n<f.size()&&f.get(n).number()==11){final var c=Closure.decode(QueryCodecSupport.nested(f.get(n++),11));if(closures.put(c.key,c)!=null)throw new IllegalArgumentException("duplicate object closure");}
                if(n>=f.size())throw new IllegalArgumentException("missing closure sequence");sequence=QueryCodecSupport.uint64Bits(f.get(n++),12);}
            if(n!=f.size())throw new IllegalArgumentException("unknown receipt root field");final var root=new Root(version,l,topics,intents,recovery,closures,sequence);QueryCodecSupport.requireCanonical(b,root.bytes(),"receipt capacity root");return root;
        }
        private static void validate(int version,Limits l,Map<String,Management> topics,Map<String,Intent> publications,Map<String,RecoveryIntent> recovery,
                Map<String,Closure> closures,long sequence){
            if(version<1||version>3||version==1&&!recovery.isEmpty()||version<3&&(!closures.isEmpty()||sequence!=0)
                    ||version==3&&(closures.isEmpty()||sequence==0)||closures.size()>publications.size()+recovery.size()
                    ||topics.size()>l.maximumTopics||publications.size()+(long)recovery.size()>l.maximumPublications)throw new IllegalArgumentException("receipt catalog cardinality bound exceeded");
            long bytes=l.rootReservedBytes();int records=1;final var keys=new HashSet<String>();
            for(var i:publications.values()){if(i.uses.size()>l.maximumPreparedReferences||!keys.add(i.key))throw new IllegalArgumentException("duplicate physical blob or prepared-use bound exceeded");bytes=Math.addExact(bytes,i.physicalBytes());records++;}
            long extraRoot=0;
            for(var i:recovery.values()){
                if(i.uses.size()>l.maximumPreparedReferences||!keys.add(i.reference.exactKey()))throw new IllegalArgumentException("duplicate recovery key or prepared-use bound exceeded");
                extraRoot=Math.addExact(extraRoot,Math.max(0,i.rootReserve(l.maximumPreparedReferences)-(4096L+128L*l.maximumPreparedReferences)));
                bytes=Math.addExact(bytes,Math.addExact(i.reference.canonicalValueBytes(),Bytes.utf8(i.reference.exactKey()).length));records++;
            }
            long closureBytes=version==3?64:0;final var revisions=new HashSet<Long>();
            for(var closure:closures.values()){
                if(closure.inventory.size()>l.maximumPublications||Long.compareUnsigned(closure.revision,sequence)>0||!revisions.add(closure.revision))
                    throw new IllegalArgumentException("invalid closure revision/inventory bound");
                if(!topics.containsKey(Bytes.hex(closure.managementTopicId))||closure.inventory.stream().noneMatch(entry->
                        entry.subtype==closure.subtype&&Arrays.equals(entry.id,closure.publicationId)))throw new IllegalArgumentException("closure lost original management/publication inventory");
                final List<PreparedUse> uses;final byte[] origin;
                if(closure.subtype==3){final var i=Objects.requireNonNull(recovery.get(Bytes.hex(closure.publicationId)),"closed recovery intent");
                    closure.requireFingerprint(i.reference.exactKey(),i.reference.proofDigest(),i.reference.valueDigest(),i.reference.canonicalValueBytes());uses=i.uses;origin=i.originDigest();}
                else{final var i=Objects.requireNonNull(publications.get(Bytes.hex(closure.publicationId)),"closed CONTROL intent");
                    if(closure.subtype!=i.kind)throw new IllegalArgumentException("closure subtype changed");closure.requireFingerprint(i.key,i.digest,i.valueHash,i.bytes);uses=i.uses;origin=i.originDigest();}
                requireExact(closure.originDigest,origin);requireExact(closure.usesDigest,preparedUsesDigest(uses));
                if(closure.useCount!=uses.size())throw new IllegalArgumentException("closed Source-use window changed");
                closureBytes=Math.addExact(closureBytes,closure.encode().length+16L);
            }
            extraRoot=Math.addExact(extraRoot,closureBytes);bytes=Math.addExact(bytes,extraRoot);
            if(l.rootReservedBytes()+extraRoot>MAX_ROOT_BYTES)throw new IllegalStateException("evidence intent/closure root window exhausted");
            if(bytes>l.physicalBytes||records>l.physicalRecords)throw new IllegalStateException("durable receipt control-storage capacity exhausted");
        }
    }
    public OxiaSyncTargetKafkaReceiptProtectionCatalog(OxiaSyncOwnerLeaseBackend.ClientHandle handle,String keyPrefix,
            TargetKafkaReceiptProtectionTrust trust,Limits limits){
        client=Objects.requireNonNull(handle,"session-fenced Registry handle").client();session=handle.backend()::assertConnectedSession;
        this.trust=Objects.requireNonNull(trust,"original CONTROL trust");this.limits=Objects.requireNonNull(limits,"explicit durable capacity configuration");
        prefix=Objects.requireNonNull(keyPrefix,"Registry prefix");
        if(prefix.isBlank()||prefix.endsWith("/")||prefix.indexOf('\0')>=0||prefix.getBytes(StandardCharsets.UTF_8).length>1024||!Normalizer.isNormalized(prefix,Normalizer.Form.NFC))throw new IllegalArgumentException("invalid receipt Registry prefix");
        ledgerKey=prefix+"/target-kafka-receipt-storage";
    }
    /** Only this invocation's fresh persistent token permits Admin effects; an old INSTALLING token is never resumed. */
    public static final class Installation {
        private final Management management;
        private final boolean fresh;
        private Installation(Management management, boolean fresh) { this.management = management; this.fresh = fresh; }
        public Management management() { return management; }
        public boolean fresh() { return fresh; }
    }
    public Installation beginManagement(TargetKafkaReceiptControlArtifact signed){
        try (var permission = trust.currentManagement(signed, Action.BEGIN_INSTALLATION)) {
        permission.requireCurrent();
        final byte[] token = new byte[16]; new java.security.SecureRandom().nextBytes(token);
        final var desired=new Management(signed,INSTALLING,1,new byte[0],new byte[0],new byte[0],token);
        final var root=update(r->{var old=r.topics.get(desired.id());if(old!=null){
            if(!Arrays.equals(old.artifact.canonicalBytes(),signed.canonicalBytes()))throw new IllegalStateException("managed topic already has a different non-revivable epoch");return r;}
            return r.topic(desired);}, permission::requireCurrent);
        final var actual = root.topics.get(desired.id());
        permission.requireCurrent(); return new Installation(actual,Arrays.equals(actual.installationId,token));
        }
    }
    public Optional<Management> findManagement(BrokerResourceIdentity topic){final var m=readRoot().root.topics.get(topicId(topic));if(m!=null)trust.requireManagementHistory(m.artifact);return Optional.ofNullable(m);}
    public Management management(BrokerResourceIdentity topic){return findManagement(topic).orElseThrow(()->new IllegalStateException("topic has no persistent deployment authorization"));}
    public Management activateManagement(Installation installation,byte[] actualPolicy){
        if (!installation.fresh) throw new IllegalStateException("historical INSTALLING cannot issue or adopt new Admin actions");
        final var original = installation.management.artifact;
        try (var permission = trust.currentManagement(original, Action.ACTIVATE_INSTALLATION)) {
        permission.requireCurrent(); final String id=topicId(TargetKafkaReceiptManagementImage.decode(original.image()).topic());
        final var root=update(r->{final var prior=Objects.requireNonNull(r.topics.get(id),"INSTALLING management epoch");requireManagementExact(prior,original);
            if (!Arrays.equals(prior.installationId,installation.management.installationId)) throw new IllegalStateException("management installation changed its original Admin owner");
            if(prior.state==BROKEN)throw new IllegalStateException("old management epoch cannot become ACTIVE again");
            if(prior.state==ACTIVE){if(!Arrays.equals(prior.policy,actualPolicy))throw new IllegalStateException("ACTIVE epoch policy was replaced");return r;}
            return r.topic(new Management(original,ACTIVE,Math.addExact(prior.revision,1),actualPolicy,new byte[0],new byte[0],prior.installationId));}, permission::requireCurrent); permission.requireCurrent(); return root.topics.get(id);
        }
    }
    public void breakManagement(TargetKafkaReceiptControlArtifact original,byte[] failure,byte[] signature){
        trust.requireFailure(original,failure,signature);
        try (var permission = trust.currentManagement(original, Action.BREAK_MANAGEMENT)) {
        permission.requireCurrent();
        update(r->{final var prior=Objects.requireNonNull(r.topics.get(topicId(TargetKafkaReceiptManagementImage.decode(original.image()).topic())));requireManagementExact(prior,original);
            return prior.state==BROKEN?r:r.topic(new Management(original,BROKEN,Math.addExact(prior.revision,1),prior.policy,failure,signature,prior.installationId));}, permission::requireCurrent);
        permission.requireCurrent(); }
    }
    public TargetKafkaReceiptControlArtifact publishProtection(TargetKafkaReceiptControlArtifact artifact){
        final var original = TargetKafkaReceiptControlArtifact.decode(artifact.protection().managementAuthorityImage());
        try (var permission = trust.currentManagement(original, Action.PUBLISH_PROTECTION)) {
            permission.requireCurrent(); trust.requireTrusted(artifact,artifact);requireActiveManagement(artifact.protection());
            writeArtifact(artifact,protectionObjectKey(artifact.protection()), permission::requireCurrent, artifact.protection());
            final var actual = requireActive(artifact.protection().resource(),artifact.protection().evidenceGeneration(),artifact.digest());
            permission.requireCurrent(); return actual;
        }
    }
    public Optional<TargetKafkaReceiptControlArtifact> findProtection(KafkaReceiptSlotResource resource,long generation){
        final String key=protectionKey(resource,generation);final var root=readRoot().root;final var intent=findByKey(root,key);
        if(intent==null)return Optional.empty();requireOpen(root,key);if(intent.brokenDigest.length!=0)throw new IllegalStateException("original receipt protection is permanently BROKEN");
        final var artifact=readArtifact(intent);requireProtectionResource(artifact,resource,generation);requireActiveManagement(artifact.protection());trust.requireTrusted(artifact,artifact);return Optional.of(artifact);
    }
    public TargetKafkaReceiptControlArtifact requireActive(KafkaReceiptSlotResource resource,long generation,byte[] digest){
        final var image=findProtection(resource,generation).orElseThrow(()->new IllegalStateException("original receipt protection is missing"));
        if(!Arrays.equals(image.digest(),digest))throw new IllegalStateException("original receipt protection digest was replaced");return image;
    }
    public TargetKafkaReceiptControlArtifact publishAnchor(TargetKafkaReceiptControlArtifact anchor,TargetKafkaReceiptControlArtifact protection,TargetKafkaReceiptBinding binding){
        anchor.requireAnchorBinding(protection,binding);requireActive(protection.protection().resource(),protection.protection().evidenceGeneration(),protection.digest());
        final var original = TargetKafkaReceiptControlArtifact.decode(protection.protection().managementAuthorityImage());
        try (var permission = trust.currentManagement(original, Action.PUBLISH_ANCHOR)) {
            permission.requireCurrent(); trust.requireTrusted(anchor,protection);
            writeArtifact(anchor,anchorObjectKey(anchor.digest()), permission::requireCurrent, protection.protection());
            final var actual = requireAnchor(protection,binding); permission.requireCurrent(); return actual;
        }
    }
    public TargetKafkaReceiptControlArtifact requireAnchor(TargetKafkaReceiptControlArtifact protection,TargetKafkaReceiptBinding binding){
        requireActive(protection.protection().resource(),protection.protection().evidenceGeneration(),binding.retentionProtectionDigest());
        final var root=readRoot().root;final String key=anchorObjectKey(binding.scanObservationDigest());requireOpen(root,key);
        final var intent=findByKey(root,key);if(intent==null)throw new IllegalStateException("original anchor publication intent is missing");
        final var artifact=readArtifact(intent);artifact.requireAnchorBinding(protection,binding);trust.requireTrusted(artifact,protection);return artifact;
    }
    /** Immutable retained read for independent checkpoint/ledger audit; BROKEN does not erase original evidence bytes. */
    public TargetKafkaReceiptControlArtifact retainedObject(String exactKey,byte[] digest){
        final var intent=findByKey(readRoot().root,exactKey);if(intent==null||!Arrays.equals(intent.digest,digest))throw new IllegalStateException("retained external object is missing/replaced");
        final var artifact=readArtifact(intent);
        final var protection = artifact.kind()==TargetKafkaReceiptControlArtifact.PROTECTION ? artifact :
                readArtifact(requiredDigest(readRoot().root,TargetKafkaReceiptControlArtifact.PROTECTION,artifact.protectionDigest()));
        trust.requireArtifactHistory(artifact,protection);
        final String actualKey=artifact.kind()==TargetKafkaReceiptControlArtifact.PROTECTION?
                protectionObjectKey(artifact.protection()):anchorObjectKey(artifact.digest());
        if(!actualKey.equals(exactKey))throw new IllegalStateException("retained artifact changed its canonical catalog key");
        return artifact;
    }
    public void bindPrepared(TargetKafkaReceiptControlArtifact protection,TargetKafkaReceiptControlArtifact anchor,SystemMutation admission){
        final var body=TargetPublishAdmissionBody.decode(admission.canonicalBody());anchor.requireAnchorBinding(protection,body.kafkaReceipt());
        if(admission.type()!=SystemMutationType.TARGET_PUBLISH_ADMISSION||!admission.shardId().equals(body.shard())
                ||!Arrays.equals(admission.logicalOperationIdentity(),body.publishAttemptId())||admission.retryUntilEpochMs()!=body.retryUntilEpochMs())
            throw new IllegalArgumentException("prepared publication changes exact Admission identity/retry scope");
        final var use=new PreparedUse(admission.systemMutationId(),admission.mutationHash(),admission.retryUntilEpochMs());
        update(r->{var next=r;for(var a:List.of(protection,anchor)){final var i=requiredIntent(next,a);
            requireOpen(next,i.key);
            if(i.phase< PUBLISHED||i.brokenDigest.length!=0||!body.shard().equals(shard(i.shard)))throw new IllegalStateException("receipt publication cannot accept new prepared Source use");
            next=next.publication(i.bind(use,limits.maximumPreparedReferences));}return next;});
    }
    /** Outside-Store snapshot check of the original signed frame binding; no catalog I/O is needed in native apply. */
    public void requirePrepared(TargetKafkaReceiptControlArtifact protection, TargetKafkaReceiptControlArtifact anchor,
            SystemMutation prepared) {
        final var root = readRoot().root;
        for (var artifact : List.of(protection, anchor)) {
            final var intent = requiredIntent(root, artifact);
            if (intent.phase < POSSIBLY_SOURCE || intent.brokenDigest.length != 0
                    || intent.uses.stream().noneMatch(u -> Arrays.equals(u.id, prepared.systemMutationId())
                            && Arrays.equals(u.hash, prepared.mutationHash()) && u.retryUntil == prepared.retryUntilEpochMs())) {
                throw new IllegalStateException("Source Admission has no exact durable prepared publication/retry binding");
            }
        }
    }
    /** Private proof of this Catalog's exact immutable publication, never proof of Source adoption. */
    public static final class RecoveryPublication {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final TargetRecoveryProofTrust.Verified verified;
        private RecoveryPublication(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,TargetRecoveryProofTrust.Verified verified){
            this.catalog=catalog;this.verified=verified;
        }
        public TargetRecoveryProofTrust.Verified verified(){return verified;}
        public TargetRecoveryProofRef reference(){return verified.reference();}
    }
    /** Actual durable prepared ID/hash/retry binding, obtained outside the native Store batch. */
    public static final class PreparedRecovery {
        private final RecoveryPublication publication;
        private final PreparedUse use;
        private PreparedRecovery(RecoveryPublication publication,PreparedUse use){this.publication=publication;this.use=use;}
        public TargetRecoveryProofTrust.Verified verified(){return publication.verified;}
        public void requireExact(SystemMutation prepared){
            OxiaSyncTargetKafkaReceiptProtectionCatalog.requireExact(use.bytes(),new PreparedUse(prepared.systemMutationId(),prepared.mutationHash(),prepared.retryUntilEpochMs()).bytes());
            requireRecoveryBody(publication.verified,prepared);
        }
    }
    /** Reserve responsibility before put; uncertain writes succeed only after exact authenticated reread. */
    public RecoveryPublication publishRecoveryProof(TargetRecoveryProof proof,TargetRecoveryProofTrust proofTrust,Runnable current){
        Objects.requireNonNull(current).run();
        final byte[] value=proof.canonicalBytes();final String key=recoveryObjectKey(proof.digest());
        final var verified=Objects.requireNonNull(proofTrust).verify(key,value);
        final var desired=new RecoveryIntent(recoveryId(proof.mapping().mappingId(),proof.evidence().evidenceId()),
                verified.reference(),proof.frozenOwner(),RESERVED,List.of(),new byte[0]);
        requireRecoveryBlobBudget(proof,key);
        update(root->{final var old=root.recoveries.get(Bytes.hex(desired.id));
            requireOpen(root,key);requireRecoveryDependenciesOpen(root,proof);
            if(old!=null){requireSameRecovery(old,desired);return root;}
            return root.recovery(desired);},current);
        final var existing=read(key);
        if(existing==null){
            try{session.run();current.run();client.put(key,value,Set.of(PutOption.IfRecordDoesNotExist));session.run();current.run();}
            catch(KeyAlreadyExistsException|UnexpectedVersionIdException|RuntimeException uncertain){
                current.run();final var actual=read(key);
                if(actual==null)throw new IllegalStateException("recovery proof publication remains uncertain",uncertain);
                requireExact(actual.value(),value);proofTrust.verify(key,actual.value()).requireExact(desired.reference);
            }
        }else{requireExact(existing.value(),value);proofTrust.verify(key,existing.value()).requireExact(desired.reference);}
        final var actual=readRecovery(desired,proofTrust);current.run();
        update(root->{final var old=Objects.requireNonNull(root.recoveries.get(Bytes.hex(desired.id)),"reserved recovery intent");
            requireOpen(root,key);requireRecoveryDependenciesOpen(root,proof);
            requireSameRecovery(old,desired);return old.phase>=PUBLISHED?root:root.recovery(old.phase(PUBLISHED));},current);
        current.run();return new RecoveryPublication(this,actual);
    }
    /** Unique mapping/evidence discovery. A reservation without a blob/publication is never reconstructed as success. */
    public Optional<RecoveryPublication> findRecoveryProof(TargetKafkaReceiptMapping mapping,PublishEvidence evidence,TargetRecoveryProofTrust proofTrust){
        final var root=readRoot().root;final var intent=root.recoveries.get(Bytes.hex(recoveryId(mapping.mappingId(),evidence.evidenceId())));
        if(intent==null)return Optional.empty();
        requireOpen(root,intent.reference.exactKey());requireRecoveryDependenciesOpen(root,mapping);
        if(intent.phase<PUBLISHED)throw new IllegalStateException("original recovery publication remains reserved/uncertain");
        final var verified=readRecovery(intent,proofTrust);
        requireExact(verified.proof().mapping().canonicalBytes(),mapping.canonicalBytes());
        requireExact(verified.proof().evidence().canonicalBytes(),evidence.canonicalBytes());
        return Optional.of(new RecoveryPublication(this,verified));
    }
    /** Crash/unknown-response continuation authenticates the selected actual blob; it never re-signs another observation. */
    public RecoveryPublication resumeRecoveryProof(TargetKafkaReceiptMapping mapping,PublishEvidence evidence,
            TargetRecoveryProofTrust proofTrust,Runnable current){
        Objects.requireNonNull(current).run();final String id=Bytes.hex(recoveryId(mapping.mappingId(),evidence.evidenceId()));
        final var original=Objects.requireNonNull(readRoot().root.recoveries.get(id),"original recovery reservation");
        final var verified=readRecovery(original,proofTrust);
        requireExact(verified.proof().mapping().canonicalBytes(),mapping.canonicalBytes());
        requireExact(verified.proof().evidence().canonicalBytes(),evidence.canonicalBytes());current.run();
        update(root->{final var actual=Objects.requireNonNull(root.recoveries.get(id),"original recovery reservation");
            requireOpen(root,actual.reference.exactKey());requireRecoveryDependenciesOpen(root,verified.proof());
            requireSameRecovery(actual,original);return actual.phase>=PUBLISHED?root:root.recovery(actual.phase(PUBLISHED));},current);
        current.run();return new RecoveryPublication(this,verified);
    }
    /** The actual Root reuses a durably selected observation before considering a new signature. */
    public Optional<RecoveryPublication> resumeSelectedRecoveryProof(TargetKafkaReceiptMapping mapping,PublishEvidence evidence,
            TargetRecoveryProofTrust proofTrust,Runnable current){
        Objects.requireNonNull(current).run();
        if(!readRoot().root.recoveries.containsKey(Bytes.hex(recoveryId(mapping.mappingId(),evidence.evidenceId()))))return Optional.empty();
        return Optional.of(resumeRecoveryProof(mapping,evidence,proofTrust,current));
    }
    /** Must complete before enqueueing the signed proof-bearing Source frame. */
    public PreparedRecovery sealRecoveryPrepared(RecoveryPublication publication,SystemMutation prepared,TargetRecoveryProofTrust proofTrust,Runnable current){
        requireOwnPublication(publication);requireRecoveryBody(publication.verified,prepared);Objects.requireNonNull(current).run();
        final var ref=publication.reference();final byte[] id=recoveryId(ref.mappingId(),ref.evidenceId());
        final var use=new PreparedUse(prepared.systemMutationId(),prepared.mutationHash(),prepared.retryUntilEpochMs());
        // Reauthenticate actual stored bytes before extending the durable possible-Source window.
        final var before=Objects.requireNonNull(readRoot().root.recoveries.get(Bytes.hex(id)),"original recovery publication");
        readRecovery(before,proofTrust).requireExact(ref);
        update(root->{final var intent=Objects.requireNonNull(root.recoveries.get(Bytes.hex(id)),"original recovery publication");
            requireOpen(root,ref.exactKey());requireRecoveryDependenciesOpen(root,publication.verified.proof());
            if(intent.phase<PUBLISHED||!intent.reference.equals(ref))throw new IllegalStateException("prepared recovery changed original publication");
            return root.recovery(intent.bind(use,limits.maximumPreparedReferences));},current);
        final var result=requireRecoveryPrepared(ref,prepared,proofTrust);current.run();return result;
    }
    /** Retained history does not depend on today's SDK, Owner or credentials; it still requires the exact durable seal. */
    public PreparedRecovery requireRecoveryPrepared(TargetRecoveryProofRef ref,SystemMutation prepared,TargetRecoveryProofTrust proofTrust){
        final var intent=Objects.requireNonNull(readRoot().root.recoveries.get(Bytes.hex(recoveryId(ref.mappingId(),ref.evidenceId()))),"original recovery intent");
        final var use=new PreparedUse(prepared.systemMutationId(),prepared.mutationHash(),prepared.retryUntilEpochMs());
        if(intent.phase<POSSIBLY_SOURCE||!intent.reference.equals(ref)
                ||intent.uses.stream().noneMatch(old->Arrays.equals(old.bytes(),use.bytes())))
            throw new IllegalStateException("recovery Source frame lacks exact durable prepared ID/hash/retry seal");
        final var verified=readRecovery(intent,proofTrust);requireRecoveryBody(verified,prepared);
        return new PreparedRecovery(new RecoveryPublication(this,verified),use);
    }
    /** Actual retained blob and original seal for a Native owner; today's Owner/material/SDK is irrelevant to history. */
    public PreparedRecovery retainedRecovery(com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner owner,
            SystemMutation outcome,TargetRecoveryProofTrust proofTrust){
        owner.requireSource(outcome,owner.adoptedAt().source());
        final var snapshot=requireRecoveryPrepared(owner.reference(),outcome,proofTrust);
        owner.requireProof(snapshot.verified().proof());return snapshot;
    }
    private TargetRecoveryProofTrust.Verified readRecovery(RecoveryIntent intent,TargetRecoveryProofTrust proofTrust){
        final var ref=intent.reference;final var actual=read(ref.exactKey());
        if(actual==null)throw new IllegalStateException("original recovery proof blob is missing");
        ref.requireObject(ref.proofDigest(),actual.value());
        final var verified=Objects.requireNonNull(proofTrust).verify(ref.exactKey(),actual.value());verified.requireExact(ref);
        requireExact(verified.proof().frozenOwner().canonicalBytes(),intent.frozenOwner.canonicalBytes());
        if(!recoveryObjectKey(ref.proofDigest()).equals(ref.exactKey()))throw new IllegalStateException("recovery proof belongs to another catalog");
        return verified;
    }
    private void requireOwnPublication(RecoveryPublication publication){
        if(Objects.requireNonNull(publication).catalog!=this)throw new IllegalArgumentException("recovery publication belongs to another catalog");
    }
    private static void requireRecoveryBody(TargetRecoveryProofTrust.Verified verified,SystemMutation prepared){
        final var body=switch(prepared.type()){
            case PUBLISH_OUTCOME->PublishOutcomeBody.decode(prepared.canonicalBody());
            case EVIDENCE_RESOLUTION->PublishOutcomeBody.decodeEvidenceResolution(prepared.canonicalBody());
            default->throw new IllegalArgumentException("only proof-bearing Publish Outcome/Evidence Resolution can be sealed");
        };
        if(body.recoveryProof()==null)throw new IllegalArgumentException("recovery Source frame requires explicit body version and exact proof reference");
        verified.requireExact(body.recoveryProof());final var proof=verified.proof();
        requireExact(body.evidence(),proof.evidence().canonicalBytes());
        if(!prepared.shardId().equals(proof.mapping().admission().shard())||!Arrays.equals(body.publishAttemptId(),verified.reference().attemptId())
                ||!Arrays.equals(prepared.logicalOperationIdentity(),prepared.type()==SystemMutationType.PUBLISH_OUTCOME?
                        body.initialLogicalOperationIdentity():body.evidenceResolutionLogicalOperationIdentity()))
            throw new IllegalArgumentException("recovery Source frame changed original Shard/Attempt/logical identity");
    }
    private void requireRecoveryBlobBudget(TargetRecoveryProof proof,String key){
        // Future Source positions are unknown here: reserve the finite complete native STATE window as well.
        final var mapping=proof.mapping();final var anchor=retainedObject(anchorObjectKey(mapping.admission().kafkaReceipt().scanObservationDigest()),
                mapping.admission().kafkaReceipt().scanObservationDigest());
        final var accounting=proof.frozenOwner().accounting();
        final var originalAnchor=accounting.externalEvidenceCharge(Bytes.utf8(anchorObjectKey(anchor.digest())).length,anchor.canonicalBytes().length);
        final var recovery=accounting.externalEvidenceCharge(Bytes.utf8(key).length,proof.canonicalBytes().length);
        if(!proof.attempt().allocated().covers(originalAnchor.add(recovery).add(TargetRecoveryEvidenceOwner.maximumStateCharge(accounting))))
            throw new IllegalStateException("original frozen Attempt cannot pay anchor/recovery/native STATE window; keep UNKNOWN without archiving");
    }
    private String recoveryObjectKey(byte[] digest){Bytes.requireLength(digest,32,"recovery proof digest");return prefix+"/target-kafka-recovery-proof/"+Bytes.hex(digest);}
    private static byte[] recoveryId(byte[] mapping,byte[] evidence){
        Bytes.requireLength(mapping,32,"recovery mapping ID");Bytes.requireLength(evidence,32,"recovery evidence ID");
        return Bytes.sha256(Bytes.utf8("nereus-delay-target-recovery-publication\0"),mapping,evidence);
    }
    private static void requireSameRecovery(RecoveryIntent actual,RecoveryIntent expected){
        if(!actual.reference.equals(expected.reference)||!Arrays.equals(actual.frozenOwner.canonicalBytes(),expected.frozenOwner.canonicalBytes()))
            throw new IllegalStateException("mapping/evidence already selected another exact recovery proof");
    }
    /** Only actual guarded Native APPLIED+owner/ref/first/POSITION facts can conservatively mark the Catalog adopted. */
    public void adoptRecovery(TargetRecoveryEvidenceStore.Adoption applied){
        adoptRecovery(applied,session,null);
    }
    private void adoptRecovery(TargetRecoveryEvidenceStore.Adoption applied,Runnable current,BoundedReadBudget budget){
        final var owner=Objects.requireNonNull(applied).owner();applied.reference().requireOwner(owner);
        final var ref=owner.reference();final String id=Bytes.hex(recoveryId(ref.mappingId(),ref.evidenceId()));
        update(root->{final var intent=Objects.requireNonNull(root.recoveries.get(id),"original recovery publication");
            if(intent.phase<POSSIBLY_SOURCE||!intent.reference.equals(ref)
                    ||!Arrays.equals(intent.frozenOwner.canonicalBytes(),owner.primaryOwner().canonicalBytes())
                    ||intent.uses.stream().noneMatch(use->Arrays.equals(use.id,owner.outcomeId())&&Arrays.equals(use.hash,owner.outcomeHash())
                            &&use.retryUntil==applied.retryUntilEpochMs()))
                throw new IllegalStateException("actual recovery adoption differs from original publication/prepared seal");
            return root.recovery(intent.adopt(Bytes.sha256(owner.adoptedAt().source().canonicalBytes())));
        },current,budget);
    }
    public void adopt(TargetExternalEvidenceStore.Adoption proof){
        final var mapping=proof.mapping();final byte[] id=SystemMutation.computeSystemMutationId(mapping.admission().shard(),SystemMutationType.TARGET_PUBLISH_ADMISSION,mapping.admission().publishAttemptId(),mapping.mutationHash());
        update(r->{var next=r;for(var owner:List.of(proof.protection(),proof.anchor())){final var i=Objects.requireNonNull(findByKey(next,owner.exactKey()),"original publication intent");
            if(i.kind!=owner.kind()||!Arrays.equals(i.digest,owner.artifactDigest())||!Arrays.equals(i.valueHash,owner.valueDigest())||i.bytes!=owner.canonicalValueBytes()
                    ||!Arrays.equals(i.controlDigest,owner.originalControlDigest())
                    ||!i.controlScope.shard().equals(owner.primaryOwner().identity().shard())
                    ||!i.controlScope.target().equals(owner.primaryOwner().identity().target())
                    ||owner.kind()==TargetKafkaReceiptControlArtifact.PROTECTION&&!i.controlScope.equals(owner.primaryOwner().identity()))
                throw new IllegalStateException("Source adoption changed exact external object/original frozen CONTROL scope");
            if(i.uses.stream().noneMatch(u->Arrays.equals(u.id,id)&&Arrays.equals(u.hash,mapping.mutationHash())&&u.retryUntil==mapping.admission().retryUntilEpochMs()))throw new IllegalStateException("Source adoption lacks its original prepared ID/hash/retry binding");
            next=next.publication(i.adopt(Bytes.sha256(mapping.mutation().source().canonicalBytes())));}return next;});
    }
    public void markBroken(KafkaReceiptSlotResource resource,long generation,byte[] originalDigest,byte[] evidence,byte[] signature){
        final String key=protectionKey(resource,generation);final var intent=Objects.requireNonNull(findByKey(readRoot().root,key));final var original=readArtifact(intent);
        if(!Arrays.equals(original.digest(),originalDigest))throw new IllegalArgumentException("failure changed original protection");trust.requireFailure(original,evidence,signature);
        final var management = TargetKafkaReceiptControlArtifact.decode(original.protection().managementAuthorityImage());
        try (var permission = trust.currentManagement(management, Action.BREAK_PROTECTION)) {
            permission.requireCurrent();
            update(r->{final var i=Objects.requireNonNull(findByKey(r,key));if(i.brokenDigest.length!=0)return r;
                return r.publication(new Intent(i.id,i.kind,i.key,i.digest,i.valueHash,i.bytes,i.controlId,i.controlDigest,i.shard,i.controlScope,i.phase,i.uses,i.adoptedSource,evidence,signature));}, permission::requireCurrent);
            permission.requireCurrent();
        }
    }
    public void releaseProtection(KafkaReceiptSlotResource resource,long generation){throw new IllegalStateException("release/orphan GC requires original Source retirement/Floor/query/shared-topic closure");}
    /** One owned Root input window; shared Root assembly must reserve it before entering the closure I/O path. */
    public static long closureInputWindowBytes(){return 16L*(MAX_ROOT_BYTES+TargetRecoveryProofRef.MAX_PROOF_BYTES);}

    /** Actual durable freeze receipt. It does not prove terminal uses, complete shared-reference release, or deletion eligibility. */
    public static final class ClosedObject {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final Closure closure;
        private final List<ClosedPreparedUse> uses;
        private final byte[] deploymentRootDigest;
        private ClosedObject(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,Closure closure,List<PreparedUse> uses,byte[] deploymentRootDigest){
            this.catalog=catalog;this.closure=closure;this.uses=uses.stream().map(u->new ClosedPreparedUse(u.id,u.hash,u.retryUntil)).toList();
            this.deploymentRootDigest=Bytes.copy(deploymentRootDigest);
        }
        public String exactKey(){return closure.key;}
        public int subtype(){return closure.subtype;}
        public long revision(){return closure.revision;}
        public byte[] digest(){return closure.digest();}
        public byte[] canonicalBytes(){return closure.encode();}
        public byte[] namespaceId(){return Bytes.copy(closure.namespaceId);}
        public byte[] publicationId(){return Bytes.copy(closure.publicationId);}
        public byte[] publicationOriginDigest(){return Bytes.copy(closure.originDigest);}
        public List<ClosedPreparedUse> preparedUses(){return uses;}
        public List<CatalogPublication> publicationInventory(){return closure.inventory;}
        /** Immutable identity data; actual Native adoption is authenticated separately, never inferred from this builder. */
        public TargetExternalEvidenceObjectResource recoveryResource(TargetRecoveryEvidenceOwner owner){
            requireRecoveryOwner(owner);final var frozen=owner.primaryOwner();
            return new TargetExternalEvidenceObjectResource(TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF,
                    closure.namespaceId,deploymentRootDigest,frozen.identity().shard(),frozen.tenantScope(),frozen.recoveryLineage(),
                    closure.key,closure.objectDigest,closure.valueDigest,closure.valueBytes,Bytes.u64beBits(closure.blobVersion),
                    new TargetExternalEvidenceObjectResource.Recovery(owner));
        }
        public void requireRecoveryOwner(TargetRecoveryEvidenceOwner owner){
            final var ref=Objects.requireNonNull(owner).reference();
            if(closure.subtype!=3)throw new IllegalArgumentException("closure is not a recovery proof");
            closure.requireFingerprint(ref.exactKey(),ref.proofDigest(),ref.valueDigest(),ref.canonicalValueBytes());
            final byte[] id=recoveryId(ref.mappingId(),ref.evidenceId());requireExact(closure.publicationId,id);
            requireExact(closure.originDigest,new RecoveryIntent(id,ref,owner.primaryOwner(),RESERVED,List.of(),new byte[0]).originDigest());
        }
        /** This matches object bytes/version only; actual Source/billing/staging provenance still requires Native joins. */
        public void requireObjectFingerprint(TargetExternalEvidenceObjectResource resource){
            if(resource.subtype().wireValue()!=closure.subtype)throw new IllegalArgumentException("closure subtype differs from resource");
            closure.requireFingerprint(resource.exactKey(),resource.objectDigest(),resource.valueDigest(),resource.canonicalValueBytes());
            requireExact(resource.catalogNamespaceId(),closure.namespaceId);requireExact(resource.immutableVersion(),Bytes.u64beBits(closure.blobVersion));
            requireExact(resource.deploymentRootDigest(),deploymentRootDigest);
        }
        /** Reauthentication of the original freeze remains possible after blob deletion, while Root metadata is retained. */
        public ClosedObject reread(BoundedReadBudget reads,Runnable current){
            Objects.requireNonNull(current).run();final var actual=catalog.readReferenceClosure(closure.key,closure.objectDigest,reads);
            requireExact(actual.closure.encode(),closure.encode());current.run();return actual;
        }
    }

    /**
     * Freezes new references to an actually present authenticated object under a distinct current Root policy action.
     * Existing sealed uses and in-flight publication responsibilities remain protected, including uncertain/RESERVED ones.
     */
    public ClosedObject closeAdoptedRecoveryReferences(TargetRecoveryEvidenceStore.Adoption applied,TargetRecoveryProofTrust proofTrust,
            BoundedReadBudget reads,Runnable current){
        final var owner=Objects.requireNonNull(applied,"actual Native APPLIED recovery adoption").owner();
        applied.reference().requireOwner(owner);
        return closePresentObjectReferences(owner.reference().exactKey(),owner.reference().proofDigest(),proofTrust,reads,current,applied);
    }
    private ClosedObject closePresentObjectReferences(String exactKey,byte[] objectDigest,TargetRecoveryProofTrust proofTrust,
            BoundedReadBudget reads,Runnable current,TargetRecoveryEvidenceStore.Adoption applied){
        Objects.requireNonNull(reads);Objects.requireNonNull(current).run();Bytes.requireLength(objectDigest,32,"exact closure object digest");
        final var before=readRoot(reads).root;final var control=findByKey(before,exactKey);RecoveryIntent recovery=null;
        if(control==null)for(var candidate:before.recoveries.values())if(candidate.reference.exactKey().equals(exactKey)){recovery=candidate;break;}
        if(control==null&&recovery==null)throw new IllegalStateException("original object publication intent is missing");
        final int subtype=control==null?3:control.kind;final byte[] publication=control==null?recovery.id:control.id;
        final byte[] typedDigest=control==null?recovery.reference.proofDigest():control.digest;requireExact(objectDigest,typedDigest);
        final byte[] rawDigest=control==null?recovery.reference.valueDigest():control.valueHash;
        final long bytes=control==null?recovery.reference.canonicalValueBytes():control.bytes;
        final var stored=read(exactKey,reads,Math.toIntExact(bytes));
        if(stored==null||stored.value().length!=bytes)throw new IllegalStateException("missing/uncertain publication needs separate original-writer settlement before closure");
        requireExact(rawDigest,Bytes.sha256(stored.value()));
        final TargetKafkaReceiptControlArtifact protection;
        if(subtype==3){
            final var verified=Objects.requireNonNull(proofTrust,"original recovery proof history").verify(exactKey,stored.value());
            verified.requireExact(recovery.reference);requireExact(recovery.frozenOwner.canonicalBytes(),verified.proof().frozenOwner().canonicalBytes());
            if(applied!=null)applied.owner().requireProof(verified.proof());
            if(!recoveryObjectKey(verified.proof().digest()).equals(exactKey))throw new IllegalStateException("proof belongs to another catalog");
            final var binding=verified.proof().mapping().admission().kafkaReceipt();
            protection=readArtifact(requiredDigest(before,1,binding.retentionProtectionDigest()),reads);
            requireProtectionResource(protection,binding.receiptResource(),verified.proof().mapping().admission().channel().context().evidenceGeneration());
            trust.requireArtifactHistory(protection,protection);
        }else{
            final var artifact=decodeArtifact(control,stored);
            protection=subtype==1?artifact:readArtifact(requiredDigest(before,1,artifact.protectionDigest()),reads);
            trust.requireArtifactHistory(artifact,protection);
            final String canonicalKey=subtype==1?protectionObjectKey(artifact.protection()):anchorObjectKey(artifact.digest());
            if(!canonicalKey.equals(exactKey))throw new IllegalStateException("CONTROL object changed its canonical catalog key");
        }
        final var authority=TargetKafkaReceiptControlArtifact.decode(protection.protection().managementAuthorityImage());
        final var management=Objects.requireNonNull(before.topics.get(topicId(TargetKafkaReceiptManagementImage.decode(authority.image()).topic())),"original managed topic");
        requireExact(management.artifact.image(),authority.image());
        final byte[] namespace=namespaceId(management.image());
        try(var permission=trust.currentManagement(authority,Action.CLOSE_EVIDENCE_OBJECT_REFERENCES)){
            final Runnable held=()->{current.run();permission.requireCurrent();if(!reads.beforeTimedWork())throw reads.incomplete();};held.run();
            if(applied!=null)adoptRecovery(applied,held,reads);
            final var closed=update(root->{
                final List<PreparedUse> uses;final byte[] origin;final int phase;final byte[] adopted;
                if(subtype==3){final var original=Objects.requireNonNull(root.recoveries.get(Bytes.hex(publication)),"original recovery intent");
                    requireExact(original.reference.proofDigest(),objectDigest);requireExact(original.reference.valueDigest(),rawDigest);
                    if(!original.reference.exactKey().equals(exactKey)||original.reference.canonicalValueBytes()!=bytes)throw new IllegalStateException("recovery object changed during closure");
                    uses=original.uses;origin=original.originDigest();phase=original.phase;adopted=original.adoptedSource;
                }else{final var original=Objects.requireNonNull(root.publications.get(Bytes.hex(publication)),"original CONTROL intent");
                    if(original.kind!=subtype||!original.key.equals(exactKey)||original.bytes!=bytes)throw new IllegalStateException("CONTROL object changed during closure");
                    requireExact(original.digest,objectDigest);requireExact(original.valueHash,rawDigest);
                    uses=original.uses;origin=original.originDigest();phase=original.phase;adopted=original.adoptedSource;
                }
                final var existing=root.closures.get(exactKey);
                if(existing!=null){existing.requireFingerprint(exactKey,objectDigest,rawDigest,bytes);requireExact(existing.namespaceId,namespace);
                    if(existing.blobVersion!=stored.version().versionId())throw new IllegalStateException("closed immutable object version changed");return root;}
                if(root.closureSequence==-1L)throw new IllegalStateException("closure revision exhausted");
                final var inventory=publicationInventory(root);
                return root.close(new Closure(subtype,exactKey,objectDigest,rawDigest,bytes,stored.version().versionId(),publication,origin,
                        preparedUsesDigest(uses),uses.size(),root.closureSequence+1,namespace,phase,adopted,inventory,inventoryDigest(inventory),
                        Bytes.sha256(root.bytes()),management.image().topicIdentity()));
            },held,reads);
            held.run();final var after=read(exactKey,reads,Math.toIntExact(bytes));
            if(after==null||after.version().versionId()!=stored.version().versionId())throw new IllegalStateException("closed object changed during authenticated reread");
            requireExact(stored.value(),after.value());held.run();final var receipt=closedSnapshot(closed,exactKey,objectDigest);held.run();return receipt;
        }
    }
    /** Historical metadata authentication only; absence of the blob is not inferred as successful deletion. */
    public ClosedObject readReferenceClosure(String exactKey,byte[] objectDigest,BoundedReadBudget reads){
        final var root=readRoot(Objects.requireNonNull(reads)).root;final var receipt=closedSnapshot(root,exactKey,objectDigest);
        if(!reads.beforeTimedWork())throw reads.incomplete();return receipt;
    }
    public ClosedObject readReferenceClosure(ClosedObject original,BoundedReadBudget reads,Runnable current){
        if(Objects.requireNonNull(original).catalog!=this)throw new IllegalArgumentException("closure belongs to another actual Catalog");
        return original.reread(reads,current);
    }
    /** Only this actual Catalog can produce the authenticated public-copy receipt. It is not delete authority. */
    public static final class RetirementCopy {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final TargetEvidenceRetirementCopy image;
        private RetirementCopy(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,TargetEvidenceRetirementCopy image){
            this.catalog=catalog;this.image=image;
        }
        public TargetEvidenceRetirementCopy image(){return image;}
        public void requireCatalog(OxiaSyncTargetKafkaReceiptProtectionCatalog expected){
            if(catalog!=expected)throw new IllegalArgumentException("copy belongs to another actual Catalog");
        }
    }
    public RetirementCopy prepareRecoveryRetirementCopy(ClosedObject original,TargetRecoveryEvidenceOwner owner,
            TargetRecoveryProofTrust proofTrust,BoundedReadBudget reads,Runnable current){
        final var actual=readReferenceClosure(original,reads,current);actual.requireRecoveryOwner(owner);
        final var resource=actual.recoveryResource(owner);current.run();
        final var stored=read(resource.exactKey(),reads,Math.toIntExact(resource.canonicalValueBytes()));
        if(stored==null||!Arrays.equals(resource.immutableVersion(),Bytes.u64beBits(stored.version().versionId())))
            throw new IllegalStateException("retirement copy lacks original present object/version");
        final var verified=Objects.requireNonNull(proofTrust).verify(resource.exactKey(),stored.value());
        owner.requireProof(verified.proof());current.run();
        final var copy=new TargetEvidenceRetirementCopy(resource,stored.value(),proofTrust.historicalPublicInputs(verified),actual.canonicalBytes());
        actual.requireObjectFingerprint(copy.resource());readReferenceClosure(actual,reads,current);
        final var after=read(resource.exactKey(),reads,Math.toIntExact(resource.canonicalValueBytes()));
        if(after==null||after.version().versionId()!=stored.version().versionId())
            throw new IllegalStateException("retirement object changed during public-copy authentication");
        requireExact(stored.value(),after.value());current.run();if(!reads.beforeTimedWork())throw reads.incomplete();
        return new RetirementCopy(this,copy);
    }
    private ClosedObject closedSnapshot(Root root,String key,byte[] digest){
        final var closure=Objects.requireNonNull(root.closures.get(key),"original durable object closure");requireExact(closure.objectDigest,digest);
        final var management=Objects.requireNonNull(root.topics.get(Bytes.hex(closure.managementTopicId)),"original closure management");
        trust.requireManagementHistory(management.artifact);requireExact(closure.namespaceId,namespaceId(management.image()));
        final var uses=closure.subtype==3?root.recoveries.get(Bytes.hex(closure.publicationId)).uses:root.publications.get(Bytes.hex(closure.publicationId)).uses;
        return new ClosedObject(this,closure,uses,management.image().deploymentRootDigest());
    }
    private byte[] namespaceId(TargetKafkaReceiptManagementImage image){return Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-catalog-namespace\0"),
            image.deploymentRootDigest(),Bytes.lp32(Bytes.utf8(prefix)),limits.capacitySourceDigest);}
    public String protectionObjectKey(TargetKafkaReceiptProtection image){return protectionKey(image.resource(),image.evidenceGeneration());}
    public String anchorObjectKey(byte[] digest){Bytes.requireLength(digest,32,"anchor digest");return prefix+"/target-kafka-receipt-anchor/"+Bytes.hex(digest);}
    private void writeArtifact(TargetKafkaReceiptControlArtifact artifact,String key,Runnable current,TargetKafkaReceiptProtection protection){
        current.run();
        final var request=(TargetChannelControlRequest)artifact.control().request().branch();final var c=request.channel().context();
        final var identity=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.TARGET,c.sourceShard(),c.accountingIncarnation(),c.target(),null);
        final byte[] id=Bytes.sha256(Bytes.utf8("nereus-delay-target-kafka-receipt-publication\0"),Bytes.u32be(artifact.kind()),artifact.digest(),Bytes.lp32(Bytes.utf8(key)),artifact.control().operationId());
        final var desired=new Intent(id,artifact.kind(),key,artifact.digest(),Bytes.sha256(artifact.canonicalBytes()),artifact.canonicalBytes().length,
            artifact.control().operationId(),Bytes.sha256(artifact.control().canonicalBytes()),shardBytes(c.sourceShard()),identity,RESERVED,List.of(),new byte[0],new byte[0],new byte[0]);
        // Capacity and original publication responsibility persist before any blob put. UNKNOWN never frees either.
        update(r->{requireOpen(r,key);if(artifact.kind()==TargetKafkaReceiptControlArtifact.ANCHOR)requireOpen(r,protectionObjectKey(protection));
            requireActiveManagement(r,protection);final var old=r.publications.get(Bytes.hex(id));if(old!=null){if(!samePublication(old,desired))throw new IllegalStateException("publication changed original exact object/scope");return r;}
            if(findByKey(r,key)!=null)throw new IllegalStateException("external key already belongs to another publication");return r.publication(desired);},current);
        final byte[] value=artifact.canonicalBytes();final var existing=read(key);
        if(existing==null){try{requireActiveManagement(protection);session.run();current.run();client.put(key,value,Set.of(PutOption.IfRecordDoesNotExist));session.run();current.run();}
            catch(KeyAlreadyExistsException|UnexpectedVersionIdException|RuntimeException uncertain){current.run();final var actual=read(key);if(actual==null)throw new IllegalStateException("artifact publication remains uncertain",uncertain);requireExact(actual.value(),value);}}
        else requireExact(existing.value(),value);
        final var actual=read(key);if(actual==null)throw new IllegalStateException("artifact publication lacks exact durable reread");requireExact(actual.value(),value);
        update(r->{requireOpen(r,key);if(artifact.kind()==TargetKafkaReceiptControlArtifact.ANCHOR)requireOpen(r,protectionObjectKey(protection));
            requireActiveManagement(r,protection);final var i=Objects.requireNonNull(r.publications.get(Bytes.hex(id)));return i.phase>=PUBLISHED?r:r.publication(i.phase(PUBLISHED));},current); current.run();
    }
    private record Observed(GetResult stored,Root root){}
    private Observed readRoot(){final var result=read(ledgerKey);final var root=result==null?new Root(limits,Map.of(),Map.of()):Root.decode(result.value());requireLimits(root.limits);return new Observed(result,root);}
    private Observed readRoot(BoundedReadBudget budget){final var result=read(ledgerKey,budget,MAX_ROOT_BYTES);
        final var root=result==null?new Root(limits,Map.of(),Map.of()):Root.decode(result.value());requireLimits(root.limits);return new Observed(result,root);}
    private Root update(UnaryOperator<Root> operation){ return update(operation, session); }
    private Root update(UnaryOperator<Root> operation,Runnable current){return update(operation,current,null);}
    private Root update(UnaryOperator<Root> operation,Runnable current,BoundedReadBudget budget){
        for(int attempt=0;attempt<MAX_CAS_ATTEMPTS;attempt++){current.run();final var before=budget==null?readRoot():readRoot(budget);requireLimits(before.root.limits);final var after=operation.apply(before.root);final byte[] desired=after.bytes();
            if(before.stored!=null&&Arrays.equals(before.stored.value(),desired)){current.run();return after;}
            try{session.run();current.run();final var result=client.put(ledgerKey,desired,before.stored==null?Set.of(PutOption.IfRecordDoesNotExist):Set.of(PutOption.IfVersionIdEquals(before.stored.version().versionId())));session.run();current.run();
                if(result==null||!ledgerKey.equals(result.key())||result.version()==null)throw new IllegalStateException("receipt capacity CAS returned no exact key/version");}
            catch(KeyAlreadyExistsException|UnexpectedVersionIdException race){continue;}
            catch(RuntimeException uncertain){current.run();final var observed=budget==null?readRoot():readRoot(budget);if(observed.stored==null||!Arrays.equals(observed.stored.value(),desired))throw uncertain;current.run();return observed.root;}
            final var observed=budget==null?readRoot():readRoot(budget);if(observed.stored!=null&&Arrays.equals(observed.stored.value(),desired)){current.run();return observed.root;}
            // A competing legitimate update may have advanced after this CAS; reapply idempotently, never overwrite it.
        }throw new IllegalStateException("receipt capacity CAS did not converge within configured bound");
    }
    private TargetKafkaReceiptControlArtifact readArtifact(Intent i){return decodeArtifact(i,read(i.key));}
    private TargetKafkaReceiptControlArtifact readArtifact(Intent i,BoundedReadBudget budget){return decodeArtifact(i,read(i.key,budget,Math.toIntExact(i.bytes)));}
    private static TargetKafkaReceiptControlArtifact decodeArtifact(Intent i,GetResult value){if(value==null||value.value().length!=i.bytes||!Arrays.equals(Bytes.sha256(value.value()),i.valueHash))throw new IllegalStateException("original external blob is missing/replaced");final var a=TargetKafkaReceiptControlArtifact.decode(value.value());if(a.kind()!=i.kind||!Arrays.equals(a.digest(),i.digest))throw new IllegalStateException("external object differs from original publication");return a;}
    private void requireActiveManagement(TargetKafkaReceiptProtection p){final var topic=BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(p.resource().authenticatedClusterId(),p.resource().receiptTopicUuid()));requireActiveManagement(management(topic),p);}
    private void requireActiveManagement(Root root,TargetKafkaReceiptProtection p){
        final var topic=BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(p.resource().authenticatedClusterId(),p.resource().receiptTopicUuid()));
        requireActiveManagement(Objects.requireNonNull(root.topics.get(topicId(topic)),"original management epoch"),p);
    }
    private void requireActiveManagement(Management m,TargetKafkaReceiptProtection p){
        if(m.state!=ACTIVE||!Arrays.equals(m.artifact.canonicalBytes(),p.managementAuthorityImage())||!Arrays.equals(m.image().managementEpoch(),p.managementEpoch())||!Arrays.equals(m.policy,p.brokerPolicyImage()))throw new IllegalStateException("receipt protection has no original ACTIVE continuous topic-management epoch");}
    private GetResult read(String key){session.run();final var r=client.get(key);session.run();if(r!=null&&(!key.equals(r.key())||r.value()==null||r.version()==null||r.value().length==0||r.value().length>MAX_ROOT_BYTES))throw new IllegalStateException("receipt Registry read lacks exact bounded key/version/value");return r;}
    private GetResult read(String key,BoundedReadBudget budget,int maximumValueBytes){
        if(!budget.beforeRead()||!budget.tryCharge(Bytes.utf8(key).length,maximumValueBytes))throw budget.incomplete();
        final var result=read(key);if(result!=null&&result.value().length>maximumValueBytes)throw new IllegalStateException("catalog object exceeds its bounded exact length");return result;
    }
    private static void requireOpen(Root root,String key){if(root.closures.containsKey(key))throw new IllegalStateException("exact catalog object is closed to new references");}
    private void requireRecoveryDependenciesOpen(Root root,TargetRecoveryProof proof){requireRecoveryDependenciesOpen(root,proof.mapping());}
    private void requireRecoveryDependenciesOpen(Root root,TargetKafkaReceiptMapping mapping){final var binding=mapping.admission().kafkaReceipt();
        requireOpen(root,protectionKey(binding.receiptResource(),mapping.admission().channel().context().evidenceGeneration()));
        requireOpen(root,anchorObjectKey(binding.scanObservationDigest()));}
    private static byte[] preparedUsesDigest(List<PreparedUse> uses){return Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-closed-prepared-uses\0"),
            CanonicalProtobuf.message(out->{for(var use:uses)CanonicalProtobuf.bytes(out,1,use.bytes());}));}
    private static byte[] inventoryDigest(List<CatalogPublication> inventory){return Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-closure-inventory\0"),
            CanonicalProtobuf.message(out->{for(var entry:inventory)CanonicalProtobuf.bytes(out,1,entry.encode());}));}
    private static List<CatalogPublication> publicationInventory(Root root){final var result=new ArrayList<CatalogPublication>();
        for(var entry:root.publications.values())result.add(new CatalogPublication(entry.kind,entry.id));
        for(var entry:root.recoveries.values())result.add(new CatalogPublication(3,entry.id));
        result.sort(Comparator.comparing(CatalogPublication::order));return List.copyOf(result);}
    private void requireLimits(Limits l){if(l.physicalBytes!=limits.physicalBytes||l.physicalRecords!=limits.physicalRecords||l.maximumPublications!=limits.maximumPublications||l.maximumTopics!=limits.maximumTopics||l.maximumPreparedReferences!=limits.maximumPreparedReferences||!Arrays.equals(l.capacitySourceDigest,limits.capacitySourceDigest))throw new IllegalStateException("durable control-storage budget changed without its authenticated deployment transition");}
    private String protectionKey(KafkaReceiptSlotResource r,long generation){if(generation==0)throw new IllegalArgumentException("unassigned evidence generation");return prefix+"/target-kafka-receipt-protection/"+Bytes.hex(ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.KAFKA_RECEIPT_SLOT,r.exactResourceCanonicalBytes()).identityHash())+"/"+Long.toUnsignedString(generation);}
    private static String topicId(BrokerResourceIdentity topic){return Bytes.hex(Bytes.sha256(Bytes.utf8("nereus-delay-target-kafka-receipt-managed-topic\0"),topic.canonicalBytes()));}
    private static Intent findByKey(Root r,String key){for(var i:r.publications.values())if(i.key.equals(key))return i;return null;}
    private static Intent requiredIntent(Root r,TargetKafkaReceiptControlArtifact a){return requiredDigest(r,a.kind(),a.digest());}
    private static Intent requiredDigest(Root r,int kind,byte[] digest){for(var i:r.publications.values())if(i.kind==kind&&Arrays.equals(i.digest,digest))return i;throw new IllegalStateException("original publication intent is missing");}
    private static void requireManagementExact(Management m,TargetKafkaReceiptControlArtifact a){if(!Arrays.equals(m.artifact.canonicalBytes(),a.canonicalBytes()))throw new IllegalStateException("topic management changed its original signed epoch");}
    private static void requireProtectionResource(TargetKafkaReceiptControlArtifact a,KafkaReceiptSlotResource r,long g){if(!a.protection().resource().equals(r)||a.protection().evidenceGeneration()!=g)throw new IllegalStateException("protection key/resource/generation mismatch");}
    private static boolean samePublication(Intent a,Intent b){return a.kind==b.kind&&a.key.equals(b.key)&&a.bytes==b.bytes&&Arrays.equals(a.digest,b.digest)&&Arrays.equals(a.valueHash,b.valueHash)&&Arrays.equals(a.controlId,b.controlId)&&Arrays.equals(a.controlDigest,b.controlDigest)&&Arrays.equals(a.shard,b.shard)&&a.controlScope.equals(b.controlScope);}
    private static byte[] shardBytes(ShardId s){return Bytes.concat(s.routeIncarnation().bytes(),Bytes.u32beBits(s.partition()));}
    private static ShardId shard(byte[] b){return new ShardId(new RouteIncarnation(Arrays.copyOf(b,16)),(int)Bytes.readU32be(b,16));}
    private static List<CanonicalProtobuf.Reader.Field> fields(byte[] b,int[] numbers,String label){final var f=QueryCodecSupport.read(b,label);QueryCodecSupport.requireNumbers(f,numbers,label);return f;}
    private static void requireExact(byte[] a,byte[] b){if(!Arrays.equals(a,b))throw new IllegalStateException("immutable receipt object changed exact bytes");}
}
