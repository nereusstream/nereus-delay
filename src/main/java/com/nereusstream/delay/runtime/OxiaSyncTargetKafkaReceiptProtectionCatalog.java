package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.TargetCatalogRootAuthority;
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

/** One bounded Registry capacity root with a permanent initialization guard and immutable external objects; no distributed-atomic claim. */
public final class OxiaSyncTargetKafkaReceiptProtectionCatalog {
    public static final int INSTALLING = 1, ACTIVE = 2, BROKEN = 3;
    private static final int RESERVED = 1, PUBLISHED = 2, POSSIBLY_SOURCE = 3, ADOPTED = 4;
    private static final int MAX_ROOT_BYTES = 16 * 1024 * 1024;
    private static final int MAX_CAS_ATTEMPTS = 16;
    private final SyncOxiaClient client;
    private final Runnable session;
    private final String prefix;
    private final String ledgerKey;
    private final String guardKey;
    private final Limits limits;
    private final TargetKafkaReceiptProtectionTrust trust;
    private final TargetCatalogRootAuthority rootAuthority;
    private final java.util.function.Supplier<byte[]> storageNamespaceBinding;
    private final java.util.function.Consumer<com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client> retainStorageInput;
    private final java.util.function.BooleanSupplier managedStorageInputOwner;

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
    /** Durable identity of the actual Native52 first used for this one physical-capacity transition. */
    private record MarkerSettlement(byte[] confirmationId,byte[] confirmationHash,byte[] recordDigest,TargetQuotaMutation stamp,byte[] markerVersion){
        MarkerSettlement{
            Bytes.requireLength(confirmationId,32,"confirmation id");Bytes.requireLength(confirmationHash,32,"confirmation hash");
            Bytes.requireLength(recordDigest,32,"confirmed Native record digest");Bytes.requireLength(markerVersion,8,"observed marker version");
            confirmationId=Bytes.copy(confirmationId);confirmationHash=Bytes.copy(confirmationHash);recordDigest=Bytes.copy(recordDigest);markerVersion=Bytes.copy(markerVersion);
            Objects.requireNonNull(stamp).requireSourceApplied();
            if(java.nio.ByteBuffer.wrap(markerVersion).getLong()<0)throw new IllegalArgumentException("invalid persistent marker version");
        }
        byte[] bytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,confirmationId);
            CanonicalProtobuf.bytes(out,3,confirmationHash);CanonicalProtobuf.bytes(out,4,recordDigest);CanonicalProtobuf.bytes(out,5,stamp.canonicalBytes());CanonicalProtobuf.bytes(out,6,markerVersion);});}
        static MarkerSettlement from(TargetEvidenceRetirementStore.AppliedConfirmation applied){
            final var record=Objects.requireNonNull(applied).record();
            return new MarkerSettlement(record.confirmation().systemMutationId(),record.confirmation().mutationHash(),Bytes.sha256(record.canonicalBytes()),
                    record.appliedAt(),record.body().markerEvidence().observedMarkerVersion());
        }
        void requireConfirmation(SystemMutation confirmation,TargetEvidenceDeleteConfirmedBody body){
            requireExact(confirmationId,confirmation.systemMutationId());requireExact(confirmationHash,confirmation.mutationHash());
            requireExact(stamp.mutationDigest(),Bytes.sha256(confirmation.canonicalEnvelope()));
            requireExact(markerVersion,body.markerEvidence().observedMarkerVersion());
        }
        static MarkerSettlement decode(byte[] bytes){
            if(bytes.length>TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+512)throw new IllegalArgumentException("physical settlement reference exceeds bound");
            final var f=fields(bytes,new int[]{1,2,3,4,5,6},"marker settlement");
            if(QueryCodecSupport.uint(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown marker settlement reference");
            final var value=new MarkerSettlement(QueryCodecSupport.fixed(f.get(1),2,32),QueryCodecSupport.fixed(f.get(2),3,32),
                    QueryCodecSupport.fixed(f.get(3),4,32),TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(4),5)),QueryCodecSupport.fixed(f.get(5),6,8));
            QueryCodecSupport.requireCanonical(bytes,value.bytes(),"marker settlement");return value;
        }
    }
    /** Original fingerprints and future metadata remain reserved after physical byte settlement. */
    private record MarkerReservation(TargetEvidenceRetiredMarker marker,MarkerSettlement settlement){
        MarkerReservation(TargetEvidenceRetiredMarker marker){this(marker,null);}
        MarkerReservation{
            Objects.requireNonNull(marker);
            if(settlement!=null){settlement.stamp.requireAfter(marker.intentAppliedAt());
                if(Arrays.equals(settlement.markerVersion,marker.payloadVersion()))throw new IllegalArgumentException("settled marker reuses payload version");}
        }
        byte[] pendingBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,marker.canonicalBytes());});}
        byte[] bytes(){return settlement==null?pendingBytes():CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,2);
            CanonicalProtobuf.bytes(out,2,marker.canonicalBytes());CanonicalProtobuf.bytes(out,3,settlement.bytes());});}
        long metadataReserve(){
            final long sourceBound=marker.intentAppliedAt().source() instanceof KafkaSourcePosition
                    ?11L+4+TargetSourcePosition.MAX_KAFKA_CANONICAL_BYTES+34:TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
            // Full observed-marker evidence plus a same-family Source confirmation reference, before any provider I/O.
            return Math.addExact(pendingBytes().length+32L,Math.addExact(marker.canonicalBytes().length,
                    Math.addExact(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES+512L,sourceBound+512L)));
        }
        long blobDelta(){final long delta=marker.canonicalBytes().length-marker.payloadBytes();return settlement==null?Math.max(0,delta):delta;}
        static MarkerReservation decode(byte[] encoded){
            if(encoded.length==0||encoded.length>TargetEvidenceRetiredMarker.MAX_BYTES+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+544)
                throw new IllegalArgumentException("marker capacity reservation exceeds finite byte bound");
            final var f=QueryCodecSupport.read(encoded,"marker capacity reservation");final long version=QueryCodecSupport.uint(f.getFirst(),1);
            QueryCodecSupport.requireNumbers(f,version==1?new int[]{1,2}:new int[]{1,2,3},"marker capacity reservation");
            if(version!=1&&version!=2)throw new IllegalArgumentException("unknown marker reservation schema");
            final var value=new MarkerReservation(TargetEvidenceRetiredMarker.decode(QueryCodecSupport.nested(f.get(1),2)),
                    version==1?null:MarkerSettlement.decode(QueryCodecSupport.bytes(f.get(2),3)));
            QueryCodecSupport.requireCanonical(encoded,value.bytes(),"marker capacity reservation");return value;
        }
    }
    private static final int MAX_GUARD_BYTES=TargetCatalogRootAuthority.MAX_POLICY_BYTES+128;
    private record GuardBinding(TargetCatalogRootAuthority.Policy policy,int state,long initialVersion,byte[] initialDigest,
            long legacyVersion,byte[] legacyDigest,long pendingGuardVersion){
        GuardBinding{
            Objects.requireNonNull(policy);Bytes.requireLength(initialDigest,32,"initial Root digest");Bytes.requireLength(legacyDigest,32,"original legacy Root digest");
            initialDigest=Bytes.copy(initialDigest);legacyDigest=Bytes.copy(legacyDigest);
            if(state<1||state>2||initialVersion<0||legacyVersion<0||pendingGuardVersion< -1||state==2&&pendingGuardVersion<0
                    ||state==1&&(initialVersion!=0||!Arrays.equals(initialDigest,new byte[32]))||state==2&&Arrays.equals(initialDigest,new byte[32])
                    ||policy.purpose()==TargetCatalogRootAuthority.Purpose.INITIALIZE_FRESH_CATALOG_ROOT
                        &&(legacyVersion!=0||!Arrays.equals(legacyDigest,new byte[32])||pendingGuardVersion<0)
                    ||policy.purpose()==TargetCatalogRootAuthority.Purpose.ADOPT_EXISTING_CATALOG_ROOT_GUARD&&Arrays.equals(legacyDigest,new byte[32]))
                throw new IllegalArgumentException("invalid Catalog Root initialization association");
        }
        GuardBinding bindPending(long version){if(state!=1||pendingGuardVersion!=-1||version<0)throw new IllegalStateException("original guard association is already bound");return new GuardBinding(policy,1,0,new byte[32],legacyVersion,legacyDigest,version);}
        GuardBinding live(long version,byte[] digest){if(state!=1||pendingGuardVersion<0)throw new IllegalStateException("Root initialization lacks its original PENDING guard");return new GuardBinding(policy,2,version,digest,legacyVersion,legacyDigest,pendingGuardVersion);}
        byte[] bytes(){return CanonicalProtobuf.message(o->{CanonicalProtobuf.uint32(o,1,1);CanonicalProtobuf.bytes(o,2,policy.canonicalBytes());
            CanonicalProtobuf.uint32(o,3,state);CanonicalProtobuf.bytes(o,4,Bytes.u64beBits(initialVersion));CanonicalProtobuf.bytes(o,5,initialDigest);
            CanonicalProtobuf.bytes(o,6,Bytes.u64beBits(legacyVersion));CanonicalProtobuf.bytes(o,7,legacyDigest);CanonicalProtobuf.bytes(o,8,Bytes.u64beBits(pendingGuardVersion));});}
        long rootReserve(){return bytes().length+16L;}
        long guardReserve(){return policy.canonicalBytes().length+128L+Bytes.utf8(policy.scope().guardKey()).length;}
        static GuardBinding decode(byte[] b){final var f=fields(b,new int[]{1,2,3,4,5,6,7,8},"Catalog Root association");
            if(QueryCodecSupport.uint32(f.get(0),1)!=1)throw new IllegalArgumentException("unknown Root association version");
            final var value=new GuardBinding(TargetCatalogRootAuthority.Policy.decode(QueryCodecSupport.bytes(f.get(1),2)),QueryCodecSupport.uint32(f.get(2),3),
                    java.nio.ByteBuffer.wrap(QueryCodecSupport.fixed(f.get(3),4,8)).getLong(),QueryCodecSupport.fixed(f.get(4),5,32),
                    java.nio.ByteBuffer.wrap(QueryCodecSupport.fixed(f.get(5),6,8)).getLong(),QueryCodecSupport.fixed(f.get(6),7,32),
                    java.nio.ByteBuffer.wrap(QueryCodecSupport.fixed(f.get(7),8,8)).getLong());
            QueryCodecSupport.requireCanonical(b,value.bytes(),"Root association");return value;}
    }
    private record RootGuard(TargetCatalogRootAuthority.Policy policy,int state,long initialVersion,byte[] initialDigest,long firstLiveVersion,long pendingVersion){
        RootGuard{Objects.requireNonNull(policy);Bytes.requireLength(initialDigest,32,"guard initial Root digest");initialDigest=Bytes.copy(initialDigest);
            if(state<1||state>2||initialVersion<0||firstLiveVersion<0||pendingVersion<0||state==1&&(initialVersion!=0||firstLiveVersion!=0||pendingVersion!=0||!Arrays.equals(initialDigest,new byte[32]))
                    ||state==2&&(firstLiveVersion<=initialVersion||Arrays.equals(initialDigest,new byte[32])))throw new IllegalArgumentException("invalid Catalog Root guard state/version");}
        byte[] bytes(){final byte[] b=CanonicalProtobuf.message(o->{CanonicalProtobuf.uint32(o,1,1);CanonicalProtobuf.bytes(o,2,policy.canonicalBytes());
            CanonicalProtobuf.uint32(o,3,state);CanonicalProtobuf.bytes(o,4,Bytes.u64beBits(initialVersion));CanonicalProtobuf.bytes(o,5,initialDigest);
            CanonicalProtobuf.bytes(o,6,Bytes.u64beBits(firstLiveVersion));CanonicalProtobuf.bytes(o,7,Bytes.u64beBits(pendingVersion));});if(b.length>MAX_GUARD_BYTES)throw new IllegalArgumentException("Root guard exceeds its finite bound");return b;}
        static RootGuard decode(byte[] b){if(b.length>MAX_GUARD_BYTES)throw new IllegalArgumentException("oversize Root guard");
            final var f=fields(b,new int[]{1,2,3,4,5,6,7},"Catalog Root guard");if(QueryCodecSupport.uint32(f.get(0),1)!=1)throw new IllegalArgumentException("unknown Root guard version");
            final var value=new RootGuard(TargetCatalogRootAuthority.Policy.decode(QueryCodecSupport.bytes(f.get(1),2)),QueryCodecSupport.uint32(f.get(2),3),
                    java.nio.ByteBuffer.wrap(QueryCodecSupport.fixed(f.get(3),4,8)).getLong(),QueryCodecSupport.fixed(f.get(4),5,32),
                    java.nio.ByteBuffer.wrap(QueryCodecSupport.fixed(f.get(5),6,8)).getLong(),java.nio.ByteBuffer.wrap(QueryCodecSupport.fixed(f.get(6),7,8)).getLong());QueryCodecSupport.requireCanonical(b,value.bytes(),"Root guard");return value;}
    }
    private record Root(int version,Limits limits,Map<String,Management> topics,Map<String,Intent> publications,Map<String,RecoveryIntent> recoveries,
            Map<String,Closure> closures,long closureSequence,Map<String,MarkerReservation> markers,GuardBinding binding) {
        Root(Limits limits,Map<String,Management> topics,Map<String,Intent> publications){this(1,limits,topics,publications,Map.of(),Map.of(),0,Map.of(),null);}
        Root {topics=Map.copyOf(topics);publications=Map.copyOf(publications);recoveries=Map.copyOf(recoveries);closures=Map.copyOf(closures);markers=Map.copyOf(markers);
            validate(version,limits,topics,publications,recoveries,closures,closureSequence,markers,binding);}
        Root guarded(GuardBinding next){return new Root(Math.max(version,5),limits,topics,publications,recoveries,closures,closureSequence,markers,next);}
        Root topic(Management m){final var map=new TreeMap<>(topics);map.put(m.id(),m);return new Root(version,limits,map,publications,recoveries,closures,closureSequence,markers,binding);}
        Root publication(Intent i){final var map=new TreeMap<>(publications);map.put(Bytes.hex(i.id),i);return new Root(version,limits,topics,map,recoveries,closures,closureSequence,markers,binding);}
        Root recovery(RecoveryIntent i){final var map=new TreeMap<>(recoveries);map.put(Bytes.hex(i.id),i);return new Root(Math.max(version,2),limits,topics,publications,map,closures,closureSequence,markers,binding);}
        Root close(Closure closure){final var map=new TreeMap<>(closures);if(map.putIfAbsent(closure.key,closure)!=null)throw new IllegalStateException("object already closed");
            return new Root(Math.max(version,3),limits,topics,publications,recoveries,map,closure.revision,markers,binding);}
        Root reserveMarker(MarkerReservation desired){
            final var map=new TreeMap<>(markers);final var old=map.putIfAbsent(desired.marker.exactKey(),desired);
            if(old!=null){requireExact(old.marker.canonicalBytes(),desired.marker.canonicalBytes());
                if(old.settlement!=null)throw new ProtectionUnavailable("original marker already has its durable Source settlement; replay that confirmation");return this;}
            return new Root(Math.max(version,4),limits,topics,publications,recoveries,closures,closureSequence,map,binding);
        }
        Root settleMarker(TargetEvidenceRetiredMarker marker,MarkerSettlement settlement){
            final var old=Objects.requireNonNull(markers.get(marker.exactKey()),"original pending marker reservation");
            requireExact(old.marker.canonicalBytes(),marker.canonicalBytes());
            if(old.settlement!=null){requireExact(old.settlement.bytes(),settlement.bytes());return this;}
            final var map=new TreeMap<>(markers);map.put(marker.exactKey(),new MarkerReservation(marker,settlement));
            return new Root(6,limits,topics,publications,recoveries,closures,closureSequence,map,binding);
        }
        long closureReserve(){long bytes=version>=3?64:0;for(var closure:closures.values())bytes=Math.addExact(bytes,closure.encode().length+16L);return bytes;}
        long markerReserve(){long bytes=0;for(var value:markers.values())bytes=Math.addExact(bytes,value.metadataReserve());return bytes;}
        long reservedBytes(){long result=limits.rootReservedBytes();for(var i:recoveries.values())
            result=Math.addExact(result,Math.max(0,i.rootReserve(limits.maximumPreparedReferences)-(4096L+128L*limits.maximumPreparedReferences)));return Math.addExact(result,Math.addExact(Math.addExact(closureReserve(),markerReserve()),binding==null?0:binding.rootReserve()));}
        byte[] bytes(){final var b=CanonicalProtobuf.message(o->{
            CanonicalProtobuf.uint32(o,1,version);CanonicalProtobuf.uint64(o,2,limits.physicalBytes);CanonicalProtobuf.uint32(o,3,limits.physicalRecords);
            CanonicalProtobuf.uint32(o,4,limits.maximumPublications);CanonicalProtobuf.uint32(o,5,limits.maximumTopics);CanonicalProtobuf.uint32(o,6,limits.maximumPreparedReferences);
            CanonicalProtobuf.bytes(o,7,limits.capacitySourceDigest);
            for(var m:new TreeMap<>(topics).values())CanonicalProtobuf.bytes(o,8,m.bytes());
            for(var i:new TreeMap<>(publications).values())CanonicalProtobuf.bytes(o,9,i.encode());
            for(var i:new TreeMap<>(recoveries).values())CanonicalProtobuf.bytes(o,10,i.encode());
            if(version>=3){for(var closure:new TreeMap<>(closures).values())CanonicalProtobuf.bytes(o,11,closure.encode());CanonicalProtobuf.uint64Bits(o,12,closureSequence);}
            if(version>=4)for(var reservation:new TreeMap<>(markers).values())CanonicalProtobuf.bytes(o,13,reservation.bytes());
            if(version>=5)CanonicalProtobuf.bytes(o,14,binding.bytes());
        });if(b.length>reservedBytes()||b.length>MAX_ROOT_BYTES)throw new IllegalArgumentException("receipt catalog root exceeds its reserved physical storage");return b;}
        static Root decode(byte[] b){
            if(b.length==0||b.length>MAX_ROOT_BYTES)throw new IllegalArgumentException("invalid receipt capacity root length");
            final var f=QueryCodecSupport.read(b,"receipt capacity root");if(f.size()<7)throw new IllegalArgumentException("incomplete receipt capacity root");
            QueryCodecSupport.requireNumbers(f.subList(0,7),new int[]{1,2,3,4,5,6,7},"receipt capacity root");
            final int version=QueryCodecSupport.uint32(f.get(0),1);if(version<1||version>6)throw new IllegalArgumentException("unknown receipt capacity root version");
            final var l=new Limits(QueryCodecSupport.uint(f.get(1),2),QueryCodecSupport.uint32(f.get(2),3),QueryCodecSupport.uint32(f.get(3),4),QueryCodecSupport.uint32(f.get(4),5),QueryCodecSupport.uint32(f.get(5),6),QueryCodecSupport.fixed(f.get(6),7,32));
            final var topics=new TreeMap<String,Management>();final var intents=new TreeMap<String,Intent>();int n=7;
            while(n<f.size()&&f.get(n).number()==8){var m=Management.decode(QueryCodecSupport.nested(f.get(n++),8));if(topics.put(m.id(),m)!=null)throw new IllegalArgumentException("duplicate managed topic");}
            while(n<f.size()&&f.get(n).number()==9){var i=Intent.decode(QueryCodecSupport.nested(f.get(n++),9));if(intents.put(Bytes.hex(i.id),i)!=null)throw new IllegalArgumentException("duplicate publication ID");}
            final var recovery=new TreeMap<String,RecoveryIntent>();
            if(version>=2)while(n<f.size()&&f.get(n).number()==10){var i=RecoveryIntent.decode(QueryCodecSupport.nested(f.get(n++),10));if(recovery.put(Bytes.hex(i.id),i)!=null)throw new IllegalArgumentException("duplicate recovery discovery ID");}
            final var closures=new TreeMap<String,Closure>();long sequence=0;
            if(version>=3){while(n<f.size()&&f.get(n).number()==11){final var c=Closure.decode(QueryCodecSupport.nested(f.get(n++),11));if(closures.put(c.key,c)!=null)throw new IllegalArgumentException("duplicate object closure");}
                if(n>=f.size())throw new IllegalArgumentException("missing closure sequence");sequence=QueryCodecSupport.uint64Bits(f.get(n++),12);}
            final var markers=new TreeMap<String,MarkerReservation>();
            if(version>=4)while(n<f.size()&&f.get(n).number()==13){final var value=MarkerReservation.decode(QueryCodecSupport.nested(f.get(n++),13));
                if(markers.put(value.marker.exactKey(),value)!=null||markers.size()>closures.size())throw new IllegalArgumentException("duplicate/excess marker reservation");}
            GuardBinding binding=null;if(version>=5){if(n>=f.size())throw new IllegalArgumentException("missing Root guard association");binding=GuardBinding.decode(QueryCodecSupport.bytes(f.get(n++),14));}
            if(n!=f.size())throw new IllegalArgumentException("unknown receipt root field");final var root=new Root(version,l,topics,intents,recovery,closures,sequence,markers,binding);QueryCodecSupport.requireCanonical(b,root.bytes(),"receipt capacity root");return root;
        }
        private static void validate(int version,Limits l,Map<String,Management> topics,Map<String,Intent> publications,Map<String,RecoveryIntent> recovery,
                Map<String,Closure> closures,long sequence,Map<String,MarkerReservation> markers,GuardBinding binding){
            if(version<1||version>6||(version>=5)!=(binding!=null)||version==1&&!recovery.isEmpty()||version<3&&(!closures.isEmpty()||sequence!=0)
                    ||version>=3&&version<=4&&(closures.isEmpty()||sequence==0)||version>=5&&(closures.isEmpty()!=(sequence==0))||version<4&&!markers.isEmpty()||version==4&&markers.isEmpty()
                    ||version<6&&markers.values().stream().anyMatch(value->value.settlement!=null)
                    ||version==6&&markers.values().stream().noneMatch(value->value.settlement!=null)
                    ||markers.size()>closures.size()||closures.size()>publications.size()+recovery.size()
                    ||topics.size()>l.maximumTopics||publications.size()+(long)recovery.size()>l.maximumPublications)throw new IllegalArgumentException("receipt catalog cardinality bound exceeded");
            long bytes=l.rootReservedBytes();int records=1;final var keys=new HashSet<String>();
            for(var i:publications.values()){if(i.uses.size()>l.maximumPreparedReferences||!keys.add(i.key))throw new IllegalArgumentException("duplicate physical blob or prepared-use bound exceeded");bytes=Math.addExact(bytes,i.physicalBytes());records++;}
            long extraRoot=0;
            if(binding!=null){extraRoot=binding.rootReserve();bytes=Math.addExact(bytes,binding.guardReserve());records++;}
            for(var i:recovery.values()){
                if(i.uses.size()>l.maximumPreparedReferences||!keys.add(i.reference.exactKey()))throw new IllegalArgumentException("duplicate recovery key or prepared-use bound exceeded");
                extraRoot=Math.addExact(extraRoot,Math.max(0,i.rootReserve(l.maximumPreparedReferences)-(4096L+128L*l.maximumPreparedReferences)));
                bytes=Math.addExact(bytes,Math.addExact(i.reference.canonicalValueBytes(),Bytes.utf8(i.reference.exactKey()).length));records++;
            }
            long closureBytes=version>=3?64:0;final var revisions=new HashSet<Long>();
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
            extraRoot=Math.addExact(extraRoot,closureBytes);
            for(var entry:markers.entrySet()){
                final var value=entry.getValue();final var marker=value.marker;final var closure=Objects.requireNonNull(closures.get(entry.getKey()),"marker original closure");
                if(!entry.getKey().equals(marker.exactKey())||marker.closureRevision()!=closure.revision)
                    throw new IllegalArgumentException("marker changed original closure/key");
                closure.requireFingerprint(marker.exactKey(),marker.objectDigest(),marker.payloadDigest(),marker.payloadBytes());
                requireExact(marker.payloadVersion(),Bytes.u64beBits(closure.blobVersion));requireExact(marker.closureDigest(),closure.digest());
                requireExact(marker.publicationId(),closure.publicationId);requireExact(marker.publicationOriginDigest(),closure.originDigest);
                requireExact(marker.capacityOwner().catalogNamespaceId(),closure.namespaceId);requireExact(marker.capacityOwner().capacitySourceDigest(),l.capacitySourceDigest);
                final var management=TargetKafkaReceiptManagementImage.decode(topics.get(Bytes.hex(closure.managementTopicId)).artifact.image());
                requireExact(marker.deploymentRootDigest(),management.deploymentRootDigest());
                extraRoot=Math.addExact(extraRoot,value.metadataReserve());bytes=Math.addExact(bytes,value.blobDelta());
            }
            bytes=Math.addExact(bytes,extraRoot);
            if(l.rootReservedBytes()+extraRoot>MAX_ROOT_BYTES)throw new IllegalStateException("evidence intent/closure root window exhausted");
            if(bytes>l.physicalBytes||records>l.physicalRecords)throw new IllegalStateException("durable receipt control-storage capacity exhausted");
        }
    }
    public OxiaSyncTargetKafkaReceiptProtectionCatalog(OxiaSyncOwnerLeaseBackend.ClientHandle handle,String keyPrefix,
            TargetKafkaReceiptProtectionTrust trust,Limits limits){
        this(handle,keyPrefix,trust,limits,null);
    }
    public OxiaSyncTargetKafkaReceiptProtectionCatalog(OxiaSyncOwnerLeaseBackend.ClientHandle handle,String keyPrefix,
            TargetKafkaReceiptProtectionTrust trust,Limits limits,TargetCatalogRootAuthority rootAuthority){
        client=Objects.requireNonNull(handle,"session-fenced Registry handle").client();session=handle.backend()::assertConnectedSession;
        storageNamespaceBinding=()->handle.backend().requireOriginalStorageNamespaceBinding(handle.client());
        retainStorageInput=handle.backend()::retainClosedClientInputUntilTermination;
        managedStorageInputOwner=handle.backend()::hasManagedClientInputOwner;
        this.trust=Objects.requireNonNull(trust,"original CONTROL trust");this.limits=Objects.requireNonNull(limits,"explicit durable capacity configuration");
        this.rootAuthority=rootAuthority;
        prefix=Objects.requireNonNull(keyPrefix,"Registry prefix");
        if(prefix.isBlank()||prefix.endsWith("/")||prefix.indexOf('\0')>=0||prefix.getBytes(StandardCharsets.UTF_8).length>1024||!Normalizer.isNormalized(prefix,Normalizer.Form.NFC))throw new IllegalArgumentException("invalid receipt Registry prefix");
        ledgerKey=prefix+"/target-kafka-receipt-storage";
        guardKey=prefix+"/target-kafka-receipt-storage-guard";
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
        adopt(proof,session,null);
    }
    private void adopt(TargetExternalEvidenceStore.Adoption proof,Runnable current,BoundedReadBudget reads){
        final var mapping=proof.mapping();final byte[] id=SystemMutation.computeSystemMutationId(mapping.admission().shard(),SystemMutationType.TARGET_PUBLISH_ADMISSION,mapping.admission().publishAttemptId(),mapping.mutationHash());
        update(r->{var next=r;for(var owner:List.of(proof.protection(),proof.anchor())){final var i=Objects.requireNonNull(findByKey(next,owner.exactKey()),"original publication intent");
            requireControlOwner(i,owner);
            if(i.uses.stream().noneMatch(u->Arrays.equals(u.id,id)&&Arrays.equals(u.hash,mapping.mutationHash())&&u.retryUntil==mapping.admission().retryUntilEpochMs()))throw new IllegalStateException("Source adoption lacks its original prepared ID/hash/retry binding");
            next=next.publication(i.adopt(Bytes.sha256(mapping.mutation().source().canonicalBytes())));}return next;},current,reads);
    }
    private static void requireControlOwner(Intent i,TargetExternalEvidenceOwner owner){
        if(i.kind!=owner.kind()||!i.key.equals(owner.exactKey())||!Arrays.equals(i.digest,owner.artifactDigest())
                ||!Arrays.equals(i.valueHash,owner.valueDigest())||i.bytes!=owner.canonicalValueBytes()
                ||!Arrays.equals(i.controlDigest,owner.originalControlDigest())
                ||!i.controlScope.shard().equals(owner.primaryOwner().identity().shard())
                ||!i.controlScope.target().equals(owner.primaryOwner().identity().target())
                ||owner.kind()==TargetKafkaReceiptControlArtifact.PROTECTION&&!i.controlScope.equals(owner.primaryOwner().identity()))
            throw new IllegalStateException("CONTROL object changed original publication/frozen scope");
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
    public static long closureInputWindowBytes(){return 16L*(MAX_ROOT_BYTES+MAX_GUARD_BYTES+TargetRecoveryProofRef.MAX_PROOF_BYTES);}

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
        public byte[] objectDigest(){return Bytes.copy(closure.objectDigest);}
        public long objectBytes(){return closure.valueBytes;}
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
        /** Actual mapping/ref joins remain Native obligations; this only binds immutable CONTROL object identity. */
        public TargetExternalEvidenceObjectResource controlResource(TargetExternalEvidenceObjectResource.Provenance provenance){
            final TargetExternalEvidenceOwner owner;
            if(provenance instanceof TargetExternalEvidenceObjectResource.Protection p)owner=p.owner();
            else if(provenance instanceof TargetExternalEvidenceObjectResource.Anchor a)owner=a.owner();
            else throw new IllegalArgumentException("CONTROL copy needs original adopted CONTROL provenance");
            if(closure.subtype!=owner.kind())throw new IllegalArgumentException("CONTROL closure changed original subtype");
            closure.requireFingerprint(owner.exactKey(),owner.artifactDigest(),owner.valueDigest(),owner.canonicalValueBytes());
            final var frozen=owner.primaryOwner();
            return new TargetExternalEvidenceObjectResource(owner.kind()==TargetKafkaReceiptControlArtifact.PROTECTION
                    ?TargetExternalEvidenceObjectResource.Subtype.CONTROL_PROTECTION:TargetExternalEvidenceObjectResource.Subtype.CONTROL_ANCHOR,
                    closure.namespaceId,deploymentRootDigest,frozen.identity().shard(),frozen.tenantScope(),frozen.recoveryLineage(),
                    closure.key,closure.objectDigest,closure.valueDigest,closure.valueBytes,Bytes.u64beBits(closure.blobVersion),provenance);
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
        return closePresentObjectReferences(owner.reference().exactKey(),owner.reference().proofDigest(),proofTrust,reads,current,applied,null);
    }
    /** Historical API lacks complete Native use/Attempt preflight and cannot freeze recovery dependencies. */
    public ClosedObject closeAdoptedControlReferences(TargetExternalEvidenceStore.Adoption applied,int kind,
            BoundedReadBudget reads,Runnable current){
        throw new IllegalStateException("CONTROL closure requires complete Native use/Attempt preflight for the captured Root");
    }
    /** An immutable actual Root cut, used only while its original owner retains the bounded I/O responsibility. */
    public static final class ControlReferenceSnapshot {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final TargetExternalEvidenceOwner owner;
        private final byte[] rootDigest;
        private final List<ClosedPreparedUse> uses;
        private ControlReferenceSnapshot(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,Root root,
                TargetExternalEvidenceOwner owner,Intent intent){
            this.catalog=catalog;this.owner=owner;rootDigest=Bytes.sha256(root.bytes());
            uses=intent.uses.stream().map(u->new ClosedPreparedUse(u.id,u.hash,u.retryUntil)).toList();
        }
        public TargetExternalEvidenceOwner owner(){return owner;}
        public List<ClosedPreparedUse> preparedUses(){return uses;}
    }
    public ControlReferenceSnapshot captureControlReferences(TargetExternalEvidenceStore.Adoption applied,int kind,
            BoundedReadBudget reads,Runnable current){
        Objects.requireNonNull(applied,"actual Native APPLIED CONTROL object adoption");
        final var owner=kind==TargetKafkaReceiptControlArtifact.PROTECTION?applied.protection()
                :kind==TargetKafkaReceiptControlArtifact.ANCHOR?applied.anchor():null;
        if(owner==null)throw new IllegalArgumentException("CONTROL closure requires protection or anchor");
        return captureControlReferences(owner,reads,current);
    }
    /** Selection data only; close still requires complete actual Native preflight for this captured Root. */
    public ControlReferenceSnapshot captureControlReferences(TargetExternalEvidenceOwner owner,
            BoundedReadBudget reads,Runnable current){
        Objects.requireNonNull(owner);
        current.run();final var root=readRoot(reads).root;
        final var intent=Objects.requireNonNull(findByKey(root,owner.exactKey()),"original CONTROL publication");
        requireControlOwner(intent,owner);current.run();return new ControlReferenceSnapshot(this,root,owner,intent);
    }
    /** Only a complete actual Native preflight for this exact unchanged Root can freeze CONTROL dependencies. */
    public ClosedObject closeAdoptedControlReferences(TargetExternalEvidenceStore.ControlClosure preflight,
            BoundedReadBudget reads,Runnable current){
        final var snapshot=Objects.requireNonNull(preflight,"actual Native CONTROL closure preflight").snapshot();
        if(snapshot.catalog!=this)throw new IllegalArgumentException("CONTROL closure belongs to another Catalog");
        return closePresentObjectReferences(snapshot.owner.exactKey(),snapshot.owner.artifactDigest(),null,reads,current,null,snapshot);
    }
    private ClosedObject closePresentObjectReferences(String exactKey,byte[] objectDigest,TargetRecoveryProofTrust proofTrust,
            BoundedReadBudget reads,Runnable current,TargetRecoveryEvidenceStore.Adoption applied,ControlReferenceSnapshot controlSnapshot){
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
            if(controlSnapshot!=null){
                final var owner=controlSnapshot.owner;
                owner.requireObject(subtype,objectDigest,exactKey,stored.value());
            }
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
                final byte[] rootCutDigest=Bytes.sha256(root.bytes());
                if(controlSnapshot!=null){
                    requireExact(rootCutDigest,controlSnapshot.rootDigest);
                    final var original=Objects.requireNonNull(findByKey(root,controlSnapshot.owner.exactKey()));
                    requireControlOwner(original,controlSnapshot.owner);
                    root=root.publication(original.adopt(Bytes.sha256(controlSnapshot.owner.createdAt().source().canonicalBytes())));
                }
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
                        rootCutDigest,management.image().topicIdentity()));
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
    /** Actual physical reservation owner, independent of a logical Target adoption or any deletion eligibility. */
    public static final class CapacityOwner {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final TargetCatalogCapacityOwnerRef reference;
        private final byte[] publicationId,originDigest;
        private CapacityOwner(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,TargetCatalogCapacityOwnerRef reference,Closure closure){
            this.catalog=catalog;this.reference=reference;publicationId=Bytes.copy(closure.publicationId);originDigest=Bytes.copy(closure.originDigest);
        }
        public TargetCatalogCapacityOwnerRef reference(){return reference;}
        public byte[] publicationId(){return Bytes.copy(publicationId);}
        public byte[] originDigest(){return Bytes.copy(originDigest);}
        public void requireCatalog(OxiaSyncTargetKafkaReceiptProtectionCatalog expected){
            if(catalog!=expected)throw new IllegalArgumentException("capacity owner belongs to another actual Catalog");
        }
    }
    /** Reads retained Root membership/reservation, original management authorization and actual SDK storage namespace. */
    public CapacityOwner readCapacityOwner(ClosedObject original,BoundedReadBudget reads,Runnable current){
        if(Objects.requireNonNull(original).catalog!=this)throw new IllegalArgumentException("closure belongs to another Catalog");
        current.run();final var root=readRoot(reads).root;
        final var actual=closedSnapshot(root,original.exactKey(),original.closure.objectDigest);
        requireExact(actual.canonicalBytes(),original.canonicalBytes());
        final var reference=new TargetCatalogCapacityOwnerRef(actual.namespaceId(),ledgerKey,
                storageNamespaceBinding.get(),root.limits.capacitySourceDigest);
        current.run();if(!reads.beforeTimedWork())throw reads.incomplete();return new CapacityOwner(this,reference,actual.closure);
    }
    /** Durable preparation only; it is neither a provider observation nor a no-recreate/confirmation receipt. */
    public static final class ReservedMarker {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final TargetEvidenceRetiredMarker marker;
        private ReservedMarker(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,TargetEvidenceRetiredMarker marker){this.catalog=catalog;this.marker=marker;}
        public TargetEvidenceRetiredMarker marker(){return marker;}
        public void requireCatalog(OxiaSyncTargetKafkaReceiptProtectionCatalog expected){
            if(catalog!=expected)throw new IllegalArgumentException("marker reservation belongs to another original Catalog");
        }
    }
    /** Actual provider-operation observation only. Pending metadata or an evidence DTO cannot construct this receipt. */
    public static final class ObservedRetiredMarker {
        public enum ConditionalResult { CONDITION_SUCCEEDED, CONDITION_REJECTED_EXACT_MARKER }
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final TargetEvidenceRetiredMarkerEvidence evidence;
        private final TargetExternalEvidenceObjectResource resource;
        private final TargetEvidenceRetirementContext context;
        private final ResourceDeleteConfirmedBody.RetireIntentRef intent;
        private final long retainedBytes;
        private final ConditionalResult conditionalResult;
        private ObservedRetiredMarker(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
                ReservedMarker reserved,TargetEvidenceRetirementStore.AppliedIntent applied,TargetEvidenceRetiredMarkerEvidence evidence,ConditionalResult conditionalResult){
            this.catalog=catalog;this.evidence=Objects.requireNonNull(evidence);
            this.conditionalResult=Objects.requireNonNull(conditionalResult);
            Objects.requireNonNull(reserved).requireCatalog(catalog);requireExact(reserved.marker.canonicalBytes(),evidence.marker().canonicalBytes());
            final var actual=Objects.requireNonNull(applied).record();resource=actual.copy().resource();context=actual.body().context();
            intent=new ResourceDeleteConfirmedBody.RetireIntentRef(actual.intent().systemMutationId(),actual.intent().mutationHash(),resource.exactIdentity().identityHash(),0);
            evidence.marker().requireBindings(resource,context,intent);actual.copy().requireMarker(evidence.marker());
            if(!actual.appliedAt().equals(evidence.marker().intentAppliedAt()))throw new IllegalArgumentException("provider observation changed actual Native intent stamp");
            retainedBytes=Math.addExact(evidence.canonicalBytes().length,Math.addExact(resource.canonicalBytes().length,context.canonicalBytes().length+128L));
        }
        public TargetEvidenceRetiredMarkerEvidence evidence(){return evidence;}
        public ConditionalResult conditionalResult(){return conditionalResult;}
        public void requireCatalog(OxiaSyncTargetKafkaReceiptProtectionCatalog expected){if(catalog!=expected)throw new IllegalArgumentException("provider receipt belongs to another original Catalog");}
        /** Pure local matching for Source prepare/commit; performs no provider or Registry I/O. */
        public void requireConfirmation(TargetEvidenceDeleteConfirmedBody body){
            if(!body.markerRetained())throw new IllegalArgumentException("retained-marker receipt cannot authenticate deletion/absence");
            requireExact(body.resource().canonicalBytes(),resource.canonicalBytes());requireExact(body.context().canonicalBytes(),context.canonicalBytes());
            requireExact(body.intent().canonicalBytes(),intent.canonicalBytes());requireExact(body.markerEvidence().canonicalBytes(),evidence.canonicalBytes());
        }
        public long retainedBytes(){return retainedBytes;}
    }
    /** A well-formed historical declaration can still lack an implemented exact authority mapping. */
    public static final class ProtectionUnavailable extends IllegalStateException {
        private static final long serialVersionUID=1L;
        public ProtectionUnavailable(String message){super(message);}
    }
    /** Original durable Source witness + actual protected current facts. Never a new conditional-operation receipt. */
    public static final class HistoricalConfirmation implements AutoCloseable {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ConfirmationRead source;
        private final com.nereusstream.delay.ownership.SourceReplayMutation entry;
        private final byte[] envelope,body;
        private final com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission;
        private final com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.EvidenceRecoveryProtection recovery;
        private final TargetEvidenceRetirementStore.AppliedIntent intent;
        private final com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Permit permission;
        private final long retainedBytes;
        private boolean closed;
        private HistoricalConfirmation(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
                com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ConfirmationRead source,
                com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
                com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.EvidenceRecoveryProtection recovery,
                TargetEvidenceRetirementStore.AppliedIntent intent,com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Permit permission){
            this.catalog=catalog;this.source=source;entry=source.entry();envelope=Bytes.sha256(entry.mutation().canonicalEnvelope());
            body=Bytes.sha256(entry.mutation().canonicalBody());this.admission=admission;this.recovery=recovery;this.intent=intent;this.permission=permission;
            retainedBytes=Math.addExact(4096L,Math.addExact(intent.record().canonicalBytes().length,
                    Math.addExact(entry.mutation().canonicalBodyLength(),(long)entry.mutation().authorIdentityLength())));
        }
        public long retainedBytes(){return retainedBytes;}
        public void requireCatalog(OxiaSyncTargetKafkaReceiptProtectionCatalog actual){if(catalog!=actual)throw new IllegalArgumentException("historical confirmation belongs to another original Root");}
        public void requireCurrent(){if(closed)throw new IllegalStateException("historical confirmation protection closed");
            source.requireCurrent();admission.requireCurrent();permission.requireCurrent();recovery.requireFor(intent);}
        public void requireEntry(com.nereusstream.delay.ownership.SourceReplayMutation actual){requireCurrent();
            requireSource(actual.mutation(),actual.position());
            if(!Objects.equals(entry.sourceConnectionGeneration(),actual.sourceConnectionGeneration())
                    ||!Arrays.equals(entry.guardAttestationDigest(),actual.guardAttestationDigest()))throw new IllegalArgumentException("historical Source connection proof changed");}
        public void requireSource(SystemMutation actual,SourcePosition position){requireCurrent();
            requireExact(envelope,Bytes.sha256(actual.canonicalEnvelope()));requireExact(entry.position().canonicalBytes(),position.canonicalBytes());}
        public void requireConfirmation(TargetEvidenceDeleteConfirmedBody actual){requireCurrent();requireExact(body,Bytes.sha256(actual.canonicalBytes()));}
        @Override public void close(){if(!closed){permission.close();closed=true;}}
    }
    /** No put, new UUID, new time, hash rewriting or signing. The real Source frame is the historical witness. */
    public HistoricalConfirmation authenticateHistoricalConfirmation(
            com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ConfirmationRead source,
            TargetEvidenceRetirementStore.SourceHistory history,
            com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
            List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget catalogReads,BoundedReadBudget nativeReads,BoundedReadBudget recoveryReads,Runnable current){
        requireManagedMarkerInputOwner();final var entry=Objects.requireNonNull(source).entry();
        final var selected=entry.mutation();final var body=TargetEvidenceDeleteConfirmedBody.decode(selected.canonicalBody());
        if(!body.markerRetained())throw new IllegalArgumentException("historical retained-marker confirmation required");
        for(var reads:List.of(catalogReads,nativeReads,recoveryReads))if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite historical reads required");
        current.run();source.requireCurrent();final var grant=history.select(body.resource(),selected,entry.position()).orElseThrow(
                ()->new ProtectionUnavailable("complete historical DELETE_CONFIRMATION grant is unavailable"));
        if(grant.action()!=TargetRetirementAuthorizationHistory.Action.DELETE_CONFIRMATION||!selected.verifySignature(grant.writerKey()))
            throw new ProtectionUnavailable("historical confirmation signature/purpose is unavailable");
        final var originalCatalog=source.catalog();admission.requireCurrent();
        originalCatalog.requireHistoricalRecoveryBinding(admission.qualifiedHistory().history(),body.resource(),entry.position());
        final var intent=source.readIntent(nativeReads,history,grant);requireSupportedProtectionDeclarations(intent.record().intent());
        final var root=readRoot(catalogReads).root;
        requireExact(root.binding.policy.scope().canonicalBytes(),admission.qualifiedHistory().history().scope().canonicalBytes());
        final var marker=body.markerEvidence().marker();intent.record().copy().requireMarker(marker);
        final var closure=closedSnapshot(root,marker.exactKey(),marker.objectDigest());closure.requireObjectFingerprint(body.resource());
        requireExact(closure.canonicalBytes(),intent.record().copy().closureImage());
        final var pending=Objects.requireNonNull(root.markers.get(marker.exactKey()),"historical durable marker reservation");
        requireExact(pending.marker.canonicalBytes(),marker.canonicalBytes());
        if(pending.settlement!=null)pending.settlement.requireConfirmation(selected,body);
        requireExact(readCapacityOwner(closure,catalogReads,current).reference().canonicalBytes(),marker.capacityOwner().canonicalBytes());
        final var recovery=originalCatalog.captureHistoricalRecoveryProtection(intent,selected,entry.position(),images,recoveryReads,
                ()->{current.run();source.requireCurrent();admission.requireCurrent();});
        final var original=Objects.requireNonNull(root.topics.get(Bytes.hex(closure.closure.managementTopicId)),"original historical read management");
        final var permission=trust.currentManagement(original.artifact,Action.OBSERVE);
        try{
            permission.requireCurrent();admission.requireCurrent();current.run();
            final var actual=persistent(read(marker.exactKey(),catalogReads,Math.toIntExact(Math.max(marker.payloadBytes(),marker.canonicalBytes().length))));
            if(actual==null)throw new ProtectionUnavailable("historical persistent marker is unavailable");
            requireExact(actual.value(),marker.canonicalBytes());requireExact(Bytes.u64beBits(actual.version().versionId()),body.markerEvidence().observedMarkerVersion());
            final var after=readRoot(catalogReads).root;
            requireExact(after.binding.policy.scope().canonicalBytes(),root.binding.policy.scope().canonicalBytes());
            requireExact(closedSnapshot(after,marker.exactKey(),marker.objectDigest()).canonicalBytes(),closure.canonicalBytes());
            final var afterReservation=Objects.requireNonNull(after.markers.get(marker.exactKey()));
            requireExact(afterReservation.marker.canonicalBytes(),marker.canonicalBytes());
            if(afterReservation.settlement!=null)afterReservation.settlement.requireConfirmation(selected,body);
            recovery.requireCurrent(recoveryReads,current);source.requireCurrent();history.current().run();admission.requireCurrent();permission.requireCurrent();current.run();
            if(!catalogReads.beforeTimedWork()||!nativeReads.beforeTimedWork())throw new ProtectionUnavailable("historical confirmation read deadline exhausted");
            final var result=new HistoricalConfirmation(this,source,admission,recovery,intent,permission);result.requireEntry(entry);return result;
        }catch(RuntimeException|Error failure){permission.close();throw failure;}
    }
    public static void requireSupportedProtectionDeclarations(SystemMutation selected){
        final var body=TargetEvidenceRetireIntentBody.decode(Objects.requireNonNull(selected).canonicalBody());
        if(selected.type()!=SystemMutationType.RESOURCE_RETIRE_INTENT||!body.protections().references().isEmpty())
            throw new ProtectionUnavailable("declared ProtectionRef ID/generation has no defined original authority mapping; physical retirement unavailable");
    }

    /** Original Root runtime composite, never a caller boolean or a declaration-derived erase permit. */
    public static final class PayloadRetirementProtection implements AutoCloseable {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private final com.nereusstream.delay.ownership.TargetWorkerSourceStartup source;
        private final com.nereusstream.delay.ownership.TargetWorkerShardRuntime worker;
        private final com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.ReceiptManagement management;
        private final SystemMutation selected;
        private final TargetEvidenceRetirementStore.AppliedIntent applied;
        private final ReservedMarker reserved;
        private final com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission;
        private final com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.EvidenceRecoveryProtection recovery;
        private final com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Permit permission;
        private boolean closed;
        private PayloadRetirementProtection(OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
                com.nereusstream.delay.ownership.TargetWorkerSourceStartup source,com.nereusstream.delay.ownership.TargetWorkerShardRuntime worker,
                com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.ReceiptManagement management,SystemMutation selected,
                TargetEvidenceRetirementStore.AppliedIntent applied,ReservedMarker reserved,
                com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
                com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.EvidenceRecoveryProtection recovery,
                com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Permit permission){
            this.catalog=catalog;this.source=source;this.worker=worker;this.management=management;this.selected=selected;
            this.applied=applied;this.reserved=reserved;this.admission=admission;this.recovery=recovery;this.permission=permission;
        }
        public TargetEvidenceRetirementStore.AppliedIntent applied(){requireHeldCurrent();return applied;}
        /** Only held/current checks here. Full Native/Registry reauthentication is explicit and budgeted below. */
        public void requireHeldCurrent(){
            if(closed)throw new IllegalStateException("original physical protection is closed");
            management.requireOriginalEvidenceCatalog(catalog);source.requireEvidenceRetirementReadCurrent(worker,management);
            admission.requireCurrent();permission.requireCurrent();recovery.requireFor(applied);
        }
        public void requireCurrent(BoundedReadBudget catalogReads,BoundedReadBudget nativeReads,BoundedReadBudget recoveryReads,Runnable current){
            current.run();requireHeldCurrent();
            final var actual=source.readEvidenceRetirementIntent(worker,management,selected,nativeReads);
            requireExact(actual.record().canonicalBytes(),applied.record().canonicalBytes());
            final var root=catalog.readRoot(catalogReads).root;
            requireExact(root.binding.policy.scope().canonicalBytes(),admission.qualifiedHistory().history().scope().canonicalBytes());
            final var closed=catalog.closedSnapshot(root,reserved.marker.exactKey(),reserved.marker.objectDigest());
            closed.requireObjectFingerprint(applied.record().copy().resource());
            requireExact(closed.canonicalBytes(),applied.record().copy().closureImage());
            final var pending=Objects.requireNonNull(root.markers.get(reserved.marker.exactKey()),"original durable marker reservation");
            if(pending.settlement!=null)throw new ProtectionUnavailable("original Source confirmation is already durably settled; replay it instead of creating a new observation");
            requireExact(pending.marker.canonicalBytes(),reserved.marker.canonicalBytes());
            recovery.requireAuthority(source.originalEvidenceRecoveryCatalog(worker));recovery.requireCurrent(recoveryReads,()->{current.run();requireHeldCurrent();});
            current.run();requireHeldCurrent();
            if(!catalogReads.beforeTimedWork()||!nativeReads.beforeTimedWork())throw new ProtectionUnavailable("physical protection reauthentication exceeded its deadline");
        }
        /** Source/image/SDK ownership remains with the original holders; Root separately closes/transfers actual admission. */
        @Override public void close(){if(!closed){permission.close();closed=true;}}
    }

    public PayloadRetirementProtection preparePayloadRetirementProtection(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup source,com.nereusstream.delay.ownership.TargetWorkerShardRuntime worker,
            com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.ReceiptManagement management,SystemMutation selected,ReservedMarker reserved,
            com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
            List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget catalogReads,BoundedReadBudget nativeReads,BoundedReadBudget recoveryReads,Runnable current){
        requireSupportedProtectionDeclarations(selected);requireManagedMarkerInputOwner();
        for(var reads:List.of(catalogReads,nativeReads,recoveryReads))if(reads.maxBytes()==Long.MAX_VALUE)
            throw new IllegalArgumentException("finite full physical protection reads required");
        management.requireOriginalEvidenceCatalog(this);source.requireEvidenceRetirementReadCurrent(worker,management);current.run();
        reserved.requireCatalog(this);admission.requireCurrent();
        final var applied=source.readEvidenceRetirementIntent(worker,management,selected,nativeReads);
        final var resource=applied.record().copy().resource();final var marker=reserved.marker;
        applied.record().copy().requireMarker(marker);
        if(!applied.record().appliedAt().equals(marker.intentAppliedAt()))throw new ProtectionUnavailable("marker changed actual intent Source stamp");
        requireExact(marker.intentId(),applied.record().intent().systemMutationId());requireExact(marker.intentHash(),applied.record().intent().mutationHash());
        final var root=readRoot(catalogReads).root;
        requireExact(root.binding.policy.scope().canonicalBytes(),admission.qualifiedHistory().history().scope().canonicalBytes());
        final var closure=closedSnapshot(root,resource.exactKey(),resource.objectDigest());closure.requireObjectFingerprint(resource);
        requireExact(closure.canonicalBytes(),applied.record().copy().closureImage());
        final var capacity=readCapacityOwner(closure,catalogReads,current);requireExact(capacity.reference().canonicalBytes(),marker.capacityOwner().canonicalBytes());
        final var actualCatalog=source.originalEvidenceRecoveryCatalog(worker);
        final var recovery=actualCatalog.captureEvidenceRecoveryProtection(applied,images,recoveryReads,
                ()->{current.run();source.requireEvidenceRetirementReadCurrent(worker,management);admission.requireCurrent();});
        final var original=Objects.requireNonNull(root.topics.get(Bytes.hex(closure.closure.managementTopicId)),"original whole-topic physical permission");
        final var permission=trust.currentManagement(original.artifact,Action.RETIRE_EVIDENCE_OBJECT_PAYLOAD);
        final var protection=new PayloadRetirementProtection(this,source,worker,management,selected,applied,reserved,admission,recovery,permission);
        try{protection.requireCurrent(catalogReads,nativeReads,recoveryReads,current);return protection;}
        catch(RuntimeException|Error failure){protection.close();throw failure;}
    }

    /** One actual dispatch followed by an exact persistent read. UNKNOWN never enters a matching-read success catch. */
    public ObservedRetiredMarker retireEvidencePayload(PayloadRetirementProtection protection,
            BoundedReadBudget catalogReads,BoundedReadBudget nativeReads,BoundedReadBudget recoveryReads,
            java.util.function.Supplier<TrustedUtcIntervalEvidence> observedTime,Runnable current){
        if(Objects.requireNonNull(protection).catalog!=this)throw new IllegalArgumentException("physical protection belongs to another Catalog");
        protection.requireCurrent(catalogReads,nativeReads,recoveryReads,current);
        final var marker=protection.reserved.marker;final byte[] value=marker.canonicalBytes();
        final var before=persistent(read(marker.exactKey(),catalogReads,Math.toIntExact(Math.max(marker.payloadBytes(),value.length))));
        final boolean absent=before==null;
        if(before!=null&&!Arrays.equals(before.value(),value)){
            if(before.version().versionId()!=java.nio.ByteBuffer.wrap(marker.payloadVersion()).getLong()
                    ||before.value().length!=marker.payloadBytes()||!Arrays.equals(Bytes.sha256(before.value()),marker.payloadDigest()))
                throw new ProtectionUnavailable("actual payload changed original exact raw/version fingerprint");
        }
        final var kind=absent?TargetEvidenceRetiredMarkerEvidence.RequestKind.CLAIM_ABSENT_KEY:TargetEvidenceRetiredMarkerEvidence.RequestKind.CAS_PAYLOAD_VERSION;
        final var nonce=UUID.randomUUID();
        final byte[] request=Bytes.sha256(Bytes.utf8("nereus-delay-evidence-marker-local-dispatch-v1\0"),
                marker.capacityOwner().storageNamespaceBinding(),Bytes.lp32(Bytes.utf8(marker.exactKey())),
                Bytes.u32be(kind.wireValue()),absent?new byte[0]:marker.payloadVersion(),Bytes.sha256(value),Bytes.u64be(value.length),
                Bytes.u64beBits(nonce.getMostSignificantBits()),Bytes.u64beBits(nonce.getLeastSignificantBits()));
        protection.requireCurrent(catalogReads,nativeReads,recoveryReads,current);
        io.oxia.client.api.PutResult put=null;int category;
        try{
            session.run();current.run();protection.requireHeldCurrent();
            put=client.put(marker.exactKey(),value,absent?Set.of(PutOption.IfRecordDoesNotExist):
                    Set.of(PutOption.IfVersionIdEquals(java.nio.ByteBuffer.wrap(marker.payloadVersion()).getLong())));
            if(put==null||!marker.exactKey().equals(put.key())||put.version()==null||put.version().versionId()<0
                    ||put.version().sessionId().isPresent()||put.version().clientIdentifier().isPresent())
                throw new IllegalStateException("conditional marker response has invalid persistent identity");
            category=1;
        }catch(UnexpectedVersionIdException rejected){
            if(absent)throw new IllegalStateException("IfAbsent failed without its matching known condition result",rejected);category=2;
        }catch(KeyAlreadyExistsException rejected){
            if(!absent)throw new IllegalStateException("payload-version CAS failed without its matching known condition result",rejected);category=3;
        }
        // Runtime/timeout/cancellation exits above. Only success or the matching known conditional rejection reaches GET.
        session.run();current.run();protection.requireHeldCurrent();
        final var after=persistent(read(marker.exactKey(),catalogReads,Math.toIntExact(Math.max(marker.payloadBytes(),value.length))));
        if(after==null||Arrays.equals(Bytes.u64beBits(after.version().versionId()),marker.payloadVersion()))
            throw new IllegalStateException("actual retained marker/version is unavailable");
        requireExact(after.value(),value);
        if(category==1&&put.version().versionId()!=after.version().versionId())throw new IllegalStateException("successful marker version changed before exact observation");
        final byte[] response=Bytes.sha256(Bytes.utf8("nereus-delay-evidence-marker-conditional-response-v1\0"),
                Bytes.u32be(category),request,Bytes.lp32(Bytes.utf8(after.key())),Bytes.sha256(after.value()),
                Bytes.u64be(after.value().length),Bytes.u64beBits(after.version().versionId()),
                category==1?Bytes.u64beBits(put.version().versionId()):new byte[0]);
        final var time=Objects.requireNonNull(observedTime.get(),"actual original Source observation time");
        protection.requireCurrent(catalogReads,nativeReads,recoveryReads,current);
        final var evidence=new TargetEvidenceRetiredMarkerEvidence(marker,Bytes.sha256(after.value()),after.value().length,
                Bytes.u64beBits(after.version().versionId()),kind,absent?new byte[0]:marker.payloadVersion(),request,response,time);
        return new ObservedRetiredMarker(this,protection.reserved,protection.applied,evidence,category==1?
                ObservedRetiredMarker.ConditionalResult.CONDITION_SUCCEEDED:ObservedRetiredMarker.ConditionalResult.CONDITION_REJECTED_EXACT_MARKER);
    }
    /** Durable Root first: read-only idempotence, with no new refund, provider observation or old SDK settlement. */
    public boolean markerCapacityAlreadySettled(SystemMutation selected,BoundedReadBudget reads,Runnable current){
        final var body=TargetEvidenceDeleteConfirmedBody.decode(Objects.requireNonNull(selected).canonicalBody());
        if(selected.type()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED||!body.markerRetained()||reads.maxBytes()==Long.MAX_VALUE)
            throw new IllegalArgumentException("bounded retained-marker confirmation settlement required");
        current.run();final var root=readRoot(reads).root;final var pending=root.markers.get(body.resource().exactKey());
        if(pending==null)throw new ProtectionUnavailable("original durable marker reservation is unavailable");
        requireExact(pending.marker.canonicalBytes(),body.markerEvidence().marker().canonicalBytes());
        if(pending.settlement==null)return false;pending.settlement.requireConfirmation(selected,body);
        current.run();if(!reads.beforeTimedWork())throw reads.incomplete();return true;
    }
    /** Separate physical accounting: actual Native52 first + exact current marker + original Root CAS. No key/record/slot GC. */
    public void settleRetiredMarkerCapacity(com.nereusstream.delay.ownership.TargetWorkerSourceStartup source,
            com.nereusstream.delay.ownership.TargetWorkerShardRuntime worker,
            com.nereusstream.delay.ownership.TargetWorkerProcessRuntime.ReceiptManagement management,SystemMutation selected,
            com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
            BoundedReadBudget catalogReads,BoundedReadBudget nativeReads,Runnable current){
        requireManagedMarkerInputOwner();management.requireOriginalEvidenceCatalog(this);
        if(catalogReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite physical settlement reads required");
        current.run();source.requireEvidenceRetirementReadCurrent(worker,management);
        final var applied=source.readEvidenceRetirementConfirmation(worker,management,selected,nativeReads);
        requireSupportedProtectionDeclarations(applied.originalIntent());final var record=applied.record();final var body=record.body();
        final var marker=body.markerEvidence().marker();final var settlement=MarkerSettlement.from(applied);
        final var root=readRoot(catalogReads).root;admission.requireCurrent();
        requireExact(root.binding.policy.scope().canonicalBytes(),admission.qualifiedHistory().history().scope().canonicalBytes());
        final var closed=closedSnapshot(root,marker.exactKey(),marker.objectDigest());closed.requireObjectFingerprint(body.resource());
        requireExact(closed.digest(),body.context().closureDigest());
        final var capacity=readCapacityOwner(closed,catalogReads,current);requireExact(capacity.reference().canonicalBytes(),marker.capacityOwner().canonicalBytes());
        final var original=Objects.requireNonNull(root.topics.get(Bytes.hex(closed.closure.managementTopicId)),"original settlement management");
        try(var permission=trust.currentManagement(original.artifact,Action.RETIRE_EVIDENCE_OBJECT_PAYLOAD)){
            final Runnable held=()->{current.run();admission.requireCurrent();permission.requireCurrent();source.requireEvidenceRetirementReadCurrent(worker,management);};
            held.run();final var observed=persistent(read(marker.exactKey(),catalogReads,Math.toIntExact(Math.max(marker.payloadBytes(),marker.canonicalBytes().length))));
            if(observed==null)throw new ProtectionUnavailable("actual retained marker is unavailable for physical settlement");
            requireExact(observed.value(),marker.canonicalBytes());requireExact(Bytes.u64beBits(observed.version().versionId()),settlement.markerVersion);
            final var updated=update(before->{
                requireExact(before.binding.policy.scope().canonicalBytes(),root.binding.policy.scope().canonicalBytes());
                final var actual=closedSnapshot(before,marker.exactKey(),marker.objectDigest());requireExact(actual.canonicalBytes(),closed.canonicalBytes());
                return before.settleMarker(marker,settlement);
            },held,catalogReads,false);
            requireExact(Objects.requireNonNull(updated.markers.get(marker.exactKey())).settlement.bytes(),settlement.bytes());
            held.run();if(!catalogReads.beforeTimedWork()||!nativeReads.beforeTimedWork())throw new ProtectionUnavailable("physical settlement deadline exhausted");
        }
    }
    /** Local budget handoff only, after the caller's other SDKs/methods close; no server outcome is inferred. */
    public void retainClientInputsUntilStorageSdkTermination(com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client input){
        retainStorageInput.accept(Objects.requireNonNull(input));
    }
    public boolean hasManagedStorageInputOwner(){return managedStorageInputOwner.getAsBoolean();}
    public void requireManagedMarkerInputOwner(){
        if(!hasManagedStorageInputOwner())throw new IllegalStateException("marker metadata requires its original managed Registry SDK owner");
    }
    /** Caller retains original owned-I/O/read-window responsibility; this CAS never writes the payload key. */
    public ReservedMarker reserveRetiredMarker(TargetEvidenceRetirementStore.AppliedIntent applied,BoundedReadBudget reads,Runnable current){
        Objects.requireNonNull(applied);Objects.requireNonNull(reads);Objects.requireNonNull(current).run();
        if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("marker reservation requires finite Catalog reads");
        final var retained=applied.record();final var resource=retained.copy().resource();final var context=retained.body().context();
        final var actual=readReferenceClosure(resource.exactKey(),resource.objectDigest(),reads);actual.requireObjectFingerprint(resource);
        requireExact(actual.canonicalBytes(),retained.copy().closureImage());
        final var capacity=readCapacityOwner(actual,reads,current);
        final var marker=new TargetEvidenceRetiredMarker(capacity.reference(),resource.deploymentRootDigest(),capacity.publicationId(),capacity.originDigest(),
                resource.exactKey(),resource.exactIdentity().identityHash(),resource.objectDigest(),resource.valueDigest(),resource.canonicalValueBytes(),
                resource.immutableVersion(),actual.revision(),actual.digest(),retained.intent().systemMutationId(),retained.intent().mutationHash(),retained.appliedAt());
        marker.requireBindings(resource,context,new ResourceDeleteConfirmedBody.RetireIntentRef(
                retained.intent().systemMutationId(),retained.intent().mutationHash(),resource.exactIdentity().identityHash(),0));
        retained.copy().requireMarker(marker);
        final var before=readRoot(reads).root;final var management=Objects.requireNonNull(before.topics.get(Bytes.hex(actual.closure.managementTopicId)),"original marker management");
        try(var permission=trust.currentManagement(management.artifact,Action.CLOSE_EVIDENCE_OBJECT_REFERENCES)){
            final Runnable guarded=()->{current.run();permission.requireCurrent();};guarded.run();
            final var desired=new MarkerReservation(marker);
            final var result=update(root->{
                final var closed=closedSnapshot(root,resource.exactKey(),resource.objectDigest());requireExact(closed.canonicalBytes(),actual.canonicalBytes());
                if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.StagingProvenance){
                    final boolean adopted=resource.subtype()==TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF
                            ?root.recoveries.get(Bytes.hex(marker.publicationId())).phase==ADOPTED
                            :root.publications.get(Bytes.hex(marker.publicationId())).phase==ADOPTED;
                    if(adopted)throw new IllegalStateException("staging marker reservation acquired an adoption");
                }
                return root.reserveMarker(desired);
            },guarded,reads,false);
            requireExact(Objects.requireNonNull(result.markers.get(marker.exactKey())).bytes(),desired.bytes());
            final var observed=readRoot(reads).root;
            requireExact(Objects.requireNonNull(observed.markers.get(marker.exactKey()),"durable marker reservation").bytes(),desired.bytes());
            guarded.run();if(!reads.beforeTimedWork())throw reads.incomplete();return new ReservedMarker(this,marker);
        }
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
    /** Rebuilds public CONTROL material from original registered history and exact retained Catalog bytes/version. */
    public RetirementCopy prepareControlRetirementCopy(ClosedObject original,TargetExternalEvidenceObjectResource.Provenance provenance,
            BoundedReadBudget reads,Runnable current){
        final var actual=readReferenceClosure(original,reads,current);final var resource=actual.controlResource(provenance);
        final var stored=read(resource.exactKey(),reads,Math.toIntExact(resource.canonicalValueBytes()));
        if(stored==null||!Arrays.equals(resource.immutableVersion(),Bytes.u64beBits(stored.version().versionId())))
            throw new IllegalStateException("CONTROL retirement copy lacks original present object/version");
        final var root=readRoot(reads).root;final var intent=Objects.requireNonNull(findByKey(root,resource.exactKey()),"original CONTROL publication");
        requireControlOwner(intent,provenance instanceof TargetExternalEvidenceObjectResource.Protection p?p.owner()
                :((TargetExternalEvidenceObjectResource.Anchor)provenance).owner());
        final var artifact=decodeArtifact(intent,stored);
        final var protection=artifact.kind()==TargetKafkaReceiptControlArtifact.PROTECTION?artifact
                :readArtifact(requiredDigest(root,TargetKafkaReceiptControlArtifact.PROTECTION,artifact.protectionDigest()),reads);
        final String key=artifact.kind()==TargetKafkaReceiptControlArtifact.PROTECTION?protectionObjectKey(artifact.protection()):anchorObjectKey(artifact.digest());
        if(!key.equals(resource.exactKey()))throw new IllegalStateException("CONTROL copy changed original Catalog key");
        current.run();final var copy=new TargetEvidenceRetirementCopy(resource,stored.value(),
                trust.historicalPublicInputs(artifact,protection),actual.canonicalBytes());
        actual.requireObjectFingerprint(copy.resource());readReferenceClosure(actual,reads,current);
        final var after=read(resource.exactKey(),reads,Math.toIntExact(resource.canonicalValueBytes()));
        if(after==null||after.version().versionId()!=stored.version().versionId())
            throw new IllegalStateException("CONTROL retirement object changed during public-copy authentication");
        requireExact(stored.value(),after.value());current.run();if(!reads.beforeTimedWork())throw reads.incomplete();
        return new RetirementCopy(this,copy);
    }
    /** Derives staging identity from the original reservation and actual Native SHARD fee owner, never caller-supplied provenance. */
    public RetirementCopy prepareStagingRetirementCopy(ClosedObject original,TargetQuotaIncarnation feeOwner,
            TargetRecoveryProofTrust proofTrust,BoundedReadBudget reads,Runnable current){
        if(feeOwner.identity().kind()!=TargetQuotaIdentity.Kind.SHARD)
            throw new IllegalArgumentException("staging copy requires actual Source SHARD fee owner");
        final var actual=readReferenceClosure(original,reads,current);
        final var capacity=readCapacityOwner(actual,reads,current);
        final var root=readRoot(reads).root;
        final byte[] authorization;final int authorizationKind;final ShardId originalShard;
        if(actual.subtype()==3){
            final var reservation=Objects.requireNonNull(root.recoveries.get(Bytes.hex(actual.publicationId())),"original recovery reservation");
            final var stored=read(actual.exactKey(),reads,Math.toIntExact(actual.objectBytes()));
            if(stored==null||stored.version().versionId()!=actual.closure.blobVersion)
                throw new IllegalStateException("staging recovery lost original immutable object/version");
            final var verified=Objects.requireNonNull(proofTrust,"original recovery public history").verify(actual.exactKey(),stored.value());
            verified.requireExact(reservation.reference);
            requireExact(reservation.frozenOwner.canonicalBytes(),verified.proof().frozenOwner().canonicalBytes());
            requireExact(feeOwner.tenantScope(),reservation.frozenOwner.tenantScope());
            requireExact(feeOwner.recoveryLineage(),reservation.frozenOwner.recoveryLineage());
            originalShard=reservation.frozenOwner.identity().shard();
            authorization=verified.proof().originalAuthorizationDigest();authorizationKind=2;
        }else{
            final var reservation=Objects.requireNonNull(findByKey(root,actual.exactKey()),"original CONTROL reservation");
            originalShard=shard(reservation.shard);authorization=reservation.controlDigest;authorizationKind=1;
        }
        if(!feeOwner.identity().shard().equals(originalShard))
            throw new IllegalArgumentException("staging copy changed original Source SHARD");
        final var resource=new TargetExternalEvidenceObjectResource(switch(actual.subtype()){
            case 1->TargetExternalEvidenceObjectResource.Subtype.CONTROL_PROTECTION;
            case 2->TargetExternalEvidenceObjectResource.Subtype.CONTROL_ANCHOR;
            case 3->TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF;
            default->throw new IllegalArgumentException("unknown staging object subtype");
        },actual.namespaceId(),actual.deploymentRootDigest,originalShard,feeOwner.tenantScope(),feeOwner.recoveryLineage(),
                actual.exactKey(),actual.objectDigest(),actual.closure.valueDigest,actual.objectBytes(),Bytes.u64beBits(actual.closure.blobVersion),
                new TargetExternalEvidenceObjectResource.ReservedStaging(capacity.publicationId(),capacity.originDigest(),
                        capacity.reference(),authorizationKind,authorization));
        return prepareStagingRetirementCopy(actual,resource,proofTrust,reads,current);
    }
    /** Read-only reconstruction from an already closed original reservation; this does not establish writer/I/O exit or authorize deletion. */
    public RetirementCopy prepareStagingRetirementCopy(ClosedObject original,TargetExternalEvidenceObjectResource resource,
            TargetRecoveryProofTrust proofTrust,BoundedReadBudget reads,Runnable current){
        if(!(resource.provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging origin))
            throw new IllegalArgumentException("staging reconstruction requires explicit capacity-root provenance2");
        final var actual=readReferenceClosure(original,reads,current);actual.requireObjectFingerprint(resource);
        if(actual.closure.capturedPhase==ADOPTED||actual.closure.capturedAdoptedSource.length!=0)
            throw new IllegalStateException("original reservation was already adopted");
        final var capacity=readCapacityOwner(actual,reads,current);capacity.requireCatalog(this);
        requireExact(capacity.reference().canonicalBytes(),origin.capacityOwner().canonicalBytes());
        requireExact(capacity.publicationId(),origin.publicationIntentId());requireExact(capacity.originDigest(),origin.publicationOriginDigest());
        final var root=readRoot(reads).root;final var captured=closedSnapshot(root,resource.exactKey(),resource.objectDigest());
        requireExact(captured.canonicalBytes(),actual.canonicalBytes());
        final var stored=read(resource.exactKey(),reads,Math.toIntExact(resource.canonicalValueBytes()));
        if(stored==null||!Arrays.equals(resource.immutableVersion(),Bytes.u64beBits(stored.version().versionId())))
            throw new IllegalStateException("staging copy lacks original present object/version");
        final byte[] inputs;
        if(resource.subtype()==TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF){
            final var reservation=Objects.requireNonNull(root.recoveries.get(Bytes.hex(origin.publicationIntentId())),"original recovery reservation");
            if(reservation.phase==ADOPTED||reservation.adoptedSource.length!=0)
                throw new IllegalStateException("recovery reservation acquired an adoption");
            final var verified=Objects.requireNonNull(proofTrust,"original recovery public history").verify(resource.exactKey(),stored.value());
            verified.requireExact(reservation.reference);requireExact(reservation.frozenOwner.canonicalBytes(),verified.proof().frozenOwner().canonicalBytes());
            requireExact(origin.originalAuthorizationDigest(),verified.proof().originalAuthorizationDigest());
            inputs=proofTrust.historicalPublicInputs(verified);
        }else{
            final var reservation=Objects.requireNonNull(findByKey(root,resource.exactKey()),"original CONTROL reservation");
            if(reservation.phase==ADOPTED||reservation.adoptedSource.length!=0||!shard(reservation.shard).equals(resource.shard()))
                throw new IllegalStateException("CONTROL reservation changed original unadopted Source scope");
            final var artifact=decodeArtifact(reservation,stored);
            final var protection=artifact.kind()==TargetKafkaReceiptControlArtifact.PROTECTION?artifact
                    :readArtifact(requiredDigest(root,TargetKafkaReceiptControlArtifact.PROTECTION,artifact.protectionDigest()),reads);
            final String key=artifact.kind()==TargetKafkaReceiptControlArtifact.PROTECTION?protectionObjectKey(artifact.protection()):anchorObjectKey(artifact.digest());
            if(!key.equals(resource.exactKey()))throw new IllegalStateException("staging CONTROL copy changed canonical key");
            requireExact(origin.originalAuthorizationDigest(),reservation.controlDigest);
            inputs=trust.historicalPublicInputs(artifact,protection);
        }
        current.run();final var copy=new TargetEvidenceRetirementCopy(resource,stored.value(),inputs,actual.canonicalBytes());
        readReferenceClosure(actual,reads,current);
        final var finalCapacity=readCapacityOwner(actual,reads,current);
        requireExact(capacity.reference().canonicalBytes(),finalCapacity.reference().canonicalBytes());
        final var finalRoot=readRoot(reads).root;
        if(resource.subtype()==TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF){
            final var reservation=Objects.requireNonNull(finalRoot.recoveries.get(Bytes.hex(origin.publicationIntentId())));
            if(reservation.phase==ADOPTED||reservation.adoptedSource.length!=0)
                throw new IllegalStateException("staging recovery reservation was adopted during copy authentication");
        }else{
            final var reservation=Objects.requireNonNull(findByKey(finalRoot,resource.exactKey()));
            if(reservation.phase==ADOPTED||reservation.adoptedSource.length!=0)
                throw new IllegalStateException("staging CONTROL reservation was adopted during copy authentication");
        }
        final var after=read(resource.exactKey(),reads,Math.toIntExact(resource.canonicalValueBytes()));
        if(after==null||after.version().versionId()!=stored.version().versionId())
            throw new IllegalStateException("staging object changed during public-copy authentication");
        requireExact(stored.value(),after.value());current.run();if(!reads.beforeTimedWork())throw reads.incomplete();
        return new RetirementCopy(this,copy);
    }
    /** Read-only reconstruction of one already-signed Source input; actual Native joins remain in Source apply. */
    public RetirementCopy readRetirementSourceCopy(TargetEvidenceRetireIntentBody intent,TargetRecoveryProofTrust proofTrust,
            BoundedReadBudget reads,Runnable current){
        Objects.requireNonNull(current).run();final var resource=Objects.requireNonNull(intent).resource();
        if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Staging)
            throw new IllegalStateException("legacy staging creation-time provenance cannot be reconstructed from current Root data");
        final var closure=readReferenceClosure(resource.exactKey(),resource.objectDigest(),reads);
        final RetirementCopy actual;
        if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery origin)
            actual=prepareRecoveryRetirementCopy(closure,origin.owner(),proofTrust,reads,current);
        else if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection
                ||resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor)
            actual=prepareControlRetirementCopy(closure,resource.provenance(),reads,current);
        else if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging)
            actual=prepareStagingRetirementCopy(closure,resource,proofTrust,reads,current);
        else throw new IllegalStateException("unsupported original retirement provenance");
        if(!Arrays.equals(actual.image().resource().canonicalBytes(),resource.canonicalBytes()))
            throw new IllegalStateException("historical retirement input changed original exact object");
        actual.image().requireContext(intent.context());current.run();return actual;
    }
    private ClosedObject closedSnapshot(Root root,String key,byte[] digest){
        final var closure=Objects.requireNonNull(root.closures.get(key),"original durable object closure");requireExact(closure.objectDigest,digest);
        final var management=Objects.requireNonNull(root.topics.get(Bytes.hex(closure.managementTopicId)),"original closure management");
        trust.requireManagementHistory(management.artifact);requireExact(closure.namespaceId,namespaceId(management.image()));
        final var uses=closure.subtype==3?root.recoveries.get(Bytes.hex(closure.publicationId)).uses:root.publications.get(Bytes.hex(closure.publicationId)).uses;
        return new ClosedObject(this,closure,uses,management.image().deploymentRootDigest());
    }
    /** Exact data identity derived from this Catalog's prefix/capacity and the authenticated management image. */
    public byte[] namespaceIdentity(TargetKafkaReceiptManagementImage image){return namespaceId(Objects.requireNonNull(image));}
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
    /** This receipt is constructed only from the exact original INITIAL image, before any READY transition. */
    public static final class InitialRootObservation {
        private final TargetCatalogRootAuthority.Scope scope;
        private final long version;
        private final byte[] digest;
        private InitialRootObservation(TargetCatalogRootAuthority.Scope scope,GetResult stored){this.scope=scope;version=stored.version().versionId();digest=Bytes.sha256(stored.value());}
        private InitialRootObservation(TargetCatalogRootAuthority.Scope scope,long version,byte[] digest){this.scope=scope;this.version=version;this.digest=Bytes.copy(digest);}
        public TargetCatalogRootAuthority.Scope scope(){return scope;}
        public long version(){return version;}
        public byte[] digest(){return Bytes.copy(digest);}
    }
    private TargetCatalogRootAuthority.Scope actualRootScope(TargetCatalogRootAuthority.Policy policy){
        requireManagedMarkerInputOwner();requireLimits(policy.scope().limits());
        return new TargetCatalogRootAuthority.Scope(storageNamespaceBinding.get(),prefix,ledgerKey,guardKey,
                policy.scope().deploymentRoot(),limits,policy.scope().allocation(),policy.scope().nonce());
    }
    private TargetCatalogRootAuthority requireRootAuthority(){if(rootAuthority==null)throw new IllegalStateException("independent Catalog root purpose/history authority is unavailable");return rootAuthority;}
    private void requireRootScope(Root root,TargetCatalogRootAuthority.Policy policy){
        requireRootAuthority().requireHistory(policy,actualRootScope(policy));
        for(var management:root.topics.values())requireExact(management.image().deploymentRootDigest(),policy.scope().deploymentRoot());
    }
    private void requireLegacyHistory(Root root){for(var management:root.topics.values())trust.requireManagementHistory(management.artifact);}
    private static GetResult persistent(GetResult result){
        if(result!=null&&(result.version().versionId()<0||result.version().sessionId().isPresent()||result.version().clientIdentifier().isPresent()))
            throw new IllegalStateException("Catalog Root/guard requires original persistent versioned records");
        return result;
    }
    private GetResult rootRead(String key,BoundedReadBudget reads,int maximum){return persistent(reads==null?read(key):read(key,reads,maximum));}
    private Observed rawRoot(BoundedReadBudget reads){final var stored=rootRead(ledgerKey,reads,MAX_ROOT_BYTES);
        if(stored==null)return new Observed(null,null);
        final var root=Root.decode(stored.value());requireLimits(root.limits);requireMarkerOwnerBindings(root);return new Observed(stored,root);}
    private Observed readRoot(){return readRoot(null);}
    private Observed readRoot(BoundedReadBudget budget){
        requireRootAuthority();final var first=Objects.requireNonNull(rootRead(guardKey,budget,MAX_GUARD_BYTES),"protected READY Catalog Root guard is missing");
        final var guard=RootGuard.decode(first.value());if(guard.state!=2)throw new IllegalStateException("Catalog Root guard is not READY");
        final var observed=rawRoot(budget);if(observed.stored==null)throw new IllegalStateException("original guarded Catalog Root is missing; initialization is forbidden");
        final var binding=observed.root.binding;
        if(binding==null||binding.state!=2||observed.stored.version().versionId()<guard.firstLiveVersion
                ||binding.pendingGuardVersion!=guard.pendingVersion||first.version().versionId()<=guard.pendingVersion)
            throw new IllegalStateException("Catalog Root is unguarded, initializing or older than its actual READY image");
        requireExact(binding.policy.canonicalBytes(),guard.policy.canonicalBytes());requireRootScope(observed.root,guard.policy);
        if(binding.initialVersion!=guard.initialVersion)throw new IllegalStateException("Catalog Root initialization version changed");
        requireExact(binding.initialDigest,guard.initialDigest);
        final var last=Objects.requireNonNull(rootRead(guardKey,budget,MAX_GUARD_BYTES),"original Catalog guard disappeared");
        if(last.version().versionId()!=first.version().versionId())throw new IllegalStateException("original READY guard version changed");
        requireExact(last.value(),first.value());return observed;
    }
    /** Startup must run this before constructing Admin or admitting ordinary operations. It never initializes storage. */
    public void requireReadyRoot(byte[] deploymentRoot){final var actual=readRoot();requireExact(actual.root.binding.policy.scope().deploymentRoot(),deploymentRoot);}

    /** Actual protected allocation identity for independent lifecycle qualification; no caller-supplied namespace. */
    public TargetCatalogRootAuthority.Scope evidenceLifecycleScope(byte[] deploymentRoot,BoundedReadBudget reads,Runnable current){
        Objects.requireNonNull(reads);Objects.requireNonNull(current);requireManagedMarkerInputOwner();
        if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite actual lifecycle scope reads required");
        current.run();final var actual=readRoot(reads);final var scope=actual.root.binding.policy.scope();
        requireExact(scope.deploymentRoot(),deploymentRoot);current.run();
        if(!reads.beforeTimedWork())throw reads.incomplete();return scope;
    }

    /** Explicit owned-I/O metadata initialization/adoption. No generic read/update path can enter it. */
    public void initializeRoot(TargetCatalogRootAuthority.Policy policy,byte[] deploymentRoot,
            com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Authenticator authenticator,
            TargetCatalogRootAuthority.FreshAllocation external,BoundedReadBudget reads,Runnable current){
        Objects.requireNonNull(current);Objects.requireNonNull(reads);
        if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite original Catalog bootstrap reads required");
        final var actual=actualRootScope(Objects.requireNonNull(policy));requireExact(actual.deploymentRoot(),deploymentRoot);
        final var permit=requireRootAuthority().acquire(policy,actual,Objects.requireNonNull(authenticator));
        final boolean fresh=policy.purpose()==TargetCatalogRootAuthority.Purpose.INITIALIZE_FRESH_CATALOG_ROOT;
        if(fresh)Objects.requireNonNull(external,"actual external never-used allocation/one-time initialization authority is unavailable");
        else if(external!=null)throw new IllegalArgumentException("legacy adoption cannot substitute a fresh allocation fact");
        final Runnable guarded=()->{current.run();permit.requireCurrent(actual);};
        for(int attempt=0;attempt<MAX_CAS_ATTEMPTS;attempt++){
            guarded.run();final var guardStored=rootRead(guardKey,reads,MAX_GUARD_BYTES);final var before=rawRoot(reads);
            final var guard=guardStored==null?null:RootGuard.decode(guardStored.value());
            if(guard!=null){requireExact(guard.policy.canonicalBytes(),policy.canonicalBytes());
                if(guard.state==2){readRoot(reads);guarded.run();return;}}
            if(fresh){external.requireNeverOrdinarilyUsed(actual);guarded.run();}
            try{
                if(guard==null){
                    if(fresh){
                        if(before.stored!=null)throw new IllegalStateException("fresh initialization found a Root without its original PENDING guard");
                        external.requireInitialCreation(actual);guarded.run();
                        // Validate original root+guard peak bytes and both records before the first persistent put.
                        final var initial=new Root(limits,Map.of(),Map.of()).guarded(new GuardBinding(policy,1,0,new byte[32],0,new byte[32],0));initial.bytes();
                    }else{
                        if(before.stored==null)throw new IllegalStateException("legacy adoption cannot reconstruct a missing Root");
                        if(before.root.binding==null){
                            requireRootScope(before.root,policy);requireLegacyHistory(before.root);
                            final var initial=before.root.guarded(new GuardBinding(policy,1,0,new byte[32],before.stored.version().versionId(),Bytes.sha256(before.stored.value()),-1));
                            putRootMetadata(ledgerKey,initial.bytes(),before.stored,guarded);continue;
                        }
                        requireExact(before.root.binding.policy.canonicalBytes(),policy.canonicalBytes());
                        if(before.root.binding.state!=1)throw new IllegalStateException("legacy READY binding lost its guard; downgrade is forbidden");
                        requireRootScope(before.root,policy);requireLegacyHistory(before.root);
                    }
                    putRootMetadata(guardKey,new RootGuard(policy,1,0,new byte[32],0,0).bytes(),null,guarded);continue;
                }
                if(before.stored==null){
                    if(!fresh)throw new IllegalStateException("legacy PENDING guard lost its Root");
                    external.requireInitialCreation(actual);guarded.run();
                    final var initial=new Root(limits,Map.of(),Map.of()).guarded(new GuardBinding(policy,1,0,new byte[32],0,new byte[32],guardStored.version().versionId()));
                    putRootMetadata(ledgerKey,initial.bytes(),null,guarded);continue;
                }
                final var binding=Objects.requireNonNull(before.root.binding,"PENDING guard found an unassociated legacy Root");
                requireExact(binding.policy.canonicalBytes(),policy.canonicalBytes());requireRootScope(before.root,policy);
                if(!fresh)requireLegacyHistory(before.root);
                if(binding.pendingGuardVersion==-1){
                    if(fresh)throw new IllegalStateException("fresh Root lost its original PENDING guard version");
                    putRootMetadata(ledgerKey,before.root.guarded(binding.bindPending(guardStored.version().versionId())).bytes(),before.stored,guarded);continue;
                }
                if(binding.pendingGuardVersion!=guardStored.version().versionId())throw new IllegalStateException("PENDING guard was recreated; an old READY/initialization cannot be downgraded");
                if(fresh&&(!before.root.topics.isEmpty()||!before.root.publications.isEmpty()||!before.root.recoveries.isEmpty()
                        ||!before.root.closures.isEmpty()||!before.root.markers.isEmpty()))throw new IllegalStateException("fresh PENDING initialization is no longer empty");
                if(binding.state==1){
                    if(fresh)external.recordInitialRoot(TargetCatalogRootAuthority.observedInitialRoot(new InitialRootObservation(actual,before.stored)));
                    guarded.run();final var live=before.root.guarded(binding.live(before.stored.version().versionId(),Bytes.sha256(before.stored.value())));
                    putRootMetadata(ledgerKey,live.bytes(),before.stored,guarded);continue;
                }
                if(before.stored.version().versionId()<=binding.initialVersion)throw new IllegalStateException("LIVE Root lacks its actual conditional initialization advance");
                if(fresh){
                    // Reconfirm the exact original birth on resume; the external record must reject any recreated initial image.
                    external.recordInitialRoot(TargetCatalogRootAuthority.observedInitialRoot(
                            new InitialRootObservation(actual,binding.initialVersion,binding.initialDigest)));
                    guarded.run();
                }
                putRootMetadata(guardKey,new RootGuard(policy,2,binding.initialVersion,binding.initialDigest,before.stored.version().versionId(),binding.pendingGuardVersion).bytes(),guardStored,guarded);
                readRoot(reads);guarded.run();return;
            }catch(KeyAlreadyExistsException|UnexpectedVersionIdException race){/* Known CAS rejection only; unknown responses retain the original owner. */}
        }
        throw new IllegalStateException("Catalog initialization did not converge within the original finite CAS bound");
    }
    private void putRootMetadata(String key,byte[] bytes,GetResult expected,Runnable current)throws KeyAlreadyExistsException,UnexpectedVersionIdException{
        current.run();session.run();final var result=client.put(key,bytes,expected==null?Set.of(PutOption.IfRecordDoesNotExist):Set.of(PutOption.IfVersionIdEquals(expected.version().versionId())));
        session.run();current.run();if(result==null||!key.equals(result.key())||result.version()==null||result.version().versionId()<0)
            throw new IllegalStateException("Catalog initialization lacks exact key/actual version response");
    }
    private void requireMarkerOwnerBindings(Root root){
        if(root.markers.isEmpty())return;
        final byte[] binding=storageNamespaceBinding.get();
        for(var value:root.markers.values()){
            final var marker=value.marker;final var owner=marker.capacityOwner();
            if(!ledgerKey.equals(owner.exactRootKey()))throw new IllegalStateException("marker reservation changed original capacity root key");
            requireExact(owner.storageNamespaceBinding(),binding);
            final var closure=root.closures.get(marker.exactKey());
            final var management=TargetKafkaReceiptManagementImage.decode(root.topics.get(Bytes.hex(closure.managementTopicId)).artifact.image());
            requireExact(owner.catalogNamespaceId(),namespaceId(management));
            final ShardId original=closure.subtype==3?root.recoveries.get(Bytes.hex(closure.publicationId)).frozenOwner.identity().shard()
                    :shard(root.publications.get(Bytes.hex(closure.publicationId)).shard);
            if(!original.equals(marker.intentAppliedAt().source().shardId()))throw new IllegalStateException("marker reservation changed original Source SHARD");
        }
    }
    private Root update(UnaryOperator<Root> operation){ return update(operation, session); }
    private Root update(UnaryOperator<Root> operation,Runnable current){return update(operation,current,null);}
    private Root update(UnaryOperator<Root> operation,Runnable current,BoundedReadBudget budget){
        return update(operation,current,budget,true);
    }
    private Root update(UnaryOperator<Root> operation,Runnable current,BoundedReadBudget budget,boolean acceptUncertainReadback){
        for(int attempt=0;attempt<MAX_CAS_ATTEMPTS;attempt++){current.run();final var before=budget==null?readRoot():readRoot(budget);requireLimits(before.root.limits);final var after=operation.apply(before.root);final byte[] desired=after.bytes();
            if(before.stored!=null&&Arrays.equals(before.stored.value(),desired)){current.run();return after;}
            try{session.run();current.run();final var result=client.put(ledgerKey,desired,Set.of(PutOption.IfVersionIdEquals(before.stored.version().versionId())));session.run();current.run();
                if(result==null||!ledgerKey.equals(result.key())||result.version()==null)throw new IllegalStateException("receipt capacity CAS returned no exact key/version");}
            catch(KeyAlreadyExistsException|UnexpectedVersionIdException race){continue;}
            catch(RuntimeException uncertain){
                if(!acceptUncertainReadback)throw uncertain;
                current.run();final var observed=budget==null?readRoot():readRoot(budget);if(observed.stored==null||!Arrays.equals(observed.stored.value(),desired))throw uncertain;current.run();return observed.root;
            }
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
