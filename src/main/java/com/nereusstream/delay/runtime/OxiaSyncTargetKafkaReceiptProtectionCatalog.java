package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.TargetRecoveryProofTrust;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Action;
import com.nereusstream.delay.protocol.*;
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
            controlId=Bytes.copy(controlId);controlDigest=Bytes.copy(controlDigest);shard=Bytes.copy(shard);
            adoptedSource=Bytes.copy(adoptedSource);brokenDigest=Bytes.copy(brokenDigest);brokenSignature=Bytes.copy(brokenSignature);
        }
        long physicalBytes(){return Math.addExact(bytes,key.getBytes(StandardCharsets.UTF_8).length);}
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
        long rootReserve(int maximumUses){
            final var empty=new RecoveryIntent(id,reference,frozenOwner,RESERVED,List.of(),new byte[0]);
            // Reserve all future uses and the Source adoption stamp before the first blob write.
            return Math.addExact(empty.encode().length+64L,Math.multiplyExact(128L,maximumUses));
        }
        RecoveryIntent phase(int next){return new RecoveryIntent(id,reference,frozenOwner,next,uses,adoptedSource);}
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
    private record Root(int version,Limits limits,Map<String,Management> topics,Map<String,Intent> publications,Map<String,RecoveryIntent> recoveries) {
        Root(Limits limits,Map<String,Management> topics,Map<String,Intent> publications){this(1,limits,topics,publications,Map.of());}
        Root {topics=Map.copyOf(topics);publications=Map.copyOf(publications);recoveries=Map.copyOf(recoveries);validate(version,limits,topics,publications,recoveries);}
        Root topic(Management m){final var map=new TreeMap<>(topics);map.put(m.id(),m);return new Root(version,limits,map,publications,recoveries);}
        Root publication(Intent i){final var map=new TreeMap<>(publications);map.put(Bytes.hex(i.id),i);return new Root(version,limits,topics,map,recoveries);}
        Root recovery(RecoveryIntent i){final var map=new TreeMap<>(recoveries);map.put(Bytes.hex(i.id),i);return new Root(2,limits,topics,publications,map);}
        long reservedBytes(){long result=limits.rootReservedBytes();for(var i:recoveries.values())
            result=Math.addExact(result,Math.max(0,i.rootReserve(limits.maximumPreparedReferences)-(4096L+128L*limits.maximumPreparedReferences)));return result;}
        byte[] bytes(){final var b=CanonicalProtobuf.message(o->{
            CanonicalProtobuf.uint32(o,1,version);CanonicalProtobuf.uint64(o,2,limits.physicalBytes);CanonicalProtobuf.uint32(o,3,limits.physicalRecords);
            CanonicalProtobuf.uint32(o,4,limits.maximumPublications);CanonicalProtobuf.uint32(o,5,limits.maximumTopics);CanonicalProtobuf.uint32(o,6,limits.maximumPreparedReferences);
            CanonicalProtobuf.bytes(o,7,limits.capacitySourceDigest);
            for(var m:new TreeMap<>(topics).values())CanonicalProtobuf.bytes(o,8,m.bytes());
            for(var i:new TreeMap<>(publications).values())CanonicalProtobuf.bytes(o,9,i.encode());
            for(var i:new TreeMap<>(recoveries).values())CanonicalProtobuf.bytes(o,10,i.encode());
        });if(b.length>reservedBytes()||b.length>MAX_ROOT_BYTES)throw new IllegalArgumentException("receipt catalog root exceeds its reserved physical storage");return b;}
        static Root decode(byte[] b){
            if(b.length==0||b.length>MAX_ROOT_BYTES)throw new IllegalArgumentException("invalid receipt capacity root length");
            final var f=QueryCodecSupport.read(b,"receipt capacity root");if(f.size()<7)throw new IllegalArgumentException("incomplete receipt capacity root");
            QueryCodecSupport.requireNumbers(f.subList(0,7),new int[]{1,2,3,4,5,6,7},"receipt capacity root");
            final int version=QueryCodecSupport.uint32(f.get(0),1);if(version!=1&&version!=2)throw new IllegalArgumentException("unknown receipt capacity root version");
            final var l=new Limits(QueryCodecSupport.uint(f.get(1),2),QueryCodecSupport.uint32(f.get(2),3),QueryCodecSupport.uint32(f.get(3),4),QueryCodecSupport.uint32(f.get(4),5),QueryCodecSupport.uint32(f.get(5),6),QueryCodecSupport.fixed(f.get(6),7,32));
            final var topics=new TreeMap<String,Management>();final var intents=new TreeMap<String,Intent>();int n=7;
            while(n<f.size()&&f.get(n).number()==8){var m=Management.decode(QueryCodecSupport.nested(f.get(n++),8));if(topics.put(m.id(),m)!=null)throw new IllegalArgumentException("duplicate managed topic");}
            while(n<f.size()&&f.get(n).number()==9){var i=Intent.decode(QueryCodecSupport.nested(f.get(n++),9));if(intents.put(Bytes.hex(i.id),i)!=null)throw new IllegalArgumentException("duplicate publication ID");}
            final var recovery=new TreeMap<String,RecoveryIntent>();
            if(version==2)while(n<f.size()&&f.get(n).number()==10){var i=RecoveryIntent.decode(QueryCodecSupport.nested(f.get(n++),10));if(recovery.put(Bytes.hex(i.id),i)!=null)throw new IllegalArgumentException("duplicate recovery discovery ID");}
            if(n!=f.size())throw new IllegalArgumentException("unknown receipt root field");final var root=new Root(version,l,topics,intents,recovery);QueryCodecSupport.requireCanonical(b,root.bytes(),"receipt capacity root");return root;
        }
        private static void validate(int version,Limits l,Map<String,Management> topics,Map<String,Intent> publications,Map<String,RecoveryIntent> recovery){
            if(version!=1&&version!=2||version==1&&!recovery.isEmpty()||topics.size()>l.maximumTopics||publications.size()+(long)recovery.size()>l.maximumPublications)throw new IllegalArgumentException("receipt catalog cardinality bound exceeded");
            long bytes=l.rootReservedBytes();int records=1;final var keys=new HashSet<String>();
            for(var i:publications.values()){if(i.uses.size()>l.maximumPreparedReferences||!keys.add(i.key))throw new IllegalArgumentException("duplicate physical blob or prepared-use bound exceeded");bytes=Math.addExact(bytes,i.physicalBytes());records++;}
            long extraRoot=0;
            for(var i:recovery.values()){
                if(i.uses.size()>l.maximumPreparedReferences||!keys.add(i.reference.exactKey()))throw new IllegalArgumentException("duplicate recovery key or prepared-use bound exceeded");
                extraRoot=Math.addExact(extraRoot,Math.max(0,i.rootReserve(l.maximumPreparedReferences)-(4096L+128L*l.maximumPreparedReferences)));
                bytes=Math.addExact(bytes,Math.addExact(i.reference.canonicalValueBytes(),Bytes.utf8(i.reference.exactKey()).length));records++;
            }
            bytes=Math.addExact(bytes,extraRoot);if(l.rootReservedBytes()+extraRoot>MAX_ROOT_BYTES)throw new IllegalStateException("recovery intent root window exhausted");
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
        if(intent==null)return Optional.empty();if(intent.brokenDigest.length!=0)throw new IllegalStateException("original receipt protection is permanently BROKEN");
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
        final var intent=findByKey(readRoot().root,anchorObjectKey(binding.scanObservationDigest()));if(intent==null)throw new IllegalStateException("original anchor publication intent is missing");
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
            requireSameRecovery(old,desired);return old.phase>=PUBLISHED?root:root.recovery(old.phase(PUBLISHED));},current);
        current.run();return new RecoveryPublication(this,actual);
    }
    /** Unique mapping/evidence discovery. A reservation without a blob/publication is never reconstructed as success. */
    public Optional<RecoveryPublication> findRecoveryProof(TargetKafkaReceiptMapping mapping,PublishEvidence evidence,TargetRecoveryProofTrust proofTrust){
        final var intent=readRoot().root.recoveries.get(Bytes.hex(recoveryId(mapping.mappingId(),evidence.evidenceId())));
        if(intent==null)return Optional.empty();
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
            requireSameRecovery(actual,original);return actual.phase>=PUBLISHED?root:root.recovery(actual.phase(PUBLISHED));},current);
        current.run();return new RecoveryPublication(this,verified);
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
        // This is the pre-publication blob floor. Native adoption separately requires all exact STATE row charges.
        final var mapping=proof.mapping();final var anchor=retainedObject(anchorObjectKey(mapping.admission().kafkaReceipt().scanObservationDigest()),
                mapping.admission().kafkaReceipt().scanObservationDigest());
        final var accounting=proof.frozenOwner().accounting();
        final var originalAnchor=accounting.externalEvidenceCharge(Bytes.utf8(anchorObjectKey(anchor.digest())).length,anchor.canonicalBytes().length);
        final var recovery=accounting.externalEvidenceCharge(Bytes.utf8(key).length,proof.canonicalBytes().length);
        if(!proof.attempt().allocated().covers(originalAnchor.add(recovery)))
            throw new IllegalStateException("original frozen Attempt cannot pay anchor and recovery proof; keep UNKNOWN without archiving");
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
        update(r->{requireActiveManagement(r,protection);final var old=r.publications.get(Bytes.hex(id));if(old!=null){if(!samePublication(old,desired))throw new IllegalStateException("publication changed original exact object/scope");return r;}
            if(findByKey(r,key)!=null)throw new IllegalStateException("external key already belongs to another publication");return r.publication(desired);},current);
        final byte[] value=artifact.canonicalBytes();final var existing=read(key);
        if(existing==null){try{requireActiveManagement(protection);session.run();current.run();client.put(key,value,Set.of(PutOption.IfRecordDoesNotExist));session.run();current.run();}
            catch(KeyAlreadyExistsException|UnexpectedVersionIdException|RuntimeException uncertain){current.run();final var actual=read(key);if(actual==null)throw new IllegalStateException("artifact publication remains uncertain",uncertain);requireExact(actual.value(),value);}}
        else requireExact(existing.value(),value);
        final var actual=read(key);if(actual==null)throw new IllegalStateException("artifact publication lacks exact durable reread");requireExact(actual.value(),value);
        update(r->{requireActiveManagement(r,protection);final var i=Objects.requireNonNull(r.publications.get(Bytes.hex(id)));return i.phase>=PUBLISHED?r:r.publication(i.phase(PUBLISHED));},current); current.run();
    }
    private record Observed(GetResult stored,Root root){}
    private Observed readRoot(){final var result=read(ledgerKey);final var root=result==null?new Root(limits,Map.of(),Map.of()):Root.decode(result.value());requireLimits(root.limits);return new Observed(result,root);}
    private Root update(UnaryOperator<Root> operation){ return update(operation, session); }
    private Root update(UnaryOperator<Root> operation,Runnable current){
        for(int attempt=0;attempt<MAX_CAS_ATTEMPTS;attempt++){current.run();final var before=readRoot();requireLimits(before.root.limits);final var after=operation.apply(before.root);final byte[] desired=after.bytes();
            if(before.stored!=null&&Arrays.equals(before.stored.value(),desired)){current.run();return after;}
            try{session.run();current.run();final var result=client.put(ledgerKey,desired,before.stored==null?Set.of(PutOption.IfRecordDoesNotExist):Set.of(PutOption.IfVersionIdEquals(before.stored.version().versionId())));session.run();current.run();
                if(result==null||!ledgerKey.equals(result.key())||result.version()==null)throw new IllegalStateException("receipt capacity CAS returned no exact key/version");}
            catch(KeyAlreadyExistsException|UnexpectedVersionIdException race){continue;}
            catch(RuntimeException uncertain){current.run();final var observed=readRoot();if(observed.stored==null||!Arrays.equals(observed.stored.value(),desired))throw uncertain;current.run();return observed.root;}
            final var observed=readRoot();if(observed.stored!=null&&Arrays.equals(observed.stored.value(),desired)){current.run();return observed.root;}
            // A competing legitimate update may have advanced after this CAS; reapply idempotently, never overwrite it.
        }throw new IllegalStateException("receipt capacity CAS did not converge within configured bound");
    }
    private TargetKafkaReceiptControlArtifact readArtifact(Intent i){final var value=read(i.key);if(value==null||value.value().length!=i.bytes||!Arrays.equals(Bytes.sha256(value.value()),i.valueHash))throw new IllegalStateException("original external blob is missing/replaced");final var a=TargetKafkaReceiptControlArtifact.decode(value.value());if(a.kind()!=i.kind||!Arrays.equals(a.digest(),i.digest))throw new IllegalStateException("external object differs from original publication");return a;}
    private void requireActiveManagement(TargetKafkaReceiptProtection p){final var topic=BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(p.resource().authenticatedClusterId(),p.resource().receiptTopicUuid()));requireActiveManagement(management(topic),p);}
    private void requireActiveManagement(Root root,TargetKafkaReceiptProtection p){
        final var topic=BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(p.resource().authenticatedClusterId(),p.resource().receiptTopicUuid()));
        requireActiveManagement(Objects.requireNonNull(root.topics.get(topicId(topic)),"original management epoch"),p);
    }
    private void requireActiveManagement(Management m,TargetKafkaReceiptProtection p){
        if(m.state!=ACTIVE||!Arrays.equals(m.artifact.canonicalBytes(),p.managementAuthorityImage())||!Arrays.equals(m.image().managementEpoch(),p.managementEpoch())||!Arrays.equals(m.policy,p.brokerPolicyImage()))throw new IllegalStateException("receipt protection has no original ACTIVE continuous topic-management epoch");}
    private GetResult read(String key){session.run();final var r=client.get(key);session.run();if(r!=null&&(!key.equals(r.key())||r.value()==null||r.version()==null||r.value().length==0||r.value().length>MAX_ROOT_BYTES))throw new IllegalStateException("receipt Registry read lacks exact bounded key/version/value");return r;}
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
