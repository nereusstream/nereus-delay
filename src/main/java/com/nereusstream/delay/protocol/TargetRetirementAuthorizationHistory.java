package com.nereusstream.delay.protocol;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Signed complete Source permission intervals. This authenticates history, not Native activation or deletion. */
public final class TargetRetirementAuthorizationHistory {
    public static final int MAX_BYTES = 256 << 10;
    public static final int MAX_INTERVALS = 64;
    public static final int MAX_GRANTS = 128;
    public static final int MAX_GRANT_BYTES = 8192;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-retirement-source-authorization-history\0");
    private static final byte[] SIGN_DOMAIN = Bytes.utf8("nereus-delay-target-retirement-source-authorization-history-signature\0");
    private TargetRetirementAuthorizationHistory() {}

    public enum Action {
        RETIRE_INTENT(1), DELETE_CONFIRMATION(2), RESOURCE_RECLAIM(3);
        private final int wire;
        Action(int wire) { this.wire=wire; }
        public int wireValue() { return wire; }
        public static Action of(SystemMutationType type) {
            return switch(type) {
                case RESOURCE_RETIRE_INTENT -> RETIRE_INTENT;
                case RESOURCE_DELETE_CONFIRMED -> DELETE_CONFIRMATION;
                case TARGET_RESOURCE_RECLAIM -> RESOURCE_RECLAIM;
                default -> throw new IllegalArgumentException("not a Target retirement action");
            };
        }
        private static Action decode(int wire) {
            for(var action:values())if(action.wire==wire)return action;
            throw new IllegalArgumentException("unknown retirement history action");
        }
    }

    /** The complete original SERVICE identity and an activation reference still requiring actual projection verification. */
    public record Grant(AuthorIdentity service,int keyVersion,byte[] publicKey,Action action,
            ProtocolActivationState.Activation activation,byte[] targetDescriptorDigest,byte[] targetActivationDigest,byte[] markerHash) {
        public Grant {
            Objects.requireNonNull(service); Objects.requireNonNull(action); Objects.requireNonNull(activation);
            if(service.kind()!=AuthorIdentity.Kind.SERVICE||service.canonicalBytes().length>1024||keyVersion==0
                    ||!(action==Action.RESOURCE_RECLAIM
                        ?activation.tuple().equals(ProtocolTuple.targetResourceReclaim())||activation.tuple().equals(ProtocolTuple.targetInlinePayloadReclaim())||activation.tuple().equals(ProtocolTuple.targetSelfContainedInlinePayloadReclaim())||activation.tuple().equals(ProtocolTuple.targetNativeRetainedReclaim())||activation.tuple().equals(ProtocolTuple.targetObjectPayloadReclaim())||activation.tuple().equals(ProtocolTuple.targetUncommittedObjectReclaim())||activation.tuple().equals(ProtocolTuple.targetAnchorAllocationReclaim())||activation.tuple().equals(ProtocolTuple.targetIncarnationReclaim())||activation.tuple().equals(ProtocolTuple.targetDuplicatePositionReclaim())
                        :activation.tuple().equals(ProtocolTuple.targetEvidenceRetirement()))
                    ||activation.canonicalBytes().length>4096)
                throw new IllegalArgumentException("history grant requires exact SERVICE/key/action and its independent activation tuple");
            publicKey=Bytes.copy(Objects.requireNonNull(publicKey));decodeKey(publicKey);
            targetDescriptorDigest=TargetCompatibilityCodec.assigned(targetDescriptorDigest,32,"exact Target activation descriptor");
            targetActivationDigest=TargetCompatibilityCodec.assigned(targetActivationDigest,32,"actual Target activation projection");
            markerHash=TargetCompatibilityCodec.assigned(markerHash,32,"original Target activation marker hash");
        }
        @Override public byte[] publicKey() { return Bytes.copy(publicKey); }
        @Override public byte[] targetDescriptorDigest(){return Bytes.copy(targetDescriptorDigest);}
        @Override public byte[] targetActivationDigest(){return Bytes.copy(targetActivationDigest);}
        @Override public byte[] markerHash(){return Bytes.copy(markerHash);}
        public PublicKey writerKey() { return decodeKey(publicKey); }
        /** Matches an independently correlated actual Native activation; a locator alone cannot satisfy this. */
        public void requireActivation(TargetProtocolActivationRecord record,SourcePosition source){
            final var payload=Objects.requireNonNull(record).body().payload();
            if(!activation.tuple().equals(payload.descriptor().tuple())||record.appliedAt().source().compareTo(source)>=0
                    ||!Arrays.equals(record.digest(),targetActivationDigest)||!Arrays.equals(payload.descriptor().digest(),targetDescriptorDigest)
                    ||!Arrays.equals(record.marker().mutationHash(),markerHash)||!Arrays.equals(activation.mutationId(),record.marker().systemMutationId())
                    ||!Arrays.equals(activation.sourcePosition().canonicalBytes(),record.appliedAt().source().canonicalBytes())
                    ||!Arrays.equals(activation.canonicalSchemaHash(),payload.descriptor().targetSchemaHash())
                    ||!Arrays.equals(activation.compatibleReaderSetEvidenceHash(),payload.readerEvidenceHash()))
                throw new IllegalArgumentException("history grant differs from actual prior Target activation");
        }
        private byte[] identity() {
            final byte[] old=Bytes.concat(Bytes.lp32(service.canonicalBytes()),Bytes.u32beBits(keyVersion),Bytes.u32be(action.wire));
            return activation.tuple().equals(ProtocolTuple.targetInlinePayloadReclaim())||activation.tuple().equals(ProtocolTuple.targetSelfContainedInlinePayloadReclaim())||activation.tuple().equals(ProtocolTuple.targetNativeRetainedReclaim())||activation.tuple().equals(ProtocolTuple.targetObjectPayloadReclaim())||activation.tuple().equals(ProtocolTuple.targetUncommittedObjectReclaim())||activation.tuple().equals(ProtocolTuple.targetAnchorAllocationReclaim())||activation.tuple().equals(ProtocolTuple.targetIncarnationReclaim())||activation.tuple().equals(ProtocolTuple.targetDuplicatePositionReclaim())
                    ?Bytes.concat(old,Bytes.lp32(activation.tuple().canonicalBytes())):old;
        }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out->{
                CanonicalProtobuf.bytes(out,1,service.canonicalBytes());CanonicalProtobuf.uint32(out,2,Integer.toUnsignedLong(keyVersion));
                CanonicalProtobuf.bytes(out,3,publicKey);CanonicalProtobuf.uint32(out,4,action.wire);
                CanonicalProtobuf.bytes(out,5,activation.canonicalBytes());
                CanonicalProtobuf.uint32(out,6,1);CanonicalProtobuf.bytes(out,7,targetDescriptorDigest);
                CanonicalProtobuf.bytes(out,8,targetActivationDigest);CanonicalProtobuf.bytes(out,9,markerHash);
            });
        }
        private static Grant decode(byte[] encoded) {
            final var f=TargetCompatibilityCodec.read(encoded,MAX_GRANT_BYTES,9,false,"retirement history grant");
            QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9},"retirement history grant");
            if(QueryCodecSupport.uint(f.get(5),6)!=1)throw new IllegalArgumentException("unknown Target activation reference version");
            final var grant=new Grant(AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(0),1)),
                    (int)QueryCodecSupport.uint(f.get(1),2),QueryCodecSupport.fixed(f.get(2),3,44),
                    Action.decode(QueryCodecSupport.uint32(f.get(3),4)),
                    ProtocolActivationState.Activation.decode(QueryCodecSupport.nested(f.get(4),5)),
                    QueryCodecSupport.fixed(f.get(6),7,32),QueryCodecSupport.fixed(f.get(7),8,32),QueryCodecSupport.fixed(f.get(8),9,32));
            QueryCodecSupport.requireCanonical(encoded,grant.canonicalBytes(),"retirement history grant");return grant;
        }
    }

    /** A complete permission table on (afterExclusive, throughInclusive]; an empty table is explicit denial. */
    public record Interval(SourcePosition afterExclusive,SourcePosition throughInclusive,List<Grant> grants) {
        public Interval {
            TargetSourcePosition.requireBounded(afterExclusive);TargetSourcePosition.requireBounded(throughInclusive);
            if(afterExclusive.compareTo(throughInclusive)>=0)throw new IllegalArgumentException("empty or foreign Source history interval");
            grants=List.copyOf(grants);
            if(grants.size()>MAX_GRANTS)throw new IllegalArgumentException("history grant count exceeds finite bound");
            String previous=null;
            for(var grant:grants){
                final String id=Bytes.hex(grant.identity());
                if(previous!=null&&previous.compareTo(id)>=0)throw new IllegalArgumentException("history grants must be unique and sorted by SERVICE/key/action");
                if(grant.activation().sourcePosition().compareTo(afterExclusive)>0)
                    throw new IllegalArgumentException("history activation reference is not effective before the interval");
                previous=id;
            }
        }
        private byte[] policy() { return CanonicalProtobuf.message(out->{for(var grant:grants)CanonicalProtobuf.bytes(out,1,grant.canonicalBytes());}); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out->{
                CanonicalProtobuf.bytes(out,1,afterExclusive.canonicalBytes());CanonicalProtobuf.bytes(out,2,throughInclusive.canonicalBytes());
                // This mandatory complete policy image also encodes the explicit empty/deny branch.
                CanonicalProtobuf.bytes(out,3,policy());
            });
        }
        private static Interval decode(byte[] encoded) {
            final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,3,false,"retirement history interval");
            QueryCodecSupport.requireNumbers(f,new int[]{1,2,3},"retirement history interval");
            final var g=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(2),3),MAX_BYTES,MAX_GRANTS,true,"complete retirement policy");
            final List<Grant> grants=new ArrayList<>();
            for(var field:g){if(field.number()!=1)throw new IllegalArgumentException("unknown complete policy field");grants.add(Grant.decode(QueryCodecSupport.nested(field,1)));}
            final var interval=new Interval(TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(0),1)),
                    TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(1),2)),grants);
            QueryCodecSupport.requireCanonical(encoded,interval.canonicalBytes(),"retirement history interval");return interval;
        }
    }

    public record Snapshot(AuthorIdentity authority,int authorityKeyVersion,long generation,byte[] parentDigest,
            byte[] deploymentRoot,byte[] namespace,TargetQuotaScope scope,byte[] lineage,
            List<Interval> intervals,byte[] signature) {
        public Snapshot {
            Objects.requireNonNull(authority);Objects.requireNonNull(scope);
            parentDigest=Bytes.copy(Objects.requireNonNull(parentDigest));Bytes.requireLength(parentDigest,32,"history parent digest");
            deploymentRoot=TargetCompatibilityCodec.assigned(deploymentRoot,32,"history deployment Root");
            namespace=TargetCompatibilityCodec.assigned(namespace,32,"history Catalog namespace");
            lineage=TargetCompatibilityCodec.assigned(lineage,16,"history lineage");
            signature=Bytes.copy(Objects.requireNonNull(signature));Bytes.requireLength(signature,64,"history signature");
            intervals=List.copyOf(intervals);
            if(authority.kind()!=AuthorIdentity.Kind.CONTROL||authority.canonicalBytes().length>1024||authorityKeyVersion==0
                    ||generation==0||scope.target()!=null||intervals.isEmpty()||intervals.size()>MAX_INTERVALS
                    ||(generation==1)!=Arrays.equals(parentDigest,new byte[32]))
                throw new IllegalArgumentException("invalid exact retirement history authority/coverage");
            int grants=0;Interval previous=null;
            for(var interval:intervals){
                if(!scope.shard().equals(interval.afterExclusive().shardId()))throw new IllegalArgumentException("history interval crossed Shard");
                if(previous!=null&&(!Arrays.equals(previous.throughInclusive().canonicalBytes(),interval.afterExclusive().canonicalBytes())
                        ||Arrays.equals(previous.policy(),interval.policy())))
                    throw new IllegalArgumentException("history coverage must be contiguous with coalesced complete policies");
                grants=Math.addExact(grants,interval.grants().size());previous=interval;
            }
            if(grants>MAX_GRANTS)throw new IllegalArgumentException("complete history exceeds total grant bound");
        }
        @Override public byte[] parentDigest(){return Bytes.copy(parentDigest);}
        @Override public byte[] deploymentRoot(){return Bytes.copy(deploymentRoot);}
        @Override public byte[] namespace(){return Bytes.copy(namespace);}
        @Override public byte[] lineage(){return Bytes.copy(lineage);}
        @Override public byte[] signature(){return Bytes.copy(signature);}
        public SourcePosition afterExclusive(){return intervals.getFirst().afterExclusive();}
        public SourcePosition throughInclusive(){return intervals.getLast().throughInclusive();}
        private byte[] fields(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,authority.canonicalBytes());
            CanonicalProtobuf.uint32(out,3,Integer.toUnsignedLong(authorityKeyVersion));CanonicalProtobuf.uint64Bits(out,4,generation);
            CanonicalProtobuf.bytes(out,5,parentDigest);CanonicalProtobuf.bytes(out,6,deploymentRoot);CanonicalProtobuf.bytes(out,7,namespace);
            CanonicalProtobuf.bytes(out,8,scope.canonicalBytes());CanonicalProtobuf.bytes(out,9,lineage);
            for(var interval:intervals)CanonicalProtobuf.bytes(out,10,interval.canonicalBytes());
        });}
        public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
        public byte[] canonicalBytes(){
            final byte[] encoded=Bytes.concat(fields(),CanonicalProtobuf.message(out->{CanonicalProtobuf.bytes(out,11,digest());CanonicalProtobuf.bytes(out,12,signature);}));
            if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("history snapshot exceeds finite byte bound");return encoded;
        }
        public void requireExtensionOf(Snapshot prior){
            if(prior.generation()==-1L||generation!=prior.generation()+1||!Arrays.equals(parentDigest,prior.digest())
                    ||!Arrays.equals(authority.canonicalBytes(),prior.authority.canonicalBytes())||authorityKeyVersion!=prior.authorityKeyVersion
                    ||!Arrays.equals(deploymentRoot,prior.deploymentRoot)||!Arrays.equals(namespace,prior.namespace)
                    ||!scope.equals(prior.scope)||!Arrays.equals(lineage,prior.lineage)
                    ||!Arrays.equals(afterExclusive().canonicalBytes(),prior.afterExclusive().canonicalBytes())
                    ||throughInclusive().compareTo(prior.throughInclusive())<=0)
                throw new IllegalArgumentException("history generation does not extend the exact sealed parent/scope/coverage");
            int next=0;
            for(var old:prior.intervals){
                while(intervals.get(next).throughInclusive().compareTo(old.afterExclusive())<=0)next++;
                for(int i=next;i<intervals.size()&&intervals.get(i).afterExclusive().compareTo(old.throughInclusive())<0;i++)
                    if(!Arrays.equals(old.policy(),intervals.get(i).policy()))throw new IllegalArgumentException("history rewrites a sealed permission interval");
            }
            // Source order alone does not permit changing the canonical timestamp/identity of a sealed policy boundary.
            for(int i=0;i<prior.intervals.size()-1;i++){
                final var old=prior.intervals.get(i).throughInclusive();
                final var boundary=intervals.stream().map(Interval::throughInclusive).filter(p->p.compareTo(old)==0).findFirst().orElseThrow();
                if(!Arrays.equals(old.canonicalBytes(),boundary.canonicalBytes()))throw new IllegalArgumentException("history changed an original sealed Source boundary");
            }
        }
        public static Snapshot decode(byte[] encoded){
            final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,MAX_INTERVALS+11,true,"retirement authorization history");
            if(f.size()<12)throw new IllegalArgumentException("incomplete retirement history");
            QueryCodecSupport.requireNumbers(f.subList(0,9),new int[]{1,2,3,4,5,6,7,8,9},"retirement history prefix");
            if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown retirement history schema");
            int n=9;final List<Interval> intervals=new ArrayList<>();
            while(n<f.size()&&f.get(n).number()==10)intervals.add(Interval.decode(QueryCodecSupport.nested(f.get(n++),10)));
            QueryCodecSupport.requireNumbers(f.subList(n,f.size()),new int[]{11,12},"retirement history seal");
            final var value=new Snapshot(AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(1),2)),(int)QueryCodecSupport.uint(f.get(2),3),
                    QueryCodecSupport.uint64Bits(f.get(3),4),QueryCodecSupport.fixed(f.get(4),5,32),QueryCodecSupport.fixed(f.get(5),6,32),
                    QueryCodecSupport.fixed(f.get(6),7,32),TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(7),8)),
                    QueryCodecSupport.fixed(f.get(8),9,16),intervals,QueryCodecSupport.fixed(f.get(n+1),12,64));
            if(!Arrays.equals(value.digest(),QueryCodecSupport.fixed(f.get(n),11,32)))throw new IllegalArgumentException("history digest mismatch");
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"retirement authorization history");return value;
        }
    }

    /** Independent trusted pin, never reconstructed from a PublicProof's own issuer/key fields. */
    public record Pin(AuthorIdentity authority,int keyVersion,PublicKey key,byte[] baselineDigest,byte[] root,byte[] namespace){
        public Pin{Objects.requireNonNull(authority);key=decodeKey(Objects.requireNonNull(key).getEncoded());
            if(authority.kind()!=AuthorIdentity.Kind.CONTROL||keyVersion==0)throw new IllegalArgumentException("invalid original history pin");
            baselineDigest=TargetCompatibilityCodec.assigned(baselineDigest,32,"history baseline pin");
            root=TargetCompatibilityCodec.assigned(root,32,"history Root pin");namespace=TargetCompatibilityCodec.assigned(namespace,32,"history namespace pin");}
        @Override public byte[] baselineDigest(){return Bytes.copy(baselineDigest);}
        @Override public byte[] root(){return Bytes.copy(root);}
        @Override public byte[] namespace(){return Bytes.copy(namespace);}
    }
    /** Complete public original chain, preserving the existing Root export bytes; data does not supply its own pin. */
    public record PublicProof(AuthorIdentity authority,int keyVersion,byte[] publicKey,byte[] baselineDigest,List<Snapshot> chain){
        public static final int MAX_SNAPSHOTS=33;
        public static final int MAX_BYTES=MAX_SNAPSHOTS*TargetRetirementAuthorizationHistory.MAX_BYTES+2048;
        public PublicProof{
            Objects.requireNonNull(authority);publicKey=Bytes.copy(publicKey);decodeKey(publicKey);
            baselineDigest=TargetCompatibilityCodec.assigned(baselineDigest,32,"original history baseline");chain=List.copyOf(chain);
            if(authority.kind()!=AuthorIdentity.Kind.CONTROL||keyVersion==0||authority.canonicalBytes().length>1024
                    ||chain.isEmpty()||chain.size()>MAX_SNAPSHOTS||!Arrays.equals(chain.getFirst().digest(),baselineDigest))
                throw new IllegalArgumentException("incomplete original public history proof");
            Snapshot prior=null;
            for(var value:chain){
                if(!Arrays.equals(value.authority().canonicalBytes(),authority.canonicalBytes())||value.authorityKeyVersion()!=keyVersion)
                    throw new IllegalArgumentException("history proof changed issuer");
                if(prior!=null)value.requireExtensionOf(prior);prior=value;
            }
        }
        @Override public byte[] publicKey(){return Bytes.copy(publicKey);}
        @Override public byte[] baselineDigest(){return Bytes.copy(baselineDigest);}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,authority.canonicalBytes());
            CanonicalProtobuf.uint32(out,3,Integer.toUnsignedLong(keyVersion));CanonicalProtobuf.bytes(out,4,publicKey);
            CanonicalProtobuf.bytes(out,5,baselineDigest);for(var value:chain)CanonicalProtobuf.bytes(out,6,value.canonicalBytes());
        });}
        /** Monotone public ancestry comparison only, never an independent signature/purpose authorization. */
        public void requireContainsTip(PublicProof previous){
            final var tip=Objects.requireNonNull(previous).chain().getLast();
            if(chain.stream().noneMatch(value->value.generation()==tip.generation()&&Arrays.equals(value.digest(),tip.digest())))
                throw new Unavailable("history proof omits or forks the actual persisted accepted tip");
        }
        public Verified authenticate(Pin pin){
            Objects.requireNonNull(pin,"independent original history pin");
            if(!Arrays.equals(authority.canonicalBytes(),pin.authority().canonicalBytes())||keyVersion!=pin.keyVersion()
                    ||!Arrays.equals(publicKey,pin.key().getEncoded())||!Arrays.equals(baselineDigest,pin.baselineDigest()))
                throw new Unavailable("public history proof differs from independent original Root pin");
            Verified last=null;
            for(var value:chain)last=verify(value,pin.authority(),pin.keyVersion(),pin.key(),pin.root(),pin.namespace());
            return Objects.requireNonNull(last);
        }
        public static PublicProof decode(byte[] encoded){
            final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,MAX_SNAPSHOTS+5,true,"public retirement history proof");
            if(f.size()<6)throw new IllegalArgumentException("incomplete public history proof");
            QueryCodecSupport.requireNumbers(f.subList(0,5),new int[]{1,2,3,4,5},"public history prefix");
            if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown public history proof version");
            final var values=new ArrayList<Snapshot>();
            for(int i=5;i<f.size();i++){if(f.get(i).number()!=6)throw new IllegalArgumentException("unknown history chain field");values.add(Snapshot.decode(QueryCodecSupport.bytes(f.get(i),6)));}
            final var value=new PublicProof(AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(1),2)),QueryCodecSupport.uint32(f.get(2),3),
                    QueryCodecSupport.fixed(f.get(3),4,44),QueryCodecSupport.fixed(f.get(4),5,32),values);
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"public retirement history proof");return value;
        }
    }

    /** Created only after verification against the independent purpose-pinned Root key; no key is carried by Snapshot. */
    public static final class Verified {
        private final Snapshot snapshot;
        private Verified(Snapshot snapshot){this.snapshot=snapshot;}
        public Snapshot snapshot(){return snapshot;}
        public void requireCoverage(TargetExternalEvidenceObjectResource resource,SourcePosition source){interval(resource,source);}
        private Interval interval(TargetExternalEvidenceObjectResource resource,SourcePosition source){
            return interval(new TargetQuotaScope(resource.shard(),resource.tenantScope(),null),resource.recoveryLineage(),
                    resource.deploymentRootDigest(),resource.catalogNamespaceId(),source);
        }
        private Interval interval(TargetQuotaScope scope,byte[] lineage,byte[] root,byte[] namespace,SourcePosition source){
            if(!snapshot.scope.equals(scope)||!Arrays.equals(snapshot.lineage,lineage)||!Arrays.equals(snapshot.deploymentRoot,root)
                    ||!Arrays.equals(snapshot.namespace,namespace))throw new Unavailable("history does not cover exact resource scope");
            if(!snapshot.afterExclusive().sameSourceIdentity(source)||source.compareTo(snapshot.afterExclusive())<=0
                    ||source.compareTo(snapshot.throughInclusive())>0)throw new Unavailable("original Source point is outside sealed complete history");
            for(var interval:snapshot.intervals)if(source.compareTo(interval.throughInclusive())<=0)return interval;
            throw new Unavailable("incomplete retirement history coverage");
        }
        /** Empty means proved deny inside complete coverage. Outside coverage throws and must never become denial. */
        public Optional<Grant> select(TargetExternalEvidenceObjectResource resource,SourcePosition source,
                AuthorIdentity service,int version,Action action){
            if(action==Action.RESOURCE_RECLAIM)throw new IllegalArgumentException("native reclaim requires its own closed Source body/scope");
            return select(interval(resource,source),service,version,action);
        }
        public void requireCoverage(TargetResourceReclaimBody body,SourcePosition source){
            interval(body.scope(),body.lineage(),body.deploymentRoot(),body.namespace(),source);
        }
        public Optional<Grant> select(TargetResourceReclaimBody body,SourcePosition source,AuthorIdentity service,int version){
            final var covered=interval(body.scope(),body.lineage(),body.deploymentRoot(),body.namespace(),source);
            final byte[] actor=Objects.requireNonNull(service).canonicalBytes();
            for(var grant:covered.grants)if(grant.keyVersion==version&&grant.action==Action.RESOURCE_RECLAIM
                    &&grant.activation().tuple().equals(body.authorizationTuple())&&Arrays.equals(grant.service.canonicalBytes(),actor))return Optional.of(grant);
            return Optional.empty();
        }
        private Optional<Grant> select(Interval covered,AuthorIdentity service,int version,Action action){
            final byte[] actor=Objects.requireNonNull(service).canonicalBytes();
            for(var grant:covered.grants)if(grant.keyVersion==version&&grant.action==action
                    &&Arrays.equals(grant.service.canonicalBytes(),actor))return Optional.of(grant);
            return Optional.empty();
        }
    }
    public static Verified verify(Snapshot snapshot,AuthorIdentity pinnedAuthority,int version,PublicKey key,
            byte[] root,byte[] namespace){
        if(!Arrays.equals(snapshot.authority.canonicalBytes(),pinnedAuthority.canonicalBytes())||snapshot.authorityKeyVersion!=version
                ||!Arrays.equals(snapshot.deploymentRoot,root)||!Arrays.equals(snapshot.namespace,namespace))
            throw new Unavailable("retirement history differs from independent purpose-pinned authority/Root/namespace");
        try{
            final var verifier=Signature.getInstance("Ed25519");verifier.initVerify(decodeKey(key.getEncoded()));
            verifier.update(Bytes.sha256(SIGN_DOMAIN,snapshot.digest()));
            if(!verifier.verify(snapshot.signature))throw new Unavailable("retirement history signature is not authenticated");
            snapshot.canonicalBytes();return new Verified(snapshot);
        }catch(java.security.GeneralSecurityException invalid){throw new Unavailable("invalid retirement history authority key",invalid);}
    }
    private static PublicKey decodeKey(byte[] encoded){
        if(encoded==null||encoded.length!=44)throw new IllegalArgumentException("history requires exact public Ed25519 X509 key");
        try{return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));}
        catch(java.security.GeneralSecurityException invalid){throw new IllegalArgumentException("invalid retirement history public key",invalid);}
    }
    public static final class Unavailable extends RuntimeException {
        private static final long serialVersionUID=1L;
        public Unavailable(String message){super(message);}
        public Unavailable(String message,Throwable cause){super(message,cause);}
    }
}
