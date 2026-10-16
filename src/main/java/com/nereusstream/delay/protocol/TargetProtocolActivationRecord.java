package com.nereusstream.delay.protocol;

import com.nereusstream.delay.assessment.DataResetManifest;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;

/** Source-applied Target activation with complete public marker/base/reader inputs; data does not self-authorize. */
public record TargetProtocolActivationRecord(SystemMutation marker,PreparedControlOperation prepared,
        TargetQuotaMutation appliedAt,TargetProtocolReaderSet readers,PublicEvidence evidence) {
    public static final int VALUE_TYPE=53;
    public static final int MAX_PREPARED_BYTES=TargetProtocolActivatePayload.MAX_BYTES+4096;
    public static final int MAX_MARKER_BYTES=TargetProtocolActivationBody.MAX_BYTES+1024;
    public static final int MAX_BYTES=MAX_MARKER_BYTES+MAX_PREPARED_BYTES+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            +TargetProtocolReaderSet.MAX_BYTES+PublicEvidence.MAX_BYTES+256;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-protocol-activation-record\0");
    public record PublicEvidence(ControlAuthorizationContext actor,byte[] preparedKey,byte[] markerKey,byte[] resetIssuer,DataResetManifest reset) {
        public static final int MAX_BYTES=(1<<20)+1024;
        public PublicEvidence {
            Objects.requireNonNull(actor);Objects.requireNonNull(reset);
            preparedKey=Bytes.copy(preparedKey);markerKey=Bytes.copy(markerKey);resetIssuer=Bytes.copy(resetIssuer);
            key(preparedKey);key(markerKey);
            if(!actor.roleSet().contains(ControlRole.PLATFORM_OPERATOR)||!reset.verifySignature(key(resetIssuer)))
                throw new IllegalArgumentException("activation evidence lacks PLATFORM_OPERATOR or authentic reset bytes");
        }
        @Override public byte[] preparedKey(){return Bytes.copy(preparedKey);}
        @Override public byte[] markerKey(){return Bytes.copy(markerKey);}
        @Override public byte[] resetIssuer(){return Bytes.copy(resetIssuer);}
        public byte[] canonicalBytes(){
            final byte[] encoded=CanonicalProtobuf.message(out->{
                CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,actor.actorIdHash());CanonicalProtobuf.bytes(out,3,actor.roleSet().canonicalBytes());
                CanonicalProtobuf.bytes(out,4,actor.tenantResourceScopeHash());CanonicalProtobuf.bytes(out,5,preparedKey);
                CanonicalProtobuf.bytes(out,6,markerKey);CanonicalProtobuf.bytes(out,7,resetIssuer);CanonicalProtobuf.bytes(out,8,reset.canonicalBytes());
            });
            if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("activation public evidence exceeds bound");return encoded;
        }
        public static PublicEvidence decode(byte[] encoded){
            final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,8,false,"Target activation public evidence");
            QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8},"Target activation public evidence");
            if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown activation public evidence schema");
            final var roles=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(2),3),128,ControlRole.values().length,true,"activation original roles");
            final var set=EnumSet.noneOf(ControlRole.class);
            for(var role:roles){
                final long wire=QueryCodecSupport.uint(role,1);
                final var value=Arrays.stream(ControlRole.values()).filter(r->r.wireValue()==wire).findFirst().orElseThrow(()->new IllegalArgumentException("unknown activation role"));
                if(!set.add(value))throw new IllegalArgumentException("duplicate activation original role");
            }
            final var value=new PublicEvidence(new ControlAuthorizationContext(QueryCodecSupport.fixed(f.get(1),2,32),new ControlRoleSet(set),QueryCodecSupport.fixed(f.get(3),4,32)),
                    QueryCodecSupport.fixed(f.get(4),5,44),QueryCodecSupport.fixed(f.get(5),6,44),QueryCodecSupport.fixed(f.get(6),7,44),
                    DataResetManifest.decode(QueryCodecSupport.bytes(f.get(7),8)));
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target activation public evidence");return value;
        }
    }
    public TargetProtocolActivationRecord {
        Objects.requireNonNull(marker);Objects.requireNonNull(prepared);Objects.requireNonNull(appliedAt).requireSourceApplied();
        Objects.requireNonNull(readers);Objects.requireNonNull(evidence);
        final var body=TargetProtocolActivationBody.decode(marker.canonicalBody());final var payload=body.payload();final var descriptor=payload.descriptor();
        final var actor=AuthorIdentity.decode(marker.authorIdentity());
        if(marker.type()!=SystemMutationType.APPLY_SHARD_CONTROL||marker.canonicalEnvelope().length>MAX_MARKER_BYTES
                ||prepared.canonicalBytes().length>MAX_PREPARED_BYTES||prepared.kind()!=ControlOperationKind.ACTIVATE_TARGET_PROTOCOL
                ||!payload.operationRequest().equals(prepared.request())||!marker.shardId().equals(body.shard())||marker.retryUntilEpochMs()!=body.retryUntil()
                ||!Arrays.equals(marker.logicalOperationIdentity(),body.logicalIdentity())||!Arrays.equals(appliedAt.mutationDigest(),Bytes.sha256(marker.canonicalEnvelope()))
                ||!body.shard().equals(appliedAt.source().shardId())||appliedAt.source().brokerPersistenceTimeEpochMs()>body.retryUntil()
                ||actor.kind()!=AuthorIdentity.Kind.CONTROL||!Arrays.equals(actor.first(),evidence.actor.actorIdHash())
                ||!Arrays.equals(actor.second(),evidence.actor.roleSet().digest())||!Arrays.equals(actor.digest(),evidence.actor.tenantResourceScopeHash())
                ||!Arrays.equals(prepared.author().operationActorIdHash(),actor.first())||!Arrays.equals(prepared.author().authenticatedRoleSetHash(),actor.second())
                ||!Arrays.equals(prepared.author().tenantResourceScopeHash(),actor.digest())
                ||!prepared.verifySignature(key(evidence.preparedKey))||!marker.verifySignature(key(evidence.markerKey))
                ||!readers.descriptor().equals(descriptor)||!Arrays.equals(readers.digest(),payload.readerEvidenceHash())
                ||!evidence.reset.artifacts().equals(descriptor.base())||!Arrays.equals(evidence.reset.manifestDigest(),descriptor.baseResetManifest())
                ||!evidence.reset.scope().shard().shardId().equals(payload.scope().shard())
                ||!Arrays.equals(evidence.reset.scope().tenantScopeDigest(),payload.scope().tenantScope()))
            throw new IllegalArgumentException("Target activation lost original signed Control/Source/base/reader binding");
        ControlTargetMutationBinding.validate(prepared,prepared.targets().getFirst(),marker);
    }
    public TargetProtocolActivationBody body(){return TargetProtocolActivationBody.decode(marker.canonicalBody());}
    public byte[] key(){return key(body().payload().descriptor().tuple());}
    public static byte[] key(ProtocolTuple tuple){return Bytes.concat(new byte[]{TargetKeyCodec.PROTOCOL_ACTIVATION_TAG,TargetKeyCodec.KEY_FORMAT},
            Bytes.sha256(Bytes.utf8("nereus-delay-target-protocol-activation-key\0"),tuple.canonicalBytes()));}
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,marker.canonicalEnvelope());CanonicalProtobuf.bytes(out,3,prepared.canonicalBytes());
        CanonicalProtobuf.bytes(out,4,appliedAt.canonicalBytes());CanonicalProtobuf.bytes(out,5,readers.canonicalBytes());CanonicalProtobuf.bytes(out,6,evidence.canonicalBytes());
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){
        final byte[] encoded=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,7,digest())));
        if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("Target activation row exceeds finite bound");return encoded;
    }
    public static TargetProtocolActivationRecord decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,7,false,"Target activation record");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7},"Target activation record");
        if(QueryCodecSupport.uint(f.get(0),1)!=1)throw new IllegalArgumentException("unknown Target activation record schema");
        final byte[] markerBytes=QueryCodecSupport.bytes(f.get(1),2),preparedBytes=QueryCodecSupport.bytes(f.get(2),3);
        if(markerBytes.length>MAX_MARKER_BYTES||preparedBytes.length>MAX_PREPARED_BYTES)throw new IllegalArgumentException("activation marker/prepared input exceeds bound");
        final var marker=SystemMutation.decodeTargetEnvelope(markerBytes);final var body=TargetProtocolActivationBody.decode(marker.canonicalBody());
        final var value=new TargetProtocolActivationRecord(marker,PreparedControlOperation.decode(preparedBytes),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3),4)),TargetProtocolReaderSet.decode(QueryCodecSupport.bytes(f.get(4),5),body.payload().descriptor()),
                PublicEvidence.decode(QueryCodecSupport.bytes(f.get(5),6)));
        if(!Arrays.equals(value.digest(),QueryCodecSupport.fixed(f.get(6),7,32)))throw new IllegalArgumentException("Target activation record digest mismatch");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target activation record");return value;
    }
    public static TargetProtocolActivationRecord decodeForStore(byte[] key,byte[] encoded,TargetQuotaScope scope,byte[] lineage){
        final var record=decode(encoded);final var payload=record.body().payload();
        if(!Arrays.equals(key,record.key())||!scope.equals(payload.scope())||!Arrays.equals(lineage,payload.lineage()))
            throw new IllegalArgumentException("Target activation key/scope/lineage mismatch");return record;
    }
    private static PublicKey key(byte[] encoded){
        if(encoded==null||encoded.length!=44)throw new IllegalArgumentException("activation evidence requires exact public Ed25519 key");
        try{return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));}
        catch(java.security.GeneralSecurityException invalid){throw new IllegalArgumentException("invalid activation evidence public key",invalid);}
    }
}
