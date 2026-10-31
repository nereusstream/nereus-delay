package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Durable Native OBJECT intent. Its owner, primary and bytes stay charged until a separate Source confirmation. */
public record TargetPayloadObjectRetiringRecord(SystemMutation intent,TargetQuotaMutation appliedAt) {
    public static final int VALUE_TYPE=56;
    public static final int MAX_FRAME_BYTES=TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES+(1<<20)+4096;
    public static final int MAX_CANONICAL_BYTES=MAX_FRAME_BYTES+TargetQuotaMutation.MAX_CANONICAL_BYTES+256;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-payload-object-retiring\0");
    public TargetPayloadObjectRetiringRecord {
        Objects.requireNonNull(intent);Objects.requireNonNull(appliedAt).requireSourceApplied();
        requireIntentOnlyFrame(intent.canonicalEnvelope());
        final var body=TargetResourceReclaimBody.decode(intent.canonicalBody());
        if(intent.type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM||!intent.shardId().equals(body.shard())
                ||intent.retryUntilEpochMs()!=body.retryUntil()||!Arrays.equals(intent.logicalOperationIdentity(),body.logicalOperationIdentity())
                ||!Arrays.equals(Bytes.sha256(intent.canonicalEnvelope()),appliedAt.mutationDigest())
                ||!body.shard().equals(appliedAt.source().shardId()))throw new IllegalArgumentException("OBJECT intent changed signed Source identity");
        appliedAt.requireAfter(body.objectOwner().mutation());
        if(body.floor().appliedSourcePosition().compareTo(appliedAt.source())>=0
                ||Long.compareUnsigned(body.floor().includedMutationSequence(),appliedAt.sequence())>=0)
            throw new IllegalArgumentException("OBJECT intent must follow its actual closed Floor");
    }
    public TargetResourceReclaimBody body(){return TargetResourceReclaimBody.decode(intent.canonicalBody());}
    public TargetResourceReclaimBody.ObjectPayloadRetire operation(){return (TargetResourceReclaimBody.ObjectPayloadRetire)body().operations().getFirst();}
    public TargetResourceReclaimBody.UncommittedObjectRetire uncommittedOperation(){return (TargetResourceReclaimBody.UncommittedObjectRetire)body().operations().getFirst();}
    public TargetQuotaPayloadOwner expectedOwner(){return body().objectOwner();}
    private int schemaVersion(){return body().migrationPayloadReclaim()?body().uncommittedObjectReclaim()?4:3:body().uncommittedObjectReclaim()?2:1;}
    public byte[] key(){return key(expectedOwner().messageId());}
    public static byte[] key(DelayMessageId message){return Bytes.concat(new byte[]{TargetKeyCodec.PAYLOAD_OBJECT_RETIRING_TAG,TargetKeyCodec.KEY_FORMAT},message.bytes());}
    public CapacityVector recordCharge(){return expectedOwner().accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key().length,canonicalBytes().length);}
    private byte[] fields(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,schemaVersion());
        CanonicalProtobuf.bytes(out,2,intent.canonicalEnvelope());CanonicalProtobuf.bytes(out,3,appliedAt.canonicalBytes());});}
    public byte[] canonicalBytes(){final byte[] fields=fields();final byte[] encoded=Bytes.concat(fields,
            CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,4,Bytes.sha256(DOMAIN,fields))));
        if(encoded.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("OBJECT intent row exceeds finite bound");return encoded;}
    public static TargetPayloadObjectRetiringRecord decode(byte[] encoded){
        final var fields=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,4,false,"Native OBJECT retiring record");
        QueryCodecSupport.requireNumbers(fields,new int[]{1,2,3,4},"Native OBJECT retiring record");
        final int schema=QueryCodecSupport.uint32(fields.get(0),1);
        if(schema<1||schema>4)throw new IllegalArgumentException("unknown Native OBJECT intent schema");
        final byte[] frame=QueryCodecSupport.bytes(fields.get(1),2);requireIntentOnlyFrame(frame);
        final var value=new TargetPayloadObjectRetiringRecord(SystemMutation.decodeEnvelope(frame),TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(2),3)));
        if(schema!=value.schemaVersion())throw new IllegalArgumentException("Native intent schema/body mismatch");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Native OBJECT retiring record");return value;
    }
    public static TargetPayloadObjectRetiringRecord decodeForStore(byte[] key,byte[] encoded,TargetQuotaScope scope,byte[] lineage){
        final var value=decode(encoded);final var body=value.body();
        if(!Arrays.equals(key,value.key())||!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage))
            throw new IllegalArgumentException("Native OBJECT intent changed key/Source scope/lineage");return value;
    }
    /** Reject a nested confirmation before SystemMutationIdentity can recurse into its original intent. */
    private static void requireIntentOnlyFrame(byte[] frame){
        final var outer=TargetCompatibilityCodec.read(frame,MAX_FRAME_BYTES,2,false,"OBJECT intent envelope");
        QueryCodecSupport.requireNumbers(outer,new int[]{1,3},"OBJECT intent envelope");
        final var system=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(outer.get(1),3),MAX_FRAME_BYTES,11,false,"OBJECT intent System");
        if(system.size()!=11||QueryCodecSupport.uint32(system.get(3),4)!=SystemMutationType.TARGET_RESOURCE_RECLAIM.wireValue())
            throw new IllegalArgumentException("OBJECT intent must be type14");
        final var body=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(system.get(5),6),TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES,11,false,"OBJECT original intent");
        QueryCodecSupport.requireNumbers(body,body.size()==10?new int[]{1,2,3,9,10,11,12,13,14,15}:new int[]{1,2,3,9,10,11,12,13,14,15,16},"OBJECT original intent");
        final int bodyVersion=QueryCodecSupport.uint32(body.get(3),9);
        if(bodyVersion!=TargetResourceReclaimBody.OBJECT_PAYLOAD_BODY_VERSION&&bodyVersion!=TargetResourceReclaimBody.UNCOMMITTED_OBJECT_BODY_VERSION&&bodyVersion!=TargetResourceReclaimBody.MIGRATION_OBJECT_BODY_VERSION&&bodyVersion!=TargetResourceReclaimBody.MIGRATION_UNCOMMITTED_OBJECT_BODY_VERSION)
            throw new IllegalArgumentException("OBJECT intent requires an explicit supported body");
        if((bodyVersion==TargetResourceReclaimBody.UNCOMMITTED_OBJECT_BODY_VERSION||bodyVersion==TargetResourceReclaimBody.MIGRATION_UNCOMMITTED_OBJECT_BODY_VERSION)!=(body.size()==10))throw new IllegalArgumentException("intent facts layout/body mismatch");
        final var group=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(body.get(9),15),TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES,1,false,"one OBJECT intent");
        QueryCodecSupport.requireNumbers(group,new int[]{1},"one OBJECT intent");
        final var operation=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(group.getFirst(),1),TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES,9,false,"OBJECT intent operation");
        if(bodyVersion==TargetResourceReclaimBody.UNCOMMITTED_OBJECT_BODY_VERSION||bodyVersion==TargetResourceReclaimBody.MIGRATION_UNCOMMITTED_OBJECT_BODY_VERSION){
            QueryCodecSupport.requireNumbers(operation,new int[]{1,2,3,4,11},"uncommitted original intent");
            if(QueryCodecSupport.uint32(operation.get(0),1)!=(bodyVersion==TargetResourceReclaimBody.MIGRATION_UNCOMMITTED_OBJECT_BODY_VERSION?2:1)||QueryCodecSupport.uint32(operation.get(1),2)!=4||QueryCodecSupport.uint32(operation.get(2),3)!=3)
                throw new IllegalArgumentException("only an original uncommitted intent may be nested");return;
        }
        final int schema=QueryCodecSupport.uint32(operation.get(0),1);
        if(schema!=1&&schema!=2&&schema!=3)throw new IllegalArgumentException("unknown OBJECT intent operation schema");
        QueryCodecSupport.requireNumbers(operation,schema==1?new int[]{1,2,3,4,5,6,7,8,9}:new int[]{1,2,3,4,5,6,7,8,10},"OBJECT intent operation");
        if(QueryCodecSupport.uint32(operation.get(1),2)!=4||QueryCodecSupport.uint32(operation.get(2),3)!=1)
            throw new IllegalArgumentException("only a nonrecursive original OBJECT intent can be retained");
    }
}
