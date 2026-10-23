package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetReservationRecord;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import java.util.Arrays;
import java.util.Objects;

/** Full projections of retained Native acceptance facts. These bytes are neither an authority nor a new ledger. */
public record TargetObjectPayloadOrigin(Kind kind,TargetScheduleBinding binding,
        TargetResultRecord acceptedFirst,TargetResultRecord acceptedPosition,TargetReservationRecord reservation,
        TargetResultRecord prepareFirst,TargetResultRecord preparePosition) {
    public static final int SCHEMA_VERSION=1;
    public static final int MAX_CANONICAL_BYTES=TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;
    public enum Kind { PREPARE_COMMIT, DIRECT_SCHEDULE }
    public TargetObjectPayloadOrigin {
        Objects.requireNonNull(kind);Objects.requireNonNull(binding);
        requirePair(acceptedFirst,acceptedPosition);
        if(kind==Kind.PREPARE_COMMIT){
            Objects.requireNonNull(reservation);requirePair(prepareFirst,preparePosition);
            if(binding.commandType()!=CommandType.PREPARE_LARGE_SCHEDULE||reservation.status()!=PayloadReservationStatus.COMMITTED
                    ||!reservation.mutation().equals(acceptedFirst.mutation())||!reservation.prepareAnchor().equals(prepareFirst.mutation()))
                throw new IllegalArgumentException("Native OBJECT acceptance changed original Prepare/first Commit identity");
            reservation.requireBinding(binding);reservation.mutation().requireSourceApplied();
            acceptedFirst.mutation().requireAfter(prepareFirst.mutation());
        }else if(reservation!=null||prepareFirst!=null||preparePosition!=null||binding.commandType()!=CommandType.SCHEDULE
                ||binding.intent().hasInlinePayload()||!Arrays.equals(binding.bindingSource().canonicalBytes(),acceptedFirst.mutation().source().canonicalBytes())){
            throw new IllegalArgumentException("DIRECT_SCHEDULE must retain its own original object Schedule and no fabricated reservation");
        }
    }
    private static void requirePair(TargetResultRecord first,TargetResultRecord position){
        Objects.requireNonNull(first);Objects.requireNonNull(position);
        if(first.kind()!=TargetResultRecord.Kind.COMMAND||!first.appliedFirst()||position.kind()!=TargetResultRecord.Kind.POSITION_COMMAND
                ||!first.mutation().equals(position.mutation()))throw new IllegalArgumentException("actual original Command first/POSITION required");
        position.requireFirst(first);
    }
    public ProfileRef profile(){return kind==Kind.PREPARE_COMMIT?PrepareLargeScheduleBody.decode(binding.canonicalBody()).objectStoreProfile():binding.intent().committedPayload().objectStoreProfile();}
    public void requireOwner(TargetQuotaPayloadOwner expected){
        expected.requireInitialBinding(binding);
        if(expected.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||expected.committedPayload()==null
                ||!Arrays.equals(profile().semanticHash(),expected.objectStoreProfileHash()))throw new IllegalArgumentException("Native acceptance changed exact committed OBJECT/Profile");
        if(reservation!=null)reservation.requireOwner(expected);
        else if(!PayloadReference.fromDescriptor(binding.intent().committedPayload()).equals(expected.committedPayload()))
            throw new IllegalArgumentException("direct Schedule acceptance changed its immutable payload descriptor");
        requireOwner(acceptedFirst,expected);
        // POSITION is charged to the original Source SHARD, joined to this Target first by requirePair.
        if(prepareFirst!=null)requireOwner(prepareFirst,expected);
        acceptedFirst.mutation().requireAtOrBefore(expected.mutation());
    }
    private static void requireOwner(TargetResultRecord record,TargetQuotaPayloadOwner owner){
        if(!record.primaryIdentity().equals(owner.primaryIdentity())||!record.accounting().equals(owner.accounting())
                ||!Arrays.equals(record.tenantScope(),owner.tenantScope())||!Arrays.equals(record.recoveryLineage(),owner.recoveryLineage()))
            throw new IllegalArgumentException("original OBJECT acceptance changed frozen owner/accounting/tenant/lineage");
    }
    public byte[] canonicalBytes(){
        final byte[] value=CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,SCHEMA_VERSION);CanonicalProtobuf.uint32(out,2,kind==Kind.PREPARE_COMMIT?1:2);
            CanonicalProtobuf.bytes(out,3,binding.canonicalBytes());CanonicalProtobuf.bytes(out,4,acceptedFirst.canonicalBytes());CanonicalProtobuf.bytes(out,5,acceptedPosition.canonicalBytes());
            if(reservation!=null){CanonicalProtobuf.bytes(out,6,reservation.canonicalBytes());CanonicalProtobuf.bytes(out,7,prepareFirst.canonicalBytes());CanonicalProtobuf.bytes(out,8,preparePosition.canonicalBytes());}
        });
        if(value.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("Native OBJECT origin exceeds original body input bound");return value;
    }
    public static TargetObjectPayloadOrigin decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,8,false,"Native OBJECT origin");
        QueryCodecSupport.requireNumbers(f,f.size()==8?new int[]{1,2,3,4,5,6,7,8}:new int[]{1,2,3,4,5},"Native OBJECT origin");
        final int kind=QueryCodecSupport.uint32(f.get(1),2);
        if(QueryCodecSupport.uint32(f.get(0),1)!=1||kind<1||kind>2||(kind==1)!=(f.size()==8))throw new IllegalArgumentException("unknown Native OBJECT origin schema/provenance");
        final var value=new TargetObjectPayloadOrigin(kind==1?Kind.PREPARE_COMMIT:Kind.DIRECT_SCHEDULE,TargetScheduleBinding.decode(QueryCodecSupport.bytes(f.get(2),3)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(3),4)),TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(4),5)),
                kind==1?TargetReservationRecord.decode(QueryCodecSupport.bytes(f.get(5),6)):null,
                kind==1?TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(6),7)):null,
                kind==1?TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(7),8)):null);
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Native OBJECT origin");return value;
    }
}
