package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.TargetReservationRecord;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.Objects;

/** Frozen reservation causality. Local materialization is never represented as a Source-applied frame. */
public record TargetUncommittedPayloadFacts(TargetScheduleBinding binding,TargetReservationRecord reservation,
        TargetResultRecord prepareFirst,TargetResultRecord preparePosition,TargetInlinePayloadSourceFacts.Frame cause,
        TargetInlinePayloadSourceFacts.Frame fence,RetryPolicySemantic retry,TerminalPolicySemantic terminal,TargetMigrationPayloadHistory migrationHistory) {
    public TargetUncommittedPayloadFacts(TargetScheduleBinding binding,TargetReservationRecord reservation,TargetResultRecord first,TargetResultRecord position,
            TargetInlinePayloadSourceFacts.Frame cause,TargetInlinePayloadSourceFacts.Frame fence,RetryPolicySemantic retry,TerminalPolicySemantic terminal){this(binding,reservation,first,position,cause,fence,retry,terminal,null);}
    public static final int SCHEMA_VERSION=1;
    public static final int MAX_BYTES=TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;
    public TargetUncommittedPayloadFacts {
        Objects.requireNonNull(binding);Objects.requireNonNull(reservation);
        if(reservation.migrationOrigin()==null){Objects.requireNonNull(prepareFirst);Objects.requireNonNull(preparePosition);if(migrationHistory!=null)throw new IllegalArgumentException("ordinary Prepare cannot carry migration history");}
        else if(!binding.imported()||prepareFirst!=null||preparePosition!=null)throw new IllegalArgumentException("migration Prepare must preserve its original history without fabricated Native first");Objects.requireNonNull(cause);Objects.requireNonNull(fence);
        Objects.requireNonNull(retry);Objects.requireNonNull(terminal);
        if(binding.commandType()!=CommandType.PREPARE_LARGE_SCHEDULE||reservation.committedPayload()!=null
                ||reservation.status()!=PayloadReservationStatus.EXPIRED&&reservation.status()!=PayloadReservationStatus.ABANDONED
                ||reservation.migrationOrigin()==null&&(prepareFirst.kind()!=TargetResultRecord.Kind.COMMAND||!prepareFirst.appliedFirst()
                ||preparePosition.kind()!=TargetResultRecord.Kind.POSITION_COMMAND
                ||!prepareFirst.mutation().equals(reservation.prepareAnchor())||!preparePosition.mutation().equals(reservation.prepareAnchor())
                ||!Arrays.equals(prepareFirst.logicalId(),reservation.prepareCommandId().bytes())))
            throw new IllegalArgumentException("original successful Prepare and terminal uncommitted reservation required");
        reservation.requireBinding(binding);if(preparePosition!=null)preparePosition.requireFirst(prepareFirst);
        if(cause.canonicalBytes().length>TargetInlinePayloadSourceFacts.MAX_BYTES||fence.canonicalBytes().length>TargetInlinePayloadSourceFacts.MAX_BYTES)
            throw new IllegalArgumentException("original causal frames exceed their unchanged Frame1 decode bound");
        if(!binding.intent().retryPolicy().matches(retry))throw new IllegalArgumentException("uncommitted retention changed frozen Retry Policy");
        terminal.requireBinding(retry);
        cause.appliedAt().requireAfter(reservation.prepareAnchor());
        cause.appliedAt().requireAtOrBefore(reservation.mutation());
        reservation.mutation().requireAtOrBefore(fence.appliedAt());
        if(fence.kind()!=2||SystemMutation.decodeEnvelope(fence.frame()).type()!=SystemMutationType.TIME_FENCE)
            throw new IllegalArgumentException("original accepted closure TimeFence required");
        if(reservation.status()==PayloadReservationStatus.EXPIRED){
            if(cause.kind()!=2||SystemMutation.decodeEnvelope(cause.frame()).type()!=SystemMutationType.TIME_FENCE
                    ||TargetTimeFenceBody.decode(SystemMutation.decodeEnvelope(cause.frame()).canonicalBody()).closeThrough()<reservation.expiryEpochMs()
                    ||reservation.mutation().reservationClosure())throw new IllegalArgumentException("expiry lost its true causal Source fence");
        }else if(reservation.closure()!=null){
            if(cause.kind()!=2||SystemMutation.decodeEnvelope(cause.frame()).type()!=SystemMutationType.APPLY_SHARD_CONTROL
                    ||!Arrays.equals(cause.appliedAt().source().canonicalBytes(),reservation.closure().source().canonicalBytes())
                    ||!reservation.mutation().reservationClosure())throw new IllegalArgumentException("local Close lost original accepted control causality");
        }else{
            if(cause.kind()!=1||CommandCodec.decodeFrame(cause.frame()).type()!=CommandType.CANCEL
                    ||!CommandCodec.decodeFrame(cause.frame()).delayMessageId().equals(binding.messageId())
                    ||!cause.appliedAt().equals(reservation.mutation()))throw new IllegalArgumentException("abandonment lost original accepted Cancel");
        }
        final long deadline=Math.addExact(cause.appliedAt().source().brokerPersistenceTimeEpochMs(),terminal.minimumPayloadRetentionMs());
        if(TargetTimeFenceBody.decode(SystemMutation.decodeEnvelope(fence.frame()).canonicalBody()).closeThrough()<deadline)
            throw new IllegalArgumentException("frozen uncommitted retention has not closed at the accepted Source fence");
    }
    public ProfileRef profile(){return PrepareLargeScheduleBody.decode(binding.canonicalBody()).objectStoreProfile();}
    public void requireOwner(TargetQuotaPayloadOwner owner){
        if(owner.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||owner.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED||owner.committedPayload()!=null
                ||!owner.mutation().equals(reservation.mutation()))throw new IllegalArgumentException("exact retained uncommitted owner required");
        reservation.requireOwner(owner);owner.requireInitialBinding(binding);
        if(prepareFirst!=null&&(!prepareFirst.primaryIdentity().equals(owner.primaryIdentity())||!prepareFirst.accounting().equals(owner.accounting())
                ||!Arrays.equals(prepareFirst.tenantScope(),owner.tenantScope())||!Arrays.equals(prepareFirst.recoveryLineage(),owner.recoveryLineage())))
            throw new IllegalArgumentException("uncommitted Prepare changed original frozen owner");
    }
    public boolean migrated(){return reservation.migrationOrigin()!=null;}
    public TargetUncommittedPayloadFacts withMigrationHistory(TargetMigrationPayloadHistory history){if(!migrated()||migrationHistory!=null)throw new IllegalArgumentException("wrong migration facts stage");return new TargetUncommittedPayloadFacts(binding,reservation,null,null,cause,fence,retry,terminal,Objects.requireNonNull(history));}
    public void requireFloor(RecoveryFloorRef floor){reservation.mutation().requireCoveredByFloor(floor);fence.appliedAt().requireCoveredByFloor(floor);}
    public byte[] canonicalBytes(){
        final byte[] bytes=CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,migrated()?2:SCHEMA_VERSION);
            CanonicalProtobuf.bytes(out,2,binding.canonicalBytes());CanonicalProtobuf.bytes(out,3,reservation.canonicalBytes());
            if(!migrated()){CanonicalProtobuf.bytes(out,4,prepareFirst.canonicalBytes());CanonicalProtobuf.bytes(out,5,preparePosition.canonicalBytes());}
            CanonicalProtobuf.bytes(out,6,cause.canonicalBytes());CanonicalProtobuf.bytes(out,7,fence.canonicalBytes());
            CanonicalProtobuf.bytes(out,8,retry.canonicalBytes());CanonicalProtobuf.bytes(out,9,terminal.canonicalBytes());if(migrationHistory!=null)CanonicalProtobuf.bytes(out,10,migrationHistory.canonicalBytes());});
        if(bytes.length>MAX_BYTES)throw new IllegalArgumentException("uncommitted original facts exceed finite intent bound");return bytes;
    }
    public static TargetUncommittedPayloadFacts decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,10,false,"uncommitted original facts");
        if(QueryCodecSupport.uint32(f.get(0),1)==2){QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,6,7,8,9,10},"migrated uncommitted facts");
            final var value=new TargetUncommittedPayloadFacts(TargetScheduleBinding.decode(QueryCodecSupport.bytes(f.get(1),2)),TargetReservationRecord.decode(QueryCodecSupport.bytes(f.get(2),3)),null,null,
                TargetInlinePayloadSourceFacts.Frame.decode(QueryCodecSupport.bytes(f.get(3),6)),TargetInlinePayloadSourceFacts.Frame.decode(QueryCodecSupport.bytes(f.get(4),7)),RetryPolicySemantic.decode(QueryCodecSupport.bytes(f.get(5),8)),
                TerminalPolicySemantic.decode(QueryCodecSupport.bytes(f.get(6),9)),TargetMigrationPayloadHistory.decode(QueryCodecSupport.bytes(f.get(7),10)));
            if(!value.migrated())throw new IllegalArgumentException("migrated facts require explicit Native38 origin");QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"migrated uncommitted facts");return value;
        }
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6,7,8,9},"uncommitted original facts");
        if(QueryCodecSupport.uint32(f.get(0),1)!=SCHEMA_VERSION)throw new IllegalArgumentException("unknown uncommitted original facts schema");
        final var value=new TargetUncommittedPayloadFacts(TargetScheduleBinding.decode(QueryCodecSupport.bytes(f.get(1),2)),
                TargetReservationRecord.decode(QueryCodecSupport.bytes(f.get(2),3)),TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(3),4)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(4),5)),TargetInlinePayloadSourceFacts.Frame.decode(QueryCodecSupport.bytes(f.get(5),6)),
                TargetInlinePayloadSourceFacts.Frame.decode(QueryCodecSupport.bytes(f.get(6),7)),RetryPolicySemantic.decode(QueryCodecSupport.bytes(f.get(7),8)),
                TerminalPolicySemantic.decode(QueryCodecSupport.bytes(f.get(8),9)));
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"uncommitted original facts");return value;
    }
}
