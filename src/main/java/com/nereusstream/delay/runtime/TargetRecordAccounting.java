package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetNativePolicyControlRecord;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Rebuilds one actual business record's frozen contribution; never accepts a persisted usage field as its oracle. */
public final class TargetRecordAccounting {
    /**
     * TargetValueEnvelope.decodeAny copies the payload range, ValueEnvelope.Decoded copies it
     * again on construction, and each payload() accessor makes one further copy. This describes
     * those actual byte-array copies only; it is not a JVM object or codec-scratch qualification.
     */
    static long envelopeDecodedPayloadCopies(long payloadBytes,long payloadAccessors){
        return Math.multiplyExact(payloadBytes,Math.addExact(2L,payloadAccessors));
    }

    /** TargetStoreBackend.Edit owns a copy of key/before/after and validates both present NVs. */
    static long envelopeEditCopies(long keyBytes,long beforeNvBytes,long afterNvBytes){
        long copied=Math.addExact(keyBytes,Math.addExact(beforeNvBytes,afterNvBytes));
        if(beforeNvBytes!=0)copied=Math.addExact(copied,envelopeDecodedPayloadCopies(Math.subtractExact(beforeNvBytes,12L),0));
        if(afterNvBytes!=0)copied=Math.addExact(copied,envelopeDecodedPayloadCopies(Math.subtractExact(afterNvBytes,12L),0));
        return copied;
    }

    /** Source-visible byte-array outputs only. retained is a reachable-material peak, never a JVM/RSS claim. */
    static final class EnvelopeMaterial {
        private final long allocated,retained,held,fieldVisits,arrayObjects;
        private EnvelopeMaterial(long allocated,long retained,long held,long fieldVisits,long arrayObjects){
            if(held<0||retained<held||allocated<retained||fieldVisits<0||arrayObjects<0)
                throw new IllegalArgumentException("invalid repository codec material");
            this.allocated=allocated;this.retained=retained;this.held=held;
            this.fieldVisits=fieldVisits;this.arrayObjects=arrayObjects;
        }
        long allocated(){return allocated;}
        long retained(){return retained;}
        long held(){return held;}
        long fieldVisits(){return fieldVisits;}
        long arrayObjects(){return arrayObjects;}
    }

    /* These fixed subgraphs repeatedly occur inside the Source family union. Each holder evaluates
       the unchanged numeric graph once; sharing its immutable five scalars does not share or remove
       any modeled codec call. Separate holders preserve their dependency order during initialization.
       No actual Source F, Reader, frame qualification or other runtime authority is retained here. */
    private static final class EvidenceResourceMaterial {
        private static final EnvelopeMaterial DECODE=calculateEnvelopeEvidenceResourceDecode();
    }
    private static final class EvidenceConfirmedBodyMaterial {
        private static final EnvelopeMaterial DECODE=calculateEnvelopeEvidenceConfirmedBodyDecode();
    }
    private static final class ResourceFrameMaterial {
        private static final EnvelopeMaterial DECODE=calculateEnvelopeResourceFrameDecode();
    }
    private static final class ResourceReclaimMaterial {
        private static final EnvelopeMaterial DECODE=calculateEnvelopeResourceReclaimDecode();
    }
    private static final class RetirementSourceHistoryMaterial {
        private static final EnvelopeMaterial DECODE=calculateEnvelopeRetirementSourceHistoryDecode();
    }

    private static EnvelopeMaterial envelopeArray(long bytes){return new EnvelopeMaterial(bytes,bytes,bytes,0,1);}
    /** A source block retaining several independently allocated arrays whose total length is bounded here. */
    private static EnvelopeMaterial envelopeArrays(long bytes,long count){return new EnvelopeMaterial(bytes,bytes,bytes,0,count);}
    private static EnvelopeMaterial envelopeEmpty(){return new EnvelopeMaterial(0,0,0,0,0);}
    /** Sequence whose preceding temporary arrays are no longer referenced by the next source expression. */
    private static EnvelopeMaterial envelopeDrop(EnvelopeMaterial before,EnvelopeMaterial after){
        return new EnvelopeMaterial(Math.addExact(before.allocated,after.allocated),
                Math.max(before.retained,after.retained),after.held,
                Math.addExact(before.fieldVisits,after.fieldVisits),Math.addExact(before.arrayObjects,after.arrayObjects));
    }
    /** Independently evaluated arguments/DTO fields that the source caller keeps together. */
    private static EnvelopeMaterial envelopeHold(EnvelopeMaterial before,EnvelopeMaterial after){
        return new EnvelopeMaterial(Math.addExact(before.allocated,after.allocated),
                Math.max(before.retained,Math.addExact(before.held,after.retained)),Math.addExact(before.held,after.held),
                Math.addExact(before.fieldVisits,after.fieldVisits),Math.addExact(before.arrayObjects,after.arrayObjects));
    }
    /** The argument arrays stay available through a call; only its explicitly retained argument fields escape. */
    private static EnvelopeMaterial envelopeCall(EnvelopeMaterial arguments,EnvelopeMaterial call,long retainedArguments){
        if(retainedArguments<0||retainedArguments>arguments.held)throw new IllegalArgumentException("invalid repository retained arguments");
        return new EnvelopeMaterial(Math.addExact(arguments.allocated,call.allocated),
                Math.max(arguments.retained,Math.addExact(arguments.held,call.retained)),
                Math.addExact(retainedArguments,call.held),Math.addExact(arguments.fieldVisits,call.fieldVisits),
                Math.addExact(arguments.arrayObjects,call.arrayObjects));
    }
    private static EnvelopeMaterial envelopeTemporary(EnvelopeMaterial frame,EnvelopeMaterial temporary){
        return new EnvelopeMaterial(Math.addExact(frame.allocated,temporary.allocated),
                Math.max(frame.retained,Math.addExact(frame.held,temporary.retained)),frame.held,
                Math.addExact(frame.fieldVisits,temporary.fieldVisits),Math.addExact(frame.arrayObjects,temporary.arrayObjects));
    }
    private static EnvelopeMaterial envelopeReturn(EnvelopeMaterial frame,long returned){
        return new EnvelopeMaterial(frame.allocated,frame.retained,returned,frame.fieldVisits,frame.arrayObjects);
    }
    private static EnvelopeMaterial envelopeRepeat(EnvelopeMaterial value,long count,boolean keepResults){
        if(count<0)throw new IllegalArgumentException("negative repository codec repetition");
        if(count==0)return envelopeEmpty();
        final long held=keepResults?Math.multiplyExact(value.held,count):value.held;
        final long peak=keepResults?Math.addExact(Math.multiplyExact(value.held,count-1),value.retained):value.retained;
        return new EnvelopeMaterial(Math.multiplyExact(value.allocated,count),peak,held,
                Math.multiplyExact(value.fieldVisits,count),Math.multiplyExact(value.arrayObjects,count));
    }
    private static EnvelopeMaterial envelopeChoice(EnvelopeMaterial left,EnvelopeMaterial right){
        return new EnvelopeMaterial(Math.max(left.allocated,right.allocated),Math.max(left.retained,right.retained),Math.max(left.held,right.held),
                Math.max(left.fieldVisits,right.fieldVisits),Math.max(left.arrayObjects,right.arrayObjects));
    }
    /** Reader copy(n), readValue copy(v), Field constructor copy(v); the returned list retains one v per field. */
    private static EnvelopeMaterial envelopeProtoRead(long encoded,long fieldBytes,long largestField){
        return envelopeProtoRead(encoded,fieldBytes,largestField,encoded/2);
    }
    private static EnvelopeMaterial envelopeProtoRead(long encoded,long fieldBytes,long largestField,long fieldCount){
        if(fieldBytes<0||fieldBytes>encoded||largestField<0||largestField>fieldBytes)
            throw new IllegalArgumentException("invalid repository protobuf field inventory");
        return new EnvelopeMaterial(Math.addExact(encoded,Math.multiplyExact(2L,fieldBytes)),
                Math.addExact(encoded,Math.addExact(fieldBytes,largestField)),fieldBytes,fieldCount,
                Math.addExact(1,Math.multiplyExact(2,fieldCount)));
    }
    /** assigned(k) creates a zero guard array, then a distinct owned copy after the comparison. */
    private static EnvelopeMaterial envelopeAssigned(long bytes){return new EnvelopeMaterial(Math.multiplyExact(2L,bytes),bytes,bytes,0,2);}
    private static EnvelopeMaterial envelopeAssigned(long bytes,long fields){
        return new EnvelopeMaterial(Math.multiplyExact(2L,bytes),bytes,bytes,0,Math.multiplyExact(2L,fields));
    }
    /** A writer consumes each temporary field image before toByteArray returns the canonical image. */
    private static EnvelopeMaterial envelopeWritten(EnvelopeMaterial fieldWork,long encoded){return envelopeDrop(fieldWork,envelopeArray(encoded));}
    /** Bytes.sha256(domain, fields) concatenates its arguments before the SHA-256 output is returned. */
    private static EnvelopeMaterial envelopeHash(EnvelopeMaterial fields,long domainBytes){
        final long concatenated=Math.addExact(domainBytes,fields.held);
        return envelopeCall(fields,new EnvelopeMaterial(Math.addExact(concatenated,32),Math.addExact(concatenated,32),32,0,2),0);
    }
    private static EnvelopeMaterial envelopeConcat(EnvelopeMaterial fields){return envelopeCall(fields,envelopeArray(fields.held),0);}

    /** Closed scalar accounting leaf: constructor fixed hash, fields hash and canonical recheck are separate edges. */
    private static EnvelopeMaterial envelopeAccountingCanonical(){
        final long canonical=TargetQuotaAccounting.MAX_CANONICAL_BYTES,fields=canonical-34;
        return envelopeWritten(envelopeArray(fields),canonical);
    }
    private static EnvelopeMaterial envelopeAccountingDecode(){
        final long canonical=TargetQuotaAccounting.MAX_CANONICAL_BYTES;
        // uint32 version/format (1 byte each), two 32-byte byte fields, four <=10-byte varints.
        final long raw=2+64+4*10;
        final var parsed=envelopeProtoRead(canonical,raw,32);
        final var constructor=envelopeHold(envelopeAssigned(32),
                envelopeHash(envelopeArray(canonical-34),"nereus-delay-target-quota-accounting\0".length()));
        var frame=envelopeHold(parsed,envelopeCall(envelopeArray(32),constructor,0));
        frame=envelopeTemporary(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeAccountingCanonical());
        return envelopeReturn(frame,64);
    }

    private static EnvelopeMaterial envelopeTupleCanonical(){return envelopeArray(4L*6+2);}
    private static EnvelopeMaterial envelopeTupleDecode(){
        // Four arbitrary positive uint32 values and the fixed 1/2 record-kind branch (ProtocolTuple).
        return envelopeReturn(envelopeTemporary(envelopeProtoRead(26,4L*5+1,5),envelopeTupleCanonical()),0);
    }
    private static EnvelopeMaterial envelopeVectorFields(){
        // CapacityVector.fieldsOneAndTwo: 66 CapacityAmount messages (dimension uint32 + nonnegative long).
        return envelopeWritten(envelopeRepeat(envelopeArray(12),com.nereusstream.delay.protocol.CapacityDimension.COUNT,false),
                com.nereusstream.delay.protocol.TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeVectorCanonical(){
        // canonicalBytes retains its local fields array in the writer lambda until message() returns.
        return envelopeCall(envelopeVectorFields(),envelopeArray(com.nereusstream.delay.protocol.TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES),0);
    }
    private static EnvelopeMaterial envelopeVectorConstruct(){
        return envelopeHash(envelopeVectorFields(),"nereus-delay-capacity-vector\0".length());
    }
    private static EnvelopeMaterial envelopeVectorDecode(){
        final long dimensions=com.nereusstream.delay.protocol.CapacityDimension.COUNT;
        final long encoded=com.nereusstream.delay.protocol.TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES;
        final var parsed=envelopeProtoRead(encoded,1+dimensions*12+32,32);
        // Each raw amount accessor is followed by its own two-field Reader, then only scalar longs escape.
        final var amount=envelopeReturn(envelopeCall(envelopeArray(12),envelopeProtoRead(12,10,9),0),0);
        var frame=envelopeHold(parsed,envelopeRepeat(amount,dimensions,false));
        frame=envelopeHold(frame,envelopeArray(32));
        frame=envelopeHold(frame,envelopeVectorConstruct());
        // The canonical private constructor copies the supplied digest after the independent public-ctor check.
        frame=envelopeHold(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeVectorCanonical());
        return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeUsageFields(){
        return envelopeWritten(envelopeVectorCanonical(),TargetQuotaUsage.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeUsageCanonical(){
        return envelopeWritten(envelopeUsageFields(),TargetQuotaUsage.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeUsageConstruct(){
        return envelopeHash(envelopeUsageFields(),"nereus-delay-target-quota-usage\0".length());
    }
    private static EnvelopeMaterial envelopeUsageDecode(){
        final long vector=TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES;
        final var parsed=envelopeProtoRead(TargetQuotaUsage.MAX_CANONICAL_BYTES,1+vector+4L*9+32,vector);
        final var arguments=envelopeCall(envelopeArray(vector),envelopeVectorDecode(),0);
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeUsageConstruct(),arguments.held));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeUsageCanonical());
        return envelopeReturn(frame,64);
    }
    private static long envelopeCapacityGrantBytes(){return 2+34+11+3L+TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES+34;}
    private static EnvelopeMaterial envelopeCapacityGrantFields(){
        return envelopeWritten(envelopeVectorCanonical(),envelopeCapacityGrantBytes()-34);
    }
    private static EnvelopeMaterial envelopeCapacityGrantCanonical(){
        return envelopeWritten(envelopeCapacityGrantFields(),envelopeCapacityGrantBytes());
    }
    private static EnvelopeMaterial envelopeCapacityGrantDecode(){
        final long vector=TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES;
        final var parsed=envelopeProtoRead(envelopeCapacityGrantBytes(),1+32+10+vector+32,vector);
        var arguments=envelopeHold(envelopeArray(32),envelopeCall(envelopeArray(vector),envelopeVectorDecode(),0));
        arguments=envelopeHold(arguments,envelopeArray(32));
        // CapacityGrant.decode uses the private constructor, retaining the decoded vector and copying two hashes.
        final var constructor=envelopeHold(envelopeArray(32),envelopeArray(32));
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,32));
        frame=envelopeTemporary(frame,envelopeHash(envelopeCapacityGrantFields(),"nereus-delay-capacity-grant\0".length()));
        frame=envelopeTemporary(frame,envelopeCapacityGrantCanonical());
        return envelopeReturn(frame,96);
    }
    private static EnvelopeMaterial envelopeSourceCanonical(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        final long encoded=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_PULSAR_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_KAFKA_CANONICAL_BYTES;
        // The UTF-8 text, ByteBuffer backing output and route.bytes() are present during put(route).
        return new EnvelopeMaterial(Math.addExact(text,Math.addExact(encoded,16)),
                Math.addExact(text,Math.addExact(encoded,16)),encoded,0,3);
    }
    private static EnvelopeMaterial envelopeSourceCanonical(){
        return envelopeChoice(envelopeSourceCanonical(false),envelopeSourceCanonical(true));
    }
    private static EnvelopeMaterial envelopeSourceBounded(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        // requireBounded UTF-8 validation; Pulsar additionally compares getter clone32 with a new zero32.
        final var work=pulsar?envelopeDrop(envelopeArray(text),envelopeHold(envelopeArray(32),envelopeArray(32))):envelopeArray(text);
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeSourceBounded(){
        return envelopeChoice(envelopeSourceBounded(false),envelopeSourceBounded(true));
    }
    private static EnvelopeMaterial envelopeSourceDecode(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        var frame=envelopeArray(16); // SourcePositionCodec.readFixed route
        if(pulsar)frame=envelopeHold(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeArray(text)); // readString input, consumed by String construction
        if(!pulsar)frame=envelopeTemporary(frame,envelopeArray(16)); // UUID input
        frame=envelopeHold(frame,envelopeArray(16)); // RouteIncarnation owns its route clone
        frame=envelopeTemporary(frame,envelopeArray(text)); // canonicalText UTF-8 round trip
        if(pulsar)frame=envelopeHold(frame,envelopeArray(32)); // Pulsar position owns its resource clone
        final long owned=pulsar?48:16;
        frame=envelopeReturn(frame,owned);
        frame=envelopeTemporary(frame,envelopeSourceCanonical(pulsar));
        frame=envelopeTemporary(frame,envelopeSourceBounded(pulsar));
        return frame;
    }
    private static EnvelopeMaterial envelopeSourceDecode(){
        return envelopeChoice(envelopeSourceDecode(false),envelopeSourceDecode(true));
    }
    private static EnvelopeMaterial envelopeMutationCanonical(boolean local){
        return envelopeWritten(envelopeSourceCanonical(),local?TargetQuotaMutation.MAX_CANONICAL_BYTES:TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMutationConstruct(){
        return envelopeDrop(envelopeSourceBounded(),envelopeAssigned(32));
    }
    private static EnvelopeMaterial envelopeMutationDecode(boolean local){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        final long encoded=local?TargetQuotaMutation.MAX_CANONICAL_BYTES:TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(encoded,source+42+(local?10:0),source);
        final var sourceArgument=envelopeCall(envelopeArray(source),envelopeSourceDecode(),0);
        final var arguments=envelopeHold(sourceArgument,envelopeArray(32));
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeMutationConstruct(),sourceArgument.held));
        frame=envelopeTemporary(frame,envelopeMutationCanonical(local));
        return envelopeReturn(frame,80);
    }
    private static EnvelopeMaterial envelopeShardCanonical(){return envelopeConcat(envelopeHold(envelopeArray(16),envelopeArray(4)));}
    private static EnvelopeMaterial envelopeShardDecode(){return envelopeCall(envelopeArray(16),envelopeArray(16),0);}
    private static EnvelopeMaterial envelopeIdentityFields(){
        return envelopeWritten(envelopeDrop(envelopeShardCanonical(),envelopeArray(32)),TargetQuotaIdentity.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeIdentityCanonical(){return envelopeWritten(envelopeIdentityFields(),TargetQuotaIdentity.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeIdentityConstruct(){
        final var assigned=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));
        return envelopeHold(assigned,envelopeHash(envelopeIdentityFields(),"nereus-delay-target-quota-identity\0".length()));
    }
    private static EnvelopeMaterial envelopeIdentityDecode(){
        // TENANT_TARGET dominates the four mutually exclusive identity shapes: Shard20/inc16/target32/tenant32.
        final var parsed=envelopeProtoRead(TargetQuotaIdentity.MAX_CANONICAL_BYTES,2+20+16+32+32+32,32);
        var arguments=envelopeCall(envelopeArray(20),envelopeShardDecode(),0);
        arguments=envelopeHold(arguments,envelopeArray(16));
        arguments=envelopeHold(arguments,envelopeCall(envelopeArray(32),envelopeArray(32),0));
        arguments=envelopeHold(arguments,envelopeArray(32));
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeIdentityConstruct(),48));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeIdentityCanonical());
        return envelopeReturn(frame,128);
    }
    private static EnvelopeMaterial envelopeScopeFields(){
        return envelopeWritten(envelopeDrop(envelopeShardCanonical(),envelopeArray(32)),TargetQuotaScope.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeScopeCanonical(){return envelopeWritten(envelopeScopeFields(),TargetQuotaScope.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeScopeConstruct(){
        return envelopeHold(envelopeAssigned(32),envelopeHash(envelopeScopeFields(),"nereus-delay-target-quota-scope\0".length()));
    }
    private static EnvelopeMaterial envelopeScopeDecode(){
        final var parsed=envelopeProtoRead(TargetQuotaScope.MAX_CANONICAL_BYTES,1+20+32+32+32,32);
        var arguments=envelopeCall(envelopeArray(20),envelopeShardDecode(),0);
        arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeCall(envelopeArray(32),envelopeArray(32),0));
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeScopeConstruct(),48));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeScopeCanonical());
        return envelopeReturn(frame,112);
    }
    private static EnvelopeMaterial envelopeControlGroupCanonical(){return envelopeArray(36);}
    private static EnvelopeMaterial envelopeControlGroupDecode(){
        // TargetControlScope.ControlGroup: kind varint plus a 32-byte assigned ID.
        var frame=envelopeHold(envelopeProtoRead(36,33,32),envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
        frame=envelopeTemporary(frame,envelopeControlGroupCanonical());
        return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeControlFields(){
        final long groups=TargetControlScope.MAX_CONTROL_GROUPS;
        var work=envelopeDrop(envelopeArray(32),envelopeConcat(envelopeHold(envelopeArray(16),envelopeArray(4))));
        work=envelopeDrop(work,envelopeRepeat(envelopeControlGroupCanonical(),groups,false));
        return envelopeWritten(work,TargetControlScope.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeControlCanonical(){
        return envelopeWritten(envelopeControlFields(),TargetControlScope.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeControlDecode(){
        final long groups=TargetControlScope.MAX_CONTROL_GROUPS,permits=TargetControlScope.MAX_PERMIT_GROUPS;
        // Version, target, Shard, complete ControlGroup messages, permit IDs and the stored digest.
        final long raw=Math.addExact(85,Math.addExact(Math.multiplyExact(36,groups),Math.multiplyExact(32,permits)));
        final var parsed=envelopeProtoRead(TargetControlScope.MAX_CANONICAL_BYTES,raw,36);
        // Query fixed target -> FixedBytes; query Shard -> route slice -> RouteIncarnation.FixedBytes.
        var arguments=envelopeHold(envelopeCall(envelopeArray(32),envelopeArray(32),0),envelopeArray(20));
        arguments=envelopeHold(arguments,envelopeCall(envelopeArray(16),envelopeArray(16),0));
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeCall(envelopeArray(36),envelopeControlGroupDecode(),0),groups,true));
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeArray(32),permits,true));
        // The constructor retains supplied target/route/ControlGroups, copies permit IDs, then hashes fields.
        final var constructor=envelopeHold(envelopeRepeat(envelopeAssigned(32),permits,true),
                envelopeHash(envelopeControlFields(),"nereus-delay-target-control-scope\0".length()));
        final long borrowed=32+16+Math.multiplyExact(32,groups);
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeControlCanonical());
        return envelopeReturn(frame,Math.addExact(80,Math.multiplyExact(32,Math.addExact(groups,permits))));
    }
    /** Exact selected Store view, implemented by both guarded plans and immutable recovery images. */
    private static EnvelopeMaterial envelopeNested(long encoded,EnvelopeMaterial decoded){
        return envelopeCall(envelopeArray(encoded),decoded,0);
    }
    private static EnvelopeMaterial envelopeComparison(EnvelopeMaterial left,EnvelopeMaterial right){
        return envelopeReturn(envelopeHold(left,right),0);
    }
    /** The three quota sums have the same fields/constructor graph, with distinct typed identities. */
    private static EnvelopeMaterial envelopeQuotaSumFields(boolean total){
        var work=total?envelopeScopeCanonical():envelopeIdentityCanonical();
        work=envelopeDrop(work,envelopeUsageCanonical());
        work=envelopeDrop(work,envelopeMutationCanonical(true));
        final long encoded=total?com.nereusstream.delay.protocol.TargetQuotaTotal.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetQuotaCounter.MAX_CANONICAL_BYTES;
        return envelopeWritten(work,encoded-34);
    }
    private static EnvelopeMaterial envelopeQuotaSumCanonical(boolean total){
        return envelopeWritten(envelopeQuotaSumFields(total),total?
                com.nereusstream.delay.protocol.TargetQuotaTotal.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetQuotaCounter.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeQuotaSumDecode(boolean total){
        final long identity=total?TargetQuotaScope.MAX_CANONICAL_BYTES:TargetQuotaIdentity.MAX_CANONICAL_BYTES;
        final long encoded=total?com.nereusstream.delay.protocol.TargetQuotaTotal.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetQuotaCounter.MAX_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(encoded,1+identity+TargetQuotaUsage.MAX_CANONICAL_BYTES+10+
                TargetQuotaMutation.MAX_CANONICAL_BYTES+32,TargetQuotaMutation.MAX_CANONICAL_BYTES);
        var arguments=envelopeNested(identity,total?envelopeScopeDecode():envelopeIdentityDecode());
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaUsage.MAX_CANONICAL_BYTES,envelopeUsageDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_CANONICAL_BYTES,envelopeMutationDecode(true)));
        final var constructor=envelopeHash(envelopeQuotaSumFields(total),total?
                "nereus-delay-target-quota-total\0".length():"nereus-delay-target-quota-counter\0".length());
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeQuotaSumCanonical(total));
        return envelopeReturn(frame,arguments.held+32);
    }
    private static EnvelopeMaterial envelopeAggregateFields(){
        var work=envelopeDrop(envelopeShardCanonical(),envelopeUsageCanonical());
        work=envelopeDrop(work,envelopeMutationCanonical(true));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetQuotaAggregate.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeAggregateCanonical(){
        return envelopeWritten(envelopeAggregateFields(),com.nereusstream.delay.protocol.TargetQuotaAggregate.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeAggregateDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetQuotaAggregate.MAX_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(encoded,1+20+16+TargetQuotaUsage.MAX_CANONICAL_BYTES+10+
                TargetQuotaMutation.MAX_CANONICAL_BYTES+32,TargetQuotaMutation.MAX_CANONICAL_BYTES);
        var arguments=envelopeNested(20,envelopeShardDecode());arguments=envelopeHold(arguments,envelopeArray(16));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaUsage.MAX_CANONICAL_BYTES,envelopeUsageDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_CANONICAL_BYTES,envelopeMutationDecode(true)));
        final var constructor=envelopeHold(envelopeAssigned(16),
                envelopeHash(envelopeAggregateFields(),"nereus-delay-target-quota-aggregate\0".length()));
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,16+64+80));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeAggregateCanonical());
        return envelopeReturn(frame,16+16+64+80+32);
    }
    private static EnvelopeMaterial envelopeGrantFields(){
        var work=envelopeDrop(envelopeScopeCanonical(),envelopeAccountingCanonical());
        work=envelopeDrop(work,envelopeUsageCanonical());
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeGrantCanonical(){
        return envelopeWritten(envelopeGrantFields(),com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeGrantDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(encoded,1+TargetQuotaScope.MAX_CANONICAL_BYTES+32+10+
                TargetQuotaAccounting.MAX_CANONICAL_BYTES+TargetQuotaUsage.MAX_CANONICAL_BYTES+10+32+32,
                TargetQuotaUsage.MAX_CANONICAL_BYTES);
        var arguments=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaUsage.MAX_CANONICAL_BYTES,envelopeUsageDecode()));
        arguments=envelopeHold(arguments,envelopeArray(32));
        final var constructor=envelopeHold(envelopeRepeat(envelopeAssigned(32),2,true),
                envelopeHash(envelopeGrantFields(),"nereus-delay-target-quota-grant\0".length()));
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,112+64+64));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeGrantCanonical());
        return envelopeReturn(frame,112+64+64+64+32);
    }
    private static EnvelopeMaterial envelopeTransferCanonical(){
        return envelopeArray(com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_TRANSFER_BYTES);
    }
    private static EnvelopeMaterial envelopeTransferDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_TRANSFER_BYTES;
        final var parsed=envelopeProtoRead(encoded,3L*32+10,32);
        var frame=envelopeHold(parsed,envelopeCall(envelopeRepeat(envelopeArray(32),3,true),
                envelopeRepeat(envelopeArray(32),3,true),0));
        frame=envelopeTemporary(frame,envelopeTransferCanonical());return envelopeReturn(frame,96);
    }
    private static EnvelopeMaterial envelopeGrantRequestCanonical(){
        var work=envelopeDrop(envelopeRepeat(envelopeGrantCanonical(),2,false),envelopeTransferCanonical());
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeGrantRequestDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES;
        final long transfer=com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_TRANSFER_BYTES;
        final var parsed=envelopeProtoRead(encoded,1+2L*com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES+transfer,
                com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES);
        // A transfer raw accessor is checked by a separate bounded Reader before its typed decode.
        var plan=envelopeArray(transfer);
        plan=envelopeTemporary(plan,envelopeProtoRead(transfer,3L*32+10,32));
        plan=envelopeCall(plan,envelopeTransferDecode(),0);
        var arguments=envelopeHold(plan,envelopeRepeat(envelopeNested(
                com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES,envelopeGrantDecode()),2,true));
        // The constructor checks requestHash()/planHash() by getter clone then assigned zero-check/copy.
        final var transferChecks=envelopeRepeat(envelopeCall(envelopeArray(32),envelopeAssigned(32),0),2,false);
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeReturn(transferChecks,0),arguments.held));
        frame=envelopeTemporary(frame,envelopeGrantRequestCanonical());return envelopeReturn(frame,arguments.held);
    }
    private static EnvelopeMaterial envelopeControlRefCanonical(){return envelopeArray(74);}
    private static EnvelopeMaterial envelopeControlRefDecode(){
        final var parsed=envelopeProtoRead(74,64+5,32);
        var frame=envelopeHold(parsed,envelopeCall(envelopeRepeat(envelopeArray(32),2,true),
                envelopeRepeat(envelopeArray(32),2,true),0));
        frame=envelopeTemporary(frame,envelopeControlRefCanonical());return envelopeReturn(frame,64);
    }
    private static EnvelopeMaterial envelopeControlLogicalIdentity(){
        final long domain="nereus-delay-control-target-logical-id\0".length();
        // ControlRef builds its domain on each call, followed by u32 index/u16 kind and varargs concat.
        return envelopeHash(envelopeHold(envelopeHold(envelopeArray(domain),envelopeArray(4)),envelopeArray(2)),32);
    }
    private static EnvelopeMaterial envelopeSourceMaximum(){
        return envelopeReturn(envelopeDrop(envelopeSourceBounded(),envelopeSourceCanonical()),0);
    }
    private static EnvelopeMaterial envelopeIdentityKey(){
        // Identity.key(): tag/kind/format, physical Shard, target getter, then one concatenation.
        var arguments=envelopeHold(envelopeArray(3),envelopeShardCanonical());
        arguments=envelopeHold(arguments,envelopeArray(32));
        return envelopeCall(arguments,envelopeArray(103),0);
    }
    private static EnvelopeMaterial envelopeIncarnationId(){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeAccountingCanonical());
        fields=envelopeDrop(fields,envelopeAssigned(16));
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        final long encoded=2+3L+TargetQuotaScope.MAX_CANONICAL_BYTES+2+TargetQuotaAccounting.MAX_CANONICAL_BYTES+
                18+4+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        final var hash=envelopeHash(envelopeWritten(fields,encoded),"nereus-delay-target-quota-incarnation-id\0".length());
        return envelopeCall(envelopeCall(hash,envelopeArray(16),0),envelopeAssigned(16),0);
    }
    private static EnvelopeMaterial envelopeIncarnationFields(boolean draining){
        var work=envelopeDrop(envelopeIdentityCanonical(),envelopeAccountingCanonical());
        work=envelopeDrop(work,envelopeRepeat(envelopeMutationCanonical(false),draining?2:1,false));
        return envelopeWritten(work,(draining?TargetQuotaIncarnation.MAX_CANONICAL_BYTES:
                TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES)-34L);
    }
    private static EnvelopeMaterial envelopeIncarnationCanonical(boolean draining){
        return envelopeWritten(envelopeIncarnationFields(draining),draining?TargetQuotaIncarnation.MAX_CANONICAL_BYTES:
                TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeIncarnationConstruct(boolean draining){
        var frame=envelopeHold(envelopeAssigned(32),envelopeAssigned(16));
        // accountingIncarnation() is evaluated before allocationId(scope(),...) and remains an argument.
        var id=envelopeCall(envelopeScopeConstruct(),envelopeIncarnationId(),0);
        frame=envelopeTemporary(frame,envelopeHold(envelopeArray(16),id));
        frame=envelopeHold(frame,envelopeHash(envelopeIncarnationFields(draining),
                "nereus-delay-target-quota-incarnation\0".length()));
        frame=envelopeTemporary(frame,envelopeSourceMaximum());
        frame=envelopeTemporary(frame,envelopeIdentityKey());
        // The immutable own contribution owns recordCharge's vector and TargetQuotaUsage's digest.
        final var contribution=envelopeHold(envelopeVectorConstruct(),envelopeUsageConstruct());
        return envelopeHold(frame,contribution);
    }
    private static EnvelopeMaterial envelopeIncarnationDecode(boolean draining){
        final long encoded=draining?TargetQuotaIncarnation.MAX_CANONICAL_BYTES:TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES;
        final long mutations=draining?2:1;
        final var parsed=envelopeProtoRead(encoded,1+TargetQuotaIdentity.MAX_CANONICAL_BYTES+32+
                TargetQuotaAccounting.MAX_CANONICAL_BYTES+mutations*TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+16+32,
                TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES);
        var arguments=envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode());
        arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        arguments=envelopeHold(arguments,envelopeArray(16));
        if(draining)arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        final long retained=128+64+mutations*80;
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeIncarnationConstruct(draining),retained));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeIncarnationCanonical(draining));
        return envelopeReturn(frame,retained+32+16+32+64);
    }
    private static EnvelopeMaterial envelopePreparedRequestHash(EnvelopeMaterial branch,long branchBytes,int kind){
        final long requestBytes=branchBytes+envelopeVarintBytes((long)kind<<3|2)+envelopeVarintBytes(branchBytes);
        // ControlOperationRequest's oneof, Bytes.lp32 (length prefix+concat), then requestHash varargs.
        final var request=envelopeWritten(branch,requestBytes);
        final var lengthPrefixed=envelopeCall(request,envelopeCall(envelopeArray(4),envelopeArray(requestBytes+4),0),0);
        return envelopeHash(envelopeHold(envelopeArray(2),lengthPrefixed),
                "nereus-delay-control-request\0".length());
    }
    private static int envelopeVarintBytes(long value){
        int bytes=1;while((value&~0x7fL)!=0){bytes++;value>>>=7;}return bytes;
    }
    private static EnvelopeMaterial envelopeSystemMutationId(){
        final long domain="nereus-delay-system-mutation-id".length();
        var arguments=envelopeHold(envelopeArray(domain),envelopeArray(2));
        final var logical=envelopeCall(envelopeArray(32),envelopeCall(envelopeArray(4),envelopeArray(36),0),0);
        arguments=envelopeHold(arguments,logical);
        arguments=envelopeHold(arguments,envelopeShardCanonical());
        arguments=envelopeHold(arguments,envelopeArray(32));
        return envelopeHash(arguments,0);
    }
    private static EnvelopeMaterial envelopeGrantRequestControlRef(){
        var work=envelopeCall(envelopeArray(32),envelopeAssigned(32),0);
        work=envelopeDrop(work,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(
                envelopeGrantRequestCanonical(),com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES,17)));
        return envelopeDrop(work,envelopeComparison(envelopeArray(32),envelopeArray(32)));
    }
    private static EnvelopeMaterial envelopeGrantActivationFields(){
        var work=envelopeDrop(envelopeGrantRequestCanonical(),envelopeControlRefCanonical());
        work=envelopeDrop(work,envelopeMutationCanonical(false));
        work=envelopeDrop(work,envelopeIncarnationCanonical(false));
        return envelopeWritten(work,TargetQuotaGrantActivation.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeGrantActivationCanonical(){
        return envelopeWritten(envelopeGrantActivationFields(),TargetQuotaGrantActivation.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeGrantActivationConstruct(){
        var frame=envelopeReturn(envelopeGrantRequestControlRef(),0);
        frame=envelopeHold(frame,envelopeRepeat(envelopeAssigned(32),2,true));
        frame=envelopeTemporary(frame,envelopeCall(envelopeControlLogicalIdentity(),envelopeSystemMutationId(),0));
        frame=envelopeTemporary(frame,envelopeScopeConstruct());
        frame=envelopeTemporary(frame,envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        return envelopeHold(frame,envelopeHash(envelopeGrantActivationFields(),
                "nereus-delay-target-quota-grant-activation\0".length()));
    }
    private static EnvelopeMaterial envelopeGrantActivationDecode(){
        final long request=com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(TargetQuotaGrantActivation.MAX_CANONICAL_BYTES,
                1+request+74+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+64+
                        TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES+32,
                TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES);
        var ref=envelopeArray(74);ref=envelopeTemporary(ref,envelopeProtoRead(74,69,32));
        var arguments=envelopeNested(request,envelopeGrantRequestDecode());
        arguments=envelopeHold(arguments,envelopeCall(ref,envelopeControlRefDecode(),0));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeArray(32),2,true));
        final var allocation=envelopeNested(TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES,envelopeIncarnationDecode(false));
        arguments=envelopeHold(arguments,allocation);
        final long retained=arguments.held-64;
        var frame=envelopeHold(parsed,envelopeCall(arguments,envelopeGrantActivationConstruct(),retained));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeGrantActivationCanonical());
        return envelopeReturn(frame,retained+64+32);
    }

    private static EnvelopeMaterial envelopeAuthorCanonical(long encodedBytes){
        // Every branch emits one body message and one tagged outer message; both locals remain reachable.
        return envelopeCall(envelopeArray(encodedBytes),envelopeArray(encodedBytes),0);
    }
    private static EnvelopeMaterial envelopeAuthorDecode(long encodedBytes){
        final var outer=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,1);
        final var inner=envelopeNested(encodedBytes,envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,3));
        var frame=envelopeHold(outer,inner);
        // OWNER/SERVICE/FENCE copy their variable IDs once in accessors, then once into the identity.
        // CONTROL's fixed three hashes additionally pass decode fixed() and factory fixed() (96 each).
        var variable=envelopeHold(envelopeArrays(encodedBytes,3),envelopeArrays(encodedBytes,3));
        variable=envelopeHold(variable,envelopeArray(32)); // OWNER decode's explicit fixed(digest) copy
        final var control=envelopeRepeat(envelopeArrays(96,3),4,true);
        frame=envelopeHold(frame,envelopeChoice(variable,control));
        frame=envelopeTemporary(frame,envelopeAuthorCanonical(encodedBytes));
        return envelopeReturn(frame,Math.max(encodedBytes,96));
    }
    private static EnvelopeMaterial envelopeShardSubjectCanonical(){
        return envelopeWritten(envelopeArray(16),24);
    }
    private static EnvelopeMaterial envelopeShardSubjectDecode(){
        var frame=envelopeHold(envelopeProtoRead(24,16+5,16),envelopeCall(envelopeArray(16),envelopeArray(16),0));
        frame=envelopeTemporary(frame,envelopeShardSubjectCanonical());return envelopeReturn(frame,16);
    }
    private static EnvelopeMaterial envelopeSystemCanonical(long inputBytes){
        if(inputBytes<4)throw new IllegalArgumentException("invalid closed System envelope bound");
        // canonicalEnvelope retains its ShardSubject and inner SystemMutation message through the outer writer.
        return envelopeCall(envelopeHold(envelopeShardSubjectCanonical(),envelopeArray(inputBytes-4)),envelopeArray(inputBytes),0);
    }
    private static EnvelopeMaterial envelopeSystemRead(long inputBytes,long bodyBytes,long authorBytes){
        var frame=envelopeProtoRead(inputBytes,inputBytes,inputBytes,2);
        frame=envelopeHold(frame,envelopeNested(inputBytes,envelopeProtoRead(inputBytes,inputBytes,inputBytes,11)));
        var fields=envelopeArray(32);
        fields=envelopeHold(fields,envelopeNested(24,envelopeShardSubjectDecode()));
        fields=envelopeHold(fields,envelopeArray(bodyBytes));fields=envelopeHold(fields,envelopeArray(32));
        fields=envelopeHold(fields,envelopeArray(authorBytes));fields=envelopeHold(fields,envelopeArray(64));
        frame=envelopeHold(frame,fields);return envelopeReturn(frame,fields.held);
    }
    private static EnvelopeMaterial envelopeSystemControlValidation(long bodyBytes,long payloadBytes){
        var frame=envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes);
        frame=envelopeTemporary(frame,envelopeNested(74,envelopeProtoRead(74,69,32)));
        frame=envelopeTemporary(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeNested(payloadBytes,envelopeProtoRead(payloadBytes,payloadBytes,payloadBytes)));
        return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeSystemPrefix(long bodyBytes){
        return envelopeReturn(envelopeHold(envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,32),
                envelopeNested(24,envelopeShardSubjectDecode())),0);
    }
    private static EnvelopeMaterial envelopeSystemMutationHash(long bodyBytes){
        var arguments=envelopeArray("nereus-delay-system-mutation-hash\0".length());
        arguments=envelopeHold(arguments,envelopeArray(1));arguments=envelopeHold(arguments,envelopeRepeat(envelopeArray(4),3,true));
        arguments=envelopeHold(arguments,envelopeArray(2));arguments=envelopeHold(arguments,envelopeShardCanonical());
        arguments=envelopeHold(arguments,envelopeArray(8));
        arguments=envelopeHold(arguments,envelopeCall(envelopeArray(4),envelopeArray(bodyBytes+4),0));
        return envelopeHash(arguments,0);
    }
    private static EnvelopeMaterial envelopeSystemConstruct(long bodyBytes,long authorBytes,EnvelopeMaterial bodyValidation){
        var frame=envelopeHold(envelopeArray(32),envelopeArray(bodyBytes));
        frame=envelopeTemporary(frame,envelopeSystemPrefix(bodyBytes));frame=envelopeTemporary(frame,bodyValidation);
        frame=envelopeHold(frame,envelopeCall(envelopeAuthorDecode(authorBytes),envelopeAuthorCanonical(authorBytes),0));
        frame=envelopeHold(frame,envelopeArrays(32+32+64,3));
        frame=envelopeTemporary(frame,envelopeSystemMutationHash(bodyBytes));
        frame=envelopeTemporary(frame,envelopeSystemMutationId());return frame;
    }
    /** Both actual readEnvelope invocations, the typed identity derivation and the independent constructor checks. */
    private static EnvelopeMaterial envelopeSystemDecode(long inputBytes,long bodyBytes,long authorBytes,
            EnvelopeMaterial bodyValidation,EnvelopeMaterial typedIdentity,boolean control){
        var frame=envelopeSystemRead(inputBytes,bodyBytes,authorBytes);
        var identity=envelopeReturn(bodyValidation,0);
        identity=envelopeHold(identity,envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,32));
        if(!control)identity=envelopeTemporary(identity,envelopeNested(24,envelopeShardSubjectDecode()));
        identity=envelopeCall(identity,typedIdentity,0);
        frame=envelopeHold(frame,identity);
        final var second=envelopeSystemRead(inputBytes,bodyBytes,authorBytes);
        final var constructor=envelopeSystemConstruct(bodyBytes,authorBytes,bodyValidation);
        frame=envelopeHold(frame,envelopeCall(second,constructor,16));
        frame=envelopeTemporary(frame,envelopeSystemCanonical(inputBytes));
        return envelopeReturn(frame,16+32+bodyBytes+authorBytes+32+32+64);
    }

    private static EnvelopeMaterial envelopeParentPhysicalSource(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        var fields=pulsar?envelopeDrop(envelopeArray(32),envelopeArray(text)):
                envelopeDrop(envelopeArray(text),envelopeConcat(envelopeRepeat(envelopeArray(8),2,true)));
        fields=envelopeWritten(fields,pulsar?2+6+34+4+text:2+6+3+text+18);
        return envelopeDrop(envelopeSourceBounded(pulsar),envelopeHash(fields,
                "nereus-delay-target-parent-control-credit-physical-source\0".length()));
    }
    private static EnvelopeMaterial envelopeParentDefinitionFields(){
        var fields=envelopeDrop(envelopeIncarnationCanonical(false),envelopeGrantCanonical());
        fields=envelopeDrop(fields,envelopeCapacityGrantCanonical());fields=envelopeDrop(fields,envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeVectorCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetParentControlCredit.Definition.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeParentDefinitionCanonical(){
        return envelopeWritten(envelopeParentDefinitionFields(),com.nereusstream.delay.protocol.TargetParentControlCredit.Definition.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeParentDefinitionConstruct(){
        var frame=envelopeAssigned(32);frame=envelopeTemporary(frame,envelopeSourceBounded());
        frame=envelopeTemporary(frame,envelopeUsageConstruct());
        frame=envelopeTemporary(frame,envelopeScopeConstruct());
        frame=envelopeTemporary(frame,envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()));
        frame=envelopeTemporary(frame,envelopeScopeConstruct());
        frame=envelopeTemporary(frame,envelopeIncarnationCanonical(false));frame=envelopeTemporary(frame,envelopeCapacityGrantCanonical());
        var idFields=envelopeDrop(envelopeArray(32),envelopeChoice(envelopeParentPhysicalSource(false),envelopeParentPhysicalSource(true)));
        idFields=envelopeWritten(idFields,2+34+11+34+34);
        frame=envelopeHold(frame,envelopeHash(idFields,"nereus-delay-target-parent-control-credit-id\0".length()));
        frame=envelopeHold(frame,envelopeHash(envelopeParentDefinitionFields(),
                "nereus-delay-target-parent-control-credit-definition\0".length()));
        frame=envelopeTemporary(frame,envelopeParentDefinitionCanonical());return frame;
    }
    private static EnvelopeMaterial envelopeParentDefinitionDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetParentControlCredit.Definition.MAX_CANONICAL_BYTES;
        final long funding=com.nereusstream.delay.protocol.TargetParentControlCredit.Definition.MAX_FUNDING_GRANT_BYTES;
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(encoded,encoded,encoded);
        // The checked root/funding/vector raw arrays are locals, retained through the complete constructor call.
        var raw=envelopeHold(envelopeArray(TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES),envelopeArray(funding));
        raw=envelopeHold(raw,envelopeArray(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES));
        var arguments=envelopeHold(envelopeIncarnationDecode(false),envelopeNested(
                com.nereusstream.delay.protocol.TargetQuotaGrant.MAX_CANONICAL_BYTES,envelopeGrantDecode()));
        arguments=envelopeHold(arguments,envelopeCapacityGrantDecode());arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeNested(source,envelopeSourceDecode()));arguments=envelopeHold(arguments,envelopeVectorDecode());
        final long retained=arguments.held-32;
        var frame=envelopeHold(parsed,envelopeHold(raw,envelopeCall(arguments,envelopeParentDefinitionConstruct(),retained)));
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),2,false));
        frame=envelopeTemporary(frame,envelopeParentDefinitionCanonical());return envelopeReturn(frame,retained+96);
    }
    private static EnvelopeMaterial envelopeParentRequestCanonical(){
        return envelopeWritten(envelopeParentDefinitionCanonical(),com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeParentRequestDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES;
        var frame=envelopeHold(envelopeProtoRead(encoded,encoded,encoded),envelopeNested(
                com.nereusstream.delay.protocol.TargetParentControlCredit.Definition.MAX_CANONICAL_BYTES,envelopeParentDefinitionDecode()));
        frame=envelopeTemporary(frame,envelopeScopeConstruct());frame=envelopeTemporary(frame,envelopeParentRequestCanonical());
        return envelopeReturn(frame,envelopeParentDefinitionDecode().held);
    }
    private static EnvelopeMaterial envelopeParentRequestControlRef(){
        var work=envelopeCall(envelopeArray(32),envelopeAssigned(32),0);
        return envelopeDrop(work,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(envelopeParentRequestCanonical(),
                com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES,32)));
    }
    private static EnvelopeMaterial envelopeParentBodySemantic(){
        return envelopeHash(envelopeHold(envelopeArray(2),envelopeParentRequestCanonical()),
                "nereus-delay-target-parent-control-credit\0".length());
    }
    private static EnvelopeMaterial envelopeParentBodyCanonical(){
        var work=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        work=envelopeDrop(work,envelopeParentBodySemantic());
        final var payload=envelopeWritten(envelopeParentRequestCanonical(),
                com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES+8L);
        return envelopeWritten(envelopeDrop(work,payload),com.nereusstream.delay.protocol.TargetParentControlCreditBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeParentBodyDecode(){
        final long body=com.nereusstream.delay.protocol.TargetParentControlCreditBody.MAX_CANONICAL_BYTES;
        final long payload=com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES+8L;
        var frame=envelopeProtoRead(body,body,body);
        var rawShard=envelopeArray(24);rawShard=envelopeTemporary(rawShard,envelopeProtoRead(24,21,16));
        var rawRef=envelopeArray(74);rawRef=envelopeTemporary(rawRef,envelopeProtoRead(74,69,32));
        var rawPayload=envelopeNested(payload,envelopeProtoRead(payload,payload,payload));
        frame=envelopeHold(frame,envelopeHold(envelopeHold(rawShard,rawRef),rawPayload));
        var arguments=envelopeHold(envelopeShardSubjectDecode(),envelopeControlRefDecode());
        arguments=envelopeHold(arguments,envelopeNested(com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES,
                envelopeParentRequestDecode()));
        final var constructor=envelopeDrop(envelopeParentRequestControlRef(),envelopeScopeConstruct());
        frame=envelopeHold(frame,envelopeCall(arguments,envelopeReturn(constructor,0),arguments.held));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeParentBodySemantic(),envelopeArray(32)));
        frame=envelopeTemporary(frame,envelopeParentBodyCanonical());return envelopeReturn(frame,arguments.held);
    }
    private static EnvelopeMaterial envelopeParentControlCanonical(){
        var work=envelopeDrop(envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_INPUT_BYTES),
                envelopeMutationCanonical(false));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeParentBodyGetter(){
        return envelopeNested(com.nereusstream.delay.protocol.TargetParentControlCreditBody.MAX_CANONICAL_BYTES,envelopeParentBodyDecode());
    }
    private static EnvelopeMaterial envelopeParentControlConstruct(){
        var frame=envelopeReturn(envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_INPUT_BYTES),0);
        frame=envelopeHold(frame,envelopeParentBodyGetter());
        // Definition.requireSource obtains a fresh scope and checks the bounded physical Source.
        frame=envelopeTemporary(frame,envelopeSourceBounded());frame=envelopeTemporary(frame,envelopeScopeConstruct());
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeControlLogicalIdentity()));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeCall(
                envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_INPUT_BYTES),envelopeArray(32),0)));
        frame=envelopeTemporary(frame,envelopeNested(104,envelopeAuthorDecode(104)));
        return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeParentControlDecode(){
        final long input=com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_INPUT_BYTES;
        final long body=com.nereusstream.delay.protocol.TargetParentControlCreditBody.MAX_CANONICAL_BYTES;
        final long payload=com.nereusstream.delay.protocol.TargetParentControlCreditRequest.MAX_CANONICAL_BYTES+8L;
        final var identity=envelopeCall(envelopeParentBodyDecode(),envelopeControlLogicalIdentity(),0);
        final var system=envelopeSystemDecode(input,body,104,envelopeSystemControlValidation(body,payload),identity,true);
        var arguments=envelopeNested(input,system);
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var frame=envelopeHold(envelopeProtoRead(com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_CANONICAL_BYTES,
                1+input+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,input),
                envelopeCall(arguments,envelopeParentControlConstruct(),arguments.held));
        frame=envelopeTemporary(frame,envelopeParentControlCanonical());return envelopeReturn(frame,arguments.held);
    }

    private static EnvelopeMaterial envelopeScopeReferenceCanonical(){
        return envelopeArray(com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeScopeReferenceDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES;
        var frame=envelopeHold(envelopeProtoRead(encoded,1+64+9,32),
                envelopeCall(envelopeRepeat(envelopeArray(32),2,true),envelopeRepeat(envelopeAssigned(32),2,true),0));
        frame=envelopeTemporary(frame,envelopeScopeReferenceCanonical());return envelopeReturn(frame,64);
    }
    private static EnvelopeMaterial envelopeParentAllocationCanonical(){
        var work=envelopeDrop(envelopeScopeReferenceCanonical(),envelopeVectorCanonical());
        work=envelopeDrop(work,envelopeMutationCanonical(false));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetParentControlCredit.Allocation.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeParentAllocationDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetParentControlCredit.Allocation.MAX_CANONICAL_BYTES;
        var parsed=envelopeProtoRead(encoded,encoded,encoded);
        final var vector=envelopeArray(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES);
        var arguments=envelopeNested(com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES,
                envelopeScopeReferenceDecode());
        arguments=envelopeHold(arguments,envelopeArray(32));arguments=envelopeHold(arguments,envelopeVectorDecode());
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        final var constructor=envelopeTemporary(envelopeAssigned(32),envelopeUsageConstruct());
        var frame=envelopeHold(parsed,envelopeHold(vector,envelopeCall(arguments,constructor,64+32+80)));
        frame=envelopeTemporary(frame,envelopeParentAllocationCanonical());return envelopeReturn(frame,64+32+80+32);
    }
    private static long envelopeParentBytes(int children){
        if(children<0||children>com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS)
            throw new IllegalArgumentException("invalid repository Parent child bound");
        return Math.subtractExact(com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_CANONICAL_BYTES,
                Math.multiplyExact((long)com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS-children,
                        6L+com.nereusstream.delay.protocol.TargetParentControlCredit.Allocation.MAX_CANONICAL_BYTES));
    }
    private static EnvelopeMaterial envelopeParentFields(int children){
        var work=envelopeDrop(envelopeParentControlCanonical(),envelopeGrantActivationCanonical());
        work=envelopeDrop(work,envelopeRepeat(envelopeParentAllocationCanonical(),children,false));
        return envelopeWritten(work,envelopeParentBytes(children)-34);
    }
    private static EnvelopeMaterial envelopeParentCanonical(int children){
        return envelopeWritten(envelopeParentFields(children),envelopeParentBytes(children));
    }
    private static EnvelopeMaterial envelopeParentKey(){
        // reference() owns assigned ID/definition hashes before key() concatenates tag and private ID.
        return envelopeCall(envelopeRepeat(envelopeAssigned(32),2,true),
                envelopeCall(envelopeArray(2),envelopeArray(34),0),0);
    }
    private static EnvelopeMaterial envelopeParentSourceCheck(){
        return envelopeReturn(envelopeDrop(envelopeSourceBounded(),envelopeScopeConstruct()),0);
    }
    private static EnvelopeMaterial envelopeMutationAtOrBefore(){
        var work=envelopeSourceBounded();
        work=envelopeDrop(work,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        // Same-sequence/ordinal also invokes TargetQuotaMutation.equals after the frontier comparison.
        return envelopeDrop(work,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
    }
    private static EnvelopeMaterial envelopeParentConstruct(int children){
        var frame=envelopeCall(envelopeParentBodyGetter(),envelopeEmpty(),envelopeParentDefinitionDecode().held);
        frame=envelopeTemporary(frame,envelopeComparison(envelopeGrantCanonical(),envelopeGrantCanonical()));
        frame=envelopeTemporary(frame,envelopeParentSourceCheck());frame=envelopeTemporary(frame,envelopeMutationAtOrBefore());
        // One empty vector, then a fresh vector for every immutable child prefix addition.
        var committed=envelopeVectorConstruct();
        for(int i=0;i<children;i++){
            var step=envelopeParentSourceCheck();
            step=envelopeDrop(step,envelopeRepeat(envelopeArray(32),2,false)); // scope ID/grant digest getter clones
            step=envelopeDrop(step,envelopeVectorConstruct());
            committed=envelopeCall(committed,step,0);
        }
        frame=envelopeHold(frame,committed);frame=envelopeHold(frame,envelopeVectorConstruct()); // remaining
        frame=envelopeHold(frame,envelopeHash(envelopeParentFields(children),"nereus-delay-target-parent-control-credit\0".length()));
        frame=envelopeTemporary(frame,envelopeParentCanonical(children));
        frame=envelopeTemporary(frame,envelopeParentKey());frame=envelopeHold(frame,envelopeVectorConstruct()); // actual charge
        frame=envelopeTemporary(frame,envelopeParentControlCanonical());frame=envelopeTemporary(frame,envelopeGrantActivationCanonical());
        frame=envelopeTemporary(frame,envelopeSourceMaximum());
        frame=envelopeTemporary(frame,envelopeParentKey());
        // maximum charge and its subtract are distinct CapacityVector constructors; only the latter escapes.
        frame=envelopeHold(frame,envelopeCall(envelopeVectorConstruct(),envelopeVectorConstruct(),0));
        return frame;
    }
    private static EnvelopeMaterial envelopeParentDecode(int children){
        final long encoded=envelopeParentBytes(children);
        var frame=envelopeProtoRead(encoded,encoded,encoded);
        var arguments=envelopeNested(com.nereusstream.delay.protocol.TargetParentControlCredit.Control.MAX_CANONICAL_BYTES,
                envelopeParentControlDecode());
        for(int i=0;i<children;i++){
            arguments=envelopeTemporary(arguments,envelopeParentBodyGetter());
            arguments=envelopeHold(arguments,envelopeNested(com.nereusstream.delay.protocol.TargetParentControlCredit.Allocation.MAX_CANONICAL_BYTES,
                    envelopeParentAllocationDecode()));
        }
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaGrantActivation.MAX_CANONICAL_BYTES,envelopeGrantActivationDecode()));
        final var constructor=envelopeParentConstruct(children);
        frame=envelopeHold(frame,envelopeCall(arguments,constructor,arguments.held));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeParentCanonical(children));
        return envelopeReturn(frame,arguments.held+constructor.held);
    }
    static EnvelopeMaterial envelopeParentOriginMaterial(){
        return envelopeParentOriginMaterial(com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS);
    }
    static EnvelopeMaterial envelopeParentOriginMaterial(int maximumChildren){
        envelopeParentBytes(maximumChildren); // Reject any value outside the immutable codec allocation bound.
        // fund() constructs Control, reparses its definition for equality, and constructs the empty Parent.
        var prefix=envelopeReturn(envelopeParentControlConstruct(),0);
        prefix=envelopeTemporary(prefix,envelopeParentBodyGetter());
        prefix=envelopeTemporary(prefix,envelopeComparison(envelopeParentDefinitionCanonical(),envelopeParentDefinitionCanonical()));
        prefix=envelopeHold(prefix,envelopeParentConstruct(0));
        for(int i=1;i<=maximumChildren;i++){
            var work=envelopeEmpty();
            // allocate() scans the existing prefix's IDs before the next constructor scans all i children.
            work=envelopeDrop(work,envelopeRepeat(envelopeComparison(envelopeArray(32),envelopeArray(32)),i-1,false));
            work=envelopeHold(work,envelopeParentConstruct(i));
            prefix=envelopeCall(prefix,work,0);
        }
        return envelopeReturn(prefix,0);
    }

    private static EnvelopeMaterial envelopeBookkeepingFields(){
        var work=envelopeDrop(envelopeIdentityCanonical(),envelopeAccountingCanonical());
        work=envelopeDrop(work,envelopeMutationCanonical(false));
        return envelopeWritten(work,TargetQuotaBookkeeping.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeBookkeepingCanonical(){
        return envelopeWritten(envelopeBookkeepingFields(),TargetQuotaBookkeeping.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeBookkeepingDecode(){
        final long n=TargetQuotaBookkeeping.MAX_CANONICAL_BYTES;
        var arguments=envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode());
        arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var constructor=envelopeAssigned(32);constructor=envelopeTemporary(constructor,envelopeSourceMaximum());
        constructor=envelopeHold(constructor,envelopeVectorConstruct());
        constructor=envelopeHold(constructor,envelopeHash(envelopeBookkeepingFields(),"nereus-delay-target-quota-bookkeeping\0".length()));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,10),envelopeCall(arguments,constructor,arguments.held-32));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeBookkeepingCanonical());
        return envelopeReturn(frame,arguments.held-32+constructor.held);
    }
    private static EnvelopeMaterial envelopePayloadReferenceBounded(){
        // requireBounded reads lengths without copying; the committed profile check clones once and creates zero32.
        return envelopeReturn(envelopeHold(envelopeArray(32),envelopeArray(32)),0);
    }
    private static EnvelopeMaterial envelopePayloadFields(){
        var work=envelopeDrop(envelopeArray(DelayMessageId.LENGTH),envelopeIdentityCanonical());
        work=envelopeDrop(work,envelopeAccountingCanonical());work=envelopeDrop(work,envelopePayloadRefCanonical());
        work=envelopeDrop(work,envelopeMutationCanonical(true));
        return envelopeWritten(work,TargetQuotaPayloadOwner.V1_MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePayloadSummary(){return envelopeRepeat(envelopeMutationCanonical(false),2,false);}
    private static EnvelopeMaterial envelopePayloadDigestFields(boolean qualified){
        final var business=envelopePayloadFields();
        if(!qualified)return business;
        return envelopeWritten(envelopeDrop(business,envelopePayloadSummary()),TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePayloadCanonical(boolean qualified){
        var work=envelopePayloadFields();if(qualified)work=envelopeDrop(work,envelopePayloadSummary());
        return envelopeWritten(work,qualified?TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES:TargetQuotaPayloadOwner.V1_MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopePayloadConstruct(boolean qualified){
        var frame=envelopeAssigned(32+32+16,3);
        frame=envelopeTemporary(frame,envelopeAssigned(64,2));
        frame=envelopeTemporary(frame,envelopePayloadReferenceBounded());
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),3,false));
        frame=envelopeHold(frame,envelopeAssigned(32)); // RELEASED branch keeps its independent Floor digest.
        frame=envelopeHold(frame,envelopeArrays(96,3)); // payload SHA, reservation ID and object profile owned copies.
        if(qualified){frame=envelopeTemporary(frame,envelopeMutationAtOrBefore());frame=envelopeHold(frame,envelopeAssigned(32));}
        return envelopeHold(frame,envelopeHash(envelopePayloadDigestFields(qualified),"nereus-delay-target-quota-payload-owner\0".length()));
    }
    private static EnvelopeMaterial envelopePayloadDecode(boolean qualified){
        final long n=qualified?TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES:TargetQuotaPayloadOwner.V1_MAX_CANONICAL_BYTES;
        final long mutations=qualified?3:1;
        var frame=envelopeProtoRead(n,n,n,qualified?24:18);
        // decode owns all byte-valued field accessors in byte[25][] through its constructor and canonical check.
        frame=envelopeHold(frame,envelopeArrays(n,qualified?16:13));
        var arguments=envelopeDelayMessageIdDecode();arguments=envelopeHold(arguments,envelopeIdentityDecode());
        arguments=envelopeHold(arguments,envelopeAccountingDecode());arguments=envelopeHold(arguments,envelopePayloadRefDecode());
        arguments=envelopeHold(arguments,envelopeMutationDecode(true));
        if(qualified)arguments=envelopeHold(arguments,envelopeRepeat(envelopeMutationDecode(false),2,true));
        final var constructor=envelopePayloadConstruct(qualified);
        frame=envelopeHold(frame,envelopeCall(arguments,constructor,arguments.held));
        frame=envelopeTemporary(frame,envelopePayloadCanonical(qualified));
        return envelopeReturn(frame,arguments.held+constructor.held);
    }

    private static EnvelopeMaterial envelopeFenceCanonical(){return envelopeArray(com.nereusstream.delay.store.IngressFenceState.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeFenceDecode(){
        var frame=envelopeHold(envelopeProtoRead(com.nereusstream.delay.store.IngressFenceState.MAX_CANONICAL_BYTES,9+32,32,2),
                envelopeCall(envelopeArray(32),envelopeArray(32),0));
        frame=envelopeTemporary(frame,envelopeFenceCanonical());return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeCursorScopeCanonical(){
        return envelopeWritten(envelopeDrop(envelopeShardCanonical(),envelopeArray(32)),128);
    }
    private static EnvelopeMaterial envelopeCursorScopeDecode(){
        var arguments=envelopeHold(envelopeArray(20),envelopeCall(envelopeArray(16),envelopeArray(16),0));
        arguments=envelopeHold(arguments,envelopeCall(envelopeArray(32),envelopeArray(32),0));
        arguments=envelopeHold(arguments,envelopeArray(16));arguments=envelopeHold(arguments,envelopeArray(32));
        var frame=envelopeHold(envelopeProtoRead(128,1+20+32+5+10+16+32,32,7),
                envelopeCall(arguments,envelopeAssigned(48,2),48));
        frame=envelopeTemporary(frame,envelopeCursorScopeCanonical());return envelopeReturn(frame,96);
    }
    /** Target Pulsar dominates the fixed branches; variable topic bytes have their own aggregate budget. */
    private static EnvelopeMaterial envelopeCursorCanonical(long topicBytes){
        final long member=83+topicBytes,encoded=381+topicBytes;
        var work=envelopeCursorScopeCanonical();
        work=envelopeDrop(work,envelopeWritten(envelopeArray(topicBytes),member));
        return envelopeWritten(work,encoded);
    }
    private static EnvelopeMaterial envelopeCursorDecode(long topicBytes){
        final long member=83+topicBytes,encoded=381+topicBytes;
        var frame=envelopeProtoRead(encoded,encoded,encoded,7);
        frame=envelopeHold(frame,envelopeNested(member,envelopeProtoRead(member,member,member,7)));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeArray(32));
        var arguments=envelopeNested(128,envelopeCursorScopeDecode());arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeArray(topicBytes));
        // utf8 round-trip, then constructor nfc round-trip are distinct source-visible arrays.
        arguments=envelopeTemporary(arguments,envelopeArray(topicBytes));
        final var constructor=envelopeTemporary(envelopeArrays(64,2),envelopeArray(topicBytes));
        frame=envelopeHold(frame,envelopeCall(arguments,constructor,96));
        frame=envelopeTemporary(frame,envelopeCursorCanonical(topicBytes));return envelopeReturn(frame,160);
    }
    private static EnvelopeMaterial envelopeCursorsCanonical(){
        final long n=com.nereusstream.delay.store.StoreRuntimeMetadata.MAX_CANONICAL_BYTES;
        final long count=com.nereusstream.delay.store.StoreRuntimeMetadata.MAX_EVIDENCE_CURSORS;
        final var base=envelopeCursorCanonical(0);
        final var expanded=envelopeCursorCanonical(n);
        // The sum of encoded cursor fields, and hence all topic UTF-8 bytes, is bounded by this one array.
        final var fields=new EnvelopeMaterial(Math.addExact(Math.multiplyExact(base.allocated,count),expanded.allocated-base.allocated),
                expanded.retained,expanded.held,Math.multiplyExact(base.fieldVisits,count),Math.multiplyExact(base.arrayObjects,count));
        var work=envelopeRepeat(envelopeComparison(envelopeCursorScopeCanonical(),envelopeCursorScopeCanonical()),count-1,false);
        return envelopeWritten(envelopeDrop(work,fields),n);
    }
    private static EnvelopeMaterial envelopeCursorsDecode(){
        final long n=com.nereusstream.delay.store.StoreRuntimeMetadata.MAX_CANONICAL_BYTES;
        final long count=com.nereusstream.delay.store.StoreRuntimeMetadata.MAX_EVIDENCE_CURSORS;
        final var base=envelopeNested(381,envelopeCursorDecode(0));
        final var expanded=envelopeNested(381+n,envelopeCursorDecode(n));
        final long held=Math.multiplyExact(base.held,count);
        final var cursors=new EnvelopeMaterial(Math.addExact(Math.multiplyExact(base.allocated,count),expanded.allocated-base.allocated),
                Math.addExact(Math.multiplyExact(base.held,count-1),expanded.retained),held,
                Math.multiplyExact(base.fieldVisits,count),Math.multiplyExact(base.arrayObjects,count));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,count),cursors);
        frame=envelopeTemporary(frame,envelopeCursorsCanonical());return envelopeReturn(frame,held);
    }


    /** The constructed fixed projections are not Backend.Edit instances. */
    static EnvelopeMaterial envelopeProjectionMaterial(TargetSourceOperationEnvelope.Row row){
        return switch(Objects.requireNonNull(row)){
            case COUNTER->envelopeHash(envelopeQuotaSumFields(false),"nereus-delay-target-quota-counter\0".length());
            case TOTAL->envelopeHash(envelopeQuotaSumFields(true),"nereus-delay-target-quota-total\0".length());
            case AGGREGATE->envelopeHold(envelopeAssigned(16),envelopeHash(envelopeAggregateFields(),"nereus-delay-target-quota-aggregate\0".length()));
            default->throw new IllegalArgumentException("not an actual fixed quota projection: "+row);
        };
    }
    static EnvelopeMaterial envelopeUsageArithmeticMaterial(){
        return envelopeCall(envelopeVectorConstruct(),envelopeUsageConstruct(),32);
    }
    /** Mathematical material upper bound only: each listed successor invokes the same decoded constructor. */
    static EnvelopeMaterial envelopeCreationUpperBound(TargetSourceOperationEnvelope.Row row,long actualSourceF){
        return switch(Objects.requireNonNull(row)){
            case QUEUE,WORK,ORDER_HEAD,ORDER,EXPIRY,BINDING,RESERVATION,MEMBER,CHANNEL_CONTROL,CHANNEL_SLOT,MEMBER_CLOSE,
                    CLOSE,CLOSE_CURSOR,NATIVE_CONTROL,EVIDENCE_OWNER,EVIDENCE_REF,RECOVERY_OWNER,RECOVERY_REF,
                    KAFKA_MAPPING,KAFKA_SLOT,TERMINAL,INITIAL,
                    MESSAGE,PAYLOAD_V1,PAYLOAD,SCOPE,GRANT,ACTIVATION,HISTORY,HISTORY_TIP,OBJECT_PENDING,
                    ARCHIVE_ROW_V1,ARCHIVE_ROW,ARCHIVE_COPY,ARCHIVE_HEAD_V2,ARCHIVE_HEAD,ARCHIVE_STEP->
                    envelopeDecodeMaterial(row,actualSourceF);
            case PARENT_EMPTY->{
                // fund() constructs its retained Control and checks the supplied full Definition before Parent.
                var prefix=envelopeReturn(envelopeParentControlConstruct(),0);
                prefix=envelopeTemporary(prefix,envelopeParentBodyGetter());
                prefix=envelopeTemporary(prefix,envelopeComparison(envelopeParentDefinitionCanonical(),envelopeParentDefinitionCanonical()));
                yield envelopeCall(envelopeReturn(prefix,0),envelopeParentDecode(0),0);
            }
            case PARENT->{
                // Scope allocation first materializes the new Reference/digest/commitment arguments.
                var arguments=envelopeRepeat(envelopeAssigned(32),2,true);
                arguments=envelopeHold(arguments,envelopeHash(envelopePaidDefinitionFields(),
                        "nereus-delay-target-scope-control-obligation-definition\0".length()));
                arguments=envelopeHold(arguments,envelopeRepeat(envelopeVectorConstruct(),2,false));
                // allocate() obtains two scopeId getter copies for every already committed child.
                arguments=envelopeTemporary(arguments,envelopeRepeat(envelopeComparison(envelopeArray(32),envelopeArray(32)),
                        com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS-1L,false));
                yield envelopeCall(arguments,envelopeParentDecode(com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS),0);
            }
            case ATTEMPT->envelopeTemporary(envelopeAttemptDecode(),envelopeMutationAtOrBefore());
            case DESCRIPTOR->{
                // allocate() computes allocationId before constructing the identity and descriptor.
                var arguments=envelopeHold(envelopeIncarnationId(),envelopeArray(32));
                var created=envelopeCall(arguments,envelopeIncarnationDecode(false),0);
                // Its original grant growth guard constructs an empty Usage in the allocation authority callback.
                yield envelopeTemporary(created,envelopeUsageEmptyMaterial());
            }
            case SLO_STATE,GENERATION,SAMPLE,SAMPLE_REF,COMMAND_SAMPLE,COMMAND_SAMPLE_REF,DUE_SAMPLE,DUE_SAMPLE_REF,EVENT,CUSTODY,WRITER->
                    envelopeSloCreationUpperBound(row);
            default->throw new IllegalArgumentException("successor construction has no established decoder upper bound: "+row);
        };
    }

    /** Factory-only calls plus the same final constructor graph, bounded over every actual result kind. */
    static EnvelopeMaterial envelopeResultFactoryUpperBound(){
        var selected=envelopeEmpty();
        for(int kind=0;kind<6;kind++){
            EnvelopeMaterial factory;
            if(kind==0){
                // command(): id.bytes(), CommandDedupeRecord's hash copy, then encode().
                factory=envelopeHold(envelopeArray(41),envelopeCall(envelopeArray(32),envelopeCommandDedupeCanonical(),0));
            }else if(kind==1){
                // result(): decode(first.payload).result().encode(), repeated by requireFirst after construction.
                factory=envelopeCall(envelopeCommandDedupeDecode(),envelopeCommandResultCanonical(),0);
                factory=envelopeTemporary(factory,envelopeCall(envelopeCommandDedupeDecode(),envelopeCommandResultCanonical(),0));
            }else if(kind==2){
                factory=envelopeHold(envelopeArray(32),envelopeSystemResultCanonical());
            }else if(kind==5){
                // expiredCommandPosition constructs CommandResult from its actual Source bytes before dedupe encoding.
                factory=envelopeCall(envelopeSourceCanonical(),envelopeCommandResultDecode(),0);
                factory=envelopeTemporary(factory,envelopeArrays(73,2));
                factory=envelopeCall(factory,envelopeCommandDedupeCanonical(),0);
            }else factory=envelopeEmpty(); // POSITION uses first's immutable private arrays directly.
            var result=envelopeCall(factory,envelopeResultDecode(kind,kind==2),0);
            // create()/requireOwner retain getter copies and check both original allocation and latest stamp.
            result=envelopeTemporary(result,envelopeArrays(96,4));
            result=envelopeTemporary(result,envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()));
            result=envelopeTemporary(result,envelopeRepeat(envelopeMutationAtOrBefore(),3,false));
            selected=envelopeChoice(selected,result);
        }
        return selected;
    }
    /** from().encode() including the actual source.canonicalBytes argument made by its Source caller. */
    static EnvelopeMaterial envelopeSystemResultFromMaterial(long actualSourceF){
        if(actualSourceF<20)throw new IllegalArgumentException("missing actual full Source maximum");
        return envelopeCall(envelopeSourceCanonical(),envelopeSystemResultFromCanonical(actualSourceF),0);
    }
    /** Only for Native callers whose original frame contract independently bounds author to MAX_AUTHOR_BYTES. */
    static EnvelopeMaterial envelopeSystemResultFromMaterial(){
        return envelopeCall(envelopeSourceCanonical(),envelopeSystemResultFromCanonical(TargetResultRecord.MAX_AUTHOR_BYTES),0);
    }
    static EnvelopeMaterial envelopeUsageEmptyMaterial(){
        return envelopeCall(envelopeVectorConstruct(),envelopeUsageConstruct(),32);
    }
    private static EnvelopeMaterial envelopeLegacyCheckpointCanonical(long n){return envelopeWritten(envelopeLegacyProfileCanonical(n),n);}
    private static EnvelopeMaterial envelopeLegacyCheckpointDecode(long n){
        var args=envelopeHold(envelopeArrays(n,6),envelopeNested(n,envelopeLegacyProfileDecode(n)));
        return envelopeCoreDecoded(n,8,args,envelopeArrays(n,6),envelopeLegacyCheckpointCanonical(n),args.held-n);
    }
    private static EnvelopeMaterial envelopeLegacyResourceBranch(long n){
        var fields=envelopeProtoRead(n,n,n,8);
        final var payload=envelopeHold(envelopeNested(n,envelopeLegacyProfileDecode(n)),envelopeArrays(n,5));
        final var checkpoint=envelopeLegacyCheckpointDecode(n);
        final var broker=envelopeHold(envelopeNested(n,envelopeLegacyBrokerDecode(n)),envelopeArrays(n,3));
        final var channel=envelopeNested(n,envelopeEvidenceChannelDecode(n));
        final var local=envelopeHold(envelopeNested(24,envelopeShardSubjectDecode()),envelopeArrays(80,3));
        return envelopeHold(fields,envelopeChoice(envelopeChoice(payload,checkpoint),envelopeChoice(broker,envelopeChoice(channel,local))));
    }
    private static EnvelopeMaterial envelopeLegacyExactResourceDecode(long n){
        var work=envelopeHold(envelopeProtoRead(n,n,n,1),envelopeArray(n));
        work=envelopeTemporary(work,envelopeLegacyResourceBranch(n));work=envelopeHold(work,envelopeArray(n));
        work=envelopeHold(work,envelopeHash(envelopeHold(envelopeArray("nereus-delay-resource-identity\0".length()),envelopeArray(n)),0));
        // The private decode invokes the public record constructor, which revalidates all branch bytes.
        var ctor=envelopeHold(envelopeProtoRead(n,n,n,1),envelopeArray(n));
        ctor=envelopeTemporary(ctor,envelopeLegacyResourceBranch(n));ctor=envelopeHold(ctor,envelopeArray(n));
        ctor=envelopeHold(ctor,envelopeHash(envelopeHold(envelopeArray("nereus-delay-resource-identity\0".length()),envelopeArray(n)),0));
        work=envelopeHold(work,ctor);return envelopeReturn(work,Math.addExact(n,32));
    }
    private static EnvelopeMaterial envelopeLegacyProtectionsDecode(long n){
        // Legacy has no Target MAX_PROTECTIONS/MAX_SOURCE ceiling. At least two wire bytes per reference.
        final long count=n/2;
        var refs=envelopeHold(envelopeArrays(n,count),envelopeCoreReaders(n,Math.max(1,count),Math.multiplyExact(7,count)));
        refs=envelopeHold(refs,envelopeArrays(n,Math.multiplyExact(5,count)));
        refs=envelopeTemporary(refs,envelopePhysicalSourcesDecode(n,count));
        refs=envelopeTemporary(refs,envelopePhysicalSourcesDecode(n,count));
        refs=envelopeHold(refs,envelopePhysicalSourcesCanonical(n,count));
        refs=envelopeHold(refs,envelopeArrays(n,Math.multiplyExact(5,count)));refs=envelopeHold(refs,envelopeArrays(n,count));
        var work=envelopeHold(envelopeProtoRead(n,n,n,Math.addExact(count,1)),refs);
        final var keys=envelopeArrays(n,count);
        work=envelopeTemporary(work,keys);work=envelopeTemporary(work,keys);
        final var hash=envelopeHash(envelopeWritten(keys,n),"nereus-delay-protection-set\0".length());
        work=envelopeTemporary(work,hash);work=envelopeTemporary(work,hash);
        work=envelopeTemporary(work,envelopeWritten(keys,n));work=envelopeHold(work,envelopeWritten(keys,n));
        work=envelopeHold(work,envelopeArray(32));return envelopeReturn(work,Math.addExact(refs.held,Math.addExact(n,32)));
    }
    private static EnvelopeMaterial envelopeLegacyRetireValidation(long n){return envelopeLegacyValidation(n,3,3);}
    private static EnvelopeMaterial envelopeLegacyRetireIdentity(long n){
        var parsed=envelopeLegacyBodyFields(n,envelopeLegacyRetireValidation(n));
        parsed=envelopeHold(parsed,envelopeNested(n,envelopeLegacyExactResourceDecode(n)));
        parsed=envelopeHold(parsed,envelopeNested(n,envelopeLegacyProtectionsDecode(n)));
        var fields=envelopeHold(envelopeArray("nereus-delay-retire-logical-id".length()),envelopeArray(1));
        fields=envelopeHold(fields,envelopeCall(envelopeArray(32),envelopeArray(32),0));
        fields=envelopeHold(fields,envelopeArray(8));
        return envelopeCall(parsed,envelopeHash(fields,0),0);
    }
    private static EnvelopeMaterial envelopeLegacyDeleteEvidenceDecode(long n){
        var args=envelopeHold(envelopeArrays(n,5),envelopeNested(n,envelopeLegacyTimeDecode(n)));
        final var canonical=envelopeWritten(envelopeLegacyTimeCanonical(n),n);
        return envelopeCoreDecoded(n,7,args,envelopeArrays(n,5),canonical,args.held-n);
    }
    private static EnvelopeMaterial envelopeLegacyDeleteIdentity(long n){
        var parsed=envelopeLegacyBodyFields(n,envelopeLegacyValidation(n,4,4));
        var args=envelopeNested(128,envelopeCoreDecoded(128,4,envelopeArrays(96,3),envelopeArrays(96,3),envelopeArray(128),0));
        args=envelopeHold(args,envelopeNested(n,envelopeLegacyDeleteEvidenceDecode(n)));
        args=envelopeHold(args,envelopeNested(n,envelopeLegacyTimeDecode(n)));
        return envelopeCall(envelopeHold(parsed,args),envelopeArray(32),0);
    }
    private static EnvelopeMaterial envelopeGrantBodyCanonical(){
        var work=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        work=envelopeDrop(work,envelopeHash(envelopeHold(envelopeArray(2),envelopeGrantRequestCanonical()),"nereus-delay-target-quota-control\0".length()));
        work=envelopeDrop(work,envelopeWritten(envelopeGrantRequestCanonical(),com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES+4L));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeGrantBodyDecode(){
        var args=envelopeNested(24,envelopeTemporary(envelopeProtoRead(24,24,24,2),envelopeShardSubjectDecode()));
        args=envelopeHold(args,envelopeNested(74,envelopeTemporary(envelopeProtoRead(74,74,74,3),envelopeControlRefDecode())));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES+4L,
                envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES+4L,1,
                        envelopeNested(com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES,envelopeGrantRequestDecode()),
                        envelopeEmpty(),envelopeEmpty(),envelopeGrantRequestDecode().held)));
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES,8,args,
                envelopeReturn(envelopeGrantRequestControlRef(),0),envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeComparison(envelopeArray(32),envelopeHash(envelopeHold(envelopeArray(2),envelopeGrantRequestCanonical()),
                "nereus-delay-target-quota-control\0".length())));return envelopeTemporary(value,envelopeGrantBodyCanonical());
    }
    private static EnvelopeMaterial envelopeSummaryRequestCanonical(){
        var work=envelopeDrop(envelopeScopeCanonical(),envelopePayloadCanonical(false));
        work=envelopeDrop(work,envelopeMutationCanonical(false));
        return envelopeWritten(envelopeDrop(work,envelopeLegacyCheckpointCanonical(com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES)),
                com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeSummaryRequestDecode(){
        final long n=com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES;
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArrays(48,2));
        args=envelopeHold(args,envelopeNested(TargetQuotaPayloadOwner.V1_MAX_CANONICAL_BYTES,envelopePayloadDecode(false)));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(n,envelopeLegacyCheckpointDecode(n)));
        var own=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));own=envelopeTemporary(own,envelopeArrays(64,4));
        own=envelopeTemporary(own,envelopeManagedRoutingDecode());own=envelopeTemporary(own,envelopeMutationAtOrBefore());
        return envelopeCoreDecoded(n,8,args,own,envelopeSummaryRequestCanonical(),args.held-48);
    }
    private static EnvelopeMaterial envelopeSummaryBodyCanonical(){
        var work=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        work=envelopeDrop(work,envelopeHash(envelopeSummaryRequestCanonical(),"nereus-delay-payload-summary-initialize-v1\0".length()));
        work=envelopeDrop(work,envelopeWritten(envelopeSummaryRequestCanonical(),com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES+8L));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeSummaryBodyDecode(){
        var args=envelopeNested(24,envelopeShardSubjectDecode());args=envelopeHold(args,envelopeNested(74,envelopeControlRefDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES+8L,
                envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES+8L,1,
                        envelopeNested(com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES,envelopeSummaryRequestDecode()),
                        envelopeEmpty(),envelopeEmpty(),envelopeSummaryRequestDecode().held)));
        var check=envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(envelopeSummaryRequestCanonical(),
                com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES,38));
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationBody.MAX_CANONICAL_BYTES,9,args,
                envelopeReturn(check,0),envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeComparison(envelopeArray(32),envelopeHash(envelopeSummaryRequestCanonical(),
                "nereus-delay-payload-summary-initialize-v1\0".length())));return envelopeTemporary(value,envelopeSummaryBodyCanonical());
    }
    private static EnvelopeMaterial envelopeSourceControlIdentity(long bodyBytes){
        var selected=envelopeLegacyApplyControlIdentity(bodyBytes);
        var target=envelopeChoice(envelopeActivationBodyDecode(),envelopeBusinessMemberBodyDecode());
        target=envelopeChoice(target,envelopeGrantBodyDecode());target=envelopeChoice(target,envelopeBusinessCloseBodyDecode());
        target=envelopeChoice(target,envelopeBusinessNativeBodyDecode());target=envelopeChoice(target,envelopeBusinessChannelBodyDecode());
        target=envelopeChoice(target,envelopeSloObservationBodyDecode());target=envelopeChoice(target,envelopeSloWriterBodyDecode());
        target=envelopeChoice(target,envelopeParentBodyDecode());target=envelopeChoice(target,envelopePaidBodyDecode());
        target=envelopeChoice(target,envelopeMigrationControlBodyDecode());target=envelopeChoice(target,envelopeSummaryBodyDecode());
        // kind1 hasTargetPayload inspects its oneof and payload discriminator before choosing the typed reader.
        var discriminator=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyApplyControlValidation(bodyBytes));
        discriminator=envelopeTemporary(discriminator,envelopeCoreReaders(bodyBytes,2,2));
        final var typed=envelopeDrop(discriminator,envelopeCall(target,envelopeControlLogicalIdentity(),0));
        return envelopeChoice(selected,typed);
    }
    private static EnvelopeMaterial envelopeSourceControlValidation(long bodyBytes){
        var selected=envelopeLegacyApplyControlValidation(bodyBytes);
        selected=envelopeChoice(selected,envelopeSummaryBodyDecode());selected=envelopeChoice(selected,envelopeMigrationControlBodyDecode());
        selected=envelopeChoice(selected,envelopeSloObservationBodyValidation());selected=envelopeChoice(selected,envelopeSloWriterBodyValidation());
        return envelopeReturn(selected,0);
    }
    private static EnvelopeMaterial envelopeSystemEnvelopeDecode(long envelopeBytes,boolean excludeReclaim){
        final long body=envelopeBytes,author=envelopeBytes;
        var selected=envelopeSystemDecode(envelopeBytes,body,author,envelopeSourceControlValidation(body),envelopeSourceControlIdentity(body),true);
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyReplayValidation(body),envelopeLegacyReplayIdentity(body),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyResolveValidation(body),envelopeLegacyResolveIdentity(body),false));
        // TIME_FENCE and EXPIRE_GENERATION identities use their registered fields, without Target-only body ceilings.
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyValidation(body,2,2),envelopeArray(32),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyPublishAdmissionValidation(body),envelopeLegacyPublishAdmissionIdentity(body),false));
        for(boolean resolution:new boolean[]{false,true})selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,
                envelopePublishOutcomeValidation(body,resolution),envelopePublishOutcomeIdentity(body,resolution),false));
        var expiry=envelopeHold(envelopeLegacyMessageIdDecode(),envelopeExpiryLogicalIdentity());
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyValidation(body,2,1),expiry,false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyRetireValidation(body),envelopeLegacyRetireIdentity(body),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyValidation(body,4,4),envelopeLegacyDeleteIdentity(body),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeEvidenceRetireValidation(),envelopeEvidenceRetireIdentity(),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeEvidenceConfirmedValidation(),envelopeEvidenceConfirmedIdentity(),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyClaimResultValidation(body),envelopeLegacyClaimResultIdentity(body),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeLegacyDlqExportValidation(body),envelopeLegacyDlqExportIdentity(body),false));
        final var targetAdmissionValidation=envelopeLegacyValidation(body,12,9);
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,targetAdmissionValidation,
                envelopeCall(envelopePublishAdmissionDecode(),envelopeArray(32),0),false));
        selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,envelopeSloEventBodyValidation(),envelopeSloEventBodyIdentity(),false));
        if(!excludeReclaim)selected=envelopeChoice(selected,envelopeSystemDecode(envelopeBytes,body,author,
                envelopeResourceReclaimValidation(),envelopeResourceReclaimIdentity(),false));
        return selected;
    }
    private static EnvelopeMaterial envelopeSystemFrame(long frameBytes,boolean excludeReclaim){
        if(frameBytes<20)throw new IllegalArgumentException("invalid actual full Source frame bound");
        final long n=Math.subtractExact(frameBytes,16);
        // ShardLogFrame decode owns copyOfRange + Decoded copy + canonicalEnvelope accessor copy.
        final var decoded=envelopeCall(envelopeCall(envelopeArray(n),envelopeArray(n),0),envelopeSystemEnvelopeDecode(n,excludeReclaim),0);
        return envelopeCall(envelopeArray(n),decoded,0);
    }
    private static EnvelopeMaterial envelopeSystemFrameDecode(long frameBytes){return envelopeSystemFrame(frameBytes,false);}
    private static EnvelopeMaterial envelopeSystemNonReclaimDecode(long frameBytes){return envelopeSystemFrame(frameBytes,true);}
    private static EnvelopeMaterial envelopeSystemFrameCanonical(long frameBytes){
        final long n=Math.subtractExact(frameBytes,16);
        var frame=envelopeHold(envelopeArray(12),envelopeArray(n+12));
        frame=envelopeDrop(frame,envelopeArray(4));frame=envelopeReturn(frame,16);
        frame=envelopeCall(frame,envelopeArray(frameBytes),0);
        return envelopeCall(envelopeSystemCanonical(n),frame,0);
    }
    private static EnvelopeMaterial envelopeSystemNonReclaimCanonical(long frameBytes){return envelopeSystemFrameCanonical(frameBytes);}
    /** Full actual Source decoder union; opaque legacy fields use F, never a typed family cap. */
    static EnvelopeMaterial envelopeSourceMaterial(long actualSourceF){
        if(actualSourceF<20)throw new IllegalArgumentException("missing actual full Source maximum");
        return envelopeChoice(envelopeCommandFrameDecode(actualSourceF),envelopeSystemFrameDecode(actualSourceF));
    }
    /** encodeFrame's actual Command/System branch; it performs no typed body decoder call. */
    static EnvelopeMaterial envelopeSourceCanonicalMaterial(long actualSourceF){
        if(actualSourceF<20)throw new IllegalArgumentException("missing actual full Source maximum");
        return envelopeChoice(envelopeCommandFrameCanonical(actualSourceF),envelopeSystemFrameCanonical(actualSourceF));
    }

    static EnvelopeMaterial envelopeDecodeMaterial(TargetSourceOperationEnvelope.Row row){
        return envelopeDecodeMaterial(row,-1);
    }
    static EnvelopeMaterial envelopeCanonicalMaterial(TargetSourceOperationEnvelope.Row row){
        return envelopeCanonicalMaterial(row,-1);
    }
    static EnvelopeMaterial envelopeDecodeMaterial(TargetSourceOperationEnvelope.Row row,long genuineFrameBytes){
        return switch(Objects.requireNonNull(row)){
            case INITIAL->envelopeInitialDecode();
            case FENCE->envelopeFenceDecode();
            case CURSORS->envelopeCursorsDecode();
            case QUEUE,IDENTITY,MESSAGE,EXPIRY,ORDER,DISPATCH,CONTROL,CHANNEL,BINDING,MEMBER,POLICY,NATIVE_SCOPE,NATIVE_SNAPSHOT,
                    RESERVATION,CLOSE,CLOSE_CURSOR,MEMBER_CLOSE,NATIVE_CONTROL,CHANNEL_CONTROL,CHANNEL_SLOT,KAFKA_MAPPING,KAFKA_SLOT,
                    EVIDENCE_OWNER,EVIDENCE_REF,RECOVERY_OWNER,RECOVERY_REF,RETIRING,CONFIRMED->envelopeBusinessDecodeMaterial(row);
            case WORK,ORDER_HEAD->envelopeBusinessWorkDecode();
            case COUNTER->envelopeQuotaSumDecode(false);
            case AGGREGATE->envelopeAggregateDecode();
            case ATTEMPT->envelopeAttemptDecode();
            case TOTAL->envelopeQuotaSumDecode(true);
            case GRANT->envelopeGrantActivationDecode();
            case BOOKKEEPING->envelopeBookkeepingDecode();
            case PAYLOAD_V1->envelopePayloadDecode(false);
            case PAYLOAD->envelopePayloadDecode(true);
            case DESCRIPTOR->envelopeIncarnationDecode(true);
            case CLAIM_CHARGE->envelopeClaimChargeDecode();
            case FIRST,POSITION->envelopeResultDecode();
            case CLAIM->envelopeClaimDecode();
            case TERMINAL->envelopeTerminalDecode();
            case ACTIVATION->envelopeActivationDecode();
            case HISTORY->envelopeRetirementSourceHistoryDecode();
            case HISTORY_TIP->envelopeRetirementHistoryTipDecode();
            case OBJECT_PENDING->envelopeObjectPendingDecode();
            case SLO_STATE,GENERATION,SAMPLE,SAMPLE_REF,COMMAND_SAMPLE,COMMAND_SAMPLE_REF,DUE_SAMPLE,DUE_SAMPLE_REF,EVENT,CUSTODY,WRITER->
                    envelopeSloDecodeMaterial(row);
            case PARENT_EMPTY->envelopeParentDecode(0);
            case PARENT->envelopeParentDecode(com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS);
            case SCOPE->envelopePaidScopeDecode();
            case MIGRATION,MIGRATION_ORIGIN,MIGRATION_GUARD,MIGRATION_PREFIX,MIGRATION_ATTEMPT,RETIREMENT,ARCHIVE_HEAD_V2,ARCHIVE_HEAD,
                    ARCHIVE_STEP,ARCHIVE_ROW_V1,ARCHIVE_ROW,ARCHIVE_COPY->envelopeMigrationDecodeMaterial(row,genuineFrameBytes);
        };
    }
    static EnvelopeMaterial envelopeCanonicalMaterial(TargetSourceOperationEnvelope.Row row,long genuineFrameBytes){
        return switch(Objects.requireNonNull(row)){
            case INITIAL->envelopeInitialCanonical();
            case FENCE->envelopeFenceCanonical();
            case CURSORS->envelopeCursorsCanonical();
            case QUEUE,IDENTITY,MESSAGE,EXPIRY,ORDER,DISPATCH,CONTROL,CHANNEL,BINDING,MEMBER,POLICY,NATIVE_SCOPE,NATIVE_SNAPSHOT,
                    RESERVATION,CLOSE,CLOSE_CURSOR,MEMBER_CLOSE,NATIVE_CONTROL,CHANNEL_CONTROL,CHANNEL_SLOT,KAFKA_MAPPING,KAFKA_SLOT,
                    EVIDENCE_OWNER,EVIDENCE_REF,RECOVERY_OWNER,RECOVERY_REF,RETIRING,CONFIRMED->envelopeBusinessCanonicalMaterial(row);
            case WORK,ORDER_HEAD->envelopeBusinessWorkCanonical();
            case COUNTER->envelopeQuotaSumCanonical(false);
            case AGGREGATE->envelopeAggregateCanonical();
            case ATTEMPT->envelopeAttemptCanonical();
            case TOTAL->envelopeQuotaSumCanonical(true);
            case GRANT->envelopeGrantActivationCanonical();
            case BOOKKEEPING->envelopeBookkeepingCanonical();
            case PAYLOAD_V1->envelopePayloadCanonical(false);
            case PAYLOAD->envelopePayloadCanonical(true);
            case DESCRIPTOR->envelopeIncarnationCanonical(true);
            case CLAIM_CHARGE->envelopeClaimChargeCanonical();
            case FIRST,POSITION->envelopeResultCanonical();
            case CLAIM->envelopeClaimCanonical();
            case TERMINAL->envelopeTerminalCanonical();
            case ACTIVATION->envelopeActivationCanonical();
            case HISTORY->envelopeRetirementSourceHistoryCanonical();
            case HISTORY_TIP->envelopeRetirementHistoryTipCanonical();
            case OBJECT_PENDING->envelopeObjectPendingCanonical();
            case SLO_STATE,GENERATION,SAMPLE,SAMPLE_REF,COMMAND_SAMPLE,COMMAND_SAMPLE_REF,DUE_SAMPLE,DUE_SAMPLE_REF,EVENT,CUSTODY,WRITER->
                    envelopeSloCanonicalMaterial(row);
            case PARENT_EMPTY->envelopeParentCanonical(0);
            case PARENT->envelopeParentCanonical(com.nereusstream.delay.protocol.TargetParentControlCredit.MAX_ALLOCATIONS);
            case SCOPE->envelopePaidScopeCanonical();
            case MIGRATION,MIGRATION_ORIGIN,MIGRATION_GUARD,MIGRATION_PREFIX,MIGRATION_ATTEMPT,RETIREMENT,ARCHIVE_HEAD_V2,ARCHIVE_HEAD,
                    ARCHIVE_STEP,ARCHIVE_ROW_V1,ARCHIVE_ROW,ARCHIVE_COPY->envelopeMigrationCanonicalMaterial(row,genuineFrameBytes);
        };
    }

    // SLO_MATERIAL_REGION: the SLO codec graph is owned independently of the Row dispatch below.

    /** Factory-only calls followed by the decoder's containing graph of the same final constructor. */
    private static EnvelopeMaterial envelopeSloCreationUpperBound(TargetSourceOperationEnvelope.Row row){
        final EnvelopeMaterial factory=switch(row){
            case SLO_STATE->envelopeSloStateFactory();
            case SAMPLE,SAMPLE_REF->envelopeChoice(envelopeSloSampleFactory(false),envelopeSloSampleFactory(true));
            case COMMAND_SAMPLE,COMMAND_SAMPLE_REF->envelopeSloSampleFactory(false);
            case DUE_SAMPLE,DUE_SAMPLE_REF->envelopeSloSampleFactory(true);
            case EVENT->envelopeHold(envelopeSloIncomingAndMerge(),envelopeArray(32)); // original before outbox digest getter
            // enabled/stopped/withCounts, registered/closed and direct custody creation add no array-producing factory calls.
            case GENERATION,CUSTODY,WRITER->envelopeEmpty();
            default->throw new IllegalArgumentException("not an established SLO creation family: "+row);
        };
        // The parser/recheck work is a mathematical majorant of each explicitly listed same-constructor call,
        // not a claim that production invokes a Reader while constructing the successor.
        return envelopeCall(factory,envelopeSloDecodeMaterial(row),0);
    }
    private static EnvelopeMaterial envelopeSloFactoryEnableBody(){
        return envelopeChoice(envelopeNested(envelopeSloControlBodyBytes(false,false,false),envelopeSloControlBodyDecode(false,true,false,false)),
                envelopeChoice(envelopeNested(envelopeSloControlBodyBytes(false,true,false),envelopeSloControlBodyDecode(false,true,true,false)),
                        envelopeNested(envelopeSloControlBodyBytes(false,true,true),envelopeSloControlBodyDecode(false,true,true,true))));
    }
    private static EnvelopeMaterial envelopeSloFactoryPolicyDigest(){
        final var digest=envelopeChoice(envelopeHash(envelopeSloPolicyFields(false),"nereus-delay-target-slo-observation-policy\0".length()),
                envelopeHash(envelopeSloPolicyFields(true),"nereus-delay-target-slo-observation-epoch-policy\0".length()));
        return envelopeCall(envelopeSloFactoryEnableBody(),digest,0);
    }
    private static EnvelopeMaterial envelopeSloStateFactory(){
        // State.from retains policy; generation() independently decodes enable.body(), and current Control.body()
        // is another typed call. controlDigest hashes both actual applied Controls when a STOP is present.
        var factory=envelopeSloFactoryEnableBody();
        factory=envelopeTemporary(factory,envelopeSloFactoryEnableBody());
        factory=envelopeHold(factory,envelopeArray(16));
        factory=envelopeHold(factory,envelopeCall(envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationControlBody.MAX_CANONICAL_BYTES,
                envelopeSloObservationBodyDecode()),envelopeControlLogicalIdentity(),0));
        final var controls=envelopeChoice(envelopeSloAppliedControlCanonical(false,true,false),envelopeSloAppliedControlCanonical(false,true,true));
        final var stopped=envelopeHold(controls,envelopeSloAppliedControlCanonical(false,false,true));
        return envelopeHold(factory,envelopeHash(envelopeChoice(envelopeHold(controls,envelopeArray(0)),stopped),
                "nereus-delay-target-slo-observation-generation-control\0".length()));
    }
    private static EnvelopeMaterial envelopeSloOpenFactory(boolean due){
        final long fields=2+6L+com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_START_BYTES;
        final var start=envelopeSloExpectedStart(due);
        final var openHash=envelopeHash(envelopeWritten(envelopeSloStartCanonical(due),fields),"nereus-delay-slo-outbox\0".length());
        return envelopeCall(start,openHash,start.held);
    }
    private static EnvelopeMaterial envelopeSloSampleFactory(boolean due){
        // Production birth reads generation(), enable's mutation ID, and policy().digest independently.
        var birth=envelopeReturn(envelopeSloFactoryEnableBody(),0);
        birth=envelopeHold(birth,envelopeArray(32));
        birth=envelopeHold(birth,envelopeSloFactoryPolicyDigest());
        EnvelopeMaterial observation;
        if(due){
            // TargetSloMessageEligibility.from(message): message.digest() is an immutable getter clone.
            final var eligibility=envelopeCall(envelopeArray(32),envelopeTemporary(envelopeAssigned(32),envelopeSourceBounded()),0);
            final var semantic=envelopeHash(envelopeSloEligibilityFields(),"nereus-delay-target-slo-message-eligibility\0".length());
            observation=envelopeCall(eligibility,envelopeCall(semantic,envelopeSloOpenFactory(true),0),eligibility.held);
        }else observation=envelopeSloOpenFactory(false);
        birth=envelopeHold(birth,observation);
        // EventStore's successor additionally owns getter clones of enableId and policyDigest and its merged outbox.
        final var update=envelopeHold(envelopeRepeat(envelopeArray(32),2,true),envelopeSloIncomingAndMerge());
        return envelopeChoice(birth,update);
    }
    private static EnvelopeMaterial envelopeSloEvidenceSampleChecks(){
        final long input=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES;
        final var body=envelopeNested(com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_SUPPORTED_BYTES,envelopePublishAdmissionDecode());
        var admission=envelopeTemporary(body,envelopeComparison(envelopeSystemCanonical(input),envelopeSystemCanonical(input)));
        admission=envelopeTemporary(admission,envelopeSloAtOrBefore());
        admission=envelopeReturn(admission,0);
        var gap=envelopeRepeat(envelopeArray(32),4,true); // exact sampleId and Start digest getter pairs
        gap=envelopeTemporary(gap,envelopeComparison(envelopeArray(32),envelopeSloAnySampleBindingDigest()));
        gap=envelopeTemporary(gap,envelopeRepeat(envelopeHold(envelopeRepeat(envelopeArray(32),2,true),
                envelopeRepeat(envelopeArray(16),2,true)),2,false)); // known first/POS tenant and lineage comparisons
        gap=envelopeTemporary(gap,envelopeRepeat(envelopeSloAtOrBefore(),2,false));
        final var commandGap=envelopeComparison(envelopeResultCanonical(),envelopeResultCanonical());
        var admissionGap=envelopeTemporary(admission,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        admissionGap=envelopeTemporary(admissionGap,envelopeComparison(envelopeIdentityCanonical(),envelopeIdentityCanonical()));
        admissionGap=envelopeTemporary(admissionGap,envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()));
        admissionGap=envelopeTemporary(admissionGap,envelopeComparison(envelopeArray(TargetResultRecord.MAX_PAYLOAD_BYTES),
                envelopeCall(envelopeSourceCanonical(),envelopeSystemResultFromCanonical(TargetResultRecord.MAX_AUTHOR_BYTES),0)));
        gap=envelopeTemporary(gap,envelopeChoice(commandGap,admissionGap));
        return envelopeDrop(envelopeSloAtOrBefore(),envelopeChoice(admission,envelopeReturn(gap,0)));
    }
    private static EnvelopeMaterial envelopeSloIncomingAndMerge(){
        // incomingFor first checks the exact sample binding. An explicit legacy Final is already borrowed;
        // Source-assigned evidence additionally constructs a new qualified endpoint and v1 Final.
        var binding=envelopeArray(16);
        binding=envelopeDrop(binding,envelopeArray(32));
        binding=envelopeDrop(binding,envelopeSloAnySampleBindingDigest());
        var observed=envelopeSloEvidenceSampleChecks();
        var finalArguments=envelopeRepeat(envelopeArray(32),2,true);
        finalArguments=envelopeHold(finalArguments,envelopeHash(envelopeSloTimeCanonical(),0));
        finalArguments=envelopeHold(finalArguments,envelopeSloAnyEvidenceDigest(true));
        // SloSampleFinal.decode contains the same Final and SloTimeEndpoint constructors as observation().
        observed=envelopeDrop(observed,envelopeCall(finalArguments,envelopeSloFinalDecode(),0));
        final var incoming=envelopeDrop(binding,envelopeChoice(envelopeEmpty(),observed));
        var merge=envelopeRepeat(envelopeArray(32),2,false); // incoming.validateAgainst(start)
        merge=envelopeDrop(merge,envelopeComparison(envelopeSloFinalCanonical(),envelopeSloFinalCanonical()));
        merge=envelopeDrop(merge,envelopeSloFinalDecode()); // SloSampleFinal.merge's same constructor majorant
        // The merged Final remains live during the Outbox constructor's independent validation and digest.
        merge=envelopeCall(merge,envelopeSloAnyOutboxDecode(),0);
        return envelopeHold(incoming,merge);
    }

    private static EnvelopeMaterial envelopeSloCanonicalMaterial(TargetSourceOperationEnvelope.Row row){
        return switch(row){
            case SLO_STATE->envelopeSloStateCanonical();
            case GENERATION->envelopeChoice(envelopeChoice(envelopeSloGenerationCanonical(false,false),envelopeSloGenerationCanonical(false,true)),
                    envelopeChoice(envelopeSloGenerationCanonical(true,false),envelopeSloGenerationCanonical(true,true)));
            case COMMAND_SAMPLE,COMMAND_SAMPLE_REF->envelopeChoice(envelopeSloSampleCanonical(1),envelopeSloSampleCanonical(4));
            case DUE_SAMPLE,DUE_SAMPLE_REF->envelopeChoice(envelopeSloSampleCanonical(2),envelopeSloSampleCanonical(3));
            case SAMPLE,SAMPLE_REF->envelopeChoice(envelopeChoice(envelopeSloSampleCanonical(1),envelopeSloSampleCanonical(4)),
                    envelopeChoice(envelopeSloSampleCanonical(2),envelopeSloSampleCanonical(3)));
            case EVENT->envelopeChoice(envelopeChoice(envelopeSloEventCanonical(false,false,false),envelopeSloEventCanonical(false,false)),
                    envelopeChoice(envelopeSloEventCanonical(true,false),envelopeSloEventCanonical(true,true)));
            case CUSTODY->envelopeChoice(envelopeSloCustodyCanonical(false),envelopeSloCustodyCanonical(true));
            case WRITER->envelopeChoice(envelopeSloWriterCanonical(false),envelopeSloWriterCanonical(true));
            default->throw new IllegalArgumentException("not an original SLO material family");
        };
    }
    private static EnvelopeMaterial envelopeSloDecodeMaterial(TargetSourceOperationEnvelope.Row row){
        return switch(row){
            case SLO_STATE->envelopeSloStateDecode();
            case GENERATION->envelopeChoice(envelopeChoice(envelopeSloGenerationDecode(false,false),envelopeSloGenerationDecode(false,true)),
                    envelopeChoice(envelopeSloGenerationDecode(true,false),envelopeSloGenerationDecode(true,true)));
            case COMMAND_SAMPLE,COMMAND_SAMPLE_REF->envelopeChoice(envelopeSloSampleDecode(1),envelopeSloSampleDecode(4));
            case DUE_SAMPLE,DUE_SAMPLE_REF->envelopeChoice(envelopeSloSampleDecode(2),envelopeSloSampleDecode(3));
            case SAMPLE,SAMPLE_REF->envelopeChoice(envelopeChoice(envelopeSloSampleDecode(1),envelopeSloSampleDecode(4)),
                    envelopeChoice(envelopeSloSampleDecode(2),envelopeSloSampleDecode(3)));
            case EVENT->envelopeChoice(envelopeChoice(envelopeSloEventDecode(false,false,false),envelopeSloEventDecode(false,false)),
                    envelopeChoice(envelopeSloEventDecode(true,false),envelopeSloEventDecode(true,true)));
            case CUSTODY->envelopeChoice(envelopeSloCustodyDecode(false),envelopeSloCustodyDecode(true));
            case WRITER->envelopeChoice(envelopeSloWriterDecode(false),envelopeSloWriterDecode(true));
            default->throw new IllegalArgumentException("not an original SLO material family");
        };
    }
    /** The fields list keeps each actual raw field; it does not keep a second copy of scalar values. */
    private static EnvelopeMaterial envelopeSloRead(long encoded,long... rawFields){
        long sum=0,largest=0,count=0;for(long field:rawFields){sum=Math.addExact(sum,field);largest=Math.max(largest,field);if(field!=0)count++;}
        return envelopeProtoRead(encoded,sum,largest,count);
    }
    /** fields(), digest(fields()) and the final concat are separate source calls in these immutable codecs. */
    private static EnvelopeMaterial envelopeSloHashed(EnvelopeMaterial fields,long encoded,String domain){
        final var suffix=envelopeWritten(envelopeHash(fields,domain.length()),34);
        return envelopeCall(envelopeHold(fields,suffix),envelopeArray(encoded),0);
    }
    private static EnvelopeMaterial envelopeSloDecoded(EnvelopeMaterial parsed,EnvelopeMaterial arguments,
            EnvelopeMaterial constructor,long retainedArguments,EnvelopeMaterial canonical){
        return envelopeSloDecoded(parsed,arguments,constructor,retainedArguments,canonical,envelopeEmpty());
    }
    private static EnvelopeMaterial envelopeSloDecoded(EnvelopeMaterial parsed,EnvelopeMaterial arguments,
            EnvelopeMaterial constructor,long retainedArguments,EnvelopeMaterial canonical,EnvelopeMaterial verification){
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,retainedArguments));
        frame=envelopeTemporary(frame,verification);
        frame=envelopeTemporary(frame,canonical);
        return envelopeReturn(frame,Math.addExact(retainedArguments,constructor.held));
    }
    private static EnvelopeMaterial envelopeSloEndpointCanonical(){return envelopeArray(2+11+11+34);}
    private static EnvelopeMaterial envelopeSloEndpointDecode(){
        final long n=2+11+11+34;
        final var parsed=envelopeSloRead(n,1,10,10,32);
        return envelopeSloDecoded(parsed,envelopeArray(32),envelopeArray(32),0,envelopeSloEndpointCanonical());
    }
    private static EnvelopeMaterial envelopeSloFinalFields(){
        return envelopeWritten(envelopeSloEndpointCanonical(),com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeSloFinalCanonical(){
        return envelopeWritten(envelopeSloFinalFields(),com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES);
    }
    private static EnvelopeMaterial envelopeSloFinalDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES,endpoint=2+11+11+34;
        final var parsed=envelopeSloRead(n,1,32,32,1,1,10,10,1,endpoint,32,10,32);
        var arguments=envelopeRepeat(envelopeArray(32),2,true);
        arguments=envelopeHold(arguments,envelopeNested(endpoint,envelopeSloEndpointDecode()));
        arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeArray(32)); // supplied digest is consumed, not retained
        final var constructor=envelopeHold(envelopeRepeat(envelopeArray(32),3,true),
                envelopeHash(envelopeSloFinalFields(),"nereus-delay-slo-final\0".length()));
        return envelopeSloDecoded(parsed,arguments,constructor,arguments.held-4L*32,envelopeSloFinalCanonical());
    }
    private static long envelopeSloStartIdentityBytes(boolean due){
        return due?2+6+DelayMessageId.LENGTH+6+11+2:com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES+8L;
    }
    private static EnvelopeMaterial envelopeSloEventIdentityCanonical(boolean due){return envelopeArray(envelopeSloStartIdentityBytes(due));}
    private static EnvelopeMaterial envelopeSloIdentityBranchValidation(boolean due){
        final long branch=envelopeSloStartIdentityBytes(due)-2;
        final var parsed=due?envelopeSloRead(branch,DelayMessageId.LENGTH,5,10,1):
                envelopeSloRead(branch,com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES);
        final var validation=due?envelopeArray(DelayMessageId.LENGTH):envelopeNested(
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode());
        return envelopeReturn(envelopeHold(parsed,validation),0);
    }
    private static EnvelopeMaterial envelopeSloDueIdentityAccess(){
        final long encoded=envelopeSloStartIdentityBytes(true),branch=encoded-2;
        final var outer=envelopeSloRead(encoded,branch);
        final var inner=envelopeNested(branch,envelopeSloRead(branch,DelayMessageId.LENGTH,5,10,1));
        return envelopeReturn(envelopeHold(outer,inner),0);
    }
    private static EnvelopeMaterial envelopeSloEventIdentityDecode(boolean due){
        final long encoded=envelopeSloStartIdentityBytes(due),branch=encoded-2;
        final var outer=envelopeSloRead(encoded,branch);
        var frame=envelopeHold(outer,envelopeArray(branch));
        frame=envelopeTemporary(frame,envelopeSloIdentityBranchValidation(due));
        // decode copies encoded, the private alreadyCanonical constructor owns another copy, then canonicalBytes clones it.
        frame=envelopeHold(frame,envelopeCall(envelopeArray(encoded),envelopeArray(encoded),0));
        frame=envelopeTemporary(frame,envelopeSloEventIdentityCanonical(due));return envelopeReturn(frame,encoded);
    }
    private static EnvelopeMaterial envelopeSloStartFields(boolean due){
        return envelopeWritten(envelopeDrop(envelopeSloEventIdentityCanonical(due),envelopeSloEndpointCanonical()),
                com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_START_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeSloStartCanonical(boolean due){
        return envelopeWritten(envelopeSloStartFields(due),com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_START_BYTES);
    }
    private static EnvelopeMaterial envelopeSloStartDecode(boolean due){
        final long n=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_START_BYTES,id=envelopeSloStartIdentityBytes(due),endpoint=2+11+11+34;
        final var parsed=envelopeSloRead(n,1,32,1,1,1,id,32,endpoint,10,32);
        var arguments=envelopeHold(envelopeArray(32),envelopeNested(id,envelopeSloEventIdentityDecode(due)));
        arguments=envelopeHold(arguments,envelopeArray(32));arguments=envelopeHold(arguments,envelopeNested(endpoint,envelopeSloEndpointDecode()));
        arguments=envelopeHold(arguments,envelopeArray(32));
        final var sampleHash=envelopeHash(envelopeCall(envelopeSloEventIdentityCanonical(due),envelopeArray(id+4),0),
                "nereus-delay-slo-sample\0".length()+32L); // this.objectiveDigest is borrowed from the constructor's fixed copy.
        var constructor=envelopeHold(envelopeRepeat(envelopeArray(32),2,true),envelopeReturn(sampleHash,0));
        if(due)constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeSloDueIdentityAccess(),2,false));
        constructor=envelopeHold(constructor,envelopeHash(envelopeSloStartFields(due),"nereus-delay-slo-start\0".length()));
        return envelopeSloDecoded(parsed,arguments,constructor,arguments.held-3L*32,envelopeSloStartCanonical(due));
    }
    private static long envelopeSloOutboxBytes(boolean due){
        return 2+6L+com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_START_BYTES+6L
                +com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES+34;
    }
    private static EnvelopeMaterial envelopeSloOutboxFields(boolean due){
        return envelopeWritten(envelopeDrop(envelopeSloStartCanonical(due),envelopeSloFinalCanonical()),envelopeSloOutboxBytes(due)-34);
    }
    private static EnvelopeMaterial envelopeSloOutboxCanonical(boolean due){return envelopeWritten(envelopeSloOutboxFields(due),envelopeSloOutboxBytes(due));}
    private static EnvelopeMaterial envelopeSloOutboxDecode(boolean due){
        final long n=envelopeSloOutboxBytes(due),start=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_START_BYTES;
        final var parsed=envelopeSloRead(n,1,start,com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES,32);
        var args=envelopeHold(envelopeNested(start,envelopeSloStartDecode(due)),envelopeNested(
                com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES,envelopeSloFinalDecode()));
        args=envelopeHold(args,envelopeArray(32));
        final var constructor=envelopeTemporary(envelopeHash(envelopeSloOutboxFields(due),"nereus-delay-slo-outbox\0".length()),
                envelopeRepeat(envelopeArray(32),2,false)); // Final.validateAgainst clones both exact Start hashes.
        return envelopeSloDecoded(parsed,args,constructor,args.held-32,envelopeSloOutboxCanonical(due));
    }
    private static EnvelopeMaterial envelopeSloAnyOutboxCanonical(){return envelopeChoice(envelopeSloOutboxCanonical(false),envelopeSloOutboxCanonical(true));}
    private static EnvelopeMaterial envelopeSloAnyOutboxDecode(){return envelopeChoice(envelopeSloOutboxDecode(false),envelopeSloOutboxDecode(true));}
    private static EnvelopeMaterial envelopeSloObjectiveCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_OBJECTIVE_BYTES;
        return envelopeWritten(envelopeArray(n-34),n);
    }
    private static EnvelopeMaterial envelopeSloObjectiveDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_OBJECTIVE_BYTES;
        final long exclusions=com.nereusstream.delay.protocol.DueExclusionReason.values().length;
        final var parsed=envelopeSloRead(n,1,1,1,1,1,10,10,10,10,10,1,exclusions,10,32,32);
        final var constructor=envelopeHold(envelopeArray(32),envelopeHash(envelopeArray(n-34),"nereus-delay-slo-objective\0".length()));
        return envelopeSloDecoded(parsed,envelopeRepeat(envelopeArray(32),2,true),constructor,0,envelopeSloObjectiveCanonical());
    }
    private static EnvelopeMaterial envelopeSloDeclarationCanonical(){
        return envelopeWritten(envelopeSloObjectiveCanonical(),com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeSloDeclarationDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES;
        var parsed=envelopeSloRead(n,1,com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_OBJECTIVE_BYTES,1);
        parsed=envelopeHold(parsed,envelopeArray(com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_OBJECTIVE_BYTES));
        final var args=envelopeSloObjectiveDecode(); // objective is a retained raw local in Declaration.decode
        return envelopeSloDecoded(parsed,args,envelopeReturn(envelopeSloObjectiveCanonical(),0),args.held,envelopeSloDeclarationCanonical());
    }
    private static EnvelopeMaterial envelopeSloScalarDecode(long n,long raw,long fields){
        return envelopeReturn(envelopeTemporary(envelopeProtoRead(n,raw,10,fields),envelopeArray(n)),0);
    }
    private static EnvelopeMaterial envelopeSloReferenceCanonical(){return envelopeArray(com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeSloReferenceDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES;
        return envelopeSloDecoded(envelopeSloRead(n,1,32,10,32),envelopeRepeat(envelopeArray(32),2,true),
                envelopeRepeat(envelopeAssigned(32),2,true),0,envelopeSloReferenceCanonical());
    }
    private static EnvelopeMaterial envelopeSloScopeReferenceCanonical(){return envelopeArray(com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeSloScopeReferenceDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES;
        return envelopeSloDecoded(envelopeSloRead(n,1,32,10,32),envelopeRepeat(envelopeArray(32),2,true),
                envelopeRepeat(envelopeAssigned(32),2,true),0,envelopeSloScopeReferenceCanonical());
    }
    private static EnvelopeMaterial envelopeSloReasonDecode(){
        final var args=envelopeRepeat(envelopeArray(32),2,true);
        return envelopeSloDecoded(envelopeSloRead(80,1,32,32),args,
                envelopeRepeat(envelopeArray(32),2,true),0,envelopeArray(80));
    }
    private static EnvelopeMaterial envelopeSloPolicyFields(boolean epoch){
        final long declaration=com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES;
        final long group=com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_DECLARATIONS*(declaration+8);
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeWritten(envelopeRepeat(envelopeSloDeclarationCanonical(),
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_DECLARATIONS,false),group));
        fields=envelopeDrop(fields,envelopeArray(com.nereusstream.delay.protocol.TargetSloObservationPolicy.Limits.MAX_CANONICAL_BYTES));
        fields=envelopeDrop(fields,envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES));
        if(epoch)fields=envelopeDrop(fields,envelopeSloReferenceCanonical());
        return envelopeWritten(fields,(epoch?com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_V1_CANONICAL_BYTES)-34L);
    }
    private static EnvelopeMaterial envelopeSloPolicyCanonical(boolean epoch){
        return envelopeSloHashed(envelopeSloPolicyFields(epoch),epoch?com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_V1_CANONICAL_BYTES,
                epoch?"nereus-delay-target-slo-observation-epoch-policy\0":"nereus-delay-target-slo-observation-policy\0");
    }
    private static EnvelopeMaterial envelopeSloStateFields(){
        return envelopeWritten(envelopeDrop(envelopeScopeCanonical(),envelopeMutationCanonical(false)),
                com.nereusstream.delay.protocol.TargetSloObservationState.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeSloStateCanonical(){return envelopeSloHashed(envelopeSloStateFields(),
            com.nereusstream.delay.protocol.TargetSloObservationState.MAX_CANONICAL_BYTES,"nereus-delay-target-slo-observation-state\0");}
    private static EnvelopeMaterial envelopeSloStateDecode(){
        final var parsed=envelopeSloRead(com.nereusstream.delay.protocol.TargetSloObservationState.MAX_CANONICAL_BYTES,
                1,TargetQuotaScope.MAX_CANONICAL_BYTES,16,10,1,32,32,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,32);
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeRepeat(envelopeArray(32),2,true));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),2,true));
        return envelopeSloDecoded(parsed,args,constructor,args.held-80,envelopeSloStateCanonical(),
                envelopeComparison(envelopeHash(envelopeSloStateFields(),"nereus-delay-target-slo-observation-state\0".length()),envelopeArray(32)));
    }
    private static EnvelopeMaterial envelopeSloWriterContractFields(){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_SERVICE_BYTES));
        fields=envelopeDrop(fields,envelopeArray(com.nereusstream.delay.protocol.TargetSloWriterContract.Limits.MAX_CANONICAL_BYTES));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeSloWriterContractCanonical(){return envelopeSloHashed(envelopeSloWriterContractFields(),
            com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_CANONICAL_BYTES,"nereus-delay-target-slo-writer-contract\0");}
    private static EnvelopeMaterial envelopeSloWriterContractDecode(){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        final var parsed=envelopeSloRead(com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_CANONICAL_BYTES,
                1,TargetQuotaScope.MAX_CANONICAL_BYTES,16,source,com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_SERVICE_BYTES,
                5,10,10,com.nereusstream.delay.protocol.TargetSloWriterContract.Limits.MAX_CANONICAL_BYTES,32,32);
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeNested(source,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_SERVICE_BYTES,
                envelopeAuthorDecode(com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_SERVICE_BYTES)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloWriterContract.Limits.MAX_CANONICAL_BYTES,
                envelopeSloScalarDecode(com.nereusstream.delay.protocol.TargetSloWriterContract.Limits.MAX_CANONICAL_BYTES,1+5+4*10,6)));
        args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));
        constructor=envelopeTemporary(constructor,envelopeSourceBounded());
        constructor=envelopeTemporary(constructor,envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_SERVICE_BYTES));
        return envelopeSloDecoded(parsed,args,constructor,args.held-48,envelopeSloWriterContractCanonical(),
                envelopeComparison(envelopeHash(envelopeSloWriterContractFields(),"nereus-delay-target-slo-writer-contract\0".length()),envelopeArray(32)));
    }
    private static long envelopeSloRequestBytes(boolean writer,boolean enable,boolean epoch){
        if(writer)return 2+(6L+TargetQuotaScope.MAX_CANONICAL_BYTES)+18+(enable?
                6L+com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_CANONICAL_BYTES+34:
                6L+com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES+6L+
                        com.nereusstream.delay.protocol.TargetSloWriterControlRequest.MAX_FENCE_INPUT_BYTES+6L+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES);
        if(!enable)return 2+(6L+TargetQuotaScope.MAX_CANONICAL_BYTES)+18+11+34+6+80;
        return epoch?com.nereusstream.delay.protocol.TargetSloObservationControlRequest.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationControlRequest.MAX_V1_CANONICAL_BYTES;
    }
    private static EnvelopeMaterial envelopeSloRequestCanonical(boolean writer,boolean enable,boolean epoch){
        var fields=envelopeScopeCanonical();
        if(writer){
            if(enable)fields=envelopeDrop(fields,envelopeSloWriterContractCanonical());
            else{
                fields=envelopeDrop(fields,envelopeSloReferenceCanonical());
                fields=envelopeDrop(fields,envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetSloWriterControlRequest.MAX_FENCE_INPUT_BYTES));
                fields=envelopeDrop(fields,envelopeMutationCanonical(false));
            }
        }else fields=envelopeDrop(fields,enable?envelopeSloPolicyCanonical(epoch):envelopeArray(80));
        return envelopeWritten(fields,envelopeSloRequestBytes(writer,enable,epoch));
    }
    private static EnvelopeMaterial envelopeSloRequestDecode(boolean writer,boolean enable,boolean epoch){
        final long n=envelopeSloRequestBytes(writer,enable,epoch),scope=TargetQuotaScope.MAX_CANONICAL_BYTES;
        final long nested=writer?(enable?com.nereusstream.delay.protocol.TargetSloWriterContract.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES):
                enable?(epoch?com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_V1_CANONICAL_BYTES):80;
        final var parsed=writer&&!enable?envelopeSloRead(n,1,scope,16,nested,
                com.nereusstream.delay.protocol.TargetSloWriterControlRequest.MAX_FENCE_INPUT_BYTES,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES):
                envelopeSloRead(n,1,scope,16,writer?0:10,32,nested);
        var args=envelopeNested(scope,envelopeScopeDecode());args=envelopeHold(args,envelopeArray(16));
        if(writer){
            if(enable){args=envelopeHold(args,envelopeNested(nested,envelopeSloWriterContractDecode()));args=envelopeHold(args,envelopeArray(32));}
            else{
                args=envelopeHold(args,envelopeNested(nested,envelopeSloReferenceDecode()));
                final long input=com.nereusstream.delay.protocol.TargetSloWriterControlRequest.MAX_FENCE_INPUT_BYTES;
                final long body=com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES;
                final var fence=envelopeSystemDecode(input,body,input,envelopeTimeFenceDecode(),
                        envelopeCall(envelopeTimeFenceDecode(),envelopeReturn(envelopeArray(32),0),0),false);
                args=envelopeHold(args,envelopeNested(input,fence));
                args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
            }
        }else{
            args=envelopeHold(args,envelopeArray(32));
            args=envelopeHold(args,enable?envelopeNested(nested,envelopeSloPolicyDecode(epoch)):
                    envelopeNested(80,envelopeSloReasonDecode()));
        }
        var constructor=envelopeHold(envelopeAssigned(16),enable||!writer?envelopeAssigned(32):envelopeEmpty());
        if(writer&&!enable){
            final long fence=com.nereusstream.delay.protocol.TargetSloWriterControlRequest.MAX_FENCE_INPUT_BYTES;
            constructor=envelopeTemporary(constructor,envelopeSystemCanonical(fence));
            constructor=envelopeTemporary(constructor,envelopeHash(envelopeSystemCanonical(
                    fence),0));
            constructor=envelopeTemporary(constructor,envelopeArray(32));
            constructor=envelopeTemporary(constructor,envelopeNested(com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES,envelopeTimeFenceDecode()));
            constructor=envelopeTemporary(constructor,envelopeNested(fence,envelopeAuthorDecode(fence)));
            constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopeArray(32)));
            constructor=envelopeTemporary(constructor,envelopeArray(TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES));
        }
        if(enable)constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(16),writer?3:1,false));
        return envelopeSloDecoded(parsed,args,constructor,args.held-16-(enable||!writer?32:0),envelopeSloRequestCanonical(writer,enable,epoch));
    }
    private static long envelopeSloControlBodyBytes(boolean writer,boolean epoch,boolean scoped){
        return writer?com.nereusstream.delay.protocol.TargetSloWriterControlBody.MAX_CANONICAL_BYTES:
                epoch||scoped?com.nereusstream.delay.protocol.TargetSloObservationControlBody.MAX_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationControlBody.MAX_V1_CANONICAL_BYTES;
    }
    private static EnvelopeMaterial envelopeSloControlBodyCanonical(boolean writer,boolean enable,boolean epoch,boolean scoped){
        final var request=envelopeSloRequestCanonical(writer,enable,epoch);
        var semantic=envelopeHold(envelopeArrays(scoped?4:2,scoped?2:1),request);
        if(scoped)semantic=envelopeHold(semantic,envelopeSloScopeReferenceCanonical());
        final var hash=envelopeHash(semantic,(writer?"nereus-delay-target-slo-writer-control\0":"nereus-delay-target-slo-observation-control\0").length());
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        fields=envelopeDrop(fields,hash);fields=envelopeDrop(fields,envelopeWritten(request,envelopeSloRequestBytes(writer,enable,epoch)+8));
        if(scoped)fields=envelopeDrop(fields,envelopeSloScopeReferenceCanonical());
        return envelopeWritten(fields,envelopeSloControlBodyBytes(writer,epoch,scoped));
    }
    private static EnvelopeMaterial envelopeSloControlBodyDecode(boolean writer,boolean enable,boolean epoch,boolean scoped){
        final long request=envelopeSloRequestBytes(writer,enable,epoch),n=envelopeSloControlBodyBytes(writer,epoch,scoped),payload=request+8;
        var parsed=envelopeSloControlBodyRead(writer,enable,epoch,scoped);
        // decode validates and keeps shard/ref raw locals before calling their typed decoders.
        parsed=envelopeHold(parsed,envelopeCall(envelopeArray(24),envelopeReturn(envelopeSloRead(24,16,5),0),24));
        parsed=envelopeHold(parsed,envelopeCall(envelopeArray(74),envelopeReturn(envelopeSloRead(74,32,5,32),0),74));
        var args=envelopeShardSubjectDecode();args=envelopeHold(args,envelopeControlRefDecode());
        parsed=envelopeHold(parsed,envelopeNested(payload,envelopeProtoRead(payload,request,request,1)));
        final var decodedRequest=envelopeNested(request,envelopeSloRequestDecode(writer,enable,epoch));
        args=envelopeHold(args,decodedRequest);if(scoped)args=envelopeHold(args,envelopeNested(
                com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES,envelopeSloScopeReferenceDecode()));
        // assigned(controlRef.operationId()) uses the getter clone, a zero guard and an owned copy;
        // the returned assigned copy is only a validation temporary in these Body constructors.
        var constructor=envelopeReturn(envelopeCall(envelopeArray(32),envelopeAssigned(32),0),0);
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(
                envelopeSloRequestCanonical(writer,enable,epoch),request,writer?(enable?30:31):(enable?28:29))));
        constructor=envelopeReturn(constructor,0);
        var semantic=envelopeHold(envelopeArrays(scoped?4:2,scoped?2:1),envelopeSloRequestCanonical(writer,enable,epoch));
        if(scoped)semantic=envelopeHold(semantic,envelopeSloScopeReferenceCanonical());
        return envelopeSloDecoded(parsed,args,constructor,args.held,envelopeSloControlBodyCanonical(writer,enable,epoch,scoped),
                envelopeComparison(envelopeArray(32),envelopeHash(semantic,
                (writer?"nereus-delay-target-slo-writer-control\0":"nereus-delay-target-slo-observation-control\0").length())));
    }
    private static EnvelopeMaterial envelopeSloControlBodyRead(boolean writer,boolean enable,boolean epoch,boolean scoped){
        return envelopeSloRead(envelopeSloControlBodyBytes(writer,epoch,scoped),24,1,10,74,1,1,32,1,
                envelopeSloRequestBytes(writer,enable,epoch)+8,scoped?
                        com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES:0);
    }
    private static EnvelopeMaterial envelopeSloObservationBodyCanonical(){
        return envelopeChoice(envelopeChoice(envelopeSloControlBodyCanonical(false,true,false,false),
                envelopeChoice(envelopeSloControlBodyCanonical(false,true,true,false),envelopeSloControlBodyCanonical(false,true,true,true))),
                envelopeChoice(envelopeSloControlBodyCanonical(false,false,false,false),envelopeSloControlBodyCanonical(false,false,true,true)));
    }
    private static EnvelopeMaterial envelopeSloObservationBodyDecode(){
        return envelopeChoice(envelopeChoice(envelopeSloControlBodyDecode(false,true,false,false),
                envelopeChoice(envelopeSloControlBodyDecode(false,true,true,false),envelopeSloControlBodyDecode(false,true,true,true))),
                envelopeChoice(envelopeSloControlBodyDecode(false,false,false,false),envelopeSloControlBodyDecode(false,false,true,true)));
    }
    private static EnvelopeMaterial envelopeSloObservationBodyIdentity(){return envelopeCall(envelopeSloObservationBodyDecode(),envelopeControlLogicalIdentity(),0);}
    private static EnvelopeMaterial envelopeSloObservationBodyValidation(){
        final var legacy=envelopeSystemControlValidation(com.nereusstream.delay.protocol.TargetSloObservationControlBody.MAX_V1_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetSloObservationControlRequest.MAX_V1_CANONICAL_BYTES+8L);
        final var scoped=envelopeDrop(envelopeSloControlBodyRead(false,true,true,true),envelopeSloControlBodyDecode(false,true,true,true));
        return envelopeChoice(legacy,envelopeChoice(scoped,envelopeDrop(envelopeSloControlBodyRead(false,false,true,true),
                envelopeSloControlBodyDecode(false,false,true,true))));
    }
    private static EnvelopeMaterial envelopeSloWriterBodyCanonical(){return envelopeChoice(
            envelopeChoice(envelopeSloControlBodyCanonical(true,true,true,false),envelopeSloControlBodyCanonical(true,true,true,true)),
            envelopeChoice(envelopeSloControlBodyCanonical(true,false,true,false),envelopeSloControlBodyCanonical(true,false,true,true)));}
    private static EnvelopeMaterial envelopeSloWriterBodyDecode(){return envelopeChoice(
            envelopeChoice(envelopeSloControlBodyDecode(true,true,true,false),envelopeSloControlBodyDecode(true,true,true,true)),
            envelopeChoice(envelopeSloControlBodyDecode(true,false,true,false),envelopeSloControlBodyDecode(true,false,true,true)));}
    private static EnvelopeMaterial envelopeSloWriterBodyIdentity(){return envelopeCall(envelopeSloWriterBodyDecode(),envelopeControlLogicalIdentity(),0);}
    private static EnvelopeMaterial envelopeSloWriterBodyValidation(){
        final var unscoped=envelopeSystemControlValidation(com.nereusstream.delay.protocol.TargetSloWriterControlBody.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetSloWriterControlRequest.MAX_CANONICAL_BYTES+8L);
        return envelopeChoice(unscoped,envelopeChoice(envelopeDrop(envelopeSloControlBodyRead(true,true,true,true),
                envelopeSloControlBodyDecode(true,true,true,true)),envelopeDrop(envelopeSloControlBodyRead(true,false,true,true),
                envelopeSloControlBodyDecode(true,false,true,true))));
    }
    private static long envelopeSloControlInputBytes(boolean writer,boolean epoch){
        return writer?com.nereusstream.delay.protocol.TargetSloWriterEpoch.Control.MAX_INPUT_BYTES:
                epoch?com.nereusstream.delay.protocol.TargetSloObservationGeneration.Control.MAX_INPUT_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationGeneration.Control.MAX_V1_INPUT_BYTES;
    }
    private static EnvelopeMaterial envelopeSloControlInputDecode(boolean writer,boolean enable,boolean epoch,boolean scoped){
        final long body=envelopeSloControlBodyBytes(writer,epoch,scoped),request=envelopeSloRequestBytes(writer,enable,epoch);
        final var typed=envelopeSloControlBodyDecode(writer,enable,epoch,scoped);
        final var validation=scoped?envelopeDrop(envelopeSloControlBodyRead(writer,enable,epoch,true),typed):
                envelopeSystemControlValidation(body,request+8);
        return envelopeSystemDecode(envelopeSloControlInputBytes(writer,epoch||scoped),body,104,validation,
                envelopeCall(typed,envelopeControlLogicalIdentity(),0),true);
    }
    private static long envelopeSloAppliedControlBytes(boolean writer,boolean epoch){
        return writer?com.nereusstream.delay.protocol.TargetSloWriterEpoch.Control.MAX_CANONICAL_BYTES:
                epoch?com.nereusstream.delay.protocol.TargetSloObservationGeneration.Control.MAX_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationGeneration.Control.MAX_V1_CANONICAL_BYTES;
    }
    private static EnvelopeMaterial envelopeSloAppliedControlCanonical(boolean writer,boolean enable,boolean epoch){
        final var fields=envelopeDrop(envelopeSystemCanonical(envelopeSloControlInputBytes(writer,epoch)),envelopeMutationCanonical(false));
        return envelopeWritten(fields,envelopeSloAppliedControlBytes(writer,epoch));
    }
    private static EnvelopeMaterial envelopeSloAppliedControlDecode(boolean writer,boolean enable,boolean epoch,boolean scoped){
        final long n=envelopeSloAppliedControlBytes(writer,epoch||scoped),input=envelopeSloControlInputBytes(writer,epoch||scoped);
        var parsed=envelopeSloRead(n,1,input,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES);
        parsed=envelopeHold(parsed,envelopeArray(input)); // Control.decode retains its input local through the canonical recheck.
        var args=envelopeSloControlInputDecode(writer,enable,epoch,scoped);
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        final var typed=envelopeSloControlBodyDecode(writer,enable,epoch,scoped);
        var checks=envelopeNested(envelopeSloControlBodyBytes(writer,epoch,scoped),typed);
        checks=envelopeTemporary(checks,envelopeComparison(envelopeArray(32),envelopeControlLogicalIdentity()));
        checks=envelopeTemporary(checks,envelopeSystemCanonical(input));
        checks=envelopeTemporary(checks,envelopeComparison(envelopeArray(32),envelopeHash(envelopeSystemCanonical(input),0)));
        if(writer)checks=envelopeTemporary(checks,envelopeNested(104,envelopeAuthorDecode(104)));
        checks=envelopeReturn(checks,0);
        return envelopeSloDecoded(parsed,args,checks,args.held,envelopeSloAppliedControlCanonical(writer,enable,epoch||scoped));
    }
    private static EnvelopeMaterial envelopeSloCountsDecode(int schema){
        final long count=schema==1?4:schema==2?6:8;
        return envelopeSloScalarDecode(2+count*11,1+count*10,1+count);
    }
    private static EnvelopeMaterial envelopeSloCountsDecode(){
        return envelopeChoice(envelopeChoice(envelopeSloCountsDecode(1),envelopeSloCountsDecode(2)),envelopeSloCountsDecode(3));
    }
    private static EnvelopeMaterial envelopeSloGenerationFields(boolean epoch,boolean stopped){
        var fields=envelopeSloAppliedControlCanonical(false,true,epoch);
        // A scoped STOP is schema3 even when its generation's frozen ENABLE policy is legacy.
        if(stopped)fields=envelopeDrop(fields,envelopeSloAppliedControlCanonical(false,false,true));
        fields=envelopeDrop(fields,envelopeArray(com.nereusstream.delay.protocol.TargetSloObservationGeneration.Counts.MAX_CANONICAL_BYTES));
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        return envelopeWritten(fields,(epoch?com.nereusstream.delay.protocol.TargetSloObservationGeneration.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationGeneration.MAX_V1_CANONICAL_BYTES)-34L);
    }
    private static EnvelopeMaterial envelopeSloGenerationCanonical(boolean epoch,boolean stopped){
        final var image=envelopeSloHashed(envelopeSloGenerationFields(epoch,stopped),epoch?
                com.nereusstream.delay.protocol.TargetSloObservationGeneration.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationGeneration.MAX_V1_CANONICAL_BYTES,"nereus-delay-target-slo-observation-generation\0");
        // The encoded local remains live while canonicalBytes calls policy()->enable.body() for its bound.
        return envelopeTemporary(image,envelopeNested(envelopeSloControlBodyBytes(false,epoch,epoch),
                envelopeSloControlBodyDecode(false,true,epoch,epoch)));
    }
    private static EnvelopeMaterial envelopeSloGenerationDecode(boolean epoch,boolean stopped){
        final long n=epoch?com.nereusstream.delay.protocol.TargetSloObservationGeneration.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationGeneration.MAX_V1_CANONICAL_BYTES,control=envelopeSloAppliedControlBytes(false,epoch);
        final long stopControl=envelopeSloAppliedControlBytes(false,true);
        final long raw=1+control+(stopped?stopControl:0)+com.nereusstream.delay.protocol.TargetSloObservationGeneration.Counts.MAX_CANONICAL_BYTES
                +TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+32;
        // The two Control field arrays share the containing generation image's aggregate byte budget.
        final var parsed=envelopeProtoRead(n,Math.min(n,raw),Math.min(n,stopped?Math.max(control,stopControl):control),stopped?6:5);
        final var enabled=envelopeChoice(envelopeSloAppliedControlDecode(false,true,epoch,false),
                epoch?envelopeSloAppliedControlDecode(false,true,true,true):envelopeSloAppliedControlDecode(false,true,false,false));
        var args=envelopeNested(control,enabled);
        if(stopped)args=envelopeHold(args,envelopeNested(stopControl,envelopeChoice(envelopeSloAppliedControlDecode(false,false,epoch,false),
                envelopeSloAppliedControlDecode(false,false,epoch,true))));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationGeneration.Counts.MAX_CANONICAL_BYTES,envelopeSloCountsDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var checks=envelopeNested(envelopeSloControlBodyBytes(false,epoch,epoch),envelopeSloControlBodyDecode(false,true,epoch,epoch));
        checks=envelopeTemporary(checks,envelopeRepeat(envelopeSourceBounded(),2,false));
        checks=envelopeTemporary(checks,envelopeSloAtOrBefore());
        if(stopped){
            // request and stopped are constructor locals kept through the prior-State digest check.
            checks=envelopeHold(checks,envelopeNested(envelopeSloControlBodyBytes(false,epoch,true),envelopeSloControlBodyDecode(false,false,epoch,true)));
            checks=envelopeTemporary(checks,envelopeRepeat(envelopeArray(16),2,true));
            checks=envelopeTemporary(checks,envelopeNested(envelopeSloControlBodyBytes(false,epoch,epoch),envelopeSloControlBodyDecode(false,true,epoch,epoch)));
            checks=envelopeTemporary(checks,envelopeControlLogicalIdentity());
            final var priorArgs=envelopeHold(envelopeArray(16),envelopeHold(envelopeControlLogicalIdentity(),envelopeHash(
                    envelopeHold(envelopeSloAppliedControlCanonical(false,true,epoch),envelopeArray(0)),
                    "nereus-delay-target-slo-observation-generation-control\0".length())));
            final var priorCtor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),2,true));
            final var prior=envelopeCall(priorArgs,priorCtor,0);
            checks=envelopeTemporary(checks,envelopeCall(prior,envelopeComparison(envelopeHash(envelopeSloStateFields(),
                    "nereus-delay-target-slo-observation-state\0".length()),envelopeArray(32)),0));
        }
        checks=envelopeReturn(checks,0);
        return envelopeSloDecoded(parsed,args,checks,args.held,envelopeSloGenerationCanonical(epoch,stopped),
                envelopeComparison(envelopeHash(envelopeSloGenerationFields(epoch,stopped),"nereus-delay-target-slo-observation-generation\0".length()),envelopeArray(32)));
    }
    private static EnvelopeMaterial envelopeSloWriterFields(boolean closed){
        var fields=envelopeSloAppliedControlCanonical(true,true,true);
        if(closed)fields=envelopeDrop(fields,envelopeSloAppliedControlCanonical(true,false,true));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetSloWriterEpoch.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeSloWriterCanonical(boolean closed){return envelopeSloHashed(envelopeSloWriterFields(closed),
            com.nereusstream.delay.protocol.TargetSloWriterEpoch.MAX_CANONICAL_BYTES,"nereus-delay-target-slo-writer-epoch\0");}
    private static EnvelopeMaterial envelopeSloWriterDecode(boolean closed){
        final long n=com.nereusstream.delay.protocol.TargetSloWriterEpoch.MAX_CANONICAL_BYTES,control=envelopeSloAppliedControlBytes(true,true);
        final var parsed=envelopeSloRead(n,1,control,closed?control:0,32);
        var args=envelopeNested(control,envelopeSloAppliedControlDecode(true,true,true,true));
        if(closed)args=envelopeHold(args,envelopeNested(control,envelopeSloAppliedControlDecode(true,false,true,true)));
        var checks=envelopeNested(envelopeSloControlBodyBytes(true,true,true),envelopeSloControlBodyDecode(true,true,true,true));
        checks=envelopeTemporary(checks,envelopeSourceBounded());
        if(closed){
            checks=envelopeHold(checks,envelopeNested(envelopeSloControlBodyBytes(true,true,true),envelopeSloControlBodyDecode(true,false,true,true)));
            checks=envelopeTemporary(checks,envelopeRepeat(envelopeArray(16),2,true));
            final var registerBody=envelopeNested(envelopeSloControlBodyBytes(true,true,true),envelopeSloControlBodyDecode(true,true,true,true));
            final var referenceArgs=envelopeHold(envelopeArray(32),envelopeHash(envelopeSloAppliedControlCanonical(true,true,true),
                    "nereus-delay-target-slo-applied-writer-contract\0".length()));
            final var reference=envelopeCall(referenceArgs,envelopeRepeat(envelopeAssigned(32),2,true),0);
            checks=envelopeTemporary(checks,envelopeCall(registerBody,envelopeCall(reference,
                    envelopeComparison(envelopeSloReferenceCanonical(),envelopeSloReferenceCanonical()),0),0));
            checks=envelopeTemporary(checks,envelopeRepeat(envelopeSourceBounded(),2,false));
            checks=envelopeTemporary(checks,envelopeNested(com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES,envelopeTimeFenceDecode()));
        }
        checks=envelopeReturn(checks,0);
        return envelopeSloDecoded(parsed,args,checks,args.held,envelopeSloWriterCanonical(closed),
                envelopeComparison(envelopeHash(envelopeSloWriterFields(closed),"nereus-delay-target-slo-writer-epoch\0".length()),envelopeArray(32)));
    }
    private static EnvelopeMaterial envelopeSloTimeCanonical(){return envelopeArray(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES);}
    private static EnvelopeMaterial envelopeSloBoundedTime(){
        return envelopeReturn(envelopeDrop(envelopeArray(TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES),envelopeSloTimeCanonical()),0);
    }
    private static EnvelopeMaterial envelopeSloAtOrBefore(){
        // Equal stamps take both the exact physical-Source comparison and TargetQuotaMutation.equals.
        var checks=envelopeSourceBounded();
        checks=envelopeDrop(checks,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        return envelopeDrop(checks,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
    }
    private static EnvelopeMaterial envelopeSloTimeDecode(){
        final long n=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,id=TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES;
        final var parsed=envelopeSloRead(n,10,10,1,id,10,10,10,32,5,64);
        var args=envelopeHold(envelopeArray(id),envelopeArray(32));args=envelopeHold(args,envelopeArray(64));
        return envelopeSloDecoded(parsed,args,envelopeHold(envelopeArray(id),
                envelopeHold(envelopeArray(32),envelopeArray(64))),0,envelopeSloTimeCanonical());
    }
    private static EnvelopeMaterial envelopeSloEligibilityFields(){
        return envelopeWritten(envelopeDrop(envelopeMessageLocatorCanonical(),envelopeSourceCanonical()),
                com.nereusstream.delay.protocol.TargetSloMessageEligibility.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeSloEligibilityCanonical(){return envelopeSloHashed(envelopeSloEligibilityFields(),
            com.nereusstream.delay.protocol.TargetSloMessageEligibility.MAX_CANONICAL_BYTES,"nereus-delay-target-slo-message-eligibility\0");}
    private static EnvelopeMaterial envelopeSloEligibilityDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloMessageEligibility.MAX_CANONICAL_BYTES;
        final long locator=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES,source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        final var parsed=envelopeSloRead(n,1,locator,10,10,10,1,source,32,32);
        var args=envelopeNested(locator,envelopeMessageLocatorDecode());args=envelopeHold(args,envelopeNested(source,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeArray(32));
        final var ctor=envelopeHold(envelopeAssigned(32),envelopeReturn(envelopeSourceBounded(),0));
        return envelopeSloDecoded(parsed,args,ctor,args.held-32,envelopeSloEligibilityCanonical(),
                envelopeComparison(envelopeHash(envelopeSloEligibilityFields(),"nereus-delay-target-slo-message-eligibility\0".length()),envelopeArray(32)));
    }
    private static EnvelopeMaterial envelopeSloAdmissionInputDecode(){
        final long input=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES;
        final long body=com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_SUPPORTED_BYTES;
        return envelopeSystemDecode(input,body,TargetResultRecord.MAX_AUTHOR_BYTES,envelopePublishAdmissionDecode(),
                envelopeCall(envelopePublishAdmissionDecode(),envelopeReturn(envelopeArray(32),0),0),false);
    }
    private static EnvelopeMaterial envelopeSloSampleAdmissionField(int schema){
        if(schema==3)return envelopeWritten(envelopeSloEligibilityCanonical(),com.nereusstream.delay.protocol.TargetSloMessageEligibility.MAX_CANONICAL_BYTES+8L);
        if(schema==2)return envelopeWritten(envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES),
                com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES+8L);
        return envelopeArray(0);
    }
    private static EnvelopeMaterial envelopeSloSampleBindingFields(int schema){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeSloDeclarationCanonical());fields=envelopeDrop(fields,envelopeResultCanonical());
        final long bytes=2+(6L+TargetQuotaScope.MAX_CANONICAL_BYTES)+18+11+34+(6L+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES)
                +34+(6L+com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES)
                +(6L+TargetResultRecord.MAX_CANONICAL_BYTES);
        return envelopeWritten(fields,bytes);
    }
    private static EnvelopeMaterial envelopeSloSampleBindingDigest(int schema){
        var fields=envelopeHold(envelopeSloSampleBindingFields(schema),envelopeSloStartCanonical(schema==2||schema==3));
        fields=envelopeHold(fields,envelopeSloSampleAdmissionField(schema));
        return envelopeHash(fields,"nereus-delay-target-slo-sample-binding\0".length());
    }
    private static EnvelopeMaterial envelopeSloSampleFields(int schema){
        final long binding=2+(6L+TargetQuotaScope.MAX_CANONICAL_BYTES)+18+11+34+(6L+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES)
                +34+(6L+com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES)+(6L+TargetResultRecord.MAX_CANONICAL_BYTES);
        var nested=envelopeDrop(envelopeSloOutboxCanonical(schema==2||schema==3),envelopeMutationCanonical(false));
        nested=envelopeDrop(nested,envelopeSloSampleBindingDigest(schema));
        final long tail=6L+envelopeSloOutboxBytes(schema==2||schema==3)+6L+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+34;
        return envelopeCall(envelopeHold(envelopeSloSampleBindingFields(schema),envelopeWritten(nested,tail)),envelopeArray(binding+tail),0);
    }
    private static EnvelopeMaterial envelopeSloSampleDigest(int schema){
        return envelopeHash(envelopeHold(envelopeSloSampleFields(schema),envelopeSloSampleAdmissionField(schema)),"nereus-delay-target-slo-sample\0".length());
    }
    private static EnvelopeMaterial envelopeSloSampleCanonical(int schema){
        final var suffix=envelopeWritten(envelopeSloSampleDigest(schema),34);
        final var parts=envelopeHold(envelopeHold(envelopeSloSampleFields(schema),suffix),envelopeSloSampleAdmissionField(schema));
        return envelopeCall(parts,envelopeArray(schema==1||schema==4?
                com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_COMMAND_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_CANONICAL_BYTES),0);
    }
    private static EnvelopeMaterial envelopeSloSampleDecode(int schema){
        final boolean due=schema==2||schema==3;
        final long n=due?com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_COMMAND_BYTES;
        final long declaration=com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES;
        final long extra=schema==2?com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES:
                schema==3?com.nereusstream.delay.protocol.TargetSloMessageEligibility.MAX_CANONICAL_BYTES:0;
        var parsed=envelopeSloRead(n,1,TargetQuotaScope.MAX_CANONICAL_BYTES,16,10,32,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,
                32,declaration,TargetResultRecord.MAX_CANONICAL_BYTES,envelopeSloOutboxBytes(due),TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,32,32,extra);
        if(schema==2)parsed=envelopeTemporary(parsed,envelopeArray(extra)); // independent decode bound check before the constructor arguments
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArray(16));
        args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeNested(declaration,envelopeSloDeclarationDecode()));
        args=envelopeHold(args,envelopeNested(TargetResultRecord.MAX_CANONICAL_BYTES,envelopeResultDecode()));
        args=envelopeHold(args,envelopeNested(envelopeSloOutboxBytes(due),envelopeSloOutboxDecode(due)));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        if(schema==2)args=envelopeHold(args,envelopeNested(extra,envelopeSloAdmissionInputDecode()));
        else if(schema==3)args=envelopeHold(args,envelopeNested(extra,envelopeSloEligibilityDecode()));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),2,true));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopeArray(32)));
        constructor=envelopeTemporary(constructor,envelopeArray(16));
        constructor=envelopeTemporary(constructor,envelopeSloAtOrBefore());
        if(schema==2){
            final long input=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES;
            constructor=envelopeTemporary(constructor,envelopeSystemCanonical(input));
            constructor=envelopeTemporary(constructor,envelopeComparison(envelopeHash(envelopeSystemCanonical(input),0),envelopeArray(32)));
            constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(TargetResultRecord.MAX_PAYLOAD_BYTES),
                    envelopeCall(envelopeSourceCanonical(),envelopeSystemResultFromCanonical(TargetResultRecord.MAX_AUTHOR_BYTES),0)));
            constructor=envelopeTemporary(constructor,envelopeNested(com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_SUPPORTED_BYTES,envelopePublishAdmissionDecode()));
        }else if(schema==3)constructor=envelopeTemporary(constructor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        // The constructor reconstructs the semantic Start and compares its exact v1 bytes with the supplied outbox.
        final long owned=constructor.held;
        final var expected=schema==2?envelopeCall(envelopeHash(envelopeChoice(envelopeOrdinaryPublicationCanonical(),envelopeNativePublicationCanonical()),0),
                envelopeSloExpectedStart(true),0):schema==3?envelopeCall(envelopeHash(envelopeSloEligibilityFields(),
                "nereus-delay-target-slo-message-eligibility\0".length()),envelopeSloExpectedStart(true),0):envelopeSloExpectedStart(false);
        constructor=envelopeHold(constructor,expected);
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeSloStartCanonical(due),envelopeSloStartCanonical(due)));
        constructor=envelopeTemporary(constructor,envelopeSloStartCanonical(due));
        constructor=envelopeTemporary(constructor,envelopeResultCanonical());constructor=envelopeTemporary(constructor,envelopeSloFinalCanonical());
        constructor=envelopeTemporary(constructor,envelopeArray(32)); // Start.validateAgainst objective digest getter
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(32),2,false));
        constructor=envelopeReturn(constructor,owned);
        final var verification=envelopeDrop(envelopeComparison(envelopeSloSampleBindingDigest(schema),envelopeArray(32)),
                envelopeComparison(envelopeSloSampleDigest(schema),envelopeArray(32)));
        return envelopeSloDecoded(parsed,args,constructor,args.held-80,envelopeSloSampleCanonical(schema),verification);
    }
    /** SloAuthoritativeStartFactory constructs a Start; it does not parse a supplied Start image. */
    private static EnvelopeMaterial envelopeSloExpectedStart(boolean due){
        final long identity=envelopeSloStartIdentityBytes(due),branch=identity-2;
        final var source=due?envelopeEmpty():envelopeSourceCanonical();
        final var branchFields=due?envelopeArray(DelayMessageId.LENGTH):envelopeEmpty();
        final var encodedBranch=envelopeWritten(branchFields,branch);
        // Public event-identity construction validates the branch, encodes its outer oneof and calls decode.
        final var outer=envelopeCall(envelopeSloEventIdentityCanonical(due),envelopeReturn(envelopeSloEventIdentityDecode(due),0),identity);
        final var event=envelopeCall(encodedBranch,envelopeDrop(envelopeSloIdentityBranchValidation(due),outer),0);
        var args=envelopeHold(envelopeArray(32),event); // objective.objectiveDigest() is an owned getter clone.
        args=envelopeHold(args,due?envelopeArray(32):envelopeCall(envelopeHash(envelopeEmpty(),source.held),envelopeArray(32),0));
        final var sampleHash=envelopeHash(envelopeCall(envelopeSloEventIdentityCanonical(due),envelopeArray(identity+4),0),
                "nereus-delay-slo-sample\0".length()+32L);
        // The public Start constructor supplies a sample hash, and its private constructor independently rechecks it.
        var ctor=envelopeRepeat(envelopeArray(32),2,true);
        if(due)ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeSloDueIdentityAccess(),2,false));
        ctor=envelopeTemporary(ctor,sampleHash);
        ctor=envelopeHold(ctor,envelopeHash(envelopeSloStartFields(due),"nereus-delay-slo-start\0".length()));
        ctor=envelopeTemporary(ctor,envelopeArray(32)); // objective.validateStart
        return envelopeCall(source,envelopeCall(args,envelopeCall(sampleHash,ctor,0),event.held+32),0);
    }
    private static EnvelopeMaterial envelopeSloGapCanonical(){
        return envelopeWritten(envelopeRepeat(envelopeResultCanonical(),2,false),
                com.nereusstream.delay.protocol.TargetSloFinalEvidence.RecoveryGap.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeSloGapDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloFinalEvidence.RecoveryGap.MAX_CANONICAL_BYTES;
        final var parsed=envelopeSloRead(n,1,32,32,32,10,1,TargetResultRecord.MAX_CANONICAL_BYTES,TargetResultRecord.MAX_CANONICAL_BYTES,32);
        var args=envelopeRepeat(envelopeArray(32),3,true);args=envelopeHold(args,envelopeRepeat(
                envelopeNested(TargetResultRecord.MAX_CANONICAL_BYTES,envelopeResultDecode()),2,true));args=envelopeHold(args,envelopeArray(32));
        var ctor=envelopeRepeat(envelopeAssigned(32),4,true);
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeArray(32),2,true));
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeArray(16),2,true));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeResultCanonical(),2,false));
        final var expiryKey=envelopeWritten(envelopeSourceCanonical(),com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES+2L);
        final var expiry=envelopeDrop(envelopeComparison(expiryKey,expiryKey),envelopeComparison(envelopeResultCanonical(),envelopeResultCanonical()));
        ctor=envelopeTemporary(ctor,envelopeChoice(expiry,envelopeSloAtOrBefore()));
        return envelopeSloDecoded(parsed,args,ctor,args.held-128,envelopeSloGapCanonical());
    }
    private static EnvelopeMaterial envelopeSloEvidenceCanonical(boolean gap,boolean admission){
        var fields=envelopeDrop(envelopeSloTimeCanonical(),envelopeMutationCanonical(false));
        if(admission){fields=envelopeDrop(fields,envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES));
            fields=envelopeDrop(fields,envelopeMutationCanonical(false));}
        if(gap)fields=envelopeDrop(fields,envelopeSloGapCanonical());
        return envelopeWritten(fields,gap?com.nereusstream.delay.protocol.TargetSloFinalEvidence.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloFinalEvidence.MAX_V1_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeSloEvidenceDecode(boolean gap,boolean admission){
        final long input=com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES;
        final var parsed=envelopeSloRead(gap?com.nereusstream.delay.protocol.TargetSloFinalEvidence.MAX_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloFinalEvidence.MAX_V1_CANONICAL_BYTES,1,1,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,admission?input:0,admission?TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES:0,
                gap?com.nereusstream.delay.protocol.TargetSloFinalEvidence.RecoveryGap.MAX_CANONICAL_BYTES:0);
        var args=envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,envelopeSloTimeDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        if(admission){args=envelopeHold(args,envelopeNested(input,envelopeSloAdmissionInputDecode()));
            args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));}
        if(gap)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloFinalEvidence.RecoveryGap.MAX_CANONICAL_BYTES,envelopeSloGapDecode()));
        var checks=envelopeSloBoundedTime();
        if(admission)checks=envelopeDrop(checks,envelopeSloAtOrBefore());
        if(gap)checks=envelopeDrop(checks,envelopeRepeat(envelopeSloAtOrBefore(),2,false));
        if(admission){checks=envelopeDrop(checks,envelopeSystemCanonical(input));
            checks=envelopeDrop(checks,envelopeComparison(envelopeHash(envelopeSystemCanonical(input),0),envelopeArray(32)));}
        return envelopeSloDecoded(parsed,args,envelopeReturn(checks,0),args.held,envelopeSloEvidenceCanonical(gap,admission));
    }
    private static EnvelopeMaterial envelopeSloAnyEvidenceCanonical(boolean gapAllowed){
        final var ordinary=envelopeChoice(envelopeSloEvidenceCanonical(false,false),envelopeSloEvidenceCanonical(false,true));
        return gapAllowed?envelopeChoice(ordinary,envelopeChoice(envelopeSloEvidenceCanonical(true,false),envelopeSloEvidenceCanonical(true,true))):ordinary;
    }
    private static EnvelopeMaterial envelopeSloAnyEvidenceDecode(boolean gapAllowed){
        final var ordinary=envelopeChoice(envelopeSloEvidenceDecode(false,false),envelopeSloEvidenceDecode(false,true));
        return gapAllowed?envelopeChoice(ordinary,envelopeChoice(envelopeSloEvidenceDecode(true,false),envelopeSloEvidenceDecode(true,true))):ordinary;
    }
    private static EnvelopeMaterial envelopeSloEvidenceDigest(boolean gap,boolean admission){
        return envelopeHash(envelopeSloEvidenceCanonical(gap,admission),(gap?
                "nereus-delay-target-slo-recovery-gap-evidence\0":"nereus-delay-target-slo-final-evidence\0").length());
    }
    private static EnvelopeMaterial envelopeSloAnyEvidenceDigest(boolean gapAllowed){
        final var ordinary=envelopeChoice(envelopeSloEvidenceDigest(false,false),envelopeSloEvidenceDigest(false,true));
        return gapAllowed?envelopeChoice(ordinary,envelopeChoice(envelopeSloEvidenceDigest(true,false),envelopeSloEvidenceDigest(true,true))):ordinary;
    }
    private static EnvelopeMaterial envelopeSloReceiptFields(){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES));
        fields=envelopeDrop(fields,envelopeSloTimeCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetSloCustodyReceipt.MAX_CANONICAL_BYTES-34L-66);
    }
    private static EnvelopeMaterial envelopeSloReceiptCanonical(){
        final var suffix=envelopeWritten(envelopeHash(envelopeSloReceiptFields(),"nereus-delay-target-slo-custody-receipt\0".length()),34+66);
        return envelopeCall(envelopeHold(envelopeSloReceiptFields(),suffix),envelopeArray(com.nereusstream.delay.protocol.TargetSloCustodyReceipt.MAX_CANONICAL_BYTES),0);
    }
    private static EnvelopeMaterial envelopeSloReceiptDecode(){
        final long n=com.nereusstream.delay.protocol.TargetSloCustodyReceipt.MAX_CANONICAL_BYTES;
        final var parsed=envelopeSloRead(n,1,TargetQuotaScope.MAX_CANONICAL_BYTES,16,10,32,32,32,32,32,10,32,
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES,5,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,10,32,64);
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeRepeat(envelopeArray(32),6,true));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES,
                envelopeAuthorDecode(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES)));
        args=envelopeHold(args,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,envelopeSloTimeDecode()));args=envelopeHold(args,envelopeArray(64));
        var ctor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),6,true));ctor=envelopeHold(ctor,envelopeArray(64));
        ctor=envelopeTemporary(ctor,envelopeSloBoundedTime());
        ctor=envelopeTemporary(ctor,envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES));
        return envelopeSloDecoded(parsed,args,ctor,args.held-16-6L*32-64,envelopeSloReceiptCanonical());
    }
    private static long envelopeSloEventBodyBytes(boolean custody,boolean assigned,boolean epoch){
        return envelopeSloEventBodyBytes(custody,assigned,epoch,true);
    }
    private static long envelopeSloEventBodyBytes(boolean custody,boolean assigned,boolean epoch,boolean evidence){
        if(custody)return epoch?com.nereusstream.delay.protocol.TargetSloObservationEventBody.MAX_EPOCH_CUSTODY_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationEventBody.MAX_CUSTODY_CANONICAL_BYTES;
        return epoch?com.nereusstream.delay.protocol.TargetSloObservationEventBody.MAX_EPOCH_FINAL_CANONICAL_BYTES:
                assigned?com.nereusstream.delay.protocol.TargetSloObservationEventBody.MAX_SOURCE_ASSIGNED_CANONICAL_BYTES:
                        evidence?com.nereusstream.delay.protocol.TargetSloObservationEventBody.MAX_FINAL_CANONICAL_BYTES:
                                com.nereusstream.delay.protocol.TargetSloObservationEventBody.MAX_V1_CANONICAL_BYTES;
    }
    private static EnvelopeMaterial envelopeSloAnySampleCanonical(){return envelopeChoice(envelopeChoice(envelopeSloSampleCanonical(1),envelopeSloSampleCanonical(4)),
            envelopeChoice(envelopeSloSampleCanonical(2),envelopeSloSampleCanonical(3)));}
    private static EnvelopeMaterial envelopeSloAnySampleDecode(){return envelopeChoice(envelopeChoice(envelopeSloSampleDecode(1),envelopeSloSampleDecode(4)),
            envelopeChoice(envelopeSloSampleDecode(2),envelopeSloSampleDecode(3)));}
    private static EnvelopeMaterial envelopeSloAnySampleDigest(){return envelopeChoice(envelopeChoice(envelopeSloSampleDigest(1),envelopeSloSampleDigest(4)),
            envelopeChoice(envelopeSloSampleDigest(2),envelopeSloSampleDigest(3)));}
    private static EnvelopeMaterial envelopeSloAnySampleBindingDigest(){return envelopeChoice(envelopeChoice(envelopeSloSampleBindingDigest(1),envelopeSloSampleBindingDigest(4)),
            envelopeChoice(envelopeSloSampleBindingDigest(2),envelopeSloSampleBindingDigest(3)));}
    private static EnvelopeMaterial envelopeSloEventBodyCanonical(boolean custody,boolean assigned,boolean epoch){
        return envelopeSloEventBodyCanonical(custody,assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventBodyCanonical(boolean custody,boolean assigned,boolean epoch,boolean evidence){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeScopeCanonical());
        if(custody){fields=envelopeDrop(fields,envelopeSloReceiptCanonical());fields=envelopeDrop(fields,envelopeSloAnySampleCanonical());}
        else{
            if(!assigned)fields=envelopeDrop(fields,envelopeSloFinalCanonical());
            if(assigned||evidence)fields=envelopeDrop(fields,envelopeSloAnyEvidenceCanonical(assigned));
        }
        if(epoch)fields=envelopeDrop(fields,envelopeSloReferenceCanonical());
        return envelopeWritten(fields,envelopeSloEventBodyBytes(custody,assigned,epoch,evidence));
    }
    private static EnvelopeMaterial envelopeSloEventIdentity(boolean custody,boolean assigned,boolean epoch){
        return envelopeSloEventIdentity(custody,assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventIdentity(boolean custody,boolean assigned,boolean epoch,boolean evidence){
        var fields=envelopeHold(envelopeScopeCanonical(),envelopeArray(8));
        if(epoch){fields=envelopeHold(fields,envelopeSloReferenceCanonical());fields=envelopeHold(fields,envelopeArray(1));}
        if(custody){
            fields=envelopeHold(fields,envelopeHash(envelopeSloReceiptFields(),"nereus-delay-target-slo-custody-receipt\0".length()));
            // exportedSample.digest evaluates the same immutable sample fields and admission branch as canonicalBytes.
            fields=envelopeHold(fields,envelopeSloAnySampleDigest());
        }else if(assigned){fields=envelopeHold(fields,envelopeSloAnyEvidenceDigest(true));if(epoch)fields=envelopeHold(fields,envelopeArray(0));}
        else fields=envelopeHold(fields,envelopeHold(envelopeSloFinalCanonical(),evidence?envelopeSloAnyEvidenceDigest(false):envelopeArray(0)));
        return envelopeHash(fields,80L+(epoch?"nereus-delay-target-slo-writer-epoch-source-event\0":custody?
                "nereus-delay-target-slo-custody-source-event\0":assigned?"nereus-delay-target-slo-source-assigned-event\0":
                        "nereus-delay-target-slo-final-source-event\0").length()); // private lineage/sample/binding are borrowed hash arguments.
    }
    private static EnvelopeMaterial envelopeSloEventBodyDecode(boolean custody,boolean assigned,boolean epoch){
        return envelopeSloEventBodyDecode(custody,assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventBodyDecode(boolean custody,boolean assigned,boolean epoch,boolean evidencePresent){
        final long n=envelopeSloEventBodyBytes(custody,assigned,epoch,evidencePresent);
        final long evidence=assigned?com.nereusstream.delay.protocol.TargetSloFinalEvidence.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloFinalEvidence.MAX_V1_CANONICAL_BYTES;
        final var parsed=envelopeSloRead(n,24,1,10,1,1,TargetQuotaScope.MAX_CANONICAL_BYTES,16,10,32,32,
                !custody&&!assigned?com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES:0,
                custody||!assigned&&!evidencePresent?0:evidence,custody?com.nereusstream.delay.protocol.TargetSloCustodyReceipt.MAX_CANONICAL_BYTES:0,
                custody?com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_CANONICAL_BYTES:0,
                epoch?com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES:0);
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeRepeat(envelopeArray(32),2,true));
        if(custody){args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloCustodyReceipt.MAX_CANONICAL_BYTES,envelopeSloReceiptDecode()));
            args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_CANONICAL_BYTES,envelopeSloAnySampleDecode()));}
        else{
            if(!assigned)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES,envelopeSloFinalDecode()));
            if(assigned||evidencePresent)args=envelopeHold(args,envelopeNested(evidence,envelopeSloAnyEvidenceDecode(assigned)));
        }
        if(epoch)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES,envelopeSloReferenceDecode()));
        var ctor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),2,true));
        if(custody){
            ctor=envelopeTemporary(ctor,envelopeArray(16));ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeArray(32),2,true));
            ctor=envelopeTemporary(ctor,envelopeSloAnySampleBindingDigest());
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeArray(32),envelopeSloAnySampleDigest()));
        }else if(!assigned){ctor=envelopeTemporary(ctor,envelopeArray(32));ctor=envelopeTemporary(ctor,envelopeSloFinalCanonical());}
        return envelopeSloDecoded(parsed,args,ctor,args.held-80,envelopeSloEventBodyCanonical(custody,assigned,epoch,evidencePresent),
                envelopeNested(24,envelopeShardSubjectDecode()));
    }
    private static long envelopeSloEventInputBytes(boolean custody,boolean assigned,boolean epoch){
        if(custody)return epoch?com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_INPUT_BYTES:
                com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_V1_INPUT_BYTES;
        return epoch?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_INPUT_BYTES:
                assigned?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_LEGACY_INPUT_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_V1_INPUT_BYTES;
    }
    private static EnvelopeMaterial envelopeSloEventInputDecode(boolean custody,boolean assigned,boolean epoch){
        return envelopeSloEventInputDecode(custody,assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventInputDecode(boolean custody,boolean assigned,boolean epoch,boolean evidence){
        final var body=envelopeSloEventBodyDecode(custody,assigned,epoch,evidence);
        return envelopeSystemDecode(envelopeSloEventInputBytes(custody,assigned,epoch),envelopeSloEventBodyBytes(custody,assigned,epoch,evidence),
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES,body,
                envelopeCall(body,envelopeSloEventIdentity(custody,assigned,epoch,evidence),0),false);
    }
    private static EnvelopeMaterial envelopeSloEventBodyCanonical(){return envelopeChoice(envelopeChoice(
            envelopeChoice(envelopeSloEventBodyCanonical(false,false,false,false),envelopeSloEventBodyCanonical(false,false,false)),
            envelopeChoice(envelopeSloEventBodyCanonical(false,true,false),envelopeSloEventBodyCanonical(false,true,true))),
            envelopeChoice(envelopeSloEventBodyCanonical(true,false,false),envelopeSloEventBodyCanonical(true,false,true)));}
    private static EnvelopeMaterial envelopeSloEventBodyDecode(){return envelopeChoice(envelopeChoice(
            envelopeChoice(envelopeSloEventBodyDecode(false,false,false,false),envelopeSloEventBodyDecode(false,false,false)),
            envelopeChoice(envelopeSloEventBodyDecode(false,true,false),envelopeSloEventBodyDecode(false,true,true))),
            envelopeChoice(envelopeSloEventBodyDecode(true,false,false),envelopeSloEventBodyDecode(true,false,true)));}
    private static EnvelopeMaterial envelopeSloEventBodyIdentity(){
        return envelopeChoice(envelopeChoice(envelopeChoice(envelopeCall(envelopeSloEventBodyDecode(false,false,false,false),
                        envelopeSloEventIdentity(false,false,false,false),0),envelopeCall(envelopeSloEventBodyDecode(false,false,false),
                        envelopeSloEventIdentity(false,false,false),0)),envelopeChoice(envelopeCall(envelopeSloEventBodyDecode(false,true,false),
                        envelopeSloEventIdentity(false,true,false),0),envelopeCall(envelopeSloEventBodyDecode(false,true,true),
                        envelopeSloEventIdentity(false,true,true),0))),envelopeChoice(envelopeCall(envelopeSloEventBodyDecode(true,false,false),
                        envelopeSloEventIdentity(true,false,false),0),envelopeCall(envelopeSloEventBodyDecode(true,false,true),
                        envelopeSloEventIdentity(true,false,true),0)));
    }
    private static EnvelopeMaterial envelopeSloEventBodyValidation(){return envelopeSloEventBodyDecode();}
    private static EnvelopeMaterial envelopeSloEventFields(boolean assigned,boolean epoch){
        return envelopeSloEventFields(assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventFields(boolean assigned,boolean epoch,boolean evidence){
        // fields() calls schemaVersion/body twice. These are typed decodes of the retained exact input body.
        var fields=envelopeRepeat(envelopeNested(envelopeSloEventBodyBytes(false,assigned,epoch,evidence),envelopeSloEventBodyDecode(false,assigned,epoch,evidence)),2,false);
        fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeSloEventInputBytes(false,assigned,epoch)));
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));fields=envelopeDrop(fields,envelopeSloAnyOutboxCanonical());
        if(assigned)fields=envelopeDrop(fields,envelopeSloFinalCanonical());
        final long n=epoch?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_CANONICAL_BYTES:
                assigned?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_LEGACY_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_V1_CANONICAL_BYTES;
        return envelopeWritten(fields,n-34);
    }
    private static EnvelopeMaterial envelopeSloEventCanonical(boolean assigned,boolean epoch){
        return envelopeSloEventCanonical(assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventCanonical(boolean assigned,boolean epoch,boolean evidence){
        final long n=epoch?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_CANONICAL_BYTES:
                assigned?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_LEGACY_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_V1_CANONICAL_BYTES;
        final var fieldKind=envelopeNested(envelopeSloEventBodyBytes(false,assigned,epoch,evidence),envelopeSloEventBodyDecode(false,assigned,epoch,evidence));
        final var suffix=envelopeWritten(envelopeDrop(fieldKind,envelopeHash(envelopeSloEventFields(assigned,epoch,evidence),
                "nereus-delay-target-slo-observation-source-record\0".length())),34);
        var frame=envelopeCall(envelopeHold(envelopeSloEventFields(assigned,epoch,evidence),suffix),envelopeArray(n),0);
        // The original schema bound check independently evaluates schemaVersion and, for schema2, body.writerEpoch.
        return envelopeTemporary(frame,envelopeRepeat(fieldKind,assigned?2:1,false));
    }
    private static EnvelopeMaterial envelopeSloEventDecode(boolean assigned,boolean epoch){
        return envelopeSloEventDecode(assigned,epoch,true);
    }
    private static EnvelopeMaterial envelopeSloEventDecode(boolean assigned,boolean epoch,boolean evidence){
        final long n=epoch?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_CANONICAL_BYTES:
                assigned?com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_LEGACY_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloObservationEventRecord.MAX_V1_CANONICAL_BYTES;
        final long input=envelopeSloEventInputBytes(false,assigned,epoch);
        var parsed=envelopeSloRead(n,1,input,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,32,envelopeSloOutboxBytes(true),
                assigned?com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES:0,32);
        parsed=envelopeHold(parsed,envelopeArray(input)); // the exact input local remains referenced until decode returns
        var args=envelopeSloEventInputDecode(false,assigned,epoch,evidence);
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeNested(envelopeSloOutboxBytes(true),envelopeSloAnyOutboxDecode()));
        if(assigned)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationSample.MAX_FINAL_BYTES,envelopeSloFinalDecode()));
        var ctor=envelopeAssigned(32);
        // The constructor keeps body until incoming selection/validation completes.
        ctor=envelopeHold(ctor,envelopeNested(envelopeSloEventBodyBytes(false,assigned,epoch,evidence),envelopeSloEventBodyDecode(false,assigned,epoch,evidence)));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeArray(32),envelopeSloEventIdentity(false,assigned,epoch,evidence)));
        ctor=envelopeTemporary(ctor,envelopeSystemCanonical(input));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeHash(envelopeSystemCanonical(input),0),envelopeArray(32)));
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeArray(32),2,true));
        if(assigned)ctor=envelopeTemporary(ctor,envelopeComparison(envelopeArray(32),envelopeSloAnyEvidenceDigest(true)));
        // incoming.validateAgainst(after.start) clones the Start's sampleId and startDigest.
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeArray(32),2,false));ctor=envelopeTemporary(ctor,envelopeSloFinalCanonical());
        // Explicit legacy input supplies incoming via body.finalObservation; that object escapes the local body.
        final long incomingHeld=assigned?0:envelopeSloFinalDecode().held;
        ctor=envelopeReturn(ctor,32+incomingHeld);
        return envelopeSloDecoded(parsed,args,ctor,args.held-32,envelopeSloEventCanonical(assigned,epoch,evidence),
                envelopeNested(envelopeSloEventBodyBytes(false,assigned,epoch,evidence),envelopeSloEventBodyDecode(false,assigned,epoch,evidence)));
    }
    private static EnvelopeMaterial envelopeSloCustodyFields(boolean epoch){
        return envelopeWritten(envelopeDrop(envelopeSystemCanonical(envelopeSloEventInputBytes(true,false,epoch)),envelopeMutationCanonical(false)),
                (epoch?com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_CANONICAL_BYTES:
                        com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_V1_CANONICAL_BYTES)-34L);
    }
    private static EnvelopeMaterial envelopeSloCustodyCanonical(boolean epoch){
        var frame=envelopeSloHashed(envelopeSloCustodyFields(epoch),epoch?com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_V1_CANONICAL_BYTES,"nereus-delay-target-slo-custody-source-record\0");
        return envelopeTemporary(frame,envelopeNested(envelopeSloEventBodyBytes(true,false,epoch),envelopeSloEventBodyDecode(true,false,epoch)));
    }
    private static EnvelopeMaterial envelopeSloCustodyDecode(boolean epoch){
        final long n=epoch?com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloCustodyRecord.MAX_V1_CANONICAL_BYTES,input=envelopeSloEventInputBytes(true,false,epoch);
        final var parsed=envelopeSloRead(n,1,input,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,32);
        var args=envelopeNested(input,envelopeSloEventInputDecode(true,false,epoch));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var checks=envelopeNested(envelopeSloEventBodyBytes(true,false,epoch),envelopeSloEventBodyDecode(true,false,epoch));
        checks=envelopeTemporary(checks,envelopeSystemCanonical(input));
        checks=envelopeTemporary(checks,envelopeComparison(envelopeArray(32),envelopeSloEventIdentity(true,false,epoch)));
        checks=envelopeTemporary(checks,envelopeComparison(envelopeHash(envelopeSystemCanonical(input),0),envelopeArray(32)));
        checks=envelopeTemporary(checks,envelopeSloAtOrBefore());
        return envelopeSloDecoded(parsed,args,envelopeReturn(checks,0),args.held,envelopeSloCustodyCanonical(epoch));
    }
    private static EnvelopeMaterial envelopeSloPolicyDecode(boolean epoch){
        final long declaration=com.nereusstream.delay.protocol.TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES;
        final long count=com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_DECLARATIONS,group=count*(declaration+8);
        final long n=epoch?com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_V1_CANONICAL_BYTES;
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        var parsed=envelopeSloRead(n,1,TargetQuotaScope.MAX_CANONICAL_BYTES,16,source,
                group,com.nereusstream.delay.protocol.TargetSloObservationPolicy.Limits.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES,5,32,
                epoch?com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES:0,32);
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeNested(source,envelopeSourceDecode()));
        parsed=envelopeHold(parsed,envelopeNested(group,envelopeProtoRead(group,count*declaration,declaration,count)));
        final var declarations=envelopeRepeat(envelopeNested(declaration,envelopeSloDeclarationDecode()),count,true);
        args=envelopeHold(args,declarations);
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationPolicy.Limits.MAX_CANONICAL_BYTES,
                envelopeSloScalarDecode(com.nereusstream.delay.protocol.TargetSloObservationPolicy.Limits.MAX_CANONICAL_BYTES,1+5+5+10+5+10,6)));
        parsed=envelopeHold(parsed,envelopeArray(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES));
        args=envelopeHold(args,envelopeAuthorDecode(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES));
        args=envelopeHold(args,envelopeArray(32));if(epoch)args=envelopeHold(args,envelopeNested(
                com.nereusstream.delay.protocol.TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES,envelopeSloReferenceDecode()));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));
        constructor=envelopeTemporary(constructor,envelopeSourceBounded());
        constructor=envelopeTemporary(constructor,envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES));
        return envelopeSloDecoded(parsed,args,constructor,args.held-48,envelopeSloPolicyCanonical(epoch),
                envelopeComparison(envelopeHash(envelopeSloPolicyFields(epoch),(epoch?
                "nereus-delay-target-slo-observation-epoch-policy\0":"nereus-delay-target-slo-observation-policy\0").length()),envelopeArray(32)));
    }


    private static EnvelopeMaterial envelopePaidScalarDecode(long bytes,long fields){
        return envelopeReturn(envelopeTemporary(envelopeProtoRead(bytes,bytes,bytes,fields),envelopeArray(bytes)),0);
    }
    private static EnvelopeMaterial envelopePaidExposureWindowCanonical(){return envelopeWritten(envelopeRepeat(envelopeArray(24),2,false),72);}
    private static EnvelopeMaterial envelopePaidExposureFields(){
        return envelopeWritten(envelopeDrop(envelopeRepeat(envelopeArray(24),2,false),envelopePaidExposureWindowCanonical()),
                com.nereusstream.delay.protocol.TargetScopeExposurePolicy.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePaidExposureCanonical(){return envelopeWritten(envelopePaidExposureFields(),com.nereusstream.delay.protocol.TargetScopeExposurePolicy.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopePaidExposureDecode(){
        var window=envelopeCoreDecoded(72,5,envelopeRepeat(envelopeNested(24,envelopePaidScalarDecode(24,3)),2,true),
                envelopeEmpty(),envelopePaidExposureWindowCanonical(),0);
        var args=envelopeRepeat(envelopeNested(24,envelopePaidScalarDecode(24,3)),2,true);
        args=envelopeHold(args,envelopeNested(72,window));
        var own=envelopeHash(envelopePaidExposureFields(),"nereus-delay-target-scope-exposure-policy\0".length());
        own=envelopeTemporary(own,envelopePaidExposureCanonical());
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeExposurePolicy.MAX_CANONICAL_BYTES,8,args,own,
                envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeArray(32));return envelopeTemporary(value,envelopePaidExposureCanonical());
    }
    private static EnvelopeMaterial envelopePaidHeaderFields(){
        var work=envelopeDrop(envelopeScopeCanonical(),envelopeSourceCanonical());
        work=envelopeDrop(work,envelopeArray(com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_REGISTRY_KEY_PREFIX_BYTES));
        work=envelopeDrop(work,envelopeRepeat(envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES),
                com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_SERVICE_ALIASES,false));
        work=envelopeDrop(work,envelopeRepeat(envelopeTupleCanonical(),com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_PROTOCOLS,false));
        work=envelopeDrop(work,envelopeArray(40));work=envelopeDrop(work,envelopePaidExposureCanonical());
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePaidHeaderCanonical(){return envelopeWritten(envelopePaidHeaderFields(),com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopePaidHeaderDecode(){
        final long aliases=com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_SERVICE_ALIASES;
        final long protocols=com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_PROTOCOLS;
        var args=envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES,
                envelopeAuthorDecode(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES)),aliases,true);
        args=envelopeHold(args,envelopeRepeat(envelopeNested(40,envelopeTupleDecode()),protocols,true));
        args=envelopeHold(args,envelopeArrays(144,5));args=envelopeHold(args,envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeNested(40,envelopePaidScalarDecode(40,5)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetScopeExposurePolicy.MAX_CANONICAL_BYTES,envelopePaidExposureDecode()));
        var own=envelopeHold(envelopeRepeat(envelopeAssigned(32),4,true),envelopeAssigned(16));
        own=envelopeTemporary(own,envelopeSourceBounded());own=envelopeTemporary(own,envelopeArray(4096));
        // services()/tuples() each cache exactly one unsigned canonical sort key per item.
        own=envelopeTemporary(own,envelopeRepeat(envelopeAuthorCanonical(com.nereusstream.delay.protocol.TargetSloObservationPolicy.MAX_SERVICE_BYTES),aliases,true));
        own=envelopeTemporary(own,envelopeRepeat(envelopeTupleCanonical(),protocols,true));
        own=envelopeHold(own,envelopeHash(envelopePaidHeaderFields(),"nereus-delay-target-slo-control-scope-header-2\0".length()));
        own=envelopeTemporary(own,envelopePaidHeaderCanonical());
        // maximumRootReservation reads the header image once more under the immutable Root format.
        own=envelopeTemporary(own,envelopePaidHeaderCanonical());
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_CANONICAL_BYTES,13+aliases+protocols,
                args,own,envelopeEmpty(),args.held-144);
        value=envelopeTemporary(value,envelopeRepeat(envelopeArray(4096),2,false));
        value=envelopeTemporary(value,envelopeArray(32));return envelopeTemporary(value,envelopePaidHeaderCanonical());
    }
    private static EnvelopeMaterial envelopePaidScopeKey(){
        return envelopeCall(envelopeArray(32),envelopeCall(envelopeArray(2),envelopeArray(34),0),0);
    }
    private static EnvelopeMaterial envelopePaidRequireSource(){
        var work=envelopeDrop(envelopeSourceBounded(),envelopeScopeConstruct());
        return envelopeDrop(work,envelopeSourceCanonical());
    }
    private static EnvelopeMaterial envelopePaidRequireHeader(){
        var work=envelopePaidRequireSource();work=envelopeTemporary(work,envelopeScopeConstruct());
        work=envelopeTemporary(work,envelopeArrays(80,3));
        return envelopeDrop(work,envelopeComparison(envelopePaidExposureCanonical(),envelopePaidExposureCanonical()));
    }
    private static EnvelopeMaterial envelopePaidEffectiveCeilings(){
        var work=envelopeDrop(envelopeSourceBounded(),envelopeSourceCanonical());
        final var key=envelopeCall(envelopeScopeConstruct(),envelopeHold(envelopeArray(87),envelopeArray(32)),0);
        return envelopeDrop(work,envelopeRepeat(key,3,false));
    }
    private static EnvelopeMaterial envelopePaidNativeBudget(){
        var work=envelopePaidRequireHeader();
        // Nine recordCeiling calls each consult keyCeiling, payloadCeiling and recordCharge's frozen formats.
        work=envelopeTemporary(work,envelopeRepeat(envelopePaidEffectiveCeilings(),27,false));
        // FIRST/POS pair + scaled sample classes + writer/generation/head + max(event,custody).
        work=envelopeHold(work,envelopeRepeat(envelopeVectorConstruct(),22,false));
        work=envelopeHold(work,envelopeUsageConstruct());return envelopeReturn(work,32);
    }
    private static EnvelopeMaterial envelopePaidFutureNative(){
        var work=envelopePaidNativeBudget();work=envelopeTemporary(work,envelopePaidEffectiveCeilings());
        work=envelopeTemporary(work,envelopeArray(4096));work=envelopeTemporary(work,envelopeRepeat(envelopePaidHeaderCanonical(),2,false));
        // Three external evidence charges, slot multiplication and four immutable additions, then payload vector/add.
        work=envelopeHold(work,envelopeRepeat(envelopeVectorConstruct(),10,false));
        work=envelopeTemporary(work,envelopeUsageConstruct());return envelopeReturn(work,32);
    }
    private static EnvelopeMaterial envelopePaidPlanFields(){
        var work=envelopeDrop(envelopeIncarnationCanonical(false),envelopeSourceCanonical());
        work=envelopeDrop(work,envelopeArray(com.nereusstream.delay.protocol.TargetSloScopeNativePlan.Ceilings.MAX_CANONICAL_BYTES));
        work=envelopeDrop(work,envelopeArray(com.nereusstream.delay.protocol.TargetSloScopeNativePlan.ExposureFormat3.MAX_CANONICAL_BYTES));
        return envelopeWritten(envelopeDrop(work,envelopePaidExposureCanonical()),com.nereusstream.delay.protocol.TargetSloScopeNativePlan.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePaidPlanCanonical(){return envelopeWritten(envelopePaidPlanFields(),com.nereusstream.delay.protocol.TargetSloScopeNativePlan.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopePaidPlanDecode(){
        var args=envelopeNested(1_049_078,envelopeIncarnationDecode(false));
        args=envelopeHold(args,envelopeNested(1_048_670,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloScopeNativePlan.Ceilings.MAX_CANONICAL_BYTES,
                envelopePaidScalarDecode(com.nereusstream.delay.protocol.TargetSloScopeNativePlan.Ceilings.MAX_CANONICAL_BYTES,18)));
        args=envelopeHold(args,envelopeNested(23,envelopePaidScalarDecode(23,4)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetScopeExposurePolicy.MAX_CANONICAL_BYTES,envelopePaidExposureDecode()));
        var own=envelopeSourceBounded();own=envelopeTemporary(own,envelopeIncarnationCanonical(false));
        own=envelopeTemporary(own,envelopeRepeat(envelopeSourceCanonical(),4,false));
        own=envelopeTemporary(own,envelopePaidRequireSource());own=envelopeTemporary(own,envelopeRepeat(envelopeScopeConstruct(),3,false));
        own=envelopeTemporary(own,envelopeRepeat(envelopeArray(87),3,false));own=envelopeTemporary(own,envelopePaidExposureCanonical());
        own=envelopeHold(own,envelopeHash(envelopePaidPlanFields(),"nereus-delay-target-slo-scope-native-plan\0".length()));
        own=envelopeTemporary(own,envelopePaidPlanCanonical());
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetSloScopeNativePlan.MAX_CANONICAL_BYTES,12,args,own,envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeArray(32));return envelopeTemporary(value,envelopePaidPlanCanonical());
    }
    private static EnvelopeMaterial envelopePaidParentReferenceCanonical(){return envelopeArray(com.nereusstream.delay.protocol.TargetParentControlCredit.Reference.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopePaidParentReferenceDecode(){
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetParentControlCredit.Reference.MAX_CANONICAL_BYTES,3,
                envelopeArrays(64,2),envelopeRepeat(envelopeAssigned(32),2,true),envelopePaidParentReferenceCanonical(),0);
    }
    private static EnvelopeMaterial envelopePaidDefinitionFields(){
        var work=envelopeDrop(envelopePaidParentReferenceCanonical(),envelopePaidHeaderCanonical());
        return envelopeWritten(envelopeDrop(work,envelopePaidPlanCanonical()),com.nereusstream.delay.protocol.TargetScopeControlObligation.Definition.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePaidDefinitionCanonical(){
        return envelopeWritten(envelopeDrop(envelopePaidDefinitionFields(),envelopeHash(envelopePaidDefinitionFields(),
                "nereus-delay-target-scope-control-obligation-definition\0".length())),com.nereusstream.delay.protocol.TargetScopeControlObligation.Definition.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopePaidDefinitionDecode(){
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetParentControlCredit.Reference.MAX_CANONICAL_BYTES,envelopePaidParentReferenceDecode());
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloControlScopeHeader.MAX_CANONICAL_BYTES,envelopePaidHeaderDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSloScopeNativePlan.MAX_CANONICAL_BYTES,envelopePaidPlanDecode()));
        var check=envelopePaidRequireHeader();check=envelopeTemporary(check,envelopePaidFutureNative());check=envelopeTemporary(check,envelopePaidHeaderCanonical());
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligation.Definition.MAX_CANONICAL_BYTES,5,args,
                envelopeReturn(check,0),envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeHash(envelopePaidDefinitionFields(),"nereus-delay-target-scope-control-obligation-definition\0".length()));
        value=envelopeTemporary(value,envelopeArray(32));return envelopeTemporary(value,envelopePaidDefinitionCanonical());
    }
    private static EnvelopeMaterial envelopePaidRequestCanonical(){return envelopeWritten(envelopePaidDefinitionCanonical(),com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopePaidRequestDecode(){
        var args=envelopeHold(envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligation.Definition.MAX_CANONICAL_BYTES,envelopePaidDefinitionDecode()),envelopeArray(32));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES,5,args,envelopeAssigned(32),envelopePaidRequestCanonical(),args.held-32);
    }
    private static EnvelopeMaterial envelopePaidRequestControlRef(){
        var work=envelopeCall(envelopeArray(32),envelopeAssigned(32),0);
        return envelopeDrop(work,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(envelopePaidRequestCanonical(),
                com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES,33)));
    }
    private static EnvelopeMaterial envelopePaidBodySemantic(){
        return envelopeHash(envelopeHold(envelopeArray(2),envelopePaidRequestCanonical()),"nereus-delay-target-scope-control-obligation\0".length());
    }
    private static EnvelopeMaterial envelopePaidBodyCanonical(){
        var work=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());work=envelopeDrop(work,envelopePaidBodySemantic());
        work=envelopeDrop(work,envelopeWritten(envelopePaidRequestCanonical(),com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES+8L));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetScopeControlObligationBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopePaidBodyDecode(){
        var args=envelopeNested(24,envelopeTemporary(envelopeProtoRead(24,24,24,2),envelopeShardSubjectDecode()));
        args=envelopeHold(args,envelopeNested(74,envelopeTemporary(envelopeProtoRead(74,74,74,3),envelopeControlRefDecode())));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES+8L,
                envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES+8L,1,
                        envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligationRequest.MAX_CANONICAL_BYTES,envelopePaidRequestDecode()),
                        envelopeEmpty(),envelopeEmpty(),envelopePaidRequestDecode().held)));
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligationBody.MAX_CANONICAL_BYTES,9,args,
                envelopeReturn(envelopePaidRequestControlRef(),0),envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeComparison(envelopePaidBodySemantic(),envelopeArray(32)));
        return envelopeTemporary(value,envelopePaidBodyCanonical());
    }
    private static EnvelopeMaterial envelopePaidBodyIdentity(){return envelopeCall(envelopePaidBodyDecode(),envelopeControlLogicalIdentity(),0);}
    private static EnvelopeMaterial envelopePaidControlCanonical(){
        // Control.schemaVersion() reparses its retained original body before writing the version field.
        var work=envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligationBody.MAX_CANONICAL_BYTES,envelopePaidBodyDecode());
        work=envelopeDrop(work,envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetScopeControlObligation.Control.MAX_INPUT_BYTES));
        return envelopeWritten(envelopeDrop(work,envelopeMutationCanonical(false)),
                com.nereusstream.delay.protocol.TargetScopeControlObligation.Control.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopePaidControlDecode(){
        final long body=com.nereusstream.delay.protocol.TargetScopeControlObligationBody.MAX_CANONICAL_BYTES;
        final long input=com.nereusstream.delay.protocol.TargetScopeControlObligation.Control.MAX_INPUT_BYTES;
        var args=envelopeNested(input,envelopeSystemDecode(input,body,104,envelopePaidBodyDecode(),envelopePaidBodyIdentity(),true));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var check=envelopeSystemCanonical(input);check=envelopeTemporary(check,envelopeNested(body,envelopePaidBodyDecode()));
        check=envelopeTemporary(check,envelopePaidRequireSource());check=envelopeTemporary(check,envelopeArray(32));
        check=envelopeTemporary(check,envelopeControlLogicalIdentity());check=envelopeTemporary(check,envelopeCall(envelopeSystemCanonical(input),envelopeArray(32),0));
        check=envelopeTemporary(check,envelopeNested(104,envelopeAuthorDecode(104)));check=envelopeTemporary(check,envelopeMutationAtOrBefore());
        // The decoder's version comparison invokes value.schemaVersion() again after its constructor.
        check=envelopeTemporary(check,envelopeNested(body,envelopePaidBodyDecode()));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligation.Control.MAX_CANONICAL_BYTES,3,args,
                envelopeReturn(check,0),envelopePaidControlCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopePaidUsageCanonical(){
        return envelopeWritten(envelopeDrop(envelopeVectorCanonical(),envelopeMutationCanonical(false)),com.nereusstream.delay.protocol.TargetScopeControlObligation.Usage.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopePaidUsageDecode(){
        var args=envelopeNested(960,envelopeVectorDecode());args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var check=envelopeUsageConstruct();check=envelopeTemporary(check,envelopeVectorCanonical());check=envelopeTemporary(check,envelopeSourceCanonical());
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligation.Usage.MAX_CANONICAL_BYTES,7,args,
                envelopeReturn(check,0),envelopePaidUsageCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopePaidScopeFields(){
        var fields=envelopeDrop(envelopePaidControlCanonical(),envelopeVectorCanonical());fields=envelopeDrop(fields,envelopePaidUsageCanonical());
        return envelopeWritten(envelopeDrop(fields,envelopeRepeat(envelopeVectorCanonical(),2,false)),com.nereusstream.delay.protocol.TargetScopeControlObligation.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopePaidScopeCanonical(){return envelopeWritten(envelopePaidScopeFields(),com.nereusstream.delay.protocol.TargetScopeControlObligation.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopePaidScopeDecode(){
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligation.Control.MAX_CANONICAL_BYTES,envelopePaidControlDecode());
        args=envelopeHold(args,envelopeNested(960,envelopeVectorDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligation.Usage.MAX_CANONICAL_BYTES,envelopePaidUsageDecode()));
        final var frozen=envelopeTemporary(envelopeVectorDecode(),envelopeUsageConstruct());
        args=envelopeHold(args,envelopeRepeat(envelopeNested(960,frozen),2,true));
        // The constructor reparses funding.definition and verifies bootstrap against FIRST+POSITION ceilings.
        var own=envelopeNested(com.nereusstream.delay.protocol.TargetScopeControlObligationBody.MAX_CANONICAL_BYTES,envelopePaidBodyDecode());
        own=envelopeTemporary(own,envelopeUsageConstruct());own=envelopeTemporary(own,envelopeRepeat(envelopePaidEffectiveCeilings(),6,false));
        own=envelopeTemporary(own,envelopeRepeat(envelopeVectorConstruct(),3,false));own=envelopeTemporary(own,envelopePaidRequireSource());
        own=envelopeTemporary(own,envelopeMutationAtOrBefore());own=envelopeTemporary(own,envelopePaidNativeBudget());
        own=envelopeHold(own,envelopePaidFutureNative());
        // originalMetadataCeiling encodes its original control/bootstrap/usage/vector shapes before metadataCharge.
        own=envelopeTemporary(own,envelopePaidControlCanonical());own=envelopeTemporary(own,envelopeVectorCanonical());
        own=envelopeTemporary(own,envelopeRepeat(envelopePaidEffectiveCeilings(),3,false));
        own=envelopeTemporary(own,envelopePaidScopeKey());own=envelopeHold(own,envelopeVectorConstruct());
        own=envelopeHold(own,envelopeHash(envelopePaidScopeFields(),"nereus-delay-target-scope-control-obligation\0".length()));
        own=envelopeTemporary(own,envelopePaidScopeCanonical());own=envelopeTemporary(own,envelopePaidUsageCanonical());
        own=envelopeTemporary(own,envelopePaidScopeKey());own=envelopeTemporary(own,envelopePaidEffectiveCeilings());
        own=envelopeHold(own,envelopeVectorConstruct());own=envelopeHold(own,envelopeVectorConstruct());
        own=envelopeTemporary(own,envelopeRepeat(envelopeVectorConstruct(),2,false));own=envelopeTemporary(own,envelopeUsageConstruct());
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetScopeControlObligation.MAX_CANONICAL_BYTES,7,args,own,
                envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeArray(32));return envelopeTemporary(value,envelopePaidScopeCanonical());
    }

    // RETIREMENT_MATERIAL_REGION: retirement and migration codec graph insertion point.

    /** Several disjoint Reader inputs: their payload bytes add; only Reader objects multiply. */
    private static EnvelopeMaterial envelopeCoreReaders(long bytes,long readers,long fields){
        if(readers==0)return envelopeEmpty();
        final var work=envelopeProtoRead(bytes,bytes,bytes,fields);
        return new EnvelopeMaterial(work.allocated,work.retained,work.held,work.fieldVisits,
                Math.addExact(work.arrayObjects,readers-1));
    }
    /** SourcePositionCodec, before Target's optional bounded-Source restriction is applied. */
    private static EnvelopeMaterial envelopePhysicalSourcesCanonical(long bytes,long count){
        var args=envelopeHold(envelopeArrays(bytes,count),envelopeArrays(Math.multiplyExact(16,count),count));
        return envelopeCall(args,envelopeArrays(bytes,count),0);
    }
    private static EnvelopeMaterial envelopePhysicalSourcesDecode(long bytes,long count){
        var work=envelopeArrays(Math.multiplyExact(16,count),count); // readFixed(route)
        work=envelopeHold(work,envelopeArrays(bytes,count)); // readBytes(resource), retained through construction
        work=envelopeTemporary(work,envelopeArrays(bytes,count)); // readString UTF-8 input
        work=envelopeTemporary(work,envelopeArrays(Math.multiplyExact(16,count),count)); // Kafka UUID input
        work=envelopeHold(work,envelopeArrays(Math.multiplyExact(16,count),count)); // RouteIncarnation copy
        work=envelopeTemporary(work,envelopeArrays(bytes,count)); // canonicalText UTF-8 round trip
        work=envelopeHold(work,envelopeArrays(bytes,count)); // Pulsar resource owned copy
        work=envelopeTemporary(work,envelopePhysicalSourcesCanonical(bytes,count));
        return envelopeReturn(work,Math.addExact(bytes,Math.multiplyExact(16,count)));
    }
    private static EnvelopeMaterial envelopeCoreDecoded(long encoded,long fields,EnvelopeMaterial arguments,
            EnvelopeMaterial constructor,EnvelopeMaterial canonical,long borrowed){
        var work=envelopeHold(envelopeProtoRead(encoded,encoded,encoded,fields),envelopeCall(arguments,constructor,borrowed));
        work=envelopeTemporary(work,canonical);
        return envelopeReturn(work,Math.addExact(borrowed,constructor.held));
    }
    private static EnvelopeMaterial envelopeCoreHashed(EnvelopeMaterial fields,long encoded,String domain){
        // The older digest() methods evaluate fields() independently of the outer writer.
        return envelopeConcat(envelopeHold(fields,envelopeWritten(envelopeHash(fields,domain.length()),34)));
    }
    private static EnvelopeMaterial envelopeEvidenceCapacityCanonical(){
        return envelopeWritten(envelopeArray(2048),com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceCapacityDecode(){
        final var args=envelopeHold(envelopeArrays(96,3),envelopeArray(2048));
        var own=envelopeRepeat(envelopeAssigned(32),3,true);
        own=envelopeTemporary(own,envelopeRepeat(envelopeArray(2048),2,false));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES,5,args,own,
                envelopeEvidenceCapacityCanonical(),0);
    }
    private static long envelopeEvidenceProvenanceBytes(){return Math.max(
            com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES+256L,
            Math.max(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.MAX_CANONICAL_BYTES+128L,
                    com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.MAX_CANONICAL_BYTES+16L));}
    private static EnvelopeMaterial envelopeEvidenceProvenanceCanonical(int branch){
        final long n=envelopeEvidenceProvenanceBytes();
        if(branch==1)return envelopeWritten(envelopeBusinessEvidenceOwnerCanonical(),n);
        if(branch==2)return envelopeWritten(envelopeWritten(envelopeBusinessEvidenceOwnerCanonical(),n-4),n);
        if(branch==3)return envelopeWritten(envelopeBusinessRecoveryOwnerCanonical(),n);
        if(branch==4)return envelopeWritten(envelopeWritten(envelopeBusinessTimeCanonical(1024),1536),n);
        return envelopeWritten(envelopeWritten(envelopeEvidenceCapacityCanonical(),
                com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES+256L),n);
    }
    private static EnvelopeMaterial envelopeEvidenceProvenanceCanonical(){
        var work=envelopeEvidenceProvenanceCanonical(1);
        for(int branch=2;branch<=5;branch++)work=envelopeChoice(work,envelopeEvidenceProvenanceCanonical(branch));return work;
    }
    private static EnvelopeMaterial envelopeEvidenceProvenanceDecode(int branch){
        final long n=envelopeEvidenceProvenanceBytes();
        var frame=envelopeHold(envelopeProtoRead(n,n,n,1),envelopeArray(n));
        EnvelopeMaterial value;
        if(branch==1)value=envelopeBusinessEvidenceOwnerDecode();
        else if(branch==2){
            var args=envelopeNested(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.MAX_CANONICAL_BYTES,envelopeBusinessEvidenceOwnerDecode());
            args=envelopeHold(args,envelopeArrays(64,2));
            value=envelopeCoreDecoded(n,3,args,envelopeRepeat(envelopeAssigned(32),2,true),
                    envelopeEvidenceProvenanceCanonical(branch),args.held-64);
        }else if(branch==3)value=envelopeBusinessRecoveryOwnerDecode();
        else if(branch==4){
            final var args=envelopeHold(envelopeArrays(128,4),envelopeNested(1024,envelopeBusinessTimeDecode(1024)));
            var own=envelopeRepeat(envelopeAssigned(32),4,true);own=envelopeTemporary(own,envelopeBusinessTimeCanonical(1024));
            value=envelopeCoreDecoded(1536,6,args,own,envelopeEvidenceProvenanceCanonical(branch),args.held-128);
        }else{
            final var args=envelopeHold(envelopeArrays(96,3),envelopeNested(
                    com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES,envelopeEvidenceCapacityDecode()));
            value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES+256L,6,args,
                    envelopeRepeat(envelopeAssigned(32),3,true),envelopeEvidenceProvenanceCanonical(branch),args.held-96);
        }
        frame=envelopeHold(frame,value);frame=envelopeTemporary(frame,envelopeEvidenceProvenanceCanonical(branch));
        return envelopeReturn(frame,value.held);
    }
    private static EnvelopeMaterial envelopeEvidenceProvenanceDecode(){
        var work=envelopeEvidenceProvenanceDecode(1);
        for(int branch=2;branch<=5;branch++)work=envelopeChoice(work,envelopeEvidenceProvenanceDecode(branch));return work;
    }
    private static EnvelopeMaterial envelopeEvidenceResourceCanonical(){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES));
        return envelopeWritten(envelopeDrop(fields,envelopeEvidenceProvenanceCanonical()),
                com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceResourceDecode(){return EvidenceResourceMaterial.DECODE;}
    private static EnvelopeMaterial calculateEnvelopeEvidenceResourceDecode(){
        final long n=com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES;
        final long key=com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES;
        var args=envelopeHold(envelopeArrays(176,6),envelopeNested(24,envelopeShardSubjectDecode()));
        args=envelopeHold(args,envelopeArray(key));args=envelopeHold(args,envelopeArray(512));
        args=envelopeHold(args,envelopeNested(envelopeEvidenceProvenanceBytes(),envelopeEvidenceProvenanceDecode()));
        var own=envelopeHold(envelopeRepeat(envelopeAssigned(32),5,true),envelopeAssigned(16));
        own=envelopeTemporary(own,envelopeRepeat(envelopeArray(key),2,false));own=envelopeHold(own,envelopeArray(512));
        own=envelopeTemporary(own,envelopeEvidenceProvenanceCanonical());
        // Adopted origins compare digest accessors and fee-owner tenant/lineage; reserved staging checks namespace.
        own=envelopeTemporary(own,envelopeArrays(32*5+16,6));
        return envelopeCoreDecoded(n,13,args,own,envelopeEvidenceResourceCanonical(),args.held-176-key-512);
    }
    private static long envelopeEvidenceIdentityBytes(){return com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES+16L;}
    private static EnvelopeMaterial envelopeEvidenceExactIdentity(){
        final long n=envelopeEvidenceIdentityBytes(),resource=com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES;
        var frame=envelopeWritten(envelopeEvidenceResourceCanonical(),n);
        // ExactResourceIdentity.decode and its public constructor each independently parse and validate the branch.
        var check=envelopeProtoRead(n,n,n,1);check=envelopeHold(check,envelopeArray(resource));
        check=envelopeTemporary(check,envelopeEvidenceResourceDecode());
        check=envelopeHold(check,envelopeWritten(envelopeEmpty(),n));
        check=envelopeHold(check,envelopeHash(envelopeHold(envelopeArray("nereus-delay-resource-identity\0".length()),envelopeArray(n)),0));
        frame=envelopeCall(frame,check,0);frame=envelopeCall(frame,check,0);
        return envelopeReturn(frame,n+32);
    }
    private static EnvelopeMaterial envelopeEvidenceResourceOneofDecode(){
        final long n=envelopeEvidenceIdentityBytes();
        return envelopeCoreDecoded(n,1,envelopeNested(com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES,
                envelopeEvidenceResourceDecode()),envelopeEmpty(),envelopeEmpty(),envelopeEvidenceResourceDecode().held);
    }
    private static EnvelopeMaterial envelopeEvidenceUseCanonical(){return envelopeArray(192);}
    private static EnvelopeMaterial envelopeEvidenceUseDecode(){
        var own=envelopeHold(envelopeAssigned(64,2),envelopeArrays(64,2));
        own=envelopeHold(own,envelopeAssigned(64,2));
        return envelopeCoreDecoded(192,6,envelopeArrays(128,4),own,envelopeEvidenceUseCanonical(),0);
    }
    private static EnvelopeMaterial envelopeEvidenceStoreCanonical(){return envelopeCall(envelopeArray(16),envelopeArray(72),0);}
    private static EnvelopeMaterial envelopeEvidenceStoreDecode(){
        return envelopeCall(envelopeArrays(64,3),envelopeArrays(64,3),0);
    }
    private static EnvelopeMaterial envelopeEvidenceContextCanonical(){
        var work=envelopeDrop(envelopeEvidenceStoreCanonical(),envelopeMutationCanonical(true));
        work=envelopeDrop(work,envelopeFenceCanonical());
        work=envelopeDrop(work,envelopeRepeat(envelopeEvidenceUseCanonical(),com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES,false));
        return envelopeWritten(envelopeDrop(work,envelopeIncarnationCanonical(true)),com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceContextDecode(){
        var args=envelopeArrays(64,2);args=envelopeHold(args,envelopeNested(72,envelopeEvidenceStoreDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_CANONICAL_BYTES,envelopeMutationDecode(true)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.store.IngressFenceState.MAX_CANONICAL_BYTES,envelopeFenceDecode()));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(192,envelopeEvidenceUseDecode()),
                com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES,true));
        args=envelopeHold(args,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES+10L,args,
                envelopeRepeat(envelopeAssigned(32),2,true),envelopeEvidenceContextCanonical(),args.held-64);
    }
    private static EnvelopeMaterial envelopeEvidenceContextResourceCheck(){
        var check=envelopeArrays(48+48,4); // fee owner and resource tenant/lineage getter pairs
        check=envelopeTemporary(check,envelopeComparison(envelopeIncarnationCanonical(true),envelopeIncarnationCanonical(true)));
        return envelopeDrop(check,envelopeRepeat(envelopeMutationAtOrBefore(),2,false));
    }
    private static EnvelopeMaterial envelopeEvidenceProtectionCanonical(){
        // ProtectionSet.canonicalBytes returns its already validated stored image by copy.
        return envelopeArray(com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceProtectionDecode(){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES;
        final long count=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTIONS;
        var parsed=envelopeProtoRead(n,n,n,count+1);
        // Target's bounded pre-read, then ProtectionSet's independent read, then every ProtectionRef read.
        parsed=envelopeHold(parsed,envelopeProtoRead(n,n,n,count+1));
        var refs=envelopeHold(envelopeArrays(n,count),envelopeCoreReaders(n,count,7*count));
        refs=envelopeHold(refs,envelopeArrays(n,5*count)); // accessors retained as ProtectionRef constructor args
        refs=envelopeTemporary(refs,envelopePhysicalSourcesDecode(n,count)); // decoder pre-check
        refs=envelopeTemporary(refs,envelopePhysicalSourcesDecode(n,count)); // constructor canonicalizes Source again
        refs=envelopeHold(refs,envelopePhysicalSourcesCanonical(n,count));
        refs=envelopeHold(refs,envelopeArrays(n,5*count)); // owned resource/source/lineage/checkpoint/manifest fields
        refs=envelopeHold(refs,envelopeArrays(n,count)); // canonicalRef outputs stored by the constructor
        var frame=envelopeHold(parsed,refs);
        // Decode and constructor each cache one canonical key per ref while ordering; no comparator re-encoding.
        final var keys=envelopeArrays(n,count);
        frame=envelopeTemporary(frame,keys);frame=envelopeTemporary(frame,keys);
        final var references=envelopeWritten(keys,n);
        final var refsHash=envelopeHash(references,"nereus-delay-protection-set\0".length());
        frame=envelopeTemporary(frame,refsHash);frame=envelopeTemporary(frame,refsHash);
        frame=envelopeTemporary(frame,envelopeWritten(keys,n));
        frame=envelopeHold(frame,envelopeWritten(keys,n));frame=envelopeHold(frame,envelopeArray(32));
        return envelopeReturn(frame,refs.held+n+32);
    }
    private static EnvelopeMaterial envelopeEvidenceRetireCanonical(){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeCall(envelopeEvidenceExactIdentity(),envelopeArray(envelopeEvidenceIdentityBytes()),0));
        fields=envelopeDrop(fields,envelopeEvidenceProtectionCanonical());fields=envelopeDrop(fields,envelopeEvidenceContextCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceRetireBodyDecode(){
        var args=envelopeNested(24,envelopeTemporary(envelopeProtoRead(24,24,24,2),envelopeShardSubjectDecode()));
        args=envelopeHold(args,envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES,envelopeEvidenceProtectionDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES,envelopeEvidenceContextDecode()));
        var check=envelopeEvidenceContextResourceCheck();check=envelopeTemporary(check,envelopeEvidenceProtectionCanonical());
        // minimumSourcePosition getter is invoked for empty test and again for the bounded Source decode.
        check=envelopeTemporary(check,envelopeArrays(2L*com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES,
                2L*com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTIONS));
        check=envelopeTemporary(check,envelopeRepeat(envelopeSourceDecode(),com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTIONS,false));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES,9,args,
                envelopeReturn(check,0),envelopeEvidenceRetireCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopeEvidenceRetireValidation(){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES;
        var work=envelopeProtoRead(n,n,n,9);
        work=envelopeTemporary(work,envelopeNested(24,envelopeProtoRead(24,24,24,2)));
        work=envelopeTemporary(work,envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode()));
        work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES,envelopeEvidenceProtectionDecode()));
        work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES,envelopeEvidenceContextDecode()));
        // validate() repeats exact kind8 identity validation after all operation-field checks.
        work=envelopeTemporary(work,envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode()));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeEvidenceRetireIdentity(){
        var identity=envelopeCall(envelopeCall(envelopeEvidenceExactIdentity(),envelopeArray(32),0),envelopeArray(32),0);
        var fields=envelopeHold(envelopeArray("nereus-delay-retire-logical-id".length()),envelopeArray(1));
        fields=envelopeHold(fields,identity);fields=envelopeHold(fields,envelopeArray(8));
        return envelopeCall(envelopeEvidenceRetireBodyDecode(),envelopeHash(fields,0),0);
    }


    private static EnvelopeMaterial envelopeEvidenceIntentReferenceCanonical(){return envelopeArray(128);}
    private static EnvelopeMaterial envelopeEvidenceIntentReferenceDecode(){
        var frame=envelopeProtoRead(128,128,128,4); // Target preflight
        var value=envelopeCoreDecoded(128,4,envelopeArrays(96,3),envelopeArrays(96,3),envelopeEvidenceIntentReferenceCanonical(),0);
        return envelopeReturn(envelopeHold(frame,value),96);
    }
    private static EnvelopeMaterial envelopeEvidenceMarkerCanonical(){
        var fields=envelopeDrop(envelopeEvidenceCapacityCanonical(),envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES));
        return envelopeWritten(envelopeDrop(fields,envelopeMutationCanonical(false)),com.nereusstream.delay.protocol.TargetEvidenceRetiredMarker.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceMarkerDecode(){
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetCatalogCapacityOwnerRef.MAX_BYTES,envelopeEvidenceCapacityDecode());
        args=envelopeHold(args,envelopeArrays(296,10));args=envelopeHold(args,envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var own=envelopeHold(envelopeRepeat(envelopeAssigned(32),9,true),envelopeArray(8));
        own=envelopeTemporary(own,envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetEvidenceRetiredMarker.MAX_BYTES,16,args,own,
                envelopeEvidenceMarkerCanonical(),args.held-296-com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceMarkerObservationCanonical(){
        return envelopeWritten(envelopeDrop(envelopeEvidenceMarkerCanonical(),envelopeBusinessTimeCanonical()),
                com.nereusstream.delay.protocol.TargetEvidenceRetiredMarkerEvidence.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceMarkerObservationDecode(){
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetiredMarker.MAX_BYTES,envelopeEvidenceMarkerDecode());
        args=envelopeHold(args,envelopeArrays(112,5));args=envelopeHold(args,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                envelopeTemporary(envelopeProtoRead(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                        TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,10),envelopeBusinessTimeDecode())));
        var own=envelopeHold(envelopeRepeat(envelopeAssigned(32),3,true),envelopeArrays(16,2));
        own=envelopeTemporary(own,envelopeCall(envelopeEvidenceMarkerCanonical(),envelopeArray(32),0));
        own=envelopeTemporary(own,envelopeBusinessTimeCanonical());own=envelopeTemporary(own,envelopeRepeat(envelopeArray(8),2,false));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetEvidenceRetiredMarkerEvidence.MAX_BYTES,10,args,own,
                envelopeEvidenceMarkerObservationCanonical(),args.held-112);
    }
    private static EnvelopeMaterial envelopeEvidenceLegacyDeleteCanonical(){return envelopeWritten(envelopeBusinessTimeCanonical(),4096);}
    private static EnvelopeMaterial envelopeEvidenceLegacyDeleteDecode(){
        // Target preflight reads observedAt, then ExternalDeleteEvidence independently reads all seven fields.
        var preflight=envelopeProtoRead(4096,4096,4096,7);
        preflight=envelopeTemporary(preflight,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                envelopeTemporary(envelopeProtoRead(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                        TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,10),envelopeBusinessTimeDecode())));
        var args=envelopeHold(envelopeArrays(608,5),envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,envelopeBusinessTimeDecode()));
        final var value=envelopeCoreDecoded(4096,7,args,envelopeArrays(608,5),envelopeEvidenceLegacyDeleteCanonical(),args.held-608);
        return envelopeReturn(envelopeHold(preflight,value),value.held);
    }
    private static EnvelopeMaterial envelopeEvidenceConfirmedBodyCanonical(boolean marker){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeEvidenceIntentReferenceCanonical());
        fields=envelopeDrop(fields,marker?envelopeEvidenceMarkerObservationCanonical():envelopeEvidenceLegacyDeleteCanonical());
        fields=envelopeDrop(fields,envelopeBusinessTimeCanonical());
        fields=envelopeDrop(fields,envelopeCall(envelopeEvidenceExactIdentity(),envelopeArray(envelopeEvidenceIdentityBytes()),0));
        return envelopeWritten(envelopeDrop(fields,envelopeEvidenceContextCanonical()),envelopeBusinessConfirmedBodyBytes());
    }
    private static EnvelopeMaterial envelopeEvidenceConfirmedBodyDecode(boolean marker){
        var args=envelopeNested(24,envelopeTemporary(envelopeProtoRead(24,24,24,2),envelopeShardSubjectDecode()));
        args=envelopeHold(args,envelopeNested(128,envelopeEvidenceIntentReferenceDecode()));
        final long evidence=marker?com.nereusstream.delay.protocol.TargetEvidenceRetiredMarkerEvidence.MAX_BYTES:4096;
        args=envelopeHold(args,envelopeNested(evidence,marker?envelopeEvidenceMarkerObservationDecode():envelopeEvidenceLegacyDeleteDecode()));
        args=envelopeHold(args,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                envelopeTemporary(envelopeProtoRead(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                        TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,10),envelopeBusinessTimeDecode())));
        args=envelopeHold(args,envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES,envelopeEvidenceContextDecode()));
        var check=envelopeEvidenceContextResourceCheck();
        check=envelopeTemporary(check,envelopeCall(envelopeEvidenceExactIdentity(),envelopeArray(32),0));
        check=envelopeTemporary(check,envelopeArray(32));check=envelopeTemporary(check,envelopeBusinessTimeCanonical());
        if(marker){
            // marker.requireBindings compares exact resource/intent/closure bytes and the optional original capacity owner.
            check=envelopeTemporary(check,envelopeArrays(32*21+16+8,23));
            check=envelopeTemporary(check,envelopeComparison(envelopeEvidenceCapacityCanonical(),envelopeEvidenceCapacityCanonical()));
            check=envelopeTemporary(check,envelopeRepeat(envelopeMutationAtOrBefore(),2,false));
        }else{
            check=envelopeTemporary(check,envelopeBusinessTimeCanonical());check=envelopeTemporary(check,envelopeEvidenceLegacyDeleteCanonical());
            check=envelopeTemporary(check,envelopeArrays(512+32,2));
            // validateExternalDeleteIdentity constructs and validates exact identity again, then reads its immutable version.
            check=envelopeTemporary(check,envelopeEvidenceExactIdentity());
            check=envelopeTemporary(check,envelopeProtoRead(com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES,
                    com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES,
                    com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES,13));
            check=envelopeTemporary(check,envelopeArray(512));
        }
        return envelopeCoreDecoded(envelopeBusinessConfirmedBodyBytes(),11,args,envelopeReturn(check,0),
                envelopeEvidenceConfirmedBodyCanonical(marker),args.held);
    }
    private static EnvelopeMaterial envelopeEvidenceConfirmedBodyDecode(){return EvidenceConfirmedBodyMaterial.DECODE;}
    private static EnvelopeMaterial calculateEnvelopeEvidenceConfirmedBodyDecode(){
        return envelopeChoice(envelopeEvidenceConfirmedBodyDecode(false),envelopeEvidenceConfirmedBodyDecode(true));
    }
    private static EnvelopeMaterial envelopeEvidenceConfirmedIdentity(){
        return envelopeCall(envelopeEvidenceConfirmedBodyDecode(),envelopeArray(32),0);
    }
    private static EnvelopeMaterial envelopeEvidenceConfirmedValidation(){
        final long n=envelopeBusinessConfirmedBodyBytes();
        var work=envelopeProtoRead(n,n,n,11);
        work=envelopeTemporary(work,envelopeNested(24,envelopeProtoRead(24,24,24,2)));
        work=envelopeTemporary(work,envelopeNested(128,envelopeEvidenceIntentReferenceDecode()));
        final var marker=envelopeTemporary(envelopeProtoRead(com.nereusstream.delay.protocol.TargetEvidenceRetiredMarkerEvidence.MAX_BYTES,
                com.nereusstream.delay.protocol.TargetEvidenceRetiredMarkerEvidence.MAX_BYTES,
                com.nereusstream.delay.protocol.TargetEvidenceRetiredMarkerEvidence.MAX_BYTES,10),envelopeEvidenceMarkerObservationDecode());
        final var old=envelopeTemporary(envelopeProtoRead(4096,4096,4096,10),envelopeEvidenceLegacyDeleteDecode());
        work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.MAX_DELETE_EVIDENCE_BYTES,
                envelopeChoice(marker,old)));
        work=envelopeTemporary(work,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                envelopeTemporary(envelopeProtoRead(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                        TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,10),envelopeBusinessTimeDecode())));
        work=envelopeTemporary(work,envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode()));
        work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES,envelopeEvidenceContextDecode()));
        // validate() independently checks kind8 resource after all typed field validations.
        work=envelopeTemporary(work,envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode()));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeEvidenceClosureFields(){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CLOSURE_BYTES;
        var frame=envelopeProtoRead(n,n,n,4096+18);
        var candidate=envelopeCoreDecoded(40,2,envelopeArray(32),envelopeEmpty(),envelopeEmpty(),0);
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeNested(40,candidate),4096,false));
        frame=envelopeTemporary(frame,envelopeArrays(6*32+32,7));
        // Inventory writer obtains each nested accessor anew, then hashes its complete canonical inventory.
        var inventory=envelopeWritten(envelopeRepeat(envelopeArray(40),4096,false),n);
        frame=envelopeTemporary(frame,envelopeHash(inventory,"nereus-delay-target-evidence-closure-inventory\0".length()));
        frame=envelopeTemporary(frame,envelopeArray(32));return frame;
    }
    private static EnvelopeMaterial envelopeEvidenceClosureCheck(){
        var work=envelopeEvidenceClosureFields();
        work=envelopeTemporary(work,envelopeArrays(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES+7*32+8+4,10));
        work=envelopeTemporary(work,envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeEvidenceCopyPublicInputs(boolean recovery){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_PUBLIC_INPUT_BYTES;
        var work=envelopeProtoRead(n,n,n,recovery?12:8);
        if(recovery){
            work=envelopeTemporary(work,envelopeNested(2048,envelopeProtoRead(2048,2048,2048,11)));
            // The copied proof's original material accessor and complete authorization parser remain separate edges.
            // Its typed public-material parser also bounds this caller's raw originalMaterial/authorization validation.
            work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_PROOF_BYTES,
                    envelopeRecoveryPublicMaterialDecode(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_PROOF_BYTES)));
            // Public history accesses and compares original material/authorization identities and SDK locks.
            work=envelopeTemporary(work,envelopeArrays(n,32));
        }else{
            work=envelopeTemporary(work,envelopeArray(44));
            work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES,
                    envelopeKafkaReceiptArtifactDecode()));
            work=envelopeTemporary(work,envelopeComparison(envelopeKafkaReceiptArtifactCanonical(),envelopeKafkaReceiptArtifactCanonical()));
            work=envelopeTemporary(work,envelopeArrays(64,2));
        }
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeEvidenceCopyObjectCheck(boolean recovery){
        var work=recovery?envelopeRecoveryProofDecode():envelopeKafkaReceiptArtifactDecode();
        if(recovery){
            work=envelopeTemporary(work,envelopeBusinessRecoveryRefCanonical());
            work=envelopeTemporary(work,envelopeArrays(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES+32*8+16,10));
        }else{
            work=envelopeTemporary(work,envelopeCall(envelopeKafkaReceiptArtifactCanonical(),envelopeArray(32),0));
            work=envelopeTemporary(work,envelopeRepeat(envelopeSourceCanonical(),2,true));
            work=envelopeTemporary(work,envelopeArrays(32*5,5));
        }
        return envelopeDrop(work,envelopeEvidenceCopyPublicInputs(recovery));
    }
    private static EnvelopeMaterial envelopeEvidenceRetirementCopyCanonical(){
        return envelopeWritten(envelopeCall(envelopeEvidenceExactIdentity(),envelopeArray(envelopeEvidenceIdentityBytes()),0),
                com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeEvidenceRetirementCopyDecode(){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_CANONICAL_BYTES;
        var args=envelopeNested(envelopeEvidenceIdentityBytes(),envelopeEvidenceResourceOneofDecode());
        // signedObject/publicInputs/closureImage are disjoint payload fields inside this one manifest.
        args=envelopeHold(args,envelopeArrays(n,3));
        var own=envelopeArrays(n,3);own=envelopeTemporary(own,envelopeCall(envelopeArray(n),envelopeArray(32),0));
        own=envelopeTemporary(own,envelopeArray(32));
        own=envelopeTemporary(own,envelopeChoice(envelopeEvidenceCopyObjectCheck(false),envelopeEvidenceCopyObjectCheck(true)));
        own=envelopeTemporary(own,envelopeEvidenceClosureCheck());own=envelopeTemporary(own,envelopeEvidenceExactIdentity());
        return envelopeCoreDecoded(n,5,args,own,envelopeEvidenceRetirementCopyCanonical(),args.held-n);
    }


    private static long envelopeHistoryBytes(){return com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.MAX_BYTES;}
    private static long envelopeHistoryGrants(){return com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.MAX_GRANTS;}
    private static long envelopeHistoryIntervals(){return com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.MAX_INTERVALS;}
    private static EnvelopeMaterial envelopeHistoryGrantCanonical(){
        return envelopeWritten(envelopeDrop(envelopeAuthorCanonical(1024),envelopeProtocolActivationCanonical()),
                com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.MAX_GRANT_BYTES);
    }
    private static EnvelopeMaterial envelopeHistoryGrantDecode(){
        var args=envelopeNested(1024,envelopeAuthorDecode(1024));args=envelopeHold(args,envelopeArray(44));
        args=envelopeHold(args,envelopeNested(4096,envelopeProtocolActivationDecode()));args=envelopeHold(args,envelopeArrays(96,3));
        var own=envelopeHold(envelopeArray(44),envelopeRepeat(envelopeAssigned(32),3,true));
        own=envelopeTemporary(own,envelopeAuthorCanonical(1024));own=envelopeTemporary(own,envelopeProtocolActivationCanonical());
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.MAX_GRANT_BYTES,9,args,own,
                envelopeHistoryGrantCanonical(),args.held-140);
    }
    private static EnvelopeMaterial envelopeHistoryGrantIdentity(){
        final var actor=envelopeCall(envelopeAuthorCanonical(1024),envelopeCall(envelopeArray(4),envelopeArray(1028),0),0);
        var args=envelopeHold(actor,envelopeArrays(8,2));
        var old=envelopeCall(args,envelopeArray(1036),0);
        final var tuple=envelopeCall(envelopeTupleCanonical(),envelopeCall(envelopeArray(4),envelopeArray(30),0),0);
        return envelopeCall(envelopeHold(old,tuple),envelopeArray(1066),0);
    }
    private static EnvelopeMaterial envelopeHistoryPolicyCanonical(){
        // One snapshot has <=128 grants in total, shared by all intervals, never 128 per interval.
        return envelopeWritten(envelopeRepeat(envelopeHistoryGrantCanonical(),envelopeHistoryGrants(),false),envelopeHistoryBytes());
    }
    private static EnvelopeMaterial envelopeHistoryIntervalsCanonical(){
        var fields=envelopeDrop(envelopePhysicalSourcesCanonical(envelopeHistoryBytes(),2*envelopeHistoryIntervals()),envelopeHistoryPolicyCanonical());
        return envelopeDrop(fields,envelopeArrays(envelopeHistoryBytes(),envelopeHistoryIntervals()));
    }
    private static EnvelopeMaterial envelopeHistoryIntervalsDecode(){
        final long n=envelopeHistoryBytes(),intervals=envelopeHistoryIntervals(),grants=envelopeHistoryGrants();
        var work=envelopeCoreReaders(n,intervals,3*intervals);
        work=envelopeHold(work,envelopeArrays(n,intervals)); // complete policy raw accessors
        work=envelopeHold(work,envelopeCoreReaders(n,intervals,grants));
        work=envelopeHold(work,envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.MAX_GRANT_BYTES,
                envelopeHistoryGrantDecode()),grants,true));
        work=envelopeHold(work,envelopeCall(envelopeArrays(n,2*intervals),envelopePhysicalSourcesDecode(n,2*intervals),0));
        work=envelopeTemporary(work,envelopeRepeat(envelopeSourceBounded(),2*intervals,false));
        work=envelopeTemporary(work,envelopeRepeat(envelopeHistoryGrantIdentity(),grants,false));
        // activation.sourcePosition() decodes its stored Source anew for the before-interval check.
        work=envelopeTemporary(work,envelopeRepeat(envelopePhysicalSourcesDecode(4096,1),grants,false));
        work=envelopeTemporary(work,envelopeHistoryIntervalsCanonical());
        return envelopeReturn(work,grants*envelopeHistoryGrantDecode().held+2*intervals*48);
    }
    private static EnvelopeMaterial envelopeHistorySnapshotFields(){
        var work=envelopeDrop(envelopeAuthorCanonical(1024),envelopeScopeCanonical());
        return envelopeWritten(envelopeDrop(work,envelopeHistoryIntervalsCanonical()),envelopeHistoryBytes()-100);
    }
    private static EnvelopeMaterial envelopeHistorySnapshotDigest(){
        return envelopeHash(envelopeHistorySnapshotFields(),"nereus-delay-target-retirement-source-authorization-history\0".length());
    }
    private static EnvelopeMaterial envelopeHistorySnapshotCanonical(){
        return envelopeConcat(envelopeHold(envelopeHistorySnapshotFields(),envelopeWritten(envelopeHistorySnapshotDigest(),100)));
    }
    private static EnvelopeMaterial envelopeHistorySnapshotDecode(){
        var args=envelopeNested(1024,envelopeAuthorDecode(1024));args=envelopeHold(args,envelopeArrays(176,5));
        args=envelopeHold(args,envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode()));
        args=envelopeHold(args,envelopeHistoryIntervalsDecode());
        var own=envelopeHold(envelopeArrays(96,2),envelopeHold(envelopeRepeat(envelopeAssigned(32),2,true),envelopeAssigned(16)));
        own=envelopeTemporary(own,envelopeAuthorCanonical(1024));own=envelopeTemporary(own,envelopeArray(32));
        // Adjacent boundaries compare full Source bytes; adjacent policies are independently encoded.
        own=envelopeTemporary(own,envelopePhysicalSourcesCanonical(envelopeHistoryBytes(),2*(envelopeHistoryIntervals()-1)));
        own=envelopeTemporary(own,envelopeRepeat(envelopeHistoryPolicyCanonical(),2,false));
        var value=envelopeCoreDecoded(envelopeHistoryBytes(),envelopeHistoryIntervals()+11,args,own,envelopeEmpty(),args.held-176);
        value=envelopeTemporary(value,envelopeHistorySnapshotDigest());value=envelopeTemporary(value,envelopeArray(32));
        value=envelopeTemporary(value,envelopeHistorySnapshotCanonical());return value;
    }
    private static EnvelopeMaterial envelopeHistoryExtension(){
        var work=envelopeHistorySnapshotDigest();work=envelopeDrop(work,envelopeArray(32));
        work=envelopeDrop(work,envelopeComparison(envelopeAuthorCanonical(1024),envelopeAuthorCanonical(1024)));
        work=envelopeDrop(work,envelopeArrays(160,6));
        work=envelopeDrop(work,envelopePhysicalSourcesCanonical(envelopeHistoryBytes(),2));
        // Across two sorted disjoint complete interval partitions there are at most old+new overlaps.
        // A policy may be re-encoded for each overlap; this bound retains both sides of each comparison.
        work=envelopeDrop(work,envelopeRepeat(envelopeComparison(envelopeHistoryPolicyCanonical(),envelopeHistoryPolicyCanonical()),
                2*envelopeHistoryIntervals(),false));
        work=envelopeDrop(work,envelopePhysicalSourcesCanonical(envelopeHistoryBytes(),2*(envelopeHistoryIntervals()-1)));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeRetirementPublicProofCanonical(){
        var fields=envelopeDrop(envelopeAuthorCanonical(1024),envelopeRepeat(envelopeHistorySnapshotCanonical(),
                com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.PublicProof.MAX_SNAPSHOTS,false));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.PublicProof.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeRetirementPublicProofDecode(){
        final long count=com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.PublicProof.MAX_SNAPSHOTS;
        var args=envelopeRepeat(envelopeNested(envelopeHistoryBytes(),envelopeHistorySnapshotDecode()),count,true);
        args=envelopeHold(args,envelopeNested(1024,envelopeAuthorDecode(1024)));args=envelopeHold(args,envelopeArrays(76,2));
        var own=envelopeHold(envelopeArray(44),envelopeAssigned(32));
        own=envelopeTemporary(own,envelopeAuthorCanonical(1024));own=envelopeTemporary(own,envelopeHistorySnapshotDigest());
        own=envelopeTemporary(own,envelopeRepeat(envelopeComparison(envelopeAuthorCanonical(1024),envelopeAuthorCanonical(1024)),count,false));
        own=envelopeTemporary(own,envelopeRepeat(envelopeHistoryExtension(),count-1,false));
        return envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.PublicProof.MAX_BYTES,
                count+5,args,own,envelopeRetirementPublicProofCanonical(),args.held-76);
    }
    private static EnvelopeMaterial envelopeRetirementInputDecode(){
        final var retire=envelopeSystemDecode(com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.MAX_INTENT_BYTES,
                com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES,1<<20,
                envelopeEvidenceRetireValidation(),envelopeEvidenceRetireIdentity(),false);
        final var confirmation=envelopeSystemDecode(com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CONFIRMATION_BYTES,
                envelopeBusinessConfirmedBodyBytes(),1<<20,envelopeEvidenceConfirmedValidation(),envelopeEvidenceConfirmedIdentity(),false);
        final var reclaim=envelopeSystemDecode(com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_ENVELOPE_BYTES,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES,1<<20,
                envelopeResourceReclaimValidation(),envelopeResourceReclaimIdentity(),false);
        return envelopeChoice(envelopeChoice(retire,confirmation),reclaim);
    }
    private static EnvelopeMaterial envelopeRetirementSourceSchema(){
        // schemaVersion() decodes type14 for each predicate on the chosen body version (<=12 calls).
        return envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES,
                envelopeResourceReclaimDecode()),12,false);
    }
    private static EnvelopeMaterial envelopeRetirementSourceHistoryFields(){
        var fields=envelopeDrop(envelopeRetirementSourceSchema(),envelopeSystemCanonical(
                com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_ENVELOPE_BYTES));
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));fields=envelopeDrop(fields,envelopeRetirementPublicProofCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeRetirementSourceHistoryCanonical(){
        return envelopeCoreHashed(envelopeRetirementSourceHistoryFields(),com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_BYTES,
                "nereus-delay-target-retirement-source-history-record\0");
    }
    private static EnvelopeMaterial envelopeRetirementSourceHistoryDecode(){return RetirementSourceHistoryMaterial.DECODE;}
    private static EnvelopeMaterial calculateEnvelopeRetirementSourceHistoryDecode(){
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_ENVELOPE_BYTES,envelopeRetirementInputDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.PublicProof.MAX_BYTES,envelopeRetirementPublicProofDecode()));
        var check=envelopeChoice(envelopeResourceReclaimDecode(),envelopeChoice(envelopeEvidenceRetireBodyDecode(),envelopeEvidenceConfirmedBodyDecode()));
        check=envelopeTemporary(check,envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_ENVELOPE_BYTES));
        check=envelopeTemporary(check,envelopeCall(envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_ENVELOPE_BYTES),envelopeArray(32),0));
        check=envelopeTemporary(check,envelopeScopeConstruct());check=envelopeTemporary(check,envelopeArrays(176,6));
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.MAX_BYTES,5,args,
                envelopeReturn(check,0),envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeRetirementSourceSchema());
        value=envelopeTemporary(value,envelopeHash(envelopeRetirementSourceHistoryFields(),"nereus-delay-target-retirement-source-history-record\0".length()));
        value=envelopeTemporary(value,envelopeArray(32));value=envelopeTemporary(value,envelopeRetirementSourceHistoryCanonical());return value;
    }
    private static EnvelopeMaterial envelopeRetirementHistoryTipFields(){
        var work=envelopeDrop(envelopeRetirementPublicProofCanonical(),envelopeMutationCanonical(false));
        return envelopeWritten(work,com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.MAX_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeRetirementHistoryTipCanonical(){
        return envelopeCoreHashed(envelopeRetirementHistoryTipFields(),com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.MAX_BYTES,
                "nereus-delay-target-retirement-accepted-history-tip\0");
    }
    private static EnvelopeMaterial envelopeRetirementHistoryTipDecode(){
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.PublicProof.MAX_BYTES,envelopeRetirementPublicProofDecode());
        args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.MAX_BYTES,5,args,envelopeAssigned(32),
                envelopeEmpty(),args.held-32);
        value=envelopeTemporary(value,envelopeHash(envelopeRetirementHistoryTipFields(),"nereus-delay-target-retirement-accepted-history-tip\0".length()));
        value=envelopeTemporary(value,envelopeArray(32));value=envelopeTemporary(value,envelopeRetirementHistoryTipCanonical());return value;
    }
    private static EnvelopeMaterial envelopeObjectPendingFrameCheck(){
        final long frame=com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_FRAME_BYTES;
        final long body=com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;
        var work=envelopeProtoRead(frame,frame,frame,2);
        work=envelopeHold(work,envelopeNested(frame,envelopeProtoRead(frame,frame,frame,11)));
        work=envelopeHold(work,envelopeNested(body,envelopeProtoRead(body,body,body,11)));
        work=envelopeHold(work,envelopeNested(body,envelopeProtoRead(body,body,body,1)));
        work=envelopeTemporary(work,envelopeNested(body,envelopeProtoRead(body,body,body,9)));return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeObjectPendingFields(){
        var work=envelopeRepeat(envelopeNested(envelopeResourceObjectIntentBytes(),envelopeResourceObjectIntentDecode()),3,false);
        work=envelopeDrop(work,envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_FRAME_BYTES));
        return envelopeWritten(envelopeDrop(work,envelopeMutationCanonical(false)),
                com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeObjectPendingCanonical(){
        final var fields=envelopeObjectPendingFields();
        return envelopeConcat(envelopeHold(fields,envelopeWritten(envelopeHash(envelopeArray(
                com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_CANONICAL_BYTES-34L),
                "nereus-delay-target-payload-object-retiring\0".length()),34)));
    }
    private static EnvelopeMaterial envelopeObjectPendingDecode(){
        final long frame=com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_FRAME_BYTES;
        final var input=envelopeSystemDecode(frame,envelopeResourceObjectIntentBytes(),1<<20,
                envelopeResourceObjectIntentValidation(),envelopeResourceObjectIntentIdentity(),false);
        var args=envelopeNested(frame,input);args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var check=envelopeObjectPendingFrameCheck();check=envelopeTemporary(check,envelopeNested(envelopeResourceObjectIntentBytes(),envelopeResourceObjectIntentDecode()));
        check=envelopeTemporary(check,envelopeSystemCanonical(frame));check=envelopeTemporary(check,envelopeCall(envelopeSystemCanonical(frame),envelopeArray(32),0));
        check=envelopeTemporary(check,envelopeMutationAtOrBefore());
        var value=envelopeCoreDecoded(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_CANONICAL_BYTES,4,args,
                envelopeReturn(check,0),envelopeEmpty(),args.held);
        value=envelopeTemporary(value,envelopeObjectPendingFrameCheck());
        value=envelopeTemporary(value,envelopeRepeat(envelopeNested(envelopeResourceObjectIntentBytes(),envelopeResourceObjectIntentDecode()),3,false));
        value=envelopeTemporary(value,envelopeObjectPendingCanonical());return value;
    }


    private static EnvelopeMaterial envelopeEvidenceCopyContextCheck(){
        var work=envelopeEvidenceContextResourceCheck();
        // Reserved staging reads the original signed object again to compare complete original Source metadata.
        var original=envelopeChoice(envelopeRecoveryProofDecode(),envelopeKafkaReceiptArtifactDecode());
        original=envelopeTemporary(original,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        work=envelopeTemporary(work,original);
        var closure=envelopeEvidenceClosureFields();
        closure=envelopeTemporary(closure,envelopeArray(32));
        final long closureBytes=com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CLOSURE_BYTES;
        closure=envelopeTemporary(closure,new EnvelopeMaterial(closureBytes+"nereus-delay-target-evidence-reference-closure\0".length()+32,
                closureBytes+"nereus-delay-target-evidence-reference-closure\0".length()+32,32,0,2));
        // Context.preparedUsesDigest writes one prepared Use per retained closed use and hashes that image.
        var uses=envelopeWritten(envelopeRepeat(envelopeArray(79),com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES,false),
                83L*com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES);
        uses=envelopeHash(envelopeHold(envelopeArray("nereus-delay-target-evidence-closed-prepared-uses\0".length()),uses),0);
        closure=envelopeTemporary(closure,envelopeComparison(uses,envelopeArray(32)));
        closure=envelopeTemporary(closure,envelopeEvidenceRetirementCopyCanonical());
        closure=envelopeTemporary(closure,envelopeComparison(envelopeArray(32),envelopeHash(envelopeEvidenceRetirementCopyCanonical(),
                "nereus-delay-target-evidence-retirement-copy\0".length())));
        return envelopeReturn(envelopeDrop(work,closure),0);
    }

    // RECOVERY_COPY_MATERIAL_REGION: original RecoveryProof and Kafka receipt CONTROL artifact codec leaves.

    private static EnvelopeMaterial envelopeRecoveryCapabilityBodyCanonical(long n){
        return envelopeWritten(envelopeLegacyBrokerCanonical(n),n);
    }
    private static EnvelopeMaterial envelopeRecoveryCapabilityCanonical(long n){
        return envelopeWritten(envelopeRecoveryCapabilityBodyCanonical(n),n);
    }
    private static EnvelopeMaterial envelopeRecoveryCapabilityDecode(long n){
        final var fields=envelopeProtoRead(n,n,n,7);
        var body=envelopeProtoRead(n,n,n,12);
        var args=envelopeNested(n,envelopeLegacyBrokerDecode(n));args=envelopeHold(args,envelopeArrays(64,2));
        body=envelopeHold(body,envelopeCall(args,envelopeArrays(64,2),args.held-64));
        body=envelopeTemporary(body,envelopeRecoveryCapabilityBodyCanonical(n));
        var values=envelopeHold(envelopeNested(n,body),envelopeArrays(288,2)); // bounded profileId and semantic hash
        var hashArgs=envelopeArray("nereus-delay-profile-semantic\0".length());hashArgs=envelopeHold(hashArgs,envelopeArray(2));
        hashArgs=envelopeHold(hashArgs,envelopeCall(envelopeArray(4),envelopeArray(260),0));
        hashArgs=envelopeHold(hashArgs,envelopeArrays(12,2));
        hashArgs=envelopeHold(hashArgs,envelopeCall(envelopeRecoveryCapabilityBodyCanonical(n),
                envelopeCall(envelopeArray(4),envelopeArray(n+4),0),0));
        var constructor=envelopeHold(envelopeArray(256),envelopeHash(hashArgs,0));
        var frame=envelopeHold(fields,envelopeCall(values,constructor,values.held-288));
        frame=envelopeTemporary(frame,envelopeRecoveryCapabilityCanonical(n));
        return envelopeReturn(frame,values.held);
    }
    private static EnvelopeMaterial envelopeRecoveryReceiptResourceCanonical(long n){
        var fields=envelopeDrop(envelopeArray(n),envelopeArray(16));fields=envelopeDrop(fields,envelopeArray(16));
        return envelopeWritten(fields,n);
    }
    private static EnvelopeMaterial envelopeRecoveryReceiptResourceDecode(long n){
        var args=envelopeArray(n);args=envelopeTemporary(args,envelopeArray(n)); // UTF-8 roundtrip
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeCall(envelopeArray(16),envelopeArray(16),0));
        var constructor=envelopeReturn(envelopeArray(n),0); // constructor NFC roundtrip; no extra byte[] owns the String
        var frame=envelopeHold(envelopeProtoRead(n,n,n,6),envelopeCall(args,constructor,16));
        frame=envelopeTemporary(frame,envelopeRecoveryReceiptResourceCanonical(n));return envelopeReturn(frame,16);
    }
    private static EnvelopeMaterial envelopeReceiptControlRequestCanonical(){
        return envelopeWritten(envelopeDrop(envelopeBusinessChannelCanonical(),
                envelopeOwnerIdentityCanonical(TargetQuotaClaimCharge.MAX_OWNER_BYTES)),
                com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeReceiptControlRequestDecode(){
        final long n=com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES;
        var args=envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode());
        args=envelopeHold(args,envelopeArray(32)); // RENEW predecessor, absent for CREATE
        args=envelopeHold(args,envelopeNested(TargetQuotaClaimCharge.MAX_OWNER_BYTES,
                envelopeOwnerIdentityDecode(TargetQuotaClaimCharge.MAX_OWNER_BYTES)));
        var constructor=envelopeAssigned(32);
        constructor=envelopeTemporary(constructor,envelopeOwnerIdentityCanonical(TargetQuotaClaimCharge.MAX_OWNER_BYTES));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,4),envelopeCall(args,constructor,args.held-32));
        frame=envelopeTemporary(frame,envelopeReceiptControlRequestCanonical());return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopeReceiptPreparedRequestCanonical(){
        return envelopeWritten(envelopeReceiptControlRequestCanonical(),
                com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES+8L);
    }
    private static EnvelopeMaterial envelopeReceiptPreparedFieldWork(){
        return envelopeDrop(envelopeDrop(envelopeArray(102),envelopeReceiptPreparedRequestCanonical()),envelopeMigrationPreparedTargetCanonical());
    }
    private static EnvelopeMaterial envelopeReceiptPreparedFields(){
        return envelopeWritten(envelopeReceiptPreparedFieldWork(),com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_PREPARED_BYTES-106L);
    }
    private static EnvelopeMaterial envelopeReceiptPreparedCanonical(){
        return envelopeWritten(envelopeReceiptPreparedFieldWork(),com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_PREPARED_BYTES);
    }
    private static EnvelopeMaterial envelopeReceiptPreparedDecode(){
        final long branch=com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES,request=branch+8;
        var wrapped=envelopeHold(envelopeProtoRead(request,request,request,1),envelopeNested(branch,envelopeReceiptControlRequestDecode()));
        wrapped=envelopeTemporary(wrapped,envelopeReceiptPreparedRequestCanonical());wrapped=envelopeReturn(wrapped,wrapped.held-request);
        var args=envelopeNested(102,envelopeMigrationControlAuthorDecode());args=envelopeHold(args,envelopeNested(request,wrapped));
        args=envelopeHold(args,envelopeNested(135,envelopeMigrationPreparedTargetDecode()));args=envelopeHold(args,envelopeArrays(192,5));
        var constructor=envelopeArrays(192,5);
        constructor=envelopeTemporary(constructor,envelopePreparedRequestHash(envelopeReceiptControlRequestCanonical(),branch,26));
        constructor=envelopeTemporary(constructor,envelopeArrays(64,2)); // single exact Shard target mutation getters
        var targets=envelopeWritten(envelopeMigrationPreparedTargetCanonical(),138);
        targets=envelopeCall(targets,envelopeCall(envelopeArray(4),envelopeArray(142),0),0);
        constructor=envelopeTemporary(constructor,envelopeHash(targets,"nereus-delay-control-target-snapshot\0".length()));
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeReceiptPreparedFields(),"nereus-delay-prepared-control\0".length()));
        final long retained=args.held-192;
        var frame=envelopeHold(envelopeProtoRead(com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_PREPARED_BYTES,
                com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_PREPARED_BYTES,
                com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_PREPARED_BYTES,13),envelopeCall(args,constructor,retained));
        frame=envelopeTemporary(frame,envelopeReceiptPreparedCanonical());return envelopeReturn(frame,retained+192);
    }
    private static EnvelopeMaterial envelopeReceiptProtectionCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptProtection.MAX_CANONICAL_BYTES;
        var fields=envelopeDrop(envelopeRecoveryReceiptResourceCanonical(n),envelopeArray(TargetKafkaReceiptBinding.MAX_TOPIC_BYTES));
        fields=envelopeDrop(fields,envelopeLegacyProfileCanonical(n));return envelopeWritten(fields,n);
    }
    private static EnvelopeMaterial envelopeReceiptProtectionDecode(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptProtection.MAX_CANONICAL_BYTES,
                policy=com.nereusstream.delay.protocol.TargetKafkaReceiptProtection.MAX_POLICY_BYTES,
                management=com.nereusstream.delay.protocol.TargetKafkaReceiptProtection.MAX_MANAGEMENT_BYTES;
        var args=envelopeNested(n,envelopeRecoveryReceiptResourceDecode(n));
        args=envelopeHold(args,envelopeNested(n,envelopeLegacyProfileDecode(n)));
        final long nested=args.held;
        args=envelopeHold(args,envelopeArrays(48,2));args=envelopeHold(args,envelopeArrays(policy+management,2));
        var constructor=envelopeHold(envelopeAssigned(48,2),envelopeArrays(policy+management,2));
        // Reused TargetKafkaReceiptBinding constructor: two assigned digest arguments and cluster UTF-8 bound check.
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeAssigned(32),2,true));
        constructor=envelopeTemporary(constructor,envelopeArray(n));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,12),envelopeArrays(2L*TargetKafkaReceiptBinding.MAX_TOPIC_BYTES,2));
        frame=envelopeHold(frame,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeReceiptProtectionCanonical());
        frame=envelopeTemporary(frame,envelopeArray(32)); // requireChannel control scope getter
        return envelopeReturn(frame,nested+48+policy+management);
    }
    private static EnvelopeMaterial envelopeReceiptManagementCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage.MAX_CANONICAL_BYTES;
        var fields=envelopeDrop(envelopeLegacyBrokerCanonical(n),envelopeArray(TargetKafkaReceiptBinding.MAX_TOPIC_BYTES));
        fields=envelopeDrop(fields,envelopeRecoveryCapabilityCanonical(n));
        fields=envelopeDrop(fields,envelopeArrays(n,64)); // the two principal lists share the management image bound
        return envelopeWritten(fields,n);
    }
    private static EnvelopeMaterial envelopeReceiptManagementDecode(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage.MAX_CANONICAL_BYTES;
        var frame=envelopeProtoRead(n,n,n,73);
        frame=envelopeTemporary(frame,envelopeArrays(n,65));frame=envelopeTemporary(frame,envelopeArrays(n,65)); // text raw and UTF-8 roundtrips
        var args=envelopeNested(n,envelopeLegacyBrokerDecode(n));
        args=envelopeHold(args,envelopeNested(n,envelopeRecoveryCapabilityDecode(n)));final long nested=args.held;
        args=envelopeHold(args,envelopeArrays(112,4));args=envelopeHold(args,envelopeArray(2048));
        final var constructor=envelopeHold(envelopeAssigned(112,4),envelopeArray(2048));
        frame=envelopeHold(frame,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeReceiptManagementCanonical());
        return envelopeReturn(frame,nested+112+2048);
    }
    private static EnvelopeMaterial envelopeReceiptAnchorDecode(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_ANCHOR_BYTES;
        var frame=envelopeProtoRead(n,n,n,14);
        frame=envelopeTemporary(frame,envelopeNested(n,envelopeRecoveryReceiptResourceDecode(n)));
        frame=envelopeTemporary(frame,envelopeArrays(96,3));return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeKafkaReceiptArtifactUnsigned(){
        return envelopeWritten(envelopeDrop(envelopeReceiptPreparedCanonical(),envelopeSourceCanonical()),
                com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES-100L);
    }
    private static EnvelopeMaterial envelopeKafkaReceiptArtifactCanonical(){
        return envelopeConcat(envelopeHold(envelopeKafkaReceiptArtifactUnsigned(),envelopeArray(100)));
    }
    private static EnvelopeMaterial envelopeKafkaReceiptArtifactDecode(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES,
                image=com.nereusstream.delay.protocol.TargetKafkaReceiptProtection.MAX_CANONICAL_BYTES,
                prepared=com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_PREPARED_BYTES,
                source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        var args=envelopeArrays(image+32,2);
        args=envelopeHold(args,envelopeNested(prepared,envelopeReceiptPreparedDecode()));
        args=envelopeHold(args,envelopeNested(source,envelopeSourceDecode()));args=envelopeHold(args,envelopeArray(64));
        final long nested=args.held-image-32-64;
        var constructor=envelopeArrays(image+32+64,3);
        constructor=envelopeTemporary(constructor,envelopeSourceBounded());
        constructor=envelopeTemporary(constructor,envelopeReceiptPreparedCanonical());
        var branch=envelopeReceiptProtectionDecode();
        branch=envelopeChoice(branch,envelopeTemporary(envelopeAssigned(32),envelopeReceiptAnchorDecode()));
        branch=envelopeChoice(branch,envelopeTemporary(envelopeReceiptManagementDecode(),envelopeArrays(64,2)));
        constructor=envelopeTemporary(constructor,branch);
        constructor=envelopeTemporary(constructor,envelopeKafkaReceiptArtifactUnsigned()); // MANAGEMENT finite size check
        final var signed=envelopeHash(envelopeHold(envelopeArray("nereus-delay-target-kafka-receipt-protection\0".length()),
                envelopeKafkaReceiptArtifactUnsigned()),0);
        final long anchorDomain="nereus-delay-target-receipt-anchor\0".length(),
                anchorImage=com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.MAX_ANCHOR_BYTES;
        final var anchorHash=new EnvelopeMaterial(anchorImage+2*anchorDomain+32,anchorImage+2*anchorDomain+32,32,0,3);
        constructor=envelopeHold(constructor,envelopeChoice(signed,anchorHash));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,8),envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeKafkaReceiptArtifactCanonical());
        return envelopeReturn(frame,nested+constructor.held);
    }
    private static EnvelopeMaterial envelopeRecoveryAuthorizationFields(int through){
        final long n=com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.MAX_BYTES;
        final long byteFields=through-(through>=29?9:through>=24?6:5);
        return envelopeWritten(envelopeArrays(n,byteFields),n);
    }
    private static EnvelopeMaterial envelopeRecoveryAuthorizationDecode(){
        final long n=com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.MAX_BYTES;
        var frame=envelopeProtoRead(n,n,n,31);
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),22,false)); // eleven fixed/nonzero checks reread value()
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),3,false)); // optional mapping digest's length/nonzero checks
        var values=envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode());
        values=envelopeHold(values,envelopeNested(CanonicalTargetPartition.MAX_CANONICAL_BYTES,envelopeBusinessIdentityDecode()));
        values=envelopeHold(values,envelopeNested(n,envelopeRecoveryReceiptResourceDecode(n)));
        values=envelopeHold(values,envelopeNested(n,envelopeLegacyTimeDecode(n)));
        frame=envelopeHold(frame,values);
        frame=envelopeTemporary(frame,envelopeArrays(64,2)); // control scope raw/getter
        frame=envelopeTemporary(frame,envelopeComparison(envelopeBusinessProducerIdentity(),envelopeArray(74)));
        // topic(9), topic(11), verifier text(27): raw accessors and UTF-8 roundtrips.
        frame=envelopeTemporary(frame,envelopeArrays(2L*TargetKafkaReceiptBinding.MAX_TOPIC_BYTES+128,3));
        frame=envelopeTemporary(frame,envelopeArrays(2L*TargetKafkaReceiptBinding.MAX_TOPIC_BYTES+128,3));
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(128),2,false));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeHash(envelopeArray(128),0),envelopeArray(32)));
        frame=envelopeHold(frame,envelopeHash(envelopeRecoveryAuthorizationFields(29),"nereus-delay-target-recovery-credential\0".length()));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeArray(64));
        frame=envelopeTemporary(frame,envelopeRecoveryAuthorizationFields(31));
        frame=envelopeHold(frame,envelopeArray(n)); // immutable encoded copy; parsed fields remain retained too
        return envelopeReturn(frame,Math.addExact(Math.multiplyExact(2,n),Math.addExact(values.held,32)));
    }
    private static EnvelopeMaterial envelopeRecoveryAuthorizationBinding(){
        var work=envelopeComparison(envelopeBusinessChannelCanonical(),envelopeBusinessChannelCanonical());
        work=envelopeDrop(work,envelopeComparison(envelopeBusinessIdentityCanonical(),envelopeBusinessIdentityCanonical()));
        work=envelopeDrop(work,envelopeArrays(2L*TargetKafkaReceiptBinding.MAX_TOPIC_BYTES,2));
        work=envelopeDrop(work,envelopeArrays(2L*TargetKafkaReceiptBinding.MAX_TOPIC_BYTES,2));
        work=envelopeDrop(work,envelopeRepeat(envelopeArray(32),2,false)); // optional original mapping digest value() checks
        work=envelopeDrop(work,envelopeHash(envelopeBusinessMappingCanonical(),0));
        work=envelopeDrop(work,envelopeArrays(128,4)); // runtime deployment and tenant raw/getter pairs
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeRecoveryMaterialScopeCanonical(){
        var fields=envelopeArray(32); // authorization.digest()
        fields=envelopeDrop(fields,envelopeHash(envelopeRecoveryAuthorizationFields(24),0));
        fields=envelopeDrop(fields,envelopeRepeat(envelopeCall(envelopeArray(32),envelopeArray(32),0),6,false));
        return envelopeWritten(fields,2+8L*34+11);
    }
    private static EnvelopeMaterial envelopeRecoveryPublicMaterialDecode(long n){
        var frame=envelopeProtoRead(n,n,n,7);
        frame=envelopeHold(frame,envelopeNested(com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.MAX_BYTES,
                envelopeRecoveryAuthorizationDecode()));
        frame=envelopeTemporary(frame,envelopeRecoveryAuthorizationBinding());
        return envelopeReturn(frame,frame.held-n);
    }
    private static EnvelopeMaterial envelopeRecoveryMappingId(){
        return envelopeHash(envelopeBusinessMappingFields(),"nereus-delay-target-kafka-receipt-mapping\0".length());
    }
    private static EnvelopeMaterial envelopeRecoveryWireKey(){
        return envelopeHash(envelopeRecoveryMappingId(),"nereus-delay-target-kafka-receipt-key\0".length());
    }
    private static EnvelopeMaterial envelopeRecoveryWireValue(){
        var fields=envelopeRecoveryMappingId();fields=envelopeDrop(fields,envelopeArrays(64,2)); // publication attempt and prepared hash
        fields=envelopeDrop(fields,envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeHash(envelopeBusinessProducerIdentity(),0));
        fields=envelopeDrop(fields,envelopeHash(envelopeKafkaReceiptBindingCanonical(),0));
        final long body=2+34+11+34+34+com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES+4+34+34;
        return envelopeConcat(envelopeHold(envelopeArray("nereus-delay-target-kafka-receipt-value\0".length()),envelopeWritten(fields,body)));
    }
    private static EnvelopeMaterial envelopeRecoveryWireRecordHash(){
        var key=envelopeCall(envelopeRecoveryWireKey(),envelopeCall(envelopeArray(4),envelopeArray(36),0),0);
        final var value=envelopeRecoveryWireValue();
        final var framedValue=envelopeCall(value,envelopeCall(envelopeArray(4),envelopeArray(value.held+4),0),0);
        return envelopeHash(envelopeHold(key,framedValue),"nereus-delay-target-kafka-receipt-wire-record\0".length());
    }
    private static EnvelopeMaterial envelopeRecoveryWireScope(){
        var args=envelopeHold(envelopeArray(16),envelopeHash(envelopeBusinessProducerIdentity(),0));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));
        return envelopeCall(args,constructor,0);
    }
    private static EnvelopeMaterial envelopeRecoveryWireCursorBinding(){
        var work=envelopeArray(16); // exact receipt topic UUID ByteBuffer
        work=envelopeTemporary(work,envelopeComparison(envelopeCall(envelopeRecoveryWireScope(),envelopeCursorScopeCanonical(),0),
                envelopeCursorScopeCanonical()));
        work=envelopeTemporary(work,envelopeArray(16));return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeRecoveryWirePublished(long n){
        var fields=envelopeEvidenceCursorCanonical(n);
        fields=envelopeDrop(fields,envelopeCall(envelopeCall(envelopeArray(32),envelopeArray(32),0),envelopeExternalDeliveryCanonical(),0));
        fields=envelopeDrop(fields,envelopeArray(32));fields=envelopeDrop(fields,envelopeBusinessBrokerCanonical());
        fields=envelopeDrop(fields,envelopeHash(envelopeBusinessProducerIdentity(),0));
        fields=envelopeDrop(fields,envelopeRecoveryWireRecordHash());
        var branch=envelopeWritten(fields,n);
        // PublishEvidence.create validates/copies its branch twice; each validation traverses the selected receipt graph.
        var create=envelopeDrop(envelopeProtoRead(n,n,n,8),envelopeArray(n));
        create=envelopeTemporary(create,envelopePublishEvidenceBranch(n,1,false));
        create=envelopeHold(create,envelopeHash(envelopeArrays(4,2),"nereus-delay-publish-evidence\0".length()+n));
        var constructor=envelopeHold(envelopeArray(32),envelopeDrop(envelopeProtoRead(n,n,n,8),envelopeArray(n)));
        constructor=envelopeTemporary(constructor,envelopePublishEvidenceBranch(n,1,false));
        create=envelopeCall(create,constructor,0);
        create=envelopeCall(create,envelopeArray(n),0); // canonicalBytes()
        branch=envelopeCall(branch,create,0);
        return envelopeReturn(envelopeDrop(envelopeRecoveryWireCursorBinding(),branch),n);
    }
    private static EnvelopeMaterial envelopeRecoveryWireRequire(long n,boolean published){
        var frame=envelopeNested(n,envelopeProtoRead(n,n,n,published?8:6)); // evidence.branch() getter and Reader
        frame=envelopeHold(frame,envelopeNested(n,envelopeEvidenceCursorDecode(n)));
        if(published){
            frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(n),envelopeRecoveryWirePublished(n)));
        }else{
            // requireOrdinaryTargetNotPublishedBinding invokes the owner, a separate cursor and the fenced Target channel.
            var binding=envelopeDrop(envelopeArray(32),envelopePublishEvidenceOwnerCheck(n));
            binding=envelopeHold(binding,envelopeProtoRead(n,n,n,6));
            binding=envelopeHold(binding,envelopeNested(n,envelopeEvidenceCursorDecode(n)));
            binding=envelopeHold(binding,envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode()));
            binding=envelopeTemporary(binding,envelopeArrays(64,3)); // cursor accounting/producer and admitted accounting getters
            binding=envelopeTemporary(binding,envelopeHash(envelopeBusinessProducerIdentity(),0));
            binding=envelopeTemporary(binding,envelopeArrays(32,2)); // fenced accounting-incarnation pair
            binding=envelopeTemporary(binding,envelopeComparison(envelopeBusinessProducerIdentity(),envelopeBusinessProducerIdentity()));
            binding=envelopeTemporary(binding,envelopeArrays(96,3)); // prepared hash pair and producer hash field
            binding=envelopeTemporary(binding,envelopeHash(envelopeBusinessProducerIdentity(),0));
            frame=envelopeTemporary(frame,binding);frame=envelopeTemporary(frame,envelopeRecoveryWireCursorBinding());
        }
        return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeRecoveryWireCursor(long n,boolean published){
        var work=envelopeRecoveryWireRequire(n,published);
        work=envelopeDrop(work,envelopeNested(n,envelopeProtoRead(n,n,n,published?8:6)));
        return envelopeCall(work,envelopeNested(n,envelopeEvidenceCursorDecode(n)),0);
    }
    private static long envelopeRecoveryFetchBytes(){
        // Eighteen closed fields: cluster/topic text, source commit, UUID, two digests and bounded scalar encodings.
        return 128L+CanonicalTargetPartition.MAX_CLUSTER_BYTES+TargetKafkaReceiptBinding.MAX_TOPIC_BYTES+40+16+64;
    }
    private static EnvelopeMaterial envelopeRecoveryFetchDecode(long totalBytes,long count){
        if(count==0)return envelopeEmpty();
        final var parsed=envelopeProtoRead(totalBytes,totalBytes,totalBytes,18*count);
        var frame=new EnvelopeMaterial(parsed.allocated,parsed.retained,parsed.held,parsed.fieldVisits,
                Math.addExact(parsed.arrayObjects,count-1));
        frame=envelopeTemporary(frame,envelopeArrays(totalBytes,6*count)); // six Query.bytes/fixed accessors per Fetch
        frame=envelopeTemporary(frame,envelopeArrays(totalBytes,2*count)); // expected cluster and topic UTF-8 images
        frame=envelopeTemporary(frame,envelopeArrays(72*count,2*count)); // proof sourceCommit and SDK-lock value() accessors
        frame=envelopeTemporary(frame,envelopeArrays(16*count,count)); // exact receipt UUID ByteBuffer
        return envelopeReturn(frame,totalBytes); // requireFetch returns its parsed field list
    }
    private static EnvelopeMaterial envelopeRecoveryTranscript(long totalBytes,long count){
        if(count==0)return envelopeHash(envelopeArray("nereus-delay-target-receipt-scan\0".length()),0);
        var frame=envelopeHash(envelopeArray("nereus-delay-target-receipt-scan\0".length()),0);
        final var read=envelopeProtoRead(totalBytes,totalBytes,totalBytes,4*count);
        final var segments=new EnvelopeMaterial(read.allocated,read.retained,read.held,read.fieldVisits,
                Math.addExact(read.arrayObjects,count-1));
        var loop=envelopeCall(envelopeArrays(totalBytes,count),segments,0);
        loop=envelopeHold(loop,envelopeCall(envelopeArrays(totalBytes,count),envelopeRecoveryFetchDecode(totalBytes,count),0));
        // Each fold allocates lp32's u32/concatenation and SHA-256's concatenation/digest.
        // Segment lengths share totalBytes; only the fixed 76 bytes and four arrays repeat per segment.
        final var folds=new EnvelopeMaterial(2*totalBytes+76*count,2*totalBytes+76,32,0,4*count);
        loop=envelopeTemporary(loop,folds);frame=envelopeTemporary(frame,loop);
        return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeRecoveryStoreContext(long n){
        var frame=envelopeProtoRead(n,n,n,7);
        // StoreMetadata.decode has fixed 72 input bytes, three slices and three independently owned identity arrays.
        var store=envelopeArrays(64,3);store=envelopeCall(store,envelopeArrays(64,3),0);
        frame=envelopeHold(frame,envelopeNested(72,store));
        frame=envelopeHold(frame,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeHash(envelopeBusinessMappingCanonical(),0)));
        frame=envelopeTemporary(frame,envelopeArray(32));return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeRecoveryObservationDecode(boolean published){
        final long n=com.nereusstream.delay.protocol.TargetRecoveryProof.MAX_CANONICAL_BYTES,
                count=n/(40+16+64+18L); // each disjoint Fetch carries source commit, UUID, two hashes and eighteen tags
        var frame=envelopeNested(n,envelopeProtoRead(n,n,n,count+11)); // proof.value(10), actual observation Reader
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeRecoveryMappingId()));
        var fence=envelopeProtoRead(n,n,n,9);
        fence=envelopeTemporary(fence,envelopeComparison(envelopeArray(32),envelopeRecoveryMappingId()));
        fence=envelopeTemporary(fence,envelopeComparison(envelopeArray(TargetChannelIdentity.MAX_CANONICAL_BYTES),envelopeBusinessChannelCanonical()));
        fence=envelopeTemporary(fence,envelopeComparison(envelopeArray(n),envelopeOwnerIdentityCanonical(n)));
        fence=envelopeTemporary(fence,envelopeArrays(64,2));fence=envelopeTemporary(fence,envelopeArrays(80,2));
        fence=envelopeTemporary(fence,envelopeArray(16));
        fence=envelopeTemporary(fence,envelopeNested(n,envelopeRecoveryStoreContext(n)));
        frame=envelopeHold(frame,envelopeNested(n,fence));
        if(published){
            frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(n),envelopeArray(n))); // observation's retained evidence image
            frame=envelopeHold(frame,envelopeRecoveryTranscript(n,count));
            frame=envelopeHold(frame,envelopeNested(envelopeRecoveryFetchBytes(),envelopeRecoveryFetchDecode(envelopeRecoveryFetchBytes(),1)));
            frame=envelopeHold(frame,envelopeRecoveryWireCursor(n,true));
            frame=envelopeTemporary(frame,envelopeNested(n,envelopeProtoRead(n,n,n,8))); // published receipt offset rereads evidence.branch()
            frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeRecoveryWireKey()));
            frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(envelopeRecoveryWireValue().held),envelopeRecoveryWireValue()));
        }else{
            frame=envelopeHold(frame,envelopeRecoveryWireCursor(n,false));
            frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(n),envelopeArray(n)));
            frame=envelopeHold(frame,envelopeRecoveryTranscript(n,count));
            frame=envelopeTemporary(frame,envelopeArray(32)); // folded digest equality's received field
        }
        frame=envelopeHold(frame,envelopeNested(n,envelopeRecoveryPublicMaterialDecode(n)));
        frame=envelopeTemporary(frame,envelopeNested(n,envelopeProtoRead(n,n,n,10))); // fence material scope projection
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(n),envelopeRecoveryMaterialScopeCanonical()));
        return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeRecoveryAdmissionFrameDecode(){
        // The decoder runs before mapping.requireOriginal rejects a different Source operation.
        // The full finite Source dispatcher has no RecoveryProof/Copy back-edge.
        return envelopeSystemFrameDecode(com.nereusstream.delay.protocol.TargetRecoveryProof.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeRecoveryOriginalAdmission(){
        final long envelope=com.nereusstream.delay.protocol.TargetRecoveryProof.MAX_CANONICAL_BYTES-16L,
                body=com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_KAFKA_RECEIPT_BYTES;
        var work=envelopeComparison(envelopeArray(body),envelopePublishAdmissionCanonical(6));
        work=envelopeDrop(work,envelopeArrays(64,2));
        work=envelopeDrop(work,envelopeComparison(envelopeHash(envelopeSystemCanonical(envelope),0),envelopeArray(32)));
        return envelopeReturn(envelopeDrop(work,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical())),0);
    }
    private static EnvelopeMaterial envelopeRecoveryAttemptBinding(){
        var work=envelopeComparison(envelopeIdentityCanonical(),envelopeIdentityCanonical());
        work=envelopeDrop(work,envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()));
        work=envelopeDrop(work,envelopeArrays(128,4)); // attempt/frozen tenant and proof value(12) pairs
        work=envelopeDrop(work,envelopeArrays(64,4)); // attempt/frozen recovery lineage and mapping getters
        work=envelopeDrop(work,envelopeArrays(128,4)); // attempt ID and Admission digest getter pairs
        work=envelopeDrop(work,envelopeComparison(envelopeMessageLocatorCanonical(),envelopeMessageLocatorCanonical()));
        work=envelopeDrop(work,envelopeRepeat(envelopeComparison(envelopeVectorCanonical(),envelopeVectorCanonical()),2,false));
        return envelopeReturn(envelopeDrop(work,envelopeMutationAtOrBefore()),0);
    }
    private static EnvelopeMaterial envelopeRecoveryProofFields(int through){
        final long n=com.nereusstream.delay.protocol.TargetRecoveryProof.MAX_CANONICAL_BYTES;
        return envelopeWritten(envelopeArrays(n,through-3L),n);
    }
    private static EnvelopeMaterial envelopeRecoveryProofCanonical(){
        // The public method returns the stored canonical image; validation's canonical(through) is a distinct writer below.
        return envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProof.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeRecoveryProofDecode(){
        final long n=com.nereusstream.delay.protocol.TargetRecoveryProof.MAX_CANONICAL_BYTES;
        var frame=envelopeProtoRead(n,n,n,19);
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),6,false)); // three fixed/nonzero digest fields, each value() twice
        frame=envelopeTemporary(frame,envelopeArray(64));
        var values=envelopeNested(com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.MAX_CANONICAL_BYTES,envelopeBusinessMappingDecode());
        values=envelopeHold(values,envelopeNested(n,envelopeRecoveryAdmissionFrameDecode()));
        values=envelopeTemporary(values,envelopeRecoveryOriginalAdmission());
        values=envelopeHold(values,envelopeNested(n,envelopePublishEvidenceDecode(n)));
        values=envelopeHold(values,envelopeNested(n,envelopeOwnerIdentityDecode(n)));
        values=envelopeHold(values,envelopeNested(n,envelopeLegacyTimeDecode(n)));
        values=envelopeHold(values,envelopeNested(n,envelopeAuthorDecode(n)));
        frame=envelopeHold(frame,values);
        frame=envelopeTemporary(frame,envelopeArray(40)); // sourceCommit() raw accessor, String/regex has no explicit byte[] output
        frame=envelopeHold(frame,envelopeNested(n,envelopeRecoveryCapabilityDecode(n)));
        frame=envelopeTemporary(frame,envelopeArrays(288,2)); // capability.ref() creates a ProfileRef projection
        frame=envelopeHold(frame,envelopeNested(TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES,
                envelopeChoice(envelopeAttemptDecode(1),envelopeAttemptDecode(2))));
        frame=envelopeHold(frame,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        frame=envelopeTemporary(frame,envelopeRecoveryAttemptBinding());
        frame=envelopeTemporary(frame,envelopeChoice(envelopeRecoveryWireRequire(n,true),envelopeRecoveryWireRequire(n,false)));
        frame=envelopeTemporary(frame,envelopeChoice(envelopeRecoveryObservationDecode(true),envelopeRecoveryObservationDecode(false)));
        frame=envelopeHold(frame,envelopeHash(envelopeRecoveryProofFields(17),"nereus-delay-target-recovery-proof\0".length()));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeRecoveryProofFields(19));
        frame=envelopeHold(frame,envelopeArray(n)); // immutable encoded, alongside the parsed field list and typed members
        return envelopeReturn(frame,2*n+values.held+32);
    }

    // MIGRATION_MATERIAL_REGION: migration and archive codec graph insertion point.
    /* These are byte-array graphs, not JVM/provider qualifications. A protobuf field inventory is
       bounded by its containing image (tags/lengths only make that bound smaller). Nested readers,
       constructor ownership, canonical rechecks and digest preimages remain separate call edges. */
    private static EnvelopeMaterial envelopeMigrationDecoded(long encoded,long largest,long fieldCount,EnvelopeMaterial arguments,
            EnvelopeMaterial constructor,EnvelopeMaterial canonical){
        final var parsed=envelopeProtoRead(encoded,encoded,Math.min(encoded,largest),fieldCount);
        var frame=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held));
        frame=envelopeTemporary(frame,canonical);
        return envelopeReturn(frame,Math.addExact(arguments.held,constructor.held));
    }
    private static EnvelopeMaterial envelopeMigrationHashed(EnvelopeMaterial fields,long encoded,String domain){
        // The older codecs call fields() once for writeBytes and again inside digest().
        return envelopeWritten(envelopeDrop(fields,envelopeHash(fields,domain.length())),encoded);
    }
    private static EnvelopeMaterial envelopeMigrationLocalHashed(EnvelopeMaterial fields,long encoded,String domain){
        // Native72/ArchiveState keep their one local fields array through the outer message writer.
        // New versions stream the already-existing domain/fields into SHA-256; no concat image.
        final var digest=envelopeArray(32);
        return envelopeCall(envelopeHold(fields,digest),envelopeArray(encoded),0);
    }
    private static EnvelopeMaterial envelopeMigrationDigestCheck(EnvelopeMaterial fields,String domain){
        return envelopeComparison(envelopeHash(fields,domain.length()),envelopeArray(32));
    }
    private static EnvelopeMaterial envelopeMigrationSha(EnvelopeMaterial image){
        // Bytes.sha256(byte[]) has no varargs concat preimage.
        return envelopeCall(image,envelopeArray(32),0);
    }
    private static long envelopeMigrationObjectBytes(){return com.nereusstream.delay.protocol.TargetMigrationObjectReference.MAX_CANONICAL_BYTES;}
    private static long envelopeMigrationRecordBytes(){return com.nereusstream.delay.protocol.TargetMigrationRecordReference.MAX_CANONICAL_BYTES;}
    private static EnvelopeMaterial envelopeMigrationObjectCanonical(){
        // CheckpointResource writes its nested ProfileRef, then the complete exact resource image.
        return envelopeWritten(envelopeArray(envelopeMigrationObjectBytes()),envelopeMigrationObjectBytes());
    }
    private static EnvelopeMaterial envelopeMigrationObjectDecode(){
        final long n=envelopeMigrationObjectBytes();
        // ObjectReference first checks its eight-field wrapper; CheckpointResource reads it again.
        var frame=envelopeProtoRead(n,n,n,8);
        final var profile=envelopeMigrationDecoded(n,n,4,envelopeArray(n),envelopeArray(n),envelopeArray(n));
        var args=envelopeNested(n,profile);
        args=envelopeHold(args,envelopeArray(n)); // fixed IDs/hash and container/key/version, aggregate <= n
        var resource=envelopeMigrationDecoded(n,n,8,args,envelopeArray(n),envelopeMigrationObjectCanonical());
        // ObjectReference ctor checks resource.canonicalBytes; its decoder checks canonical once more.
        resource=envelopeTemporary(resource,envelopeMigrationObjectCanonical());
        resource=envelopeTemporary(resource,envelopeMigrationObjectCanonical());
        return envelopeReturn(envelopeHold(frame,resource),resource.held);
    }
    private static EnvelopeMaterial envelopeMigrationRecordCanonical(){
        return envelopeWritten(envelopeMigrationObjectCanonical(),envelopeMigrationRecordBytes());
    }
    private static EnvelopeMaterial envelopeMigrationRecordDecode(){
        return envelopeMigrationDecoded(envelopeMigrationRecordBytes(),envelopeMigrationObjectBytes(),3,
                envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()),envelopeEmpty(),envelopeMigrationRecordCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationPlanCanonical(){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeRepeat(envelopeMigrationObjectCanonical(),6,false));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationPlan.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationPlanDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationPlan.MAX_CANONICAL_BYTES;
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        // migration/predecessor/old+new Store/DB/lineage/management IDs.
        args=envelopeHold(args,envelopeArray(32+32+16+32+16+16+32+16+32));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()),6,true));
        var ctor=envelopeRepeat(envelopeAssigned(32),5,true);
        ctor=envelopeHold(ctor,envelopeRepeat(envelopeAssigned(16),4,true));
        ctor=envelopeTemporary(ctor,envelopeArray(16)); // oldManifest.resource().recoveryLineageId()
        return envelopeMigrationDecoded(n,envelopeMigrationObjectBytes(),21,args,ctor,envelopeMigrationPlanCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationIssuerFields(){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeTupleCanonical(),9,false));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_BYTES-102L);
    }
    private static EnvelopeMaterial envelopeMigrationIssuerCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_BYTES;
        final var suffix=envelopeWritten(envelopeHash(envelopeMigrationIssuerFields(),
                "nereus-delay-migration-root-issuer-authorization-v1\0".length()),102);
        return envelopeCall(envelopeHold(envelopeMigrationIssuerFields(),suffix),envelopeArray(n),0);
    }
    private static EnvelopeMaterial envelopeMigrationIssuerDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_BYTES;
        var args=envelopeRepeat(envelopeArray(32),6,true);
        args=envelopeHold(args,envelopeRepeat(envelopeArray(16),2,true));
        args=envelopeHold(args,envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(26,envelopeTupleDecode()),9,true));
        args=envelopeHold(args,envelopeArray(com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_HISTORY_BYTES));
        args=envelopeHold(args,envelopeArray(64));
        var ctor=envelopeHold(envelopeRepeat(envelopeAssigned(32),6,true),envelopeRepeat(envelopeAssigned(16),2,true));
        ctor=envelopeTemporary(ctor,envelopeSourceBounded());
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeTupleCanonical(),9,false));
        ctor=envelopeHold(ctor,envelopeArray(com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_HISTORY_BYTES));
        ctor=envelopeHold(ctor,envelopeArray(64));
        ctor=envelopeTemporary(ctor,envelopeMigrationDigestCheck(envelopeMigrationIssuerFields(),
                "nereus-delay-migration-root-issuer-authorization-v1\0"));
        return envelopeMigrationDecoded(n,com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_HISTORY_BYTES,25,
                args,ctor,envelopeMigrationIssuerCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationOriginFields(){
        var fields=envelopeDrop(envelopeMutationCanonical(false),envelopeMigrationRecordCanonical());
        fields=envelopeDrop(fields,envelopeIncarnationCanonical(true));
        fields=envelopeDrop(fields,envelopeUsageCanonical());fields=envelopeDrop(fields,envelopeVectorCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationOrigin.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeMigrationOriginCanonical(){
        return envelopeMigrationHashed(envelopeMigrationOriginFields(),com.nereusstream.delay.protocol.TargetMigrationOrigin.MAX_CANONICAL_BYTES,
                "nereus-delay-target-migration-origin-v1\0");
    }
    private static EnvelopeMaterial envelopeMigrationOriginDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationOrigin.MAX_CANONICAL_BYTES;
        var args=envelopeRepeat(envelopeArray(32),6,true);
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(envelopeMigrationRecordBytes(),envelopeMigrationRecordDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        args=envelopeHold(args,envelopeNested(TargetQuotaUsage.MAX_CANONICAL_BYTES,envelopeUsageDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES,envelopeVectorDecode()));
        var ctor=envelopeRepeat(envelopeAssigned(32),6,true);
        ctor=envelopeTemporary(ctor,envelopeMigrationDigestCheck(envelopeMigrationOriginFields(),"nereus-delay-target-migration-origin-v1\0"));
        return envelopeMigrationDecoded(n,Math.max(envelopeMigrationRecordBytes(),TargetQuotaIncarnation.MAX_CANONICAL_BYTES),15,args,ctor,envelopeMigrationOriginCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationPrefixFields(){
        var fields=envelopeMigrationRecordCanonical();fields=envelopeDrop(fields,envelopeRepeat(envelopeMutationCanonical(false),2,false));
        fields=envelopeDrop(fields,envelopeMigrationObjectCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeMigrationPrefixCanonical(){
        return envelopeMigrationHashed(envelopeMigrationPrefixFields(),com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.MAX_CANONICAL_BYTES,
                "nereus-delay-target-migration-prefix-record-v1\0");
    }
    private static EnvelopeMaterial envelopeMigrationPrefixDecode(boolean command){
        final long n=com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.MAX_CANONICAL_BYTES;
        final long identity=command?com.nereusstream.delay.protocol.CommandId.LENGTH:32;
        var args=envelopeHold(envelopeRepeat(envelopeArray(32),4,true),envelopeArray(identity));
        args=envelopeHold(args,envelopeNested(envelopeMigrationRecordBytes(),envelopeMigrationRecordDecode()));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),2,true));
        args=envelopeHold(args,envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()));
        var ctor=envelopeHold(envelopeRepeat(envelopeAssigned(32),4,true),envelopeAssigned(identity));
        if(command){
            // Prefix constructs a CommandId and then independently decodes its routingId for the Shard check.
            ctor=envelopeTemporary(ctor,envelopeManagedIdDecode());
            ctor=envelopeTemporary(ctor,envelopeManagedRoutingDecode());
        }
        ctor=envelopeTemporary(ctor,envelopeMigrationDigestCheck(envelopeMigrationPrefixFields(),"nereus-delay-target-migration-prefix-record-v1\0"));
        return envelopeMigrationDecoded(n,envelopeMigrationRecordBytes(),12,args,ctor,envelopeMigrationPrefixCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationPrefixDecode(){
        return envelopeChoice(envelopeMigrationPrefixDecode(false),envelopeMigrationPrefixDecode(true));
    }
    private static long envelopeMigrationEntryBytes(long raw){
        return Math.addExact(raw,com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_CANONICAL_BYTES-
                (long)com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationEntryCanonical(long raw){
        var fields=envelopeDrop(envelopeSourceCanonical(),envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeMigrationRecordCanonical());
        return envelopeWritten(fields,envelopeMigrationEntryBytes(raw));
    }
    private static EnvelopeMaterial envelopeMigrationEntryDecode(long raw){
        final long n=envelopeMigrationEntryBytes(raw),key=com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_KEY_BYTES;
        var args=envelopeHold(envelopeArray(key),envelopeArray(raw));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(envelopeMigrationRecordBytes(),envelopeMigrationRecordDecode()));
        var ctor=envelopeHold(envelopeArray(key),envelopeArray(raw));
        ctor=envelopeTemporary(ctor,envelopeSourceBounded());
        return envelopeMigrationDecoded(n,Math.max(raw,envelopeMigrationRecordBytes()),7,args,ctor,envelopeMigrationEntryCanonical(raw));
    }
    private static EnvelopeMaterial envelopeMigrationCoverageCanonical(){
        final long range=com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.Range.MAX_CANONICAL_BYTES;
        return envelopeWritten(envelopeRepeat(envelopeArray(range),5,false),com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationCoverageDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES;
        final long range=com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.Range.MAX_CANONICAL_BYTES;
        final long keys=3L*com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_KEY_BYTES;
        final var one=envelopeMigrationDecoded(range,com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_KEY_BYTES,9,
                envelopeArray(keys+32),envelopeHold(envelopeArray(keys),envelopeAssigned(32)),envelopeArray(range));
        return envelopeMigrationDecoded(n,range,6,envelopeRepeat(envelopeNested(range,one),5,true),envelopeEmpty(),envelopeMigrationCoverageCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationCatalogCanonical(){
        return envelopeWritten(envelopeRepeat(envelopeArray(4096),2,false),
                com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationCatalogDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES;
        final long cut=com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_MEMBER_CUT_BYTES;
        var args=envelopeHold(envelopeArray(8192),envelopeArray(32+cut));
        var ctor=envelopeRepeat(envelopeArray(4096),2,false); // UTF-8 validation of the two exact strings
        ctor=envelopeHold(ctor,envelopeAssigned(32));ctor=envelopeHold(ctor,envelopeArray(cut));
        return envelopeMigrationDecoded(n,cut,6,args,ctor,envelopeMigrationCatalogCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationResourceCanonical(){
        return envelopeWritten(envelopeDrop(envelopeIdentityCanonical(),envelopeVectorCanonical()),
                com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.Resource.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationResourceDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.Resource.MAX_CANONICAL_BYTES;
        final long raw=3L*com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.Resource.MAX_MATERIAL_BYTES;
        var args=envelopeArray(32+raw);args=envelopeHold(args,envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES,envelopeVectorDecode()));
        return envelopeMigrationDecoded(n,com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.Resource.MAX_MATERIAL_BYTES,9,
                args,envelopeHold(envelopeAssigned(32),envelopeArray(raw)),envelopeMigrationResourceCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationInventoryCanonical(){
        var fields=envelopeMigrationCatalogCanonical();
        fields=envelopeDrop(fields,envelopeRepeat(envelopeMigrationResourceCanonical(),
                com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES,false));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationInventoryDecode(){
        final long count=com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES;
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES,envelopeMigrationCatalogDecode());
        args=envelopeHold(args,envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.Resource.MAX_CANONICAL_BYTES,
                envelopeMigrationResourceDecode()),count,true));
        // Constructor checks the whole canonical inventory size, retaining the preceding resource ID.
        var ctor=envelopeReturn(envelopeMigrationCatalogCanonical(),0);
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeHold(envelopeArray(32),envelopeMigrationResourceCanonical()),count,false));
        return envelopeMigrationDecoded(com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES,count+2,args,ctor,envelopeMigrationInventoryCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationBarrierCanonical(){
        return envelopeWritten(envelopeDrop(envelopeMutationCanonical(false),envelopeMigrationCatalogCanonical()),
                com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationBarrierDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt.MAX_CANONICAL_BYTES;
        final long closure=com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt.MAX_CLOSURE_BYTES;
        var args=envelopeArray(64+closure);args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES,envelopeMigrationCatalogDecode()));
        return envelopeMigrationDecoded(n,com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.CatalogDomain.MAX_CANONICAL_BYTES,7,args,
                envelopeHold(envelopeRepeat(envelopeAssigned(32),2,true),envelopeArray(closure)),envelopeMigrationBarrierCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationResourceReceiptCanonical(){
        var fields=envelopeDrop(envelopeRepeat(envelopeIdentityCanonical(),2,false),envelopeVectorCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeMigrationObjectCanonical(),com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt.MAX_MATERIALS,false));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationResourceReceiptDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt.MAX_CANONICAL_BYTES;
        final long count=com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt.MAX_MATERIALS;
        final long evidence=com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt.MAX_EVIDENCE_BYTES;
        var args=envelopeArray(96+evidence);
        args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode()),2,true));
        args=envelopeHold(args,envelopeNested(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES,envelopeVectorDecode()));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()),count,true));
        var ctor=envelopeHold(envelopeRepeat(envelopeAssigned(32),3,true),envelopeArray(evidence));
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeComparison(envelopeMigrationObjectCanonical(),envelopeMigrationObjectCanonical()),count-1,false));
        return envelopeMigrationDecoded(n,evidence,count+9,args,ctor,envelopeMigrationResourceReceiptCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationClosureCanonical(){return envelopeArray(com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest.Closure.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeMigrationClosureDecode(){
        final long raw=7L*com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest.Closure.MAX_MATERIAL_BYTES;
        return envelopeMigrationDecoded(com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest.Closure.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest.Closure.MAX_MATERIAL_BYTES,8,envelopeArray(raw),envelopeArray(raw),envelopeMigrationClosureCanonical());
    }
    private static EnvelopeMaterial envelopeMigrationArchiveReceiptCanonical(){
        var fields=envelopeRepeat(envelopeMutationCanonical(false),3,false);fields=envelopeDrop(fields,envelopeIdentityCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeMigrationObjectCanonical(),com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt.MAX_RAW_OBJECTS+2L,false));
        fields=envelopeDrop(fields,envelopeMigrationCoverageCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveReceiptDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt.MAX_CANONICAL_BYTES;
        final long count=com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt.MAX_RAW_OBJECTS;
        var args=envelopeArray(96);args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),3,true));
        args=envelopeHold(args,envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode()));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()),count+2,true));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES,envelopeMigrationCoverageDecode()));
        var ctor=envelopeRepeat(envelopeAssigned(32),3,true);
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeComparison(envelopeMigrationObjectCanonical(),envelopeMigrationObjectCanonical()),count-1,false));
        return envelopeMigrationDecoded(n,com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES,count+13,args,ctor,envelopeMigrationArchiveReceiptCanonical());
    }
    /** COPY has at most 32 entries but their aggregate raw bytes share the one 32MiB request.
        Entry's variable byte paths are affine: Reader/raw accessor, owned raw, recheck and fields.
        Keep fixed per-entry work at its count bound; charge the variable slope to the aggregate. */
    private static EnvelopeMaterial envelopeMigrationCopyEntries(boolean decode){
        final long count=com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_ROWS;
        final long each=com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES;
        final long aggregate=com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES;
        final var zero=decode?envelopeMigrationEntryDecode(0):envelopeMigrationEntryCanonical(0);
        final var full=decode?envelopeMigrationEntryDecode(each):envelopeMigrationEntryCanonical(each);
        final long allocatedSlope=Math.floorDiv(Math.addExact(full.allocated-zero.allocated,each-1),each);
        final long heldSlope=Math.floorDiv(Math.addExact(full.held-zero.held,each-1),each);
        final long allocated=Math.addExact(Math.multiplyExact(zero.allocated,count),Math.multiplyExact(allocatedSlope,aggregate));
        final long held=decode?Math.addExact(Math.multiplyExact(zero.held,count),Math.multiplyExact(heldSlope,aggregate)):zero.held+each;
        final long peak=decode?Math.addExact(held,full.retained-full.held):full.retained;
        return new EnvelopeMaterial(allocated,Math.max(held,peak),held,
                Math.multiplyExact(full.fieldVisits,count),Math.multiplyExact(full.arrayObjects,count));
    }
    private static EnvelopeMaterial envelopeMigrationArchiveActionCanonical(boolean takeover){
        var fields=envelopeRepeat(envelopeMutationCanonical(false),2,false);fields=envelopeDrop(fields,envelopeMigrationCoverageCanonical());
        fields=envelopeDrop(fields,takeover?envelopeMigrationArchiveReceiptCanonical():envelopeDrop(envelopeMigrationCopyEntries(false),envelopeMigrationObjectCanonical()));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES-4096L);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveActionDecode(boolean takeover){
        final long n=com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES-4096L;
        var args=envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),2,true);
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES,envelopeMigrationCoverageDecode()));
        args=envelopeHold(args,takeover?envelopeNested(com.nereusstream.delay.protocol.TargetMigrationArchiveReceipt.MAX_CANONICAL_BYTES,envelopeMigrationArchiveReceiptDecode()):
                envelopeHold(envelopeMigrationCopyEntries(true),envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode())));
        var ctor=envelopeReturn(envelopeMigrationCoverageCanonical(),0);
        if(takeover)ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationCoverageCanonical(),envelopeMigrationCoverageCanonical()));
        else{
            ctor=envelopeTemporary(ctor,envelopeMigrationCopyEntries(false));
            ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeComparison(envelopeArray(com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_KEY_BYTES),
                    envelopeArray(com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_KEY_BYTES)),com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_ROWS-1L,false));
        }
        return envelopeMigrationDecoded(n,Math.min(n,com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_CANONICAL_BYTES),
                takeover?6:com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_ROWS+6,args,ctor,envelopeMigrationArchiveActionCanonical(takeover));
    }
    private static long envelopeMigrationRequestBytes(int kind,boolean issuer){
        return switch(kind){
            case 34->issuer?com.nereusstream.delay.protocol.TargetMigrationActivateRequest.MAX_CANONICAL_BYTES:com.nereusstream.delay.protocol.TargetMigrationActivateRequest.V1_MAX_CANONICAL_BYTES;
            case 35->com.nereusstream.delay.protocol.TargetMigrationSettlementRequest.MAX_CANONICAL_BYTES;
            case 36->com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest.MAX_CANONICAL_BYTES;
            case 37->com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest.MAX_CANONICAL_BYTES;
            case 39->com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES;
            default->throw new IllegalArgumentException("not an actual migration request kind");
        };
    }
    private static EnvelopeMaterial envelopeMigrationRequestCanonical(int kind,boolean issuer){
        EnvelopeMaterial fields;
        switch(kind){
            case 34->{fields=envelopeDrop(envelopeMigrationPlanCanonical(),envelopeMigrationObjectCanonical());
                if(issuer)fields=envelopeDrop(fields,envelopeMigrationIssuerCanonical());}
            case 35->{fields=envelopeDrop(envelopeScopeCanonical(),envelopeArray(DelayMessageId.LENGTH));
                fields=envelopeDrop(fields,envelopeMigrationRecordCanonical());fields=envelopeDrop(fields,envelopeSourceCanonical());fields=envelopeDrop(fields,envelopeMigrationObjectCanonical());}
            case 36->{fields=envelopeDrop(envelopeScopeCanonical(),envelopeMutationCanonical(false));
                fields=envelopeDrop(fields,envelopeMigrationInventoryCanonical());fields=envelopeDrop(fields,envelopeMigrationObjectCanonical());}
            case 37->{fields=envelopeDrop(envelopeScopeCanonical(),envelopeRepeat(envelopeMutationCanonical(false),2,false));
                fields=envelopeDrop(fields,envelopeMigrationBarrierCanonical());fields=envelopeDrop(fields,envelopeRepeat(envelopeMigrationResourceReceiptCanonical(),
                        com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES,false));fields=envelopeDrop(fields,envelopeMigrationClosureCanonical());}
            case 39->{fields=envelopeDrop(envelopeScopeCanonical(),envelopeRepeat(envelopeMutationCanonical(false),2,false));
                fields=envelopeDrop(fields,envelopeChoice(envelopeMigrationArchiveActionCanonical(false),envelopeMigrationArchiveActionCanonical(true)));}
            default->throw new IllegalArgumentException("not an actual migration request kind");
        }
        return envelopeWritten(fields,envelopeMigrationRequestBytes(kind,issuer));
    }
    private static EnvelopeMaterial envelopeMigrationRequestDecode(int kind,boolean issuer){
        final long n=envelopeMigrationRequestBytes(kind,issuer);EnvelopeMaterial args,ctor;
        switch(kind){
            case 34->{args=envelopeNested(com.nereusstream.delay.protocol.TargetMigrationPlan.MAX_CANONICAL_BYTES,envelopeMigrationPlanDecode());
                args=envelopeHold(args,envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()));
                if(issuer)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization.MAX_BYTES,envelopeMigrationIssuerDecode()));
                ctor=envelopeReturn(envelopeRepeat(envelopeArray(16),2,false),0);
                ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationObjectCanonical(),envelopeMigrationObjectCanonical()));
                if(issuer){ctor=envelopeTemporary(ctor,envelopeHash(envelopeMigrationPlanCanonical(),"nereus-delay-target-migration-plan-v1\0".length()));
                    ctor=envelopeTemporary(ctor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
                    ctor=envelopeTemporary(ctor,envelopeArray(32+32+32+16+32+16));}}
            case 35->{args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArray(128));
                args=envelopeHold(args,envelopeNested(DelayMessageId.LENGTH,envelopeDelayMessageIdDecode()));
                args=envelopeHold(args,envelopeNested(envelopeMigrationRecordBytes(),envelopeMigrationRecordDecode()));
                args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
                args=envelopeHold(args,envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()));
                ctor=envelopeRepeat(envelopeAssigned(32),4,true);ctor=envelopeTemporary(ctor,envelopeSourceBounded());
                ctor=envelopeTemporary(ctor,envelopeManagedRoutingDecode());}
            case 36->{args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArray(64));
                args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
                args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_CANONICAL_BYTES,envelopeMigrationInventoryDecode()));
                args=envelopeHold(args,envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()));ctor=envelopeRepeat(envelopeAssigned(32),2,true);}
            case 37->{args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArray(96));
                args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),2,true));
                args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationCatalogBarrierReceipt.MAX_CANONICAL_BYTES,envelopeMigrationBarrierDecode()));
                args=envelopeHold(args,envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt.MAX_CANONICAL_BYTES,
                        envelopeMigrationResourceReceiptDecode()),com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES,true));
                args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest.Closure.MAX_CANONICAL_BYTES,envelopeMigrationClosureDecode()));
                ctor=envelopeRepeat(envelopeAssigned(32),3,true);ctor=envelopeTemporary(ctor,envelopeMigrationBarrierCanonical());ctor=envelopeTemporary(ctor,envelopeMigrationClosureCanonical());
                ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeHold(envelopeArray(64),envelopeMigrationResourceReceiptCanonical()),
                        com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES,false));}
            case 39->{args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArray(16+5*32));
                args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),2,true));
                args=envelopeHold(args,envelopeNested(n-4096,envelopeChoice(envelopeMigrationArchiveActionDecode(false),envelopeMigrationArchiveActionDecode(true))));
                ctor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),5,true));
                ctor=envelopeTemporary(ctor,envelopeChoice(envelopeMigrationArchiveActionCanonical(false),envelopeMigrationArchiveActionCanonical(true)));ctor=envelopeTemporary(ctor,envelopeArray(96));}
            default->throw new IllegalArgumentException("not an actual migration request kind");
        }
        final long fields=switch(kind){
            case 34->issuer?4:3;
            case 35->12;
            case 36->7;
            case 37->com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES+9L;
            case 39->12;
            default->throw new IllegalArgumentException("not an actual migration request kind");
        };
        return envelopeMigrationDecoded(n,n,fields,args,ctor,envelopeMigrationRequestCanonical(kind,issuer));
    }
    private static long envelopeMigrationBodyBytes(int kind,boolean issuer){return Math.addExact(envelopeMigrationRequestBytes(kind,issuer),192);}
    private static long envelopeMigrationInputBytes(int kind,boolean issuer){return Math.addExact(envelopeMigrationBodyBytes(kind,issuer),512);}
    private static EnvelopeMaterial envelopeMigrationControlSemantic(int kind,boolean issuer){
        return envelopeHash(envelopeHold(envelopeArray(2),envelopeMigrationRequestCanonical(kind,issuer)),
                "nereus-delay-target-migration-control-v1\0".length());
    }
    private static EnvelopeMaterial envelopeMigrationControlBodyCanonical(int kind,boolean issuer){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        fields=envelopeDrop(fields,envelopeMigrationControlSemantic(kind,issuer));
        fields=envelopeDrop(fields,envelopeWritten(envelopeMigrationRequestCanonical(kind,issuer),envelopeMigrationRequestBytes(kind,issuer)+8));
        return envelopeWritten(fields,envelopeMigrationBodyBytes(kind,issuer));
    }
    private static EnvelopeMaterial envelopeMigrationControlBodyDecode(int kind,boolean issuer){
        final long n=envelopeMigrationBodyBytes(kind,issuer),request=envelopeMigrationRequestBytes(kind,issuer),payload=request+8;
        var args=envelopeNested(24,envelopeShardSubjectDecode());args=envelopeHold(args,envelopeNested(74,envelopeControlRefDecode()));
        final var requestFrame=envelopeHold(envelopeProtoRead(payload,payload,payload,1),envelopeNested(request,envelopeMigrationRequestDecode(kind,issuer)));
        args=envelopeHold(args,envelopeCall(envelopeArray(payload),requestFrame,0));
        var ctor=envelopeReturn(envelopeCall(envelopeArray(32),envelopeAssigned(32),0),0);
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(
                envelopeMigrationRequestCanonical(kind,issuer),request,kind+1)));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeArray(32),envelopeMigrationControlSemantic(kind,issuer)));
        return envelopeMigrationDecoded(n,payload,9,args,ctor,envelopeMigrationControlBodyCanonical(kind,issuer));
    }
    // The outer System-family dispatcher can select every legal migration request without a raw-body fallback.
    private static EnvelopeMaterial envelopeMigrationControlBodyCanonical(){
        var selected=envelopeChoice(envelopeMigrationControlBodyCanonical(34,false),envelopeMigrationControlBodyCanonical(34,true));
        for(int kind:new int[]{35,36,37,39})selected=envelopeChoice(selected,envelopeMigrationControlBodyCanonical(kind,false));
        return selected;
    }
    private static EnvelopeMaterial envelopeMigrationControlBodyDecode(){
        var selected=envelopeChoice(envelopeMigrationControlBodyDecode(34,false),envelopeMigrationControlBodyDecode(34,true));
        for(int kind:new int[]{35,36,37,39})selected=envelopeChoice(selected,envelopeMigrationControlBodyDecode(kind,false));
        return selected;
    }
    private static EnvelopeMaterial envelopeMigrationControlValidation(long body,long payload){
        // ApplyShardControlBody's nine-field registered branch, exact ControlRef and one payload member.
        var work=envelopeProtoRead(body,body,body,9);
        work=envelopeTemporary(work,envelopeNested(74,envelopeProtoRead(74,69,32,3)));
        work=envelopeTemporary(work,envelopeArray(32));
        work=envelopeTemporary(work,envelopeNested(payload,envelopeProtoRead(payload,payload,payload,1)));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeMigrationInputAuthorDecode(){
        var work=envelopeProtoRead(104,102,102,1);
        work=envelopeHold(work,envelopeNested(102,envelopeProtoRead(102,96,32,3)));
        // raw accessor, decode fixed(), control factory fixed(), and immutable AuthorIdentity copies.
        work=envelopeHold(work,envelopeRepeat(envelopeArrays(96,3),4,true));
        work=envelopeTemporary(work,envelopeCall(envelopeArray(102),envelopeArray(104),0));
        return envelopeReturn(work,96);
    }
    private static EnvelopeMaterial envelopeMigrationInputDecode(int kind,boolean issuer){
        final long body=envelopeMigrationBodyBytes(kind,issuer),input=envelopeMigrationInputBytes(kind,issuer);
        final var identity=envelopeCall(envelopeMigrationControlBodyDecode(kind,issuer),envelopeControlLogicalIdentity(),0);
        return envelopeSystemDecode(input,body,104,envelopeMigrationControlValidation(body,envelopeMigrationRequestBytes(kind,issuer)+8),identity,true);
    }
    private static EnvelopeMaterial envelopeMigrationRequireInput(int kind,boolean issuer){
        var checks=envelopeNested(104,envelopeMigrationInputAuthorDecode());
        checks=envelopeTemporary(checks,envelopeComparison(envelopeArray(envelopeMigrationBodyBytes(kind,issuer)),envelopeMigrationControlBodyCanonical(kind,issuer)));
        checks=envelopeTemporary(checks,envelopeComparison(envelopeArray(32),envelopeControlLogicalIdentity()));
        checks=envelopeTemporary(checks,envelopeSystemCanonical(envelopeMigrationInputBytes(kind,issuer)));
        return envelopeReturn(checks,0);
    }
    private static EnvelopeMaterial envelopeMigrationControlAuthorDecode(){
        return envelopeMigrationDecoded(102,32,3,envelopeRepeat(envelopeArray(32),3,true),envelopeRepeat(envelopeArray(32),3,true),envelopeArray(102));
    }
    private static EnvelopeMaterial envelopeMigrationPreparedTargetFields(){return envelopeWritten(envelopeShardSubjectCanonical(),100);}
    private static EnvelopeMaterial envelopeMigrationPreparedTargetCanonical(){return envelopeWritten(envelopeShardSubjectCanonical(),135);}
    private static EnvelopeMaterial envelopeMigrationPreparedTargetDecode(){
        var args=envelopeNested(24,envelopeShardSubjectDecode());args=envelopeHold(args,envelopeArray(96));
        var ctor=envelopeArray(64);ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeMigrationPreparedTargetFields()));ctor=envelopeHold(ctor,envelopeArray(32));
        return envelopeMigrationDecoded(135,32,6,args,ctor,envelopeMigrationPreparedTargetCanonical());
    }
    private static long envelopeMigrationPreparedBytes(int kind,boolean issuer){
        return Math.addExact(envelopeMigrationRequestBytes(kind,issuer),kind==34?1024:2048);
    }
    private static EnvelopeMaterial envelopeMigrationPreparedRequestCanonical(int kind,boolean issuer){
        return envelopeWritten(envelopeMigrationRequestCanonical(kind,issuer),envelopeMigrationRequestBytes(kind,issuer)+8);
    }
    private static EnvelopeMaterial envelopeMigrationPreparedFields(int kind,boolean issuer){
        var fields=envelopeDrop(envelopeArray(102),envelopeMigrationPreparedRequestCanonical(kind,issuer));
        fields=envelopeDrop(fields,envelopeMigrationPreparedTargetCanonical());
        return envelopeWritten(fields,envelopeMigrationPreparedBytes(kind,issuer)-112);
    }
    private static EnvelopeMaterial envelopeMigrationPreparedCanonical(int kind,boolean issuer){
        // Prepared stores its digest/signature: canonicalBytes does not calculate them again.
        return envelopeWritten(envelopeMigrationPreparedFields(kind,issuer),envelopeMigrationPreparedBytes(kind,issuer));
    }
    private static EnvelopeMaterial envelopeMigrationPreparedDecode(int kind,boolean issuer){
        final long request=envelopeMigrationRequestBytes(kind,issuer),n=envelopeMigrationPreparedBytes(kind,issuer);
        final var wrapped=envelopeMigrationDecoded(request+8,request,1,envelopeNested(request,envelopeMigrationRequestDecode(kind,issuer)),
                envelopeEmpty(),envelopeMigrationPreparedRequestCanonical(kind,issuer));
        var args=envelopeArray(32*4+64);args=envelopeHold(args,envelopeNested(102,envelopeMigrationControlAuthorDecode()));
        args=envelopeHold(args,envelopeNested(request+8,wrapped));args=envelopeHold(args,envelopeNested(135,envelopeMigrationPreparedTargetDecode()));
        var ctor=envelopeArray(32*4+64);
        ctor=envelopeTemporary(ctor,envelopePreparedRequestHash(envelopeMigrationRequestCanonical(kind,issuer),request,kind+1));
        var targets=envelopeWritten(envelopeMigrationPreparedTargetCanonical(),138);
        targets=envelopeCall(targets,envelopeCall(envelopeArray(4),envelopeArray(142),0),0);
        ctor=envelopeTemporary(ctor,envelopeHash(targets,"nereus-delay-control-target-snapshot\0".length()));
        ctor=envelopeTemporary(ctor,envelopeHash(envelopeMigrationPreparedFields(kind,issuer),"nereus-delay-prepared-control\0".length()));
        ctor=envelopeTemporary(ctor,envelopeArray(64)); // validateTargetPresence's expectedMutationId/Hash accessors
        return envelopeMigrationDecoded(n,request+8,13,args,ctor,envelopeMigrationPreparedCanonical(kind,issuer));
    }
    private static EnvelopeMaterial envelopeMigrationBinding(int kind,boolean issuer){
        // ControlTargetMutationBinding.validate: exact target IDs, fresh ControlRef, body/author,
        // complete registered request equality and final logical-operation identity.
        var work=envelopeArray(128);work=envelopeHold(work,envelopeCall(envelopeArray(64),envelopeRepeat(envelopeAssigned(32),2,true),0));
        work=envelopeTemporary(work,envelopeNested(envelopeMigrationBodyBytes(kind,issuer),envelopeMigrationControlBodyDecode(kind,issuer)));
        work=envelopeTemporary(work,envelopeMigrationRequireInput(kind,issuer));
        work=envelopeTemporary(work,envelopeNested(104,envelopeMigrationInputAuthorDecode()));
        work=envelopeTemporary(work,envelopeArray(192));
        work=envelopeTemporary(work,envelopeComparison(envelopeMigrationRequestCanonical(kind,issuer),envelopeMigrationRequestCanonical(kind,issuer)));
        work=envelopeTemporary(work,envelopeComparison(envelopeControlLogicalIdentity(),envelopeArray(32)));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeMigrationSignature(int kind,boolean issuer){
        // SystemMutation.signatureDigest keeps every actual lp32 argument until Bytes.sha256's concat.
        final long body=envelopeMigrationBodyBytes(kind,issuer);
        var args=envelopeArray("nereus-delay-system-mutation-signature\0".length());
        args=envelopeHold(args,envelopeArrays(20,7));
        final var fixed=envelopeCall(envelopeArray(32),envelopeCall(envelopeArray(4),envelopeArray(36),0),0);
        args=envelopeHold(args,fixed);args=envelopeHold(args,envelopeShardCanonical());
        args=envelopeHold(args,envelopeArray(8));
        args=envelopeHold(args,envelopeCall(envelopeArray(4),envelopeArray(body+4),0));
        args=envelopeHold(args,fixed);
        args=envelopeHold(args,envelopeCall(envelopeArray(4),envelopeArray(108),0));
        args=envelopeHold(args,envelopeArray(4));
        return envelopeHash(args,0);
    }
    /** Repository-owned local calls before the Native migration planner; original external Holds are separate. */
    static EnvelopeMaterial envelopeMigrationOperationMaterial(int kind,boolean issuer){
        var work=envelopeMigrationBodyGetter(kind,issuer);
        work=envelopeTemporary(work,envelopeMigrationRequireInput(kind,issuer));
        work=envelopeTemporary(work,envelopeSourceBounded());
        work=envelopeTemporary(work,envelopeComparison(envelopeMigrationRequestCanonical(kind,issuer),
                envelopeMigrationRequestCanonical(kind,issuer)));
        work=envelopeTemporary(work,envelopeMigrationSignature(kind,issuer));
        work=envelopeTemporary(work,envelopeNested(104,envelopeMigrationInputAuthorDecode()));
        work=envelopeTemporary(work,envelopeArrays(192,6)); // three exact decoded/registered author pairs
        work=envelopeTemporary(work,envelopeComparison(envelopeMigrationPreparedCanonical(kind,issuer),
                envelopeMigrationPreparedCanonical(kind,issuer)));
        work=envelopeTemporary(work,envelopeMigrationBinding(kind,issuer));
        // authenticate's exact migration ID/plan/Source comparisons stay local, without provider material.
        if(kind==34){
            work=envelopeTemporary(work,envelopeComparison(envelopeMigrationPlanCanonical(),envelopeMigrationPlanCanonical()));
            work=envelopeTemporary(work,envelopeComparison(envelopeMigrationObjectCanonical(),envelopeMigrationObjectCanonical()));
            if(issuer){
                work=envelopeTemporary(work,envelopeHash(envelopeMigrationPlanCanonical(),"nereus-delay-target-migration-plan-v1\0".length()));
                work=envelopeTemporary(work,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
                work=envelopeTemporary(work,envelopeArrays(32*4+16*2,6));
                // Independent issuer.verifySignature hashes its statement and prepends its signature purpose.
                final var digest=envelopeHash(envelopeMigrationIssuerFields(),"nereus-delay-migration-root-issuer-authorization-v1\0".length());
                final var signature=envelopeConcat(envelopeHold(envelopeArray("nereus-delay-migration-root-issuer-authorization-signature-v1\0".length()),digest));
                work=envelopeTemporary(work,signature);
            }
        }
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeMigrationBodyGetter(int kind,boolean issuer){
        return envelopeNested(envelopeMigrationBodyBytes(kind,issuer),envelopeMigrationControlBodyDecode(kind,issuer));
    }
    private static EnvelopeMaterial envelopeMigrationRootFields(boolean active,boolean issuer){
        var fields=active?envelopeReturn(envelopeMigrationBodyGetter(34,issuer),0):envelopeEmpty(); // schemaVersion()
        fields=envelopeDrop(fields,envelopeMigrationPlanCanonical());fields=envelopeDrop(fields,envelopeIncarnationCanonical(true));
        if(active){fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeMigrationInputBytes(34,issuer)));fields=envelopeDrop(fields,envelopeMutationCanonical(false));}
        return envelopeWritten(fields,(issuer?com.nereusstream.delay.protocol.TargetMigrationRoot.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetMigrationRoot.V1_MAX_CANONICAL_BYTES)-34L);
    }
    private static EnvelopeMaterial envelopeMigrationRootDigest(boolean active,boolean issuer){
        var work=active?envelopeReturn(envelopeMigrationBodyGetter(34,issuer),0):envelopeEmpty(); // digest's domain discriminator
        work=envelopeDrop(work,envelopeHash(envelopeMigrationRootFields(active,issuer),
                (issuer?"nereus-delay-target-migration-root-v2\0":"nereus-delay-target-migration-root-v1\0").length()));
        return work;
    }
    private static EnvelopeMaterial envelopeMigrationRootCanonical(boolean active,boolean issuer){
        return envelopeWritten(envelopeDrop(envelopeMigrationRootFields(active,issuer),envelopeMigrationRootDigest(active,issuer)),
                issuer?com.nereusstream.delay.protocol.TargetMigrationRoot.MAX_CANONICAL_BYTES:com.nereusstream.delay.protocol.TargetMigrationRoot.V1_MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationRootDecode(boolean active,boolean issuer){
        final long n=issuer?com.nereusstream.delay.protocol.TargetMigrationRoot.MAX_CANONICAL_BYTES:com.nereusstream.delay.protocol.TargetMigrationRoot.V1_MAX_CANONICAL_BYTES;
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetMigrationPlan.MAX_CANONICAL_BYTES,envelopeMigrationPlanDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        if(active){args=envelopeHold(args,envelopeNested(envelopeMigrationInputBytes(34,issuer),envelopeMigrationInputDecode(34,issuer)));
            args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));}
        var ctor=envelopeReturn(envelopeScopeConstruct(),0);
        ctor=envelopeTemporary(ctor,envelopeArray(32));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        ctor=envelopeTemporary(ctor,envelopeHash(envelopeMigrationPlanCanonical(),"nereus-delay-target-migration-plan-v1\0".length()));
        if(active){
            ctor=envelopeTemporary(ctor,envelopeMigrationBodyGetter(34,issuer));ctor=envelopeTemporary(ctor,envelopeMigrationRequireInput(34,issuer));
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationPlanCanonical(),envelopeMigrationPlanCanonical()));
            ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeSystemCanonical(envelopeMigrationInputBytes(34,issuer))));
            ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeMigrationBodyGetter(34,issuer),2,false)); // decode's schema rechecks
        }
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationRootDigest(active,issuer),envelopeArray(32)));
        return envelopeMigrationDecoded(n,Math.max(com.nereusstream.delay.protocol.TargetMigrationPlan.MAX_CANONICAL_BYTES,
                active?envelopeMigrationInputBytes(34,issuer):0),active?7:5,args,ctor,envelopeMigrationRootCanonical(active,issuer));
    }
    private static EnvelopeMaterial envelopeMigrationGuardFields(boolean settled){
        var fields=envelopeArray(DelayMessageId.LENGTH);fields=envelopeDrop(fields,envelopeMigrationRecordCanonical());
        fields=envelopeDrop(fields,envelopeIncarnationCanonical(true));fields=envelopeDrop(fields,envelopeVectorCanonical());
        if(settled){fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeMigrationInputBytes(35,false)));fields=envelopeDrop(fields,envelopeMutationCanonical(false));}
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeMigrationGuardCanonical(boolean settled){
        return envelopeMigrationHashed(envelopeMigrationGuardFields(settled),com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.MAX_CANONICAL_BYTES,
                "nereus-delay-target-migration-custody-guard-v1\0");
    }
    private static EnvelopeMaterial envelopeMigrationGuardDecode(boolean settled){
        final long n=com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.MAX_CANONICAL_BYTES;
        var args=envelopeArray(96);args=envelopeHold(args,envelopeNested(DelayMessageId.LENGTH,envelopeDelayMessageIdDecode()));
        args=envelopeHold(args,envelopeNested(envelopeMigrationRecordBytes(),envelopeMigrationRecordDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        args=envelopeHold(args,envelopeNested(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES,envelopeVectorDecode()));
        if(settled){args=envelopeHold(args,envelopeNested(envelopeMigrationInputBytes(35,false),envelopeMigrationInputDecode(35,false)));
            args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));}
        var ctor=envelopeRepeat(envelopeAssigned(32),3,true);
        ctor=envelopeTemporary(ctor,envelopeManagedRoutingDecode());
        if(settled){
            ctor=envelopeTemporary(ctor,envelopeMigrationBodyGetter(35,false));ctor=envelopeTemporary(ctor,envelopeMigrationRequireInput(35,false));
            ctor=envelopeTemporary(ctor,envelopeArray(96));
            ctor=envelopeTemporary(ctor,envelopeHash(envelopeMigrationGuardFields(false),"nereus-delay-target-migration-custody-guard-v1\0".length()));
            ctor=envelopeTemporary(ctor,envelopeScopeConstruct());
            ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeSystemCanonical(envelopeMigrationInputBytes(35,false))));
        }
        ctor=envelopeTemporary(ctor,envelopeMigrationDigestCheck(envelopeMigrationGuardFields(settled),"nereus-delay-target-migration-custody-guard-v1\0"));
        return envelopeMigrationDecoded(n,Math.max(envelopeMigrationRecordBytes(),settled?envelopeMigrationInputBytes(35,false):0),
                settled?13:11,args,ctor,envelopeMigrationGuardCanonical(settled));
    }
    private static EnvelopeMaterial envelopeMigrationAttemptFields(boolean issuer){
        var fields=envelopeReturn(envelopeMigrationBodyGetter(34,issuer),0); // fields' schemaVersion
        fields=envelopeDrop(fields,envelopeMigrationObjectCanonical());fields=envelopeDrop(fields,envelopeMigrationPreparedCanonical(34,issuer));
        fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeMigrationInputBytes(34,issuer)));fields=envelopeDrop(fields,envelopeMutationCanonical(true));
        return envelopeWritten(fields,(issuer?com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.V1_MAX_CANONICAL_BYTES)-34L);
    }
    private static EnvelopeMaterial envelopeMigrationAttemptDigest(boolean issuer){
        return envelopeDrop(envelopeReturn(envelopeMigrationBodyGetter(34,issuer),0),envelopeHash(envelopeMigrationAttemptFields(issuer),
                (issuer?"nereus-delay-target-migration-activation-attempt-v2\0":"nereus-delay-target-migration-activation-attempt-v1\0").length()));
    }
    private static EnvelopeMaterial envelopeMigrationAttemptCanonical(boolean issuer){
        return envelopeWritten(envelopeDrop(envelopeMigrationAttemptFields(issuer),envelopeMigrationAttemptDigest(issuer)),issuer?
                com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.MAX_CANONICAL_BYTES:com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.V1_MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationAttemptDecode(boolean issuer){
        final long n=issuer?com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.MAX_CANONICAL_BYTES:com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.V1_MAX_CANONICAL_BYTES;
        var args=envelopeArray(64);args=envelopeHold(args,envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()));
        args=envelopeHold(args,envelopeNested(envelopeMigrationPreparedBytes(34,issuer),envelopeMigrationPreparedDecode(34,issuer)));
        args=envelopeHold(args,envelopeNested(envelopeMigrationInputBytes(34,issuer),envelopeMigrationInputDecode(34,issuer)));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(true)));
        var ctor=envelopeRepeat(envelopeAssigned(32),2,true);
        ctor=envelopeTemporary(ctor,envelopeMigrationBodyGetter(34,issuer));ctor=envelopeTemporary(ctor,envelopeMigrationRequireInput(34,issuer));
        ctor=envelopeTemporary(ctor,envelopeHash(envelopeMigrationPlanCanonical(),"nereus-delay-target-migration-plan-v1\0".length()));
        ctor=envelopeTemporary(ctor,envelopeArray(32));ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationObjectCanonical(),envelopeMigrationObjectCanonical()));
        ctor=envelopeTemporary(ctor,envelopeMigrationPreparedCanonical(34,issuer));
        ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeSystemCanonical(envelopeMigrationInputBytes(34,issuer))));
        ctor=envelopeTemporary(ctor,envelopeMigrationBinding(34,issuer));
        ctor=envelopeTemporary(ctor,envelopeRepeat(envelopeMigrationBodyGetter(34,issuer),2,false));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationAttemptDigest(issuer),envelopeArray(32)));
        return envelopeMigrationDecoded(n,Math.max(envelopeMigrationPreparedBytes(34,issuer),envelopeMigrationInputBytes(34,issuer)),
                9,args,ctor,envelopeMigrationAttemptCanonical(issuer));
    }
    private static EnvelopeMaterial envelopeMigrationRetirementFields(boolean retired){
        var fields=envelopeIncarnationCanonical(true);fields=envelopeDrop(fields,envelopeMigrationPreparedCanonical(36,false));
        fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeMigrationInputBytes(36,false)));fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        if(retired){fields=envelopeDrop(fields,envelopeMigrationPreparedCanonical(37,false));fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeMigrationInputBytes(37,false)));
            fields=envelopeDrop(fields,envelopeMutationCanonical(false));}
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeMigrationRetirementCanonical(boolean retired){
        return envelopeMigrationHashed(envelopeMigrationRetirementFields(retired),com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.MAX_CANONICAL_BYTES,
                "nereus-delay-target-migration-retirement-root-v1\0");
    }
    private static EnvelopeMaterial envelopeMigrationRequireIntent(){
        var checks=envelopeHash(envelopeMigrationRequestCanonical(36,false),"nereus-delay-target-migration-retire-intent-v1\0".length());
        checks=envelopeDrop(checks,envelopeComparison(envelopeMigrationCatalogCanonical(),envelopeMigrationCatalogCanonical()));
        checks=envelopeDrop(checks,envelopeHash(envelopeMigrationRequestCanonical(36,false),"nereus-delay-target-migration-retire-intent-v1\0".length()));
        final var resource=envelopeDrop(envelopeArray(96),envelopeHash(envelopeMigrationResourceCanonical(),"nereus-delay-target-migration-retirement-inventory-v1\0".length()));
        return envelopeReturn(envelopeDrop(checks,envelopeRepeat(resource,com.nereusstream.delay.protocol.TargetMigrationRetirementInventory.MAX_RESOURCES,false)),0);
    }
    private static EnvelopeMaterial envelopeMigrationRetirementDecode(boolean retired){
        final long n=com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.MAX_CANONICAL_BYTES;
        var args=envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true));
        args=envelopeHold(args,envelopeNested(envelopeMigrationPreparedBytes(36,false),envelopeMigrationPreparedDecode(36,false)));
        args=envelopeHold(args,envelopeNested(envelopeMigrationInputBytes(36,false),envelopeMigrationInputDecode(36,false)));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        if(retired){args=envelopeHold(args,envelopeNested(envelopeMigrationPreparedBytes(37,false),envelopeMigrationPreparedDecode(37,false)));
            args=envelopeHold(args,envelopeNested(envelopeMigrationInputBytes(37,false),envelopeMigrationInputDecode(37,false)));
            args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));}
        var ctor=envelopeReturn(envelopeMigrationBodyGetter(36,false),0);ctor=envelopeTemporary(ctor,envelopeMigrationRequireInput(36,false));
        ctor=envelopeTemporary(ctor,envelopeScopeConstruct());ctor=envelopeTemporary(ctor,envelopeMigrationPreparedCanonical(36,false));
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationRequestCanonical(36,false),envelopeMigrationRequestCanonical(36,false)));
        ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeSystemCanonical(envelopeMigrationInputBytes(36,false))));
        ctor=envelopeTemporary(ctor,envelopeMigrationBinding(36,false));
        if(retired){ctor=envelopeTemporary(ctor,envelopeMigrationBodyGetter(37,false));ctor=envelopeTemporary(ctor,envelopeMigrationRequireInput(37,false));
            ctor=envelopeTemporary(ctor,envelopeMigrationPreparedCanonical(37,false));
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationRequestCanonical(37,false),envelopeMigrationRequestCanonical(37,false)));
            ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeSystemCanonical(envelopeMigrationInputBytes(37,false))));
            ctor=envelopeTemporary(ctor,envelopeMigrationRequireIntent());ctor=envelopeTemporary(ctor,envelopeMigrationBinding(37,false));}
        ctor=envelopeTemporary(ctor,envelopeMigrationDigestCheck(envelopeMigrationRetirementFields(retired),"nereus-delay-target-migration-retirement-root-v1\0"));
        return envelopeMigrationDecoded(n,retired?envelopeMigrationPreparedBytes(37,false):envelopeMigrationPreparedBytes(36,false),
                retired?10:7,args,ctor,envelopeMigrationRetirementCanonical(retired));
    }
    private static EnvelopeMaterial envelopeMigrationFrameCheck(long frameBytes){
        final long input=Math.subtractExact(frameBytes,com.nereusstream.delay.protocol.ShardLogFrame.HEADER_LENGTH+
                com.nereusstream.delay.protocol.ShardLogFrame.TRAILER_LENGTH);
        if(input<4)throw new IllegalArgumentException("genuine archive Source maximum must contain a complete frame");
        // requireFrame first decodes NDL1 for its kind; each selected public codec decodes NDL1 again.
        final var framing=envelopeCall(envelopeArray(input),envelopeArray(input),0);
        var command=envelopeCommandFrameDecode(frameBytes);
        command=envelopeTemporary(command,envelopeMigrationSha(envelopeCommandFrameCanonical(frameBytes)));
        var system=envelopeSystemFrameDecode(frameBytes);
        system=envelopeTemporary(system,envelopeMigrationSha(envelopeSystemCanonical(input)));
        var checked=envelopeHold(framing,envelopeChoice(command,system));
        checked=envelopeTemporary(checked,envelopeArray(32)); // actual at.mutationDigest() comparison
        return envelopeReturn(checked,0);
    }
    private static long envelopeMigrationArchiveRowBytes(int version,boolean copy,long frameBytes,long raw){
        if(version==1)return com.nereusstream.delay.protocol.TargetMigrationArchiveRow.V1_MAX_CANONICAL_BYTES;
        if(copy)return Math.addExact(envelopeMigrationArchiveCopyMetadataBytes(),raw);
        return com.nereusstream.delay.protocol.TargetMigrationArchiveRow.maximumCanonicalBytes(version,frameBytes);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRowMetadata(long raw){
        var fields=envelopeDrop(envelopeIncarnationCanonical(true),envelopeMigrationEntryCanonical(raw));
        return envelopeDrop(fields,envelopeMutationCanonical(false));
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRowFields(int version,boolean copy,long frameBytes,long raw){
        var fields=version==2?envelopeReturn(envelopeMigrationArchiveRowMetadata(raw),0):envelopeEmpty(); // canonicalLength preflight
        fields=envelopeDrop(fields,envelopeMigrationArchiveRowMetadata(raw));
        return envelopeWritten(fields,envelopeMigrationArchiveRowBytes(version,copy,frameBytes,raw)-34);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRowCanonical(int version,boolean copy,long frameBytes){
        return envelopeMigrationArchiveRowCanonical(version,copy,frameBytes,
                com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRowCanonical(int version,boolean copy,long frameBytes,long raw){
        final var fields=envelopeMigrationArchiveRowFields(version,copy,frameBytes,raw);
        final long n=envelopeMigrationArchiveRowBytes(version,copy,frameBytes,raw);
        return version==1?envelopeMigrationHashed(fields,n,"nereus-delay-migration-logical-first-native72-v1\0"):
                envelopeMigrationLocalHashed(fields,n,"nereus-delay-migration-logical-first-native72-v2\0");
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRowDecode(int version,boolean copy,long frameBytes){
        return envelopeMigrationArchiveRowDecode(version,copy,frameBytes,
                com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRowDecode(int version,boolean copy,long frameBytes,long raw){
        final long n=envelopeMigrationArchiveRowBytes(version,copy,frameBytes,raw);
        var args=envelopeArray(32+32+16+32+16);
        args=envelopeHold(args,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        args=envelopeHold(args,envelopeNested(envelopeMigrationEntryBytes(raw),envelopeMigrationEntryDecode(raw)));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeArray(copy?32:frameBytes));
        var ctor=envelopeHold(envelopeRepeat(envelopeAssigned(32),3,true),envelopeRepeat(envelopeAssigned(16),2,true));
        if(version==2)ctor=envelopeTemporary(ctor,envelopeMigrationArchiveRowMetadata(raw)); // before full frame copy
        if(copy){ctor=envelopeHold(ctor,envelopeAssigned(32));ctor=envelopeTemporary(ctor,envelopeArray(32));}
        else{ctor=envelopeHold(ctor,envelopeArray(frameBytes));ctor=envelopeTemporary(ctor,envelopeMigrationFrameCheck(frameBytes));}
        final var digest=version==1?envelopeHash(envelopeMigrationArchiveRowFields(version,copy,frameBytes,raw),
                "nereus-delay-migration-logical-first-native72-v1\0".length()):
                envelopeCall(envelopeMigrationArchiveRowFields(version,copy,frameBytes,raw),envelopeArray(32),0);
        ctor=envelopeTemporary(ctor,envelopeComparison(digest,envelopeArray(32)));
        return envelopeMigrationDecoded(n,copy?envelopeMigrationEntryBytes(raw):Math.max(frameBytes,envelopeMigrationEntryBytes(raw)),copy?13:12,args,ctor,
                envelopeMigrationArchiveRowCanonical(version,copy,frameBytes,raw));
    }
    private static long envelopeMigrationArchiveCopyMetadataBytes(){
        // Keep the codec's maximal nested length prefixes even for the zero-raw fixed term.
        return Math.subtractExact(com.nereusstream.delay.protocol.TargetMigrationArchiveRow.maximumCanonicalBytes(2,0),
                com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES);
    }
    private static void envelopeMigrationArchiveCopyCohort(long rows,long rawAggregate){
        if(rows<0||rows>com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_ROWS
                ||rawAggregate<0||rawAggregate>com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES
                ||rawAggregate>Math.multiplyExact(rows,com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES)
                ||rows==0&&rawAggregate!=0)
            throw new IllegalArgumentException("COPY cohort exceeds its original complete request aggregate");
    }
    static long envelopeArchiveCopyCanonicalBytes(long rows,long rawAggregate){
        envelopeMigrationArchiveCopyCohort(rows,rawAggregate);
        return Math.addExact(Math.multiplyExact(rows,envelopeMigrationArchiveCopyMetadataBytes()),rawAggregate);
    }
    private static long envelopeMigrationArchiveCopySlope(long full,long zero,long raw){
        return Math.floorDiv(Math.addExact(Math.subtractExact(full,zero),raw-1),raw);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveCopyMaterial(long rows,long rawAggregate,boolean decode){
        envelopeMigrationArchiveCopyCohort(rows,rawAggregate);
        if(rows==0)return envelopeEmpty();
        final long raw=com.nereusstream.delay.protocol.TargetMigrationArchiveEntry.MAX_RAW_BYTES;
        final var zero=decode?envelopeMigrationArchiveRowDecode(2,true,0,0):envelopeMigrationArchiveRowCanonical(2,true,0,0);
        final var full=decode?envelopeMigrationArchiveRowDecode(2,true,0,raw):envelopeMigrationArchiveRowCanonical(2,true,0,raw);
        final var fixed=envelopeRepeat(zero,rows,true);
        final long heldSlope=envelopeMigrationArchiveCopySlope(full.held,zero.held,raw);
        final long peakSlope=Math.max(heldSlope,envelopeMigrationArchiveCopySlope(full.retained,zero.retained,raw));
        final long allocated=Math.addExact(fixed.allocated,Math.multiplyExact(rawAggregate,
                envelopeMigrationArchiveCopySlope(full.allocated,zero.allocated,raw)));
        final long peak=Math.addExact(fixed.retained,Math.multiplyExact(rawAggregate,peakSlope));
        final long held=Math.addExact(fixed.held,Math.multiplyExact(rawAggregate,heldSlope));
        return new EnvelopeMaterial(allocated,peak,held,Math.multiplyExact(rows,Math.max(zero.fieldVisits,full.fieldVisits)),
                Math.multiplyExact(rows,Math.max(zero.arrayObjects,full.arrayObjects)));
    }
    static EnvelopeMaterial envelopeArchiveCopyDecodeMaterial(long rows,long rawAggregate){
        return envelopeMigrationArchiveCopyMaterial(rows,rawAggregate,true);
    }
    static EnvelopeMaterial envelopeArchiveCopyCanonicalMaterial(long rows,long rawAggregate){
        return envelopeMigrationArchiveCopyMaterial(rows,rawAggregate,false);
    }
    private static long envelopeMigrationArchiveStateBytes(int version,boolean history,long frameBytes){
        // Management steps keep the historical bounded COPY/TAKEOVER ceiling in either schema.
        if(history||version==2)return com.nereusstream.delay.protocol.TargetMigrationArchiveState.V2_MAX_CANONICAL_BYTES;
        return com.nereusstream.delay.protocol.TargetMigrationArchiveState.maximumCanonicalBytes(version,frameBytes);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveStateMetadata(boolean history){
        var fields=envelopeDrop(envelopeIncarnationCanonical(true),envelopeRepeat(envelopeMutationCanonical(false),5,false));
        fields=envelopeDrop(fields,envelopeMigrationCoverageCanonical());fields=envelopeDrop(fields,envelopeMigrationObjectCanonical());
        if(history){fields=envelopeDrop(fields,envelopeMigrationPreparedCanonical(39,false));fields=envelopeDrop(fields,envelopeSystemCanonical(envelopeMigrationInputBytes(39,false)));}
        return fields;
    }
    private static EnvelopeMaterial envelopeMigrationArchiveStateFields(int version,boolean history,boolean tail,long frameBytes){
        var fields=version==3?envelopeReturn(envelopeMigrationArchiveStateMetadata(history),0):envelopeEmpty();
        fields=envelopeDrop(fields,envelopeMigrationArchiveStateMetadata(history));
        return envelopeWritten(fields,envelopeMigrationArchiveStateBytes(version,history,frameBytes)-35);
    }
    private static EnvelopeMaterial envelopeMigrationArchiveStateCanonical(int version,boolean history,boolean tail,long frameBytes){
        final var fields=envelopeMigrationArchiveStateFields(version,history,tail,frameBytes);
        final long n=envelopeMigrationArchiveStateBytes(version,history,frameBytes);
        return version==2?envelopeMigrationHashed(fields,n,"nereus-delay-migration-archive-state-v1\0"):
                envelopeMigrationLocalHashed(fields,n,"nereus-delay-migration-archive-state-v3\0");
    }
    private static EnvelopeMaterial envelopeMigrationArchiveRequestRoot(){
        // resultingRoot: one assigned previous root and one Entry.digest per actual COPY row.
        final long count=com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_ROWS;
        final var each=envelopeHold(envelopeAssigned(32),envelopeArray(32));
        final var append=envelopeHash(each,"nereus-delay-migration-logical-first-archive-v1\0".length());
        return envelopeDrop(envelopeMigrationCopyEntries(false),envelopeRepeat(append,count,false));
    }
    private static EnvelopeMaterial envelopeMigrationArchiveStateDecode(int version,boolean history,boolean tail,long frameBytes){
        final long n=envelopeMigrationArchiveStateBytes(version,history,frameBytes);
        var args=envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true));args=envelopeHold(args,envelopeArray(5*32));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),5,true));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES,envelopeMigrationCoverageDecode()));
        args=envelopeHold(args,envelopeNested(envelopeMigrationObjectBytes(),envelopeMigrationObjectDecode()));
        if(history){args=envelopeHold(args,envelopeNested(envelopeMigrationPreparedBytes(39,false),envelopeMigrationPreparedDecode(39,false)));
            args=envelopeHold(args,envelopeNested(envelopeMigrationInputBytes(39,false),envelopeMigrationInputDecode(39,false)));}
        if(tail)args=envelopeHold(args,envelopeArray(frameBytes));
        var ctor=envelopeRepeat(envelopeAssigned(32),5,true);
        // coveredThrough.equals(mutation) includes the actual Source canonical images.
        ctor=envelopeTemporary(ctor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        ctor=envelopeTemporary(ctor,envelopeArray(32));
        if(version==3)ctor=envelopeTemporary(ctor,envelopeMigrationArchiveStateMetadata(history));
        if(history){
            ctor=envelopeTemporary(ctor,envelopeMigrationBodyGetter(39,false));ctor=envelopeTemporary(ctor,envelopeMigrationRequireInput(39,false));
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationRequestCanonical(39,false),envelopeMigrationRequestCanonical(39,false)));
            ctor=envelopeTemporary(ctor,envelopeScopeConstruct());ctor=envelopeTemporary(ctor,envelopeArray(96));
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeMigrationCoverageCanonical(),envelopeMigrationCoverageCanonical()));
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
            ctor=envelopeTemporary(ctor,envelopeMigrationArchiveRequestRoot());
            ctor=envelopeTemporary(ctor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
            ctor=envelopeTemporary(ctor,envelopeMigrationSha(envelopeSystemCanonical(envelopeMigrationInputBytes(39,false))));
            ctor=envelopeTemporary(ctor,envelopeMigrationBinding(39,false));
        }else if(tail){ctor=envelopeHold(ctor,envelopeArray(frameBytes));ctor=envelopeTemporary(ctor,envelopeMigrationFrameCheck(frameBytes));}
        final var digest=version==2?envelopeHash(envelopeMigrationArchiveStateFields(version,history,tail,frameBytes),
                "nereus-delay-migration-archive-state-v1\0".length()):
                envelopeCall(envelopeMigrationArchiveStateFields(version,history,tail,frameBytes),envelopeArray(32),0);
        ctor=envelopeTemporary(ctor,envelopeComparison(digest,envelopeArray(32)));
        return envelopeMigrationDecoded(n,history?Math.max(envelopeMigrationPreparedBytes(39,false),envelopeMigrationInputBytes(39,false)):
                tail?frameBytes:com.nereusstream.delay.protocol.TargetMigrationArchiveCoverage.MAX_CANONICAL_BYTES,history?22:tail?21:20,args,ctor,
                envelopeMigrationArchiveStateCanonical(version,history,tail,frameBytes));
    }
    private static EnvelopeMaterial envelopeMigrationDecodeMaterial(TargetSourceOperationEnvelope.Row row,long genuineFrameBytes){
        final long legacy=com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES;
        return switch(row){
            case MIGRATION->envelopeChoice(envelopeMigrationRootDecode(false,false),envelopeChoice(envelopeMigrationRootDecode(true,false),envelopeMigrationRootDecode(true,true)));
            case MIGRATION_ORIGIN->envelopeMigrationOriginDecode();
            case MIGRATION_GUARD->envelopeChoice(envelopeMigrationGuardDecode(false),envelopeMigrationGuardDecode(true));
            case MIGRATION_PREFIX->envelopeMigrationPrefixDecode();
            case MIGRATION_ATTEMPT->envelopeChoice(envelopeMigrationAttemptDecode(false),envelopeMigrationAttemptDecode(true));
            case RETIREMENT->envelopeChoice(envelopeMigrationRetirementDecode(false),envelopeMigrationRetirementDecode(true));
            case ARCHIVE_COPY->envelopeMigrationArchiveRowDecode(2,true,0);
            case ARCHIVE_ROW_V1->envelopeChoice(envelopeMigrationArchiveRowDecode(1,true,legacy),envelopeMigrationArchiveRowDecode(1,false,legacy));
            case ARCHIVE_ROW->{if(genuineFrameBytes<20)throw new IllegalArgumentException("missing genuine full Source maximum");
                final var retained=envelopeChoice(envelopeMigrationArchiveRowDecode(1,true,legacy),envelopeMigrationArchiveRowDecode(1,false,legacy));
                final var current=envelopeChoice(envelopeMigrationArchiveRowDecode(2,true,genuineFrameBytes),envelopeMigrationArchiveRowDecode(2,false,genuineFrameBytes));
                yield envelopeChoice(retained,current);}
            case ARCHIVE_HEAD_V2->envelopeChoice(envelopeMigrationArchiveStateDecode(2,false,false,legacy),envelopeMigrationArchiveStateDecode(2,false,true,legacy));
            case ARCHIVE_HEAD->{if(genuineFrameBytes<20)throw new IllegalArgumentException("missing genuine full Source maximum");
                final var retained=envelopeChoice(envelopeMigrationArchiveStateDecode(2,false,false,legacy),envelopeMigrationArchiveStateDecode(2,false,true,legacy));
                final var current=envelopeChoice(envelopeMigrationArchiveStateDecode(3,false,false,genuineFrameBytes),envelopeMigrationArchiveStateDecode(3,false,true,genuineFrameBytes));
                yield envelopeChoice(retained,current);}
            case ARCHIVE_STEP->envelopeChoice(envelopeMigrationArchiveStateDecode(2,true,false,legacy),envelopeMigrationArchiveStateDecode(3,true,false,legacy));
            default->throw new IllegalArgumentException("not an actual migration/archive Native row family");
        };
    }
    private static EnvelopeMaterial envelopeMigrationCanonicalMaterial(TargetSourceOperationEnvelope.Row row,long genuineFrameBytes){
        final long legacy=com.nereusstream.delay.protocol.TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES;
        return switch(row){
            case MIGRATION->envelopeChoice(envelopeMigrationRootCanonical(false,false),envelopeChoice(envelopeMigrationRootCanonical(true,false),envelopeMigrationRootCanonical(true,true)));
            case MIGRATION_ORIGIN->envelopeMigrationOriginCanonical();
            case MIGRATION_GUARD->envelopeChoice(envelopeMigrationGuardCanonical(false),envelopeMigrationGuardCanonical(true));
            case MIGRATION_PREFIX->envelopeMigrationPrefixCanonical();
            case MIGRATION_ATTEMPT->envelopeChoice(envelopeMigrationAttemptCanonical(false),envelopeMigrationAttemptCanonical(true));
            case RETIREMENT->envelopeChoice(envelopeMigrationRetirementCanonical(false),envelopeMigrationRetirementCanonical(true));
            case ARCHIVE_COPY->envelopeMigrationArchiveRowCanonical(2,true,0);
            case ARCHIVE_ROW_V1->envelopeChoice(envelopeMigrationArchiveRowCanonical(1,true,legacy),envelopeMigrationArchiveRowCanonical(1,false,legacy));
            case ARCHIVE_ROW->{if(genuineFrameBytes<20)throw new IllegalArgumentException("missing genuine full Source maximum");
                final var retained=envelopeChoice(envelopeMigrationArchiveRowCanonical(1,true,legacy),envelopeMigrationArchiveRowCanonical(1,false,legacy));
                final var current=envelopeChoice(envelopeMigrationArchiveRowCanonical(2,true,genuineFrameBytes),envelopeMigrationArchiveRowCanonical(2,false,genuineFrameBytes));
                yield envelopeChoice(retained,current);}
            case ARCHIVE_HEAD_V2->envelopeChoice(envelopeMigrationArchiveStateCanonical(2,false,false,legacy),envelopeMigrationArchiveStateCanonical(2,false,true,legacy));
            case ARCHIVE_HEAD->{if(genuineFrameBytes<20)throw new IllegalArgumentException("missing genuine full Source maximum");
                final var retained=envelopeChoice(envelopeMigrationArchiveStateCanonical(2,false,false,legacy),envelopeMigrationArchiveStateCanonical(2,false,true,legacy));
                final var current=envelopeChoice(envelopeMigrationArchiveStateCanonical(3,false,false,genuineFrameBytes),envelopeMigrationArchiveStateCanonical(3,false,true,genuineFrameBytes));
                yield envelopeChoice(retained,current);}
            case ARCHIVE_STEP->envelopeChoice(envelopeMigrationArchiveStateCanonical(2,true,false,legacy),envelopeMigrationArchiveStateCanonical(3,true,false,legacy));
            default->throw new IllegalArgumentException("not an actual migration/archive Native row family");
        };
    }

    /** Explicit original DTO creation, separate from its later typed-charge decode and serialization.
     * Each decoder graph contains the exact immutable constructor used by the named Source factory. */
    static EnvelopeMaterial envelopeMigrationCreationMaterial(TargetSourceOperationEnvelope.Row row){
        return switch(row){
            // TargetMigrationRoot.activate creates the active A1/A2 Root with this same constructor.
            case MIGRATION->envelopeChoice(envelopeMigrationRootDecode(true,false),envelopeMigrationRootDecode(true,true));
            case MIGRATION_GUARD->{
                // settle decodes/requires its request before constructing the new settled guard.
                var prefix=envelopeMigrationBodyGetter(35,false);
                prefix=envelopeTemporary(prefix,envelopeMigrationRequireInput(35,false));
                yield envelopeCall(prefix,envelopeMigrationGuardDecode(true),0);
            }
            // retiring and confirm directly invoke the corresponding complete immutable Record constructor.
            case RETIREMENT->envelopeChoice(envelopeMigrationRetirementDecode(false),envelopeMigrationRetirementDecode(true));
            default->throw new IllegalArgumentException("not an actual migration Source DTO factory");
        };
    }

    // LEGACY_SOURCE_MATERIAL_REGION: closed legacy Source body codec graph insertion point.

    /** Disjoint nested fields share the enclosing byte inventory, while each Reader still allocates. */
    private static EnvelopeMaterial envelopeLegacyReaders(long bytes,long readers){
        if(readers==0)return envelopeEmpty();
        final var parsed=envelopeProtoRead(bytes,bytes,bytes,bytes/2);
        return new EnvelopeMaterial(parsed.allocated,parsed.retained,parsed.held,parsed.fieldVisits,
                Math.addExact(parsed.arrayObjects,readers-1));
    }
    /** SystemMutationBodyCodec.validate: one top-level Reader and the spec's raw/nested accessors. */
    private static EnvelopeMaterial envelopeLegacyValidation(long bodyBytes,int byteFields,int nestedFields){
        final var parsed=envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,32);
        final var accessed=envelopeArrays(bodyBytes,byteFields);
        final var nested=envelopeLegacyReaders(bodyBytes,nestedFields);
        return envelopeReturn(envelopeTemporary(parsed,envelopeCall(accessed,nested,0)),0);
    }
    private static EnvelopeMaterial envelopeLegacyBodyFields(long bodyBytes,EnvelopeMaterial validation){
        return envelopeDrop(validation,envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,32));
    }
    /** Legacy body nested(field) obtains rawValue and reads its protobuf before returning that same raw. */
    private static EnvelopeMaterial envelopeLegacyNestedField(long bytes){
        return envelopeCall(envelopeArray(bytes),envelopeReturn(envelopeProtoRead(bytes,bytes,bytes),0),bytes);
    }
    private static EnvelopeMaterial envelopeLegacySelfRoutingDecode(){
        // SelfRoutingId.decode: route slice, RouteIncarnation's owned copy, FixedBytes' ID copy.
        return envelopeCall(envelopeArray(16),envelopeHold(envelopeArray(16),envelopeArray(DelayMessageId.LENGTH)),0);
    }
    private static EnvelopeMaterial envelopeLegacyMessageIdDecode(){
        return envelopeTemporary(envelopeArray(DelayMessageId.LENGTH),envelopeLegacySelfRoutingDecode());
    }
    private static EnvelopeMaterial envelopeLegacyMessageShardCheck(){
        final var message=envelopeLegacyMessageIdDecode();
        var work=envelopeCall(message,envelopeNested(24,envelopeShardSubjectDecode()),message.held);
        work=envelopeTemporary(work,envelopeLegacySelfRoutingDecode());
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeLegacyReplayValidation(long bodyBytes){
        return envelopeLegacyValidation(bodyBytes,4,2);
    }
    private static EnvelopeMaterial envelopeLegacyReplayDecode(long bodyBytes){
        var parsed=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyReplayValidation(bodyBytes));
        parsed=envelopeHold(parsed,envelopeNested(24,envelopeShardSubjectDecode()));
        var args=envelopeCall(envelopeLegacyNestedField(74),envelopeControlRefDecode(),0);
        args=envelopeHold(args,envelopeCall(envelopeArray(DelayMessageId.LENGTH),envelopeLegacyMessageIdDecode(),0));
        args=envelopeTemporary(args,envelopeLegacySelfRoutingDecode());
        args=envelopeHold(args,envelopeLegacyNestedField(bodyBytes));
        args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeArray(bodyBytes); // copyNested(retryPolicy)
        constructor=envelopeHold(constructor,envelopeRetryPolicyRefDecode(bodyBytes));
        constructor=envelopeHold(constructor,envelopeArray(32));
        return envelopeReturn(envelopeHold(parsed,envelopeCall(args,constructor,64+DelayMessageId.LENGTH)),
                Math.addExact(64+DelayMessageId.LENGTH,constructor.held));
    }
    private static EnvelopeMaterial envelopeLegacyReplayIdentity(long bodyBytes){
        return envelopeCall(envelopeLegacyReplayDecode(bodyBytes),envelopeControlLogicalIdentity(),0);
    }
    private static EnvelopeMaterial envelopeLegacyResolveValidation(long bodyBytes){
        return envelopeLegacyValidation(bodyBytes,7,2);
    }
    private static EnvelopeMaterial envelopeLegacyResolveDecode(long bodyBytes){
        var parsed=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyResolveValidation(bodyBytes));
        parsed=envelopeHold(parsed,envelopeNested(24,envelopeShardSubjectDecode()));
        var args=envelopeCall(envelopeLegacyNestedField(74),envelopeControlRefDecode(),0);
        args=envelopeHold(args,envelopeCall(envelopeArray(32),envelopeArray(32),0)); // DestinationLaneId
        args=envelopeHold(args,envelopeArray(16));
        args=envelopeHold(args,envelopeCall(envelopeArray(DelayMessageId.LENGTH),envelopeLegacyMessageIdDecode(),0));
        args=envelopeTemporary(args,envelopeLegacySelfRoutingDecode());
        args=envelopeHold(args,envelopeArray(32));
        // ResolveUncertain.optionalNested only takes rawValue; its generic Reader was in validate.
        args=envelopeHold(args,envelopeArray(bodyBytes));
        args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeHold(envelopeArrays(16+32+32,3),envelopeArray(bodyBytes));
        constructor=envelopeTemporary(constructor,envelopePublishEvidenceDecode(bodyBytes));
        constructor=envelopeTemporary(constructor,envelopePublishEvidenceOwnerCheck(bodyBytes));
        return envelopeReturn(envelopeHold(parsed,envelopeCall(args,constructor,64+32+DelayMessageId.LENGTH)),
                Math.addExact(64+32+DelayMessageId.LENGTH,constructor.held));
    }
    private static EnvelopeMaterial envelopeLegacyResolveIdentity(long bodyBytes){
        return envelopeCall(envelopeLegacyResolveDecode(bodyBytes),envelopeControlLogicalIdentity(),0);
    }
    private static EnvelopeMaterial envelopeLegacyApplyControlValidation(long bodyBytes){
        return envelopeLegacyValidation(bodyBytes,3,2);
    }
    private static EnvelopeMaterial envelopeLegacyApplyControlDecode(long bodyBytes){
        final var parsed=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyApplyControlValidation(bodyBytes));
        var args=envelopeCall(envelopeLegacyNestedField(74),envelopeControlRefDecode(),0);
        args=envelopeHold(args,envelopeArray(32));
        args=envelopeHold(args,envelopeLegacyNestedField(bodyBytes));
        var constructor=envelopeHold(envelopeArray(32),envelopeArray(bodyBytes));
        // validatePayloadBranch: one branch Reader, its raw accessor, then the selected payload Reader.
        var branch=envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,1);
        branch=envelopeTemporary(branch,envelopeCall(envelopeArray(bodyBytes),
                envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes),0));
        constructor=envelopeTemporary(constructor,branch);
        return envelopeReturn(envelopeHold(parsed,envelopeCall(args,constructor,64)),64+constructor.held);
    }
    private static EnvelopeMaterial envelopeLegacyApplyControlIdentity(long bodyBytes){
        return envelopeCall(envelopeLegacyApplyControlDecode(bodyBytes),envelopeControlLogicalIdentity(),0);
    }

    private static EnvelopeMaterial envelopeLegacyCanonicalFields(long encodedBytes,long byteFields){
        return envelopeWritten(envelopeArrays(encodedBytes,byteFields),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyChargeValidation(boolean canonical){
        // Fields 1..15 have a one-byte tag, 16..17 two; each uint64 has at most ten bytes.
        final long encoded=15L*11+2L*12;
        var work=envelopeProtoRead(encoded,0,0,17);
        if(canonical)work=envelopeTemporary(work,envelopeArray(encoded));
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeLegacyClaimPreconditionDecode(long encodedBytes){
        final long fixedBytes=5L*32+DelayMessageId.LENGTH+2L*16;
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,20);
        var materialized=envelopeLegacyNestedField(encodedBytes);
        materialized=envelopeTemporary(materialized,envelopeLegacyClaimMaterializationDecode(encodedBytes));
        materialized=envelopeTemporary(materialized,envelopeArrays(64,2));
        materialized=envelopeTemporary(materialized,envelopeLegacyClaimMaterializationDigest(encodedBytes));
        var args=envelopeChoice(materialized,envelopeArray(0));
        var owner=envelopeLegacyNestedField(encodedBytes);
        owner=envelopeTemporary(owner,envelopeOwnerIdentityDecode(encodedBytes));
        args=envelopeHold(args,owner);
        // Validation and the retained claimedCharge are distinct nested(field12) calls.
        args=envelopeTemporary(args,envelopeCall(envelopeLegacyNestedField(189),envelopeLegacyChargeValidation(true),0));
        args=envelopeHold(args,envelopeLegacyNestedField(189));
        args=envelopeHold(args,envelopeCall(envelopeArrays(fixedBytes,8),envelopeArrays(fixedBytes,8),0));
        var constructor=envelopeHold(envelopeArray(encodedBytes),envelopeArrays(fixedBytes,8));
        constructor=envelopeHold(constructor,envelopeArray(encodedBytes)); // materialization
        constructor=envelopeHold(constructor,envelopeArray(189)); // claimedCharge
        constructor=envelopeHold(constructor,envelopeArray(encodedBytes)); // ownerIdentity
        var work=envelopeHold(parsed,envelopeCall(args,constructor,0));
        work=envelopeTemporary(work,envelopeLegacyCanonicalFields(encodedBytes,12));
        return envelopeReturn(work,constructor.held);
    }
    private static EnvelopeMaterial envelopeLegacyClaimResultValidation(long bodyBytes){
        return envelopeLegacyValidation(bodyBytes,7,3);
    }
    private static EnvelopeMaterial envelopeLegacyClaimResultDecode(long bodyBytes){
        final long fixedBytes=2L*32+DelayMessageId.LENGTH+16;
        var parsed=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyClaimResultValidation(bodyBytes));
        var args=envelopeCall(envelopeArrays(fixedBytes,4),envelopeArrays(fixedBytes,4),0);
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyClaimPreconditionDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyTimeDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeLegacyNestedField(189));
        args=envelopeTemporary(args,envelopeLegacyChargeValidation(true));
        args=envelopeTemporary(args,envelopeLegacyMessageShardCheck());
        var constructor=envelopeHold(envelopeArrays(fixedBytes,4),envelopeArray(189));
        constructor=envelopeTemporary(constructor,envelopeArrays(fixedBytes,4));
        constructor=envelopeTemporary(constructor,envelopeArray(189));
        return envelopeReturn(envelopeHold(parsed,envelopeCall(args,constructor,args.held-fixedBytes-189)),
                Math.addExact(args.held-fixedBytes-189,constructor.held));
    }
    private static EnvelopeMaterial envelopeLegacyClaimResultIdentity(long bodyBytes){
        return envelopeCall(envelopeLegacyClaimResultDecode(bodyBytes),envelopeArray(32),0);
    }
    private static EnvelopeMaterial envelopeLegacyDlqExportValidation(long bodyBytes){
        return envelopeLegacyValidation(bodyBytes,7,4);
    }
    private static EnvelopeMaterial envelopeLegacyDlqRetryDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,9);
        final var policy=envelopeCall(envelopeLegacyNestedField(encodedBytes),envelopeRetryPolicyRefDecode(encodedBytes),0);
        return envelopeReturn(envelopeHold(parsed,envelopeCall(policy,envelopeArray(encodedBytes),policy.held)),
                Math.addExact(policy.held,encodedBytes));
    }
    private static EnvelopeMaterial envelopeLegacyDlqExportDecode(long bodyBytes){
        final long fixedBytes=64L+DelayMessageId.LENGTH;
        final var parsed=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyDlqExportValidation(bodyBytes));
        final var timeValue=envelopeLegacyTimeDecode(bodyBytes);
        final var retryValue=envelopeLegacyDlqRetryDecode(bodyBytes);
        var args=envelopeArrays(fixedBytes,3);
        var evidence=envelopeLegacyNestedField(bodyBytes);
        // evidenceValue lives through the final requireDlqMutation, alongside its original raw bytes.
        evidence=envelopeCall(evidence,envelopePublishEvidenceDecode(bodyBytes),bodyBytes);
        args=envelopeHold(args,envelopeChoice(evidence,envelopeArray(0)));
        args=envelopeHold(args,envelopeLegacyNestedField(189));
        args=envelopeTemporary(args,envelopeLegacyChargeValidation(true));
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),timeValue,0));
        var retry=envelopeLegacyNestedField(bodyBytes);
        retry=envelopeCall(retry,retryValue,bodyBytes);
        args=envelopeHold(args,retry);
        args=envelopeTemporary(args,envelopeLegacyMessageShardCheck());
        var constructor=envelopeHold(envelopeArrays(fixedBytes,3),envelopeArray(bodyBytes)); // evidence
        constructor=envelopeHold(constructor,envelopeArray(189)); // transfer
        constructor=envelopeHold(constructor,envelopeArray(bodyBytes)); // retryDecision
        var work=envelopeHold(parsed,envelopeCall(args,constructor,args.held-fixedBytes));
        work=envelopeTemporary(work,envelopePublishEvidenceOwnerCheck(bodyBytes));
        final long returned=Math.addExact(fixedBytes+189,Math.addExact(Math.addExact(bodyBytes,bodyBytes),
                Math.addExact(timeValue.held,retryValue.held)));
        return envelopeReturn(work,returned);
    }
    private static EnvelopeMaterial envelopeLegacyDlqExportIdentity(long bodyBytes){
        final long attemptDomain="nereus-delay-dlq-export-attempt-logical-id\0".length();
        final long evidenceDomain="nereus-delay-dlq-export-evidence-logical-id\0".length();
        final var attemptArgs=envelopeHold(envelopeArray(attemptDomain),envelopeHold(envelopeArray(32),envelopeArray(4)));
        final var attempt=envelopeHash(attemptArgs,0);
        var evidence=envelopePublishEvidenceDecode(bodyBytes);
        evidence=envelopeCall(evidence,envelopeArray(32),0);
        evidence=envelopeCall(evidence,envelopeArrays(64,2),0);
        evidence=envelopeHash(envelopeHold(envelopeArray(evidenceDomain),evidence),0);
        return envelopeCall(envelopeLegacyDlqExportDecode(bodyBytes),envelopeChoice(attempt,evidence),0);
    }

    // ACTIVATION_MATERIAL_REGION: protocol activation and its original public issuer inputs insertion point.
    /** Batched public inputs share their enclosing byte budget; count is the independent schema cardinality. */
    private static EnvelopeMaterial envelopeActivationBatchRead(long encoded,long fields,long count,long objects){
        if(objects==0)return envelopeEmpty();
        return new EnvelopeMaterial(Math.addExact(encoded,Math.multiplyExact(2,fields)),
                Math.addExact(encoded,Math.multiplyExact(2,fields)),fields,count,
                Math.addExact(objects,Math.multiplyExact(2,count)));
    }
    private static EnvelopeMaterial envelopeActivationBatchWritten(EnvelopeMaterial fields,long encoded,long count){
        if(count==0)return envelopeEmpty();
        // Earlier result images may remain in a list/key cache while a later image evaluates its fields.
        return new EnvelopeMaterial(Math.addExact(fields.allocated,encoded),Math.addExact(fields.retained,encoded),encoded,
                fields.fieldVisits,Math.addExact(fields.arrayObjects,count));
    }
    private static EnvelopeMaterial envelopeActivationBatchHash(EnvelopeMaterial fields,String domain,long count){
        if(count==0)return envelopeEmpty();
        final long joined=Math.addExact(fields.held,Math.multiplyExact(domain.length(),count));
        final long digests=Math.multiplyExact(32,count);
        return envelopeCall(fields,new EnvelopeMaterial(Math.addExact(joined,digests),Math.addExact(joined,digests),digests,0,
                Math.multiplyExact(2,count)),0);
    }
    private static long envelopeActivationArtifactBytes(){return 158;}
    private static EnvelopeMaterial envelopeActivationBaseTupleCanonical(){return envelopeArray(10);}
    private static EnvelopeMaterial envelopeActivationBaseTupleDecode(){
        return envelopeReturn(envelopeTemporary(envelopeProtoRead(10,5,1,5),envelopeActivationBaseTupleCanonical()),0);
    }
    private static EnvelopeMaterial envelopeActivationArtifactFields(){
        return envelopeWritten(envelopeRepeat(envelopeActivationBaseTupleCanonical(),2,false),123);
    }
    private static EnvelopeMaterial envelopeActivationArtifactCanonical(){
        // The generation-set digest is stored, rather than recomputed by canonicalBytes.
        return envelopeWritten(envelopeRepeat(envelopeActivationBaseTupleCanonical(),2,false),envelopeActivationArtifactBytes());
    }
    private static EnvelopeMaterial envelopeActivationArtifactDecode(){
        final var parsed=envelopeProtoRead(envelopeActivationArtifactBytes(),136,32,16);
        var args=envelopeRepeat(envelopeNested(10,envelopeActivationBaseTupleDecode()),2,true);
        args=envelopeHold(args,envelopeArrays(96,3));
        var constructor=envelopeArrays(64,2);
        final var lock=envelopeCall(envelopeArray(com.nereusstream.delay.protocol.PulsarSourceLock.SOURCE_LOCK.length()),envelopeArray(32),0);
        constructor=envelopeTemporary(constructor,lock);
        var digestFields=envelopeHold(envelopeArray("nereus-delay-artifact-generation-set\0".length()),envelopeActivationArtifactFields());
        constructor=envelopeHold(constructor,envelopeHash(digestFields,0));
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,0));
        frame=envelopeTemporary(frame,envelopeActivationArtifactCanonical());return envelopeReturn(frame,96);
    }
    private static long envelopeActivationDescriptorBudget(long count,long enclosing){
        return Math.min(Math.multiplyExact(com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES,count),enclosing);
    }
    private static EnvelopeMaterial envelopeActivationDescriptorFields(long count,long budget){
        if(count==0)return envelopeEmpty();
        var fields=envelopeRepeat(envelopeActivationArtifactCanonical(),count,false);
        fields=envelopeDrop(fields,envelopeArrays(32*count,count)); // base.setDigest accessor
        fields=envelopeDrop(fields,envelopeRepeat(envelopeTupleCanonical(),count,false));
        // Descriptor1..21: at most 50 uint32 version-matrix fields, two-byte tags and <=5-byte values.
        fields=envelopeDrop(fields,envelopeArrays(350*count,count));
        fields=envelopeDrop(fields,envelopeArrays(80*count,2*count)); // two exact 40-byte source commits
        return envelopeActivationBatchWritten(fields,budget,count);
    }
    private static EnvelopeMaterial envelopeActivationDescriptorDigest(long count,long budget){
        return envelopeActivationBatchHash(envelopeActivationDescriptorFields(count,budget),
                "nereus-delay-target-protocol-activation-descriptor\0",count);
    }
    private static EnvelopeMaterial envelopeActivationDescriptorCanonical(long count,long enclosing){
        if(count==0)return envelopeEmpty();
        final long budget=envelopeActivationDescriptorBudget(count,enclosing);
        final var fields=envelopeActivationDescriptorFields(count,budget);
        final var seal=envelopeActivationBatchWritten(envelopeActivationDescriptorDigest(count,budget),34*count,count);
        return envelopeCall(envelopeHold(fields,seal),envelopeArrays(budget,count),0);
    }
    private static EnvelopeMaterial envelopeActivationDescriptorCanonical(){
        return envelopeActivationDescriptorCanonical(1,com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeActivationDescriptorDecode(long count,long enclosing){
        if(count==0)return envelopeEmpty();
        final long budget=envelopeActivationDescriptorBudget(count,enclosing);
        var frame=envelopeActivationBatchRead(budget,budget,12*count,count);
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArrays(350*count,count),envelopeArrays(350*count,count)));
        var args=envelopeRepeat(envelopeNested(envelopeActivationArtifactBytes(),envelopeActivationArtifactDecode()),count,true);
        args=envelopeHold(args,envelopeArrays(128*count,4*count));
        args=envelopeTemporary(args,envelopeArrays(80*count,2*count)); // US_ASCII input arrays become Strings
        final var fourAssigned=envelopeRepeat(envelopeAssigned(32),4*count,true);
        frame=envelopeHold(frame,envelopeCall(args,fourAssigned,96*count));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArrays(32*count,count),envelopeArrays(32*count,count)));
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeNested(26,envelopeTupleDecode()),count,false));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArrays(32*count,count),envelopeActivationDescriptorDigest(count,budget)));
        frame=envelopeTemporary(frame,envelopeActivationDescriptorCanonical(count,budget));
        return envelopeReturn(frame,224*count);
    }
    private static EnvelopeMaterial envelopeActivationCapabilitySix(long count,long budget){
        var fields=envelopeArrays(budget,count); // aggregate UTF-8 worker IDs, bounded by the enclosing bytes
        fields=envelopeDrop(fields,envelopeRepeat(envelopeTupleCanonical(),32*count,false));
        return envelopeActivationBatchWritten(fields,budget,count);
    }
    private static long envelopeActivationDescriptorSlots(int version){return version==5?11:version==4?10:version==3?4:0;}
    private static EnvelopeMaterial envelopeActivationCapabilityFields(int version,long count,long budget){
        var fields=envelopeActivationCapabilitySix(count,budget);
        if(version!=1)fields=envelopeDrop(fields,envelopeRepeat(envelopeActivationArtifactCanonical(),32*count,false));
        final long descriptors=envelopeActivationDescriptorSlots(version)*count;
        if(descriptors!=0)fields=envelopeDrop(fields,envelopeActivationDescriptorCanonical(descriptors,budget));
        return envelopeActivationBatchWritten(fields,budget,count);
    }
    private static EnvelopeMaterial envelopeActivationCapabilityCanonical(int version,long count,long budget){
        var fields=envelopeActivationCapabilitySix(count,budget);
        if(version!=1)fields=envelopeDrop(fields,envelopeRepeat(envelopeActivationArtifactCanonical(),32*count,false));
        final long descriptors=envelopeActivationDescriptorSlots(version)*count;
        if(descriptors!=0)fields=envelopeDrop(fields,envelopeActivationDescriptorCanonical(descriptors,budget));
        return envelopeActivationBatchWritten(fields,budget,count);
    }
    private static EnvelopeMaterial envelopeActivationCapabilityCanonical(long count,long budget){
        var graph=envelopeActivationCapabilityCanonical(1,count,budget);
        for(int version=2;version<=5;version++)graph=envelopeChoice(graph,envelopeActivationCapabilityCanonical(version,count,budget));
        return graph;
    }
    private static EnvelopeMaterial envelopeActivationCapabilityDecode(int version,long count,long budget){
        final long artifacts=version==1?0:32*count,descriptors=envelopeActivationDescriptorSlots(version)*count;
        final long fields=6*count+32*count+artifacts+descriptors;
        var frame=envelopeActivationBatchRead(budget,budget,fields,count);
        frame=envelopeTemporary(frame,envelopeArrays(budget,count)); // workerId bytes accessor
        frame=envelopeTemporary(frame,envelopeArrays(budget,count)); // UTF-8 round trip
        var args=envelopeRepeat(envelopeNested(26,envelopeTupleDecode()),32*count,true);
        args=envelopeHold(args,envelopeRepeat(envelopeNested(envelopeActivationArtifactBytes(),envelopeActivationArtifactDecode()),artifacts,true));
        if(descriptors!=0)args=envelopeHold(args,envelopeCall(envelopeArrays(budget,descriptors),envelopeActivationDescriptorDecode(descriptors,budget),0));
        args=envelopeHold(args,envelopeArrays(96*count,3*count)); // identity, session and supplied digest
        var constructor=envelopeArrays(64*count,2*count);
        // CanonicalOrder caches each actual tuple/artifact/descriptor key once per constructor.
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeTupleCanonical(),32*count,true));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeActivationArtifactCanonical(),artifacts,true));
        if(descriptors!=0)constructor=envelopeHold(constructor,envelopeActivationDescriptorCanonical(descriptors,budget));
        // Duplicate descriptors compare the cached keys; base/tuple contains use scalar/fixed fields.
        constructor=envelopeHold(constructor,envelopeActivationBatchHash(envelopeActivationCapabilityFields(version,count,budget),
                "nereus-delay-protocol-capability-declaration\0",count));
        constructor=envelopeReturn(constructor,96*count); // outer targetOrder keys cease to escape at constructor return
        final long nested=96*artifacts+224*descriptors;
        frame=envelopeHold(frame,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeActivationCapabilityCanonical(version,count,budget));
        return envelopeReturn(frame,nested+96*count);
    }
    private static EnvelopeMaterial envelopeActivationCapabilityDecode(long count,long budget){
        var graph=envelopeActivationCapabilityDecode(1,count,budget);
        for(int version=2;version<=5;version++)graph=envelopeChoice(graph,envelopeActivationCapabilityDecode(version,count,budget));
        return graph;
    }
    private static EnvelopeMaterial envelopeActivationReadersFields(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_READERS,budget=com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_BYTES;
        var fields=envelopeActivationDescriptorDigest(1,com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES);
        final var readers=envelopeActivationBatchWritten(envelopeActivationCapabilityCanonical(n,budget),budget,n);
        fields=envelopeDrop(fields,readers);return envelopeWritten(fields,budget);
    }
    private static EnvelopeMaterial envelopeActivationReadersDigest(){
        return envelopeHash(envelopeActivationReadersFields(),"nereus-delay-target-protocol-eligible-reader-set\0".length());
    }
    private static EnvelopeMaterial envelopeActivationReadersCanonical(){
        return envelopeConcat(envelopeHold(envelopeActivationReadersFields(),envelopeWritten(envelopeActivationReadersDigest(),34)));
    }
    private static EnvelopeMaterial envelopeActivationReadersDecode(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_READERS,budget=com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_BYTES;
        var frame=envelopeProtoRead(budget,budget,budget,n+3);
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeActivationDescriptorDigest(1,com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES)));
        var readers=envelopeActivationBatchRead(budget,budget,2*n,n);
        readers=envelopeHold(readers,envelopeCall(envelopeArrays(budget,n),envelopeActivationCapabilityDecode(n,budget),0));
        final long kept=readers.held-budget;
        readers=envelopeReturn(readers,kept);
        var constructor=envelopeReturn(envelopeActivationCapabilityCanonical(n,budget),0); // finite-size preflight
        // supports(descriptor) scans at most eleven descriptors; each equals canonicalizes both operands.
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeActivationDescriptorCanonical(),envelopeActivationDescriptorCanonical()),11*n,false));
        frame=envelopeHold(frame,envelopeCall(readers,constructor,kept));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeActivationReadersDigest()));
        frame=envelopeTemporary(frame,envelopeActivationReadersCanonical());return envelopeReturn(frame,kept);
    }
    private static EnvelopeMaterial envelopeActivationProfileCanonical(long count,long budget){return envelopeArrays(budget,count);}
    private static EnvelopeMaterial envelopeActivationProfileDecode(long count,long budget){
        var frame=envelopeActivationBatchRead(budget,budget,4*count,count);
        var args=envelopeHold(envelopeArrays(budget,count),envelopeArrays(32*count,count));
        final var constructor=envelopeHold(envelopeArrays(budget,count),envelopeArrays(32*count,count));
        frame=envelopeHold(frame,envelopeCall(args,constructor,0));
        frame=envelopeTemporary(frame,envelopeActivationProfileCanonical(count,budget));return envelopeReturn(frame,budget+32*count);
    }
    private static EnvelopeMaterial envelopeInitialInputFields(){
        final long budget=com.nereusstream.delay.protocol.TargetControlSnapshotInput.MAX_CANONICAL_BYTES;
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeTupleCanonical(),32,false));
        fields=envelopeDrop(fields,envelopeActivationProfileCanonical(4096,budget));return envelopeWritten(fields,budget);
    }
    private static EnvelopeMaterial envelopeInitialInputCanonical(){
        return envelopeWritten(envelopeInitialInputFields(),com.nereusstream.delay.protocol.TargetControlSnapshotInput.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeInitialInputDecode(){
        final long budget=com.nereusstream.delay.protocol.TargetControlSnapshotInput.MAX_CANONICAL_BYTES;
        var frame=envelopeProtoRead(budget,budget,budget,4140);
        var args=envelopeRepeat(envelopeNested(26,envelopeTupleDecode()),32,true);
        args=envelopeHold(args,envelopeCall(envelopeArrays(budget,4096),envelopeActivationProfileDecode(4096,budget),0));
        args=envelopeHold(args,envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode()));
        args=envelopeHold(args,envelopeArrays(80,3));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        args=envelopeHold(args,envelopeArray(64));
        var constructor=envelopeReturn(envelopeSourceBounded(),0);
        // assigned return values are discarded here, then each of the three fields is independently copied.
        constructor=envelopeTemporary(constructor,envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),2,true)));
        constructor=envelopeHold(constructor,envelopeArrays(80,3));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeTupleCanonical(),32,false)); // TreeMap key images
        constructor=envelopeTemporary(constructor,envelopeActivationProfileCanonical(4096,budget));
        constructor=envelopeTemporary(constructor,envelopeArrays(budget,4096)); // duplicate immutable profileId getters
        constructor=envelopeHold(constructor,envelopeHash(envelopeInitialInputFields(),"nereus-delay-target-control-snapshot-input\0".length()));
        constructor=envelopeHold(constructor,envelopeArray(64));
        final long borrowed=budget+32L*4096+112+48;
        frame=envelopeHold(frame,envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeInitialInputCanonical());
        return envelopeReturn(frame,borrowed+176);
    }
    private static EnvelopeMaterial envelopeInitialFields(){
        var fields=envelopeDrop(envelopeInitialInputCanonical(),envelopeIdentityCanonical());
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));fields=envelopeDrop(fields,envelopeGrantActivationCanonical());
        return envelopeWritten(fields,TargetInitialControlSnapshot.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeInitialCanonical(){
        return envelopeWritten(envelopeInitialFields(),TargetInitialControlSnapshot.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeInitialDecode(){
        final long input=com.nereusstream.delay.protocol.TargetControlSnapshotInput.MAX_CANONICAL_BYTES;
        var args=envelopeNested(input,envelopeInitialInputDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(TargetQuotaGrantActivation.MAX_CANONICAL_BYTES,envelopeGrantActivationDecode()));
        args=envelopeHold(args,envelopeArray(104));
        var constructor=envelopeReturn(envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()),0);
        constructor=envelopeTemporary(constructor,envelopeArrays(64,2)); // mutationDigest, original envelopeDigest
        constructor=envelopeTemporary(constructor,envelopeAuthorDecode(104));
        constructor=envelopeTemporary(constructor,envelopeArrays(80,3)); // requireInitial call arguments
        constructor=envelopeTemporary(constructor,envelopeArray(32)); // grant.digest in requireInitial
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical())); // mutation.equals
        constructor=envelopeHold(constructor,envelopeArray(104));
        constructor=envelopeHold(constructor,envelopeHash(envelopeInitialFields(),"nereus-delay-target-initial-control-snapshot\0".length()));
        final long borrowed=args.held-104;
        var frame=envelopeHold(envelopeProtoRead(TargetInitialControlSnapshot.MAX_CANONICAL_BYTES,
                TargetInitialControlSnapshot.MAX_CANONICAL_BYTES,TargetInitialControlSnapshot.MAX_CANONICAL_BYTES,7),
                envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeInitialCanonical());
        return envelopeReturn(frame,borrowed+136);
    }
    private static EnvelopeMaterial envelopeActivationResetScopeCanonical(long budget){
        var fields=envelopeArrays(budget,2); // both variable UTF-8 identifiers share the manifest budget
        fields=envelopeDrop(fields,envelopeShardSubjectCanonical());return envelopeWritten(fields,budget);
    }
    private static EnvelopeMaterial envelopeActivationResetScopeDecode(long budget){
        var frame=envelopeProtoRead(budget,budget,budget,5);
        frame=envelopeTemporary(frame,envelopeArrays(budget,2)); // identifier accessors
        frame=envelopeTemporary(frame,envelopeArrays(budget,2)); // strict UTF-8 round trips
        var args=envelopeHold(envelopeArrays(64,2),envelopeNested(24,envelopeShardSubjectDecode()));
        frame=envelopeHold(frame,envelopeCall(args,envelopeArrays(64,2),16));
        frame=envelopeTemporary(frame,envelopeActivationResetScopeCanonical(budget));return envelopeReturn(frame,80);
    }
    private static EnvelopeMaterial envelopeActivationResetResourcesCanonical(long count,long budget){
        return envelopeActivationBatchWritten(envelopeArrays(budget,2*count),budget,count);
    }
    private static EnvelopeMaterial envelopeActivationResetResourcesDecode(long count,long budget){
        var frame=envelopeActivationBatchRead(budget,budget,5*count,count);
        frame=envelopeTemporary(frame,envelopeArrays(budget,2*count)); // kind/identity accessors
        frame=envelopeTemporary(frame,envelopeArrays(budget,2*count)); // strict UTF-8 round trips
        frame=envelopeHold(frame,envelopeCall(envelopeArrays(64*count,2*count),envelopeArrays(64*count,2*count),0));
        frame=envelopeTemporary(frame,envelopeActivationResetResourcesCanonical(count,budget));return envelopeReturn(frame,64*count);
    }
    private static EnvelopeMaterial envelopeActivationResetWorkersCanonical(long count,long budget){
        var fields=envelopeArrays(budget,count);
        fields=envelopeDrop(fields,envelopeActivationCapabilityCanonical(count,budget));
        return envelopeActivationBatchWritten(fields,budget,count);
    }
    private static EnvelopeMaterial envelopeActivationResetWorkersDecode(long count,long budget){
        var frame=envelopeActivationBatchRead(budget,budget,5*count,count);
        frame=envelopeTemporary(frame,envelopeArrays(budget,count));frame=envelopeTemporary(frame,envelopeArrays(budget,count));
        var args=envelopeHold(envelopeArrays(64*count,2*count),envelopeCall(envelopeArrays(budget,count),envelopeActivationCapabilityDecode(count,budget),0));
        final long nested=args.held-64*count;
        args=envelopeHold(args,envelopeArrays(32*count,count));
        var constructor=envelopeArrays(96*count,3*count);
        constructor=envelopeTemporary(constructor,envelopeArrays(64*count,2*count)); // declaration identity/session getters
        frame=envelopeHold(frame,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeActivationResetWorkersCanonical(count,budget));return envelopeReturn(frame,nested+96*count);
    }
    private static EnvelopeMaterial envelopeActivationResetObligationsCanonical(){return envelopeArray(40);}
    private static EnvelopeMaterial envelopeActivationResetObligationsDecode(){
        var frame=envelopeHold(envelopeProtoRead(40,35,32,4),envelopeCall(envelopeArray(32),envelopeArray(32),0));
        frame=envelopeTemporary(frame,envelopeActivationResetObligationsCanonical());return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeActivationResetWindowDecode(){
        return envelopeReturn(envelopeTemporary(envelopeProtoRead(20,18,9,2),envelopeArray(20)),0);
    }
    private static EnvelopeMaterial envelopeActivationResetFields(){
        final long budget=1L<<20,count=4096;
        var fields=envelopeActivationResetScopeCanonical(budget);fields=envelopeDrop(fields,envelopeArray(40));
        fields=envelopeDrop(fields,envelopeActivationArtifactCanonical());
        // Each sortedResources invocation retains at most one cached canonical key per resource.
        fields=envelopeDrop(fields,envelopeReturn(envelopeActivationResetResourcesCanonical(count,budget),0));
        fields=envelopeDrop(fields,envelopeActivationResetResourcesCanonical(count,budget));
        fields=envelopeDrop(fields,envelopeActivationResetObligationsCanonical());
        fields=envelopeDrop(fields,envelopeReturn(envelopeArrays(budget,count),0)); // sortedWorkers cached UTF-8 IDs
        fields=envelopeDrop(fields,envelopeActivationResetWorkersCanonical(count,budget));
        fields=envelopeDrop(fields,envelopeBusinessTimeCanonical(budget));fields=envelopeDrop(fields,envelopeArray(20));
        return envelopeWritten(fields,budget);
    }
    private static EnvelopeMaterial envelopeActivationResetCanonical(){return envelopeWritten(envelopeActivationResetFields(),1L<<20);}
    private static EnvelopeMaterial envelopeActivationResetDecode(){
        final long budget=1L<<20,count=4096;
        var args=envelopeNested(budget,envelopeActivationResetScopeDecode(budget));
        args=envelopeTemporary(args,envelopeArray(40));args=envelopeTemporary(args,envelopeArray(40)); // baseline text plus strict UTF-8
        args=envelopeHold(args,envelopeNested(envelopeActivationArtifactBytes(),envelopeActivationArtifactDecode()));
        args=envelopeHold(args,envelopeCall(envelopeArrays(budget,count),envelopeActivationResetResourcesDecode(count,budget),0));
        args=envelopeHold(args,envelopeArray(32));
        args=envelopeHold(args,envelopeNested(40,envelopeActivationResetObligationsDecode()));
        args=envelopeHold(args,envelopeCall(envelopeArrays(budget,count),envelopeActivationResetWorkersDecode(count,budget),0));
        args=envelopeHold(args,envelopeNested(budget,envelopeBusinessTimeDecode(budget)));
        args=envelopeHold(args,envelopeNested(20,envelopeActivationResetWindowDecode()));args=envelopeHold(args,envelopeArrays(96,2));
        var constructor=envelopeReturn(envelopeActivationResetResourcesCanonical(count,budget),0);
        constructor=envelopeTemporary(constructor,envelopeArrays(budget,count)); // sorted worker keys
        constructor=envelopeHold(constructor,envelopeArray(32));
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeActivationResetFields(),"nereus-delay-data-reset-manifest\0".length()));
        constructor=envelopeHold(constructor,envelopeArrays(96,2));
        final long borrowed=args.held-128;
        var frame=envelopeHold(envelopeProtoRead(budget,budget,budget,12+2*count),envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeActivationResetCanonical());return envelopeReturn(frame,borrowed+128);
    }
    private static EnvelopeMaterial envelopeActivationResetVerify(){
        // signatureInput: the source-created u32 key-generation image and concatenated signature preimage.
        return envelopeReturn(envelopeCall(envelopeArray(4),envelopeArray("nereus-delay-data-reset-manifest-signature\0".length()+36),0),0);
    }
    private static long envelopeProtocolActivationBytes(){
        return envelopeCommonBytesField(1,26)+envelopeCommonBytesField(2,32)+envelopeCommonBytesField(3,32)
                +envelopeCommonBytesField(4,com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES)
                +envelopeCommonBytesField(5,32)+envelopeCommonBytesField(6,envelopeActivationArtifactBytes())+envelopeCommonBytesField(7,32);
    }
    private static EnvelopeMaterial envelopeProtocolVersionCanonical(){
        return envelopeWritten(envelopeDrop(envelopeTupleCanonical(),envelopeActivationArtifactCanonical()),
                3L*34+28+envelopeActivationArtifactBytes()+3);
    }
    private static EnvelopeMaterial envelopeProtocolVersionDecode(){
        final long n=3L*34+28+envelopeActivationArtifactBytes()+3;
        var args=envelopeNested(26,envelopeTupleDecode());args=envelopeHold(args,envelopeArrays(96,3));
        args=envelopeHold(args,envelopeNested(envelopeActivationArtifactBytes(),envelopeActivationArtifactDecode()));
        final var constructor=envelopeTemporary(envelopeArrays(96,3),envelopeArray(32));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,5),envelopeCall(args,constructor,96));
        frame=envelopeTemporary(frame,envelopeProtocolVersionCanonical());return envelopeReturn(frame,192);
    }
    private static EnvelopeMaterial envelopeProtocolActivationCanonical(){
        return envelopeWritten(envelopeDrop(envelopeTupleCanonical(),envelopeActivationArtifactCanonical()),envelopeProtocolActivationBytes());
    }
    private static EnvelopeMaterial envelopeProtocolActivationDecode(){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,n=envelopeProtocolActivationBytes();
        var args=envelopeNested(26,envelopeTupleDecode());args=envelopeHold(args,envelopeArrays(64,2));
        args=envelopeHold(args,envelopeNested(envelopeActivationArtifactBytes(),envelopeActivationArtifactDecode()));
        args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeArray(source));args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeArrays(64,2);
        // Current Activation constructs ProtocolVersionActivatePayload, copies its two hashes/manifest,
        // checks the artifact schema getter and takes one further manifestDigest getter copy.
        var payload=envelopeArrays(96,3);payload=envelopeTemporary(payload,envelopeArray(32));
        constructor=envelopeTemporary(constructor,payload);constructor=envelopeHold(constructor,envelopeArray(32));
        constructor=envelopeHold(constructor,envelopeCall(envelopeSourceDecode(),envelopeSourceCanonical(),0));
        constructor=envelopeHold(constructor,envelopeArray(32));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,7),envelopeCall(args,constructor,96));
        return envelopeReturn(frame,96+128+source);
    }
    private static EnvelopeMaterial envelopeActivationAuthorizationFields(){
        var fields=envelopeDrop(envelopeIdentityCanonical(),envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeScopeCanonical());fields=envelopeDrop(fields,envelopeRepeat(envelopeTupleCanonical(),2,false));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetProtocolTupleAuthorization.MAX_BYTES-100L);
    }
    private static EnvelopeMaterial envelopeActivationAuthorizationDigest(){
        return envelopeHash(envelopeActivationAuthorizationFields(),"nereus-delay-target-migration-root-tuple-authorization-v1\0".length());
    }
    private static EnvelopeMaterial envelopeActivationAuthorizationCanonical(){
        final var seal=envelopeWritten(envelopeActivationAuthorizationDigest(),102);
        return envelopeCall(envelopeHold(envelopeActivationAuthorizationFields(),seal),
                envelopeArray(com.nereusstream.delay.protocol.TargetProtocolTupleAuthorization.MAX_BYTES),0);
    }
    private static EnvelopeMaterial envelopeActivationAuthorizationDecode(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolTupleAuthorization.MAX_BYTES;
        var args=envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(26,envelopeTupleDecode()),2,true));
        args=envelopeHold(args,envelopeArrays(208,6));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),4,true));
        constructor=envelopeHold(constructor,envelopeArray(64));
        final long borrowed=args.held-208;
        var frame=envelopeHold(envelopeProtoRead(n,n,n,14),envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeActivationAuthorizationDigest(),envelopeArray(32)));
        frame=envelopeTemporary(frame,envelopeActivationAuthorizationCanonical());return envelopeReturn(frame,borrowed+208);
    }
    private static EnvelopeMaterial envelopeActivationPayloadCanonical(){
        var fields=envelopeDrop(envelopeScopeCanonical(),envelopeActivationDescriptorCanonical());
        fields=envelopeDrop(fields,envelopeActivationAuthorizationCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeActivationPayloadDecode(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES;
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());args=envelopeHold(args,envelopeArrays(48,2));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES,
                envelopeActivationDescriptorDecode(1,com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetProtocolTupleAuthorization.MAX_BYTES,envelopeActivationAuthorizationDecode()));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(16),envelopeArray(16)));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeActivationDescriptorDigest(1,
                com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES),envelopeArray(32)));
        final long borrowed=args.held-48;
        var frame=envelopeHold(envelopeProtoRead(n,n,n,6),envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeActivationPayloadCanonical());return envelopeReturn(frame,borrowed+48);
    }
    private static EnvelopeMaterial envelopeActivationBodySemantic(){
        return envelopeHash(envelopeHold(envelopeArray(2),envelopeActivationPayloadCanonical()),"nereus-delay-target-protocol-activation-control\0".length());
    }
    private static EnvelopeMaterial envelopeActivationBodyCanonical(){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        fields=envelopeDrop(fields,envelopeActivationBodySemantic());
        fields=envelopeDrop(fields,envelopeWritten(envelopeActivationPayloadCanonical(),com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES+8L));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeActivationBodyValidation(){
        final long body=com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES,
                payload=com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES;
        var fields=envelopeProtoRead(body,body,body,9);
        fields=envelopeTemporary(fields,envelopeNested(74,envelopeProtoRead(74,69,32,3)));
        fields=envelopeTemporary(fields,envelopeArray(32));
        fields=envelopeTemporary(fields,envelopeNested(payload+8,envelopeProtoRead(payload+8,payload,payload,1)));
        return envelopeReturn(fields,0);
    }
    private static EnvelopeMaterial envelopeActivationBodyDecode(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES,
                payload=com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES;
        var frame=envelopeProtoRead(n,n,n,9);
        frame=envelopeHold(frame,envelopeNested(payload+8,envelopeProtoRead(payload+8,payload,payload,1)));
        var args=envelopeNested(24,envelopeShardSubjectDecode());args=envelopeHold(args,envelopeNested(74,envelopeControlRefDecode()));
        args=envelopeHold(args,envelopeNested(payload,envelopeActivationPayloadDecode()));
        var constructor=envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(envelopeActivationPayloadCanonical(),payload,28));
        frame=envelopeHold(frame,envelopeCall(args,constructor,args.held));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeActivationBodySemantic()));
        frame=envelopeTemporary(frame,envelopeActivationBodyCanonical());return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopeActivationBodyIdentity(){
        return envelopeCall(envelopeActivationBodyDecode(),envelopeControlLogicalIdentity(),0);
    }
    private static EnvelopeMaterial envelopeActivationBodyProbe(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES,payload=com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES;
        var fields=envelopeReturn(envelopeActivationBodyValidation(),0);
        fields=envelopeHold(fields,envelopeProtoRead(n,n,n,9)); // SystemMutationBodyCodec.fields reread
        fields=envelopeHold(fields,envelopeNested(payload+8,envelopeProtoRead(payload+8,payload,payload,1)));
        fields=envelopeTemporary(fields,envelopeNested(payload,envelopeProtoRead(payload,1,1,1))); // only first discriminator is read
        return envelopeReturn(fields,0);
    }
    private static EnvelopeMaterial envelopeActivationInputDecode(){
        final long marker=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_MARKER_BYTES,
                body=com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES;
        return envelopeSystemDecode(marker,body,104,envelopeActivationBodyValidation(),
                envelopeDrop(envelopeActivationBodyProbe(),envelopeNested(body,envelopeActivationBodyIdentity())),true);
    }
    private static EnvelopeMaterial envelopeActivationPreparedRequestCanonical(){
        return envelopeWritten(envelopeActivationPayloadCanonical(),com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES+8L);
    }
    private static EnvelopeMaterial envelopeActivationPreparedFieldWork(){
        var fields=envelopeDrop(envelopeArray(102),envelopeActivationPreparedRequestCanonical());
        return envelopeDrop(fields,envelopeMigrationPreparedTargetCanonical());
    }
    private static EnvelopeMaterial envelopeActivationPreparedFields(){
        return envelopeWritten(envelopeActivationPreparedFieldWork(),com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_PREPARED_BYTES-106L);
    }
    private static EnvelopeMaterial envelopeActivationPreparedCanonical(){
        return envelopeWritten(envelopeActivationPreparedFieldWork(),com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_PREPARED_BYTES);
    }
    private static EnvelopeMaterial envelopeActivationPreparedDecode(){
        final long payload=com.nereusstream.delay.protocol.TargetProtocolActivatePayload.MAX_BYTES,request=payload+8,
                n=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_PREPARED_BYTES;
        var wrapped=envelopeHold(envelopeProtoRead(request,payload,payload,1),envelopeNested(payload,envelopeActivationPayloadDecode()));
        wrapped=envelopeTemporary(wrapped,envelopeActivationPreparedRequestCanonical());
        wrapped=envelopeReturn(wrapped,wrapped.held-payload);
        var args=envelopeNested(102,envelopeMigrationControlAuthorDecode());args=envelopeHold(args,envelopeNested(request,wrapped));
        args=envelopeHold(args,envelopeNested(135,envelopeMigrationPreparedTargetDecode()));args=envelopeHold(args,envelopeArrays(192,5));
        var constructor=envelopeArrays(192,5);
        constructor=envelopeTemporary(constructor,envelopePreparedRequestHash(envelopeActivationPayloadCanonical(),payload,28));
        constructor=envelopeTemporary(constructor,envelopeArrays(64,2)); // validateTargetPresence getters
        var targets=envelopeWritten(envelopeMigrationPreparedTargetCanonical(),138);
        targets=envelopeCall(targets,envelopeCall(envelopeArray(4),envelopeArray(142),0),0);
        constructor=envelopeTemporary(constructor,envelopeHash(targets,"nereus-delay-control-target-snapshot\0".length()));
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeActivationPreparedFields(),"nereus-delay-prepared-control\0".length()));
        final long borrowed=args.held-192;
        var frame=envelopeHold(envelopeProtoRead(n,n,n,13),envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeActivationPreparedCanonical());return envelopeReturn(frame,borrowed+192);
    }
    private static EnvelopeMaterial envelopeActivationRolesCanonical(){return envelopeArray(10);}
    private static EnvelopeMaterial envelopeActivationPublicCanonical(){
        var fields=envelopeDrop(envelopeArray(32),envelopeActivationRolesCanonical());fields=envelopeDrop(fields,envelopeArray(32));
        fields=envelopeDrop(fields,envelopeActivationResetCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetProtocolActivationRecord.PublicEvidence.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeActivationPublicDecode(){
        final long n=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.PublicEvidence.MAX_BYTES;
        var roles=envelopeNested(128,envelopeProtoRead(128,5,1,5));
        roles=envelopeHold(roles,envelopeHash(envelopeActivationRolesCanonical(),"nereus-delay-control-role-set\0".length()));
        roles=envelopeReturn(roles,32);
        var actor=envelopeHold(envelopeArrays(64,2),roles);actor=envelopeCall(actor,envelopeArrays(64,2),32);
        var args=envelopeHold(actor,envelopeArrays(132,3));args=envelopeHold(args,envelopeNested(1L<<20,envelopeActivationResetDecode()));
        var constructor=envelopeArrays(132,3);constructor=envelopeTemporary(constructor,envelopeActivationResetVerify());
        final long borrowed=args.held-132;
        var frame=envelopeHold(envelopeProtoRead(n,n,n,8),envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeActivationPublicCanonical());return envelopeReturn(frame,borrowed+132);
    }
    private static EnvelopeMaterial envelopeActivationRootEvidenceCanonical(boolean migrated){
        final var origin=migrated?envelopeMigrationRootCanonical(true,true):envelopeInitialCanonical();
        return envelopeWritten(origin,com.nereusstream.delay.protocol.TargetProtocolRootIssuerEvidence.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeActivationRootEvidenceCanonical(){
        return envelopeChoice(envelopeActivationRootEvidenceCanonical(false),envelopeActivationRootEvidenceCanonical(true));
    }
    private static EnvelopeMaterial envelopeActivationRootEvidenceDecode(boolean migrated){
        final long n=com.nereusstream.delay.protocol.TargetProtocolRootIssuerEvidence.MAX_BYTES;
        var args=migrated?envelopeNested(com.nereusstream.delay.protocol.TargetMigrationRoot.MAX_CANONICAL_BYTES,envelopeMigrationRootDecode(true,true)):
                envelopeNested(TargetInitialControlSnapshot.MAX_CANONICAL_BYTES,envelopeInitialDecode());
        args=envelopeHold(args,envelopeArray(44));if(migrated)args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeArray(44);
        if(migrated)constructor=envelopeHold(constructor,envelopeAssigned(32));
        else constructor=envelopeTemporary(constructor,envelopeArray("nereus-delay-target-control-snapshot-signature\0".length()+32));
        final long borrowed=args.held-(migrated?76:44);
        var frame=envelopeHold(envelopeProtoRead(n,n,n,migrated?4:3),envelopeCall(args,constructor,borrowed));
        frame=envelopeTemporary(frame,envelopeActivationRootEvidenceCanonical(migrated));return envelopeReturn(frame,borrowed+(migrated?76:44));
    }
    private static EnvelopeMaterial envelopeActivationRootEvidenceDecode(){
        return envelopeChoice(envelopeActivationRootEvidenceDecode(false),envelopeActivationRootEvidenceDecode(true));
    }
    private static EnvelopeMaterial envelopeActivationAuthorizationVerify(){
        return envelopeReturn(envelopeCall(envelopeActivationAuthorizationDigest(),
                envelopeArray("nereus-delay-target-root-tuple-authorization-signature-v1\0".length()+32),0),0);
    }
    private static EnvelopeMaterial envelopeActivationRootRequire(boolean migrated){
        var work=envelopeReturn(envelopeActivationDescriptorDigest(1,com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor.MAX_BYTES),0);
        if(migrated){
            work=envelopeTemporary(work,envelopeMigrationRootDigest(true,true));
            work=envelopeTemporary(work,envelopeHash(envelopeMigrationPlanCanonical(),"nereus-delay-target-migration-plan-v1\0".length()));
            work=envelopeTemporary(work,envelopeArrays(16+32+32+16+32+32,6)); // origin fields and requireExtension accessors
            final var policy=envelopeHash(envelopeMigrationIssuerFields(),"nereus-delay-migration-root-issuer-authorization-v1\0".length());
            work=envelopeTemporary(work,envelopeCall(policy,envelopeArray("nereus-delay-migration-root-issuer-authorization-signature-v1\0".length()+32),0));
        }else work=envelopeTemporary(work,envelopeArrays(16+32+32+32,4)); // lineage and three stored initial digests
        work=envelopeTemporary(work,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        work=envelopeTemporary(work,envelopeActivationAuthorizationVerify());
        if(migrated){
            // requireMigrationRootDataReaders scans <=64 declarations and <=11 descriptor candidates;
            // equality of bases/tuples/commits is allocation-free; the four SDK hash accessors copy32 each.
            work=envelopeTemporary(work,envelopeRepeat(envelopeArrays(128,4),64L*11,false));
        }
        return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeActivationRootRequire(){
        return envelopeChoice(envelopeActivationRootRequire(false),envelopeActivationRootRequire(true));
    }
    private static EnvelopeMaterial envelopeActivationSignatureVerify(){
        var fields=envelopeArray("nereus-delay-system-mutation-signature\0".length());
        fields=envelopeHold(fields,envelopeArrays(20,5));fields=envelopeHold(fields,envelopeArrays(2,2));fields=envelopeHold(fields,envelopeArray(2));
        final var fixedLp=envelopeCall(envelopeArray(32),envelopeCall(envelopeArray(4),envelopeArray(36),0),0);
        fields=envelopeHold(fields,fixedLp);fields=envelopeHold(fields,envelopeShardCanonical());fields=envelopeHold(fields,envelopeArray(8));
        fields=envelopeHold(fields,envelopeCall(envelopeArray(4),envelopeArray(com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES+4L),0));
        fields=envelopeHold(fields,fixedLp);fields=envelopeHold(fields,envelopeCall(envelopeArray(4),envelopeArray(108),0));
        return envelopeReturn(envelopeHash(fields,0),0);
    }
    private static EnvelopeMaterial envelopeActivationBinding(){
        var work=envelopeArrays(64,2);work=envelopeTemporary(work,envelopeArrays(64,2)); // expected and mutation IDs/hashes
        work=envelopeHold(work,envelopeCall(envelopeArrays(64,2),envelopeArrays(64,2),0));
        work=envelopeTemporary(work,envelopeNested(com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES,envelopeActivationBodyDecode()));
        work=envelopeTemporary(work,envelopeComparison(envelopeActivationPayloadCanonical(),envelopeActivationPayloadCanonical()));
        work=envelopeTemporary(work,envelopeComparison(envelopeControlLogicalIdentity(),envelopeArray(32)));return envelopeReturn(work,0);
    }
    private static EnvelopeMaterial envelopeActivationFields(){
        var fields=envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_MARKER_BYTES);
        fields=envelopeDrop(fields,envelopeActivationPreparedCanonical());fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeActivationReadersCanonical());fields=envelopeDrop(fields,envelopeActivationPublicCanonical());
        fields=envelopeDrop(fields,envelopeActivationRootEvidenceCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeActivationDigest(){
        return envelopeHash(envelopeActivationFields(),"nereus-delay-target-protocol-activation-record\0".length());
    }
    private static EnvelopeMaterial envelopeActivationCanonical(){
        return envelopeConcat(envelopeHold(envelopeActivationFields(),envelopeWritten(envelopeActivationDigest(),34)));
    }
    private static EnvelopeMaterial envelopeActivationDecode(){
        final long marker=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_MARKER_BYTES,
                prepared=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_PREPARED_BYTES,
                body=com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES;
        var frame=envelopeProtoRead(com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_BYTES,
                com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_BYTES,
                com.nereusstream.delay.protocol.TargetProtocolActivationRecord.MAX_BYTES,8);
        var args=envelopeHold(envelopeArray(marker),envelopeArray(prepared));
        args=envelopeHold(args,envelopeActivationInputDecode());
        final var decodedBody=envelopeNested(body,envelopeActivationBodyDecode());
        args=envelopeHold(args,decodedBody); // its descriptor is retained by the decoded reader set
        final long transientInputs=marker+prepared+decodedBody.held-224;
        args=envelopeHold(args,envelopeActivationPreparedDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_BYTES,envelopeActivationReadersDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetProtocolActivationRecord.PublicEvidence.MAX_BYTES,envelopeActivationPublicDecode()));
        args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetProtocolRootIssuerEvidence.MAX_BYTES,envelopeActivationRootEvidenceDecode()));
        var constructor=envelopeNested(body,envelopeActivationBodyDecode());
        constructor=envelopeHold(constructor,envelopeNested(104,envelopeAuthorDecode(104)));
        constructor=envelopeTemporary(constructor,envelopeSystemCanonical(marker));constructor=envelopeTemporary(constructor,envelopeActivationPreparedCanonical());
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeActivationPayloadCanonical(),envelopeActivationPayloadCanonical()));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopeControlLogicalIdentity()));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopeCall(envelopeSystemCanonical(marker),envelopeArray(32),0)));
        constructor=envelopeTemporary(constructor,envelopeArrays(384,12)); // six exact actor/role/scope comparisons
        constructor=envelopeTemporary(constructor,envelopeActivationSignatureVerify());
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeActivationDescriptorCanonical(),envelopeActivationDescriptorCanonical()));
        constructor=envelopeTemporary(constructor,envelopeComparison(envelopeActivationReadersDigest(),envelopeArray(32)));
        constructor=envelopeTemporary(constructor,envelopeArrays(128,4)); // reset manifest and tenant binding accessors
        final var legacyBounds=envelopeDrop(envelopeSystemCanonical(marker),envelopeActivationPreparedCanonical());
        constructor=envelopeTemporary(constructor,envelopeChoice(envelopeActivationRootRequire(),legacyBounds));
        constructor=envelopeTemporary(constructor,envelopeActivationBinding());
        // The explicit markerBytes/preparedBytes/body locals remain available through both final rechecks.
        frame=envelopeHold(frame,envelopeCall(args,envelopeReturn(constructor,0),args.held));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeActivationDigest(),envelopeArray(32)));
        frame=envelopeTemporary(frame,envelopeActivationCanonical());return envelopeReturn(frame,args.held-transientInputs);
    }

    // LEGACY_ADMISSION_MATERIAL_REGION: legacy PublishAdmission codec graph insertion point.

    // These legacy values have no Target Profile/Broker/time text cap. Each caller supplies
    // its actual enclosing field inventory; independent fields never acquire a Target MAX.
    private static EnvelopeMaterial envelopeLegacyProfileCanonical(long encodedBytes){return envelopeArray(encodedBytes);}
    private static EnvelopeMaterial envelopeLegacyProfileDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,4);
        final var args=envelopeArrays(encodedBytes,2); // profileId and semanticHash rawValue
        var frame=envelopeHold(parsed,envelopeCall(args,envelopeArrays(encodedBytes,2),0));
        frame=envelopeTemporary(frame,envelopeLegacyProfileCanonical(encodedBytes));
        return envelopeReturn(frame,encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyBrokerCanonical(long encodedBytes,boolean pulsar){
        var fields=envelopeArrays(encodedBytes,pulsar?2:1); // UTF-8 cluster/topic encodings share the inventory
        if(!pulsar)fields=envelopeDrop(fields,envelopeArray(16)); // UUID ByteBuffer
        return envelopeWritten(envelopeWritten(fields,encodedBytes),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyBrokerCanonical(long encodedBytes){
        return envelopeChoice(envelopeLegacyBrokerCanonical(encodedBytes,false),envelopeLegacyBrokerCanonical(encodedBytes,true));
    }
    private static EnvelopeMaterial envelopeLegacyBrokerDecode(long encodedBytes,boolean pulsar){
        // BrokerResourceIdentity's branch probe and the branch decoder's own outer Reader are distinct.
        var frame=envelopeHold(envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,1),
                envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,1));
        var branch=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,pulsar?4:2);
        var args=envelopeArrays(encodedBytes,pulsar?2:1); // Query.bytes on cluster/topic
        args=envelopeTemporary(args,envelopeArrays(encodedBytes,pulsar?2:1)); // utf8 round trips
        args=envelopeHold(args,envelopeArray(pulsar?32:16));
        var constructor=pulsar?envelopeArray(32):envelopeEmpty();
        constructor=envelopeTemporary(constructor,envelopeArrays(encodedBytes,pulsar?2:1)); // nfc UTF-8 validation
        branch=envelopeHold(branch,envelopeCall(args,constructor,0));
        branch=envelopeTemporary(branch,envelopeLegacyBrokerCanonical(encodedBytes,pulsar));
        frame=envelopeHold(frame,envelopeNested(encodedBytes,branch));
        return envelopeReturn(frame,pulsar?32:0);
    }
    private static EnvelopeMaterial envelopeLegacyBrokerDecode(long encodedBytes){
        return envelopeChoice(envelopeLegacyBrokerDecode(encodedBytes,false),envelopeLegacyBrokerDecode(encodedBytes,true));
    }
    private static EnvelopeMaterial envelopeLegacyTimeCanonical(long encodedBytes){return envelopeArray(encodedBytes);}
    private static EnvelopeMaterial envelopeLegacyTimeDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,10);
        var args=envelopeArray(encodedBytes); // uncapped sourceId rawValue
        args=envelopeHold(args,envelopeCall(envelopeArray(32),envelopeArray(32),0));
        args=envelopeHold(args,envelopeCall(envelopeArray(64),envelopeArray(64),0));
        final var constructor=envelopeHold(envelopeArray(encodedBytes),envelopeArrays(96,2));
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,0));
        frame=envelopeTemporary(frame,envelopeLegacyTimeCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(encodedBytes,96));
    }
    private static EnvelopeMaterial envelopeLegacyLeaseFields(long encodedBytes){
        return envelopeWritten(envelopeDrop(envelopeLegacyProfileCanonical(encodedBytes),
                envelopeLegacyTimeCanonical(encodedBytes)),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyLeaseDigest(long encodedBytes){
        return envelopeHash(envelopeLegacyLeaseFields(encodedBytes),"nereus-delay-credential-use-lease\0".length());
    }
    private static EnvelopeMaterial envelopeLegacyLeaseCanonical(long encodedBytes){
        var fields=envelopeDrop(envelopeLegacyProfileCanonical(encodedBytes),envelopeLegacyTimeCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyLeaseDigest(encodedBytes));
        return envelopeWritten(fields,encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyLeaseDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,11);
        var args=envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes));
        args=envelopeHold(args,envelopeArrays(96,3));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyTimeDecode(encodedBytes)));
        final long nested=args.held-96;
        var frame=envelopeHold(parsed,envelopeCall(args,envelopeArrays(96,3),nested));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeLegacyLeaseDigest(encodedBytes)));
        frame=envelopeTemporary(frame,envelopeLegacyLeaseCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(nested,96));
    }
    private static EnvelopeMaterial envelopeLegacyCommittedPayloadCanonical(long encodedBytes){
        return envelopeWritten(envelopeLegacyProfileCanonical(encodedBytes),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyCommittedPayloadDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,9);
        var args=envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes));
        final long profile=args.held;
        // container/key/version/optional etag share one complete descriptor inventory.
        args=envelopeHold(args,envelopeArrays(encodedBytes,4));args=envelopeHold(args,envelopeArrays(96,3));
        final var constructor=envelopeHold(envelopeArrays(encodedBytes,4),envelopeArrays(96,3));
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,profile));
        frame=envelopeTemporary(frame,envelopeLegacyCommittedPayloadCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(profile,Math.addExact(encodedBytes,96)));
    }
    private static EnvelopeMaterial envelopeLegacyPayloadCanonical(long encodedBytes,boolean object){
        return envelopeWritten(object?envelopeLegacyCommittedPayloadCanonical(encodedBytes):envelopeEmpty(),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyPayloadCanonical(long encodedBytes){
        return envelopeChoice(envelopeLegacyPayloadCanonical(encodedBytes,false),envelopeLegacyPayloadCanonical(encodedBytes,true));
    }
    private static EnvelopeMaterial envelopeLegacyPayloadDecode(long encodedBytes,boolean object){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,3);
        var args=envelopeArray(32);
        var constructor=envelopeArray(32);
        long nested=0;
        if(object){
            final var descriptor=envelopeNested(encodedBytes,envelopeLegacyCommittedPayloadDecode(encodedBytes));
            args=envelopeHold(args,descriptor);nested=descriptor.held;
            constructor=envelopeTemporary(constructor,envelopeArray(32)); // descriptor.payloadSha256()
        }else{
            args=envelopeHold(args,envelopeArray(encodedBytes));
            constructor=envelopeHold(constructor,envelopeArray(encodedBytes));
            // Bytes.sha256(inlinePayload) copies the existing argument into concat, then returns its digest.
            constructor=envelopeTemporary(constructor,new EnvelopeMaterial(Math.addExact(encodedBytes,32),
                    Math.addExact(encodedBytes,32),32,0,2));
        }
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeLegacyPayloadCanonical(encodedBytes,object));
        return envelopeReturn(frame,Math.addExact(32,object?nested:encodedBytes));
    }
    private static EnvelopeMaterial envelopeLegacyPayloadDecode(long encodedBytes){
        return envelopeChoice(envelopeLegacyPayloadDecode(encodedBytes,false),envelopeLegacyPayloadDecode(encodedBytes,true));
    }
    /** All repeated property/Header images share totalBytes; each nested Reader is still counted. */
    private static EnvelopeMaterial envelopeLegacyPropertiesDecode(long totalBytes,long count){
        if(count==0)return envelopeEmpty();
        final var read=envelopeProtoRead(totalBytes,totalBytes,totalBytes,Math.multiplyExact(2,count));
        var frame=new EnvelopeMaterial(read.allocated,read.retained,read.held,read.fieldVisits,
                Math.addExact(read.arrayObjects,count-1));
        var args=envelopeArrays(totalBytes,Math.multiplyExact(2,count));
        var constructor=envelopeArrays(totalBytes,Math.multiplyExact(2,count));
        constructor=envelopeTemporary(constructor,envelopeArrays(totalBytes,Math.multiplyExact(2,count))); // UTF-8 checks
        frame=envelopeHold(frame,envelopeCall(args,constructor,0));
        frame=envelopeTemporary(frame,envelopeArrays(totalBytes,count)); // each entry canonical recheck
        return envelopeReturn(frame,totalBytes);
    }
    private static EnvelopeMaterial envelopeLegacyMetadataCanonical(long encodedBytes){
        final long entries=encodedBytes/2;
        return envelopeWritten(envelopeWritten(envelopeArrays(encodedBytes,entries),encodedBytes),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyMetadataDecode(long encodedBytes){
        final long entries=encodedBytes/2;
        final var outer=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,1);
        var branch=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,Math.addExact(entries,3));
        var args=envelopeArrays(encodedBytes,2); // optional partition/key and ordering-key raw accessors
        args=envelopeHold(args,envelopeCall(envelopeArrays(encodedBytes,entries),
                envelopeLegacyPropertiesDecode(encodedBytes,entries),0));
        final long properties=args.held-encodedBytes;
        var constructor=envelopeArrays(encodedBytes,2);
        constructor=envelopeTemporary(constructor,envelopeArrays(Math.multiplyExact(2,encodedBytes),
                Math.multiplyExact(2,Math.max(0,entries-1)))); // adjacent Pulsar property key getters
        branch=envelopeHold(branch,envelopeCall(args,constructor,properties));
        branch=envelopeTemporary(branch,envelopeWritten(envelopeArrays(encodedBytes,entries),encodedBytes));
        var frame=envelopeHold(outer,envelopeNested(encodedBytes,branch));
        frame=envelopeTemporary(frame,envelopeLegacyMetadataCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(encodedBytes,properties));
    }
    private static EnvelopeMaterial envelopeLegacyPolicyHeadCanonical(long encodedBytes){return envelopeArray(encodedBytes);}
    private static EnvelopeMaterial envelopeLegacyPolicyHeadDecode(long encodedBytes){
        var frame=envelopeHold(envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,4),
                envelopeCall(envelopeArrays(64,2),envelopeArrays(64,2),0));
        frame=envelopeTemporary(frame,envelopeLegacyPolicyHeadCanonical(encodedBytes));return envelopeReturn(frame,64);
    }
    private static EnvelopeMaterial envelopeLegacyClaimMaterializationCanonical(long encodedBytes){
        var fields=envelopeDrop(envelopeLegacyProfileCanonical(encodedBytes),envelopeLegacyProfileCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyBrokerCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeArray(DelayMessageId.LENGTH));
        fields=envelopeDrop(fields,envelopeLegacyPayloadCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyMetadataCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyPolicyHeadCanonical(encodedBytes));
        return envelopeWritten(fields,encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyClaimMaterializationDigest(long encodedBytes){
        return envelopeHash(envelopeHold(envelopeArray("nereus-delay-claim-materialization\0".length()),
                envelopeLegacyClaimMaterializationCanonical(encodedBytes)),0);
    }
    private static EnvelopeMaterial envelopeLegacyClaimMaterializationDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,14);
        var args=envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes)));
        args=envelopeHold(args,envelopeCall(envelopeArray(DelayMessageId.LENGTH),envelopeLegacyMessageIdDecode(),0));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyPayloadDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyMetadataDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyPolicyHeadDecode(encodedBytes)));
        var frame=envelopeHold(parsed,args); // constructor retains the typed projections, with no extra byte-array copy
        frame=envelopeTemporary(frame,envelopeLegacyClaimMaterializationCanonical(encodedBytes));
        return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopeLegacyHandoffSnapshotFields(long encodedBytes){
        return envelopeWritten(envelopeLegacyTimeCanonical(encodedBytes),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyHandoffSnapshotCanonical(long encodedBytes){
        return envelopeWritten(envelopeLegacyTimeCanonical(encodedBytes),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyHandoffSnapshotDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,13);
        var args=envelopeArrays(160,4); // scope, artifact digest, snapshot digest and signature
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyTimeDecode(encodedBytes)));
        var frame=envelopeHold(parsed,args);
        frame=envelopeTemporary(frame,envelopeHash(envelopeHold(envelopeArray("nereus-delay-handoff-policy-snapshot\0".length()),
                envelopeLegacyHandoffSnapshotFields(encodedBytes)),0));
        frame=envelopeHold(frame,envelopeArrays(160,4));
        frame=envelopeTemporary(frame,envelopeLegacyHandoffSnapshotCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(args.held-160,160));
    }
    private static EnvelopeMaterial envelopeLegacyPulsarKeyCanonical(long encodedBytes){return envelopeArray(encodedBytes);}
    private static EnvelopeMaterial envelopeLegacyPulsarKeyDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,2);
        var constructor=envelopeArray(encodedBytes);
        constructor=envelopeTemporary(constructor,envelopeArray(encodedBytes)); // UTF-8 branch roundtrip
        var frame=envelopeHold(parsed,envelopeCall(envelopeArray(encodedBytes),constructor,0));
        frame=envelopeTemporary(frame,envelopeLegacyPulsarKeyCanonical(encodedBytes));
        return envelopeReturn(frame,encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyPulsarTemplateCanonical(long encodedBytes){
        var fields=envelopeDrop(envelopeLegacyBrokerCanonical(encodedBytes),envelopeLegacyPulsarKeyCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeArrays(encodedBytes,encodedBytes/2)); // caller property canonical images
        fields=envelopeDrop(fields,envelopeReservedMetadataCanonical());
        fields=envelopeDrop(fields,envelopeLegacyPayloadCanonical(encodedBytes));
        return envelopeWritten(fields,encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyPulsarTemplateHash(long encodedBytes){
        return envelopeHash(envelopeHold(envelopeArray("nereus-delay-pulsar-record-template\0".length()),
                envelopeLegacyPulsarTemplateCanonical(encodedBytes)),0);
    }
    private static EnvelopeMaterial envelopeLegacyPulsarTemplateDecode(long encodedBytes){
        final long properties=encodedBytes/2;
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,Math.addExact(properties,12));
        var args=envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyPulsarKeyDecode(encodedBytes)));
        args=envelopeHold(args,envelopeArray(encodedBytes)); // optional orderingKey
        args=envelopeHold(args,envelopeCall(envelopeArrays(encodedBytes,properties),
                envelopeLegacyPropertiesDecode(encodedBytes,properties),0));
        args=envelopeHold(args,envelopeNested(envelopeReservedMetadataBytes(),envelopeReservedMetadataDecode()));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyPayloadDecode(encodedBytes)));
        args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeHold(envelopeArray(encodedBytes),envelopeArray(32));
        constructor=envelopeTemporary(constructor,envelopeArrays(Math.multiplyExact(2,encodedBytes),
                Math.multiplyExact(2,Math.max(0,properties-1)))); // sortedProperties key getter pairs
        final long retained=args.held-encodedBytes-32;
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,retained));
        frame=envelopeTemporary(frame,envelopeLegacyPulsarTemplateCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(retained,Math.addExact(encodedBytes,32)));
    }
    private static EnvelopeMaterial envelopeLegacyPreparedCanonical(long encodedBytes){
        var fields=envelopeArray(32); // DestinationLaneId.bytes()
        fields=envelopeDrop(fields,envelopeLegacyProfileCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyProfileCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyBrokerCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeEvidenceChannelCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeArray(DelayMessageId.LENGTH));
        fields=envelopeDrop(fields,envelopeLegacyPayloadCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyMetadataCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeReservedMetadataCanonical());
        fields=envelopeDrop(fields,envelopeLegacyHandoffSnapshotCanonical(encodedBytes));
        fields=envelopeDrop(fields,envelopeLegacyPulsarTemplateCanonical(encodedBytes));
        return envelopeWritten(fields,encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyPreparedHash(long encodedBytes){
        return envelopeHash(envelopeHold(envelopeArray("nereus-delay-prepared-publish\0".length()),
                envelopeLegacyPreparedCanonical(encodedBytes)),0);
    }
    private static EnvelopeMaterial envelopeLegacyPreparedIdentityCheck(){
        var work=envelopeArrays(64,2); // channel lane getter and DestinationLaneId.bytes()
        work=envelopeDrop(work,envelopeArray(16));
        work=envelopeDrop(work,envelopeArray(32)); // reserved attempt getter
        work=envelopeDrop(work,envelopeArrays(64,2)); // destination Profile semantic hash equality
        work=envelopeDrop(work,envelopeArrays(64,2)); // capability Profile semantic hash equality
        return envelopeReturn(envelopeDrop(work,envelopeLegacySelfRoutingDecode()),0);
    }
    private static EnvelopeMaterial envelopeLegacyPreparedDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,27);
        var args=envelopeCall(envelopeArray(32),envelopeArray(32),0); // DestinationLaneId
        args=envelopeHold(args,envelopeArray(16));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeEvidenceChannelDecode(encodedBytes)));
        args=envelopeHold(args,envelopeCall(envelopeArray(DelayMessageId.LENGTH),envelopeLegacyMessageIdDecode(),0));
        args=envelopeHold(args,envelopeArray(32));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyPayloadDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyMetadataDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(envelopeReservedMetadataBytes(),envelopeReservedMetadataDecode()));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyHandoffSnapshotDecode(encodedBytes)));
        args=envelopeHold(args,envelopeNested(encodedBytes,envelopeLegacyPulsarTemplateDecode(encodedBytes)));
        args=envelopeHold(args,envelopeArrays(64,2)); // optional template hash and v2 artifact digest
        var constructor=envelopeHold(envelopeArray(16),envelopeArrays(96,3));
        constructor=envelopeTemporary(constructor,envelopeLegacyPulsarTemplateHash(encodedBytes));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(32),2,false)); // template/snapshot artifact getters
        constructor=envelopeTemporary(constructor,envelopeLegacyPreparedIdentityCheck());
        // The v1 public constructor computes its legacy artifact digest before copying it.
        var legacy=envelopeHold(envelopeArray(16),envelopeArrays(64,2));
        legacy=envelopeTemporary(legacy,envelopeHash(envelopeArray("nereus-delay-legacy-artifact-generation-set\0".length()),0));
        legacy=envelopeTemporary(legacy,envelopeLegacyPreparedIdentityCheck());
        constructor=envelopeChoice(constructor,legacy);
        final long retained=args.held-16-32-64;
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,retained));
        frame=envelopeTemporary(frame,envelopeLegacyPreparedCanonical(encodedBytes));
        return envelopeReturn(frame,Math.addExact(retained,constructor.held));
    }
    private static EnvelopeMaterial envelopeLegacyActivationBarrierCanonical(long encodedBytes){
        return envelopeWritten(envelopeWritten(envelopeLegacyBrokerCanonical(encodedBytes),encodedBytes),encodedBytes);
    }
    private static EnvelopeMaterial envelopeLegacyActivationBarrierDecode(long encodedBytes){
        final var outer=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,1);
        final var branch=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,8);
        var args=envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes));
        args=envelopeHold(args,envelopeArray(32)); // optional/Pulsar guard digest rawValue
        final var constructor=envelopeArray(32);
        final var decoded=envelopeHold(branch,envelopeCall(args,constructor,args.held-32));
        var frame=envelopeHold(outer,envelopeNested(encodedBytes,decoded));
        frame=envelopeTemporary(frame,envelopeLegacyActivationBarrierCanonical(encodedBytes));
        return envelopeReturn(frame,64);
    }
    /** PublishAdmission's Channel DTO copies the decoded identity's getter/canonical projections. */
    private static EnvelopeMaterial envelopeLegacyAdmissionChannelDecode(long encodedBytes){
        var args=envelopeArrays(112,4); // lane, incarnation, binding digest, credential fingerprint
        args=envelopeHold(args,envelopeLegacyBrokerCanonical(encodedBytes));
        args=envelopeHold(args,envelopeArray(encodedBytes)); // producer identity getter
        var constructor=envelopeArrays(encodedBytes,3); // canonical, target resource and producer identity copies
        constructor=envelopeHold(constructor,envelopeArrays(112,4));
        return envelopeCall(envelopeEvidenceChannelDecode(encodedBytes),envelopeCall(args,constructor,0),0);
    }
    private static EnvelopeMaterial envelopeLegacyAdmissionDescriptorDecode(long encodedBytes){
        var args=envelopeLegacyPreparedHash(encodedBytes);
        args=envelopeHold(args,envelopeArrays(121,4)); // lane, incarnation, message and attempt getters
        args=envelopeHold(args,envelopeEvidenceChannelCanonical(encodedBytes));
        args=envelopeHold(args,envelopeLegacyProfileCanonical(encodedBytes));
        args=envelopeHold(args,envelopeLegacyProfileCanonical(encodedBytes));
        args=envelopeHold(args,envelopeLegacyBrokerCanonical(encodedBytes));
        args=envelopeHold(args,envelopeLegacyPayloadCanonical(encodedBytes));
        args=envelopeHold(args,envelopeLegacyMetadataCanonical(encodedBytes));
        final var constructor=envelopeHold(envelopeArrays(Math.multiplyExact(7,encodedBytes),7),envelopeArrays(153,5));
        return envelopeCall(envelopeLegacyPreparedDecode(encodedBytes),envelopeCall(args,constructor,0),0);
    }
    private static EnvelopeMaterial envelopeLegacyReadyCertificateDigest(long encodedBytes,long fieldCount){
        // canonicalFields(fields,15) obtains each non-scalar rawValue before writing the fields image.
        final var fields=envelopeWritten(envelopeArrays(encodedBytes,fieldCount),encodedBytes);
        return envelopeHash(envelopeHold(envelopeArray("nereus-delay-ready-certificate\0".length()),fields),0);
    }
    private static EnvelopeMaterial envelopeLegacyReadyCertificateDecode(long encodedBytes){
        final long cursors=encodedBytes/2,fieldCount=Math.addExact(cursors,15);
        var frame=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,fieldCount);
        var owner=envelopeLegacyNestedField(encodedBytes);
        owner=envelopeTemporary(owner,envelopeOwnerIdentityDecode(encodedBytes));frame=envelopeHold(frame,owner);
        var channel=envelopeLegacyNestedField(encodedBytes);
        channel=envelopeTemporary(channel,envelopeLegacyAdmissionChannelDecode(encodedBytes));frame=envelopeHold(frame,channel);
        frame=envelopeTemporary(frame,envelopeCall(envelopeLegacyNestedField(encodedBytes),
                envelopeLegacyActivationBarrierDecode(encodedBytes),0));
        // Admission.nested's accessors/Readers and every typed cursor share the certificate inventory.
        final var cursorRaw=envelopeCall(envelopeArrays(encodedBytes,cursors),
                envelopeReturn(envelopeLegacyReaders(encodedBytes,cursors),0),encodedBytes);
        frame=envelopeHold(frame,envelopeCall(cursorRaw,envelopeEvidenceCursorSequenceDecode(encodedBytes,cursors),0));
        frame=envelopeTemporary(frame,envelopeRepeat(envelopeComparison(envelopeCursorScopeCanonical(),
                envelopeCursorScopeCanonical()),Math.max(0,cursors-1),false)); // Target cursor compareTo's two scope images
        var issuedAt=envelopeCall(envelopeLegacyNestedField(encodedBytes),envelopeLegacyTimeDecode(encodedBytes),0);
        frame=envelopeHold(frame,issuedAt);
        frame=envelopeHold(frame,envelopeArray(32)); // received digest remains live across its independent calculation
        frame=envelopeTemporary(frame,envelopeLegacyReadyCertificateDigest(encodedBytes,fieldCount));
        final var args=envelopeArrays(128,5); // store/lane/incarnation/binding/fingerprint, digest already read above
        var constructor=envelopeArrays(Math.multiplyExact(3,encodedBytes),3); // canonical/owner/channel DTO copies
        constructor=envelopeHold(constructor,envelopeArrays(160,6));
        frame=envelopeHold(frame,envelopeCall(args,constructor,0));
        return envelopeReturn(frame,Math.addExact(issuedAt.held,constructor.held));
    }
    private static EnvelopeMaterial envelopeLegacyAdmissionClaimDecode(long encodedBytes){
        var frame=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,20);
        final var materializationRaw=envelopeLegacyNestedField(encodedBytes);
        frame=envelopeHold(frame,materializationRaw);
        frame=envelopeHold(frame,envelopeLegacyClaimMaterializationDecode(encodedBytes));
        frame=envelopeHold(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeLegacyClaimMaterializationDigest(encodedBytes));
        var owner=envelopeLegacyNestedField(encodedBytes);
        owner=envelopeTemporary(owner,envelopeOwnerIdentityDecode(encodedBytes));frame=envelopeHold(frame,owner);
        frame=envelopeTemporary(frame,envelopeCall(envelopeLegacyNestedField(encodedBytes),
                envelopeReturn(envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,17),0),0));
        final var args=envelopeArrays(137,5); // claim/message/lane/incarnation/store raw values
        final var constructor=envelopeHold(envelopeArrays(Math.multiplyExact(3,encodedBytes),3),envelopeArrays(137,5));
        frame=envelopeHold(frame,envelopeCall(args,constructor,0));
        return envelopeReturn(frame,constructor.held);
    }
    private static EnvelopeMaterial envelopeLegacyAdmissionCrossEqualities(long encodedBytes){
        // The Channel DTO's canonical accessor precedes a complete independent identity decode.
        var frame=envelopeNested(encodedBytes,envelopeEvidenceChannelDecode(encodedBytes));
        frame=envelopeTemporary(frame,envelopeArray(32)); // descriptor lane getter
        frame=envelopeTemporary(frame,envelopeArray(16));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(encodedBytes),envelopeArray(encodedBytes)));
        frame=envelopeTemporary(frame,envelopeArray(DelayMessageId.LENGTH));
        frame=envelopeTemporary(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(encodedBytes),envelopeLegacyProfileCanonical(encodedBytes)));
        frame=envelopeTemporary(frame,envelopeArray(encodedBytes)); // certificate owner getter
        frame=envelopeTemporary(frame,envelopeArrays(64,3)); // certificate store/lane/incarnation getters
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(encodedBytes),envelopeArray(encodedBytes)));
        frame=envelopeTemporary(frame,envelopeArray(32)); // certificate digest getter
        frame=envelopeTemporary(frame,envelopeArrays(137,5)); // Claim fixed identity getters
        frame=envelopeTemporary(frame,envelopeArray(encodedBytes)); // Claim owner getter
        final var materialization=envelopeLegacyClaimMaterializationDecode(encodedBytes); // materializationValue(), no raw getter
        frame=envelopeHold(frame,materialization);
        var rebuild=envelopeLegacyPreparedDecode(encodedBytes); // Descriptor.value() reparses its stored canonical image
        rebuild=envelopeTemporary(rebuild,envelopeArrays(128,4)); // optional snapshot/head scope and digest equalities
        rebuild=envelopeCall(rebuild,envelopeLegacyClaimMaterializationCanonical(encodedBytes),0);
        frame=envelopeTemporary(frame,envelopeComparison(rebuild,envelopeArray(encodedBytes))); // Claim.materialization() getter
        frame=envelopeTemporary(frame,envelopeArray(32)); // descriptor prepared hash getter
        frame=envelopeTemporary(frame,envelopeArrays(64,2)); // certificate/channel binding getters
        frame=envelopeTemporary(frame,envelopeArrays(64,2)); // certificate/channel fingerprint getters
        return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeLegacyPublishAdmissionValidation(long bodyBytes){
        return envelopeLegacyValidation(bodyBytes,15,7);
    }
    private static EnvelopeMaterial envelopeLegacyPublishAdmissionDecode(long bodyBytes){
        final var parsed=envelopeLegacyBodyFields(bodyBytes,envelopeLegacyPublishAdmissionValidation(bodyBytes));
        var owner=envelopeLegacyNestedField(bodyBytes);
        owner=envelopeTemporary(owner,envelopeOwnerIdentityDecode(bodyBytes));
        var args=owner;
        var reserve=envelopeLegacyNestedField(bodyBytes);
        reserve=envelopeTemporary(reserve,envelopeReturn(envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,17),0));
        args=envelopeHold(args,reserve);
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyAdmissionChannelDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyAdmissionDescriptorDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyReadyCertificateDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyTimeDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyAdmissionClaimDecode(bodyBytes),0));
        args=envelopeHold(args,envelopeArray(DelayMessageId.LENGTH));
        args=envelopeTemporary(args,envelopeLegacyMessageShardCheck());
        args=envelopeHold(args,envelopeArrays(192,7)); // store/claim/lane/incarnation/attempt/prepared hash/certificate digest
        final long retained=args.held-Math.multiplyExact(2,bodyBytes)-DelayMessageId.LENGTH-192;
        var constructor=envelopeHold(envelopeArrays(Math.multiplyExact(2,bodyBytes),2),envelopeArrays(233,8));
        constructor=envelopeTemporary(constructor,envelopeLegacyAdmissionCrossEqualities(bodyBytes));
        return envelopeReturn(envelopeHold(parsed,envelopeCall(args,constructor,retained)),Math.addExact(retained,constructor.held));
    }
    private static EnvelopeMaterial envelopeLegacyPublishAdmissionIdentity(long bodyBytes){
        return envelopeCall(envelopeLegacyPublishAdmissionDecode(bodyBytes),envelopeArray(32),0);
    }

    // COMMON_TYPED_MATERIAL_REGION: shared Command, message, admission and result codec graph insertion point.
    private static long envelopeCommonBytesField(int number,long length){
        return Math.addExact(length,Math.addExact(envelopeVarintBytes(((long)number<<3)|2),envelopeVarintBytes(length)));
    }
    private static long envelopeReceiptSourceBytes(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        final long branch=(pulsar?105:70)+envelopeCommonBytesField(pulsar?3:2,text);
        return envelopeCommonBytesField(pulsar?2:1,branch);
    }
    private static EnvelopeMaterial envelopeReceiptSourceCanonical(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        var fields=envelopeDrop(envelopeArray(16),envelopeArray(text));
        fields=envelopeDrop(fields,envelopeArray(pulsar?32:16));
        final long encoded=envelopeReceiptSourceBytes(pulsar),branch=encoded-1-envelopeVarintBytes(encoded);
        return envelopeWritten(envelopeWritten(fields,branch),encoded);
    }
    private static EnvelopeMaterial envelopeReceiptSourceCanonical(){
        return envelopeChoice(envelopeReceiptSourceCanonical(false),envelopeReceiptSourceCanonical(true));
    }
    private static EnvelopeMaterial envelopeReceiptSourceDecode(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        final long encoded=envelopeReceiptSourceBytes(pulsar),branch=encoded-1-envelopeVarintBytes(encoded);
        final var outer=envelopeProtoRead(encoded,branch,branch,1);
        final var inner=envelopeProtoRead(branch,text+(pulsar?48+50:32+35),text,pulsar?10:7);
        var arguments=envelopeHold(envelopeArray(16),envelopeArray(text));
        arguments=envelopeHold(arguments,envelopeArray(pulsar?32:16));
        var constructor=envelopeTemporary(envelopeArray(16),envelopeArray(text));
        if(pulsar)constructor=envelopeHold(constructor,envelopeArray(32));
        var work=envelopeHold(inner,envelopeCall(arguments,constructor,0));
        work=envelopeTemporary(work,envelopeArray(text)); // canonical UTF-8 roundtrip in decodeSourcePosition
        return envelopeReturn(envelopeHold(outer,envelopeNested(branch,work)),pulsar?48:16);
    }
    private static EnvelopeMaterial envelopeReceiptSourceDecode(){
        return envelopeChoice(envelopeReceiptSourceDecode(false),envelopeReceiptSourceDecode(true));
    }
    private static EnvelopeMaterial envelopeReservationClosureDigest(){
        var args=envelopeHold(envelopeArray(32),envelopeSourceCanonical());
        args=envelopeHold(args,envelopeArray(8));
        return envelopeHash(args,48+"nereus-delay-target-reservation-closure\0".length());
    }
    private static EnvelopeMaterial envelopeReservationClosureCanonical(){
        var fields=envelopeDrop(envelopeArray(32),envelopeSourceCanonical());
        fields=envelopeDrop(fields,envelopeReservationClosureDigest());
        return envelopeWritten(fields,TargetReservationControls.Closure.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeReservationClosureDecode(){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(TargetReservationControls.Closure.MAX_CANONICAL_BYTES,32+32+16+source+10+32,source,6);
        var arguments=envelopeHold(envelopeNested(32,envelopeArray(32)),envelopeArray(48));
        arguments=envelopeHold(arguments,envelopeNested(source,envelopeSourceDecode()));
        var constructor=envelopeHold(envelopeArray(32),envelopeArray(16));
        constructor=envelopeTemporary(constructor,envelopeSourceBounded());
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held-48));
        work=envelopeTemporary(work,envelopeReservationClosureCanonical());return envelopeReturn(work,arguments.held);
    }
    private static EnvelopeMaterial envelopeReceiptTrustCanonical(){return envelopeArray(45);}
    private static EnvelopeMaterial envelopeReceiptTrustDecode(){
        var work=envelopeHold(envelopeProtoRead(45,10+32,32,2),envelopeArray(32));
        work=envelopeTemporary(work,envelopeReceiptTrustCanonical());return envelopeReturn(work,32);
    }
    private static EnvelopeMaterial envelopeReservationReceiptFields(boolean digest){
        var fields=envelopeDrop(envelopeArray(41),envelopeArray(16));
        fields=envelopeDrop(fields,envelopeReceiptSourceCanonical());fields=envelopeDrop(fields,envelopeBusinessProfileCanonical());
        fields=envelopeDrop(fields,envelopeReceiptTrustCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationReservationOrigin.MAX_RECEIPT_BYTES-16L-(digest?0:34));
    }
    private static EnvelopeMaterial envelopeReservationReceiptCanonical(){
        final long payload=com.nereusstream.delay.protocol.TargetMigrationReservationOrigin.MAX_RECEIPT_BYTES-16L;
        final var prefix=envelopeCall(envelopeArray(12),envelopeArray(payload+12),0);
        final var framed=envelopeCall(envelopeHold(prefix,envelopeArray(4)),envelopeArray(payload+16),0);
        return envelopeCall(envelopeReservationReceiptFields(true),framed,0);
    }
    private static EnvelopeMaterial envelopeReservationReceiptDecode(){
        final long payload=com.nereusstream.delay.protocol.TargetMigrationReservationOrigin.MAX_RECEIPT_BYTES-16L;
        final long query=Math.max(envelopeReceiptSourceBytes(false),envelopeReceiptSourceBytes(true));
        final var frame=envelopeCall(envelopeArray(payload),envelopeArray(payload),0);
        final var parsed=envelopeProtoRead(payload,1+32+41+16+5+query+10+TargetChannelIdentity.MAX_PROFILE_REF_BYTES+1024+4096+10+32+10+45+32,
                Math.max(query,4096),15);
        var arguments=envelopeHold(envelopeArray(32),envelopeNested(41,envelopeManagedIdDecode()));
        arguments=envelopeHold(arguments,envelopeNested(16,envelopeArray(16)));
        arguments=envelopeHold(arguments,envelopeNested(query,envelopeReceiptSourceDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,envelopeBusinessProfileDecode()));
        arguments=envelopeHold(arguments,envelopeArrays(1024+4096+64,4));
        arguments=envelopeHold(arguments,envelopeNested(45,envelopeReceiptTrustDecode()));
        var constructor=envelopeArrays(1024+4096+96,5);
        constructor=envelopeTemporary(constructor,envelopeManagedRoutingDecode());
        var decoded=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held-(1024+4096+96)));
        decoded=envelopeTemporary(decoded,envelopeHash(envelopeReservationReceiptFields(false),0));
        decoded=envelopeTemporary(decoded,envelopeReservationReceiptFields(true));
        return envelopeCall(frame,envelopeNested(payload,decoded),0);
    }
    private static EnvelopeMaterial envelopeMigrationReservationOriginCanonical(){
        var fields=envelopeRepeat(envelopeMigrationRecordCanonical(),2,false);
        fields=envelopeDrop(fields,envelopeRepeat(envelopeSourceCanonical(),3,false));
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeReservationReceiptCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetMigrationReservationOrigin.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeMigrationReservationOriginDecode(){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        final long receipt=com.nereusstream.delay.protocol.TargetMigrationReservationOrigin.MAX_RECEIPT_BYTES;
        final long raw=1+64+2*envelopeMigrationRecordBytes()+3*source+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+receipt+21;
        final var parsed=envelopeProtoRead(com.nereusstream.delay.protocol.TargetMigrationReservationOrigin.MAX_CANONICAL_BYTES,
                raw,Math.max(receipt,envelopeMigrationRecordBytes()),13);
        var arguments=envelopeArrays(64,2);
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeNested(envelopeMigrationRecordBytes(),envelopeMigrationRecordDecode()),2,true));
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeNested(source,envelopeSourceDecode()),3,true));
        arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        arguments=envelopeHold(arguments,envelopeNested(receipt,envelopeReservationReceiptDecode()));
        var constructor=envelopeRepeat(envelopeAssigned(32),2,true);
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeSourceBounded(),3,false));
        constructor=envelopeTemporary(constructor,envelopeReservationReceiptCanonical());
        constructor=envelopeTemporary(constructor,envelopeBusinessProfileCanonical());
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeSourceCanonical(),2,true));
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held-64));
        work=envelopeTemporary(work,envelopeMigrationReservationOriginCanonical());return envelopeReturn(work,arguments.held);
    }
    private static EnvelopeMaterial envelopeTimeFenceCanonical(){
        return envelopeWritten(envelopeDrop(envelopeShardSubjectCanonical(),envelopeBusinessTimeCanonical()),
                com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeTimeFenceDecode(){
        final long evidence=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
        final var parsed=envelopeProtoRead(com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES,
                24+1+10+10+5+32+evidence,evidence,7);
        // subject/evidence are named Query.bytes locals, reused by both the preliminary readers and decoders.
        var checked=envelopeHold(parsed,envelopeArray(24));
        checked=envelopeTemporary(checked,envelopeProtoRead(24,21,16,2));
        checked=envelopeHold(checked,envelopeArray(evidence));
        checked=envelopeHold(checked,envelopeProtoRead(evidence,evidence,evidence,10));
        checked=envelopeTemporary(checked,envelopeArray(TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES));
        var arguments=envelopeShardSubjectDecode();
        arguments=envelopeHold(arguments,envelopeBusinessTimeDecode());
        var constructor=envelopeTemporary(envelopeEmpty(),envelopeArray(TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES));
        constructor=envelopeTemporary(constructor,envelopeBusinessTimeCanonical());
        var preimage=envelopeHold(envelopeArray(16),envelopeArrays(4+8+4,3));
        preimage=envelopeHold(preimage,envelopeCall(envelopeBusinessTimeCanonical(),
                envelopeCall(envelopeArray(4),envelopeArray(evidence+4),0),0));
        constructor=envelopeHold(constructor,envelopeHash(preimage,"nereus-delay-time-fence-proof\0".length()));
        var work=envelopeHold(checked,envelopeCall(arguments,constructor,arguments.held));
        work=envelopeTemporary(work,envelopeArray(32));work=envelopeTemporary(work,envelopeTimeFenceCanonical());
        return envelopeReturn(work,arguments.held+32);
    }
    private static EnvelopeMaterial envelopeExpiryLogicalIdentity(){
        // TargetExpireGenerationBody already owns the DelayMessageId: bytes() then fixed() each clone it.
        var preimage=envelopeHold(envelopeArray("nereus-delay-expiry-logical-id".length()),
                envelopeCall(envelopeArray(41),envelopeArray(41),0));
        preimage=envelopeHold(preimage,envelopeArray(4));
        preimage=envelopeHold(preimage,envelopeArray(8));
        return envelopeHash(preimage,0);
    }
    private static EnvelopeMaterial envelopeExpiryBodyValidation(long bodyBytes){
        // SystemMutationBodyCodec.validate reads the body once, copies the fixed message field,
        // then copies and shape-reads evidence. It does not construct either a subject or message ID.
        var parsed=envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,7);
        parsed=envelopeTemporary(parsed,envelopeArray(41));
        parsed=envelopeTemporary(parsed,envelopeNested(bodyBytes,envelopeProtoRead(bodyBytes,bodyBytes,bodyBytes,10)));
        return envelopeReturn(parsed,0);
    }
    private static EnvelopeMaterial envelopeExpiryBodyCanonical(){
        return envelopeWritten(envelopeDrop(envelopeDrop(envelopeShardSubjectCanonical(),envelopeArray(41)),
                envelopeBusinessTimeCanonical()),com.nereusstream.delay.protocol.TargetExpireGenerationBody.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeExpiryBodyDecode(){
        final long encoded=com.nereusstream.delay.protocol.TargetExpireGenerationBody.MAX_CANONICAL_BYTES;
        final long evidence=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
        final var validation=envelopeExpiryBodyValidation(encoded);
        final var parsed=envelopeProtoRead(encoded,24+1+10+41+5+10+evidence,evidence,7);
        var arguments=envelopeNested(24,envelopeShardSubjectDecode());
        arguments=envelopeHold(arguments,envelopeNested(41,envelopeManagedIdDecode()));
        arguments=envelopeHold(arguments,envelopeNested(evidence,envelopeBusinessTimeDecode()));
        var constructor=envelopeTemporary(envelopeEmpty(),envelopeManagedRoutingDecode());
        constructor=envelopeTemporary(constructor,envelopeArray(TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES));
        constructor=envelopeTemporary(constructor,envelopeBusinessTimeCanonical());
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held));
        work=envelopeTemporary(work,envelopeExpiryBodyCanonical());
        return envelopeReturn(envelopeDrop(validation,work),arguments.held);
    }
    private static long envelopePublishAdmissionBytes(int version){
        return switch(version){case 3->com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_CANONICAL_BYTES;
            case 4->com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_MATERIALIZED_BYTES;
            case 5->com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_NATIVE_MATERIALIZED_BYTES;
            case 6->com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_KAFKA_RECEIPT_BYTES;
            default->throw new IllegalArgumentException("unknown closed Admission material version");};
    }
    private static EnvelopeMaterial envelopePublishAdmissionCanonical(int version){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeOwnerIdentityCanonical());
        fields=envelopeDrop(fields,envelopeMessageLocatorCanonical());fields=envelopeDrop(fields,envelopeObligationCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeVectorCanonical(),2,false));
        fields=envelopeDrop(fields,envelopeBusinessTimeCanonical());
        if(version>=4){fields=envelopeDrop(fields,version==5?envelopeNativePublicationCanonical():envelopeOrdinaryPublicationCanonical());
            fields=envelopeDrop(fields,envelopeClaimCanonical(version==5?2:1));}
        if(version==6)fields=envelopeDrop(fields,envelopeKafkaReceiptBindingCanonical());
        return envelopeWritten(fields,envelopePublishAdmissionBytes(version));
    }
    private static EnvelopeMaterial envelopePublishAdmissionCanonical(){
        return envelopeChoice(envelopePublishAdmissionCanonical(5),envelopePublishAdmissionCanonical(6));
    }
    private static EnvelopeMaterial envelopePublishAdmissionDecode(int version){
        final long encoded=envelopePublishAdmissionBytes(version),owner=Math.min(encoded,TargetQuotaClaimCharge.MAX_OWNER_BYTES);
        final var fields=envelopeProtoRead(encoded,encoded,encoded,version==6?18:version>=4?17:15);
        // BodyCodec.fields validates one Reader and returns a second independent Reader inventory.
        var validation=envelopeTemporary(fields,envelopeArrays(80,3));
        validation=envelopeTemporary(validation,envelopeNested(owner,envelopeProtoRead(owner,owner,owner,4)));
        final long locator=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES;
        validation=envelopeTemporary(validation,envelopeNested(locator,envelopeProtoRead(locator,locator,locator,11)));
        final long obligation=TargetGenerationRuntimeIndex.MAX_OBLIGATION_BYTES,vector=TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES;
        validation=envelopeTemporary(validation,envelopeNested(obligation,envelopeProtoRead(obligation,obligation,obligation,6)));
        validation=envelopeTemporary(validation,envelopeRepeat(envelopeNested(vector,envelopeProtoRead(vector,vector,vector,17)),2,false));
        final long evidence=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
        validation=envelopeTemporary(validation,envelopeNested(evidence,envelopeProtoRead(evidence,evidence,evidence,10)));
        if(version>=4){
            final long publication=version==5?com.nereusstream.delay.protocol.TargetNativePublicationBinding.MAX_CANONICAL_BYTES:
                    com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding.MAX_CANONICAL_BYTES;
            final long claim=version==5?TargetClaimRecord.MAX_CANONICAL_BYTES:TargetClaimRecord.MAX_V1_CANONICAL_BYTES;
            validation=envelopeTemporary(validation,envelopeNested(publication,envelopeProtoRead(publication,publication,publication,version==5?26:21)));
            validation=envelopeTemporary(validation,envelopeNested(claim,envelopeProtoRead(claim,claim,claim,version==5?15:14)));
        }
        if(version==6){final long receipt=com.nereusstream.delay.protocol.TargetKafkaReceiptBinding.MAX_CANONICAL_BYTES;
            validation=envelopeTemporary(validation,envelopeNested(receipt,envelopeProtoRead(receipt,receipt,receipt,9)));}
        var parsed=envelopeDrop(validation,fields);
        var arguments=envelopeNested(24,envelopeShardSubjectDecode());
        arguments=envelopeHold(arguments,envelopeNested(owner,envelopeOwnerIdentityDecode(owner)));
        arguments=envelopeHold(arguments,envelopeArrays(16+32+32,3));
        arguments=envelopeHold(arguments,envelopeNested(com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES,envelopeMessageLocatorDecode()));
        arguments=envelopeHold(arguments,envelopeNested(TargetGenerationRuntimeIndex.MAX_OBLIGATION_BYTES,envelopeObligationDecode()));
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeNested(TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES,envelopeVectorDecode()),2,true));
        arguments=envelopeHold(arguments,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,envelopeBusinessTimeDecode()));
        if(version>=4){final long publication=version==5?com.nereusstream.delay.protocol.TargetNativePublicationBinding.MAX_CANONICAL_BYTES:
                    com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding.MAX_CANONICAL_BYTES;
            arguments=envelopeHold(arguments,envelopeNested(publication,version==5?envelopeNativePublicationDecode():envelopeOrdinaryPublicationDecode()));
            arguments=envelopeHold(arguments,envelopeNested(version==5?TargetClaimRecord.MAX_CANONICAL_BYTES:TargetClaimRecord.MAX_V1_CANONICAL_BYTES,
                    envelopeClaimDecode(version==5?2:1)));}
        if(version==6)arguments=envelopeHold(arguments,envelopeNested(com.nereusstream.delay.protocol.TargetKafkaReceiptBinding.MAX_CANONICAL_BYTES,envelopeKafkaReceiptBindingDecode()));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeRepeat(envelopeAssigned(32),2,true));
        constructor=envelopeTemporary(constructor,envelopeManagedRoutingDecode());
        // Exact attempt logical-ID preimage, actual inflight key and obligation accessors.
        var attempt=envelopeHold(envelopeArray(32),envelopeCall(envelopeArray(41),envelopeArray(41),0));
        attempt=envelopeHold(attempt,envelopeArrays(8,2));
        constructor=envelopeTemporary(constructor,envelopeHash(attempt,"nereus-delay-publish-attempt-id\0".length()));
        final var inflight=envelopeCall(envelopeHold(envelopeArrays(10,2),
                envelopeCall(envelopeArray(4),envelopeArray(36),0)),envelopeArray(46),0);
        constructor=envelopeTemporary(constructor,envelopeHold(inflight,envelopeHold(envelopeArray(32),envelopeArray(46))));
        if(version>=4){constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeMessageLocatorCanonical(),4,false));
            constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(32),4,false));
            constructor=envelopeTemporary(constructor,envelopeArray(16));}
        constructor=envelopeTemporary(constructor,envelopePublishAdmissionCanonical(version));
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held-80));
        work=envelopeTemporary(work,envelopePublishAdmissionCanonical(version));return envelopeReturn(work,arguments.held);
    }
    private static EnvelopeMaterial envelopePublishAdmissionDecode(){
        return envelopeChoice(envelopeChoice(envelopePublishAdmissionDecode(3),envelopePublishAdmissionDecode(4)),
                envelopeChoice(envelopePublishAdmissionDecode(5),envelopePublishAdmissionDecode(6)));
    }
    private static EnvelopeMaterial envelopeManagedRoutingDecode(){
        return envelopeReturn(envelopeHold(envelopeHold(envelopeArray(16),envelopeArray(16)),envelopeArray(41)),57);
    }
    private static EnvelopeMaterial envelopeManagedIdDecode(){
        return envelopeReturn(envelopeTemporary(envelopeArray(41),envelopeManagedRoutingDecode()),41);
    }
    private static EnvelopeMaterial envelopeDelayMessageIdDecode(){return envelopeManagedIdDecode();}
    private static EnvelopeMaterial envelopeCommandFrameCanonical(){
        return envelopeCommandFrameCanonical(com.nereusstream.delay.protocol.TargetMigrationArchiveSourceFrame.MAX_COMMAND_FRAME_BYTES);
    }
    private static EnvelopeMaterial envelopeCommandFrameCanonical(long frameBytes){
        // decodeFrame accepts all five CommandType values without decoding canonicalBody. Its body is
        // bounded by the caller's actual complete F, not a typed Schedule cap or inverted wire overhead.
        final long envelope=Math.subtractExact(frameBytes,16),client=envelope,body=frameBytes;
        var fields=envelopeDrop(envelopeRepeat(envelopeArray(41),2,false),envelopeArray(body));
        fields=envelopeDrop(fields,envelopeArray(32));
        var encoded=envelopeWritten(envelopeWritten(fields,client),envelope);
        // NDL1 owns a header, CRC preimage concat, CRC bytes and final header/envelope/CRC concat.
        final var header=envelopeArray(12);
        var framing=envelopeHold(header,envelopeArray(envelope+12));
        framing=envelopeDrop(framing,envelopeArray(4));
        framing=envelopeCall(envelopeReturn(framing,16),envelopeArray(frameBytes),0);
        return envelopeCall(encoded,framing,0);
    }
    private static EnvelopeMaterial envelopeCommandFrameDecode(){
        return envelopeCommandFrameDecode(com.nereusstream.delay.protocol.TargetMigrationArchiveSourceFrame.MAX_COMMAND_FRAME_BYTES);
    }
    private static EnvelopeMaterial envelopeCommandFrameDecode(long frameBytes){
        final long envelope=Math.subtractExact(frameBytes,16),client=envelope,body=frameBytes;
        final var framed=envelopeCall(envelopeArray(envelope),envelopeArray(envelope),0);
        final var outer=envelopeProtoRead(envelope,envelope,envelope,2);
        final var inner=envelopeProtoRead(client,client,client,8);
        var arguments=envelopeRepeat(envelopeNested(41,envelopeManagedIdDecode()),2,true);
        arguments=envelopeHold(arguments,envelopeArray(body));arguments=envelopeHold(arguments,envelopeArray(32));
        var constructor=envelopeTemporary(envelopeArray(body),envelopeRepeat(envelopeManagedRoutingDecode(),2,false));
        var preimage=envelopeHold(envelopeArrays(1+4+4+4+2+8,6),envelopeRepeat(
                envelopeCall(envelopeArray(41),envelopeCall(envelopeArray(4),envelopeArray(45),0),0),2,true));
        preimage=envelopeHold(preimage,envelopeCall(envelopeArray(4),envelopeArray(body+4),0));
        constructor=envelopeTemporary(constructor,envelopeHash(preimage,"nereus-delay-command-hash\0".length()));
        constructor=envelopeHold(constructor,envelopeArray(32));
        var decoded=envelopeHold(inner,envelopeCall(arguments,constructor,82));
        decoded=envelopeTemporary(decoded,envelopeRepeat(envelopeManagedRoutingDecode(),2,false));
        var work=envelopeCall(framed,envelopeCall(envelopeArray(envelope),
                envelopeHold(outer,envelopeNested(client,decoded)),0),0);
        return envelopeReturn(work,82+body+32+16);
    }
    private static EnvelopeMaterial envelopeCommandResultCanonical(){return envelopeArray(26L+com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeCommandResultDecode(){
        var work=envelopeArray(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES);
        work=envelopeCall(work,envelopeCall(envelopeResultSourceDecode(),envelopeSourceCanonical(),0),0);
        return envelopeReturn(work,com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeCommandDedupeCanonical(){
        final var fields=envelopeHold(envelopeTupleCanonical(),envelopeCommandResultCanonical());
        return envelopeCall(envelopeHold(fields,envelopeRepeat(envelopeArray(4),3,true)),
                envelopeArray(12+26+32+26L+com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES),0);
    }
    private static EnvelopeMaterial envelopeCommandDedupeDecode(){
        var arguments=envelopeNested(26,envelopeTupleDecode());arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeNested(26L+com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeCommandResultDecode()));
        var work=envelopeCall(arguments,envelopeArray(32),arguments.held-32);
        work=envelopeTemporary(work,envelopeCommandDedupeCanonical());return envelopeReturn(work,arguments.held);
    }
    /** SourcePositionCodec.decode itself; TargetSourcePosition's additional bound check is not called here. */
    private static EnvelopeMaterial envelopeResultSourceDecode(boolean pulsar){
        final long text=pulsar?com.nereusstream.delay.protocol.TargetSourcePosition.MAX_TOPIC_UTF8_BYTES:
                com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CLUSTER_UTF8_BYTES;
        var work=envelopeArray(16);
        if(pulsar)work=envelopeHold(work,envelopeArray(32));
        work=envelopeTemporary(work,envelopeArray(text));
        if(!pulsar)work=envelopeTemporary(work,envelopeArray(16));
        work=envelopeHold(work,envelopeArray(16));
        work=envelopeTemporary(work,envelopeArray(text));
        if(pulsar)work=envelopeHold(work,envelopeArray(32));
        work=envelopeReturn(work,pulsar?48:16);
        return envelopeTemporary(work,envelopeSourceCanonical(pulsar));
    }
    private static EnvelopeMaterial envelopeResultSourceDecode(){
        return envelopeChoice(envelopeResultSourceDecode(false),envelopeResultSourceDecode(true));
    }
    private static EnvelopeMaterial envelopeSystemResultCanonical(long author){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        var fields=envelopeHold(envelopeRepeat(envelopeArray(4),3,true),envelopeArray(8));
        fields=envelopeHold(fields,envelopeArray(1));
        fields=envelopeHold(fields,envelopeCall(envelopeArray(4),envelopeArray(author+4),0));
        fields=envelopeHold(fields,envelopeCall(envelopeArray(4),envelopeArray(source+4),0));
        return envelopeCall(fields,envelopeArray(89+author+source),0);
    }
    private static EnvelopeMaterial envelopeSystemResultCanonical(){
        return envelopeSystemResultCanonical(TargetResultRecord.MAX_AUTHOR_BYTES);
    }
    private static EnvelopeMaterial envelopeSystemResultConstruct(long author){
        return envelopeHold(envelopeArrays(64+author,3),
                envelopeCall(envelopeResultSourceDecode(),envelopeSourceCanonical(),0));
    }
    /** from(mutation,...,source).encode(); the caller owns and accounts for the source argument array. */
    private static EnvelopeMaterial envelopeSystemResultFromCanonical(long author){
        final var decoded=envelopeResultSourceDecode();
        var arguments=envelopeHold(envelopeArrays(64+author,3),envelopeSourceCanonical());
        final var result=envelopeCall(decoded,envelopeCall(arguments,envelopeSystemResultConstruct(author),0),0);
        return envelopeCall(result,envelopeSystemResultCanonical(author),0);
    }
    private static EnvelopeMaterial envelopeSystemResultDecode(){
        final long author=TargetResultRecord.MAX_AUTHOR_BYTES,source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        var arguments=envelopeHold(envelopeArrays(64+author,3),envelopeArray(source));
        var work=envelopeCall(arguments,envelopeSystemResultConstruct(author),0);
        work=envelopeTemporary(work,envelopeSystemResultCanonical());
        return envelopeReturn(work,64+author+source);
    }
    private static long envelopeResultPayloadBytes(int kind){
        final long source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES;
        return kind==0||kind==5?96+source:kind==1?26+source:kind==2?TargetResultRecord.MAX_AUTHOR_BYTES+source+89:kind==4?32:41;
    }
    private static boolean envelopeResultReferencesFirst(int kind){return kind==1||kind==3||kind==4;}
    private static long envelopeResultLogicalIdBytes(int kind){return kind==2||kind==4?32:41;}
    private static long envelopeResultFieldsBytes(int kind,boolean allocation){
        long encoded=4+envelopeCommonBytesField(3,envelopeResultLogicalIdBytes(kind))
                +envelopeCommonBytesField(4,TargetQuotaIdentity.MAX_CANONICAL_BYTES)+34
                +envelopeCommonBytesField(6,TargetQuotaAccounting.MAX_CANONICAL_BYTES)+18
                +envelopeCommonBytesField(8,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES)
                +envelopeCommonBytesField(9,envelopeResultPayloadBytes(kind));
        if(envelopeResultReferencesFirst(kind))encoded=Math.addExact(encoded,34);
        if(allocation)encoded=Math.addExact(encoded,envelopeCommonBytesField(11,TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES));
        return encoded;
    }
    private static EnvelopeMaterial envelopeResultFields(int kind,boolean allocation){
        var fields=envelopeDrop(envelopeIdentityCanonical(),envelopeAccountingCanonical());
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        if(allocation)fields=envelopeDrop(fields,envelopeIncarnationCanonical(false));
        return envelopeWritten(fields,envelopeResultFieldsBytes(kind,allocation));
    }
    private static EnvelopeMaterial envelopeResultCanonical(int kind,boolean allocation){
        return envelopeWritten(envelopeResultFields(kind,allocation),envelopeResultFieldsBytes(kind,allocation)+34);
    }
    private static EnvelopeMaterial envelopeResultCanonical(){
        var selected=envelopeResultCanonical(2,true);
        for(int kind=0;kind<6;kind++)selected=envelopeChoice(selected,envelopeResultCanonical(kind,false));
        return selected;
    }
    private static EnvelopeMaterial envelopeResultDecode(int kind,boolean allocation){
        final long payload=envelopeResultPayloadBytes(kind),id=envelopeResultLogicalIdBytes(kind);
        final boolean referencesFirst=envelopeResultReferencesFirst(kind);
        final long encoded=envelopeResultFieldsBytes(kind,allocation)+34;
        final long raw=2+id+TargetQuotaIdentity.MAX_CANONICAL_BYTES+32+TargetQuotaAccounting.MAX_CANONICAL_BYTES+16
                +TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+payload+(referencesFirst?32:0)+32
                +(allocation?TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES:0);
        final var parsed=envelopeProtoRead(encoded,raw,Math.max(payload,allocation?TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES:0),10+(referencesFirst?1:0)+(allocation?1:0));
        // decode reads the optional first/allocation and expected digest before its constructor arguments.
        var arguments=referencesFirst?envelopeArray(32):envelopeEmpty();
        if(allocation)arguments=envelopeHold(arguments,envelopeNested(TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES,envelopeIncarnationDecode(false)));
        arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeArray(id));arguments=envelopeHold(arguments,envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode()));
        arguments=envelopeHold(arguments,envelopeArray(32));arguments=envelopeHold(arguments,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        arguments=envelopeHold(arguments,envelopeArray(16));arguments=envelopeHold(arguments,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        arguments=envelopeHold(arguments,envelopeArray(payload));
        var constructor=envelopeHold(envelopeAssigned(id),envelopeHold(envelopeAssigned(32),envelopeAssigned(16)));
        if(id==41){constructor=envelopeTemporary(constructor,envelopeManagedIdDecode());
            constructor=envelopeTemporary(constructor,envelopeManagedRoutingDecode());}
        constructor=envelopeHold(constructor,envelopeArray(payload));
        if(referencesFirst)constructor=envelopeHold(constructor,envelopeAssigned(32));
        EnvelopeMaterial validation;
        if(kind==0||kind==5){validation=envelopeCommandDedupeDecode();
            validation=envelopeTemporary(validation,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
            validation=envelopeTemporary(validation,envelopeCommandDedupeCanonical());}
        else if(kind==1)validation=envelopeTemporary(envelopeCommandResultDecode(),envelopeCommandResultCanonical());
        else if(kind==2){validation=envelopeSystemResultDecode();
            validation=envelopeTemporary(validation,envelopeArray(32));
            validation=envelopeTemporary(validation,envelopeArray(TargetResultRecord.MAX_AUTHOR_BYTES));
            validation=envelopeTemporary(validation,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
            final long author=TargetResultRecord.MAX_AUTHOR_BYTES;
            var authorProbe=envelopeCall(envelopeArray(author),envelopeProtoRead(author,author,author,1),0);
            authorProbe=envelopeHold(authorProbe,envelopeNested(author,envelopeProtoRead(author,author,author,4)));
            validation=envelopeTemporary(validation,authorProbe);
            validation=envelopeTemporary(validation,envelopeNested(author,envelopeAuthorDecode(author)));
            if(allocation)validation=envelopeTemporary(validation,envelopeSystemResultDecode());}
        else validation=envelopeEmpty();
        if(kind<3||kind==5){
            validation=envelopeHold(validation,envelopeArray(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES));
            validation=envelopeTemporary(validation,envelopeComparison(envelopeSourceCanonical(),
                    envelopeCall(envelopeSourceDecode(),envelopeSourceCanonical(),0)));}
        constructor=envelopeTemporary(constructor,validation);
        constructor=envelopeHold(constructor,envelopeHash(envelopeResultFields(kind,allocation),"nereus-delay-target-result-record\0".length()));
        final long retainedArguments=arguments.held-id-32-16-payload-32-(referencesFirst?32:0);
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,retainedArguments));
        work=envelopeTemporary(work,envelopeResultCanonical(kind,allocation));
        return envelopeReturn(work,retainedArguments+id+32+16+payload+(referencesFirst?32:0)+32);
    }
    private static EnvelopeMaterial envelopeResultDecode(){
        var selected=envelopeResultDecode(2,true);
        for(int kind=0;kind<6;kind++)selected=envelopeChoice(selected,envelopeResultDecode(kind,false));
        return selected;
    }
    private static EnvelopeMaterial envelopeRetryRefCanonical(){return envelopeArray(304);}
    private static EnvelopeMaterial envelopeRetryRefDecode(){
        var work=envelopeHold(envelopeProtoRead(304,256+10+32,256,3),
                envelopeCall(envelopeHold(envelopeArray(256),envelopeArray(32)),envelopeArray(288),0));
        work=envelopeTemporary(work,envelopeRetryRefCanonical());return envelopeReturn(work,288);
    }
    private static EnvelopeMaterial envelopeAdapterMetadataCanonical(long totalBytes,int entries,boolean pulsar){
        // totalBytes is the entire bounded metadata branch: entry payload is never multiplied by entries.
        final var entryCanonicals=envelopeArrays(totalBytes,entries);
        final var branch=envelopeWritten(entryCanonicals,totalBytes);
        return envelopeWritten(branch,totalBytes);
    }
    private static EnvelopeMaterial envelopeAdapterMetadataCanonical(){
        return envelopeAdapterMetadataCanonical(1L<<20,TargetScheduleBinding.MAX_METADATA_ENTRIES,true);
    }
    private static EnvelopeMaterial envelopeAdapterMetadataDecode(long totalBytes,int entries,boolean pulsar){
        final var union=envelopeProtoRead(totalBytes,totalBytes,totalBytes,1);
        final var branch=envelopeProtoRead(totalBytes,totalBytes,totalBytes,entries+(pulsar?3L:1L));
        // Nested entry Readers, their field ownership and the caller's complete entry-array copies.
        var entryWork=envelopeProtoRead(totalBytes,totalBytes,totalBytes,2L*entries);
        entryWork=envelopeHold(entryWork,envelopeArrays(totalBytes,2L*entries));
        // UTF-8 roundtrip plus owned name/value bytes; Kafka values skip the UTF-8 roundtrip.
        final var text=envelopeDrop(envelopeArrays(totalBytes,2L*entries),envelopeArrays(totalBytes,2L*entries));
        entryWork=envelopeTemporary(entryWork,text);
        entryWork=envelopeHold(entryWork,envelopeArrays(totalBytes,2L*entries));
        entryWork=envelopeTemporary(entryWork,envelopeArrays(totalBytes,entries)); // per-entry canonical sum
        if(pulsar){
            // Property ordering compares two key accessor clones for every adjacent pair.
            entryWork=envelopeTemporary(entryWork,envelopeArrays(Math.multiplyExact(2L,totalBytes),Math.multiplyExact(2L,Math.max(0,entries-1))));
        }
        var arguments=envelopeHold(envelopeArray(totalBytes),envelopeCall(envelopeArray(totalBytes),branch,0));
        arguments=envelopeHold(arguments,entryWork);
        var work=envelopeHold(union,arguments);
        work=envelopeTemporary(work,envelopeAdapterMetadataCanonical(totalBytes,entries,pulsar));
        return envelopeReturn(work,totalBytes);
    }
    private static EnvelopeMaterial envelopeAdapterMetadataDecode(){
        return envelopeChoice(envelopeAdapterMetadataDecode(1L<<20,TargetScheduleBinding.MAX_METADATA_ENTRIES,false),
                envelopeAdapterMetadataDecode(1L<<20,TargetScheduleBinding.MAX_METADATA_ENTRIES,true));
    }
    private static EnvelopeMaterial envelopeCommittedDescriptorCanonical(){
        return envelopeWritten(envelopeBusinessProfileCanonical(),4L*(1L<<20)+1024);
    }
    private static EnvelopeMaterial envelopeCommittedDescriptorDecode(){
        final long n=4L*(1L<<20)+1024,owned=4L*(1L<<20)+96;
        final var parsed=envelopeProtoRead(n,TargetChannelIdentity.MAX_PROFILE_REF_BYTES+owned+10,1L<<20,9);
        var arguments=envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,envelopeBusinessProfileDecode());
        arguments=envelopeHold(arguments,envelopeArrays(owned,7));
        var constructor=envelopeHold(envelopeArrays(4L*(1L<<20)+32,5),envelopeRepeat(envelopeAssigned(32),2,true));
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held-owned));
        work=envelopeTemporary(work,envelopeCommittedDescriptorCanonical());return envelopeReturn(work,arguments.held);
    }
    private static long envelopeScheduleIntentBytes(int payload){return 3L*(1L<<20)+4096+(payload==1?1L<<24:payload==2?4L*(1L<<20)+1024:0);}
    private static EnvelopeMaterial envelopeScheduleIntentCanonical(int payload){
        var fields=envelopeDrop(envelopeBusinessProfileCanonical(),envelopeRetryRefCanonical());
        if(payload==2)fields=envelopeDrop(fields,envelopeCommittedDescriptorCanonical());
        fields=envelopeDrop(fields,envelopeAdapterMetadataCanonical());
        return envelopeWritten(fields,envelopeScheduleIntentBytes(payload));
    }
    private static EnvelopeMaterial envelopeScheduleIntentCanonical(){
        return envelopeChoice(envelopeScheduleIntentCanonical(1),envelopeScheduleIntentCanonical(2));
    }
    private static EnvelopeMaterial envelopeScheduleIntentDecode(int payload){
        final long data=2L*(1L<<20)+(payload==1?1L<<24:0),encoded=envelopeScheduleIntentBytes(payload);
        final long branch=payload==2?4L*(1L<<20)+1024:payload==1?1L<<24:0;
        final var parsed=envelopeProtoRead(encoded,TargetChannelIdentity.MAX_PROFILE_REF_BYTES+304+data+(1L<<20)
                +(payload==2?branch:0)+34,Math.max(branch,1L<<20),payload==0?12:13);
        var arguments=envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,envelopeBusinessProfileDecode());
        arguments=envelopeHold(arguments,envelopeNested(304,envelopeRetryRefDecode()));
        arguments=envelopeHold(arguments,envelopeArrays(data,payload==1?3:2));
        if(payload==2)arguments=envelopeHold(arguments,envelopeNested(branch,envelopeCommittedDescriptorDecode()));
        arguments=envelopeHold(arguments,envelopeNested(1L<<20,envelopeAdapterMetadataDecode()));
        var work=envelopeHold(parsed,envelopeCall(arguments,envelopeArrays(data,payload==1?3:2),arguments.held-data));
        work=envelopeTemporary(work,envelopeScheduleIntentCanonical(payload));return envelopeReturn(work,arguments.held);
    }
    private static EnvelopeMaterial envelopeScheduleIntentDecode(){
        return envelopeChoice(envelopeChoice(envelopeScheduleIntentDecode(0),envelopeScheduleIntentDecode(1)),envelopeScheduleIntentDecode(2));
    }
    private static EnvelopeMaterial envelopeScheduleBodyCanonical(){
        return envelopeWritten(envelopeDrop(envelopeArray(41),envelopeScheduleIntentCanonical()),TargetScheduleBinding.MAX_BODY_BYTES);
    }
    private static EnvelopeMaterial envelopePrepareBodyCanonical(){
        var fields=envelopeDrop(envelopeArray(41),envelopeScheduleIntentCanonical(0));
        fields=envelopeDrop(fields,envelopeArray(45));fields=envelopeDrop(fields,envelopeBusinessProfileCanonical());
        return envelopeWritten(fields,envelopeScheduleIntentBytes(0)+TargetChannelIdentity.MAX_PROFILE_REF_BYTES+256);
    }
    private static EnvelopeMaterial envelopeScheduleBodyDecode(){return envelopeScheduleBodyDecode(false);}
    private static EnvelopeMaterial envelopePrepareBodyDecode(){return envelopeScheduleBodyDecode(true);}
    private static EnvelopeMaterial envelopeScheduleProfileValidation(){
        final long profile=TargetChannelIdentity.MAX_PROFILE_REF_BYTES;
        final var read=envelopeTemporary(envelopeProtoRead(profile,profile,256,4),envelopeArray(256));
        return envelopeReturn(envelopeNested(profile,read),0);
    }
    private static EnvelopeMaterial envelopeScheduleMetadataValidation(){
        final long bytes=1L<<20,entries=TargetScheduleBinding.MAX_METADATA_ENTRIES;
        var parsed=envelopeProtoRead(bytes,bytes,bytes,1);
        final var branch=envelopeProtoRead(bytes,bytes,bytes,entries+3);
        // Each loop iteration copies its entry, constructs one Reader and copies each of its two fields twice.
        final var entryReads=new EnvelopeMaterial(4*bytes,4*bytes,0,2*entries,6*entries);
        parsed=envelopeHold(parsed,envelopeCall(envelopeArray(bytes),envelopeTemporary(branch,entryReads),0));
        return envelopeReturn(envelopeNested(bytes,parsed),0);
    }
    private static EnvelopeMaterial envelopeScheduleIntentValidation(int payload){
        final long auxiliary=1L<<20,intent=envelopeScheduleIntentBytes(payload);
        final long data=2*auxiliary+(payload==1?1L<<24:0),descriptor=4*auxiliary+1024;
        var parsed=envelopeProtoRead(intent,Math.min(intent,TargetChannelIdentity.MAX_PROFILE_REF_BYTES+304+data+auxiliary
                +(payload==2?descriptor:0)+34),Math.max(auxiliary,payload==1?1L<<24:payload==2?descriptor:0),payload==0?12:13);
        parsed=envelopeTemporary(parsed,envelopeScheduleProfileValidation());
        final var retry=envelopeTemporary(envelopeProtoRead(304,298,256,3),envelopeArray(256));
        parsed=envelopeTemporary(parsed,envelopeNested(304,retry));
        parsed=envelopeTemporary(parsed,envelopeArrays(2*auxiliary,2)); // ordering/business Query.bytes
        if(payload==1)parsed=envelopeTemporary(parsed,envelopeArray(1L<<24));
        if(payload==2){
            var parts=envelopeProtoRead(descriptor,descriptor,auxiliary,9);
            parts=envelopeTemporary(parts,envelopeScheduleProfileValidation());
            parts=envelopeTemporary(parts,envelopeArrays(4*auxiliary,4));
            parsed=envelopeTemporary(parsed,envelopeNested(descriptor,parts));
        }
        parsed=envelopeTemporary(parsed,envelopeScheduleMetadataValidation());
        return envelopeReturn(parsed,0);
    }
    private static EnvelopeMaterial envelopeScheduleBodyDecode(boolean prepare){
        final long intent=prepare?envelopeScheduleIntentBytes(0):TargetScheduleBinding.MAX_BODY_BYTES;
        final long encoded=prepare?intent+TargetChannelIdentity.MAX_PROFILE_REF_BYTES+256:TargetScheduleBinding.MAX_BODY_BYTES;
        // TargetScheduleBody validation parses the outer body, nested intent and each closed auxiliary branch first.
        var validation=envelopeProtoRead(encoded,Math.min(encoded,intent+41+10+45+32+TargetChannelIdentity.MAX_PROFILE_REF_BYTES),intent,prepare?9:4);
        final var intentValidation=prepare?envelopeScheduleIntentValidation(0):
                envelopeChoice(envelopeScheduleIntentValidation(1),envelopeScheduleIntentValidation(2));
        validation=envelopeTemporary(validation,envelopeNested(intent,intentValidation));
        if(prepare){validation=envelopeTemporary(validation,envelopeNested(45,envelopeProtoRead(45,42,32,2)));
            validation=envelopeTemporary(validation,envelopeScheduleProfileValidation());}
        final var parsed=envelopeProtoRead(encoded,Math.min(encoded,intent+41+10+(prepare?45+32+TargetChannelIdentity.MAX_PROFILE_REF_BYTES+20:0)),intent,prepare?9:4);
        var arguments=envelopeHold(envelopeNested(41,envelopeManagedIdDecode()),
                envelopeNested(intent,prepare?envelopeScheduleIntentDecode(0):envelopeScheduleIntentDecode()));
        if(prepare){arguments=envelopeHold(arguments,envelopeArray(32));
            arguments=envelopeHold(arguments,envelopeCall(envelopeArray(45),
                    envelopeTemporary(envelopeHold(envelopeProtoRead(45,42,32,2),envelopeArray(32)),envelopeArray(45)),0));
            arguments=envelopeHold(arguments,envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,envelopeBusinessProfileDecode()));}
        var frame=envelopeHold(parsed,arguments);if(prepare)frame=envelopeHold(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,prepare?envelopePrepareBodyCanonical():envelopeScheduleBodyCanonical());
        return envelopeReturn(envelopeDrop(validation,frame),arguments.held+(prepare?32:0));
    }
    private static EnvelopeMaterial envelopeMessageLocatorFields(boolean ordered){
        return envelopeWritten(envelopeDrop(envelopeArray(DelayMessageId.LENGTH),envelopeArray(32)),
                com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES-34L-(ordered?0:34));
    }
    private static EnvelopeMaterial envelopeMessageLocatorCanonical(boolean ordered){
        return envelopeWritten(envelopeMessageLocatorFields(ordered),com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES-(ordered?0:34));
    }
    private static EnvelopeMaterial envelopeMessageLocatorCanonical(){return envelopeMessageLocatorCanonical(true);}
    private static EnvelopeMaterial envelopeMessageLocatorDecode(boolean ordered){
        final long encoded=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES-(ordered?0:34);
        final var parsed=envelopeProtoRead(encoded,1+41+5+32+3+10+16+1+(ordered?32:0)+32+32,41,ordered?11:10);
        var arguments=envelopeHold(envelopeNested(41,envelopeManagedIdDecode()),
                envelopeCall(envelopeArray(32),envelopeArray(32),0));
        arguments=envelopeHold(arguments,envelopeArray(16));
        if(ordered)arguments=envelopeHold(arguments,envelopeArray(32));
        arguments=envelopeHold(arguments,envelopeArray(32));
        var constructor=envelopeHold(envelopeAssigned(16),envelopeAssigned(32));
        if(ordered)constructor=envelopeHold(constructor,envelopeAssigned(32));
        constructor=envelopeHold(constructor,envelopeHash(envelopeMessageLocatorFields(ordered),
                "nereus-delay-target-message-locator\0".length()));
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,73));
        work=envelopeTemporary(work,envelopeArray(32));
        work=envelopeTemporary(work,envelopeMessageLocatorCanonical(ordered));
        return envelopeReturn(work,41+32+16+(ordered?32:0)+32+32);
    }
    private static EnvelopeMaterial envelopeMessageLocatorDecode(){
        return envelopeChoice(envelopeMessageLocatorDecode(false),envelopeMessageLocatorDecode(true));
    }
    private static EnvelopeMaterial envelopePayloadRefCanonical(boolean etag){
        final long component=com.nereusstream.delay.protocol.TargetPayloadReference.MAX_COMPONENT_BYTES;
        // Four lp32 arrays remain arguments of the base concat; committed IDs add a second concat.
        final var part=envelopeCall(envelopeArray(4),envelopeArray(component+4),0);
        var arguments=envelopeHold(envelopeArray(4),envelopeRepeat(part,etag?4:3,true));
        if(!etag)arguments=envelopeHold(arguments,envelopeCall(envelopeArray(0),
                envelopeCall(envelopeArray(4),envelopeArray(4),0),0));
        arguments=envelopeHold(arguments,envelopeArray(8));
        final long encoded=com.nereusstream.delay.protocol.TargetPayloadReference.MAX_CANONICAL_BYTES-(etag?0:component);
        final var base=envelopeCall(arguments,envelopeArray(encoded-64L),0);
        return envelopeCall(base,envelopeArray(encoded),0);
    }
    private static EnvelopeMaterial envelopePayloadRefCanonical(){
        return envelopeChoice(envelopePayloadRefCanonical(false),envelopePayloadRefCanonical(true));
    }
    private static EnvelopeMaterial envelopePayloadRefDecode(boolean etag){
        final long owned=(etag?4L:3L)*com.nereusstream.delay.protocol.TargetPayloadReference.MAX_COMPONENT_BYTES+4L*32;
        final var binaryRead=envelopeArrays(owned,8);
        // readFixed inputs and constructor-owned clones coexist; fixedNonZero handles both commit IDs.
        var work=envelopeHold(binaryRead,envelopeArrays(owned,etag?8:7));
        work=envelopeTemporary(work,envelopePayloadRefCanonical(etag));
        work=envelopeTemporary(work,envelopeHold(envelopeArray(32),envelopeArray(32)));
        return envelopeReturn(work,owned);
    }
    private static EnvelopeMaterial envelopePayloadRefDecode(){
        return envelopeChoice(envelopePayloadRefDecode(false),envelopePayloadRefDecode(true));
    }
    private static EnvelopeMaterial envelopeObligationFields(boolean digest){
        return envelopeArray(TargetGenerationRuntimeIndex.MAX_OBLIGATION_BYTES-(digest?0:34));
    }
    private static EnvelopeMaterial envelopeObligationCanonical(){return envelopeObligationFields(true);}
    private static EnvelopeMaterial envelopeObligationDecode(){
        final long encoded=TargetGenerationRuntimeIndex.MAX_OBLIGATION_BYTES;
        final var parsed=envelopeProtoRead(encoded,32+5+1+46+32+32,46,6);
        final var arguments=envelopeHold(envelopeRepeat(envelopeArray(32),3,true),envelopeArray(46));
        var constructor=envelopeHold(envelopeArray(32),envelopeArray(46));
        constructor=envelopeHold(constructor,envelopeHash(envelopeArray(46),0));
        constructor=envelopeHold(constructor,envelopeHash(envelopeObligationFields(false),"nereus-delay-attempt-obligation-ref\0".length()));
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,0));
        work=envelopeTemporary(work,envelopeObligationCanonical());
        return envelopeReturn(work,32+46+32+32);
    }
    private static EnvelopeMaterial envelopeGenerationRuntimeFields(int branch){
        var fields=branch==1?envelopeBusinessWorkCanonical():envelopeEmpty();
        fields=envelopeDrop(fields,envelopeRepeat(envelopeObligationCanonical(),TargetGenerationRuntimeIndex.MAX_OBLIGATIONS,false));
        final long encoded=TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES-34L
                -(branch==1?0:TargetTimelineWorkRef.MAX_CANONICAL_BYTES+4L)+(branch>=2?34:0);
        return envelopeWritten(fields,encoded);
    }
    private static EnvelopeMaterial envelopeGenerationRuntimeCanonical(int branch){
        return envelopeWritten(envelopeGenerationRuntimeFields(branch),TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES
                -(branch==1?0:TargetTimelineWorkRef.MAX_CANONICAL_BYTES+4L)+(branch>=2?34:0));
    }
    private static EnvelopeMaterial envelopeGenerationRuntimeCanonical(){
        return envelopeChoice(envelopeGenerationRuntimeCanonical(1),envelopeGenerationRuntimeCanonical(3));
    }
    private static EnvelopeMaterial envelopeGenerationRuntimeDecode(int branch){
        final long n=TargetGenerationRuntimeIndex.MAX_OBLIGATIONS,ref=TargetGenerationRuntimeIndex.MAX_OBLIGATION_BYTES;
        final long encoded=TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES
                -(branch==1?0:TargetTimelineWorkRef.MAX_CANONICAL_BYTES+4L)+(branch>=2?34:0);
        final long raw=1+5+1+1+(branch==1?TargetTimelineWorkRef.MAX_CANONICAL_BYTES:branch>=2?32:0)+n*ref+5+5+1+10+32;
        final var parsed=envelopeProtoRead(encoded,raw,Math.max(ref,branch==1?TargetTimelineWorkRef.MAX_CANONICAL_BYTES:32),9+n+(branch==0?0:1));
        var arguments=branch==1?envelopeNested(TargetTimelineWorkRef.MAX_CANONICAL_BYTES,envelopeBusinessWorkDecode()):
                branch>=2?envelopeArray(32):envelopeEmpty();
        arguments=envelopeHold(arguments,envelopeRepeat(envelopeNested(ref,envelopeObligationDecode()),n,true));
        var constructor=branch>=2?envelopeAssigned(32):envelopeEmpty();
        // validateObligations owns ID/key accessors and the copied key suffix once for each retained ref.
        final var check=envelopeReturn(envelopeTemporary(envelopeHold(envelopeArray(32),envelopeArray(46)),envelopeArray(32)),32);
        constructor=envelopeTemporary(constructor,envelopeCall(check,envelopeRepeat(check,n-1,false),0));
        if(branch==3)constructor=envelopeTemporary(constructor,envelopeArray(32));
        constructor=envelopeHold(constructor,envelopeHash(envelopeGenerationRuntimeFields(branch),
                "nereus-delay-target-generation-runtime\0".length()));
        var work=envelopeHold(parsed,envelopeCall(arguments,constructor,arguments.held));
        work=envelopeTemporary(work,envelopeArray(32));
        work=envelopeTemporary(work,envelopeGenerationRuntimeCanonical(branch));
        return envelopeReturn(work,arguments.held+(branch>=2?32:0)+32);
    }
    private static EnvelopeMaterial envelopeGenerationRuntimeDecode(){
        return envelopeChoice(envelopeChoice(envelopeGenerationRuntimeDecode(0),envelopeGenerationRuntimeDecode(1)),
                envelopeChoice(envelopeGenerationRuntimeDecode(2),envelopeGenerationRuntimeDecode(3)));
    }

    // ATTEMPT_MATERIAL_REGION: attempt, Claim and terminal codec graph insertion point.
    private static EnvelopeMaterial envelopeOwnerIdentityCanonical(){return envelopeOwnerIdentityCanonical(TargetQuotaClaimCharge.MAX_OWNER_BYTES);}
    private static EnvelopeMaterial envelopeOwnerIdentityCanonical(long encodedBound){return envelopeArray(encodedBound);}
    private static EnvelopeMaterial envelopeOwnerIdentityDecode(){return envelopeOwnerIdentityDecode(TargetQuotaClaimCharge.MAX_OWNER_BYTES);}
    private static EnvelopeMaterial envelopeOwnerIdentityDecode(long encodedBound){
        if(encodedBound<42)throw new IllegalArgumentException("Owner identity bound lacks its required fields");
        // The two variable IDs share this caller's complete Owner bound; neither gets the whole bound independently.
        final long raw=encodedBound-7,owned=encodedBound-8;
        final var parsed=envelopeProtoRead(encodedBound,raw,Math.max(32,encodedBound-41),4);
        final var args=envelopeArrays(owned,3);
        var frame=envelopeHold(parsed,envelopeCall(args,envelopeArrays(owned,3),0));
        frame=envelopeTemporary(frame,envelopeOwnerIdentityCanonical(encodedBound));
        return envelopeReturn(frame,owned);
    }
    private static EnvelopeMaterial envelopeRetryPolicyRefCanonical(long encodedBytes){return envelopeArray(encodedBytes);}
    private static EnvelopeMaterial envelopeRetryPolicyRefDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,3);
        final var args=envelopeArrays(encodedBytes,2); // policyId and semanticHash raw accessors
        var frame=envelopeHold(parsed,envelopeCall(args,envelopeArrays(encodedBytes,2),0));
        frame=envelopeTemporary(frame,envelopeRetryPolicyRefCanonical(encodedBytes));
        return envelopeReturn(frame,encodedBytes);
    }
    private static long envelopeChargeVectorBytes(){return 15L*11+2L*12;}
    private static EnvelopeMaterial envelopeChargeVectorCanonical(){return envelopeArray(envelopeChargeVectorBytes());}
    private static EnvelopeMaterial envelopeChargeVectorDecode(){
        // ChargeVector's 17 raw uint64 fields include the complete unsigned bit-pattern range.
        return envelopeReturn(envelopeTemporary(envelopeProtoRead(envelopeChargeVectorBytes(),170,10,17),
                envelopeChargeVectorCanonical()),0);
    }
    private static EnvelopeMaterial envelopeExternalDeliveryCanonical(){return envelopeArray(34);}
    private static EnvelopeMaterial envelopeExternalDeliveryDecode(){
        var frame=envelopeHold(envelopeProtoRead(34,32,32,1),envelopeCall(envelopeArray(32),envelopeArray(32),0));
        frame=envelopeTemporary(frame,envelopeExternalDeliveryCanonical());return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopePulsarSequenceCanonical(){return envelopeArray(81);}
    private static EnvelopeMaterial envelopePulsarSequenceDecode(){
        // The managed branch dominates the one-scalar producer-assigned branch.
        var frame=envelopeHold(envelopeProtoRead(81,75,32,4),
                envelopeCall(envelopeArrays(64,2),envelopeArrays(64,2),0));
        frame=envelopeTemporary(frame,envelopePulsarSequenceCanonical());return envelopeReturn(frame,64);
    }
    private static EnvelopeMaterial envelopeEvidenceChannelFields(long encodedBytes,boolean evidence){
        var fields=envelopeLegacyBrokerCanonical(encodedBytes);
        if(evidence)fields=envelopeDrop(fields,envelopeLegacyBrokerCanonical(encodedBytes));
        return fields;
    }
    private static EnvelopeMaterial envelopeEvidenceChannelCanonical(long encodedBytes,boolean evidence){
        return envelopeWritten(envelopeDrop(envelopeEvidenceChannelFields(encodedBytes,evidence),
                envelopeLegacyLeaseCanonical(encodedBytes)),encodedBytes);
    }
    private static EnvelopeMaterial envelopeEvidenceChannelCanonical(long encodedBytes){
        return envelopeChoice(envelopeEvidenceChannelCanonical(encodedBytes,false),envelopeEvidenceChannelCanonical(encodedBytes,true));
    }
    private static EnvelopeMaterial envelopeEvidenceChannelDecode(long encodedBytes,boolean evidence){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,evidence?17:15);
        var nested=envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes));
        if(evidence)nested=envelopeHold(nested,envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes)));
        nested=envelopeHold(nested,envelopeNested(encodedBytes,envelopeLegacyLeaseDecode(encodedBytes)));
        final var args=envelopeHold(nested,envelopeArrays(Math.addExact(encodedBytes,176),7));
        var constructor=envelopeArrays(Math.addExact(encodedBytes,176),7);
        constructor=envelopeTemporary(constructor,envelopeArray(32)); // single-input producer SHA-256
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(32),2,false)); // lease binding/fingerprint getters
        constructor=envelopeTemporary(constructor,envelopeHold(envelopeArray(32),envelopeHash(
                envelopeWritten(envelopeEvidenceChannelFields(encodedBytes,evidence),encodedBytes),
                "nereus-delay-credential-holder-destination-channel\0".length())));
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,nested.held));
        frame=envelopeTemporary(frame,envelopeEvidenceChannelCanonical(encodedBytes,evidence));
        return envelopeReturn(frame,Math.addExact(nested.held,constructor.held));
    }
    private static EnvelopeMaterial envelopeEvidenceChannelDecode(long encodedBytes){
        return envelopeChoice(envelopeEvidenceChannelDecode(encodedBytes,false),envelopeEvidenceChannelDecode(encodedBytes,true));
    }
    private static EnvelopeMaterial envelopeEvidenceCursorCanonical(long encodedBytes,boolean target,boolean pulsar){
        var fields=target?envelopeCursorScopeCanonical():envelopeEmpty();
        fields=envelopeDrop(fields,envelopeWritten(pulsar?envelopeArray(encodedBytes):envelopeEmpty(),encodedBytes));
        return envelopeWritten(fields,encodedBytes);
    }
    private static EnvelopeMaterial envelopeEvidenceCursorCanonical(long encodedBytes){
        return envelopeChoice(envelopeChoice(envelopeEvidenceCursorCanonical(encodedBytes,false,false),
                envelopeEvidenceCursorCanonical(encodedBytes,false,true)),envelopeChoice(
                envelopeEvidenceCursorCanonical(encodedBytes,true,false),envelopeEvidenceCursorCanonical(encodedBytes,true,true)));
    }
    private static EnvelopeMaterial envelopeEvidenceCursorDecode(long encodedBytes,boolean target,boolean pulsar){
        var frame=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,target?7:8);
        frame=envelopeHold(frame,envelopeNested(encodedBytes,envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,pulsar?7:3)));
        final long resource=pulsar?32:16;
        if(target){
            frame=envelopeHold(frame,envelopeArray(resource));
            frame=envelopeTemporary(frame,envelopeArray(resource)); // member resource comparison
        }else frame=envelopeHold(frame,envelopeArrays(48+resource,3));
        var args=target?envelopeNested(128,envelopeCursorScopeDecode()):envelopeEmpty();
        if(!target)args=envelopeHold(args,envelopeArray(resource)); // fixed(resource) before construction
        if(pulsar)args=envelopeTemporary(args,envelopeCall(envelopeArray(encodedBytes),envelopeReturn(envelopeArray(encodedBytes),0),0));
        var constructor=envelopeArrays((target?0:48)+2*resource,target?2:4);
        if(pulsar)constructor=envelopeTemporary(constructor,envelopeArray(encodedBytes)); // nfc UTF-8 round trip
        frame=envelopeHold(frame,envelopeCall(args,constructor,args.held));
        frame=envelopeTemporary(frame,envelopeEvidenceCursorCanonical(encodedBytes,target,pulsar));
        return envelopeReturn(frame,Math.addExact(args.held,constructor.held));
    }
    private static EnvelopeMaterial envelopeEvidenceCursorDecode(long encodedBytes){
        return envelopeChoice(envelopeChoice(envelopeEvidenceCursorDecode(encodedBytes,false,false),
                envelopeEvidenceCursorDecode(encodedBytes,false,true)),envelopeChoice(
                envelopeEvidenceCursorDecode(encodedBytes,true,false),envelopeEvidenceCursorDecode(encodedBytes,true,true)));
    }
    private static EnvelopeMaterial envelopeEvidenceCursorSequenceDecode(long totalEncodedBytes,long count){
        if(count==0)return envelopeEmpty();
        if(count<0)throw new IllegalArgumentException("negative EvidenceCursor count");
        // Typed Cursor graphs have only fixed field counts plus their aggregate topic/encoded bytes.
        // Place all variable bytes in one member and charge every other member's fixed graph once.
        final var fixed=envelopeEvidenceCursorDecode(0);
        final var expanded=envelopeEvidenceCursorDecode(totalEncodedBytes);
        final long prefix=Math.multiplyExact(count-1,fixed.held);
        return new EnvelopeMaterial(Math.addExact(Math.multiplyExact(count-1,fixed.allocated),expanded.allocated),
                Math.addExact(prefix,expanded.retained),Math.addExact(prefix,expanded.held),
                Math.addExact(Math.multiplyExact(count-1,fixed.fieldVisits),expanded.fieldVisits),
                Math.addExact(Math.multiplyExact(count-1,fixed.arrayObjects),expanded.arrayObjects));
    }
    private static EnvelopeMaterial envelopeEvidenceCursorSequenceCanonical(long totalEncodedBytes,long count){
        if(count==0)return envelopeEmpty();
        if(count<0)throw new IllegalArgumentException("negative EvidenceCursor count");
        final var fixed=envelopeEvidenceCursorCanonical(0);
        final var expanded=envelopeEvidenceCursorCanonical(totalEncodedBytes);
        return new EnvelopeMaterial(Math.addExact(Math.multiplyExact(count-1,fixed.allocated),expanded.allocated),
                expanded.retained,expanded.held,
                Math.addExact(Math.multiplyExact(count-1,fixed.fieldVisits),expanded.fieldVisits),
                Math.addExact(Math.multiplyExact(count-1,fixed.arrayObjects),expanded.arrayObjects));
    }
    private static EnvelopeMaterial envelopePublishEvidenceBranch(long encodedBytes,int branch,boolean secondGeneration){
        final int fields=switch(branch){case 0->8;case 1->8;case 2->5;case 3->secondGeneration?22:11;
            case 4->10;case 5->7;case 6->10;case 7->11;case 8->7;case 9->9;default->throw new IllegalArgumentException("unknown evidence branch");};
        var frame=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,fields);
        final var broker=envelopeNested(encodedBytes,envelopeLegacyBrokerDecode(encodedBytes));
        final var cursor=envelopeNested(encodedBytes,envelopeEvidenceCursorDecode(encodedBytes));
        final var external=envelopeNested(34,envelopeExternalDeliveryDecode());
        final var channel=envelopeNested(encodedBytes,envelopeChoice(envelopeEvidenceChannelDecode(encodedBytes),envelopeBusinessChannelDecode()));
        switch(branch){
            case 0->{frame=envelopeTemporary(frame,broker);frame=envelopeTemporary(frame,external);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),2,false));}
            case 1->{frame=envelopeTemporary(frame,cursor);frame=envelopeTemporary(frame,external);frame=envelopeTemporary(frame,broker);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),3,false));}
            case 2,5->{frame=envelopeTemporary(frame,cursor);frame=envelopeHold(frame,cursor);frame=envelopeTemporary(frame,channel);
                frame=envelopeTemporary(frame,external);frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),branch==2?2:3,false));}
            case 3->{frame=envelopeTemporary(frame,broker);
                if(secondGeneration){frame=envelopeHold(frame,external);frame=envelopeHold(frame,envelopeNested(81,envelopePulsarSequenceDecode()));}
                else frame=envelopeTemporary(frame,external);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),secondGeneration?8:3,false));}
            case 4->{frame=envelopeTemporary(frame,cursor);frame=envelopeTemporary(frame,external);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),4,false));}
            case 6->{frame=envelopeTemporary(frame,broker);frame=envelopeTemporary(frame,external);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),4,false));}
            case 7->{frame=envelopeTemporary(frame,envelopeNested(encodedBytes,envelopeLegacyProfileDecode(encodedBytes)));
                frame=envelopeTemporary(frame,external);frame=envelopeTemporary(frame,broker);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),2,false));frame=envelopeTemporary(frame,envelopeArray(64));}
            case 8->{frame=envelopeTemporary(frame,envelopeNested(encodedBytes,envelopeEvidenceChannelDecode(encodedBytes)));
                frame=envelopeTemporary(frame,external);frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),2,false));}
            case 9->{frame=envelopeHold(frame,broker);frame=envelopeTemporary(frame,external);
                frame=envelopeTemporary(frame,envelopeRepeat(envelopeArray(32),3,false));}
            default->throw new IllegalArgumentException("unknown evidence branch");
        }
        return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopePublishEvidenceBranch(long encodedBytes){
        var branches=envelopePublishEvidenceBranch(encodedBytes,0,false);
        for(int branch=1;branch<10;branch++)branches=envelopeChoice(branches,envelopePublishEvidenceBranch(encodedBytes,branch,false));
        return envelopeChoice(branches,envelopePublishEvidenceBranch(encodedBytes,3,true));
    }
    private static EnvelopeMaterial envelopePublishEvidenceDecode(long encodedBytes){
        final var parsed=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,4);
        final var args=envelopeHold(envelopeArray(32),envelopeArray(encodedBytes));
        var constructor=envelopeArray(32); // nonZero owns evidenceId
        constructor=envelopeHold(constructor,envelopeDrop(envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,22),envelopeArray(encodedBytes)));
        constructor=envelopeTemporary(constructor,envelopePublishEvidenceBranch(encodedBytes));
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,0));
        frame=envelopeTemporary(frame,envelopeHash(envelopeArrays(4,2),"nereus-delay-publish-evidence\0".length()+encodedBytes));
        frame=envelopeTemporary(frame,envelopeArray(encodedBytes)); // canonicalBytes, branch is an already-owned field
        return envelopeReturn(frame,Math.addExact(encodedBytes,32));
    }
    private static EnvelopeMaterial envelopePublishEvidenceOwnerCheck(long encodedBytes){
        // requireBusinessMutation/requireDlqMutation -> requireOwner -> externalIdentityBytes -> decode -> identity().
        var frame=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,22);
        frame=envelopeHold(frame,envelopeNested(34,envelopeExternalDeliveryDecode()));
        frame=envelopeTemporary(frame,envelopeArray(32));return envelopeReturn(frame,0);
    }
    private static EnvelopeMaterial envelopeOutcomeRetryDecode(long encodedBytes,boolean unknown){
        var frame=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,9);
        frame=envelopeHold(frame,envelopeLegacyNestedField(encodedBytes)); // policyBytes local and its generic Reader
        frame=envelopeTemporary(frame,envelopeRetryPolicyRefDecode(encodedBytes)); // validateRetryPolicyRef
        frame=envelopeHold(frame,envelopeRetryPolicyRefDecode(encodedBytes));
        frame=envelopeHold(frame,envelopeArray(encodedBytes)); // RetryDecision owns canonicalBytes
        final var definitive=envelopeReturn(frame,Math.multiplyExact(2L,encodedBytes));
        if(!unknown)return definitive;
        final var initial=envelopeProtoRead(encodedBytes,encodedBytes,encodedBytes,9);
        final var placeholder=envelopeReturn(envelopeHold(initial,envelopeArray(encodedBytes)),encodedBytes);
        return envelopeChoice(placeholder,envelopeReturn(envelopeHold(initial,definitive),definitive.held));
    }
    private static EnvelopeMaterial envelopePublishOutcomeValidation(long bodyBytes,boolean resolution,boolean proof){
        return envelopeLegacyValidation(bodyBytes,(resolution?6:5)+(proof?1:0),(resolution?5:4)+(proof?1:0));
    }
    private static EnvelopeMaterial envelopePublishOutcomeValidation(long bodyBytes,boolean resolution){
        return envelopeChoice(envelopePublishOutcomeValidation(bodyBytes,resolution,false),envelopePublishOutcomeValidation(bodyBytes,resolution,true));
    }
    private static EnvelopeMaterial envelopePublishOutcomeDecode(long bodyBytes,boolean resolution,boolean unknown,boolean proof){
        var frame=envelopeLegacyBodyFields(bodyBytes,envelopePublishOutcomeValidation(bodyBytes,resolution,proof));
        long typedHeld=0;
        if(resolution){final var cursor=envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeEvidenceCursorDecode(bodyBytes),0);
            frame=envelopeHold(frame,cursor);typedHeld=Math.addExact(typedHeld,cursor.held);}
        frame=envelopeHold(frame,envelopeArray(32)); // attemptId local
        frame=envelopeHold(frame,unknown?envelopeArray(0):envelopeLegacyNestedField(bodyBytes)); // UNKNOWN's absent evidence
        frame=envelopeHold(frame,envelopeLegacyNestedField(bodyBytes)); // transfer local, opaque for UNKNOWN
        if(!unknown)frame=envelopeTemporary(frame,envelopeChargeVectorDecode());
        final var observed=envelopeCall(envelopeLegacyNestedField(bodyBytes),envelopeLegacyTimeDecode(bodyBytes),0);
        frame=envelopeHold(frame,observed);typedHeld=Math.addExact(typedHeld,observed.held);
        frame=envelopeHold(frame,envelopeLegacyNestedField(bodyBytes)); // retryBytes local
        final var retry=envelopeOutcomeRetryDecode(bodyBytes,unknown);
        frame=envelopeHold(frame,retry);typedHeld=Math.addExact(typedHeld,retry.held);
        if(!unknown)frame=envelopeTemporary(frame,envelopeCall(envelopePublishEvidenceDecode(bodyBytes),envelopePublishEvidenceOwnerCheck(bodyBytes),0));
        if(proof){
            final var reference=envelopeCall(envelopeLegacyNestedField(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_CANONICAL_BYTES),
                    envelopeBusinessRecoveryRefDecode(),0);
            frame=envelopeHold(frame,reference);typedHeld=Math.addExact(typedHeld,reference.held);
            var check=envelopePublishEvidenceDecode(bodyBytes);
            check=envelopeTemporary(check,envelopeArray(32)); // recovery ref evidenceId comparison
            check=envelopeTemporary(check,envelopePublishEvidenceOwnerCheck(bodyBytes));
            frame=envelopeTemporary(frame,check);
        }
        final long owned=Math.addExact(32,Math.multiplyExact(unknown?1L:2L,bodyBytes));
        frame=envelopeHold(frame,envelopeArrays(owned,3)); // PublishOutcomeBody's attempt/evidence/transfer copies
        return envelopeReturn(frame,Math.addExact(typedHeld,owned));
    }
    private static EnvelopeMaterial envelopePublishOutcomeDecode(long bodyBytes,boolean resolution){
        var result=envelopeChoice(envelopePublishOutcomeDecode(bodyBytes,resolution,false,false),
                envelopePublishOutcomeDecode(bodyBytes,resolution,false,true));
        if(!resolution)result=envelopeChoice(result,envelopePublishOutcomeDecode(bodyBytes,false,true,false));
        return result;
    }
    private static EnvelopeMaterial envelopePublishOutcomeIdentity(long bodyBytes,boolean resolution){
        final var body=envelopePublishOutcomeDecode(bodyBytes,resolution);
        if(!resolution)return envelopeCall(body,envelopeArray(32),0);
        final long domain="nereus-delay-evidence-resolution-logical-id\0".length();
        final var logical=envelopeHold(envelopeArray(domain),envelopeCall(
                envelopePublishEvidenceDecode(bodyBytes),envelopeArray(32),0)); // evidenceId getter
        return envelopeCall(body,envelopeHash(logical,32),0);
    }
    private static long envelopeReservedMetadataBytes(){return 18+6+43+6+3L*34+10+2;}
    private static EnvelopeMaterial envelopeReservedMetadataCanonical(){
        return envelopeWritten(envelopeDrop(envelopeArray(16),envelopeArray(DelayMessageId.LENGTH)),envelopeReservedMetadataBytes());
    }
    private static EnvelopeMaterial envelopeReservedMetadataDecode(){
        final var parsed=envelopeProtoRead(envelopeReservedMetadataBytes(),16+5+41+5+96+9+1,41,9);
        var args=envelopeCall(envelopeArray(16),envelopeArray(16),0);
        args=envelopeHold(args,envelopeCall(envelopeArray(41),envelopeLegacyMessageIdDecode(),0));
        args=envelopeHold(args,envelopeArrays(96,3));
        var frame=envelopeHold(parsed,envelopeCall(args,envelopeArrays(96,3),57));
        frame=envelopeTemporary(frame,envelopeReservedMetadataCanonical());return envelopeReturn(frame,153);
    }
    private static EnvelopeMaterial envelopeAttemptPolicyHeadCanonical(){return envelopeArray(TargetClaimRecord.MAX_POLICY_HEAD_BYTES);}
    private static EnvelopeMaterial envelopeAttemptPolicyHeadDecode(){
        final var parsed=envelopeProtoRead(TargetClaimRecord.MAX_POLICY_HEAD_BYTES,64+10+9,32,4);
        var frame=envelopeHold(parsed,envelopeCall(envelopeArrays(64,2),envelopeArrays(64,2),0));
        frame=envelopeTemporary(frame,envelopeAttemptPolicyHeadCanonical());return envelopeReturn(frame,64);
    }
    private static long envelopeAttemptReceiptResourceBytes(){return 3L+CanonicalTargetPartition.MAX_CLUSTER_BYTES+2L*18+6+6+11;}
    private static EnvelopeMaterial envelopeAttemptReceiptResourceCanonical(){
        var fields=envelopeDrop(envelopeArray(CanonicalTargetPartition.MAX_CLUSTER_BYTES),envelopeArray(16));
        fields=envelopeDrop(fields,envelopeArray(16));return envelopeWritten(fields,envelopeAttemptReceiptResourceBytes());
    }
    private static EnvelopeMaterial envelopeAttemptReceiptResourceDecode(){
        final long cluster=CanonicalTargetPartition.MAX_CLUSTER_BYTES,n=envelopeAttemptReceiptResourceBytes();
        final var parsed=envelopeProtoRead(n,cluster+32+5+5+10,cluster,6);
        var args=envelopeCall(envelopeArray(cluster),envelopeReturn(envelopeArray(cluster),0),0); // UTF-8 round trip
        args=envelopeHold(args,envelopeReturn(envelopeArray(16),0)); // UUID has no retained byte array
        args=envelopeHold(args,envelopeCall(envelopeArray(16),envelopeArray(16),0));
        var constructor=envelopeReturn(envelopeArray(cluster),0); // canonicalText UTF-8 check
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,16));
        frame=envelopeTemporary(frame,envelopeAttemptReceiptResourceCanonical());return envelopeReturn(frame,16);
    }
    private static long envelopeKafkaReceiptBindingBytes(){return 2+2L*(3+com.nereusstream.delay.protocol.TargetKafkaReceiptBinding.MAX_TOPIC_BYTES)
            +3+envelopeAttemptReceiptResourceBytes()+6+6+10+2L*34;}
    private static EnvelopeMaterial envelopeKafkaReceiptBindingCanonical(){
        var fields=envelopeDrop(envelopeArray(com.nereusstream.delay.protocol.TargetKafkaReceiptBinding.MAX_TOPIC_BYTES),envelopeAttemptReceiptResourceCanonical());
        fields=envelopeDrop(fields,envelopeArray(com.nereusstream.delay.protocol.TargetKafkaReceiptBinding.MAX_TOPIC_BYTES));
        return envelopeWritten(fields,envelopeKafkaReceiptBindingBytes());
    }
    private static EnvelopeMaterial envelopeKafkaReceiptBindingDecode(){
        final long resource=envelopeAttemptReceiptResourceBytes(),topic=com.nereusstream.delay.protocol.TargetKafkaReceiptBinding.MAX_TOPIC_BYTES;
        final var parsed=envelopeProtoRead(envelopeKafkaReceiptBindingBytes(),1+2*topic+resource+5+5+9+64,resource,9);
        var text=envelopeCall(envelopeArray(topic),envelopeReturn(envelopeArray(topic),0),0);
        var args=envelopeHold(text,envelopeNested(resource,envelopeAttemptReceiptResourceDecode()));
        args=envelopeHold(args,text);args=envelopeHold(args,envelopeArrays(64,2));
        var constructor=envelopeAssigned(64,2);
        constructor=envelopeTemporary(constructor,envelopeArray(CanonicalTargetPartition.MAX_CLUSTER_BYTES));
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,16));
        frame=envelopeTemporary(frame,envelopeKafkaReceiptBindingCanonical());return envelopeReturn(frame,80);
    }
    private static long envelopePublicationBytes(boolean nativePublication){
        return nativePublication?com.nereusstream.delay.protocol.TargetNativePublicationBinding.MAX_CANONICAL_BYTES:
                com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding.MAX_CANONICAL_BYTES;
    }
    private static EnvelopeMaterial envelopePublicationFields(boolean nativePublication){
        var fields=envelopeDrop(envelopeMessageLocatorCanonical(),envelopeBusinessIdentityCanonical());
        fields=envelopeDrop(fields,envelopeBusinessChannelCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeLegacyProfileCanonical(TargetChannelIdentity.MAX_PROFILE_REF_BYTES),2,false));
        fields=envelopeDrop(fields,envelopeAdapterMetadataCanonical());
        fields=envelopeDrop(fields,envelopeReservedMetadataCanonical());
        if(nativePublication){fields=envelopeDrop(fields,envelopeBusinessNativeScopeCanonical());
            fields=envelopeDrop(fields,envelopeBusinessSnapshotCanonical());fields=envelopeDrop(fields,envelopeAttemptPolicyHeadCanonical());}
        return envelopeWritten(fields,envelopePublicationBytes(nativePublication)-35L);
    }
    private static EnvelopeMaterial envelopePublicationHash(boolean nativePublication){
        return envelopeHash(envelopePublicationFields(nativePublication),(nativePublication?
                "nereus-delay-target-native-publication\0":"nereus-delay-target-ordinary-publication\0").length());
    }
    private static EnvelopeMaterial envelopePublicationCanonical(boolean nativePublication){
        // canonicalBytes calls fields(), then preparedPublishHash calls fields() again.
        return envelopeWritten(envelopeDrop(envelopePublicationFields(nativePublication),envelopePublicationHash(nativePublication)),
                envelopePublicationBytes(nativePublication));
    }
    private static EnvelopeMaterial envelopeOrdinaryPublicationCanonical(){return envelopePublicationCanonical(false);}
    private static EnvelopeMaterial envelopeNativePublicationCanonical(){return envelopePublicationCanonical(true);}
    private static EnvelopeMaterial envelopePublicationConstructor(boolean nativePublication){
        // Four assigned hashes have zero guards; payloadSha256 is an ordinary fixed-length copy.
        var constructor=envelopeHold(envelopeAssigned(128,4),envelopeArray(32));
        constructor=envelopeTemporary(constructor,envelopeLegacySelfRoutingDecode());
        if(nativePublication){
            constructor=envelopeTemporary(constructor,envelopeArrays(32,2)); // scope/locator accounting incarnations
            constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeArray(32),envelopeArray(32)),2,false));
            constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopeArray(32))); // scope/snapshot digest
            constructor=envelopeTemporary(constructor,envelopeArrays(64,2)); // snapshot.headRef constructor; equals has no encoding
            constructor=envelopeTemporary(constructor,envelopeComparison(envelopeArray(32),envelopeArray(32))); // artifact digests
        }
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeLegacyProfileCanonical(TargetChannelIdentity.MAX_PROFILE_REF_BYTES),2,false));
        constructor=envelopeTemporary(constructor,envelopeAdapterMetadataCanonical());
        constructor=envelopeTemporary(constructor,envelopeCall(envelopeHash(envelopeBusinessIdentityCanonical(),
                "nereus-delay-target-partition\0".length()),envelopeArray(32),0)); // physical.id() owns the derived ID
        constructor=envelopeTemporary(constructor,envelopeArrays(32,2)); // locator/context accounting incarnation getters
        constructor=envelopeTemporary(constructor,envelopeArray(32)); // reservedMetadata.publishAttemptId()
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeArray(32),envelopeArray(32)),2,false));
        return constructor;
    }
    private static EnvelopeMaterial envelopePublicationDecode(boolean nativePublication){
        final long encoded=envelopePublicationBytes(nativePublication),profile=TargetChannelIdentity.MAX_PROFILE_REF_BYTES;
        final var parsed=envelopeProtoRead(encoded,encoded,encoded,nativePublication?26:21);
        var nested=envelopeNested(TargetMessageLocator.MAX_CANONICAL_BYTES,envelopeMessageLocatorDecode());
        nested=envelopeHold(nested,envelopeNested(CanonicalTargetPartition.MAX_CANONICAL_BYTES,envelopeBusinessIdentityDecode()));
        nested=envelopeHold(nested,envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode()));
        nested=envelopeHold(nested,envelopeRepeat(envelopeNested(profile,envelopeLegacyProfileDecode(profile)),2,true));
        nested=envelopeHold(nested,envelopeNested(TargetScheduleBinding.MAX_BODY_BYTES,envelopeAdapterMetadataDecode()));
        nested=envelopeHold(nested,envelopeNested(envelopeReservedMetadataBytes(),envelopeReservedMetadataDecode()));
        if(nativePublication){
            nested=envelopeHold(nested,envelopeNested(TargetNativePolicyScope.MAX_CANONICAL_BYTES,envelopeBusinessNativeScopeDecode()));
            nested=envelopeHold(nested,envelopeNested(TargetNativePolicySnapshot.MAX_CANONICAL_BYTES,envelopeBusinessSnapshotDecode()));
            nested=envelopeHold(nested,envelopeNested(TargetClaimRecord.MAX_POLICY_HEAD_BYTES,envelopeAttemptPolicyHeadDecode()));
        }
        final var args=envelopeHold(nested,envelopeArrays(160,5));
        var frame=envelopeHold(parsed,envelopeCall(args,envelopePublicationConstructor(nativePublication),nested.held));
        frame=envelopeTemporary(frame,envelopeComparison(envelopePublicationHash(nativePublication),envelopeArray(32)));
        frame=envelopeTemporary(frame,envelopePublicationCanonical(nativePublication));
        return envelopeReturn(frame,Math.addExact(nested.held,160));
    }
    private static EnvelopeMaterial envelopeOrdinaryPublicationDecode(){return envelopePublicationDecode(false);}
    private static EnvelopeMaterial envelopeNativePublicationDecode(){return envelopePublicationDecode(true);}
    private static long envelopeAttemptBytes(int phase){
        return TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES-(phase<3?4L+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES:0)-(phase<4?34L:0);
    }
    private static EnvelopeMaterial envelopeAttemptFields(int phase){
        var fields=envelopeDrop(envelopeMessageLocatorCanonical(),envelopeAccountingCanonical());
        fields=envelopeDrop(fields,envelopeRepeat(envelopeVectorCanonical(),2,false));
        fields=envelopeDrop(fields,envelopeRepeat(envelopeMutationCanonical(false),phase<3?1:2,false));
        return envelopeWritten(fields,envelopeAttemptBytes(phase)-35L);
    }
    private static EnvelopeMaterial envelopeAttemptCanonical(int phase){return envelopeWritten(envelopeAttemptFields(phase),envelopeAttemptBytes(phase));}
    private static EnvelopeMaterial envelopeAttemptCanonical(){
        return envelopeChoice(envelopeChoice(envelopeAttemptCanonical(1),envelopeAttemptCanonical(2)),
                envelopeChoice(envelopeAttemptCanonical(3),envelopeChoice(envelopeAttemptCanonical(4),envelopeAttemptCanonical(5))));
    }
    private static EnvelopeMaterial envelopeAttemptDecode(int phase){
        final long locator=TargetMessageLocator.MAX_CANONICAL_BYTES,vector=TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES;
        final long mutation=TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        final long raw=1+locator+96+TargetQuotaAccounting.MAX_CANONICAL_BYTES+9+2*vector+1+10+(phase<3?1:2)*mutation+(phase<4?0:32)+16+32;
        final var parsed=envelopeProtoRead(envelopeAttemptBytes(phase),raw,Math.max(mutation,Math.max(locator,vector)),phase<3?14:phase<4?15:16);
        var args=envelopeNested(locator,envelopeMessageLocatorDecode());args=envelopeHold(args,envelopeArrays(96,3));
        args=envelopeHold(args,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(vector,envelopeVectorDecode()),2,true));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(mutation,envelopeMutationDecode(false)),phase<3?1:2,true));
        if(phase>=4)args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeArray(16));
        final long fixed=phase<4?112:144;
        var constructor=envelopeAssigned(fixed,phase<4?4:5);
        constructor=envelopeTemporary(constructor,envelopeLegacySelfRoutingDecode()); // locator.messageId().routingId()
        if(phase>=3)constructor=envelopeTemporary(constructor,envelopeComparison(envelopeSourceCanonical(),envelopeSourceCanonical()));
        constructor=envelopeHold(constructor,envelopeHash(envelopeAttemptFields(phase),"nereus-delay-target-quota-attempt-budget\0".length()));
        final long nested=args.held-fixed;
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeAttemptCanonical(phase));
        return envelopeReturn(frame,nested+fixed+32);
    }
    private static EnvelopeMaterial envelopeAttemptDecode(){
        return envelopeChoice(envelopeChoice(envelopeAttemptDecode(1),envelopeAttemptDecode(2)),
                envelopeChoice(envelopeAttemptDecode(3),envelopeChoice(envelopeAttemptDecode(4),envelopeAttemptDecode(5))));
    }
    private static EnvelopeMaterial envelopeClaimChargeFields(){
        var fields=envelopeDrop(envelopeBusinessWorkCanonical(),envelopeIdentityCanonical());
        fields=envelopeDrop(fields,envelopeAccountingCanonical());fields=envelopeDrop(fields,envelopeOwnerIdentityCanonical());
        fields=envelopeDrop(fields,envelopeMutationCanonical(true));
        return envelopeWritten(fields,TargetQuotaClaimCharge.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeClaimChargeCanonical(){return envelopeWritten(envelopeClaimChargeFields(),TargetQuotaClaimCharge.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeClaimChargeDecode(){
        final long work=TargetTimelineWorkRef.MAX_CANONICAL_BYTES,owner=TargetQuotaClaimCharge.MAX_OWNER_BYTES,mutation=TargetQuotaMutation.MAX_CANONICAL_BYTES;
        final long raw=1+32+work+TargetQuotaIdentity.MAX_CANONICAL_BYTES+32+TargetQuotaAccounting.MAX_CANONICAL_BYTES+owner+16+10+9+9+32+mutation+16+32;
        final var parsed=envelopeProtoRead(TargetQuotaClaimCharge.MAX_CANONICAL_BYTES,raw,Math.max(owner,Math.max(work,mutation)),15);
        // ownerBytes is a retained local in decode, in addition to its Field bytes and decoded Owner object.
        var frame=envelopeHold(parsed,envelopeArray(owner));
        var args=envelopeArray(32);args=envelopeHold(args,envelopeNested(work,envelopeBusinessWorkDecode()));
        args=envelopeHold(args,envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode()));
        args=envelopeHold(args,envelopeArray(32));args=envelopeHold(args,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        args=envelopeHold(args,envelopeOwnerIdentityDecode());args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeArray(32));
        args=envelopeHold(args,envelopeNested(mutation,envelopeMutationDecode(true)));args=envelopeHold(args,envelopeArray(16));
        var constructor=envelopeAssigned(128,5);constructor=envelopeTemporary(constructor,envelopeOwnerIdentityCanonical());
        constructor=envelopeTemporary(constructor,envelopeArrays(32,2)); // identity/locator accounting-incarnation comparison
        constructor=envelopeTemporary(constructor,envelopeLegacySelfRoutingDecode());
        constructor=envelopeHold(constructor,envelopeHash(envelopeClaimChargeFields(),"nereus-delay-target-quota-claim-charge\0".length()));
        final long nested=args.held-128;
        frame=envelopeHold(frame,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeClaimChargeCanonical());
        return envelopeReturn(frame,nested+128+32);
    }
    private static EnvelopeMaterial envelopeClaimFields(){
        var fields=envelopeDrop(envelopeGenerationRuntimeCanonical(1),envelopeBusinessHeadCanonical());
        fields=envelopeDrop(fields,envelopeOwnerIdentityCanonical());fields=envelopeDrop(fields,envelopeMutationCanonical(true));
        return envelopeWritten(fields,TargetClaimRecord.MAX_V1_CANONICAL_BYTES-68L);
    }
    private static EnvelopeMaterial envelopeClaimIdentityFields(int version){
        var fields=envelopeClaimFields();
        if(version==2)fields=envelopeDrop(fields,envelopeAttemptPolicyHeadCanonical());
        return envelopeWritten(fields,TargetClaimRecord.MAX_V1_CANONICAL_BYTES-68L
                +(version==2?TargetClaimRecord.MAX_POLICY_HEAD_BYTES+2L:0));
    }
    private static EnvelopeMaterial envelopeClaimCanonical(int version){
        var fields=envelopeClaimFields();if(version==2)fields=envelopeDrop(fields,envelopeAttemptPolicyHeadCanonical());
        return envelopeWritten(fields,version==2?TargetClaimRecord.MAX_CANONICAL_BYTES:TargetClaimRecord.MAX_V1_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeClaimCanonical(){return envelopeChoice(envelopeClaimCanonical(1),envelopeClaimCanonical(2));}
    private static EnvelopeMaterial envelopeClaimHeadProjection(){
        // DUE/Native candidate construction dominates the alternative FIFO head key projection.
        final var prefix=envelopeTemporary(envelopeArray(44),envelopeArray(32));
        var args=envelopeHold(prefix,envelopeArray(8));args=envelopeHold(args,envelopeArray(DelayMessageId.LENGTH));
        args=envelopeHold(args,envelopeArray(4));
        final var expected=envelopeCall(args,envelopeArray(118),0);
        return envelopeReturn(envelopeHold(expected,envelopeArray(118)),0); // TargetHeadRef.key()
    }
    private static EnvelopeMaterial envelopeClaimDecode(int version){
        final long runtime=TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES,head=com.nereusstream.delay.protocol.TargetHeadRef.MAX_CANONICAL_BYTES;
        final long owner=TargetQuotaClaimCharge.MAX_OWNER_BYTES,mutation=TargetQuotaMutation.MAX_CANONICAL_BYTES;
        final long encoded=version==2?TargetClaimRecord.MAX_CANONICAL_BYTES:TargetClaimRecord.MAX_V1_CANONICAL_BYTES;
        final long raw=1+runtime+10+32+head+owner+16+10+9+9+10+mutation+64+(version==2?TargetClaimRecord.MAX_POLICY_HEAD_BYTES:0);
        var frame=envelopeHold(envelopeProtoRead(encoded,raw,Math.max(runtime,mutation),version==2?15:14),envelopeArray(owner));
        var args=envelopeNested(runtime,envelopeGenerationRuntimeDecode(1));args=envelopeHold(args,envelopeArray(32));
        args=envelopeHold(args,envelopeNested(head,envelopeBusinessHeadDecode()));args=envelopeHold(args,envelopeOwnerIdentityDecode());
        args=envelopeHold(args,envelopeArray(16));args=envelopeHold(args,envelopeNested(mutation,envelopeMutationDecode(true)));
        if(version==2)args=envelopeHold(args,envelopeNested(TargetClaimRecord.MAX_POLICY_HEAD_BYTES,envelopeAttemptPolicyHeadDecode()));
        var constructor=envelopeAssigned(48,2);constructor=envelopeTemporary(constructor,envelopeOwnerIdentityCanonical());
        constructor=envelopeTemporary(constructor,envelopeLegacySelfRoutingDecode());
        constructor=envelopeTemporary(constructor,envelopeClaimHeadProjection());
        if(version==2)constructor=envelopeTemporary(constructor,envelopeAttemptPolicyHeadCanonical());
        constructor=envelopeHold(constructor,envelopeHash(envelopeClaimIdentityFields(version),"nereus-delay-target-claim-id\0".length()));
        // The second hash also concatenates the already retained claimId, without allocating another claimId accessor.
        constructor=envelopeHold(constructor,envelopeHash(envelopeClaimIdentityFields(version),"nereus-delay-target-claim-record\0".length()+32));
        final long nested=args.held-48;
        frame=envelopeHold(frame,envelopeCall(args,constructor,nested));
        frame=envelopeTemporary(frame,envelopeClaimCanonical(version));return envelopeReturn(frame,nested+48+64);
    }
    private static EnvelopeMaterial envelopeClaimDecode(){return envelopeChoice(envelopeClaimDecode(1),envelopeClaimDecode(2));}
    private static EnvelopeMaterial envelopeTerminalFields(){
        var fields=envelopeDrop(envelopeMessageLocatorCanonical(),envelopeGenerationRuntimeCanonical(0));
        fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        return envelopeWritten(fields,TargetTerminalGenerationRecord.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeTerminalCanonical(){
        // canonicalBytes retains fields while hashing it, then emits that exact fields/hash pair.
        final var fields=envelopeTerminalFields();
        return envelopeWritten(envelopeHold(fields,envelopeHash(envelopeEmpty(),
                "nereus-delay-target-terminal\0".length()+fields.held)),TargetTerminalGenerationRecord.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeTerminalDecode(){
        final long locator=TargetMessageLocator.MAX_CANONICAL_BYTES,runtime=TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES;
        final long mutation=TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        final var parsed=envelopeProtoRead(TargetTerminalGenerationRecord.MAX_CANONICAL_BYTES,1+locator+10+5+runtime+mutation+16+32,Math.max(runtime,mutation),8);
        var args=envelopeNested(locator,envelopeMessageLocatorDecode());args=envelopeHold(args,envelopeNested(runtime,envelopeGenerationRuntimeDecode(0)));
        args=envelopeHold(args,envelopeNested(mutation,envelopeMutationDecode(false)));args=envelopeHold(args,envelopeArray(16));
        final var constructor=envelopeTemporary(envelopeAssigned(16),envelopeLegacySelfRoutingDecode());
        var frame=envelopeHold(parsed,envelopeCall(args,constructor,args.held-16));
        frame=envelopeTemporary(frame,envelopeTerminalCanonical());return envelopeReturn(frame,args.held);
    }

    // BUSINESS_MATERIAL_REGION: ordinary Target record codec graph insertion point.
    /* Ordinary business codecs. Every nested call below is a real codec/constructor call; parse
       totals bound the bytes carried by the actual enclosing message, not a NativeValue estimate. */
    private static EnvelopeMaterial envelopeBusinessDecoded(long encoded,long raw,long largest,long fields,
            EnvelopeMaterial arguments,EnvelopeMaterial constructor,EnvelopeMaterial canonical,long returned){
        var frame=envelopeHold(envelopeProtoRead(encoded,raw,largest,fields),envelopeCall(arguments,constructor,arguments.held));
        frame=envelopeTemporary(frame,canonical);return envelopeReturn(frame,returned);
    }
    private static EnvelopeMaterial envelopeBusinessFields(long encoded,EnvelopeMaterial children){return envelopeWritten(children,encoded-34);}
    private static EnvelopeMaterial envelopeBusinessHashedCanonical(long encoded,EnvelopeMaterial children){
        return envelopeWritten(envelopeBusinessFields(encoded,children),encoded);
    }
    private static EnvelopeMaterial envelopeBusinessHashedDecode(long encoded,long raw,long largest,long fields,
            EnvelopeMaterial nested,long rawArguments,long rawArrays,EnvelopeMaterial owned,EnvelopeMaterial children,String domain){
        final var arguments=envelopeHold(nested,envelopeArrays(rawArguments,rawArrays));
        final var constructor=envelopeHold(owned,envelopeHash(envelopeBusinessFields(encoded,children),domain.length()));
        var frame=envelopeHold(envelopeProtoRead(encoded,raw,largest,fields),envelopeCall(arguments,constructor,nested.held));
        frame=envelopeTemporary(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeBusinessHashedCanonical(encoded,children));
        return envelopeReturn(frame,nested.held+owned.held+32);
    }
    private static EnvelopeMaterial envelopeBusinessProfileCanonical(){return envelopeArray(TargetChannelIdentity.MAX_PROFILE_REF_BYTES);}
    private static EnvelopeMaterial envelopeBusinessProfileDecode(){
        final long id=TargetChannelIdentity.MAX_PROFILE_ID_BYTES,n=TargetChannelIdentity.MAX_PROFILE_REF_BYTES;
        return envelopeBusinessDecoded(n,id+32+11,id,4,envelopeArrays(id+32,2),envelopeArrays(id+32,2),envelopeBusinessProfileCanonical(),id+32);
    }
    private static EnvelopeMaterial envelopeBusinessTimeCanonical(){return envelopeArray(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES);}
    private static EnvelopeMaterial envelopeBusinessTimeCanonical(long encodedBound){return envelopeArray(encodedBound);}
    private static EnvelopeMaterial envelopeBusinessTimeDecode(long encodedBound){
        // Time itself has no source-id cap. The actual enclosing encoded value bounds the aggregate
        // source-id/hash/signature bytes; it is not a separate encodedBound allowance per field.
        final var parsed=envelopeProtoRead(encodedBound,encodedBound,encodedBound,10);
        // Three variable byte fields share the enclosing byte budget. fixed(hash/signature)
        // additionally copies the fixed 96 bytes before the constructor owns its three arrays.
        final var args=envelopeHold(envelopeArrays(encodedBound,3),envelopeArrays(96,2));
        var frame=envelopeHold(parsed,envelopeCall(args,envelopeArrays(encodedBound,3),0));
        frame=envelopeTemporary(frame,envelopeBusinessTimeCanonical(encodedBound));return envelopeReturn(frame,encodedBound);
    }
    private static EnvelopeMaterial envelopeBusinessTimeDecode(){
        final long id=TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES,n=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
        // TrustedUtc fixed(bytes(...)) and the constructor each own the evidence/signature arrays.
        final var fields=envelopeArrays(id+32+64,3);
        return envelopeBusinessDecoded(n,id+32+64+6L*10+5,id,10,envelopeHold(fields,envelopeArrays(32+64,2)),
                envelopeArrays(id+32+64,3),envelopeBusinessTimeCanonical(),id+32+64);
    }
    private static EnvelopeMaterial envelopeBusinessLeaseFields(){
        return envelopeWritten(envelopeDrop(envelopeBusinessProfileCanonical(),envelopeBusinessTimeCanonical()),TargetChannelIdentity.MAX_LEASE_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeBusinessLeaseDigest(){return envelopeHash(envelopeBusinessLeaseFields(),"nereus-delay-credential-use-lease\0".length());}
    private static EnvelopeMaterial envelopeBusinessLeaseCanonical(){
        // canonicalBytes writes Profile/issuedAt, then digest() writes those two children again.
        return envelopeWritten(envelopeDrop(envelopeDrop(envelopeBusinessProfileCanonical(),envelopeBusinessTimeCanonical()),
                envelopeBusinessLeaseDigest()),TargetChannelIdentity.MAX_LEASE_BYTES);
    }
    private static EnvelopeMaterial envelopeBusinessLeaseDecode(){
        final long profile=TargetChannelIdentity.MAX_PROFILE_REF_BYTES,time=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
        final var nested=envelopeHold(envelopeNested(profile,envelopeBusinessProfileDecode()),envelopeNested(time,envelopeBusinessTimeDecode()));
        final var arguments=envelopeHold(nested,envelopeArrays(96,3));
        var frame=envelopeHold(envelopeProtoRead(TargetChannelIdentity.MAX_LEASE_BYTES,profile+time+128+35,Math.max(profile,time),11),
                envelopeCall(arguments,envelopeArrays(96,3),nested.held));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeBusinessLeaseDigest()));
        frame=envelopeTemporary(frame,envelopeBusinessLeaseCanonical());return envelopeReturn(frame,nested.held+96);
    }
    private static long envelopeBusinessBrokerBytes(){return CanonicalTargetPartition.MAX_CANONICAL_BYTES-9L;}
    private static EnvelopeMaterial envelopeBusinessBrokerCanonical(){
        // Pulsar's larger oneof dominates Kafka: UTF8 cluster/topic, resource token, inner and outer messages.
        final long text=CanonicalTargetPartition.MAX_CLUSTER_BYTES+(long)CanonicalTargetPartition.MAX_PHYSICAL_TOPIC_BYTES;
        final var fields=envelopeArrays(text,2);
        return envelopeWritten(envelopeWritten(fields,envelopeBusinessBrokerBytes()-4),envelopeBusinessBrokerBytes());
    }
    private static EnvelopeMaterial envelopeBusinessBrokerDecode(){
        final long n=envelopeBusinessBrokerBytes(),text=CanonicalTargetPartition.MAX_CLUSTER_BYTES+(long)CanonicalTargetPartition.MAX_PHYSICAL_TOPIC_BYTES;
        var frame=envelopeHold(envelopeProtoRead(n,n-4,n-4,1),envelopeProtoRead(n,n-4,n-4,1));
        frame=envelopeHold(frame,envelopeNested(n-4,envelopeProtoRead(n-4,text+32+10,CanonicalTargetPartition.MAX_PHYSICAL_TOPIC_BYTES,4)));
        frame=envelopeHold(frame,envelopeArrays(text+32,3));
        frame=envelopeTemporary(frame,envelopeArrays(text,2));
        frame=envelopeHold(frame,envelopeArray(32)); // Pulsar constructor resource token ownership
        frame=envelopeTemporary(frame,envelopeArrays(text,2)); // nfc UTF-8 round trips
        frame=envelopeTemporary(frame,envelopeBusinessBrokerCanonical());return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeBusinessIdentityCanonical(){
        final var parts=envelopeHold(envelopeArray(1),envelopeCall(envelopeBusinessBrokerCanonical(),
                envelopeConcat(envelopeHold(envelopeArray(4),envelopeArray(envelopeBusinessBrokerBytes()))),0));
        return envelopeConcat(envelopeHold(parts,envelopeArray(4)));
    }
    private static EnvelopeMaterial envelopeBusinessIdentityDecode(){
        final var broker=envelopeNested(envelopeBusinessBrokerBytes(),envelopeBusinessBrokerDecode());
        var frame=envelopeTemporary(broker,envelopeArrays(CanonicalTargetPartition.MAX_CLUSTER_BYTES+(long)CanonicalTargetPartition.MAX_PHYSICAL_TOPIC_BYTES,2));
        frame=envelopeTemporary(frame,envelopeHold(envelopeArray(32),envelopeArray(32)));
        frame=envelopeTemporary(frame,envelopeBusinessIdentityCanonical());return envelopeReturn(frame,broker.held);
    }
    private static EnvelopeMaterial envelopeBusinessDeliveryCanonical(){return envelopeWritten(envelopeBusinessBrokerCanonical(),TargetDispatchCompatibility.MAX_CAPABILITY_BYTES);}
    private static EnvelopeMaterial envelopeBusinessDeliveryDecode(){
        final long n=TargetDispatchCompatibility.MAX_CAPABILITY_BYTES,b=envelopeBusinessBrokerBytes();
        return envelopeBusinessDecoded(n,b+113,b,12,envelopeHold(envelopeNested(b,envelopeBusinessBrokerDecode()),envelopeArrays(64,2)),
                envelopeArrays(64,2),envelopeBusinessDeliveryCanonical(),envelopeBusinessBrokerDecode().held+64);
    }
    private static EnvelopeMaterial envelopeBusinessDispatchFields(){return envelopeArray(32);}
    private static EnvelopeMaterial envelopeBusinessDispatchCanonical(){return envelopeBusinessHashedCanonical(TargetDispatchCompatibility.MAX_CANONICAL_BYTES,envelopeBusinessDispatchFields());}
    private static EnvelopeMaterial envelopeBusinessDispatchDecode(){
        final long n=TargetDispatchCompatibility.MAX_CANONICAL_BYTES,cap=TargetDispatchCompatibility.MAX_CAPABILITY_BYTES;
        var nested=envelopeNested(cap,envelopeBusinessDeliveryDecode());
        nested=envelopeHold(nested,envelopeCall(envelopeArray(cap),envelopeProtoRead(cap,cap,cap,12),0));
        var owned=envelopeHold(envelopeAssigned(64,2),envelopeBusinessDeliveryCanonical());
        owned=envelopeTemporary(owned,envelopeBusinessIdentityCanonical());
        owned=envelopeTemporary(owned,envelopeRepeat(envelopeCall(envelopeArray(32),envelopeAssigned(32),0),2,false));
        return envelopeBusinessHashedDecode(n,cap+32*4L+30,cap,9,nested,96,3,owned,envelopeBusinessDispatchFields(),"nereus-delay-target-dispatch-compatibility\0");
    }
    private static EnvelopeMaterial envelopeBusinessControlGroupCanonical(){return envelopeArray(36);}
    private static EnvelopeMaterial envelopeBusinessControlGroupDecode(){return envelopeBusinessDecoded(36,34,32,2,envelopeArray(32),envelopeAssigned(32),envelopeBusinessControlGroupCanonical(),32);}
    private static EnvelopeMaterial envelopeBusinessControlScopeFields(){
        return envelopeDrop(envelopeDrop(envelopeArray(32),envelopeShardCanonical()),
                envelopeRepeat(envelopeBusinessControlGroupCanonical(),TargetControlScope.MAX_CONTROL_GROUPS,false));
    }
    private static EnvelopeMaterial envelopeBusinessControlScopeCanonical(){return envelopeBusinessHashedCanonical(TargetControlScope.MAX_CANONICAL_BYTES,envelopeBusinessControlScopeFields());}
    private static EnvelopeMaterial envelopeBusinessControlScopeDecode(){
        final int groups=TargetControlScope.MAX_CONTROL_GROUPS,permits=TargetControlScope.MAX_PERMIT_GROUPS;
        final var nested=envelopeRepeat(envelopeNested(36,envelopeBusinessControlGroupDecode()),groups,true);
        final var owned=envelopeHold(envelopeArrays(32+16,2),envelopeRepeat(envelopeAssigned(32),permits,true));
        return envelopeBusinessHashedDecode(TargetControlScope.MAX_CANONICAL_BYTES,32+20+36L*groups+32L*permits+34,36,4L+groups+permits,nested,
                52+32L*permits,3L+permits,owned,envelopeBusinessControlScopeFields(),"nereus-delay-target-control-scope\0");
    }
    private static EnvelopeMaterial envelopeBusinessProducerIdentity(){
        final long scope=22+34+4+11+18+2+6+34+34;
        final var fields=envelopeWritten(envelopeDrop(envelopeShardCanonical(),envelopeArray(32)),scope);
        final var hash=envelopeHash(fields,"nereus-delay-target-producer\0".length());
        // Hex is a String; the actual final UTF8 producer identity is 10-byte prefix plus 64 hex bytes.
        return envelopeDrop(hash,envelopeArray(74));
    }
    private static EnvelopeMaterial envelopeBusinessChannelContextCanonical(){
        final var producer=envelopeBusinessProducerIdentity();
        final var fields=envelopeDrop(envelopeDrop(envelopeShardCanonical(),envelopeArray(32)),
                envelopeHold(producer,envelopeHash(envelopeArray(74),0)));
        return envelopeWritten(fields,TargetChannelIdentity.MAX_CONTEXT_BYTES);
    }
    private static EnvelopeMaterial envelopeBusinessChannelFields(){return envelopeDrop(envelopeBusinessChannelContextCanonical(),envelopeBusinessLeaseCanonical());}
    private static EnvelopeMaterial envelopeBusinessChannelCanonical(){return envelopeBusinessHashedCanonical(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelFields());}
    private static EnvelopeMaterial envelopeBusinessChannelDecode(){
        final long n=TargetChannelIdentity.MAX_CANONICAL_BYTES,lease=TargetChannelIdentity.MAX_LEASE_BYTES;
        // Context is built from the enclosing fields; there is no separate Context Reader.
        final var contextArguments=envelopeArrays(20+32+16+32*3L,6);
        var contextOwn=envelopeHold(envelopeAssigned(16+32*3L,4),envelopeArray(32));
        contextOwn=envelopeHold(contextOwn,envelopeArrays(32,2)); // shard prefix and RouteIncarnation copy
        final var context=envelopeCall(contextArguments,contextOwn,0);
        var nested=envelopeHold(context,envelopeNested(lease,envelopeBusinessLeaseDecode()));
        // decode explicitly pre-parses lease, Profile and time before invoking CredentialUseLease.decode.
        nested=envelopeHold(nested,envelopeCall(envelopeArray(lease),envelopeProtoRead(lease,lease,lease,11),0));
        nested=envelopeHold(nested,envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,
                envelopeProtoRead(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,TargetChannelIdentity.MAX_PROFILE_REF_BYTES,256,4)));
        nested=envelopeHold(nested,envelopeNested(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,
                envelopeProtoRead(TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES,256,10)));
        var constructor=envelopeReturn(envelopeBusinessChannelContextCanonical(),0);
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeBusinessChannelContextCanonical(),"nereus-delay-credential-holder-target-channel\0".length()));
        constructor=envelopeTemporary(constructor,envelopeArrays(TargetChannelIdentity.MAX_PROFILE_ID_BYTES+TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES+32,3));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeCall(envelopeArray(32),envelopeAssigned(32),0),3,false));
        return envelopeBusinessHashedDecode(n,n,lease,17,nested,0,0,constructor,envelopeBusinessChannelFields(),"nereus-delay-target-channel-identity\0");
    }
    private static EnvelopeMaterial envelopeBusinessHeadFields(){return envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH);}
    private static EnvelopeMaterial envelopeBusinessHeadCanonical(){return envelopeBusinessHashedCanonical(com.nereusstream.delay.protocol.TargetHeadRef.MAX_CANONICAL_BYTES,envelopeBusinessHeadFields());}
    private static EnvelopeMaterial envelopeBusinessHeadDecode(){
        final long key=120,n=com.nereusstream.delay.protocol.TargetHeadRef.MAX_CANONICAL_BYTES;
        // Candidate decoding creates target/token/Message slices and their typed owned copies;
        // only the target escapes into Head (the supplied Message is independently decoded).
        var decoded=envelopeArrays(32+21+com.nereusstream.delay.protocol.DelayMessageId.LENGTH,3);
        decoded=envelopeHold(decoded,envelopeArrays(32+21,2));
        decoded=envelopeHold(decoded,envelopeLegacyMessageIdDecode());
        decoded=envelopeReturn(decoded,32);
        final var owned=envelopeHold(envelopeArray(key),decoded);
        return envelopeBusinessHashedDecode(n,key+com.nereusstream.delay.protocol.DelayMessageId.LENGTH+48,key,5,envelopeLegacyMessageIdDecode(),
                key+com.nereusstream.delay.protocol.DelayMessageId.LENGTH,2,owned,envelopeBusinessHeadFields(),"nereus-delay-target-head\0");
    }
    private static EnvelopeMaterial envelopeBusinessDomainFields(){return envelopeRepeat(envelopeBusinessHeadCanonical(),2,false);}
    private static EnvelopeMaterial envelopeBusinessDomainCanonical(){return envelopeBusinessHashedCanonical(com.nereusstream.delay.protocol.TargetDomainState.MAX_CANONICAL_BYTES,envelopeBusinessDomainFields());}
    private static EnvelopeMaterial envelopeBusinessDomainDecode(){
        final long head=com.nereusstream.delay.protocol.TargetHeadRef.MAX_CANONICAL_BYTES,n=com.nereusstream.delay.protocol.TargetDomainState.MAX_CANONICAL_BYTES;
        final var nested=envelopeRepeat(envelopeNested(head,envelopeBusinessHeadDecode()),2,true);
        final var own=envelopeRepeat(envelopeAssigned(32),3,true);
        return envelopeBusinessHashedDecode(n,2*head+96+45,head,9,nested,96,3,own,envelopeBusinessDomainFields(),"nereus-delay-target-execution-domain\0");
    }
    private static EnvelopeMaterial envelopeBusinessQueueFields(){return envelopeDrop(envelopeArray(32),
            envelopeRepeat(envelopeBusinessDomainCanonical(),TargetQueueState.MAX_DOMAIN_SLOTS,false));}
    private static EnvelopeMaterial envelopeBusinessQueueCanonical(){return envelopeBusinessHashedCanonical(TargetQueueState.MAX_CANONICAL_BYTES,envelopeBusinessQueueFields());}
    private static EnvelopeMaterial envelopeBusinessQueueDecode(){
        final long domain=com.nereusstream.delay.protocol.TargetDomainState.MAX_CANONICAL_BYTES;
        final var nested=envelopeRepeat(envelopeNested(domain,envelopeBusinessDomainDecode()),TargetQueueState.MAX_DOMAIN_SLOTS,true);
        return envelopeBusinessHashedDecode(TargetQueueState.MAX_CANONICAL_BYTES,32+16+32+34+domain*TargetQueueState.MAX_DOMAIN_SLOTS,domain,
                8L+TargetQueueState.MAX_DOMAIN_SLOTS,nested,48,2,envelopeHold(envelopeArray(32),envelopeAssigned(16)),envelopeBusinessQueueFields(),"nereus-delay-target-queue-state\0");
    }
    private static EnvelopeMaterial envelopeBusinessWorkFields(){return envelopeDrop(envelopeDrop(envelopeMessageLocatorCanonical(),envelopeControlRefCanonical()),envelopeSourceCanonical());}
    private static EnvelopeMaterial envelopeBusinessWorkCanonical(){
        final long n=TargetTimelineWorkRef.MAX_CANONICAL_BYTES;
        return envelopeWritten(envelopeWritten(envelopeWritten(envelopeBusinessWorkFields(),n-68),n-34),n);
    }
    private static EnvelopeMaterial envelopeBusinessWorkDecode(){
        final long n=TargetTimelineWorkRef.MAX_CANONICAL_BYTES,locator=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES;
        var nested=envelopeHold(envelopeNested(locator,envelopeMessageLocatorDecode()),envelopeControlRefDecode());
        nested=envelopeHold(nested,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        final var semantic=envelopeHash(envelopeWritten(envelopeBusinessWorkFields(),n-79),"nereus-delay-target-work-semantic\0".length());
        final var instance=envelopeHash(envelopeWritten(envelopeWritten(envelopeBusinessWorkFields(),n-68),n-34),"nereus-delay-target-work-instance\0".length());
        var construct=envelopeHold(envelopeArray(21),envelopeHold(semantic,instance));
        construct=envelopeTemporary(construct,envelopeSourceBounded());
        construct=envelopeTemporary(construct,envelopeArrays(32+32+44+8+com.nereusstream.delay.protocol.DelayMessageId.LENGTH+4,6));
        construct=envelopeTemporary(construct,envelopeArray(TargetOrderBarrier.MAX_ORDERED_KEY_BYTES));
        var frame=envelopeHold(envelopeProtoRead(n,n,locator,14),envelopeArray(74)); // retained controlBytes local
        frame=envelopeHold(frame,envelopeCall(envelopeHold(nested,envelopeArray(21)),construct,nested.held));
        frame=envelopeTemporary(frame,envelopeArrays(64,2));frame=envelopeTemporary(frame,envelopeBusinessWorkCanonical());
        return envelopeReturn(frame,nested.held+21+64);
    }
    private static EnvelopeMaterial envelopeBusinessExpiryCanonical(){return envelopeBusinessHashedCanonical(TargetExpiryRef.MAX_CANONICAL_BYTES,envelopeMessageLocatorCanonical());}
    private static EnvelopeMaterial envelopeBusinessExpiryDecode(){
        final long locator=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES;
        return envelopeBusinessHashedDecode(TargetExpiryRef.MAX_CANONICAL_BYTES,locator+43,locator,4,envelopeNested(locator,envelopeMessageLocatorDecode()),
                0,0,envelopeEmpty(),envelopeMessageLocatorCanonical(),"nereus-delay-target-expiry\0");
    }
    private static EnvelopeMaterial envelopeBusinessMessageFields(boolean inline){
        final var payload=inline?envelopeEmpty():envelopePayloadRefCanonical();
        return envelopeDrop(envelopeDrop(envelopeDrop(envelopeMessageLocatorCanonical(),envelopeSourceCanonical()),payload),envelopeGenerationRuntimeCanonical());
    }
    private static EnvelopeMaterial envelopeBusinessMessageCanonical(){return envelopeChoice(
            envelopeBusinessHashedCanonical(TargetMessageRecord.MAX_CANONICAL_BYTES,envelopeBusinessMessageFields(true)),
            envelopeBusinessHashedCanonical(TargetMessageRecord.MAX_CANONICAL_BYTES,envelopeBusinessMessageFields(false)));}
    private static EnvelopeMaterial envelopeBusinessMessageDecodeBranch(boolean inline){
        final long locator=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES,
                source=com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,
                runtime=TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES;
        var nested=envelopeHold(envelopeNested(locator,envelopeMessageLocatorDecode()),envelopeNested(source,envelopeSourceDecode()));
        nested=envelopeHold(nested,envelopeNested(runtime,envelopeGenerationRuntimeDecode()));
        if(!inline)nested=envelopeHold(nested,envelopeNested(com.nereusstream.delay.protocol.TargetPayloadReference.MAX_CANONICAL_BYTES,envelopePayloadRefDecode()));
        final long payload=inline?TargetMessageRecord.MAX_INLINE_BYTES:0;
        var own=envelopeArray(payload);own=envelopeTemporary(own,envelopeSourceBounded());
        if(!inline)own=envelopeTemporary(own,envelopePayloadRefCanonical());
        own=envelopeTemporary(own,envelopeHold(envelopeArray(21),envelopeArray(21)));
        return envelopeBusinessHashedDecode(TargetMessageRecord.MAX_CANONICAL_BYTES,TargetMessageRecord.MAX_CANONICAL_BYTES,
                Math.max(runtime,inline?payload:com.nereusstream.delay.protocol.TargetPayloadReference.MAX_CANONICAL_BYTES),11,
                nested,payload,inline?1:0,own,envelopeBusinessMessageFields(inline),"nereus-delay-target-message\0");
    }
    private static EnvelopeMaterial envelopeBusinessMessageDecode(){return envelopeChoice(envelopeBusinessMessageDecodeBranch(true),envelopeBusinessMessageDecodeBranch(false));}
    private static EnvelopeMaterial envelopeBusinessOrderedDecode(){
        // TargetKeyCodec.decodeOrdered retains target/domain/token/Message, with independent
        // input slices and constructor copies. The enclosing caller owns the raw key itself.
        final long fields=32+32+21+com.nereusstream.delay.protocol.DelayMessageId.LENGTH;
        var frame=envelopeArrays(fields,4);
        frame=envelopeHold(frame,envelopeArrays(32+32+21,3));
        frame=envelopeHold(frame,envelopeLegacyMessageIdDecode());return envelopeReturn(frame,fields);
    }
    private static EnvelopeMaterial envelopeBusinessBarrierFields(){return envelopeDrop(envelopeMessageLocatorCanonical(),envelopeArray(TargetOrderBarrier.MAX_ORDERED_KEY_BYTES));}
    private static EnvelopeMaterial envelopeBusinessBarrierCanonical(){return envelopeBusinessHashedCanonical(TargetOrderBarrier.MAX_CANONICAL_BYTES,envelopeBusinessBarrierFields());}
    private static EnvelopeMaterial envelopeBusinessBarrierDecode(){
        final long locator=com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES;
        final var nested=envelopeHold(envelopeNested(locator,envelopeMessageLocatorDecode()),envelopeBusinessOrderedDecode());
        return envelopeBusinessHashedDecode(TargetOrderBarrier.MAX_CANONICAL_BYTES,locator+TargetOrderBarrier.MAX_ORDERED_KEY_BYTES+75,
                locator,5,nested,TargetOrderBarrier.MAX_ORDERED_KEY_BYTES+32,2,envelopeAssigned(32),envelopeBusinessBarrierFields(),"nereus-delay-target-order-barrier\0");
    }
    private static EnvelopeMaterial envelopeBusinessOrderFields(boolean barrier){
        return envelopeDrop(envelopeDrop(envelopeArray(32),envelopeShardCanonical()),envelopeDrop(
                envelopeArray(TargetOrderBarrier.MAX_ORDERED_KEY_BYTES),barrier?envelopeBusinessBarrierCanonical():envelopeBusinessHeadCanonical()));
    }
    private static EnvelopeMaterial envelopeBusinessOrderCanonical(){return envelopeChoice(
            envelopeBusinessHashedCanonical(TargetOrderState.MAX_CANONICAL_BYTES,envelopeBusinessOrderFields(true)),
            envelopeBusinessHashedCanonical(TargetOrderState.MAX_CANONICAL_BYTES,envelopeBusinessOrderFields(false)));}
    private static EnvelopeMaterial envelopeBusinessOrderDecodeBranch(boolean barrier){
        final long child=barrier?TargetOrderBarrier.MAX_CANONICAL_BYTES:com.nereusstream.delay.protocol.TargetHeadRef.MAX_CANONICAL_BYTES;
        final var nested=envelopeHold(envelopeBusinessOrderedDecode(),envelopeNested(child,barrier?envelopeBusinessBarrierDecode():envelopeBusinessHeadDecode()));
        var own=envelopeHold(envelopeArrays(32+16+32+16,4),envelopeArray(16));
        own=envelopeTemporary(own,envelopeArray(66));
        if(!barrier)own=envelopeTemporary(own,envelopeBusinessOrderedDecode());
        return envelopeBusinessHashedDecode(TargetOrderState.MAX_CANONICAL_BYTES,TargetOrderState.MAX_CANONICAL_BYTES,
                Math.max(child,TargetOrderBarrier.MAX_ORDERED_KEY_BYTES),14,nested,32+32+20+16+TargetOrderBarrier.MAX_ORDERED_KEY_BYTES,
                5,own,envelopeBusinessOrderFields(barrier),"nereus-delay-target-order-state\0");
    }
    private static EnvelopeMaterial envelopeBusinessOrderDecode(){return envelopeChoice(envelopeBusinessOrderDecodeBranch(true),envelopeBusinessOrderDecodeBranch(false));}
    private static EnvelopeMaterial envelopeBusinessMemberRegistration(){
        final var children=envelopeDrop(envelopeBusinessProfileCanonical(),envelopeDrop(
                envelopeRepeat(envelopeBusinessDispatchCanonical(),2,false),envelopeBusinessControlScopeCanonical()));
        var constructor=envelopeHold(envelopeAssigned(96,3),envelopeArray(TargetChannelIdentity.MAX_PROFILE_ID_BYTES));
        constructor=envelopeTemporary(constructor,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
        constructor=envelopeTemporary(constructor,envelopeArrays(64,2)); // canServe authorization getter comparisons
        return envelopeDrop(constructor,envelopeWritten(children,TargetMembershipGrant.MAX_REGISTRATION_BYTES));
    }
    private static EnvelopeMaterial envelopeBusinessRegistrationCheck(){
        final long n=TargetMembershipGrant.MAX_REGISTRATION_BYTES;
        var frame=envelopeProtoRead(n,n,TargetDispatchCompatibility.MAX_CANONICAL_BYTES,8);
        frame=envelopeHold(frame,envelopeNested(TargetDispatchCompatibility.MAX_CANONICAL_BYTES,envelopeBusinessDispatchDecode()));
        frame=envelopeHold(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeBusinessMemberRegistration());
        frame=envelopeTemporary(frame,envelopeArray(32));return envelopeReturn(frame,32);
    }
    private static EnvelopeMaterial envelopeBusinessMemberFields(){return envelopeSourceCanonical();}
    private static EnvelopeMaterial envelopeBusinessMemberCanonical(){return envelopeBusinessHashedCanonical(TargetMembershipGrant.MAX_CANONICAL_BYTES,envelopeBusinessMemberFields());}
    private static EnvelopeMaterial envelopeBusinessMemberDecode(){
        var nested=envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,envelopeBusinessProfileDecode());
        nested=envelopeHold(nested,envelopeRepeat(envelopeNested(TargetDispatchCompatibility.MAX_CANONICAL_BYTES,envelopeBusinessDispatchDecode()),2,true));
        nested=envelopeHold(nested,envelopeNested(TargetControlScope.MAX_CANONICAL_BYTES,envelopeBusinessControlScopeDecode()));
        nested=envelopeHold(nested,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        var own=envelopeHold(envelopeAssigned(128,4),envelopeBusinessMemberRegistration());
        own=envelopeTemporary(own,envelopeArray(TargetChannelIdentity.MAX_PROFILE_ID_BYTES));
        own=envelopeTemporary(own,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
        own=envelopeTemporary(own,envelopeSourceBounded());
        own=envelopeTemporary(own,envelopeCall(envelopeArray(TargetChannelIdentity.MAX_PROFILE_REF_BYTES),
                envelopeProtoRead(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,TargetChannelIdentity.MAX_PROFILE_REF_BYTES,256,4),0));
        return envelopeBusinessHashedDecode(TargetMembershipGrant.MAX_CANONICAL_BYTES,TargetMembershipGrant.MAX_CANONICAL_BYTES,
                TargetDispatchCompatibility.MAX_CANONICAL_BYTES,11,nested,128,4,own,envelopeBusinessMemberFields(),"nereus-delay-target-membership-grant\0");
    }
    private static EnvelopeMaterial envelopeBusinessPolicyFields(){return envelopeDrop(envelopeBusinessProfileCanonical(),
            envelopeDrop(envelopeBusinessDispatchCanonical(),envelopeBusinessControlScopeCanonical()));}
    private static EnvelopeMaterial envelopeBusinessPolicyCanonical(){return envelopeBusinessHashedCanonical(TargetMembershipPolicy.MAX_CANONICAL_BYTES,envelopeBusinessPolicyFields());}
    private static EnvelopeMaterial envelopeBusinessPolicyDecode(){
        var nested=envelopeNested(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,envelopeBusinessProfileDecode());
        nested=envelopeHold(nested,envelopeNested(TargetDispatchCompatibility.MAX_CANONICAL_BYTES,envelopeBusinessDispatchDecode()));
        nested=envelopeHold(nested,envelopeNested(TargetControlScope.MAX_CANONICAL_BYTES,envelopeBusinessControlScopeDecode()));
        nested=envelopeHold(nested,envelopeCall(envelopeArray(TargetChannelIdentity.MAX_PROFILE_REF_BYTES),
                envelopeProtoRead(TargetChannelIdentity.MAX_PROFILE_REF_BYTES,TargetChannelIdentity.MAX_PROFILE_REF_BYTES,256,4),0));
        var constructor=envelopeAssigned(96,3);constructor=envelopeTemporary(constructor,envelopeArray(TargetChannelIdentity.MAX_PROFILE_ID_BYTES));
        constructor=envelopeTemporary(constructor,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
        return envelopeBusinessHashedDecode(TargetMembershipPolicy.MAX_CANONICAL_BYTES,TargetMembershipPolicy.MAX_CANONICAL_BYTES,
                TargetDispatchCompatibility.MAX_CANONICAL_BYTES,9,nested,96,3,constructor,envelopeBusinessPolicyFields(),"nereus-delay-target-membership-policy\0");
    }
    private static EnvelopeMaterial envelopeBusinessArtifactsFields(){return envelopeEmpty();}
    private static EnvelopeMaterial envelopeBusinessArtifactsCanonical(){return envelopeBusinessHashedCanonical(com.nereusstream.delay.protocol.TargetNativeArtifactSet.MAX_CANONICAL_BYTES,envelopeBusinessArtifactsFields());}
    private static EnvelopeMaterial envelopeBusinessArtifactsDecode(){
        var constructor=envelopeAssigned(64,2);
        constructor=envelopeTemporary(constructor,envelopeCall(envelopeArray(com.nereusstream.delay.protocol.PulsarSourceLock.SOURCE_LOCK.length()),envelopeArray(32),0));
        return envelopeBusinessHashedDecode(com.nereusstream.delay.protocol.TargetNativeArtifactSet.MAX_CANONICAL_BYTES,64+32+15,32,8,envelopeEmpty(),64,2,constructor,
                envelopeBusinessArtifactsFields(),"nereus-delay-target-native-artifacts\0");}
    private static EnvelopeMaterial envelopeBusinessNativeScopeFields(){return envelopeDrop(envelopeDrop(envelopeShardCanonical(),envelopeArray(32)),envelopeBusinessArtifactsCanonical());}
    private static EnvelopeMaterial envelopeBusinessNativeScopeCanonical(){return envelopeBusinessHashedCanonical(TargetNativePolicyScope.MAX_CANONICAL_BYTES,envelopeBusinessNativeScopeFields());}
    private static EnvelopeMaterial envelopeBusinessNativeScopeDecode(){
        final long artifact=com.nereusstream.delay.protocol.TargetNativeArtifactSet.MAX_CANONICAL_BYTES;
        final var nested=envelopeNested(artifact,envelopeBusinessArtifactsDecode());
        final var own=envelopeHold(envelopeAssigned(32*4L+16,5),envelopeArrays(32+16,2));
        return envelopeBusinessHashedDecode(TargetNativePolicyScope.MAX_CANONICAL_BYTES,TargetNativePolicyScope.MAX_CANONICAL_BYTES,
                artifact,14,nested,32*5L+16+20,7,own,envelopeBusinessNativeScopeFields(),"nereus-delay-target-native-policy-scope\0");
    }
    private static EnvelopeMaterial envelopeBusinessSnapshotFields(){return envelopeWritten(envelopeBusinessTimeCanonical(),TargetNativePolicySnapshot.MAX_CANONICAL_BYTES-100L);}
    private static EnvelopeMaterial envelopeBusinessSnapshotCanonical(){return envelopeWritten(envelopeBusinessTimeCanonical(),TargetNativePolicySnapshot.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessSnapshotDecode(){
        final long n=TargetNativePolicySnapshot.MAX_CANONICAL_BYTES,time=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES;
        final var nested=envelopeNested(time,envelopeBusinessTimeDecode());
        var constructor=envelopeHold(envelopeAssigned(96,3),envelopeArray(64));
        constructor=envelopeTemporary(constructor,envelopeArray(256));
        constructor=envelopeTemporary(constructor,envelopeBusinessTimeCanonical());
        var frame=envelopeHold(envelopeProtoRead(n,n,time,13),envelopeCall(envelopeHold(nested,envelopeArrays(160,4)),constructor,nested.held));
        frame=envelopeTemporary(frame,envelopeCall(envelopeArray(time),envelopeProtoRead(time,time,256,10),0));
        frame=envelopeTemporary(frame,envelopeHash(envelopeHold(envelopeArray("nereus-delay-target-native-policy-snapshot\0".length()),envelopeBusinessSnapshotFields()),0));
        frame=envelopeTemporary(frame,envelopeBusinessSnapshotCanonical());return envelopeReturn(frame,nested.held+160);
    }
    private static EnvelopeMaterial envelopeBusinessBindingFields(boolean imported,boolean prefix){
        var fields=envelopeDrop(envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH),envelopeDrop(envelopeSourceCanonical(),envelopeArray(32)));
        if(imported)fields=envelopeDrop(fields,envelopeArray(com.nereusstream.delay.protocol.TargetMigrationRecordReference.MAX_CANONICAL_BYTES));
        if(prefix)fields=envelopeDrop(fields,envelopeMutationCanonical(false));return fields;
    }
    private static EnvelopeMaterial envelopeBusinessBindingCanonical(){return envelopeChoice(
            envelopeBusinessHashedCanonical(TargetScheduleBinding.LEGACY_MAX_CANONICAL_BYTES,envelopeBusinessBindingFields(false,false)),
            envelopeBusinessHashedCanonical(TargetScheduleBinding.MAX_CANONICAL_BYTES,envelopeBusinessBindingFields(true,true)));}
    private static EnvelopeMaterial envelopeBusinessBindingDecodeBranch(boolean imported,boolean prefix){
        final long n=imported?TargetScheduleBinding.MAX_CANONICAL_BYTES:TargetScheduleBinding.LEGACY_MAX_CANONICAL_BYTES;
        var nested=envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode());
        if(imported)nested=envelopeHold(nested,envelopeMigrationRecordDecode());
        if(prefix)nested=envelopeHold(nested,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        final var body=envelopeChoice(envelopeScheduleBodyDecode(),envelopePrepareBodyDecode());
        var own=envelopeHold(envelopeAssigned(16+32*5L,6),envelopeArrays(TargetScheduleBinding.MAX_BODY_BYTES+32+com.nereusstream.delay.protocol.DelayMessageId.LENGTH,3));
        own=envelopeTemporary(own,envelopeNested(TargetScheduleBinding.MAX_BODY_BYTES,body));
        return envelopeBusinessHashedDecode(n,n,TargetScheduleBinding.MAX_BODY_BYTES,18,nested,
                TargetScheduleBinding.MAX_BODY_BYTES+16+32*7L+com.nereusstream.delay.protocol.DelayMessageId.LENGTH,10,own,
                envelopeBusinessBindingFields(imported,prefix),prefix?"nereus-delay-target-migration-prefix-schedule-binding-v3\0":imported?"nereus-delay-target-migration-schedule-binding-v2\0":"nereus-delay-target-schedule-binding\0");
    }
    private static EnvelopeMaterial envelopeBusinessBindingDecode(){return envelopeChoice(envelopeBusinessBindingDecodeBranch(false,false),
            envelopeChoice(envelopeBusinessBindingDecodeBranch(true,false),envelopeBusinessBindingDecodeBranch(true,true)));}
    private static EnvelopeMaterial envelopeBusinessReservationFields(boolean committed){
        var fields=envelopeDrop(envelopeMessageLocatorCanonical(),envelopeArray(com.nereusstream.delay.protocol.CommandId.LENGTH));
        fields=envelopeDrop(fields,envelopeTupleCanonical());fields=envelopeDrop(fields,envelopeMutationCanonical(false));
        fields=envelopeDrop(fields,envelopeMutationCanonical(true));
        return envelopeDrop(fields,committed?envelopePayloadRefCanonical():envelopeArray(0));
    }
    private static EnvelopeMaterial envelopeBusinessReservationCanonicalBranch(boolean imported,boolean committed){
        final long n=TargetReservationRecord.MAX_CANONICAL_BYTES;
        final var fields=envelopeWritten(envelopeBusinessReservationFields(committed),n-34);
        final var evidence=committed?envelopeArray(0):envelopeReservationClosureCanonical();
        var core=envelopeHold(fields,evidence);
        if(imported)core=envelopeHold(core,envelopeMigrationReservationOriginCanonical());
        core=envelopeWritten(core,n-34);
        final var hash=envelopeHash(envelopeArray(n-34),
                (imported?"nereus-delay-target-migration-reservation-v2\0":"nereus-delay-target-reservation\0").length());
        return envelopeWritten(envelopeHold(core,hash),n);
    }
    private static EnvelopeMaterial envelopeBusinessReservationCanonical(){return envelopeChoice(envelopeBusinessReservationCanonicalBranch(false,true),
            envelopeChoice(envelopeBusinessReservationCanonicalBranch(false,false),envelopeChoice(
                    envelopeBusinessReservationCanonicalBranch(true,true),envelopeBusinessReservationCanonicalBranch(true,false))));}
    private static EnvelopeMaterial envelopeBusinessReservationDecodeBranch(boolean imported,boolean committed){
        final long n=TargetReservationRecord.MAX_CANONICAL_BYTES;
        var nested=envelopeNested(com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES,envelopeMessageLocatorDecode());
        nested=envelopeHold(nested,envelopeNested(26,envelopeTupleDecode()));
        nested=envelopeHold(nested,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        nested=envelopeHold(nested,envelopeNested(TargetQuotaMutation.MAX_CANONICAL_BYTES,envelopeMutationDecode(true)));
        nested=envelopeHold(nested,committed?envelopeNested(com.nereusstream.delay.protocol.TargetPayloadReference.MAX_CANONICAL_BYTES,envelopePayloadRefDecode())
                :envelopeReservationClosureDecode());
        if(imported)nested=envelopeHold(nested,envelopeMigrationReservationOriginDecode());
        var own=envelopeHold(envelopeAssigned(80,3),envelopeArray(com.nereusstream.delay.protocol.CommandId.LENGTH));
        own=envelopeTemporary(own,envelopePayloadRefCanonical());
        return envelopeBusinessDecoded(n,n,n,20,envelopeHold(nested,envelopeArrays(80+com.nereusstream.delay.protocol.CommandId.LENGTH,4)),own,
                envelopeBusinessReservationCanonicalBranch(imported,committed),nested.held+own.held);
    }
    private static EnvelopeMaterial envelopeBusinessReservationDecode(){return envelopeChoice(envelopeBusinessReservationDecodeBranch(false,true),
            envelopeChoice(envelopeBusinessReservationDecodeBranch(false,false),envelopeChoice(
                    envelopeBusinessReservationDecodeBranch(true,true),envelopeBusinessReservationDecodeBranch(true,false))));}
    private static EnvelopeMaterial envelopeBusinessReasonCanonical(){return envelopeArray(70);}
    private static EnvelopeMaterial envelopeBusinessReasonDecode(){return envelopeBusinessDecoded(70,65,32,3,envelopeArrays(64,2),envelopeArrays(64,2),envelopeBusinessReasonCanonical(),64);}
    private static EnvelopeMaterial envelopeBusinessClosePolicyCanonical(){
        final long acknowledgements=6L*com.nereusstream.delay.protocol.AcknowledgementKind.values().length;
        return envelopeWritten(envelopeDrop(envelopeBusinessReasonCanonical(),envelopeArray(acknowledgements)),
                com.nereusstream.delay.protocol.TargetCloseRequest.MAX_POLICY_BYTES);
    }
    private static EnvelopeMaterial envelopeBusinessClosePolicyDecode(){
        final long acknowledgements=6L*com.nereusstream.delay.protocol.AcknowledgementKind.values().length;
        final var children=envelopeHold(envelopeNested(70,envelopeBusinessReasonDecode()),
                envelopeNested(acknowledgements,envelopeProtoRead(acknowledgements,acknowledgements,5,com.nereusstream.delay.protocol.AcknowledgementKind.values().length)));
        return envelopeBusinessDecoded(com.nereusstream.delay.protocol.TargetCloseRequest.MAX_POLICY_BYTES,
                70+acknowledgements+6,Math.max(70,acknowledgements),4,children,envelopeEmpty(),envelopeBusinessClosePolicyCanonical(),children.held);
    }
    private static EnvelopeMaterial envelopeBusinessCloseShardCanonical(){return envelopeWritten(envelopeShardSubjectCanonical(),55);}
    private static EnvelopeMaterial envelopeBusinessCloseShardDecode(){
        final var subject=envelopeNested(24,envelopeShardSubjectDecode());
        var frame=envelopeHold(envelopeProtoRead(55,24+16+10,24,3),envelopeCall(envelopeHold(subject,envelopeArray(16)),envelopeAssigned(16),subject.held));
        frame=envelopeTemporary(frame,envelopeCall(envelopeArray(24),envelopeProtoRead(24,24,16,2),0));
        frame=envelopeTemporary(frame,envelopeBusinessCloseShardCanonical());return envelopeReturn(frame,subject.held+16);
    }
    private static EnvelopeMaterial envelopeBusinessCloseRequestCanonical(){
        final var children=envelopeDrop(envelopeArray(32),envelopeDrop(envelopeRepeat(envelopeBusinessCloseShardCanonical(),
                com.nereusstream.delay.protocol.TargetCloseRequest.MAX_SHARDS,false),envelopeBusinessClosePolicyCanonical()));
        return envelopeWritten(children,com.nereusstream.delay.protocol.TargetCloseRequest.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeBusinessCloseRequestDecode(){
        final int shards=com.nereusstream.delay.protocol.TargetCloseRequest.MAX_SHARDS;
        var children=envelopeRepeat(envelopeNested(55,envelopeBusinessCloseShardDecode()),shards,true);
        children=envelopeHold(children,envelopeNested(com.nereusstream.delay.protocol.TargetCloseRequest.MAX_POLICY_BYTES,envelopeBusinessClosePolicyDecode()));
        var construct=envelopeArray(32);construct=envelopeTemporary(construct,envelopeBusinessClosePolicyCanonical());
        construct=envelopeTemporary(construct,envelopeRepeat(envelopeComparison(envelopeShardSubjectCanonical(),envelopeShardSubjectCanonical()),shards-1,false));
        var frame=envelopeHold(envelopeProtoRead(com.nereusstream.delay.protocol.TargetCloseRequest.MAX_CANONICAL_BYTES,
                32+55L*shards+com.nereusstream.delay.protocol.TargetCloseRequest.MAX_POLICY_BYTES+1,com.nereusstream.delay.protocol.TargetCloseRequest.MAX_POLICY_BYTES,shards+3L),
                envelopeCall(envelopeHold(children,envelopeArray(32)),construct,children.held));
        frame=envelopeTemporary(frame,envelopeBusinessCloseRequestCanonical());return envelopeReturn(frame,children.held+32);
    }
    private static EnvelopeMaterial envelopeBusinessControlBodyCanonical(long body,long request,EnvelopeMaterial canonical,String domain,boolean requestRefCheck){
        var fields=envelopeDrop(envelopeShardSubjectCanonical(),envelopeControlRefCanonical());
        if(requestRefCheck){
            fields=envelopeDrop(fields,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
            fields=envelopeDrop(fields,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(canonical,request,18)));
        }
        final var semantic=envelopeHash(envelopeHold(envelopeArray(2),canonical),domain.length());
        fields=envelopeDrop(fields,semantic);
        fields=envelopeDrop(fields,envelopeWritten(canonical,request+5));return envelopeWritten(fields,body);
    }
    private static EnvelopeMaterial envelopeBusinessControlBodyDecode(long body,long request,EnvelopeMaterial decode,
            EnvelopeMaterial canonical,String domain,boolean requestRefCheck){
        var args=envelopeHold(envelopeNested(24,envelopeShardSubjectDecode()),envelopeNested(74,envelopeControlRefDecode()));
        args=envelopeHold(args,envelopeNested(request,decode));
        var frame=envelopeHold(envelopeProtoRead(body,body,request+5,9),args);
        frame=envelopeTemporary(frame,envelopeCall(envelopeArray(24),envelopeProtoRead(24,24,16,2),0));
        frame=envelopeTemporary(frame,envelopeCall(envelopeArray(74),envelopeProtoRead(74,74,32,3),0));
        frame=envelopeTemporary(frame,envelopeNested(request+5,envelopeProtoRead(request+5,request,request,1)));
        if(requestRefCheck){
            // Close checks the selected prior version, constructs the body, then encodes it;
            // each requireControlRef recomputes the complete prepared request hash.
            frame=envelopeTemporary(frame,envelopeRepeat(envelopeCall(envelopeArray(32),envelopeAssigned(32),0),2,false));
            frame=envelopeTemporary(frame,envelopeRepeat(envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(canonical,request,18)),2,false));
            frame=envelopeTemporary(frame,envelopeNested(74,envelopeControlRefDecode()));
        }else{
            frame=envelopeTemporary(frame,envelopeCall(envelopeArray(32),envelopeAssigned(32),0));
            frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopePreparedRequestHash(canonical,request,32)));
        }
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeHash(envelopeHold(envelopeArray(2),canonical),domain.length())));
        frame=envelopeTemporary(frame,envelopeBusinessControlBodyCanonical(body,request,canonical,domain,requestRefCheck));
        return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopeBusinessCloseBodyCanonical(){return envelopeBusinessControlBodyCanonical(
            com.nereusstream.delay.protocol.TargetCloseBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetCloseRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessCloseRequestCanonical(),"nereus-delay-target-close-control\0",true);}
    private static EnvelopeMaterial envelopeBusinessCloseBodyDecode(){return envelopeBusinessControlBodyDecode(
            com.nereusstream.delay.protocol.TargetCloseBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetCloseRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessCloseRequestDecode(),envelopeBusinessCloseRequestCanonical(),"nereus-delay-target-close-control\0",true);}
    private static EnvelopeMaterial envelopeBusinessCloseCanonical(){return envelopeBusinessHashedCanonical(com.nereusstream.delay.protocol.TargetCloseRecord.MAX_CANONICAL_BYTES,
            envelopeDrop(envelopeBusinessCloseBodyCanonical(),envelopeMutationCanonical(false)));}
    private static EnvelopeMaterial envelopeBusinessCloseDecode(){
        final var nested=envelopeHold(envelopeNested(com.nereusstream.delay.protocol.TargetCloseBody.MAX_CANONICAL_BYTES,envelopeBusinessCloseBodyDecode()),
                envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        return envelopeBusinessHashedDecode(com.nereusstream.delay.protocol.TargetCloseRecord.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetCloseRecord.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetCloseBody.MAX_CANONICAL_BYTES,
                6,nested,16,1,envelopeAssigned(16),envelopeDrop(envelopeBusinessCloseBodyCanonical(),envelopeMutationCanonical(false)),"nereus-delay-target-close-record\0");
    }
    private static EnvelopeMaterial envelopeBusinessCloseCursorFields(){return envelopeDrop(envelopeArray(32),envelopeMutationCanonical(true));}
    private static EnvelopeMaterial envelopeBusinessCloseCursorCanonical(){return envelopeBusinessHashedCanonical(com.nereusstream.delay.protocol.TargetCloseCursorRecord.MAX_CANONICAL_BYTES,envelopeBusinessCloseCursorFields());}
    private static EnvelopeMaterial envelopeBusinessCloseCursorDecode(){
        final var nested=envelopeNested(TargetQuotaMutation.MAX_CANONICAL_BYTES,envelopeMutationDecode(true));
        final long raw=32+32+16+com.nereusstream.delay.protocol.DelayMessageId.LENGTH;
        return envelopeBusinessHashedDecode(com.nereusstream.delay.protocol.TargetCloseCursorRecord.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetCloseCursorRecord.MAX_CANONICAL_BYTES,TargetQuotaMutation.MAX_CANONICAL_BYTES,9,nested,raw,4,
                envelopeHold(envelopeArray(32),envelopeAssigned(raw-32,3)),envelopeBusinessCloseCursorFields(),"nereus-delay-target-close-cursor\0");
    }
    private static EnvelopeMaterial envelopeBusinessMemberRequestCanonical(){return envelopeWritten(
            envelopeDrop(envelopeBusinessPolicyCanonical(),envelopeBusinessReasonCanonical()),com.nereusstream.delay.protocol.TargetMembershipControlRequest.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessMemberRequestDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMembershipControlRequest.MAX_CANONICAL_BYTES;
        final var policy=envelopeNested(TargetMembershipPolicy.MAX_CANONICAL_BYTES,envelopeBusinessPolicyDecode());
        final var issue=envelopeArray(TargetMembershipGrant.MAX_REGISTRATION_BYTES);
        final var close=envelopeHold(envelopeArray(32),envelopeNested(70,envelopeBusinessReasonDecode()));
        final var args=envelopeHold(policy,envelopeChoice(issue,close));
        var construct=envelopeArray(TargetMembershipGrant.MAX_REGISTRATION_BYTES);
        construct=envelopeTemporary(construct,envelopeChoice(envelopeBusinessRegistrationCheck(),envelopeAssigned(32)));
        return envelopeBusinessDecoded(n,n,TargetMembershipGrant.MAX_REGISTRATION_BYTES,3,args,construct,
                envelopeBusinessMemberRequestCanonical(),policy.held+Math.max(TargetMembershipGrant.MAX_REGISTRATION_BYTES,close.held));
    }
    private static EnvelopeMaterial envelopeBusinessMemberBodyCanonical(){return envelopeBusinessControlBodyCanonical(
            com.nereusstream.delay.protocol.TargetMembershipControlBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetMembershipControlRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessMemberRequestCanonical(),"nereus-delay-target-membership-control\0",false);}
    private static EnvelopeMaterial envelopeBusinessMemberBodyDecode(){
        var work=envelopeBusinessControlBodyDecode(
            com.nereusstream.delay.protocol.TargetMembershipControlBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetMembershipControlRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessMemberRequestDecode(),envelopeBusinessMemberRequestCanonical(),"nereus-delay-target-membership-control\0",false);
        work=envelopeTemporary(work,envelopeCall(envelopeArray(TargetMembershipGrant.MAX_REGISTRATION_BYTES),envelopeBusinessRegistrationCheck(),0));
        return envelopeTemporary(work,envelopeArray(32));
    }
    private static EnvelopeMaterial envelopeBusinessMemberCloseCanonical(){return envelopeBusinessHashedCanonical(
            com.nereusstream.delay.protocol.TargetMembershipClosureRecord.MAX_CANONICAL_BYTES,envelopeDrop(envelopeBusinessMemberBodyCanonical(),envelopeMutationCanonical(false)));}
    private static EnvelopeMaterial envelopeBusinessMemberCloseDecode(){
        final long n=com.nereusstream.delay.protocol.TargetMembershipClosureRecord.MAX_CANONICAL_BYTES,body=com.nereusstream.delay.protocol.TargetMembershipControlBody.MAX_CANONICAL_BYTES;
        final var nested=envelopeHold(envelopeNested(body,envelopeBusinessMemberBodyDecode()),
                envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        return envelopeBusinessHashedDecode(n,n,body,5,nested,16,1,envelopeAssigned(16),
                envelopeDrop(envelopeBusinessMemberBodyCanonical(),envelopeMutationCanonical(false)),"nereus-delay-target-membership-closure\0");
    }
    private static EnvelopeMaterial envelopeBusinessNativeRequestCanonical(){return envelopeWritten(
            envelopeDrop(envelopeBusinessNativeScopeCanonical(),envelopeChoice(envelopeBusinessSnapshotCanonical(),envelopeBusinessReasonCanonical())),
            com.nereusstream.delay.protocol.TargetNativePolicyControlRequest.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessNativeRequestDecode(){
        final long n=com.nereusstream.delay.protocol.TargetNativePolicyControlRequest.MAX_CANONICAL_BYTES;
        final var scope=envelopeNested(TargetNativePolicyScope.MAX_CANONICAL_BYTES,envelopeBusinessNativeScopeDecode());
        final var activate=envelopeNested(TargetNativePolicySnapshot.MAX_CANONICAL_BYTES,envelopeBusinessSnapshotDecode());
        final var reason=envelopeNested(70,envelopeBusinessReasonDecode());
        // INSTALL carries encoded Ed25519 bytes; CLOSE_MEMBER carries a grant digest and reason.
        final var other=envelopeChoice(envelopeHold(envelopeArray(44),envelopeArray(44)),envelopeHold(envelopeAssigned(32),reason));
        final var args=envelopeHold(scope,envelopeChoice(activate,other));
        return envelopeBusinessDecoded(n,n,TargetNativePolicyScope.MAX_CANONICAL_BYTES,4,args,envelopeEmpty(),
                envelopeBusinessNativeRequestCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopeBusinessNativeBodyCanonical(){return envelopeBusinessControlBodyCanonical(
            com.nereusstream.delay.protocol.TargetNativePolicyControlBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetNativePolicyControlRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessNativeRequestCanonical(),"nereus-delay-target-native-policy-control\0",false);}
    private static EnvelopeMaterial envelopeBusinessNativeBodyDecode(){return envelopeBusinessControlBodyDecode(
            com.nereusstream.delay.protocol.TargetNativePolicyControlBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetNativePolicyControlRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessNativeRequestDecode(),envelopeBusinessNativeRequestCanonical(),"nereus-delay-target-native-policy-control\0",false);}
    private static EnvelopeMaterial envelopeBusinessNativeControlFields(){return envelopeDrop(envelopeBusinessNativeBodyCanonical(),
            envelopeDrop(envelopeMutationCanonical(false),envelopeDrop(envelopeControlCanonical(),envelopeBusinessMemberCanonical())));}
    private static EnvelopeMaterial envelopeBusinessNativeControlCanonical(){return envelopeBusinessHashedCanonical(
            com.nereusstream.delay.protocol.TargetNativePolicyControlRecord.MAX_CANONICAL_BYTES,envelopeBusinessNativeControlFields());}
    private static EnvelopeMaterial envelopeBusinessNativeControlDecode(){
        final long n=com.nereusstream.delay.protocol.TargetNativePolicyControlRecord.MAX_CANONICAL_BYTES,body=com.nereusstream.delay.protocol.TargetNativePolicyControlBody.MAX_CANONICAL_BYTES;
        var args=envelopeNested(body,envelopeBusinessNativeBodyDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(104,envelopeControlDecode()));
        args=envelopeHold(args,envelopeNested(TargetMembershipGrant.MAX_CANONICAL_BYTES,envelopeBusinessMemberDecode()));
        return envelopeBusinessHashedDecode(n,n,Math.max(body,TargetMembershipGrant.MAX_CANONICAL_BYTES),7,args,16,1,envelopeAssigned(16),
                envelopeBusinessNativeControlFields(),"nereus-delay-target-native-control-record\0");
    }
    private static EnvelopeMaterial envelopeBusinessChannelRequestCanonical(){return envelopeWritten(envelopeDrop(envelopeBusinessChannelCanonical(),
            envelopeDrop(envelopeOwnerIdentityCanonical(TargetQuotaClaimCharge.MAX_OWNER_BYTES),envelopeBusinessReasonCanonical())),com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessChannelRequestDecode(){
        final long n=com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES;
        var args=envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaClaimCharge.MAX_OWNER_BYTES,envelopeOwnerIdentityDecode(TargetQuotaClaimCharge.MAX_OWNER_BYTES)));
        args=envelopeHold(args,envelopeNested(70,envelopeBusinessReasonDecode()));
        args=envelopeHold(args,envelopeArrays(TargetQuotaClaimCharge.MAX_OWNER_BYTES+32,2));
        var constructor=envelopeAssigned(32);constructor=envelopeTemporary(constructor,envelopeOwnerIdentityCanonical(TargetQuotaClaimCharge.MAX_OWNER_BYTES));
        constructor=envelopeTemporary(constructor,envelopeArray(32));
        return envelopeBusinessDecoded(n,n,Math.max(TargetChannelIdentity.MAX_CANONICAL_BYTES,TargetQuotaClaimCharge.MAX_OWNER_BYTES),5,args,constructor,
                envelopeBusinessChannelRequestCanonical(),args.held+32);
    }
    private static EnvelopeMaterial envelopeBusinessChannelBodyCanonical(){return envelopeBusinessControlBodyCanonical(
            com.nereusstream.delay.protocol.TargetChannelControlBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessChannelRequestCanonical(),"nereus-delay-target-channel-control\0",false);}
    private static EnvelopeMaterial envelopeBusinessChannelBodyDecode(){return envelopeBusinessControlBodyDecode(
            com.nereusstream.delay.protocol.TargetChannelControlBody.MAX_CANONICAL_BYTES,com.nereusstream.delay.protocol.TargetChannelControlRequest.MAX_CANONICAL_BYTES,
            envelopeBusinessChannelRequestDecode(),envelopeBusinessChannelRequestCanonical(),"nereus-delay-target-channel-control\0",false);}
    private static EnvelopeMaterial envelopeBusinessChannelControlFields(){return envelopeDrop(envelopeBusinessChannelBodyCanonical(),envelopeDrop(envelopeMutationCanonical(false),envelopeControlCanonical()));}
    private static EnvelopeMaterial envelopeBusinessChannelControlCanonical(){return envelopeBusinessHashedCanonical(
            com.nereusstream.delay.protocol.TargetChannelControlRecord.MAX_CANONICAL_BYTES,envelopeBusinessChannelControlFields());}
    private static EnvelopeMaterial envelopeBusinessChannelControlDecode(){
        final long n=com.nereusstream.delay.protocol.TargetChannelControlRecord.MAX_CANONICAL_BYTES,body=com.nereusstream.delay.protocol.TargetChannelControlBody.MAX_CANONICAL_BYTES;
        var args=envelopeNested(body,envelopeBusinessChannelBodyDecode());
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(104,envelopeControlDecode()));
        return envelopeBusinessHashedDecode(n,n,body,6,args,16,1,envelopeAssigned(16),envelopeBusinessChannelControlFields(),"nereus-delay-target-channel-control-record\0");
    }
    private static EnvelopeMaterial envelopeBusinessChannelSlotFields(){return envelopeDrop(envelopeBusinessChannelCanonical(),envelopeMutationCanonical(false));}
    private static EnvelopeMaterial envelopeBusinessChannelSlotCanonical(){return envelopeBusinessHashedCanonical(
            com.nereusstream.delay.protocol.TargetChannelSlot.MAX_CANONICAL_BYTES,envelopeBusinessChannelSlotFields());}
    private static EnvelopeMaterial envelopeBusinessChannelSlotDecode(){
        final var args=envelopeHold(envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode()),
                envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        return envelopeBusinessHashedDecode(com.nereusstream.delay.protocol.TargetChannelSlot.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetChannelSlot.MAX_CANONICAL_BYTES,TargetChannelIdentity.MAX_CANONICAL_BYTES,7,args,48,2,envelopeAssigned(48,2),
                envelopeBusinessChannelSlotFields(),"nereus-delay-target-channel-slot-record\0");
    }
    private static EnvelopeMaterial envelopeBusinessMappingFields(){return envelopeWritten(envelopeDrop(envelopePublishAdmissionCanonical(),envelopeMutationCanonical(false)),
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.MAX_CANONICAL_BYTES-34L);}
    private static EnvelopeMaterial envelopeBusinessMappingCanonical(){
        final var hash=envelopeHash(envelopeBusinessMappingFields(),"nereus-delay-target-kafka-receipt-mapping\0".length());
        return envelopeConcat(envelopeHold(envelopeBusinessMappingFields(),envelopeWritten(hash,34)));
    }
    private static EnvelopeMaterial envelopeBusinessMappingDecode(){
        final long n=com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.MAX_CANONICAL_BYTES;
        final var args=envelopeHold(envelopeNested(com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_KAFKA_RECEIPT_BYTES,envelopePublishAdmissionDecode()),
                envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var frame=envelopeHold(envelopeProtoRead(n,n,com.nereusstream.delay.protocol.TargetPublishAdmissionBody.MAX_KAFKA_RECEIPT_BYTES,7),
                envelopeCall(envelopeHold(args,envelopeArrays(48,2)),envelopeAssigned(48,2),args.held));
        frame=envelopeTemporary(frame,envelopeComparison(envelopeArray(32),envelopeHash(envelopeBusinessMappingFields(),"nereus-delay-target-kafka-receipt-mapping\0".length())));
        frame=envelopeTemporary(frame,envelopeBusinessMappingCanonical());return envelopeReturn(frame,args.held+48);
    }
    private static EnvelopeMaterial envelopeBusinessKafkaSlotCanonical(){return envelopeWritten(envelopeDrop(envelopeBusinessChannelCanonical(),envelopeMutationCanonical(false)),
            com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessKafkaSlotDecode(){
        final var args=envelopeHold(envelopeNested(TargetChannelIdentity.MAX_CANONICAL_BYTES,envelopeBusinessChannelDecode()),
                envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        return envelopeBusinessDecoded(com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.MAX_CANONICAL_BYTES,
                com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.MAX_CANONICAL_BYTES,TargetChannelIdentity.MAX_CANONICAL_BYTES,6,
                envelopeHold(args,envelopeArrays(48,2)),envelopeAssigned(48,2),envelopeBusinessKafkaSlotCanonical(),args.held+48);
    }
    private static EnvelopeMaterial envelopeBusinessEvidenceOwnerCanonical(){
        final var fields=envelopeDrop(envelopeArray(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.MAX_KEY_BYTES),
                envelopeDrop(envelopeIncarnationCanonical(true),envelopeDrop(envelopeSourceCanonical(),envelopeMutationCanonical(false))));
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeBusinessEvidenceOwnerDecode(){
        final long n=com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.MAX_CANONICAL_BYTES,key=com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.MAX_KEY_BYTES;
        var nested=envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true));
        nested=envelopeHold(nested,envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode()));
        nested=envelopeHold(nested,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        var own=envelopeHold(envelopeAssigned(96,3),envelopeArray(32));
        own=envelopeTemporary(own,envelopeRepeat(envelopeArray(key),2,false));
        return envelopeBusinessDecoded(n,n,Math.max(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,
                Math.max(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES)),13,
                envelopeHold(nested,envelopeArrays(key+128,5)),own,envelopeBusinessEvidenceOwnerCanonical(),nested.held+128);
    }
    private static EnvelopeMaterial envelopeBusinessEvidenceReferenceCanonical(){return envelopeWritten(
            envelopeDrop(envelopeIdentityCanonical(),envelopeDrop(envelopeAccountingCanonical(),envelopeMutationCanonical(false))),
            com.nereusstream.delay.protocol.TargetExternalEvidenceReference.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessEvidenceReferenceDecode(){
        var nested=envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode());
        nested=envelopeHold(nested,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        nested=envelopeHold(nested,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        final long n=com.nereusstream.delay.protocol.TargetExternalEvidenceReference.MAX_CANONICAL_BYTES;
        return envelopeBusinessDecoded(n,n,Math.max(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,TargetQuotaAccounting.MAX_CANONICAL_BYTES),9,
                envelopeHold(nested,envelopeArrays(112,4)),envelopeAssigned(112,4),envelopeBusinessEvidenceReferenceCanonical(),nested.held+112);
    }
    private static EnvelopeMaterial envelopeBusinessRecoveryRefCanonical(){return envelopeWritten(
            envelopeArray(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES),com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessRecoveryRefDecode(){
        final long n=com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_CANONICAL_BYTES,key=com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES;
        var own=envelopeAssigned(160,5);own=envelopeTemporary(own,envelopeRepeat(envelopeArray(key),2,false));
        return envelopeBusinessDecoded(n,key+160+12,key,9,envelopeArrays(key+160,6),own,envelopeBusinessRecoveryRefCanonical(),160);
    }
    private static EnvelopeMaterial envelopeBusinessRecoveryOwnerCanonical(){return envelopeWritten(
            envelopeDrop(envelopeBusinessRecoveryRefCanonical(),envelopeDrop(envelopeIncarnationCanonical(true),
                    envelopeDrop(envelopeRepeat(envelopeMutationCanonical(false),2,false),envelopeBusinessTimeCanonical(
                            com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.MAX_CANONICAL_BYTES)))),
            com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessRecoveryOwnerDecode(){
        final long n=com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.MAX_CANONICAL_BYTES;
        var nested=envelopeNested(com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_CANONICAL_BYTES,envelopeBusinessRecoveryRefDecode());
        nested=envelopeHold(nested,envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)));
        nested=envelopeHold(nested,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),2,true));
        nested=envelopeHold(nested,envelopeNested(n,envelopeBusinessTimeDecode(n)));
        return envelopeBusinessDecoded(n,n,Math.max(n,TargetQuotaIncarnation.MAX_CANONICAL_BYTES),11,envelopeHold(nested,envelopeArrays(128,4)),envelopeAssigned(128,4),
                envelopeBusinessRecoveryOwnerCanonical(),nested.held+128);
    }
    private static EnvelopeMaterial envelopeBusinessRecoveryReferenceCanonical(){return envelopeWritten(
            envelopeDrop(envelopeIdentityCanonical(),envelopeDrop(envelopeAccountingCanonical(),envelopeMutationCanonical(false))),
            com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.MAX_CANONICAL_BYTES);}
    private static EnvelopeMaterial envelopeBusinessRecoveryReferenceDecode(){
        var nested=envelopeNested(TargetQuotaIdentity.MAX_CANONICAL_BYTES,envelopeIdentityDecode());
        nested=envelopeHold(nested,envelopeNested(TargetQuotaAccounting.MAX_CANONICAL_BYTES,envelopeAccountingDecode()));
        nested=envelopeHold(nested,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        final long n=com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.MAX_CANONICAL_BYTES;
        return envelopeBusinessDecoded(n,n,Math.max(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,TargetQuotaAccounting.MAX_CANONICAL_BYTES),11,
                envelopeHold(nested,envelopeArrays(208,7)),envelopeAssigned(208,7),envelopeBusinessRecoveryReferenceCanonical(),nested.held+208);
    }
    private static EnvelopeMaterial envelopeBusinessRetiringFields(){return envelopeWritten(
            envelopeDrop(envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.MAX_INTENT_BYTES),
                    envelopeDrop(envelopeMutationCanonical(false),envelopeEvidenceRetirementCopyCanonical())),
            com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.MAX_CANONICAL_BYTES-34L);}
    private static EnvelopeMaterial envelopeBusinessRetiringCanonical(){
        final var hash=envelopeHash(envelopeArray(com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.MAX_CANONICAL_BYTES-34L),
                "nereus-delay-target-evidence-retiring-record\0".length());
        return envelopeConcat(envelopeHold(envelopeBusinessRetiringFields(),envelopeWritten(hash,34)));
    }
    private static EnvelopeMaterial envelopeBusinessRetiringDecode(){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.MAX_CANONICAL_BYTES,
                source=com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.MAX_INTENT_BYTES,
                body=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES,
                copy=com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_CANONICAL_BYTES;
        var args=envelopeNested(source,envelopeSystemDecode(source,body,1<<20,envelopeEvidenceRetireValidation(),envelopeEvidenceRetireIdentity(),false));
        args=envelopeHold(args,envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)));
        args=envelopeHold(args,envelopeNested(copy,envelopeEvidenceRetirementCopyDecode()));
        var construct=envelopeCall(envelopeArray(body),envelopeEvidenceRetireIdentity(),0);
        construct=envelopeTemporary(construct,envelopeArray(1<<20));
        construct=envelopeTemporary(construct,envelopeSystemCanonical(source));
        construct=envelopeTemporary(construct,envelopeComparison(envelopeEvidenceResourceCanonical(),envelopeEvidenceResourceCanonical()));
        construct=envelopeTemporary(construct,envelopeHash(envelopeSystemCanonical(source),0));
        construct=envelopeTemporary(construct,envelopeComparison(envelopeEvidenceContextCanonical(),envelopeEvidenceContextCanonical()));
        construct=envelopeTemporary(construct,envelopeEvidenceCopyContextCheck());
        var frame=envelopeHold(envelopeProtoRead(n,n,Math.max(source,copy),6),envelopeCall(args,construct,args.held));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeBusinessRetiringCanonical());
        return envelopeReturn(frame,args.held);
    }
    private static long envelopeBusinessConfirmedBodyBytes(){return (long)com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES
            +com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES
            +com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.MAX_DELETE_EVIDENCE_BYTES
            +com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.MAX_TIME_BYTES+2048;}
    private static EnvelopeMaterial envelopeBusinessConfirmedFields(){
        // fields() decodes the actual retained body to choose marker-retained schema before writing it.
        return envelopeWritten(envelopeDrop(envelopeCall(envelopeArray(envelopeBusinessConfirmedBodyBytes()),envelopeEvidenceConfirmedBodyDecode(),0),
                envelopeDrop(envelopeSystemCanonical(com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CONFIRMATION_BYTES),
                        envelopeRepeat(envelopeMutationCanonical(false),2,false))),com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CANONICAL_BYTES-34L);
    }
    private static EnvelopeMaterial envelopeBusinessConfirmedCanonical(){return envelopeConcat(envelopeHold(envelopeBusinessConfirmedFields(),
            envelopeWritten(envelopeHash(envelopeArray(com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CANONICAL_BYTES-34L),
                    "nereus-delay-target-evidence-confirmed-record\0".length()),34)));}
    private static EnvelopeMaterial envelopeBusinessConfirmedDecode(){
        final long n=com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CANONICAL_BYTES,
                source=com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CONFIRMATION_BYTES,body=envelopeBusinessConfirmedBodyBytes();
        var args=envelopeNested(source,envelopeSystemDecode(source,body,1<<20,envelopeEvidenceConfirmedValidation(),envelopeEvidenceConfirmedIdentity(),false));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false)),2,true));
        var construct=envelopeCall(envelopeArray(body),envelopeEvidenceConfirmedIdentity(),0);
        construct=envelopeTemporary(construct,envelopeArray(1<<20));
        construct=envelopeTemporary(construct,envelopeNested(1<<20,envelopeAuthorDecode(1<<20)));
        construct=envelopeTemporary(construct,envelopeSystemCanonical(source));
        construct=envelopeTemporary(construct,envelopeHash(envelopeSystemCanonical(source),0));
        var frame=envelopeHold(envelopeProtoRead(n,n,source,6),envelopeCall(args,construct,args.held));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeBusinessConfirmedCanonical());
        return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopePayloadObjectResourceCanonical(long encodedBound){
        return envelopeWritten(envelopeLegacyProfileCanonical(encodedBound),encodedBound);
    }
    private static EnvelopeMaterial envelopePayloadObjectResourceDecode(long encodedBound){
        final var profile=envelopeNested(encodedBound,envelopeLegacyProfileDecode(encodedBound));
        // container/key/version/optional etag/hash share the actual enclosing encoded budget.
        final var args=envelopeHold(profile,envelopeArrays(encodedBound,5));
        var frame=envelopeHold(envelopeProtoRead(encodedBound,encodedBound,encodedBound,7),
                envelopeCall(args,envelopeArrays(encodedBound,5),profile.held));
        frame=envelopeTemporary(frame,envelopePayloadObjectResourceCanonical(encodedBound));
        return envelopeReturn(frame,profile.held+encodedBound);
    }
    private static EnvelopeMaterial envelopeResourceFloorFields(long encodedBound){
        final long cursors=encodedBound/2;
        return envelopeWritten(envelopeDrop(envelopeSourceCanonical(),
                envelopeEvidenceCursorSequenceCanonical(encodedBound,cursors)),encodedBound);
    }
    private static EnvelopeMaterial envelopeResourceFloorCanonical(long encodedBound){
        return envelopeWritten(envelopeResourceFloorFields(encodedBound),encodedBound);
    }
    private static EnvelopeMaterial envelopeResourceFloorDecode(long encodedBound){
        // RecoveryFloor has seven mandatory fields; each additional cursor consumes at least a
        // tag and length byte from this exact enclosing budget. Cursor variable bytes aggregate.
        final long cursors=encodedBound/2;
        var args=envelopeNested(com.nereusstream.delay.protocol.TargetSourcePosition.MAX_CANONICAL_BYTES,envelopeSourceDecode());
        args=envelopeHold(args,envelopeCall(envelopeArrays(encodedBound,cursors),
                envelopeEvidenceCursorSequenceDecode(encodedBound,cursors),0));
        args=envelopeHold(args,envelopeArrays(64,3));
        var constructor=envelopeArrays(64,3);
        constructor=envelopeHold(constructor,envelopeHash(envelopeResourceFloorFields(encodedBound),"nereus-delay-recovery-floor-ref\0".length()));
        var frame=envelopeHold(envelopeProtoRead(encodedBound,encodedBound,encodedBound,7+cursors),
                envelopeCall(args,constructor,args.held-64));
        frame=envelopeTemporary(frame,envelopeArray(32));
        frame=envelopeTemporary(frame,envelopeResourceFloorCanonical(encodedBound));
        return envelopeReturn(frame,args.held+32);
    }
    private static long envelopeResourceFrameBytes(){return com.nereusstream.delay.protocol.TargetInlinePayloadSourceFacts.MAX_BYTES;}
    private static EnvelopeMaterial envelopeResourceFenceSystem(){
        final long frame=envelopeResourceFrameBytes(),body=com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES;
        return envelopeSystemDecode(frame,body,frame,envelopeTimeFenceDecode(),
                envelopeCall(envelopeTimeFenceDecode(),envelopeReturn(envelopeArray(32),0),0),false);
    }
    private static EnvelopeMaterial envelopeResourceFrameCanonical(){
        return envelopeWritten(envelopeMutationCanonical(false),envelopeResourceFrameBytes());
    }
    private static EnvelopeMaterial envelopeResourceFrameDecode(){return ResourceFrameMaterial.DECODE;}
    private static EnvelopeMaterial calculateEnvelopeResourceFrameDecode(){
        final long frame=envelopeResourceFrameBytes();
        var args=envelopeNested(TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES,envelopeMutationDecode(false));
        args=envelopeHold(args,envelopeArray(frame));
        var system=envelopeProtoRead(frame,frame,frame,2);
        system=envelopeHold(system,envelopeNested(frame,envelopeProtoRead(frame,frame,frame,11)));
        system=envelopeTemporary(system,envelopeSystemNonReclaimDecode(frame));
        system=envelopeTemporary(system,envelopeHash(envelopeSystemNonReclaimCanonical(frame),0));
        final var command=envelopeCommandFrameDecode(frame);
        var constructor=envelopeArray(frame);
        constructor=envelopeTemporary(constructor,envelopeSourceBounded());
        constructor=envelopeTemporary(constructor,envelopeChoice(command,system));
        constructor=envelopeTemporary(constructor,envelopeArray(32));
        var result=envelopeHold(envelopeProtoRead(frame,frame,frame,3),envelopeCall(args,constructor,args.held-frame));
        result=envelopeTemporary(result,envelopeResourceFrameCanonical());return envelopeReturn(result,args.held);
    }
    private static EnvelopeMaterial envelopeResourceSourceFactsCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetInlinePayloadSourceFacts.MAX_BYTES;
        final int count=com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS;
        final var retained=envelopeWritten(envelopeDrop(envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH),
                envelopeResourceFrameCanonical()),n);
        return envelopeWritten(envelopeDrop(envelopeResourceFrameCanonical(),envelopeRepeat(retained,count,false)),n);
    }
    private static EnvelopeMaterial envelopeResourceSourceFactsDecode(){
        final long n=com.nereusstream.delay.protocol.TargetInlinePayloadSourceFacts.MAX_BYTES;
        final int count=com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS;
        var retainedArgs=envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH);
        retainedArgs=envelopeHold(retainedArgs,envelopeNested(n,envelopeResourceFrameDecode()));
        var retained=envelopeHold(envelopeProtoRead(n,n,n,2),envelopeCall(retainedArgs,
                envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH),retainedArgs.held-com.nereusstream.delay.protocol.DelayMessageId.LENGTH));
        retained=envelopeTemporary(retained,envelopeCommandFrameDecode(n));
        var args=envelopeHold(envelopeNested(n,envelopeResourceFrameDecode()),envelopeRepeat(envelopeNested(n,retained),count,true));
        var constructor=envelopeReturn(envelopeResourceFenceSystem(),0);
        constructor=envelopeTemporary(constructor,envelopeCall(envelopeArray(com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES),envelopeTimeFenceDecode(),0));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH),count,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeWritten(envelopeDrop(envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH),envelopeResourceFrameCanonical()),n),count,false));
        constructor=envelopeTemporary(constructor,envelopeResourceFrameCanonical());
        var result=envelopeHold(envelopeProtoRead(n,n,n,count+2L),envelopeCall(args,constructor,args.held));
        result=envelopeTemporary(result,envelopeResourceSourceFactsCanonical());return envelopeReturn(result,args.held);
    }
    private static EnvelopeMaterial envelopeResourceMigrationEvidenceCanonical(){
        return envelopeWritten(envelopeArray(com.nereusstream.delay.protocol.TargetMigrationRecordReference.MAX_CANONICAL_BYTES),
                com.nereusstream.delay.protocol.TargetMigrationRecordReference.MAX_CANONICAL_BYTES+48L);
    }
    private static EnvelopeMaterial envelopeResourceMigrationHistoryCanonical(){
        return envelopeWritten(envelopeDrop(envelopeMigrationOriginCanonical(),envelopeRepeat(envelopeResourceMigrationEvidenceCanonical(),9,false)),
                com.nereusstream.delay.protocol.TargetMigrationPayloadHistory.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeResourceMigrationHistoryDecode(){
        final long entry=com.nereusstream.delay.protocol.TargetMigrationRecordReference.MAX_CANONICAL_BYTES+48L;
        var evidence=envelopeHold(envelopeProtoRead(entry,entry,entry,3),envelopeCall(
                envelopeHold(envelopeMigrationRecordDecode(),envelopeArray(32)),envelopeAssigned(32),envelopeMigrationRecordDecode().held));
        var args=envelopeHold(envelopeRepeat(envelopeNested(entry,evidence),9,true),
                envelopeNested(com.nereusstream.delay.protocol.TargetMigrationOrigin.MAX_CANONICAL_BYTES,envelopeMigrationOriginDecode()));
        args=envelopeHold(args,envelopeArray(32));
        var constructor=envelopeAssigned(32);constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResourceMigrationEvidenceCanonical(),9,false));
        return envelopeBusinessDecoded(com.nereusstream.delay.protocol.TargetMigrationPayloadHistory.MAX_BYTES,
                com.nereusstream.delay.protocol.TargetMigrationPayloadHistory.MAX_BYTES,entry,12,args,constructor,
                envelopeResourceMigrationHistoryCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopeResourceQuotaRefCanonical(){return envelopeWritten(envelopeChargeVectorCanonical(),271);}
    private static EnvelopeMaterial envelopeResourceQuotaRefDecode(){
        var args=envelopeHold(envelopeArrays(64,2),envelopeNested(envelopeChargeVectorBytes(),envelopeChargeVectorDecode()));
        var constructor=envelopeArrays(64,2);
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeHold(envelopeArray(8),envelopeChargeVectorCanonical()),32+"nereus-delay-quota-grant-semantic\0".length()));
        return envelopeBusinessDecoded(271,263,189,4,args,constructor,envelopeResourceQuotaRefCanonical(),64);
    }
    private static EnvelopeMaterial envelopeResourceRoutePartitionCanonical(long encoded){
        return envelopeWritten(envelopeDrop(envelopeLegacyActivationBarrierCanonical(encoded),envelopeResourceQuotaRefCanonical()),encoded);
    }
    private static EnvelopeMaterial envelopeResourceRoutePartitionDecode(long encoded){
        var args=envelopeHold(envelopeNested(encoded,envelopeLegacyActivationBarrierDecode(encoded)),envelopeNested(271,envelopeResourceQuotaRefDecode()));
        args=envelopeHold(args,envelopeArray(32));
        return envelopeBusinessDecoded(encoded,encoded,encoded,5,args,envelopeArray(32),envelopeResourceRoutePartitionCanonical(encoded),args.held);
    }
    private static EnvelopeMaterial envelopeResourceRoutePartitions(long total,long count,boolean decode){
        if(count==0)return envelopeEmpty();
        final var fixed=decode?envelopeResourceRoutePartitionDecode(0):envelopeResourceRoutePartitionCanonical(0);
        final var variable=decode?envelopeResourceRoutePartitionDecode(total):envelopeResourceRoutePartitionCanonical(total);
        final long held=decode?Math.multiplyExact(count-1,fixed.held):0;
        return new EnvelopeMaterial(Math.addExact(Math.multiplyExact(count-1,fixed.allocated),variable.allocated),
                Math.addExact(held,variable.retained),Math.addExact(held,variable.held),
                Math.addExact(Math.multiplyExact(count-1,fixed.fieldVisits),variable.fieldVisits),
                Math.addExact(Math.multiplyExact(count-1,fixed.arrayObjects),variable.arrayObjects));
    }
    private static EnvelopeMaterial envelopeResourceIngressCanonical(long encoded){
        final long count=encoded/2;
        final var partitions=envelopeDrop(envelopeArrays(encoded,count),envelopeArrays(encoded,count));
        final var pulsar=envelopeWritten(envelopeDrop(envelopeArrays(encoded,2),partitions),encoded);
        final var kafka=envelopeWritten(envelopeArrays(encoded+16,3),encoded);
        return envelopeWritten(envelopeChoice(kafka,pulsar),encoded);
    }
    private static EnvelopeMaterial envelopeResourceIngressDecode(long encoded){
        final long count=encoded/2;
        // The four-field physical partition codec owns one resource hash per item. UTF-8 and
        // canonical member bytes aggregate over the exact enclosing Route resource image.
        var partitions=envelopeProtoRead(encoded,encoded,encoded,4*count);
        partitions=envelopeHold(partitions,envelopeArrays(encoded+32*count,2*count));
        partitions=envelopeHold(partitions,envelopeArrays(32*count,count));
        partitions=envelopeTemporary(partitions,envelopeArrays(encoded,2*count));
        partitions=envelopeTemporary(partitions,envelopeArrays(encoded,count));
        var frame=envelopeProtoRead(encoded,encoded,encoded,1);
        frame=envelopeHold(frame,envelopeProtoRead(encoded,encoded,encoded,1));
        frame=envelopeHold(frame,envelopeNested(encoded,envelopeProtoRead(encoded,encoded,encoded,3+count)));
        frame=envelopeHold(frame,partitions);
        frame=envelopeTemporary(frame,envelopeArrays(encoded,4));
        frame=envelopeTemporary(frame,envelopeArray(16));
        frame=envelopeTemporary(frame,envelopeResourceIngressCanonical(encoded));return envelopeReturn(frame,32*count);
    }
    private static EnvelopeMaterial envelopeResourceRouteFields(long n){
        var fields=envelopeDrop(envelopeArray(16),envelopeResourceIngressCanonical(n));
        fields=envelopeDrop(fields,envelopeTupleCanonical());fields=envelopeDrop(fields,envelopeResourceRoutePartitions(n,n/2,false));
        fields=envelopeDrop(fields,envelopeArray(147));fields=envelopeDrop(fields,envelopeBusinessTimeCanonical(n));
        return envelopeWritten(fields,n);
    }
    private static EnvelopeMaterial envelopeResourceRouteCanonical(long n){return envelopeWritten(envelopeResourceRouteFields(n),n);}
    private static EnvelopeMaterial envelopeResourceRouteDecode(long n){
        final long count=n/2;
        var args=envelopeNested(n,envelopeResourceIngressDecode(n));args=envelopeHold(args,envelopeNested(26,envelopeTupleDecode()));
        args=envelopeHold(args,envelopeCall(envelopeArrays(n,count),envelopeResourceRoutePartitions(n,count,true),0));
        var credential=envelopeHold(envelopeProtoRead(147,138,32,5),envelopeCall(envelopeArrays(128,4),envelopeArrays(128,4),0));
        credential=envelopeTemporary(credential,envelopeArray(147));args=envelopeHold(args,envelopeNested(147,credential));
        args=envelopeHold(args,envelopeNested(n,envelopeBusinessTimeDecode(n)));args=envelopeHold(args,envelopeArrays(16+32*4L+64,6));
        var constructor=envelopeArrays(16+32*4L+64,6);
        constructor=envelopeHold(constructor,envelopeHash(envelopeHold(envelopeArray("nereus-delay-ingress-route-snapshot\0".length()),envelopeResourceRouteFields(n)),0));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,25+count),envelopeCall(args,constructor,args.held-(16+32*4L+64)));
        frame=envelopeTemporary(frame,envelopeHash(envelopeHold(envelopeArray("nereus-delay-ingress-route-snapshot-signature\0".length()),envelopeHold(envelopeArray(32),envelopeArray(4))),0));
        frame=envelopeTemporary(frame,envelopeResourceRouteCanonical(n));return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopeResourceNativeFactsCanonical(){return envelopeWritten(envelopeResourceFrameCanonical(),com.nereusstream.delay.protocol.TargetNativeReclaimFacts.MAX_BYTES);}
    private static EnvelopeMaterial envelopeResourceNativeFactsRoute(){
        // X509EncodedKeySpec owns the passed public material; provider-private/JVM allocations
        // are outside the source-visible byte-array graph.
        return envelopeCall(envelopeArray(44),envelopeResourceRouteDecode(com.nereusstream.delay.protocol.TargetNativeReclaimFacts.MAX_ROUTE_BYTES),0);
    }
    private static EnvelopeMaterial envelopeResourceNativeFactsDecode(){
        final long n=com.nereusstream.delay.protocol.TargetNativeReclaimFacts.MAX_BYTES,route=com.nereusstream.delay.protocol.TargetNativeReclaimFacts.MAX_ROUTE_BYTES;
        var args=envelopeNested(envelopeResourceFrameBytes(),envelopeResourceFrameDecode());args=envelopeHold(args,envelopeArrays(route+44,2));
        var constructor=envelopeHold(envelopeArray(route),envelopeAssigned(44));constructor=envelopeTemporary(constructor,envelopeResourceFenceSystem());
        constructor=envelopeTemporary(constructor,envelopeResourceNativeFactsRoute());
        return envelopeBusinessDecoded(n,n,route,4,args,constructor,envelopeResourceNativeFactsCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopeResourceProofCanonical(long n){
        return envelopeWritten(envelopeDrop(envelopeArray(com.nereusstream.delay.protocol.DelayMessageId.LENGTH),envelopeLegacyProfileCanonical(n)),n);
    }
    private static EnvelopeMaterial envelopeResourceProofDecode(long n){
        var args=envelopeNested(n,envelopeLegacyProfileDecode(n));args=envelopeHold(args,envelopeArrays(n,11));
        args=envelopeHold(args,envelopeLegacyMessageIdDecode());
        var constructor=envelopeArrays(n,10);constructor=envelopeTemporary(constructor,envelopeLegacySelfRoutingDecode());
        constructor=envelopeTemporary(constructor,envelopeArray(16));
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeHold(envelopeArray("nereus-delay-payload-proof-id\0".length()),envelopeResourceProofCanonical(n)),0));
        return envelopeBusinessDecoded(n,n,n,18,args,constructor,envelopeResourceProofCanonical(n),args.held);
    }
    private static EnvelopeMaterial envelopeResourceObjectOriginCanonical(boolean migrated,boolean prepare,boolean nativeCommit){
        var fields=envelopeBusinessBindingCanonical();
        if(!migrated||nativeCommit)fields=envelopeDrop(fields,envelopeRepeat(envelopeResultCanonical(),2,false));
        if(prepare)fields=envelopeDrop(fields,envelopeBusinessReservationCanonical());
        if(prepare&&!migrated)fields=envelopeDrop(fields,envelopeRepeat(envelopeResultCanonical(),2,false));
        if(migrated)fields=envelopeDrop(fields,envelopeResourceMigrationHistoryCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetObjectPayloadOrigin.MAX_CANONICAL_BYTES);
    }
    private static EnvelopeMaterial envelopeResourceObjectOriginDecode(boolean migrated,boolean prepare,boolean nativeCommit){
        final long n=com.nereusstream.delay.protocol.TargetObjectPayloadOrigin.MAX_CANONICAL_BYTES;
        var args=envelopeNested(TargetScheduleBinding.MAX_CANONICAL_BYTES,envelopeBusinessBindingDecode());
        if(!migrated||nativeCommit)args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetResultRecord.MAX_CANONICAL_BYTES,envelopeResultDecode()),2,true));
        if(prepare)args=envelopeHold(args,envelopeNested(TargetReservationRecord.MAX_CANONICAL_BYTES,envelopeBusinessReservationDecode()));
        if(prepare&&!migrated)args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetResultRecord.MAX_CANONICAL_BYTES,envelopeResultDecode()),2,true));
        if(migrated)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetMigrationPayloadHistory.MAX_BYTES,envelopeResourceMigrationHistoryDecode()));
        var constructor=envelopeReturn(envelopeSourceBounded(),0);constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResultCanonical(),4,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeSourceCanonical(),2,false));constructor=envelopeTemporary(constructor,envelopeBusinessBindingCanonical());
        return envelopeBusinessDecoded(n,n,n,9,args,constructor,envelopeResourceObjectOriginCanonical(migrated,prepare,nativeCommit),args.held);
    }
    private static EnvelopeMaterial envelopeResourceObjectOriginDecode(){return envelopeChoice(
            envelopeChoice(envelopeResourceObjectOriginDecode(false,false,true),envelopeResourceObjectOriginDecode(false,true,true)),
            envelopeChoice(envelopeResourceObjectOriginDecode(true,false,false),envelopeChoice(envelopeResourceObjectOriginDecode(true,true,false),envelopeResourceObjectOriginDecode(true,true,true))));}
    private static EnvelopeMaterial envelopeResourceObjectOriginCanonical(){return envelopeChoice(envelopeResourceObjectOriginCanonical(false,true,true),envelopeResourceObjectOriginCanonical(true,true,true));}
    private static EnvelopeMaterial envelopeResourceRetryCanonical(long n){return envelopeArray(n);}
    private static EnvelopeMaterial envelopeResourceRetryDecode(long n){
        var constructor=envelopeArrays(n,2);
        constructor=envelopeHold(constructor,envelopeHash(envelopeHold(envelopeArray("nereus-delay-retry-policy-semantic\0".length()),envelopeArray(n)),0));
        var frame=envelopeHold(envelopeProtoRead(n,n,n,18),envelopeCall(envelopeArrays(n,2),constructor,0));
        frame=envelopeTemporary(frame,envelopeArray(32));frame=envelopeTemporary(frame,envelopeResourceRetryCanonical(n));return envelopeReturn(frame,n+32);
    }
    private static EnvelopeMaterial envelopeResourceTerminalCanonical(){return envelopeArray(19);}
    private static EnvelopeMaterial envelopeResourceTerminalDecode(){return envelopeReturn(envelopeTemporary(envelopeProtoRead(19,14,10,5),envelopeResourceTerminalCanonical()),0);}
    private static EnvelopeMaterial envelopeResourceUncommittedFactsCanonical(boolean migrated){
        var fields=envelopeDrop(envelopeBusinessBindingCanonical(),envelopeBusinessReservationCanonical());
        if(!migrated)fields=envelopeDrop(fields,envelopeRepeat(envelopeResultCanonical(),2,false));
        fields=envelopeDrop(fields,envelopeRepeat(envelopeResourceFrameCanonical(),2,false));
        fields=envelopeDrop(fields,envelopeResourceRetryCanonical(com.nereusstream.delay.protocol.TargetUncommittedPayloadFacts.MAX_BYTES));fields=envelopeDrop(fields,envelopeResourceTerminalCanonical());
        if(migrated)fields=envelopeDrop(fields,envelopeResourceMigrationHistoryCanonical());
        return envelopeWritten(fields,com.nereusstream.delay.protocol.TargetUncommittedPayloadFacts.MAX_BYTES);
    }
    private static EnvelopeMaterial envelopeResourceUncommittedFactsCanonical(){return envelopeChoice(envelopeResourceUncommittedFactsCanonical(false),envelopeResourceUncommittedFactsCanonical(true));}
    private static EnvelopeMaterial envelopeResourceUncommittedFactsDecode(boolean migrated){
        final long n=com.nereusstream.delay.protocol.TargetUncommittedPayloadFacts.MAX_BYTES;
        var args=envelopeNested(TargetScheduleBinding.MAX_CANONICAL_BYTES,envelopeBusinessBindingDecode());args=envelopeHold(args,envelopeNested(TargetReservationRecord.MAX_CANONICAL_BYTES,envelopeBusinessReservationDecode()));
        if(!migrated)args=envelopeHold(args,envelopeRepeat(envelopeNested(TargetResultRecord.MAX_CANONICAL_BYTES,envelopeResultDecode()),2,true));
        args=envelopeHold(args,envelopeRepeat(envelopeNested(envelopeResourceFrameBytes(),envelopeResourceFrameDecode()),2,true));
        args=envelopeHold(args,envelopeNested(n,envelopeResourceRetryDecode(n)));args=envelopeHold(args,envelopeNested(19,envelopeResourceTerminalDecode()));
        if(migrated)args=envelopeHold(args,envelopeNested(n,envelopeResourceMigrationHistoryDecode()));
        var constructor=envelopeReturn(envelopeBusinessBindingCanonical(),0);constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResultCanonical(),2,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResourceFrameCanonical(),2,false));
        constructor=envelopeTemporary(constructor,envelopeHash(envelopeResourceTerminalCanonical(),"nereus-delay-terminal-policy-semantic\0".length()));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResourceFenceSystem(),5,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetTimeFenceBody.MAX_CANONICAL_BYTES,envelopeTimeFenceDecode()),2,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeCommandFrameDecode(envelopeResourceFrameBytes()),2,false));constructor=envelopeTemporary(constructor,envelopeArrays(64,2));
        return envelopeBusinessDecoded(n,n,n,migrated?8:9,args,constructor,envelopeResourceUncommittedFactsCanonical(migrated),args.held);
    }
    private static EnvelopeMaterial envelopeResourceUncommittedFactsDecode(){return envelopeChoice(envelopeResourceUncommittedFactsDecode(false),envelopeResourceUncommittedFactsDecode(true));}
    private static EnvelopeMaterial envelopeResourceDeletionCanonical(){return envelopeArray(com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_PUBLIC_BYTES+128L);}
    private static EnvelopeMaterial envelopeResourceDeletionDecode(){
        final long n=com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_PUBLIC_BYTES+128L;
        return envelopeBusinessDecoded(n,n,n,3,envelopeArrays(n,3),envelopeHold(envelopeAssigned(64,2),
                envelopeArray(com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_PUBLIC_BYTES)),envelopeResourceDeletionCanonical(),n);
    }
    private static EnvelopeMaterial envelopeResourceClosedVersionCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_BYTES;
        return envelopeWritten(envelopeDrop(envelopePayloadObjectResourceCanonical(n),envelopeResourceDeletionCanonical()),n);
    }
    private static EnvelopeMaterial envelopeResourceClosedVersionDecode(){
        final long n=com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_BYTES;
        var args=envelopeHold(envelopeNested(n,envelopePayloadObjectResourceDecode(n)),
                envelopeNested(com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_PUBLIC_BYTES+128L,envelopeResourceDeletionDecode()));
        var constructor=envelopeReturn(envelopePayloadObjectResourceCanonical(n),0);constructor=envelopeTemporary(constructor,envelopeArray(n));
        return envelopeBusinessDecoded(n,n,n,2,args,constructor,envelopeResourceClosedVersionCanonical(),args.held);
    }
    private static EnvelopeMaterial envelopeResourceClosureCanonical(){
        final long n=com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_BYTES;
        return envelopeWritten(envelopeDrop(envelopeLegacyProfileCanonical(n),envelopeRepeat(envelopeResourceClosedVersionCanonical(),
                com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_VERSIONS,false)),n);
    }
    private static EnvelopeMaterial envelopeResourceClosureDecode(){
        final long n=com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_BYTES;
        final int count=com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_VERSIONS;
        var args=envelopeRepeat(envelopeNested(n,envelopeResourceClosedVersionDecode()),count,true);
        args=envelopeHold(args,envelopeNested(n,envelopeLegacyProfileDecode(n)));
        args=envelopeHold(args,envelopeArrays(32+1024+4096+com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_PUBLIC_BYTES,4));
        var constructor=envelopeHold(envelopeAssigned(32),envelopeArrays(1024+4096+com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_PUBLIC_BYTES,3));
        // Sort compares immutable versions using accessor copies; the bounded list has at most
        // count^2 comparisons. The ownership loop additionally obtains container/key/version.
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArrays(n,2),(long)count*count,false));
        constructor=envelopeTemporary(constructor,envelopeLegacyProfileCanonical(n));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArrays(n,4),count,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResourceClosedVersionCanonical(),count,false));
        return envelopeBusinessDecoded(n,n,n,count+6L,args,constructor,envelopeResourceClosureCanonical(),args.held);
    }
    private static long envelopeResourceEntryBytes(int kind){return switch(kind){
        case 0->TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES+32L;
        case 1->TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES+com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES+128L;
        case 2->TargetResultRecord.MAX_CANONICAL_BYTES+32L;
        case 4->TargetQuotaIncarnation.MAX_CANONICAL_BYTES+(long)com.nereusstream.delay.protocol.TargetCloseRecord.MAX_CANONICAL_BYTES+32;
        case 5->TargetQuotaIncarnation.MAX_CANONICAL_BYTES+(long)com.nereusstream.delay.protocol.TargetQuotaGrantActivation.MAX_CANONICAL_BYTES+32;
        case 6,8->com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;
        default->com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES;
    };}
    private static EnvelopeMaterial envelopeResourceEntryCanonical(int kind){
        EnvelopeMaterial fields;
        switch(kind){
            case 0->fields=envelopeAttemptCanonical();
            case 1->fields=envelopeDrop(envelopePayloadCanonical(true),envelopeMessageLocatorCanonical());
            case 2->fields=envelopeResultCanonical();
            case 3->{
                // operationSchema calls includesAnchor and decodes each retained confirmation body.
                fields=envelopeReturn(envelopeRepeat(envelopeNested(envelopeBusinessConfirmedBodyBytes(),envelopeEvidenceConfirmedBodyDecode()),
                        com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS,false),0);
                fields=envelopeDrop(fields,envelopeAttemptCanonical());
                fields=envelopeDrop(fields,envelopeRepeat(envelopeBusinessConfirmedCanonical(),com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS,false));
            }
            case 4->fields=envelopeDrop(envelopeIncarnationCanonical(true),envelopeBusinessCloseCanonical());
            case 5->fields=envelopeDrop(envelopeIncarnationCanonical(true),envelopeGrantActivationCanonical());
            case 6->{fields=envelopeDrop(envelopePayloadCanonical(true),envelopeMessageLocatorCanonical());fields=envelopeDrop(fields,envelopeChoice(
                    envelopeResourceProofCanonical(envelopeResourceObjectIntentBytes()),envelopeResourceObjectOriginCanonical()));}
            case 7->fields=envelopeObjectPendingCanonical();
            case 8->fields=envelopeDrop(envelopePayloadCanonical(true),envelopeResourceUncommittedFactsCanonical());
            case 9->fields=envelopeDrop(envelopeObjectPendingCanonical(),envelopeResourceClosureCanonical());
            default->throw new IllegalArgumentException("unknown reclaim operation material");
        }
        return envelopeWritten(fields,envelopeResourceEntryBytes(kind));
    }
    private static EnvelopeMaterial envelopeResourceEntryIdentity(int kind){
        if(kind==2)return envelopeConcat(envelopeHold(envelopeArray(2),envelopeConcat(envelopeHold(envelopeArray(2),envelopeSourceCanonical()))));
        if(kind==4||kind==5)return envelopeConcat(envelopeHold(envelopeArray(2),envelopeIdentityCanonical()));
        if(kind==7||kind==9){
            var body=envelopeCall(envelopeArray(envelopeResourceObjectIntentBytes()),envelopeResourceObjectIntentDecode(),0);
            body=envelopeReturn(body,0);
            final var fields=envelopeHold(envelopeArray(2),envelopeArrays(com.nereusstream.delay.protocol.DelayMessageId.LENGTH+16L,2));
            return envelopeDrop(body,envelopeConcat(fields));
        }
        return envelopeConcat(envelopeHold(envelopeArray(kind==0?1:2),envelopeArray(kind==0||kind==3?32:com.nereusstream.delay.protocol.DelayMessageId.LENGTH)));
    }
    private static EnvelopeMaterial envelopeResourceEntryDecode(int kind){
        final long n=envelopeResourceEntryBytes(kind);EnvelopeMaterial args,constructor=envelopeEmpty();long fields;
        switch(kind){
            case 0->{args=envelopeNested(TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES,envelopeAttemptDecode());fields=3;}
            case 1->{args=envelopeHold(envelopeNested(TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES,envelopePayloadDecode(true)),envelopeNested(com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES,envelopeMessageLocatorDecode()));
                args=envelopeHold(args,envelopeArrays(64,2));constructor=envelopeAssigned(64,2);constructor=envelopeTemporary(constructor,envelopeArrays(80,3));fields=7;}
            case 2->{args=envelopeNested(TargetResultRecord.MAX_CANONICAL_BYTES,envelopeResultDecode());fields=4;}
            case 3->{args=envelopeNested(TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES,envelopeAttemptDecode());
                args=envelopeHold(args,envelopeRepeat(envelopeNested(com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.MAX_CANONICAL_BYTES,envelopeBusinessConfirmedDecode()),com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS,true));
                final int count=com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS;
                constructor=envelopeReturn(envelopeRepeat(envelopeNested(envelopeBusinessConfirmedBodyBytes(),envelopeEvidenceConfirmedBodyDecode()),count,false),0);
                constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArray(112),count,false));
                // Provenance, key(), shareIdentity(), confirmedShare(), operationSchema and the
                // legacy-anchor guard each obtain an independently decoded confirmation body.
                constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeNested(envelopeBusinessConfirmedBodyBytes(),envelopeEvidenceConfirmedBodyDecode()),6L*count,false));
                constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeEvidenceExactIdentity(),count,false));
                constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeIdentityCanonical(),envelopeIdentityCanonical()),count,false));
                constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()),count,false));
                constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeMutationCanonical(false),envelopeMutationCanonical(false)),count,false));fields=count+4L;}
            case 4->{args=envelopeHold(envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)),envelopeNested(com.nereusstream.delay.protocol.TargetCloseRecord.MAX_CANONICAL_BYTES,envelopeBusinessCloseDecode()));
                constructor=envelopeIdentityConstruct();constructor=envelopeTemporary(constructor,envelopeBusinessCloseRequestCanonical());constructor=envelopeTemporary(constructor,envelopeArrays(32,2));fields=5;}
            case 5->{args=envelopeHold(envelopeNested(TargetQuotaIncarnation.MAX_CANONICAL_BYTES,envelopeIncarnationDecode(true)),envelopeNested(com.nereusstream.delay.protocol.TargetQuotaGrantActivation.MAX_CANONICAL_BYTES,envelopeGrantActivationDecode()));
                constructor=envelopeReturn(envelopeComparison(envelopeAccountingCanonical(),envelopeAccountingCanonical()),0);constructor=envelopeTemporary(constructor,envelopeArrays(96,4));fields=5;}
            case 6->{args=envelopeHold(envelopeNested(TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES,envelopePayloadDecode(true)),envelopeNested(com.nereusstream.delay.protocol.TargetMessageLocator.MAX_CANONICAL_BYTES,envelopeMessageLocatorDecode()));
                args=envelopeHold(args,envelopeArrays(64,2));args=envelopeHold(args,envelopeChoice(envelopeNested(n,envelopeResourceProofDecode(n)),envelopeNested(com.nereusstream.delay.protocol.TargetObjectPayloadOrigin.MAX_CANONICAL_BYTES,envelopeResourceObjectOriginDecode())));
                constructor=envelopeAssigned(64,2);constructor=envelopeTemporary(constructor,envelopePayloadReferenceBounded());constructor=envelopeTemporary(constructor,envelopePayloadRefDecode());
                constructor=envelopeTemporary(constructor,envelopePrepareBodyDecode());constructor=envelopeTemporary(constructor,envelopeArrays(n,12));fields=9;}
            case 7->{args=envelopeHold(envelopeNested(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_CANONICAL_BYTES,envelopeObjectPendingDecode()),envelopeArrays(64,2));
                constructor=envelopeAssigned(64,2);constructor=envelopeTemporary(constructor,envelopeNested(envelopeResourceObjectIntentBytes(),envelopeResourceObjectIntentDecode()));fields=6;}
            case 8->{args=envelopeHold(envelopeNested(TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES,envelopePayloadDecode(true)),envelopeNested(com.nereusstream.delay.protocol.TargetUncommittedPayloadFacts.MAX_BYTES,envelopeResourceUncommittedFactsDecode()));
                constructor=envelopeReturn(envelopeBusinessBindingCanonical(),0);constructor=envelopeTemporary(constructor,envelopeBusinessReservationCanonical());constructor=envelopeTemporary(constructor,envelopeArrays(96,4));fields=5;}
            case 9->{args=envelopeHold(envelopeNested(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.MAX_CANONICAL_BYTES,envelopeObjectPendingDecode()),envelopeNested(com.nereusstream.delay.protocol.TargetUncommittedObjectClosure.MAX_BYTES,envelopeResourceClosureDecode()));
                constructor=envelopeReturn(envelopeNested(envelopeResourceObjectIntentBytes(),envelopeResourceObjectIntentDecode()),0);constructor=envelopeTemporary(constructor,envelopeHash(envelopeObjectPendingCanonical(),0));
                constructor=envelopeTemporary(constructor,envelopePrepareBodyDecode());fields=5;}
            default->throw new IllegalArgumentException("unknown reclaim operation material");
        }
        return envelopeBusinessDecoded(n,n,n,fields,args,constructor,envelopeResourceEntryCanonical(kind),args.held+constructor.held);
    }
    private static long envelopeResourceReclaimBytes(int kind){return kind==0||kind==1
            ?com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_CANONICAL_BYTES
            :kind==6||kind==8?com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES
            :com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES;}
    private static int envelopeResourceOperationCount(int kind){return kind>=4?1:com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS;}
    private static EnvelopeMaterial envelopeResourceOperationGroup(int kind){
        final var entry=kind==2?envelopeChoice(envelopeResourceEntryCanonical(2),envelopeResourceEntryCanonical(3)):envelopeResourceEntryCanonical(kind);
        return envelopeWritten(envelopeRepeat(entry,envelopeResourceOperationCount(kind),false),envelopeResourceReclaimBytes(kind));
    }
    private static EnvelopeMaterial envelopeResourceNativeFactsRequireFor(int kind){
        var work=envelopeReturn(envelopeSourceBounded(),0);
        if(kind==2){work=envelopeTemporary(work,envelopeResourceNativeFactsRoute());work=envelopeTemporary(work,envelopeArray(32));}
        return work;
    }
    private static EnvelopeMaterial envelopeResourceReclaimCanonical(int kind,boolean sourceFacts,boolean nativeFacts){
        final long n=envelopeResourceReclaimBytes(kind);
        var work=nativeFacts?envelopeResourceNativeFactsRequireFor(kind):envelopeEmpty();
        work=envelopeDrop(work,envelopeShardSubjectCanonical());work=envelopeDrop(work,envelopeScopeCanonical());
        work=envelopeDrop(work,envelopeResourceFloorCanonical(n));work=envelopeDrop(work,envelopeResourceOperationGroup(kind));
        if(sourceFacts)work=envelopeDrop(work,envelopeResourceSourceFactsCanonical());
        if(nativeFacts)work=envelopeDrop(work,envelopeResourceNativeFactsCanonical());
        return envelopeWritten(work,n);
    }
    private static EnvelopeMaterial envelopeResourceReclaimLogical(int kind,boolean sourceFacts){
        var fields=envelopeHold(envelopeScopeCanonical(),envelopeResourceOperationGroup(kind));
        if(kind>=2||sourceFacts){
            final var tuple=envelopeCall(envelopeTupleCanonical(),envelopeCall(envelopeArray(4),envelopeArray(30),0),0);
            fields=envelopeHold(fields,tuple);
        }
        return envelopeHash(fields,80+"nereus-delay-target-resource-reclaim-logical-identity\0".length());
    }
    private static EnvelopeMaterial envelopeResourceReclaimDecode(int kind,boolean sourceFacts,boolean nativeFacts){
        final long n=envelopeResourceReclaimBytes(kind),entry=envelopeResourceEntryBytes(kind);
        final int count=envelopeResourceOperationCount(kind);
        // The group Reader owns one raw entry per operation; the raw payload local in Native
        // retained operations additionally survives their typed constructor call.
        var operations=envelopeProtoRead(n,n,n,count);
        var entries=kind==2?envelopeChoice(envelopeNested(entry,envelopeResourceEntryDecode(2)),
                envelopeNested(envelopeResourceEntryBytes(3),envelopeResourceEntryDecode(3))):envelopeNested(entry,envelopeResourceEntryDecode(kind));
        if(kind==2||kind==3)entries=envelopeHold(envelopeArray(Math.max(TargetResultRecord.MAX_CANONICAL_BYTES,TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES)),entries);
        operations=envelopeHold(operations,envelopeRepeat(entries,count,true));
        var args=envelopeNested(TargetQuotaScope.MAX_CANONICAL_BYTES,envelopeScopeDecode());
        args=envelopeHold(args,envelopeArrays(80,3));args=envelopeHold(args,envelopeNested(n,envelopeResourceFloorDecode(n)));
        args=envelopeHold(args,operations);
        if(sourceFacts)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetInlinePayloadSourceFacts.MAX_BYTES,envelopeResourceSourceFactsDecode()));
        if(nativeFacts)args=envelopeHold(args,envelopeNested(com.nereusstream.delay.protocol.TargetNativeReclaimFacts.MAX_BYTES,envelopeResourceNativeFactsDecode()));
        var constructor=envelopeAssigned(80,3);constructor=envelopeTemporary(constructor,envelopeArray(16));
        constructor=envelopeTemporary(constructor,envelopeSourceBounded());
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeComparison(envelopeResourceEntryIdentity(kind),envelopeResourceEntryIdentity(kind)),(long)count*count,false));
        constructor=envelopeTemporary(constructor,envelopeResourceFloorCanonical(n));constructor=envelopeTemporary(constructor,envelopeScopeCanonical());
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeResourceEntryIdentity(kind),count,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(kind==2?envelopeChoice(envelopeResourceEntryCanonical(2),envelopeResourceEntryCanonical(3)):
                envelopeResourceEntryCanonical(kind),count,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeSourceBounded(),count,false));
        constructor=envelopeTemporary(constructor,envelopeRepeat(envelopeArrays(48,2),count,false));
        if(kind==7||kind==9)constructor=envelopeTemporary(constructor,envelopeNested(envelopeResourceObjectIntentBytes(),envelopeResourceObjectIntentDecode()));
        if(sourceFacts)constructor=envelopeTemporary(constructor,envelopeResourceSourceFactsCanonical());
        if(nativeFacts)constructor=envelopeTemporary(constructor,envelopeResourceNativeFactsCanonical());
        var frame=envelopeHold(envelopeProtoRead(n,n,n,sourceFacts||nativeFacts?11:10),envelopeCall(args,constructor,args.held-80));
        frame=envelopeTemporary(frame,envelopeNested(24,envelopeShardSubjectDecode()));
        if(nativeFacts)frame=envelopeTemporary(frame,envelopeResourceNativeFactsRequireFor(kind));
        frame=envelopeTemporary(frame,envelopeResourceReclaimCanonical(kind,sourceFacts,nativeFacts));
        return envelopeReturn(frame,args.held);
    }
    private static EnvelopeMaterial envelopeResourceObjectIntentDecode(){return envelopeChoice(
            envelopeResourceReclaimDecode(6,true,false),envelopeResourceReclaimDecode(8,false,false));}
    private static EnvelopeMaterial envelopeResourceObjectIntentCanonical(){return envelopeChoice(
            envelopeResourceReclaimCanonical(6,true,false),envelopeResourceReclaimCanonical(8,false,false));}
    private static long envelopeResourceObjectIntentBytes(){return com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;}
    private static EnvelopeMaterial envelopeResourceObjectIntentValidation(){return envelopeReturn(envelopeResourceObjectIntentDecode(),0);}
    private static EnvelopeMaterial envelopeResourceObjectIntentIdentity(){
        return envelopeCall(envelopeResourceObjectIntentDecode(),envelopeChoice(envelopeResourceReclaimLogical(6,true),envelopeResourceReclaimLogical(8,false)),0);
    }
    private static EnvelopeMaterial envelopeResourceReclaimDecode(){return ResourceReclaimMaterial.DECODE;}
    private static EnvelopeMaterial calculateEnvelopeResourceReclaimDecode(){
        var result=envelopeChoice(envelopeResourceReclaimDecode(0,false,false),envelopeResourceReclaimDecode(1,false,false));
        result=envelopeChoice(result,envelopeResourceReclaimDecode(1,true,false));
        for(int kind=2;kind<=5;kind++)result=envelopeChoice(result,envelopeResourceReclaimDecode(kind,false,true));
        result=envelopeChoice(result,envelopeResourceObjectIntentDecode());
        result=envelopeChoice(result,envelopeResourceReclaimDecode(7,false,false));
        return envelopeChoice(result,envelopeResourceReclaimDecode(9,false,false));
    }
    private static EnvelopeMaterial envelopeResourceReclaimCanonical(){
        var result=envelopeChoice(envelopeResourceReclaimCanonical(0,false,false),envelopeResourceReclaimCanonical(1,false,false));
        result=envelopeChoice(result,envelopeResourceReclaimCanonical(1,true,false));
        for(int kind=2;kind<=5;kind++)result=envelopeChoice(result,envelopeResourceReclaimCanonical(kind,false,true));
        result=envelopeChoice(result,envelopeResourceObjectIntentCanonical());
        result=envelopeChoice(result,envelopeResourceReclaimCanonical(7,false,false));
        return envelopeChoice(result,envelopeResourceReclaimCanonical(9,false,false));
    }
    private static EnvelopeMaterial envelopeResourceReclaimValidation(){return envelopeReturn(envelopeResourceReclaimDecode(),0);}
    private static EnvelopeMaterial envelopeResourceReclaimIdentity(){
        var logical=envelopeChoice(envelopeResourceReclaimLogical(0,false),envelopeResourceReclaimLogical(1,false));
        logical=envelopeChoice(logical,envelopeResourceReclaimLogical(1,true));
        for(int kind=2;kind<=9;kind++)logical=envelopeChoice(logical,envelopeResourceReclaimLogical(kind,kind==6));
        return envelopeCall(envelopeResourceReclaimDecode(),logical,0);
    }
    private static EnvelopeMaterial envelopeBusinessCanonicalMaterial(TargetSourceOperationEnvelope.Row row){return switch(row){
        case QUEUE->envelopeBusinessQueueCanonical();case IDENTITY->envelopeBusinessIdentityCanonical();
        case WORK->envelopeBusinessWorkCanonical();case MESSAGE->envelopeBusinessMessageCanonical();
        case EXPIRY->envelopeBusinessExpiryCanonical();case ORDER->envelopeBusinessOrderCanonical();
        case DISPATCH->envelopeBusinessDispatchCanonical();case CONTROL->envelopeBusinessControlScopeCanonical();
        case CHANNEL->envelopeBusinessChannelCanonical();case BINDING->envelopeBusinessBindingCanonical();
        case MEMBER->envelopeBusinessMemberCanonical();case POLICY->envelopeBusinessPolicyCanonical();
        case NATIVE_SCOPE->envelopeBusinessNativeScopeCanonical();case NATIVE_SNAPSHOT->envelopeBusinessSnapshotCanonical();
        case RESERVATION->envelopeBusinessReservationCanonical();case CLOSE->envelopeBusinessCloseCanonical();
        case CLOSE_CURSOR->envelopeBusinessCloseCursorCanonical();case MEMBER_CLOSE->envelopeBusinessMemberCloseCanonical();
        case NATIVE_CONTROL->envelopeBusinessNativeControlCanonical();case CHANNEL_CONTROL->envelopeBusinessChannelControlCanonical();
        case CHANNEL_SLOT->envelopeBusinessChannelSlotCanonical();case KAFKA_MAPPING->envelopeBusinessMappingCanonical();
        case KAFKA_SLOT->envelopeBusinessKafkaSlotCanonical();case EVIDENCE_OWNER->envelopeBusinessEvidenceOwnerCanonical();
        case EVIDENCE_REF->envelopeBusinessEvidenceReferenceCanonical();case RECOVERY_OWNER->envelopeBusinessRecoveryOwnerCanonical();
        case RECOVERY_REF->envelopeBusinessRecoveryReferenceCanonical();case RETIRING->envelopeBusinessRetiringCanonical();
        case CONFIRMED->envelopeBusinessConfirmedCanonical();
        default->throw new IllegalArgumentException("row has no ordinary business codec graph: "+row);
    };}
    private static EnvelopeMaterial envelopeBusinessDecodeMaterial(TargetSourceOperationEnvelope.Row row){return switch(row){
        case QUEUE->envelopeBusinessQueueDecode();case IDENTITY->envelopeBusinessIdentityDecode();
        case WORK->envelopeBusinessWorkDecode();case MESSAGE->envelopeBusinessMessageDecode();
        case EXPIRY->envelopeBusinessExpiryDecode();case ORDER->envelopeBusinessOrderDecode();
        case DISPATCH->envelopeBusinessDispatchDecode();case CONTROL->envelopeBusinessControlScopeDecode();
        case CHANNEL->envelopeBusinessChannelDecode();case BINDING->envelopeBusinessBindingDecode();
        case MEMBER->envelopeBusinessMemberDecode();case POLICY->envelopeBusinessPolicyDecode();
        case NATIVE_SCOPE->envelopeBusinessNativeScopeDecode();case NATIVE_SNAPSHOT->envelopeBusinessSnapshotDecode();
        case RESERVATION->envelopeBusinessReservationDecode();case CLOSE->envelopeBusinessCloseDecode();
        case CLOSE_CURSOR->envelopeBusinessCloseCursorDecode();case MEMBER_CLOSE->envelopeBusinessMemberCloseDecode();
        case NATIVE_CONTROL->envelopeBusinessNativeControlDecode();case CHANNEL_CONTROL->envelopeBusinessChannelControlDecode();
        case CHANNEL_SLOT->envelopeBusinessChannelSlotDecode();case KAFKA_MAPPING->envelopeBusinessMappingDecode();
        case KAFKA_SLOT->envelopeBusinessKafkaSlotDecode();case EVIDENCE_OWNER->envelopeBusinessEvidenceOwnerDecode();
        case EVIDENCE_REF->envelopeBusinessEvidenceReferenceDecode();case RECOVERY_OWNER->envelopeBusinessRecoveryOwnerDecode();
        case RECOVERY_REF->envelopeBusinessRecoveryReferenceDecode();case RETIRING->envelopeBusinessRetiringDecode();
        case CONFIRMED->envelopeBusinessConfirmedDecode();
        default->throw new IllegalArgumentException("row has no ordinary business codec graph: "+row);
    };}

    public interface View {
        ShardId shardId();

        byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay);

        /** Only the actual hot writer kernel can qualify a prefix; cold views always verify the complete chain. */
        default TargetSloWriterOrigin.Context writerOriginContext() { return null; }

        /** A privately minted complete retirement preparation is visible only inside its exact final plan. */
        default TargetRetirementPreparation.Proof retirementProof() { return null; }

        /** Private actual Source tail, available only during its original Source action. */
        default TargetMigrationArchivePreparation.PreparedTail archiveTailProof() { return null; }

        /** Complete cold-path reference joins need a finite ordered range in the same selected view. */
        default com.nereusstream.delay.store.ShardStore.KeyValue first(ColumnFamily family, byte[] lower, byte[] upper,
                List<TargetStoreBackend.Edit> overlay) {
            throw new IllegalStateException("bounded accounting range reads are not configured");
        }
    }

    public record Charge(TargetQuotaIncarnation owner, TargetQuotaUsage primary) {
        public Charge {
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(primary, "primary");
        }

        public TargetQuotaUsage mirror() {
            return resources(primary.resources());
        }
    }

    private final View reader;
    private final List<TargetStoreBackend.Edit> overlay;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final TargetQuotaMutation operation;
    private final TargetQuotaBookkeeping root;
    private final int maximumDomains;

    public TargetRecordAccounting(
            final View reader,
            final List<TargetStoreBackend.Edit> overlay,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final TargetQuotaMutation operation,
            final TargetQuotaBookkeeping root,
            final int maximumDomains) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.overlay = List.copyOf(overlay);
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        this.lineage = Bytes.copy(lineage);
        this.operation = Objects.requireNonNull(operation, "operation");
        this.root = Objects.requireNonNull(root, "root");
        if (scope.target() != null
                || !scope.shard().equals(reader.shardId())
                || maximumDomains < 1
                || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("invalid record accounting scope or domain limit");
        }
        this.maximumDomains = maximumDomains;
    }

    /** Null is reserved for grant activation storage, which is counted by the root's fixed inventory reserve. */
    public Charge charge(final ColumnFamily family, final byte[] key, final byte[] raw) {
        final var value = TargetValueEnvelope.decodeAny(raw);
        final byte[] payload = value.payload();
        if (!Arrays.equals(raw, reader.projected(family, key, overlay))) {
            throw new IllegalStateException("accounting record is not the exact selected Store view");
        }
        final Charge result;
        switch (value.valueType()) {
            case TargetInitialControlSnapshot.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var snapshot = TargetInitialControlSnapshot.decode(payload);
                if (!Arrays.equals(key, snapshot.key()) || !snapshot.root().equals(root.owner())
                        || !snapshot.input().scope().equals(scope)
                        || !Arrays.equals(snapshot.input().lineage(), lineage)) {
                    throw new IllegalStateException("Target initial control accounting crosses key/root/scope");
                }
                snapshot.mutation().requireAtOrBefore(operation);
                final var owner = descriptor(root.owner());
                result = new Charge(owner, resources(owner.accounting().recordCharge(
                        TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }

            case TargetReservationRecord.VALUE_TYPE -> {
                final var reservation = TargetReservationRecord.decode(payload);
                final boolean expiryIndex = family == ColumnFamily.TIMELINE;
                final boolean targetIndex =
                        family == ColumnFamily.ID && Arrays.equals(key, reservation.targetIndexKey());
                final boolean index = expiryIndex || targetIndex;
                if ((family != ColumnFamily.ID && !expiryIndex)
                        || (expiryIndex
                                ? !Arrays.equals(key, reservation.expiryKey())
                                : !Arrays.equals(key, reservation.key())
                                        && !Arrays.equals(key, reservation.lookupKey())
                                        && !targetIndex)
                        || (index && reservation.status() != PayloadReservationStatus.RESERVED)) {
                    throw new IllegalStateException("Target reservation key/family/status differs");
                }
                if (!Arrays.equals(
                                payload,
                                payload(ColumnFamily.ID, reservation.key(), TargetReservationRecord.VALUE_TYPE))
                        || !Arrays.equals(
                                payload,
                                payload(
                                        ColumnFamily.ID,
                                        reservation.lookupKey(),
                                        TargetReservationRecord.VALUE_TYPE))) {
                    throw new IllegalStateException("reservation message/id lookup projections differ");
                }
                {
                    final byte[] expiry = reader.projected(ColumnFamily.TIMELINE, reservation.expiryKey(), overlay);
                    if (reservation.status() == PayloadReservationStatus.RESERVED) {
                        if (expiry == null
                                || !Arrays.equals(
                                        payload,
                                        TargetValueEnvelope.decode(expiry, TargetReservationRecord.VALUE_TYPE)
                                                .payload())) {
                            throw new IllegalStateException("reserved record lacks its exact expiry index");
                        }
                    } else if (expiry != null) {
                        throw new IllegalStateException("terminal reservation still owns an expiry index");
                    }
                }
                final byte[] targetProjection =
                        reader.projected(ColumnFamily.ID, reservation.targetIndexKey(), overlay);
                if (reservation.status() == PayloadReservationStatus.RESERVED) {
                    if (targetProjection == null
                            || !Arrays.equals(
                                    payload,
                                    TargetValueEnvelope.decode(targetProjection, TargetReservationRecord.VALUE_TYPE)
                                            .payload())) {
                        throw new IllegalStateException("reserved accounting lacks its exact Target index");
                    }
                } else if (targetProjection != null) {
                    throw new IllegalStateException("terminal accounting retains a Target reservation index");
                }
                final var bindingKey =
                        TargetKeyCodec.scheduleBinding(reservation.locator().scheduleBindingDigest());
                final var binding = TargetScheduleBinding.decodeForStore(
                        bindingKey,
                        payload(ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                        scope.shard());
                reservation.requireBinding(binding);
                final var payloadOwner = payloadOwner(reservation.locator().messageId());
                payloadOwner.requireInitialBinding(binding);
                reservation.requireOwner(payloadOwner);
                reservation.mutation().requireAtOrBefore(operation);
                final var owner = descriptor(payloadOwner.primaryIdentity());
                requirePayloadOwner(owner, payloadOwner);
                result = new Charge(
                        owner,
                        resources(owner.accounting()
                                .recordCharge(
                                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                                        key.length,
                                        payload.length)));
            }
            case TargetTerminalGenerationRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.TERMINAL);
                final var terminal = TargetTerminalGenerationRecord.decode(payload);
                if (!Arrays.equals(key, terminal.key())) {
                    throw new IllegalStateException("Target terminal key differs from its generation");
                }
                final var payloadOwner = payloadOwner(terminal.locator().messageId());
                terminal.requireOwner(payloadOwner);
                terminal.mutation().requireAtOrBefore(operation);
                final var owner = descriptor(payloadOwner.primaryIdentity());
                requirePayloadOwner(owner, payloadOwner);
                result = new Charge(
                        owner,
                        resources(owner.accounting()
                                .recordCharge(
                                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                                        key.length,
                                        payload.length)));
            }
            case TargetMessageRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.ID);
                final var message = TargetMessageRecord.decodeForStore(key, payload, scope.shard());
                final var owner = payloadOwner(message.locator().messageId());
                result = messageCharge(owner, TargetQuotaMessageRecords.message(owner, key, payload));
            }
            case TargetScheduleBinding.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.ID);
                final var binding = TargetScheduleBinding.decodeForStore(key, payload, scope.shard());
                final var owner = payloadOwner(binding.messageId());
                result = messageCharge(owner, TargetQuotaMessageRecords.initialBinding(owner, key, payload));
            }
            case TargetTimelineWorkRef.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.TIMELINE);
                final var work = TargetTimelineWorkRef.decode(payload);
                final var message = message(work.locator().messageId());
                final var owner = payloadOwner(work.locator().messageId());
                result = messageCharge(
                        owner,
                        key[0] == TargetKeyCodec.ORDER_HEAD_TAG
                                ? TargetQuotaMessageRecords.orderHead(
                                        owner,
                                        message,
                                        order(
                                                work.locator().target(),
                                                work.locator().orderingDomain()),
                                        key,
                                        payload)
                                : TargetQuotaMessageRecords.timeline(owner, message, key, payload));
            }
            case TargetExpiryRef.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.TIMELINE);
                final var expiry = TargetExpiryRef.decode(payload);
                final var message = message(expiry.locator().messageId());
                final var owner = payloadOwner(expiry.locator().messageId());
                result = messageCharge(owner, TargetQuotaMessageRecords.expiry(owner, message, key, payload));
            }
            case TargetQuotaPayloadOwner.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var owner =
                        TargetQuotaPayloadOwner.decodeForStore(key, payload, scope.shard(), scope.tenantScope());
                final var descriptor = descriptor(owner.primaryIdentity());
                requirePayloadOwner(descriptor, owner);
                TargetRetirementSourceHistoryStore.requirePayloadReclaim(reader,overlay,scope,lineage,
                        root.owner(),owner,operation);
                result = new Charge(
                        descriptor,
                        resources(TargetQuotaMessageRecords.payloadOwner(owner, key, payload)
                                .charge()
                                .add(owner.payloadCharge())));
            }
            case TargetQuotaIncarnation.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var owner =
                        TargetQuotaIncarnation.decodeForStore(key, payload, scope.shard(), scope.tenantScope());
                if(owner.draining())TargetIncarnationReclaim.requireDrainingOrigin(reader,overlay,scope,lineage,owner,operation);
                result = new Charge(owner, owner.ownContribution());
            }
            case com.nereusstream.delay.protocol.TargetCloseRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var close = com.nereusstream.delay.protocol.TargetCloseRecord.decodeForStore(
                        key, payload, scope.shard(), lineage);
                close.mutation().requireAtOrBefore(operation);
                final var queue = TargetQueueState.decode(payload(
                        ColumnFamily.META,
                        TargetKeyCodec.state(close.body().request().target()),
                        TargetQueueState.VALUE_TYPE));
                close.requireQueue(queue);
                final var owner = descriptor(close.ownerIdentity());
                result = new Charge(
                        owner,
                        resources(owner.accounting()
                                .recordCharge(
                                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                                        key.length,
                                        payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetCloseCursorRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var cursor = com.nereusstream.delay.protocol.TargetCloseCursorRecord.decode(payload);
                final var markerKey = TargetKeyCodec.close(cursor.target());
                final var marker = com.nereusstream.delay.protocol.TargetCloseRecord.decodeForStore(
                        markerKey,
                        payload(
                                ColumnFamily.META,
                                markerKey,
                                com.nereusstream.delay.protocol.TargetCloseRecord.VALUE_TYPE),
                        scope.shard(),
                        lineage);
                if (!Arrays.equals(key, cursor.key()) || !Arrays.equals(cursor.recoveryLineage(), lineage)) {
                    throw new IllegalStateException("Close cursor key/lineage differs from Store projection");
                }
                cursor.requireMarker(marker);
                cursor.mutation().requireAtOrBefore(operation);
                final var queue = TargetQueueState.decode(
                        payload(ColumnFamily.META, TargetKeyCodec.state(cursor.target()), TargetQueueState.VALUE_TYPE));
                marker.requireQueue(queue);
                final var owner = descriptor(cursor.ownerIdentity(marker));
                result = new Charge(
                        owner,
                        resources(owner.accounting()
                                .recordCharge(
                                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                                        key.length,
                                        payload.length)));
            }
            case TargetQueueState.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var queue = TargetQueueState.decode(payload);
                result = metadata(TargetQuotaMetadataRecords.queue(
                        targetOwner(queue.targetId(), queue.accountingIncarnation()),
                        physical(queue.targetId()),
                        maximumDomains,
                        key,
                        payload));
            }
            case TargetOrderState.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var state = TargetOrderState.decode(payload);
                result = metadata(TargetQuotaMetadataRecords.strictDomain(
                        targetOwner(state.target(), state.accountingIncarnation()), key, payload));
            }
            case TargetChannelIdentity.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var channel = TargetChannelIdentity.decodeForStore(key, payload, scope.shard());
                result = metadata(TargetQuotaMetadataRecords.channel(
                        targetOwner(
                                channel.context().target(), channel.context().accountingIncarnation()),
                        key,
                        payload));
            }
            case com.nereusstream.delay.protocol.TargetChannelControlRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var record = com.nereusstream.delay.protocol.TargetChannelControlRecord.decodeForStore(
                        key, payload, scope.shard());
                if (!Arrays.equals(record.recoveryLineage(), lineage)) {
                    throw new IllegalStateException("channel lifecycle record belongs to another lineage");
                }
                record.mutation().requireAtOrBefore(operation);
                final var context = record.body().request().channel().context();
                final var owner = targetOwner(context.target(), context.accountingIncarnation());
                result = new Charge(owner, resources(owner.accounting().recordCharge(
                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                        key.length, payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetChannelSlot.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var slot = com.nereusstream.delay.protocol.TargetChannelSlot.decodeForStore(
                        key, payload, scope.shard());
                if (!Arrays.equals(slot.recoveryLineage(), lineage)) {
                    throw new IllegalStateException("channel slot belongs to another lineage");
                }
                slot.mutation().requireAtOrBefore(operation);
                final var context = slot.channel().context();
                final var owner = targetOwner(context.target(), context.accountingIncarnation());
                result = new Charge(owner, resources(owner.accounting().recordCharge(
                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                        key.length, payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var mapping = com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.decodeForStore(key, payload, scope.shard());
                if (!Arrays.equals(mapping.recoveryLineage(), lineage)) { throw new IllegalStateException("Target K2 mapping lineage differs"); }
                mapping.mutation().requireAtOrBefore(operation);
                final var context = mapping.admission().channel().context();
                final var owner = targetOwner(context.target(), context.accountingIncarnation());
                result = new Charge(owner, resources(owner.accounting().recordCharge(
                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var slot = com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.decodeForStore(key, payload, scope.shard());
                if (!Arrays.equals(slot.recoveryLineage(), lineage)) { throw new IllegalStateException("Target K2 slot lineage differs"); }
                slot.mutation().requireAtOrBefore(operation);
                final var context = slot.channel().context();
                final var owner = targetOwner(context.target(), context.accountingIncarnation());
                result = new Charge(owner, resources(owner.accounting().recordCharge(
                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }
            case TargetNativePolicyScope.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var nativeScope = TargetNativePolicyScope.decodeForStore(key, payload, scope.shard());
                result = metadata(TargetQuotaMetadataRecords.nativeScope(
                        targetOwner(nativeScope.target(), nativeScope.accountingIncarnation()), key, payload));
            }
            case TargetNativePolicySnapshot.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var snapshot = TargetNativePolicySnapshot.decode(payload);
                final byte[] scopeKey = TargetKeyCodec.nativePolicyScope(snapshot.policyScopeDigest());
                final var nativeScope = TargetNativePolicyScope.decodeForStore(
                        scopeKey,
                        payload(ColumnFamily.META, scopeKey, TargetNativePolicyScope.VALUE_TYPE),
                        scope.shard());
                result = metadata(TargetQuotaMetadataRecords.nativeSnapshot(
                        targetOwner(nativeScope.target(), nativeScope.accountingIncarnation()),
                        nativeScope,
                        key,
                        payload));
            }
            case CanonicalTargetPartition.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.IDENTITY,
                        CanonicalTargetPartition.decodeForStore(key, payload).id(),
                        key,
                        payload);
            }
            case TargetDispatchCompatibility.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.DISPATCH,
                        TargetDispatchCompatibility.decode(payload).target(),
                        key,
                        payload);
            }
            case TargetControlScope.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.CONTROL,
                        TargetControlScope.decode(payload).target(),
                        key,
                        payload);
            }
            case TargetMembershipGrant.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var member = TargetMembershipGrant.decodeForStore(key, payload, scope.shard());
                final byte[] policyKey = TargetKeyCodec.membershipPolicy(member.authorityPolicyRef());
                final var policy = TargetMembershipPolicy.decodeForStore(
                        policyKey,
                        payload(ColumnFamily.META, policyKey, TargetMembershipPolicy.VALUE_TYPE),
                        scope.shard());
                policy.requireGrant(member);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_GRANT,
                        member.offered().target(),
                        key,
                        payload);
            }
            case TargetMembershipPolicy.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_POLICY,
                        TargetMembershipPolicy.decodeForStore(key, payload, scope.shard())
                                .offered()
                                .target(),
                        key,
                        payload);
            }
            case TargetMembershipClosureRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var closure = TargetMembershipClosureRecord.decodeForStore(key, payload, scope.shard(), lineage);
                closure.mutation().requireAtOrBefore(operation);
                final byte[] memberKey =
                        TargetKeyCodec.membershipGrant(closure.body().request().value());
                final var member = TargetMembershipGrant.decodeForStore(
                        memberKey,
                        payload(ColumnFamily.META, memberKey, TargetMembershipGrant.VALUE_TYPE),
                        scope.shard());
                final var target = member.offered().target();
                final var targetScope = scope.forTarget(target);
                final byte[] allocationKey = Bytes.concat(
                        new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                        targetScope.keySuffix());
                final var allocation = TargetQuotaGrantActivation.decodeForStore(
                        allocationKey,
                        payload(ColumnFamily.META, allocationKey, TargetQuotaGrantActivation.VALUE_TYPE),
                        scope.shard(),
                        scope.tenantScope());
                if (allocation.allocation() == null) {
                    throw new IllegalStateException("membership closure lacks first Target allocation");
                }
                result = metadata(TargetQuotaMetadataRecords.membershipClosure(
                        allocation,
                        descriptor(allocation.allocation().identity()),
                        physical(target),
                        member,
                        key,
                        payload));
            }
            case TargetNativePolicyControlRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var record = TargetNativePolicyControlRecord.decodeForStore(key, payload, scope.shard(), lineage);
                record.mutation().requireAtOrBefore(operation);
                final var nativeScope = record.body().request().scope();
                final byte[] scopeKey = TargetKeyCodec.nativePolicyScope(nativeScope.digest());
                final var retainedScope = TargetNativePolicyScope.decodeForStore(
                        scopeKey,
                        payload(ColumnFamily.META, scopeKey, TargetNativePolicyScope.VALUE_TYPE),
                        scope.shard());
                if (!nativeScope.equals(retainedScope)) {
                    throw new IllegalStateException("Target Native authority differs from its retained scope");
                }
                if (record.grant() != null) {
                    final byte[] grantKey =
                            TargetKeyCodec.membershipGrant(record.grant().digest());
                    final var storedGrant = TargetMembershipGrant.decodeForStore(
                            grantKey,
                            payload(ColumnFamily.META, grantKey, TargetMembershipGrant.VALUE_TYPE),
                            scope.shard());
                    final byte[] policyKey = TargetKeyCodec.membershipPolicy(storedGrant.authorityPolicyRef());
                    final var policy = TargetMembershipPolicy.decodeForStore(
                            policyKey,
                            payload(ColumnFamily.META, policyKey, TargetMembershipPolicy.VALUE_TYPE),
                            scope.shard());
                    policy.requireGrant(storedGrant);
                    if (!record.grant().equals(storedGrant)) {
                        throw new IllegalStateException("Target Native authority differs from its retained B2 grant");
                    }
                    final byte[] closureKey = TargetKeyCodec.membershipClosure(storedGrant.digest());
                    final byte[] rawClosure = reader.projected(ColumnFamily.META, closureKey, overlay);
                    if (rawClosure != null) {
                        final var closure = TargetMembershipClosureRecord.decodeForStore(
                                closureKey,
                                TargetValueEnvelope.decode(rawClosure, TargetMembershipClosureRecord.VALUE_TYPE)
                                        .payload(),
                                scope.shard(),
                                lineage);
                        closure.requireGrant(storedGrant);
                        closure.mutation().requireAtOrBefore(operation);
                    }
                }
                result = metadata(TargetQuotaMetadataRecords.nativeControl(
                        targetOwner(retainedScope.target(), retainedScope.accountingIncarnation()),
                        retainedScope,
                        key,
                        payload));
            }
            case TargetResultRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.DEDUPE);
                final var record = TargetResultRecord.decode(payload);
                record.requireStored(key, value.valueType(), payload);
                final var owner = descriptor(record.primaryIdentity());
                record.requireOwner(owner);
                record.mutation().requireAtOrBefore(operation);
                TargetRetirementSourceHistoryStore.requireFirstHistory(reader,overlay,scope,lineage,root.owner(),record,operation);
                if (record.kind().referencesFirst()) {
                    final byte[] firstKey = Bytes.concat(
                            new byte[] {
                                (byte)
                                        (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM
                                                ? TargetKeyCodec.RESULT_SYSTEM_TAG
                                                : TargetKeyCodec.RESULT_COMMAND_TAG),
                                1
                            },
                            record.logicalId());
                    record.requireFirst(TargetResultRecord.decode(
                            payload(ColumnFamily.DEDUPE, firstKey, TargetResultRecord.VALUE_TYPE)));
                }
                result = new Charge(owner, resources(record.recordCharge()));
            }
            case TargetClaimRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.INFLIGHT);
                final var claim = TargetClaimRecord.decode(payload);
                claim.requireStored(key);
                final var charge = com.nereusstream.delay.protocol.TargetQuotaClaimCharge.decode(
                        payload(ColumnFamily.META, claim.chargeKey(), TargetQuotaClaimCharge.VALUE_TYPE));
                claim.requireCharge(charge);
                claim.requireCurrent(message(claim.work().locator().messageId()));
                if (claim.policyHead() != null) {
                    final byte[] scopeKey = TargetKeyCodec.nativePolicyScope(claim.policyHead().scopeDigest());
                    final var nativeScope = TargetNativePolicyScope.decodeForStore(scopeKey,
                            payload(ColumnFamily.META, scopeKey, TargetNativePolicyScope.VALUE_TYPE), scope.shard());
                    final byte[] bindingKey = TargetKeyCodec.scheduleBinding(claim.work().locator().scheduleBindingDigest());
                    final var binding = TargetScheduleBinding.decodeForStore(bindingKey,
                            payload(ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE), scope.shard());
                    nativeScope.requireBinding(binding);
                    final var snapshot = TargetNativePolicySnapshot.decode(payload(ColumnFamily.META,
                            TargetKeyCodec.nativePolicySnapshot(claim.policyHead().snapshotDigest()),
                            TargetNativePolicySnapshot.VALUE_TYPE));
                    snapshot.requireScope(nativeScope);
                    if (!snapshot.headRef(claim.policyHead().oxiaVersion()).equals(claim.policyHead())) {
                        throw new IllegalStateException("Native Claim changes its retained snapshot/head reference");
                    }
                }
                final var owner = descriptor(charge.primaryIdentity());
                charge.requireDescriptor(owner);
                claim.creation().requireAtOrBefore(operation);
                result = new Charge(
                        owner,
                        resources(owner.accounting()
                                .recordCharge(
                                        com.nereusstream.delay.protocol.TargetQuotaAccounting.RecordClass.STATE,
                                        key.length,
                                        payload.length)));
            }
            case TargetQuotaClaimCharge.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var claim = TargetQuotaClaimCharge.decode(payload);
                claim.requireStored(key, value.valueType(), payload);
                final byte[] businessKey = TargetClaimRecord.key(claim.claimId());
                final var businessClaim = TargetClaimRecord.decode(
                        payload(ColumnFamily.INFLIGHT, businessKey, TargetClaimRecord.VALUE_TYPE));
                businessClaim.requireStored(businessKey);
                businessClaim.requireCharge(claim);
                businessClaim.requireCurrent(message(claim.work().locator().messageId()));
                final var owner = descriptor(claim.primaryIdentity());
                claim.requireDescriptor(owner);
                claim.creation().requireAtOrBefore(operation);
                result = new Charge(owner, resources(claim.contribution()));
            }
            case com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var artifact = com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.decodeForStore(
                        key, payload, scope.shard(), scope.tenantScope(), lineage);
                artifact.createdAt().requireAtOrBefore(operation);
                final var owner = descriptor(artifact.primaryOwner().identity());
                if (!owner.accounting().equals(artifact.primaryOwner().accounting())) {
                    throw new IllegalStateException("external blob changes frozen primary accounting");
                }
                // STATE is this native owner record; EVIDENCE is its distinct exact external key/value object.
                result = new Charge(owner, resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,
                        key.length, payload.length).add(artifact.blobCharge())));
            }
            case com.nereusstream.delay.protocol.TargetExternalEvidenceReference.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var ref = com.nereusstream.delay.protocol.TargetExternalEvidenceReference.decodeForStore(key, payload, scope.shard(), lineage);
                ref.mutation().requireAtOrBefore(operation);
                final var owner = descriptor(ref.referenceOwner());
                if (!owner.accounting().equals(ref.accounting())) { throw new IllegalStateException("external reference changes frozen accounting"); }
                final byte[] ownerKey = com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.key(ref.kind(), ref.artifactDigest());
                com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.decodeForStore(ownerKey,
                        payload(ColumnFamily.META, ownerKey, com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.VALUE_TYPE),
                        scope.shard(), scope.tenantScope(), lineage);
                result = new Charge(owner, resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var artifact=com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.decodeForStore(key,payload,scope.shard(),scope.tenantScope(),lineage);
                artifact.adoptedAt().requireAtOrBefore(operation);
                final var owner=descriptor(artifact.primaryOwner().identity());
                if(!owner.accounting().equals(artifact.primaryOwner().accounting()))throw new IllegalStateException("recovery blob changed frozen accounting");
                final var ref=artifact.referenceRecord();
                for(byte[] refKey:List.of(ref.key(),ref.attemptKey())){
                    final var actual=com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.decodeForStore(refKey,
                            payload(ColumnFamily.META,refKey,com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.VALUE_TYPE),scope.shard(),lineage);
                    actual.requireOwner(artifact);
                }
                final byte[] budgetKey=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT},ref.attemptId());
                final var budget=TargetQuotaAttemptBudget.decodeForStore(budgetKey,payload(ColumnFamily.META,budgetKey,TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
                ref.requireAttempt(budget);
                if(budget.resolvedAt()==null||!budget.resolvedAt().equals(artifact.adoptedAt())||!Arrays.equals(budget.admissionDigest(),artifact.admissionHash()))
                    throw new IllegalStateException("recovery owner lacks its original resolved Attempt");
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length).add(artifact.blobCharge())));
            }
            case com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var ref=com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.decodeForStore(key,payload,scope.shard(),lineage);
                ref.adoptedAt().requireAtOrBefore(operation);final var owner=descriptor(ref.referenceOwner());
                if(!owner.accounting().equals(ref.accounting()))throw new IllegalStateException("recovery reference changed frozen accounting");
                final byte[] ownerKey=com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.key(ref.proofDigest());
                final var artifact=com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.decodeForStore(ownerKey,
                        payload(ColumnFamily.META,ownerKey,com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.VALUE_TYPE),scope.shard(),scope.tenantScope(),lineage);
                ref.requireOwner(artifact);
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var intent=com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.decodeForStore(key,payload,scope,lineage);
                TargetObjectPayloadReclaim.requirePending(reader,overlay,intent,operation);
                final var expected=intent.expectedOwner();final var owner=descriptor(expected.primaryIdentity());
                if(!owner.accounting().equals(expected.accounting()))throw new IllegalStateException("OBJECT intent changed frozen accounting");
                result=new Charge(owner,resources(intent.recordCharge()));
            }
            case com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var retiring=com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.decodeForStore(
                        key,payload,scope.shard(),scope.tenantScope(),lineage);
                final var owner=TargetEvidenceRetirementStore.requireRetiringState(reader,overlay,scope,lineage,root.owner(),retiring,operation);
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var confirmed=com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.decodeForStore(
                        key,payload,scope.shard(),scope.tenantScope(),lineage);
                TargetEvidenceRetirementStore.requireConfirmedState(reader,overlay,scope,lineage,root.owner(),confirmed,operation);
                final var frozen=confirmed.feeOwner();final var owner=descriptor(frozen.identity());
                if(!owner.accounting().equals(frozen.accounting()))throw new IllegalStateException("confirmed row changed frozen accounting");
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var tip=com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.decodeForStore(key,payload,scope,lineage);
                TargetRetirementSourceHistoryStore.requireTip(reader,overlay,scope,lineage,root.owner(),tip,operation);
                final var owner=descriptor(root.owner());
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var retained=com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.decodeForStore(key,payload,scope,lineage);
                TargetRetirementSourceHistoryStore.requireFirst(reader,overlay,scope,lineage,root.owner(),retained,operation);
                final var owner=descriptor(root.owner());
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetProtocolActivationRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var activation=com.nereusstream.delay.protocol.TargetProtocolActivationRecord.decodeForStore(key,payload,scope,lineage);
                TargetProtocolActivationStore.requireApplied(reader,overlay,scope,lineage,root.owner(),activation,operation);
                final var owner=descriptor(root.owner());
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetSloObservationGeneration.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var generation = com.nereusstream.delay.protocol.TargetSloObservationGeneration.decodeForStore(key, payload, scope, lineage);
                TargetSloObservationStore.requireOrigin(reader, overlay, scope, lineage, root.owner(), generation, operation);
                final var owner = descriptor(root.owner());
                result = new Charge(owner, resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetSloWriterEpoch.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var writer=com.nereusstream.delay.protocol.TargetSloWriterEpoch.decodeForStore(key,payload,scope,lineage);
                TargetSloWriterEpochStore.requireOrigin(reader,overlay,scope,lineage,root.owner(),writer,operation);
                final var owner=descriptor(root.owner());
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetParentControlCredit.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var credit=com.nereusstream.delay.protocol.TargetParentControlCredit.decodeForStore(key,payload,scope,lineage);
                final var owner=descriptor(root.owner());
                TargetParentControlCreditStore.requireOrigin(reader,overlay,scope,lineage,owner,credit,operation);
                result=new Charge(owner,credit.ownContribution());
            }
            case com.nereusstream.delay.protocol.TargetScopeControlObligation.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var obligation=com.nereusstream.delay.protocol.TargetScopeControlObligation.decodeForStore(key,payload,scope,lineage);
                final var owner=descriptor(root.owner());
                TargetScopeControlObligationStore.requireOrigin(reader,overlay,scope,lineage,owner,obligation,operation);
                result=new Charge(owner,obligation.ownContribution());
            }
            case com.nereusstream.delay.protocol.TargetSloObservationSample.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var sample = com.nereusstream.delay.protocol.TargetSloObservationSample.decodeForStore(key, payload, scope, lineage);
                TargetSloObservationSamples.requireOrigin(reader, overlay, scope, lineage, root.owner(), sample, operation);
                final var owner = descriptor(root.owner());
                result = new Charge(owner, resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetSloObservationEventRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var event=com.nereusstream.delay.protocol.TargetSloObservationEventRecord.decodeForStore(key,payload,scope,lineage);
                TargetSloObservationEventStore.requireOrigin(reader,overlay,scope,lineage,root.owner(),event,operation);
                final var owner=descriptor(root.owner());
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetSloCustodyRecord.VALUE_TYPE -> {
                requireFamily(family,ColumnFamily.META);
                final var custody=com.nereusstream.delay.protocol.TargetSloCustodyRecord.decodeForStore(key,payload,scope,lineage);
                TargetSloObservationEventStore.requireCustodyOrigin(reader,overlay,scope,lineage,root.owner(),custody,operation);
                final var owner=descriptor(root.owner());
                result=new Charge(owner,resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key.length,payload.length)));
            }
            case com.nereusstream.delay.protocol.TargetSloObservationState.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var state = com.nereusstream.delay.protocol.TargetSloObservationState.decodeForStore(key, payload, scope, lineage);
                final var generationKey = com.nereusstream.delay.protocol.TargetSloObservationGeneration.key(scope, state.generation());
                final var generationRaw = Objects.requireNonNull(reader.projected(ColumnFamily.META, generationKey, overlay), "original Target SLO generation");
                final var generation = com.nereusstream.delay.protocol.TargetSloObservationGeneration.decodeForStore(generationKey,
                        TargetValueEnvelope.decode(generationRaw, com.nereusstream.delay.protocol.TargetSloObservationGeneration.VALUE_TYPE).payload(), scope, lineage);
                state.requireGeneration(generation);
                TargetSloObservationStore.requireOrigin(reader, overlay, scope, lineage, root.owner(), generation, operation);
                final var owner = descriptor(root.owner());
                result = new Charge(owner, resources(owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
            }
            case TargetQuotaAttemptBudget.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var budget = TargetQuotaAttemptBudget.decodeForStore(key, payload, scope.shard());
                final var owner = descriptor(budget.primaryIdentity());
                owner.requireAttemptBudget(budget);
                budget.mutation().requireAtOrBefore(operation);
                TargetRetirementSourceHistoryStore.requireAttemptReclaim(reader,overlay,scope,lineage,root.owner(),budget,operation);
                result = new Charge(owner, resources(TargetExternalEvidenceStore.remainingAttemptCharge(reader, overlay,
                        budget, scope, lineage).add(root.attemptRecordCharge(budget))));
            }
            case TargetQuotaGrantActivation.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var grant =
                        TargetQuotaGrantActivation.decodeForStore(key, payload, scope.shard(), scope.tenantScope());
                grant.mutation().requireAtOrBefore(operation);
                if (grant.allocation() != null) {
                    final var owner = descriptor(grant.allocation().identity());
                    if (!owner.allocation().equals(grant.allocation().allocation())
                            || !owner.accounting().equals(grant.allocation().accounting())
                            || !Arrays.equals(
                                    owner.tenantScope(), grant.allocation().tenantScope())
                            || !Arrays.equals(
                                    owner.recoveryLineage(), grant.allocation().recoveryLineage())) {
                        throw new IllegalStateException(
                                "grant activation first allocation differs from its descriptor");
                    }
                }
                return null;
            }
            case com.nereusstream.delay.protocol.TargetMigrationArchiveRow.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = TargetMigrationArchiveStore.nativeCharge(reader, overlay, scope, lineage, operation, key, payload, true);
            }
            case com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.VALUE_TYPE -> {
                result = com.nereusstream.delay.protocol.TargetMigrationArchiveState.isKey(key)
                        ? TargetMigrationArchiveStore.nativeCharge(reader, overlay, scope, lineage, operation, key, payload, false)
                        : TargetMigrationRetirementStore.nativeCharge(reader, overlay, scope, lineage, operation, family, key, payload);
            }
            case com.nereusstream.delay.protocol.TargetMigrationRoot.VALUE_TYPE,
                    com.nereusstream.delay.protocol.TargetMigrationOrigin.VALUE_TYPE,
                    com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.VALUE_TYPE,
                    com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.VALUE_TYPE,
                    com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.VALUE_TYPE -> {
                result = TargetMigrationStore.nativeCharge(reader, overlay, scope, lineage, operation,
                        family, key, payload, value.valueType());
            }
            default ->
                throw new IllegalArgumentException("unhandled business record accounting type: " + value.valueType());
        }
        requireOwner(result.owner());
        return result;
    }

    public TargetQuotaIncarnation descriptor(final TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var descriptor = TargetQuotaIncarnation.decodeForStore(
                key,
                payload(ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (!descriptor.identity().equals(identity)) {
            throw new IllegalStateException("descriptor identity mismatch");
        }
        requireOwner(descriptor);
        return descriptor;
    }

    private void requireOwner(final TargetQuotaIncarnation owner) {
        if (!owner.identity().shard().equals(scope.shard())
                || !Arrays.equals(owner.tenantScope(), scope.tenantScope())
                || !Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("record accounting crossed Shard/tenant/lineage");
        }
        final var latest = owner.latestMutation();
        if (latest.sequence() == operation.sequence() && latest.localOrdinal() == operation.localOrdinal()
                && !latest.equals(operation)) {
            final var migration = TargetMigrationStore.decodeRoot(reader.projected(ColumnFamily.META,
                    com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard()), overlay));
            if (migration != null && owner.allocation().equals(migration.plan().baseline())
                    && operation.equals(com.nereusstream.delay.protocol.TargetMigrationRoot.baselineFor(migration.plan()))) {
                if (owner.draining()) throw new IllegalStateException("import baseline cannot invent a drained owner");
            } else latest.requireAtOrBefore(operation);
        } else latest.requireAtOrBefore(operation);
    }

    private TargetQuotaIncarnation targetOwner(final TargetPartitionId target, final byte[] incarnation) {
        return descriptor(
                new TargetQuotaIdentity(TargetQuotaIdentity.Kind.TARGET, scope.shard(), incarnation, target, null));
    }

    private TargetQuotaPayloadOwner payloadOwner(final DelayMessageId messageId) {
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, messageId.bytes());
        return TargetQuotaPayloadOwner.decodeForStore(
                key,
                payload(ColumnFamily.META, key, TargetQuotaPayloadOwner.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
    }

    private TargetMessageRecord message(final DelayMessageId messageId) {
        final byte[] key = TargetKeyCodec.message(messageId);
        return TargetMessageRecord.decodeForStore(
                key, payload(ColumnFamily.ID, key, TargetMessageRecord.VALUE_TYPE), scope.shard());
    }

    private TargetOrderState order(final TargetPartitionId target, final byte[] orderingDomain) {
        final byte[] key = TargetKeyCodec.orderState(target, orderingDomain);
        final var state = TargetOrderState.decode(payload(ColumnFamily.META, key, TargetOrderState.VALUE_TYPE));
        if (!Arrays.equals(key, state.encodedKey())) {
            throw new IllegalStateException("strict accounting key mismatch");
        }
        return state;
    }

    private CanonicalTargetPartition physical(final TargetPartitionId target) {
        final byte[] key = TargetKeyCodec.identity(target);
        return CanonicalTargetPartition.decodeForStore(
                key, payload(ColumnFamily.META, key, CanonicalTargetPartition.VALUE_TYPE));
    }

    private Charge shared(
            final TargetQuotaMetadataRecords.SharedRole role,
            final TargetPartitionId target,
            final byte[] key,
            final byte[] payload) {
        final var targetScope = scope.forTarget(target);
        final byte[] grantKey =
                Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1}, targetScope.keySuffix());
        final byte[] rawGrant = reader.projected(ColumnFamily.META, grantKey, overlay);
        if (rawGrant == null) {
            final var migration = TargetMigrationStore.decodeRoot(reader.projected(ColumnFamily.META,
                    com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard()), overlay));
            if (migration == null) throw new IllegalStateException("shared metadata lacks first allocation origin");
            final var origin = TargetMigrationStore.origin(reader, overlay, migration.plan(), ColumnFamily.META, key);
            origin.requireRow(ColumnFamily.META, key, TargetValueEnvelope.encode(
                    switch (role) {
                        case IDENTITY -> CanonicalTargetPartition.VALUE_TYPE;
                        case DISPATCH -> TargetDispatchCompatibility.VALUE_TYPE;
                        case CONTROL -> TargetControlScope.VALUE_TYPE;
                        case MEMBERSHIP_GRANT -> TargetMembershipGrant.VALUE_TYPE;
                        case MEMBERSHIP_POLICY -> TargetMembershipPolicy.VALUE_TYPE;
                    }, payload));
            final var owner = descriptor(origin.owner().identity());
            if (!Objects.equals(owner.identity().target(), target)
                    || !owner.allocation().equals(migration.plan().baseline()))
                throw new IllegalStateException("imported shared metadata changed original C allocation");
            final var identity = physical(target);
            switch (role) {
                case IDENTITY -> CanonicalTargetPartition.decodeForStore(key, payload);
                case DISPATCH -> TargetDispatchCompatibility.decodeForStore(key, payload, identity);
                case CONTROL -> TargetControlScope.decodeForStore(key, payload, target, scope.shard());
                case MEMBERSHIP_GRANT -> TargetMembershipGrant.decodeForStore(key, payload, scope.shard());
                case MEMBERSHIP_POLICY -> TargetMembershipPolicy.decodeForStore(key, payload, scope.shard());
            }
            return new Charge(owner, resources(owner.accounting().recordCharge(
                    TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
        }
        final var grant = TargetQuotaGrantActivation.decodeForStore(
                grantKey,
                TargetValueEnvelope.decode(rawGrant, TargetQuotaGrantActivation.VALUE_TYPE).payload(),
                scope.shard(),
                scope.tenantScope());
        if (grant.allocation() == null) {
            throw new IllegalStateException("shared metadata lacks first allocation origin");
        }
        return metadata(TargetQuotaMetadataRecords.shared(
                role, grant, descriptor(grant.allocation().identity()), physical(target), key, payload));
    }

    private Charge messageCharge(final TargetQuotaPayloadOwner owner, final TargetQuotaMessageRecords.Record record) {
        final var descriptor = descriptor(owner.primaryIdentity());
        requirePayloadOwner(descriptor, owner);
        return new Charge(descriptor, resources(record.charge()));
    }

    private void requirePayloadOwner(final TargetQuotaIncarnation descriptor, final TargetQuotaPayloadOwner owner) {
        if (owner.summaryQualified() && owner.summaryMutation() != null)
            owner.summaryMutation().requireAtOrBefore(operation);
        if (descriptor.allocation().sequence() == owner.mutation().sequence()
                && descriptor.allocation().localOrdinal() == owner.mutation().localOrdinal()
                && !descriptor.allocation().equals(owner.mutation())) {
            final var migration = Objects.requireNonNull(TargetMigrationStore.decodeRoot(reader.projected(ColumnFamily.META,
                    com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard()), overlay)), "imported payload migration root");
            if (!owner.mutation().equals(com.nereusstream.delay.protocol.TargetMigrationRoot.baselineFor(migration.plan())))
                throw new IllegalStateException("payload owner changed its complete source allocation stamp");
            final byte[] bindingKey = TargetKeyCodec.scheduleBinding(owner.initialBindingDigest());
            final var binding = TargetScheduleBinding.decodeForStore(bindingKey,
                    payload(ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE), scope.shard());
            descriptor.requireImportedPayloadOwner(owner, binding, migration.plan());
        } else descriptor.requirePayloadOwner(owner);
    }

    private Charge metadata(final TargetQuotaMetadataRecords.Record record) {
        record.requireCurrent(key -> {
            final byte[] raw = reader.projected(ColumnFamily.META, key, overlay);
            if (raw == null) {
                return null;
            }
            final var value = TargetValueEnvelope.decodeAny(raw);
            return new TargetQuotaMetadataRecords.Stored(value.valueType(), value.payload());
        });
        return new Charge(record.owner(), record.contribution());
    }

    private byte[] payload(final ColumnFamily family, final byte[] key, final int type) {
        final byte[] raw = reader.projected(family, key, overlay);
        if (raw == null) {
            throw new IllegalStateException("missing frozen accounting dependency");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    private static void requireFamily(final ColumnFamily actual, final ColumnFamily expected) {
        if (actual != expected) {
            throw new IllegalArgumentException("business accounting CF/type mismatch");
        }
    }

    public static TargetQuotaUsage resources(final CapacityVector resources) {
        return new TargetQuotaUsage(resources, 0, 0, 0, 0);
    }
}
