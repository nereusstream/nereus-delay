package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import com.nereusstream.delay.runtime.TargetResultRecord;

/** Closed Source requests, never database edits, caller deltas, or a protection-release authority. */
public record TargetResourceReclaimBody(TargetQuotaScope scope, byte[] lineage, byte[] deploymentRoot,
        byte[] namespace, long retryUntil, RecoveryFloorRef floor, List<Operation> operations,TargetInlinePayloadSourceFacts sourceFacts,
        TargetNativeReclaimFacts nativeFacts) {
    public static final int BODY_VERSION=9;
    public static final int INLINE_BODY_VERSION=10;
    public static final int SELF_CONTAINED_INLINE_BODY_VERSION=11;
    public static final int NATIVE_RETAINED_BODY_VERSION=12;
    public static final int MAX_NATIVE_CANONICAL_BYTES=32*1024*1024;
    public static final int MAX_OPERATIONS=32;
    public static final int MAX_CANONICAL_BYTES=8*1024*1024;
    private static final byte[] ID_DOMAIN=Bytes.utf8("nereus-delay-target-resource-reclaim-logical-identity\0");
    public TargetResourceReclaimBody(TargetQuotaScope scope,byte[] lineage,byte[] root,byte[] namespace,long retryUntil,
            RecoveryFloorRef floor,List<Operation> operations){this(scope,lineage,root,namespace,retryUntil,floor,operations,null,null);}
    public TargetResourceReclaimBody(TargetQuotaScope scope,byte[] lineage,byte[] root,byte[] namespace,long retryUntil,
            RecoveryFloorRef floor,List<Operation> operations,TargetInlinePayloadSourceFacts facts){this(scope,lineage,root,namespace,retryUntil,floor,operations,facts,null);}

    public sealed interface Operation permits UnusedAttemptReserve, InlinePayloadRelease,RetainedRecordDelete,RetainedAllocationRelease {
        byte[] identity();
        byte[] canonicalBytes();
    }
    /** Closed leaf kinds derive their one DEDUPE key from the full semantic value; no arbitrary delete address. */
    public record RetainedRecordDelete(TargetResultRecord expected) implements Operation {
        public RetainedRecordDelete {
            Objects.requireNonNull(expected);
            if(expected.kind()!=TargetResultRecord.Kind.RESULT&&expected.kind()!=TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED)
                throw new IllegalArgumentException("only query-result and standalone expired POSITION leaves are supported");
        }
        public int action(){return expected.kind()==TargetResultRecord.Kind.RESULT?1:2;}
        public byte[] identity(){return Bytes.concat(new byte[]{3,(byte)action()},expected.key());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,3);
            CanonicalProtobuf.uint32(out,3,action());CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());});}
    }
    /** Confirmed recovery-object shares have one immutable ownership/settlement identity per original Attempt. */
    public record RetainedAllocationRelease(TargetQuotaAttemptBudget expected,List<TargetEvidenceConfirmedRecord> confirmations) implements Operation {
        public RetainedAllocationRelease {
            Objects.requireNonNull(expected);
            if(expected.phase()!=TargetQuotaAttemptBudget.Phase.RETAINED||confirmations.size()>MAX_OPERATIONS)
                throw new IllegalArgumentException("bounded retained Attempt settlement required");
            final var sorted=new java.util.TreeMap<String,TargetEvidenceConfirmedRecord>();
            for(var confirmation:confirmations){
                if(!(confirmation.body().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery)
                        ||!Arrays.equals(recovery.owner().reference().attemptId(),expected.publishAttemptId())
                        ||!recovery.owner().primaryOwner().identity().equals(expected.primaryIdentity())
                        ||!recovery.owner().primaryOwner().accounting().equals(expected.accounting())
                        ||!Arrays.equals(recovery.owner().admissionHash(),expected.admissionDigest())
                        ||!recovery.owner().adoptedAt().equals(expected.resolvedAt())
                        ||sorted.put(Bytes.hex(confirmation.key()),confirmation)!=null)
                    throw new IllegalArgumentException("recovery settlement changed original Attempt/frozen allocation origin");
            }
            confirmations=List.copyOf(sorted.values());
            if(!expected.allocated().isZero()&&confirmations.isEmpty())throw new IllegalArgumentException("nonzero allocation requires actual one-time settlement facts");
            CapacityVector remaining=expected.allocated();for(var confirmation:confirmations)
                remaining=remaining.subtract(((TargetExternalEvidenceObjectResource.Recovery)confirmation.body().resource().provenance()).owner().adoptedCharge());
        }
        public CapacityVector remaining(){CapacityVector value=expected.allocated();for(var confirmation:confirmations)
            value=value.subtract(((TargetExternalEvidenceObjectResource.Recovery)confirmation.body().resource().provenance()).owner().adoptedCharge());return value;}
        public byte[] identity(){return Bytes.concat(new byte[]{3,3},expected.publishAttemptId());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,3);
            CanonicalProtobuf.uint32(out,3,3);CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());
            for(var confirmation:confirmations)CanonicalProtobuf.bytes(out,5,confirmation.canonicalBytes());});}
    }
    /** The full original value freezes revision, digest, owner, allocation, resolution and charge class. */
    public record UnusedAttemptReserve(TargetQuotaAttemptBudget expected) implements Operation {
        public UnusedAttemptReserve {
            Objects.requireNonNull(expected);
            if(expected.phase()!=TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR)
                throw new IllegalArgumentException("unused reserve requires a Source-resolved attempt");
        }
        public byte[] identity(){return Bytes.concat(new byte[]{1},expected.publishAttemptId());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,1);
            CanonicalProtobuf.bytes(out,3,expected.canonicalBytes());
        });}
    }
    /** Full retained owner plus exact primary value digest; policy and dynamic closure are independent inputs. */
    public record InlinePayloadRelease(TargetQuotaPayloadOwner expected,TargetMessageLocator locator,long stateVersion,
            byte[] primaryDigest,byte[] terminalDigest) implements Operation {
        public InlinePayloadRelease {
            Objects.requireNonNull(expected);Objects.requireNonNull(locator);
            primaryDigest=TargetCompatibilityCodec.assigned(primaryDigest,32,"exact primary Message digest");
            terminalDigest=TargetCompatibilityCodec.assigned(terminalDigest,32,"exact retained terminal record digest");
            if(expected.kind()!=TargetQuotaPayloadOwner.Kind.INLINE||expected.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED
                    ||stateVersion==0||!expected.messageId().equals(locator.messageId())||!expected.primaryIdentity().target().equals(locator.target())
                    ||!Arrays.equals(expected.primaryIdentity().accountingIncarnation(),locator.accountingIncarnation())
                    ||!Arrays.equals(expected.initialBindingDigest(),locator.scheduleBindingDigest()))
                throw new IllegalArgumentException("inline release requires an exact retained INLINE owner");
        }
        @Override public byte[] primaryDigest(){return Bytes.copy(primaryDigest);}
        @Override public byte[] terminalDigest(){return Bytes.copy(terminalDigest);}
        public byte[] identity(){return Bytes.concat(new byte[]{2},expected.messageId().bytes());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,2);
            CanonicalProtobuf.bytes(out,3,expected.canonicalBytes());CanonicalProtobuf.bytes(out,4,primaryDigest);
            CanonicalProtobuf.bytes(out,5,locator.canonicalBytes());CanonicalProtobuf.uint64Bits(out,6,stateVersion);
            CanonicalProtobuf.bytes(out,7,terminalDigest);
        });}
    }
    public TargetResourceReclaimBody {
        Objects.requireNonNull(scope);Objects.requireNonNull(floor);
        lineage=TargetCompatibilityCodec.assigned(lineage,16,"reclaim lineage");
        deploymentRoot=TargetCompatibilityCodec.assigned(deploymentRoot,32,"reclaim deployment Root");
        namespace=TargetCompatibilityCodec.assigned(namespace,32,"reclaim namespace");
        if(scope.target()!=null||retryUntil<0||operations.isEmpty()||operations.size()>MAX_OPERATIONS
                ||!scope.shard().equals(floor.appliedSourcePosition().shardId())||!Arrays.equals(lineage,floor.recoveryLineageId()))
            throw new IllegalArgumentException("reclaim requires a bounded Source scope/operation group and same-lineage Floor");
        TargetSourcePosition.requireBounded(floor.appliedSourcePosition());
        final var ordered=new ArrayList<>(operations);ordered.sort((a,b)->Arrays.compareUnsigned(a.identity(),b.identity()));
        final boolean inline=ordered.getFirst() instanceof InlinePayloadRelease;
        final boolean nativeRetained=ordered.getFirst() instanceof RetainedRecordDelete||ordered.getFirst() instanceof RetainedAllocationRelease;
        if(sourceFacts!=null&&nativeFacts!=null)throw new IllegalArgumentException("reclaim source facts cannot mix kinds");
        byte[] previous=null;long bytes=floor.canonicalBytes().length+scope.canonicalBytes().length+256L;
        for(var operation:ordered){
            Objects.requireNonNull(operation);
            if(inline!=(operation instanceof InlinePayloadRelease)||nativeRetained!=(operation instanceof RetainedRecordDelete||operation instanceof RetainedAllocationRelease))
                throw new IllegalArgumentException("reclaim operation families cannot mix");
            final byte[] identity=operation.identity();
            if(previous!=null&&Arrays.compareUnsigned(previous,identity)>=0)throw new IllegalArgumentException("duplicate reclaim resource");
            previous=identity;
            switch(operation){case UnusedAttemptReserve unused->{
                final var expected=unused.expected();scope.requireRoute(expected.locator().messageId().routingId().shardId(),expected.tenantScope());
                if(!Arrays.equals(lineage,expected.recoveryLineage()))throw new IllegalArgumentException("reclaim changed frozen attempt lineage");
                expected.mutation().requireCoveredByFloor(floor);
            }case InlinePayloadRelease payload->{
                final var expected=payload.expected();scope.requireRoute(expected.messageId().routingId().shardId(),expected.tenantScope());
                if(!Arrays.equals(lineage,expected.recoveryLineage()))throw new IllegalArgumentException("inline reclaim changed frozen owner lineage");
                expected.mutation().requireCoveredByFloor(floor);
            }case RetainedRecordDelete record->{
                scope.requireRoute(record.expected().primaryIdentity().shard(),record.expected().tenantScope());
                if(!Arrays.equals(lineage,record.expected().recoveryLineage()))throw new IllegalArgumentException("record reclaim changed frozen lineage");
                record.expected().mutation().requireCoveredByFloor(floor);
            }case RetainedAllocationRelease allocation->{
                final var expected=allocation.expected();scope.requireRoute(expected.locator().messageId().routingId().shardId(),expected.tenantScope());
                if(!Arrays.equals(lineage,expected.recoveryLineage()))throw new IllegalArgumentException("allocation reclaim changed frozen lineage");
                expected.mutation().requireCoveredByFloor(floor);
                for(var confirmation:allocation.confirmations())confirmation.appliedAt().requireCoveredByFloor(floor);
            }}
            bytes=Math.addExact(bytes,operation.canonicalBytes().length+8L);
            if(bytes>(nativeRetained?MAX_NATIVE_CANONICAL_BYTES:MAX_CANONICAL_BYTES))throw new IllegalArgumentException("reclaim group exceeds encoded byte bound");
        }
        operations=List.copyOf(ordered);
        if(sourceFacts!=null){sourceFacts.requireFor(operations,floor);
            if(Math.addExact(bytes,sourceFacts.canonicalBytes().length+8L)>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("self-contained reclaim exceeds total byte bound");}
        if(nativeFacts!=null&&(!nativeRetained||Math.addExact(bytes,nativeFacts.canonicalBytes().length+8L)>MAX_NATIVE_CANONICAL_BYTES))
            throw new IllegalArgumentException("Native reclaim facts exceed their explicit new body bound");
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] deploymentRoot(){return Bytes.copy(deploymentRoot);}
    @Override public byte[] namespace(){return Bytes.copy(namespace);}
    public ShardId shard(){return scope.shard();}
    public boolean inlinePayloadRelease(){return operations.getFirst() instanceof InlinePayloadRelease;}
    public boolean selfContainedInlinePayloadRelease(){return sourceFacts!=null;}
    public boolean nativeRetainedReclaim(){return operations.getFirst() instanceof RetainedRecordDelete||operations.getFirst() instanceof RetainedAllocationRelease;}
    public int maximumCanonicalBytes(){return nativeRetainedReclaim()?MAX_NATIVE_CANONICAL_BYTES:MAX_CANONICAL_BYTES;}
    public int bodyVersion(){return nativeRetainedReclaim()?NATIVE_RETAINED_BODY_VERSION:selfContainedInlinePayloadRelease()?SELF_CONTAINED_INLINE_BODY_VERSION:inlinePayloadRelease()?INLINE_BODY_VERSION:BODY_VERSION;}
    public ProtocolTuple requiredTuple(){return nativeRetainedReclaim()?ProtocolTuple.targetNativeRetainedReclaim():selfContainedInlinePayloadRelease()?ProtocolTuple.targetSelfContainedInlinePayloadReclaim():inlinePayloadRelease()?ProtocolTuple.targetInlinePayloadReclaim():ProtocolTuple.targetResourceReclaim();}
    public TargetResourceReclaimBody withNativeFacts(TargetNativeReclaimFacts facts){
        if(nativeFacts!=null||!nativeRetainedReclaim())throw new IllegalArgumentException("Native reclaim facts already frozen or wrong family");
        final var body=new TargetResourceReclaimBody(scope,lineage,deploymentRoot,namespace,retryUntil,floor,operations,null,Objects.requireNonNull(facts));
        facts.requireFor(body);return body;
    }
    public TargetResourceReclaimBody withSourceFacts(TargetInlinePayloadSourceFacts facts){
        if(sourceFacts!=null)throw new IllegalStateException("inline SourceFacts already frozen");
        return new TargetResourceReclaimBody(scope,lineage,deploymentRoot,namespace,retryUntil,floor,operations,Objects.requireNonNull(facts));
    }
    private byte[] operationGroup(){return CanonicalProtobuf.message(out->{
        for(var operation:operations)CanonicalProtobuf.bytes(out,1,operation.canonicalBytes());
    });}
    /** Retry/deadline/Floor changes cannot create a fresh logical resource identity. The outer hash stays exact. */
    public byte[] logicalOperationIdentity(){return selfContainedInlinePayloadRelease()||nativeRetainedReclaim()
            ?Bytes.sha256(ID_DOMAIN,scope.canonicalBytes(),lineage,deploymentRoot,namespace,operationGroup(),Bytes.lp32(requiredTuple().canonicalBytes()))
            :Bytes.sha256(ID_DOMAIN,scope.canonicalBytes(),lineage,deploymentRoot,namespace,operationGroup());}
    public byte[] canonicalBytes(){
        if(nativeRetainedReclaim())Objects.requireNonNull(nativeFacts,"freeze original Native reclaim facts before encoding").requireFor(this);
        final byte[] value=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.bytes(out,1,new ShardSubject(shard()).canonicalBytes());
            CanonicalProtobuf.uint32(out,2,SystemMutationType.TARGET_RESOURCE_RECLAIM.wireValue());
            CanonicalProtobuf.int64(out,3,retryUntil);CanonicalProtobuf.uint32(out,9,bodyVersion());
            CanonicalProtobuf.bytes(out,10,scope.canonicalBytes());CanonicalProtobuf.bytes(out,11,lineage);
            CanonicalProtobuf.bytes(out,12,deploymentRoot);CanonicalProtobuf.bytes(out,13,namespace);
            CanonicalProtobuf.bytes(out,14,floor.canonicalBytes());CanonicalProtobuf.bytes(out,15,operationGroup());
            if(sourceFacts!=null)CanonicalProtobuf.bytes(out,16,sourceFacts.canonicalBytes());
            if(nativeRetainedReclaim())CanonicalProtobuf.bytes(out,16,Objects.requireNonNull(nativeFacts,"Native reclaim original facts must be frozen before encoding").canonicalBytes());
        });
        if(value.length>maximumCanonicalBytes())throw new IllegalArgumentException("reclaim body exceeds byte bound");return value;
    }
    public static TargetResourceReclaimBody decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_NATIVE_CANONICAL_BYTES,11,false,"Target reclaim body");
        QueryCodecSupport.requireNumbers(f,f.size()==11?new int[]{1,2,3,9,10,11,12,13,14,15,16}:new int[]{1,2,3,9,10,11,12,13,14,15},"Target reclaim body");
        final int version=QueryCodecSupport.uint32(f.get(3),9);
        if(QueryCodecSupport.uint32(f.get(1),2)!=SystemMutationType.TARGET_RESOURCE_RECLAIM.wireValue()
                ||version!=BODY_VERSION&&version!=INLINE_BODY_VERSION&&version!=SELF_CONTAINED_INLINE_BODY_VERSION&&version!=NATIVE_RETAINED_BODY_VERSION
                ||(version==SELF_CONTAINED_INLINE_BODY_VERSION||version==NATIVE_RETAINED_BODY_VERSION)!=(f.size()==11)
                ||version!=NATIVE_RETAINED_BODY_VERSION&&encoded.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("unknown Target reclaim type/body version");
        final var operations=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(9),15),version==NATIVE_RETAINED_BODY_VERSION?MAX_NATIVE_CANONICAL_BYTES:MAX_CANONICAL_BYTES,MAX_OPERATIONS,true,"reclaim operation group");
        final var decoded=new ArrayList<Operation>();
        for(var field:operations){
            if(version==NATIVE_RETAINED_BODY_VERSION){
                final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),MAX_NATIVE_CANONICAL_BYTES,MAX_OPERATIONS+4,true,"Native retained reclaim operation");
                if(entry.size()<4||QueryCodecSupport.uint32(entry.get(0),1)!=1||QueryCodecSupport.uint32(entry.get(1),2)!=3)
                    throw new IllegalArgumentException("unknown Native reclaim operation schema/kind");
                final int action=QueryCodecSupport.uint32(entry.get(2),3);final byte[] payload=QueryCodecSupport.bytes(entry.get(3),4);
                if(action==1||action==2){
                    QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3,4},"Native leaf operation");
                    final var value=new RetainedRecordDelete(TargetResultRecord.decode(payload));
                    if(value.action()!=action)throw new IllegalArgumentException("Native leaf action/value mismatch");decoded.add(value);
                }else if(action==3){
                    final var confirmations=new ArrayList<TargetEvidenceConfirmedRecord>();
                    for(int i=4;i<entry.size();i++)confirmations.add(TargetEvidenceConfirmedRecord.decode(QueryCodecSupport.bytes(entry.get(i),5)));
                    decoded.add(new RetainedAllocationRelease(TargetQuotaAttemptBudget.decode(payload),confirmations));
                }else throw new IllegalArgumentException("unknown Native retained reclaim action");
                continue;
            }
            final boolean inline=version!=BODY_VERSION;
            final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),
                    inline?TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES+TargetMessageLocator.MAX_CANONICAL_BYTES+128:TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES+32,inline?7:3,false,"reclaim operation");
            QueryCodecSupport.requireNumbers(entry,inline?new int[]{1,2,3,4,5,6,7}:new int[]{1,2,3},"reclaim operation");
            if(QueryCodecSupport.uint32(entry.get(0),1)!=1||QueryCodecSupport.uint32(entry.get(1),2)!=(inline?2:1))
                throw new IllegalArgumentException("unknown reclaim operation schema/kind");
            decoded.add(inline?new InlinePayloadRelease(TargetQuotaPayloadOwner.decode(QueryCodecSupport.bytes(entry.get(2),3)),
                    TargetMessageLocator.decode(QueryCodecSupport.bytes(entry.get(4),5)),QueryCodecSupport.uint64Bits(entry.get(5),6),
                    QueryCodecSupport.fixed(entry.get(3),4,32),QueryCodecSupport.fixed(entry.get(6),7,32))
                    :new UnusedAttemptReserve(TargetQuotaAttemptBudget.decode(QueryCodecSupport.bytes(entry.get(2),3))));
        }
        final var value=new TargetResourceReclaimBody(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(4),10)),
                QueryCodecSupport.fixed(f.get(5),11,16),QueryCodecSupport.fixed(f.get(6),12,32),QueryCodecSupport.fixed(f.get(7),13,32),
                QueryCodecSupport.uint(f.get(2),3),RecoveryFloorRef.decode(QueryCodecSupport.bytes(f.get(8),14)),decoded,
                version==SELF_CONTAINED_INLINE_BODY_VERSION?TargetInlinePayloadSourceFacts.decode(QueryCodecSupport.bytes(f.get(10),16)):null,
                version==NATIVE_RETAINED_BODY_VERSION?TargetNativeReclaimFacts.decode(QueryCodecSupport.bytes(f.get(10),16)):null);
        if(!value.shard().equals(ShardSubject.decode(QueryCodecSupport.bytes(f.get(0),1)).shardId()))throw new IllegalArgumentException("reclaim subject differs from scope");
        if(value.nativeFacts()!=null)value.nativeFacts().requireFor(value);
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target reclaim body");return value;
    }
}
