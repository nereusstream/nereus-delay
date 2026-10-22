package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed Source requests, never database edits, caller deltas, or a protection-release authority. */
public record TargetResourceReclaimBody(TargetQuotaScope scope, byte[] lineage, byte[] deploymentRoot,
        byte[] namespace, long retryUntil, RecoveryFloorRef floor, List<Operation> operations) {
    public static final int BODY_VERSION=9;
    public static final int MAX_OPERATIONS=32;
    public static final int MAX_CANONICAL_BYTES=8*1024*1024;
    private static final byte[] ID_DOMAIN=Bytes.utf8("nereus-delay-target-resource-reclaim-logical-identity\0");

    public sealed interface Operation permits UnusedAttemptReserve {
        byte[] identity();
        byte[] canonicalBytes();
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
        byte[] previous=null;long bytes=floor.canonicalBytes().length+scope.canonicalBytes().length+256L;
        for(var operation:ordered){
            Objects.requireNonNull(operation);
            final byte[] identity=operation.identity();
            if(previous!=null&&Arrays.compareUnsigned(previous,identity)>=0)throw new IllegalArgumentException("duplicate reclaim resource");
            previous=identity;
            switch(operation){case UnusedAttemptReserve unused->{
                final var expected=unused.expected();scope.requireRoute(expected.locator().messageId().routingId().shardId(),expected.tenantScope());
                if(!Arrays.equals(lineage,expected.recoveryLineage()))throw new IllegalArgumentException("reclaim changed frozen attempt lineage");
                expected.mutation().requireCoveredByFloor(floor);
            }}
            bytes=Math.addExact(bytes,operation.canonicalBytes().length+8L);
            if(bytes>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("reclaim group exceeds encoded byte bound");
        }
        operations=List.copyOf(ordered);
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] deploymentRoot(){return Bytes.copy(deploymentRoot);}
    @Override public byte[] namespace(){return Bytes.copy(namespace);}
    public ShardId shard(){return scope.shard();}
    public ProtocolTuple requiredTuple(){return ProtocolTuple.targetResourceReclaim();}
    private byte[] operationGroup(){return CanonicalProtobuf.message(out->{
        for(var operation:operations)CanonicalProtobuf.bytes(out,1,operation.canonicalBytes());
    });}
    /** Retry/deadline/Floor changes cannot create a fresh logical resource identity. The outer hash stays exact. */
    public byte[] logicalOperationIdentity(){return Bytes.sha256(ID_DOMAIN,scope.canonicalBytes(),lineage,deploymentRoot,namespace,operationGroup());}
    public byte[] canonicalBytes(){
        final byte[] value=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.bytes(out,1,new ShardSubject(shard()).canonicalBytes());
            CanonicalProtobuf.uint32(out,2,SystemMutationType.TARGET_RESOURCE_RECLAIM.wireValue());
            CanonicalProtobuf.int64(out,3,retryUntil);CanonicalProtobuf.uint32(out,9,BODY_VERSION);
            CanonicalProtobuf.bytes(out,10,scope.canonicalBytes());CanonicalProtobuf.bytes(out,11,lineage);
            CanonicalProtobuf.bytes(out,12,deploymentRoot);CanonicalProtobuf.bytes(out,13,namespace);
            CanonicalProtobuf.bytes(out,14,floor.canonicalBytes());CanonicalProtobuf.bytes(out,15,operationGroup());
        });
        if(value.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("reclaim body exceeds byte bound");return value;
    }
    public static TargetResourceReclaimBody decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,10,false,"Target reclaim body");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,9,10,11,12,13,14,15},"Target reclaim body");
        if(QueryCodecSupport.uint32(f.get(1),2)!=SystemMutationType.TARGET_RESOURCE_RECLAIM.wireValue()
                ||QueryCodecSupport.uint32(f.get(3),9)!=BODY_VERSION)throw new IllegalArgumentException("unknown Target reclaim type/body version");
        final var operations=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(9),15),MAX_CANONICAL_BYTES,MAX_OPERATIONS,true,"reclaim operation group");
        final var decoded=new ArrayList<Operation>();
        for(var field:operations){
            final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES+32,3,false,"reclaim operation");
            QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3},"reclaim operation");
            if(QueryCodecSupport.uint32(entry.get(0),1)!=1||QueryCodecSupport.uint32(entry.get(1),2)!=1)
                throw new IllegalArgumentException("unknown reclaim operation schema/kind");
            decoded.add(new UnusedAttemptReserve(TargetQuotaAttemptBudget.decode(QueryCodecSupport.bytes(entry.get(2),3))));
        }
        final var value=new TargetResourceReclaimBody(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(4),10)),
                QueryCodecSupport.fixed(f.get(5),11,16),QueryCodecSupport.fixed(f.get(6),12,32),QueryCodecSupport.fixed(f.get(7),13,32),
                QueryCodecSupport.uint(f.get(2),3),RecoveryFloorRef.decode(QueryCodecSupport.bytes(f.get(8),14)),decoded);
        if(!value.shard().equals(ShardSubject.decode(QueryCodecSupport.bytes(f.get(0),1)).shardId()))throw new IllegalArgumentException("reclaim subject differs from scope");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target reclaim body");return value;
    }
}
