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
    public static final int OBJECT_PAYLOAD_BODY_VERSION=13;
    public static final int UNCOMMITTED_OBJECT_BODY_VERSION=14;
    public static final int ANCHOR_ALLOCATION_BODY_VERSION=15;
    public static final int INCARNATION_BODY_VERSION=16;
    public static final int DUPLICATE_POSITION_BODY_VERSION=17;
    public static final int MAX_OBJECT_INTENT_BYTES=24*1024*1024;
    public static final int MAX_NATIVE_CANONICAL_BYTES=32*1024*1024;
    public static final int MAX_OPERATIONS=32;
    public static final int MAX_CANONICAL_BYTES=8*1024*1024;
    private static final byte[] ID_DOMAIN=Bytes.utf8("nereus-delay-target-resource-reclaim-logical-identity\0");
    public TargetResourceReclaimBody(TargetQuotaScope scope,byte[] lineage,byte[] root,byte[] namespace,long retryUntil,
            RecoveryFloorRef floor,List<Operation> operations){this(scope,lineage,root,namespace,retryUntil,floor,operations,null,null);}
    public TargetResourceReclaimBody(TargetQuotaScope scope,byte[] lineage,byte[] root,byte[] namespace,long retryUntil,
            RecoveryFloorRef floor,List<Operation> operations,TargetInlinePayloadSourceFacts facts){this(scope,lineage,root,namespace,retryUntil,floor,operations,facts,null);}

    public sealed interface Operation permits UnusedAttemptReserve, InlinePayloadRelease,RetainedRecordDelete,RetainedAllocationRelease,IncarnationDrain,IncarnationRetire,ObjectPayloadRetire,ObjectPayloadConfirm,UncommittedObjectRetire,UncommittedObjectConfirm {
        byte[] identity();
        byte[] canonicalBytes();
    }
    /** An accepted explicit per-incarnation Close, never a temporary zero grant, authorizes the drain cause. */
    public record IncarnationDrain(TargetQuotaIncarnation expected,TargetCloseRecord close) implements Operation {
        public IncarnationDrain {
            Objects.requireNonNull(expected);Objects.requireNonNull(close);
            if(expected.identity().kind()!=TargetQuotaIdentity.Kind.TARGET||expected.draining()
                    ||!expected.identity().equals(close.ownerIdentity())||!Arrays.equals(expected.recoveryLineage(),close.recoveryLineage()))
                throw new IllegalArgumentException("drain requires the original OPEN Target incarnation and its explicit Close");
            expected.allocation().requireAtOrBefore(close.mutation());
        }
        public byte[] identity(){return Bytes.concat(new byte[]{3,4},expected.identity().canonicalBytes());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,3);
            CanonicalProtobuf.uint32(out,3,4);CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());CanonicalProtobuf.bytes(out,5,close.canonicalBytes());});}
    }
    /** No business records are implicitly removed; actual full ledgers and independent admission prove eligibility. */
    public record IncarnationRetire(TargetQuotaIncarnation expected,TargetQuotaGrantActivation zeroGrant) implements Operation {
        public IncarnationRetire {
            Objects.requireNonNull(expected);Objects.requireNonNull(zeroGrant);
            final var origin=zeroGrant.allocation();
            if(expected.identity().kind()!=TargetQuotaIdentity.Kind.TARGET||!expected.draining()||origin==null
                    ||!zeroGrant.grant().limit().isZero()||!expected.identity().equals(origin.identity())
                    ||!expected.allocation().equals(origin.allocation())||!expected.accounting().equals(origin.accounting())
                    ||!Arrays.equals(expected.tenantScope(),origin.tenantScope())||!Arrays.equals(expected.recoveryLineage(),origin.recoveryLineage()))
                throw new IllegalArgumentException("retirement requires its actual DRAINING Target and original zero grant reference");
        }
        public byte[] identity(){return Bytes.concat(new byte[]{3,5},expected.identity().canonicalBytes());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,3);
            CanonicalProtobuf.uint32(out,3,5);CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());CanonicalProtobuf.bytes(out,5,zeroGrant.canonicalBytes());});}
    }
    /** The same exact terminal primary projection is required for both inline and provider-backed payloads. */
    public interface RetainedPayload {
        TargetQuotaPayloadOwner expected(); TargetMessageLocator locator(); long stateVersion();
        byte[] primaryDigest(); byte[] terminalDigest();
    }
    public record ObjectPayloadRetire(TargetQuotaPayloadOwner expected,TargetMessageLocator locator,long stateVersion,
            byte[] primaryDigest,byte[] terminalDigest,CanonicalPayloadCommitProof proof,TargetObjectPayloadOrigin nativeOrigin) implements Operation,RetainedPayload {
        public ObjectPayloadRetire(TargetQuotaPayloadOwner expected,TargetMessageLocator locator,long stateVersion,byte[] primary,byte[] terminal,CanonicalPayloadCommitProof proof){this(expected,locator,stateVersion,primary,terminal,proof,null);}
        public ObjectPayloadRetire(TargetQuotaPayloadOwner expected,TargetMessageLocator locator,long stateVersion,byte[] primary,byte[] terminal,TargetObjectPayloadOrigin origin){this(expected,locator,stateVersion,primary,terminal,null,origin);}
        public ObjectPayloadRetire {
            Objects.requireNonNull(expected);Objects.requireNonNull(locator);
            if((proof==null)==(nativeOrigin==null))throw new IllegalArgumentException("one explicitly versioned OBJECT acceptance origin required");
            primaryDigest=TargetCompatibilityCodec.assigned(primaryDigest,32,"object primary digest");
            terminalDigest=TargetCompatibilityCodec.assigned(terminalDigest,32,"object terminal digest");
            if(expected.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||expected.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED
                    ||expected.committedPayload()==null||stateVersion==0||!expected.messageId().equals(locator.messageId())
                    ||!expected.primaryIdentity().target().equals(locator.target())
                    ||!Arrays.equals(expected.primaryIdentity().accountingIncarnation(),locator.accountingIncarnation())
                    ||!Arrays.equals(expected.initialBindingDigest(),locator.scheduleBindingDigest()))
                throw new IllegalArgumentException("exact retained committed OBJECT owner/primary required");
            if(proof!=null){
            final var reference=TargetPayloadReference.requireBounded(new PayloadReference(proof.objectStoreProfileHash(),proof.container(),
                    proof.objectKey(),proof.immutableObjectVersion(),proof.etag(),proof.length(),proof.payloadSha256(),proof.reservationId(),proof.proofId()));
            if(!reference.equals(expected.committedPayload())||!proof.delayMessageId().equals(expected.messageId())
                    ||!Arrays.equals(proof.tenantRoutingScope(),expected.tenantScope()))throw new IllegalArgumentException("object intent changed original commit proof");
            }else nativeOrigin.requireOwner(expected);
        }
        public int operationSchema(){return nativeOrigin==null?1:2;}
        @Override public byte[] primaryDigest(){return Bytes.copy(primaryDigest);}
        @Override public byte[] terminalDigest(){return Bytes.copy(terminalDigest);}
        public byte[] identity(){return Bytes.concat(new byte[]{4,1},expected.messageId().bytes());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,operationSchema());CanonicalProtobuf.uint32(out,2,4);CanonicalProtobuf.uint32(out,3,1);
            CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());CanonicalProtobuf.bytes(out,5,locator.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out,6,stateVersion);CanonicalProtobuf.bytes(out,7,primaryDigest);
            CanonicalProtobuf.bytes(out,8,terminalDigest);
            if(proof!=null)CanonicalProtobuf.bytes(out,9,proof.canonicalBytes());else CanonicalProtobuf.bytes(out,10,nativeOrigin.canonicalBytes());
        });}
    }
    /** Draft facts may be absent only before original image authentication; no such body can be signed/decoded. */
    public record UncommittedObjectRetire(TargetQuotaPayloadOwner expected,TargetUncommittedPayloadFacts facts) implements Operation {
        public UncommittedObjectRetire {
            Objects.requireNonNull(expected);
            if(expected.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||expected.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED||expected.committedPayload()!=null)
                throw new IllegalArgumentException("exact retained uncommitted reservation owner required");
            if(facts!=null)facts.requireOwner(expected);
        }
        public byte[] identity(){return Bytes.concat(new byte[]{4,3},expected.messageId().bytes());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,4);CanonicalProtobuf.uint32(out,3,3);
            CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());if(facts!=null)CanonicalProtobuf.bytes(out,11,facts.canonicalBytes());});}
    }
    public record UncommittedObjectConfirm(TargetPayloadObjectRetiringRecord intent,TargetUncommittedObjectClosure closure) implements Operation {
        public UncommittedObjectConfirm {
            Objects.requireNonNull(intent);Objects.requireNonNull(closure);closure.requireFor(intent);
            if(!closure.complete())throw new IllegalArgumentException("Source confirmation requires every closed version's actual deletion terminal");
        }
        public byte[] identity(){return Bytes.concat(new byte[]{4,4},intent.expectedOwner().messageId().bytes(),intent.intent().systemMutationId());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,4);CanonicalProtobuf.uint32(out,3,4);
            CanonicalProtobuf.bytes(out,4,intent.canonicalBytes());CanonicalProtobuf.bytes(out,12,closure.canonicalBytes());});}
    }
    /** Serialized response hashes only correlate the actual hot or independently authenticated historical witness. */
    public record ObjectPayloadConfirm(TargetPayloadObjectRetiringRecord intent,byte[] providerRequestIdHash,byte[] providerResponseHash) implements Operation {
        public ObjectPayloadConfirm {
            Objects.requireNonNull(intent);
            if(intent.body().uncommittedObjectReclaim())throw new IllegalArgumentException("committed confirmation cannot reinterpret an uncommitted intent");
            providerRequestIdHash=TargetCompatibilityCodec.assigned(providerRequestIdHash,32,"object delete request ID hash");
            providerResponseHash=TargetCompatibilityCodec.assigned(providerResponseHash,32,"object delete response hash");
        }
        @Override public byte[] providerRequestIdHash(){return Bytes.copy(providerRequestIdHash);}
        @Override public byte[] providerResponseHash(){return Bytes.copy(providerResponseHash);}
        public byte[] identity(){return Bytes.concat(new byte[]{4,2},intent.operation().expected().messageId().bytes(),intent.intent().systemMutationId());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,4);CanonicalProtobuf.uint32(out,3,2);
            CanonicalProtobuf.bytes(out,4,intent.canonicalBytes());CanonicalProtobuf.bytes(out,5,providerRequestIdHash);CanonicalProtobuf.bytes(out,6,providerResponseHash);
        });}
    }
    /** Closed leaf kinds derive their one DEDUPE key from the full semantic value; no arbitrary delete address. */
    public record RetainedRecordDelete(TargetResultRecord expected) implements Operation {
        public RetainedRecordDelete {
            Objects.requireNonNull(expected);
            if(expected.kind()!=TargetResultRecord.Kind.RESULT&&expected.kind()!=TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED&&expected.kind()!=TargetResultRecord.Kind.POSITION_COMMAND)
                throw new IllegalArgumentException("only query-result, standalone expired POSITION and strictly later COMMAND POSITION leaves are supported");
        }
        public int action(){return expected.kind()==TargetResultRecord.Kind.RESULT?1:expected.kind()==TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED?2:6;}
        public byte[] identity(){return Bytes.concat(new byte[]{3,(byte)action()},expected.key());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,3);
            CanonicalProtobuf.uint32(out,3,action());CanonicalProtobuf.bytes(out,4,expected.canonicalBytes());});}
    }
    /** Confirmed owned-object shares have one immutable ownership/settlement identity per original Attempt. */
    public record RetainedAllocationRelease(TargetQuotaAttemptBudget expected,List<TargetEvidenceConfirmedRecord> confirmations) implements Operation {
        public RetainedAllocationRelease {
            Objects.requireNonNull(expected);
            if(expected.phase()!=TargetQuotaAttemptBudget.Phase.RETAINED||confirmations.size()>MAX_OPERATIONS)
                throw new IllegalArgumentException("bounded retained Attempt settlement required");
            final var sorted=new java.util.TreeMap<String,TargetEvidenceConfirmedRecord>();
            final var shares=new java.util.HashSet<String>();
            for(var confirmation:confirmations){
                final var provenance=confirmation.body().resource().provenance();
                if(provenance instanceof TargetExternalEvidenceObjectResource.Recovery recovery){
                    if(!Arrays.equals(recovery.owner().reference().attemptId(),expected.publishAttemptId())
                        ||!recovery.owner().primaryOwner().identity().equals(expected.primaryIdentity())
                        ||!recovery.owner().primaryOwner().accounting().equals(expected.accounting())
                        ||!Arrays.equals(recovery.owner().admissionHash(),expected.admissionDigest())
                        ||!recovery.owner().adoptedAt().equals(expected.resolvedAt()))
                        throw new IllegalArgumentException("recovery settlement changed original Attempt/frozen allocation origin");
                }else if(provenance instanceof TargetExternalEvidenceObjectResource.Anchor anchor){
                    final var owner=anchor.owner();
                    if(!Arrays.equals(owner.creatorAttemptId(),expected.publishAttemptId())
                            ||!owner.primaryOwner().identity().equals(expected.primaryIdentity())
                            ||!owner.primaryOwner().accounting().equals(expected.accounting())
                            ||!Arrays.equals(owner.primaryOwner().tenantScope(),expected.tenantScope())
                            ||!Arrays.equals(owner.primaryOwner().recoveryLineage(),expected.recoveryLineage())
                            ||!Arrays.equals(anchor.admissionHash(),expected.admissionDigest()))
                        throw new IllegalArgumentException("anchor settlement changed its unique creator/frozen allocation origin");
                    expected.resolvedAt().requireAfter(owner.createdAt());
                }else throw new IllegalArgumentException("allocation settlement requires an actually owned Anchor or Recovery share");
                if(sorted.put(Bytes.hex(confirmation.key()),confirmation)!=null)
                    throw new IllegalArgumentException("duplicate allocation settlement fact");
                if(!shares.add(Bytes.hex(shareIdentity(confirmation))))throw new IllegalArgumentException("duplicate original allocation share");
            }
            confirmations=List.copyOf(sorted.values());
            if(!expected.allocated().isZero()&&confirmations.isEmpty())throw new IllegalArgumentException("nonzero allocation requires actual one-time settlement facts");
            CapacityVector remaining=expected.allocated();for(var confirmation:confirmations)
                remaining=remaining.subtract(confirmedShare(confirmation));
        }
        public CapacityVector remaining(){CapacityVector value=expected.allocated();for(var confirmation:confirmations)
            value=value.subtract(confirmedShare(confirmation));return value;}
        public static CapacityVector confirmedShare(TargetEvidenceConfirmedRecord confirmation){
            final var provenance=confirmation.body().resource().provenance();
            if(provenance instanceof TargetExternalEvidenceObjectResource.Recovery recovery)return recovery.owner().adoptedCharge();
            if(provenance instanceof TargetExternalEvidenceObjectResource.Anchor anchor)return anchor.owner().blobCharge();
            throw new IllegalArgumentException("no uniquely owned allocation share for this confirmation");
        }
        public boolean includesAnchor(){return confirmations.stream().anyMatch(c->c.body().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Anchor);}
        public static byte[] shareIdentity(TargetEvidenceConfirmedRecord confirmation){
            final var provenance=confirmation.body().resource().provenance();
            if(provenance instanceof TargetExternalEvidenceObjectResource.Recovery recovery)return recovery.owner().key();
            if(provenance instanceof TargetExternalEvidenceObjectResource.Anchor anchor)return anchor.owner().key();
            throw new IllegalArgumentException("confirmation has no unique original allocation share");
        }
        public int operationSchema(){return includesAnchor()?2:1;}
        public byte[] identity(){return Bytes.concat(new byte[]{3,3},expected.publishAttemptId());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,operationSchema());CanonicalProtobuf.uint32(out,2,3);
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
            byte[] primaryDigest,byte[] terminalDigest) implements Operation,RetainedPayload {
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
        final boolean nativeRetained=nativeOperation(ordered.getFirst());
        final boolean incarnation=ordered.stream().anyMatch(o->o instanceof IncarnationDrain||o instanceof IncarnationRetire);
        if(ordered.stream().anyMatch(o->o instanceof RetainedRecordDelete d&&d.expected().kind()==TargetResultRecord.Kind.POSITION_COMMAND)
                &&ordered.stream().anyMatch(o->!(o instanceof RetainedRecordDelete d)||d.expected().kind()!=TargetResultRecord.Kind.POSITION_COMMAND))
            throw new IllegalArgumentException("duplicate POSITION lifecycle cannot mix other operation generations");
        if(incarnation&&ordered.size()!=1)throw new IllegalArgumentException("one incarnation lifecycle transition per Source decision");
        final boolean uncommitted=ordered.getFirst() instanceof UncommittedObjectRetire||ordered.getFirst() instanceof UncommittedObjectConfirm;
        final boolean object=uncommitted||ordered.getFirst() instanceof ObjectPayloadRetire||ordered.getFirst() instanceof ObjectPayloadConfirm;
        if(object&&ordered.size()!=1)throw new IllegalArgumentException("one exact OBJECT lifecycle stage per Source decision");
        if(sourceFacts!=null&&nativeFacts!=null)throw new IllegalArgumentException("reclaim source facts cannot mix kinds");
        byte[] previous=null;long bytes=floor.canonicalBytes().length+scope.canonicalBytes().length+256L;
        for(var operation:ordered){
            Objects.requireNonNull(operation);
            if(inline!=(operation instanceof InlinePayloadRelease)||nativeRetained!=nativeOperation(operation)
                    ||object!=(operation instanceof ObjectPayloadRetire||operation instanceof ObjectPayloadConfirm||operation instanceof UncommittedObjectRetire||operation instanceof UncommittedObjectConfirm)
                    ||uncommitted!=(operation instanceof UncommittedObjectRetire||operation instanceof UncommittedObjectConfirm))
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
            }case IncarnationDrain drain->{
                requireIncarnation(scope,lineage,floor,drain.expected());drain.close().mutation().requireCoveredByFloor(floor);
            }case IncarnationRetire retire->{
                requireIncarnation(scope,lineage,floor,retire.expected());retire.zeroGrant().mutation().requireCoveredByFloor(floor);
            }case ObjectPayloadRetire payload->{
                scope.requireRoute(payload.expected().messageId().routingId().shardId(),payload.expected().tenantScope());
                if(!Arrays.equals(lineage,payload.expected().recoveryLineage()))throw new IllegalArgumentException("object intent changed frozen lineage");
                payload.expected().mutation().requireCoveredByFloor(floor);
            }case UncommittedObjectRetire payload->{
                scope.requireRoute(payload.expected().primaryIdentity().shard(),payload.expected().tenantScope());
                if(!Arrays.equals(lineage,payload.expected().recoveryLineage()))throw new IllegalArgumentException("uncommitted intent changed frozen lineage");
                payload.expected().mutation().requireCoveredByFloor(floor);if(payload.facts()!=null)payload.facts().requireFloor(floor);
            }case UncommittedObjectConfirm confirmation->{
                final var original=confirmation.intent().body();
                if(!scope.equals(original.scope())||!Arrays.equals(lineage,original.lineage())||!Arrays.equals(deploymentRoot,original.deploymentRoot())
                        ||!Arrays.equals(namespace,original.namespace()))throw new IllegalArgumentException("uncommitted confirmation changed original scope");
                confirmation.intent().appliedAt().requireCoveredByFloor(floor);
            }case ObjectPayloadConfirm confirmation->{
                final var original=confirmation.intent().body();
                if(!scope.equals(original.scope())||!Arrays.equals(lineage,original.lineage())||!Arrays.equals(deploymentRoot,original.deploymentRoot())
                        ||!Arrays.equals(namespace,original.namespace()))throw new IllegalArgumentException("object confirmation changed original intent scope");
                confirmation.intent().appliedAt().requireCoveredByFloor(floor);
            }}
            bytes=Math.addExact(bytes,operation.canonicalBytes().length+8L);
            if(bytes>(object||nativeRetained?MAX_NATIVE_CANONICAL_BYTES:MAX_CANONICAL_BYTES))throw new IllegalArgumentException("reclaim group exceeds encoded byte bound");
        }
        operations=List.copyOf(ordered);
        if(uncommitted&&(sourceFacts!=null||nativeFacts!=null))throw new IllegalArgumentException("uncommitted causality cannot reinterpret existing SourceFacts");
        if(sourceFacts!=null){sourceFacts.requireFor(operations,floor);
            if(Math.addExact(bytes,sourceFacts.canonicalBytes().length+8L)>(object?MAX_OBJECT_INTENT_BYTES:MAX_CANONICAL_BYTES))throw new IllegalArgumentException("self-contained reclaim exceeds total byte bound");}
        if(nativeFacts!=null&&(!nativeRetained||Math.addExact(bytes,nativeFacts.canonicalBytes().length+8L)>MAX_NATIVE_CANONICAL_BYTES))
            throw new IllegalArgumentException("Native reclaim facts exceed their explicit new body bound");
    }
    @Override public byte[] lineage(){return Bytes.copy(lineage);}
    @Override public byte[] deploymentRoot(){return Bytes.copy(deploymentRoot);}
    @Override public byte[] namespace(){return Bytes.copy(namespace);}
    public ShardId shard(){return scope.shard();}
    public boolean inlinePayloadRelease(){return operations.getFirst() instanceof InlinePayloadRelease;}
    public boolean selfContainedInlinePayloadRelease(){return inlinePayloadRelease()&&sourceFacts!=null;}
    public boolean uncommittedObjectReclaim(){return operations.getFirst() instanceof UncommittedObjectRetire||operations.getFirst() instanceof UncommittedObjectConfirm;}
    public UncommittedObjectRetire uncommittedOperation(){return operations.getFirst() instanceof UncommittedObjectRetire intent?intent:((UncommittedObjectConfirm)operations.getFirst()).intent().uncommittedOperation();}
    public TargetQuotaPayloadOwner objectOwner(){return uncommittedObjectReclaim()?uncommittedOperation().expected():objectOperation().expected();}
    public boolean objectPayloadReclaim(){return uncommittedObjectReclaim()||operations.getFirst() instanceof ObjectPayloadRetire||operations.getFirst() instanceof ObjectPayloadConfirm;}
    public TargetResourceReclaimBody withUncommittedFacts(TargetUncommittedPayloadFacts facts){
        if(!(operations.getFirst() instanceof UncommittedObjectRetire selected)||selected.facts()!=null)throw new IllegalArgumentException("uncommitted facts already frozen or wrong stage");
        return new TargetResourceReclaimBody(scope,lineage,deploymentRoot,namespace,retryUntil,floor,List.of(new UncommittedObjectRetire(selected.expected(),Objects.requireNonNull(facts))));
    }
    public ObjectPayloadRetire objectOperation(){return operations.getFirst() instanceof ObjectPayloadRetire operation?operation:((ObjectPayloadConfirm)operations.getFirst()).intent().operation();}
    public List<RetainedPayload> payloadOperations(){return objectPayloadReclaim()?List.of(objectOperation()):operations.stream().map(o->(RetainedPayload)o).toList();}
    public TargetInlinePayloadSourceFacts payloadFacts(){return operations.getFirst() instanceof ObjectPayloadConfirm confirmation?confirmation.intent().body().sourceFacts():sourceFacts;}
    public boolean selfContainedPayloadRelease(){return (inlinePayloadRelease()||objectPayloadReclaim())&&payloadFacts()!=null;}
    private static boolean nativeOperation(Operation operation){return operation instanceof RetainedRecordDelete||operation instanceof RetainedAllocationRelease||operation instanceof IncarnationDrain||operation instanceof IncarnationRetire;}
    private static void requireIncarnation(TargetQuotaScope scope,byte[] lineage,RecoveryFloorRef floor,TargetQuotaIncarnation expected){
        scope.requireRoute(expected.identity().shard(),expected.tenantScope());
        if(!Arrays.equals(lineage,expected.recoveryLineage()))throw new IllegalArgumentException("incarnation lifecycle changed original lineage");
        expected.latestMutation().requireCoveredByFloor(floor);
    }
    public boolean duplicatePositionReclaim(){return operations.stream().anyMatch(o->o instanceof RetainedRecordDelete d&&d.expected().kind()==TargetResultRecord.Kind.POSITION_COMMAND);}
    public boolean incarnationReclaim(){return operations.getFirst() instanceof IncarnationDrain||operations.getFirst() instanceof IncarnationRetire;}
    public boolean nativeRetainedReclaim(){return nativeOperation(operations.getFirst());}
    public boolean anchorAllocationReclaim(){return operations.stream().anyMatch(o->o instanceof RetainedAllocationRelease a&&a.includesAnchor());}
    public int maximumCanonicalBytes(){return objectPayloadReclaim()?operations.getFirst() instanceof ObjectPayloadRetire||operations.getFirst() instanceof UncommittedObjectRetire?MAX_OBJECT_INTENT_BYTES:MAX_NATIVE_CANONICAL_BYTES:nativeRetainedReclaim()?MAX_NATIVE_CANONICAL_BYTES:MAX_CANONICAL_BYTES;}
    public int bodyVersion(){return duplicatePositionReclaim()?DUPLICATE_POSITION_BODY_VERSION:incarnationReclaim()?INCARNATION_BODY_VERSION:anchorAllocationReclaim()?ANCHOR_ALLOCATION_BODY_VERSION:uncommittedObjectReclaim()?UNCOMMITTED_OBJECT_BODY_VERSION:objectPayloadReclaim()?OBJECT_PAYLOAD_BODY_VERSION:nativeRetainedReclaim()?NATIVE_RETAINED_BODY_VERSION:selfContainedInlinePayloadRelease()?SELF_CONTAINED_INLINE_BODY_VERSION:inlinePayloadRelease()?INLINE_BODY_VERSION:BODY_VERSION;}
    public ProtocolTuple requiredTuple(){return duplicatePositionReclaim()?ProtocolTuple.targetDuplicatePositionReclaim():incarnationReclaim()?ProtocolTuple.targetIncarnationReclaim():anchorAllocationReclaim()?ProtocolTuple.targetAnchorAllocationReclaim():uncommittedObjectReclaim()?ProtocolTuple.targetUncommittedObjectReclaim():objectPayloadReclaim()?ProtocolTuple.targetObjectPayloadReclaim():nativeRetainedReclaim()?ProtocolTuple.targetNativeRetainedReclaim():selfContainedInlinePayloadRelease()?ProtocolTuple.targetSelfContainedInlinePayloadReclaim():inlinePayloadRelease()?ProtocolTuple.targetInlinePayloadReclaim():ProtocolTuple.targetResourceReclaim();}
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
    public byte[] logicalOperationIdentity(){return objectPayloadReclaim()||selfContainedInlinePayloadRelease()||nativeRetainedReclaim()
            ?Bytes.sha256(ID_DOMAIN,scope.canonicalBytes(),lineage,deploymentRoot,namespace,operationGroup(),Bytes.lp32(requiredTuple().canonicalBytes()))
            :Bytes.sha256(ID_DOMAIN,scope.canonicalBytes(),lineage,deploymentRoot,namespace,operationGroup());}
    public byte[] canonicalBytes(){
        if(operations.getFirst() instanceof UncommittedObjectRetire uncommitted&&uncommitted.facts()==null)throw new IllegalStateException("freeze original uncommitted causality before signing");
        if(operations.getFirst() instanceof ObjectPayloadRetire&&sourceFacts==null)throw new IllegalStateException("OBJECT intent facts must be frozen before signing");
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
                ||version!=BODY_VERSION&&version!=INLINE_BODY_VERSION&&version!=SELF_CONTAINED_INLINE_BODY_VERSION&&version!=NATIVE_RETAINED_BODY_VERSION&&version!=OBJECT_PAYLOAD_BODY_VERSION&&version!=UNCOMMITTED_OBJECT_BODY_VERSION&&version!=ANCHOR_ALLOCATION_BODY_VERSION&&version!=INCARNATION_BODY_VERSION&&version!=DUPLICATE_POSITION_BODY_VERSION
                ||version!=OBJECT_PAYLOAD_BODY_VERSION&&(version==SELF_CONTAINED_INLINE_BODY_VERSION||version==NATIVE_RETAINED_BODY_VERSION||version==ANCHOR_ALLOCATION_BODY_VERSION||version==INCARNATION_BODY_VERSION||version==DUPLICATE_POSITION_BODY_VERSION)!=(f.size()==11)
                ||version<NATIVE_RETAINED_BODY_VERSION&&encoded.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("unknown Target reclaim type/body version");
        final var operations=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(9),15),version>=NATIVE_RETAINED_BODY_VERSION?MAX_NATIVE_CANONICAL_BYTES:MAX_CANONICAL_BYTES,MAX_OPERATIONS,true,"reclaim operation group");
        final var decoded=new ArrayList<Operation>();
        for(var field:operations){
            if(version==UNCOMMITTED_OBJECT_BODY_VERSION){
                if(f.size()!=10)throw new IllegalArgumentException("uncommitted body cannot reinterpret old SourceFacts");
                final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),MAX_NATIVE_CANONICAL_BYTES,5,false,"uncommitted OBJECT stage");
                if(entry.size()!=5||QueryCodecSupport.uint32(entry.get(0),1)!=1||QueryCodecSupport.uint32(entry.get(1),2)!=4)throw new IllegalArgumentException("unknown uncommitted stage schema/kind");
                final int action=QueryCodecSupport.uint32(entry.get(2),3);
                if(action==3){QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3,4,11},"uncommitted intent");
                    decoded.add(new UncommittedObjectRetire(TargetQuotaPayloadOwner.decode(QueryCodecSupport.bytes(entry.get(3),4)),TargetUncommittedPayloadFacts.decode(QueryCodecSupport.bytes(entry.get(4),11))));
                }else if(action==4){QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3,4,12},"uncommitted confirmation");
                    decoded.add(new UncommittedObjectConfirm(TargetPayloadObjectRetiringRecord.decode(QueryCodecSupport.bytes(entry.get(3),4)),TargetUncommittedObjectClosure.decode(QueryCodecSupport.bytes(entry.get(4),12))));
                }else throw new IllegalArgumentException("unknown uncommitted action");
                continue;
            }
            if(version==OBJECT_PAYLOAD_BODY_VERSION){
                final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),MAX_NATIVE_CANONICAL_BYTES,9,false,"OBJECT payload stage");
                if(entry.size()<6||QueryCodecSupport.uint32(entry.get(0),1)<1||QueryCodecSupport.uint32(entry.get(0),1)>2||QueryCodecSupport.uint32(entry.get(1),2)!=4)
                    throw new IllegalArgumentException("unknown OBJECT payload operation kind/schema");
                final int action=QueryCodecSupport.uint32(entry.get(2),3);
                if(action==1){
                    final int schema=QueryCodecSupport.uint32(entry.get(0),1);
                    QueryCodecSupport.requireNumbers(entry,schema==1?new int[]{1,2,3,4,5,6,7,8,9}:new int[]{1,2,3,4,5,6,7,8,10},"OBJECT intent");
                    decoded.add(new ObjectPayloadRetire(TargetQuotaPayloadOwner.decode(QueryCodecSupport.bytes(entry.get(3),4)),
                            TargetMessageLocator.decode(QueryCodecSupport.bytes(entry.get(4),5)),QueryCodecSupport.uint64Bits(entry.get(5),6),
                            QueryCodecSupport.fixed(entry.get(6),7,32),QueryCodecSupport.fixed(entry.get(7),8,32),
                            schema==1?CanonicalPayloadCommitProof.decode(QueryCodecSupport.bytes(entry.get(8),9)):null,
                            schema==2?TargetObjectPayloadOrigin.decode(QueryCodecSupport.bytes(entry.get(8),10)):null));
                    if(f.size()!=11)throw new IllegalArgumentException("OBJECT intent omitted original Source facts");
                }else if(action==2){
                    if(QueryCodecSupport.uint32(entry.get(0),1)!=1)throw new IllegalArgumentException("unknown OBJECT confirmation operation schema");
                    QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3,4,5,6},"OBJECT confirmation");
                    decoded.add(new ObjectPayloadConfirm(TargetPayloadObjectRetiringRecord.decode(QueryCodecSupport.bytes(entry.get(3),4)),
                            QueryCodecSupport.fixed(entry.get(4),5,32),QueryCodecSupport.fixed(entry.get(5),6,32)));
                    if(f.size()!=10)throw new IllegalArgumentException("OBJECT confirmation must retain facts through its exact intent");
                }else throw new IllegalArgumentException("unknown OBJECT payload stage");
                continue;
            }
            if(version==INCARNATION_BODY_VERSION){
                final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),MAX_NATIVE_CANONICAL_BYTES,5,false,"incarnation lifecycle operation");
                QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3,4,5},"incarnation lifecycle operation");
                if(QueryCodecSupport.uint32(entry.get(0),1)!=1||QueryCodecSupport.uint32(entry.get(1),2)!=3)
                    throw new IllegalArgumentException("unknown incarnation lifecycle schema/kind");
                final int action=QueryCodecSupport.uint32(entry.get(2),3);final var owner=TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(entry.get(3),4));
                if(action==4)decoded.add(new IncarnationDrain(owner,TargetCloseRecord.decode(QueryCodecSupport.bytes(entry.get(4),5))));
                else if(action==5)decoded.add(new IncarnationRetire(owner,TargetQuotaGrantActivation.decode(QueryCodecSupport.bytes(entry.get(4),5))));
                else throw new IllegalArgumentException("unknown incarnation lifecycle action");
                continue;
            }
            if(version==NATIVE_RETAINED_BODY_VERSION||version==ANCHOR_ALLOCATION_BODY_VERSION||version==DUPLICATE_POSITION_BODY_VERSION){
                final var entry=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(field,1),MAX_NATIVE_CANONICAL_BYTES,MAX_OPERATIONS+4,true,"Native retained reclaim operation");
                if(entry.size()<4)throw new IllegalArgumentException("incomplete Native reclaim operation");
                final int schema=QueryCodecSupport.uint32(entry.getFirst(),1);
                if(schema<1||schema>(version==ANCHOR_ALLOCATION_BODY_VERSION?2:1)||QueryCodecSupport.uint32(entry.get(1),2)!=3)
                    throw new IllegalArgumentException("unknown Native reclaim operation schema/kind");
                final int action=QueryCodecSupport.uint32(entry.get(2),3);final byte[] payload=QueryCodecSupport.bytes(entry.get(3),4);
                if(version==DUPLICATE_POSITION_BODY_VERSION?action==6:action==1||action==2){
                    if(schema!=1)throw new IllegalArgumentException("unknown Native leaf schema");
                    QueryCodecSupport.requireNumbers(entry,new int[]{1,2,3,4},"Native leaf operation");
                    final var value=new RetainedRecordDelete(TargetResultRecord.decode(payload));
                    if(value.action()!=action)throw new IllegalArgumentException("Native leaf action/value mismatch");decoded.add(value);
                }else if(action==3&&version!=DUPLICATE_POSITION_BODY_VERSION){
                    final var confirmations=new ArrayList<TargetEvidenceConfirmedRecord>();
                    for(int i=4;i<entry.size();i++)confirmations.add(TargetEvidenceConfirmedRecord.decode(QueryCodecSupport.bytes(entry.get(i),5)));
                    final var allocation=new RetainedAllocationRelease(TargetQuotaAttemptBudget.decode(payload),confirmations);
                    if(schema!=allocation.operationSchema()||version==NATIVE_RETAINED_BODY_VERSION&&allocation.includesAnchor())
                        throw new IllegalArgumentException("allocation provenance requires its explicit schema/body generation");
                    decoded.add(allocation);
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
                version==SELF_CONTAINED_INLINE_BODY_VERSION||version==OBJECT_PAYLOAD_BODY_VERSION&&f.size()==11?TargetInlinePayloadSourceFacts.decode(QueryCodecSupport.bytes(f.get(10),16)):null,
                version==NATIVE_RETAINED_BODY_VERSION||version==ANCHOR_ALLOCATION_BODY_VERSION||version==INCARNATION_BODY_VERSION||version==DUPLICATE_POSITION_BODY_VERSION?TargetNativeReclaimFacts.decode(QueryCodecSupport.bytes(f.get(10),16)):null);
        if(!value.shard().equals(ShardSubject.decode(QueryCodecSupport.bytes(f.get(0),1)).shardId()))throw new IllegalArgumentException("reclaim subject differs from scope");
        if(value.nativeFacts()!=null)value.nativeFacts().requireFor(value);
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target reclaim body");return value;
    }
}
