package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Local Native joins only; independent original Root authentication happens outside these views. */
public final class TargetRetirementSourceHistoryStore {
    private TargetRetirementSourceHistoryStore(){}
    /** Exact Native point lookup: no full history scan, current file head or currently admitted Worker is substituted. */
    public static TargetRetirementHistoryTipRecord currentTip(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            TargetRetirementAuthorizationHistory.PublicProof candidate){
        final byte[] key=TargetRetirementHistoryTipRecord.key(candidate),raw=reader.get(ColumnFamily.META,key);
        if(raw==null)return null;
        final var tip=TargetRetirementHistoryTipRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetRetirementHistoryTipRecord.VALUE_TYPE).payload(),scope,lineage);
        final var owner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        requireTip(reader,List.of(),scope,lineage,owner,tip,reader.aggregate().mutation());candidate.requireContainsTip(tip.proof());return tip;
    }
    public static void requireTip(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,byte[] lineage,
            TargetQuotaIdentity owner,TargetRetirementHistoryTipRecord tip,TargetQuotaMutation frontier){
        tip.decidedAt().requireAtOrBefore(frontier);
        final byte[] decisionKey=TargetRetirementSourceHistoryRecord.key(tip.mutationId());
        final var decision=TargetRetirementSourceHistoryRecord.decodeForStore(decisionKey,payload(view,overlay,ColumnFamily.META,decisionKey,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        tip.requireDecision(decision);requireFirst(view,overlay,scope,lineage,owner,decision,frontier);
        final var activated=activation(view,overlay,scope,lineage,owner,frontier,decision.requiredTuple());
        if(!activated.body().payload().descriptor().supportsAcceptedHistoryTip())throw new IllegalStateException("Native55 lacks actual descriptor3 activation");
        tip.decidedAt().requireAfter(activated.appliedAt());
        final byte[] original=view.projected(ColumnFamily.META,tip.key(),List.of());
        if(original==null){
            if(!tip.decidedAt().equals(frontier))throw new IllegalStateException("first accepted tip differs from actual new Source decision");
        }else{
            final var old=TargetRetirementHistoryTipRecord.decodeForStore(tip.key(),TargetValueEnvelope.decode(original,TargetRetirementHistoryTipRecord.VALUE_TYPE).payload(),scope,lineage);
            if(!Arrays.equals(tip.canonicalBytes(),old.canonicalBytes())){
                if(!tip.decidedAt().equals(frontier))throw new IllegalStateException("updated accepted tip differs from actual new Source decision");
                tip.requireAfter(old);
            }
        }
    }
    public static void requireFirstHistory(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,
            byte[] lineage,TargetQuotaIdentity owner,TargetResultRecord first,TargetQuotaMutation frontier){
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM)return;
        final var result=SystemMutationResult.decode(first.typedPayload());
        final boolean reclaim=result.mutationType()==SystemMutationType.TARGET_RESOURCE_RECLAIM;
        if(!reclaim&&result.mutationType()!=SystemMutationType.RESOURCE_RETIRE_INTENT&&result.mutationType()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED)return;
        if(result.applyStatus()!=ApplyStatus.APPLIED&&result.stableCode()!=StableCode.UNAUTHORIZED_SYSTEM_MUTATION)return;
        final var tuple=reclaim?TargetRetirementSourceHistoryRecord.decodeForStore(
                TargetRetirementSourceHistoryRecord.key(result.mutationId()),payload(view,overlay,ColumnFamily.META,
                TargetRetirementSourceHistoryRecord.key(result.mutationId()),TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage).requiredTuple()
                :ProtocolTuple.targetEvidenceRetirement();
        final byte[] key=TargetProtocolActivationRecord.key(tuple),raw=view.projected(ColumnFamily.META,key,overlay);
        if(raw==null){if(reclaim)throw new IllegalStateException("reclaim first lacks independent activation");return;}
        final var activation=TargetProtocolActivationRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
        if(reclaim&&(!activation.body().payload().descriptor().supportsResourceReclaim()||activation.appliedAt().source().compareTo(first.mutation().source())>=0))
            throw new IllegalStateException("reclaim first lacks actual prior descriptor6");
        if(tuple.equals(ProtocolTuple.targetInlinePayloadReclaim())&&!activation.body().payload().descriptor().supportsInlinePayloadReclaim())
            throw new IllegalStateException("inline reclaim first lacks actual prior descriptor7");
        if(tuple.equals(ProtocolTuple.targetSelfContainedInlinePayloadReclaim())&&!activation.body().payload().descriptor().supportsSelfContainedInlinePayloadReclaim())
            throw new IllegalStateException("self-contained inline reclaim first lacks actual prior descriptor8");
        if(tuple.equals(ProtocolTuple.targetUncommittedObjectReclaim())&&!activation.body().payload().descriptor().supportsUncommittedObjectPayloadReclaim())
            throw new IllegalStateException("uncommitted first lacks descriptor11 activation");
        if(tuple.equals(ProtocolTuple.targetObjectPayloadReclaim())&&!activation.body().payload().descriptor().supportsObjectPayloadReclaim())
            throw new IllegalStateException("OBJECT first lacks its distinct prior activation");
        if(tuple.equals(ProtocolTuple.targetNativeRetainedReclaim())&&!activation.body().payload().descriptor().supportsNativeRetainedReclaim())
            throw new IllegalStateException("Native retained reclaim first lacks actual prior descriptor9");
        if(activation.body().payload().descriptor().supportsSourceHistoryRecords()&&activation.appliedAt().source().compareTo(first.mutation().source())<0)
            requireRetained(view,overlay,scope,lineage,owner,result.mutationId(),first.mutation(),frontier,tuple);
    }
    public static void requireFirst(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,
            byte[] lineage,TargetQuotaIdentity owner,TargetRetirementSourceHistoryRecord record,TargetQuotaMutation frontier){
        record.decidedAt().requireAtOrBefore(frontier);
        if(!owner.shard().equals(scope.shard())||owner.kind()!=TargetQuotaIdentity.Kind.SHARD)throw new IllegalStateException("Source history changed first owner");
        final var input=record.input();final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},input.systemMutationId());
        final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());final var result=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.primaryIdentity().equals(owner)||!first.mutation().equals(record.decidedAt())
                ||result.mutationType()!=input.type()||!Arrays.equals(result.mutationId(),input.systemMutationId())
                ||!Arrays.equals(result.mutationHash(),input.mutationHash())||!Arrays.equals(result.authorIdentity(),input.authorIdentity())
                ||result.retryUntilEpochMs()!=input.retryUntilEpochMs()||!Arrays.equals(result.appliedSourcePosition(),record.decidedAt().source().canonicalBytes()))
            throw new IllegalStateException("retained Source history differs from actual first decision");
        final byte[] ownerKey=owner.key();ownerKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(ownerKey,payload(view,overlay,ColumnFamily.META,ownerKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!Arrays.equals(root.recoveryLineage(),lineage))throw new IllegalStateException("Source history changed actual lineage");
        root.latestMutation().requireAtOrBefore(frontier);first.requireOwner(root);
        final byte[] positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},record.decidedAt().source().canonicalBytes());
        final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(root);
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(record.decidedAt()))throw new IllegalStateException("Source history lost original POSITION");
        final byte[] activationKey=TargetProtocolActivationRecord.key(record.requiredTuple());
        final byte[] activationRaw=view.projected(ColumnFamily.META,activationKey,overlay);
        if(activationRaw!=null){
            final var activated=TargetProtocolActivationRecord.decodeForStore(activationKey,TargetValueEnvelope.decode(activationRaw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
            if(activated.body().payload().descriptor().supportsAcceptedHistoryTip()&&activated.appliedAt().source().compareTo(record.decidedAt().source())<0){
                final byte[] tipKey=TargetRetirementHistoryTipRecord.key(record.proof());
                final var tip=TargetRetirementHistoryTipRecord.decodeForStore(tipKey,payload(view,overlay,ColumnFamily.META,tipKey,TargetRetirementHistoryTipRecord.VALUE_TYPE),scope,lineage);
                tip.decidedAt().requireAtOrBefore(frontier);tip.requireCovers(record);
            }
        }
        if(result.applyStatus()==ApplyStatus.APPLIED){
            if(result.stableCode()!=StableCode.OK||record.decidedAt().source().brokerPersistenceTimeEpochMs()>input.retryUntilEpochMs())throw new IllegalStateException("APPLIED Source history changed window/code");
            final var activation=activation(view,overlay,scope,lineage,owner,frontier,record.requiredTuple());
            if(!activation.body().payload().descriptor().supportsSourceHistoryRecords())throw new IllegalStateException("Native54 lacks explicitly activated descriptor2");
            if(input.type()==SystemMutationType.TARGET_RESOURCE_RECLAIM&&!activation.body().payload().descriptor().supportsResourceReclaim())
                throw new IllegalStateException("reclaim history lacks actual descriptor6");
            if(record.requiredTuple().equals(ProtocolTuple.targetInlinePayloadReclaim())&&!activation.body().payload().descriptor().supportsInlinePayloadReclaim())
                throw new IllegalStateException("inline reclaim history lacks actual descriptor7");
            if(record.requiredTuple().equals(ProtocolTuple.targetSelfContainedInlinePayloadReclaim())){
                if(!activation.body().payload().descriptor().supportsSelfContainedInlinePayloadReclaim())
                    throw new IllegalStateException("self-contained inline reclaim history lacks actual descriptor8");
                TargetInlinePayloadReclaim.requireRecordedSourceFacts(view,overlay,TargetResourceReclaimBody.decode(input.canonicalBody()),frontier);
            }
            if(record.requiredTuple().equals(ProtocolTuple.targetUncommittedObjectReclaim())){
                if(!activation.body().payload().descriptor().supportsUncommittedObjectPayloadReclaim())throw new IllegalStateException("uncommitted history lacks descriptor11 activation");
                TargetUncommittedPayloadReclaim.requireAppliedDecision(view,overlay,input,record.decidedAt(),frontier);
            }
            if(record.requiredTuple().equals(ProtocolTuple.targetObjectPayloadReclaim())){
                if(!activation.body().payload().descriptor().supportsObjectPayloadReclaim())throw new IllegalStateException("OBJECT history lacks its exact activation");
                TargetObjectPayloadReclaim.requireAppliedDecision(view,overlay,input,record.decidedAt(),frontier);
            }
            if(record.requiredTuple().equals(ProtocolTuple.targetNativeRetainedReclaim())){
                if(!activation.body().payload().descriptor().supportsNativeRetainedReclaim())throw new IllegalStateException("Native retained history lacks actual descriptor9");
                TargetNativeRetainedReclaim.requireAppliedDecision(view,overlay,TargetResourceReclaimBody.decode(input.canonicalBody()),record.decidedAt(),frontier);
            }
            final var grant=record.declaredGrant().orElseThrow(()->new IllegalStateException("APPLIED Source history has no declared SERVICE grant"));
            if(!input.verifySignature(grant.writerKey()))throw new IllegalStateException("APPLIED Source frame differs from retained grant key");
            grant.requireActivation(activation,record.decidedAt().source());
        }
    }
    /** Descriptor1 remains a readable historical format; new descriptor2 state must retain its atomic proof. */
    public static void requireRetained(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,byte[] lineage,
            TargetQuotaIdentity owner,byte[] id,TargetQuotaMutation decided,TargetQuotaMutation frontier){
        requireRetained(view,overlay,scope,lineage,owner,id,decided,frontier,ProtocolTuple.targetEvidenceRetirement());
    }
    private static void requireRetained(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,byte[] lineage,
            TargetQuotaIdentity owner,byte[] id,TargetQuotaMutation decided,TargetQuotaMutation frontier,ProtocolTuple tuple){
        final var activation=activation(view,overlay,scope,lineage,owner,frontier,tuple);
        if(!activation.body().payload().descriptor().supportsSourceHistoryRecords())return;
        final byte[] key=TargetRetirementSourceHistoryRecord.key(id);
        final var record=TargetRetirementSourceHistoryRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        if(!record.decidedAt().equals(decided)||!record.requiredTuple().equals(tuple))throw new IllegalStateException("retirement lost exact Source history stamp/tuple");
        requireFirst(view,overlay,scope,lineage,owner,record,frontier);
    }
    private static TargetProtocolActivationRecord activation(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,
            byte[] lineage,TargetQuotaIdentity owner,TargetQuotaMutation frontier,ProtocolTuple tuple){
        final byte[] key=TargetProtocolActivationRecord.key(tuple);
        final var value=TargetProtocolActivationRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        TargetProtocolActivationStore.requireApplied(view,overlay,scope,lineage,owner,value,frontier);return value;
    }
    /** Read-only recovery/accounting join. It neither acquires protection nor approves a new release. */
    public static void requireAttemptReclaim(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity owner,TargetQuotaAttemptBudget budget,TargetQuotaMutation frontier){
        if(budget.phase()!=TargetQuotaAttemptBudget.Phase.RETAINED&&budget.phase()!=TargetQuotaAttemptBudget.Phase.RELEASED)return;
        final byte[] nativeKey=TargetProtocolActivationRecord.key(ProtocolTuple.targetNativeRetainedReclaim()),nativeRaw=view.projected(ColumnFamily.META,nativeKey,overlay);
        if(nativeRaw!=null){
            final var nativeActivation=TargetProtocolActivationRecord.decodeForStore(nativeKey,TargetValueEnvelope.decode(nativeRaw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
            if(nativeActivation.appliedAt().source().compareTo(budget.mutation().source())<0){
                final var at=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},budget.mutation().source().canonicalBytes()),TargetResultRecord.VALUE_TYPE));
                final byte[] key=TargetRetirementSourceHistoryRecord.key(at.logicalId());
                final var record=TargetRetirementSourceHistoryRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
                if(record.requiredTuple().equals(ProtocolTuple.targetNativeRetainedReclaim())){
                    requireFirst(view,overlay,scope,lineage,owner,record,frontier);
                    final byte[] firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},record.input().systemMutationId());
                    if(!TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,firstKey,TargetResultRecord.VALUE_TYPE)).appliedFirst())
                        throw new IllegalStateException("retained allocation references a rejected Native reclaim");
                    final var body=TargetResourceReclaimBody.decode(record.input().canonicalBody());
                    TargetNativeRetainedReclaim.requireAllocationSuccessor(view,overlay,body,budget,record.decidedAt());return;
                }
            }
        }
        final byte[] activationKey=TargetProtocolActivationRecord.key(ProtocolTuple.targetResourceReclaim());
        final byte[] active=view.projected(ColumnFamily.META,activationKey,overlay);if(active==null)return;
        final var activation=TargetProtocolActivationRecord.decodeForStore(activationKey,TargetValueEnvelope.decode(active,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
        final int activationOrder=activation.appliedAt().source().compareTo(budget.mutation().source());
        if(activationOrder>0)return; // Earlier states retain their original historical contract.
        if(activationOrder==0)throw new IllegalStateException("reclaim budget cannot share its activation Source mutation");
        TargetProtocolActivationStore.requireApplied(view,overlay,scope,lineage,owner,activation,frontier);
        if(!activation.body().payload().descriptor().supportsResourceReclaim())throw new IllegalStateException("new reclaim state lacks descriptor6");
        final byte[] positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},budget.mutation().source().canonicalBytes());
        final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(budget.mutation()))throw new IllegalStateException("reclaimed budget lost its actual Source POSITION");
        final byte[] historyKey=TargetRetirementSourceHistoryRecord.key(position.logicalId());
        final var history=TargetRetirementSourceHistoryRecord.decodeForStore(historyKey,payload(view,overlay,ColumnFamily.META,historyKey,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        if(history.input().type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM||!history.decidedAt().equals(budget.mutation()))
            throw new IllegalStateException("reclaimed budget lost exact reclaim Source history");
        requireFirst(view,overlay,scope,lineage,owner,history,frontier);
        final byte[] firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},position.logicalId());
        final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,firstKey,TargetResultRecord.VALUE_TYPE));position.requireFirst(first);
        if(SystemMutationResult.decode(first.typedPayload()).applyStatus()!=ApplyStatus.APPLIED)throw new IllegalStateException("reclaimed budget references a rejected Source first");
        final var body=TargetResourceReclaimBody.decode(history.input().canonicalBody());
        final var operation=body.operations().stream().filter(o->o instanceof TargetResourceReclaimBody.UnusedAttemptReserve unused
                &&Arrays.equals(unused.expected().publishAttemptId(),budget.publishAttemptId())).findFirst().orElseThrow(
                ()->new IllegalStateException("reclaim Source omitted exact attempt operation"));
        final var expected=((TargetResourceReclaimBody.UnusedAttemptReserve)operation).expected();
        if(budget.phase()!=TargetQuotaAttemptBudget.Phase.RETAINED||budget.revision()!=TargetQuotaMutation.increment(expected.revision())
                ||!budget.locator().equals(expected.locator())||!Arrays.equals(budget.tenantScope(),expected.tenantScope())
                ||!Arrays.equals(budget.admissionDigest(),expected.admissionDigest())||!budget.accounting().equals(expected.accounting())
                ||budget.executionBytes()!=expected.executionBytes()||!budget.commitment().equals(expected.commitment())
                ||!budget.allocated().equals(expected.allocated())||!budget.resolvedAt().equals(expected.resolvedAt())
                ||!Arrays.equals(budget.recoveryLineage(),expected.recoveryLineage())||!Arrays.equals(budget.floorDigest(),body.floor().floorDigest()))
            throw new IllegalStateException("reclaimed attempt changed its exact expected phase/owner/allocation");
        budget.mutation().requireAfter(expected.mutation());expected.mutation().requireCoveredByFloor(body.floor());
        if(body.floor().appliedSourcePosition().compareTo(budget.mutation().source())>=0
                ||Long.compareUnsigned(body.floor().includedMutationSequence(),budget.mutation().sequence())>=0)
            throw new IllegalStateException("reclaim Source is not strictly after the checked Floor");
    }
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"retained Source history dependency"),type).payload();
    }

    /** Every INLINE RELEASED row retains its exact APPLIED inline Source, activation and absent primary. */
    public static void requirePayloadReclaim(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity owner,TargetQuotaPayloadOwner released,TargetQuotaMutation frontier){
        if(released.kind()==TargetQuotaPayloadOwner.Kind.OBJECT&&released.phase()==TargetQuotaPayloadOwner.Phase.RELEASED){
            final var history=TargetObjectPayloadReclaim.releaseHistory(view,overlay,scope,lineage,released,frontier);
            requireFirst(view,overlay,scope,lineage,owner,history,frontier);
        }else releasedInlineOperation(view,overlay,scope,lineage,owner,released,frontier);
    }
    public static TargetResourceReclaimBody.RetainedPayload releasedPayloadOperation(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity root,TargetQuotaPayloadOwner released,TargetQuotaMutation frontier){
        if(released.kind()==TargetQuotaPayloadOwner.Kind.INLINE)return releasedInlineOperation(view,overlay,scope,lineage,root,released,frontier);
        if(released.committedPayload()==null)throw new IllegalArgumentException("uncommitted reservation has no terminal Message locator; use original reservation metadata");
        final var history=TargetObjectPayloadReclaim.releaseHistory(view,overlay,scope,lineage,released,frontier);
        requireFirst(view,overlay,scope,lineage,root,history,frontier);
        return TargetResourceReclaimBody.decode(history.input().canonicalBody()).objectOperation();
    }
    /** Same complete read-only validation, returning the retained metadata locator without primary payload. */
    public static TargetResourceReclaimBody.InlinePayloadRelease releasedInlineOperation(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity owner,TargetQuotaPayloadOwner released,TargetQuotaMutation frontier){
        if(released.kind()!=TargetQuotaPayloadOwner.Kind.INLINE||released.phase()!=TargetQuotaPayloadOwner.Phase.RELEASED)return null;
        released.mutation().requireAtOrBefore(frontier);released.mutation().requireSourceApplied();
        final byte[] posKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},released.mutation().source().canonicalBytes());
        final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,posKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(posKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(released.mutation()))
            throw new IllegalStateException("released inline owner lacks its actual Source POSITION");
        final byte[] key=TargetRetirementSourceHistoryRecord.key(position.logicalId());
        final var history=TargetRetirementSourceHistoryRecord.decodeForStore(key,
                payload(view,overlay,ColumnFamily.META,key,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        if(history.input().type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM||!history.decidedAt().equals(released.mutation())
                ||!TargetResourceReclaimBody.decode(history.input().canonicalBody()).inlinePayloadRelease())
            throw new IllegalStateException("released inline owner lacks its exact inline Source history");
        requireFirst(view,overlay,scope,lineage,owner,history,frontier);
        final byte[] firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},position.logicalId());
        final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,firstKey,TargetResultRecord.VALUE_TYPE));
        position.requireFirst(first);
        if(SystemMutationResult.decode(first.typedPayload()).applyStatus()!=ApplyStatus.APPLIED)
            throw new IllegalStateException("released inline owner references a rejected Source decision");
        final var body=TargetResourceReclaimBody.decode(history.input().canonicalBody());
        final var operation=body.operations().stream().map(o->(TargetResourceReclaimBody.InlinePayloadRelease)o)
                .filter(o->o.expected().messageId().equals(released.messageId())).findFirst().orElseThrow(
                        ()->new IllegalStateException("inline Source omitted the released owner"));
        released.requireReleasedFrom(operation.expected(),body.floor());
        final byte[] terminalKey=TargetTerminalGenerationRecord.key(operation.locator());
        final byte[] terminalBytes=payload(view,overlay,ColumnFamily.TERMINAL,terminalKey,TargetTerminalGenerationRecord.VALUE_TYPE);
        final var terminal=TargetTerminalGenerationRecord.decode(terminalBytes);terminal.requireOwner(released);
        if(!Arrays.equals(terminal.key(),terminalKey)||!terminal.locator().equals(operation.locator())
                ||terminal.stateVersion()!=operation.stateVersion()||!terminal.runtime().attemptObligations().isEmpty()
                ||!Arrays.equals(Bytes.sha256(terminalBytes),operation.terminalDigest()))
            throw new IllegalStateException("released inline owner lost its exact closed terminal metadata");
        terminal.mutation().requireCoveredByFloor(body.floor());
        if(view.projected(ColumnFamily.ID,TargetKeyCodec.message(released.messageId()),overlay)!=null)
            throw new IllegalStateException("RELEASED inline owner still has a primary Message");
        return operation;
    }
}
