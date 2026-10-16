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
        final var activated=activation(view,overlay,scope,lineage,owner,frontier);
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
        if(result.mutationType()!=SystemMutationType.RESOURCE_RETIRE_INTENT&&result.mutationType()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED)return;
        if(result.applyStatus()!=ApplyStatus.APPLIED&&result.stableCode()!=StableCode.UNAUTHORIZED_SYSTEM_MUTATION)return;
        final byte[] key=TargetProtocolActivationRecord.key(ProtocolTuple.targetEvidenceRetirement()),raw=view.projected(ColumnFamily.META,key,overlay);
        if(raw==null)return;
        final var activation=TargetProtocolActivationRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
        if(activation.body().payload().descriptor().supportsSourceHistoryRecords()&&activation.appliedAt().source().compareTo(first.mutation().source())<0)
            requireRetained(view,overlay,scope,lineage,owner,result.mutationId(),first.mutation(),frontier);
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
        final byte[] activationKey=TargetProtocolActivationRecord.key(ProtocolTuple.targetEvidenceRetirement());
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
            final var activation=activation(view,overlay,scope,lineage,owner,frontier);
            if(!activation.body().payload().descriptor().supportsSourceHistoryRecords())throw new IllegalStateException("Native54 lacks explicitly activated descriptor2");
            final var grant=record.declaredGrant().orElseThrow(()->new IllegalStateException("APPLIED Source history has no declared SERVICE grant"));
            if(!input.verifySignature(grant.writerKey()))throw new IllegalStateException("APPLIED Source frame differs from retained grant key");
            grant.requireActivation(activation,record.decidedAt().source());
        }
    }
    /** Descriptor1 remains a readable historical format; new descriptor2 state must retain its atomic proof. */
    public static void requireRetained(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,byte[] lineage,
            TargetQuotaIdentity owner,byte[] id,TargetQuotaMutation decided,TargetQuotaMutation frontier){
        final var activation=activation(view,overlay,scope,lineage,owner,frontier);
        if(!activation.body().payload().descriptor().supportsSourceHistoryRecords())return;
        final byte[] key=TargetRetirementSourceHistoryRecord.key(id);
        final var record=TargetRetirementSourceHistoryRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        if(!record.decidedAt().equals(decided))throw new IllegalStateException("retirement lost exact Source history stamp");
        requireFirst(view,overlay,scope,lineage,owner,record,frontier);
    }
    private static TargetProtocolActivationRecord activation(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaScope scope,
            byte[] lineage,TargetQuotaIdentity owner,TargetQuotaMutation frontier){
        final byte[] key=TargetProtocolActivationRecord.key(ProtocolTuple.targetEvidenceRetirement());
        final var value=TargetProtocolActivationRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.META,key,TargetProtocolActivationRecord.VALUE_TYPE),scope,lineage);
        TargetProtocolActivationStore.requireApplied(view,overlay,scope,lineage,owner,value,frontier);return value;
    }
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"retained Source history dependency"),type).payload();
    }
}
