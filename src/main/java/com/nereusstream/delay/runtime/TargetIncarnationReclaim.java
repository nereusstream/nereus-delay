package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.*;

/** Exact Source lifecycle of an existing non-root owner; numeric balances never replace the protected full fold. */
public final class TargetIncarnationReclaim {
    private TargetIncarnationReclaim() {}
    private static byte[] firstKey(byte[] id){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},id);}
    private static byte[] positionKey(TargetQuotaMutation at){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},at.source().canonicalBytes());}
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(ColumnFamily.META,key,overlay),"incarnation lifecycle Native dependency"),type).payload();
    }
    private static TargetResultRecord result(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key){
        final var raw=Objects.requireNonNull(view.projected(ColumnFamily.DEDUPE,key,overlay),"incarnation lifecycle Source decision");
        final var first=TargetResultRecord.decode(TargetValueEnvelope.decode(raw,TargetResultRecord.VALUE_TYPE).payload());
        first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());return first;
    }
    private static boolean same(byte[] a,byte[] b){return Arrays.equals(a,b);}
    private static TargetQuotaIncarnation expected(TargetResourceReclaimBody body){
        return body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationDrain d?d.expected():((TargetResourceReclaimBody.IncarnationRetire)body.operations().getFirst()).expected();
    }
    private static TargetQuotaMutation closeFirst(TargetResourceReclaimBody.IncarnationDrain drain,TargetResultRecord first){
        final var body=drain.close().body();final var accepted=SystemMutationResult.decode(first.typedPayload());
        final byte[] hash=SystemMutation.computeMutationHash(body.shard(),SystemMutationType.APPLY_SHARD_CONTROL,body.retryUntil(),body.canonicalBytes());
        final byte[] id=SystemMutation.computeSystemMutationId(body.shard(),SystemMutationType.APPLY_SHARD_CONTROL,body.logicalIdentity(),hash);
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.appliedFirst()||!first.mutation().equals(drain.close().mutation())||accepted.stableCode()!=StableCode.OK
                ||accepted.mutationType()!=SystemMutationType.APPLY_SHARD_CONTROL||!same(id,first.logicalId())||!same(hash,accepted.mutationHash()))
            throw new IllegalStateException("drain lacks its actual accepted explicit Close decision");
        return first.mutation();
    }
    private static byte[] closeId(TargetCloseRecord close){
        final var b=close.body();return SystemMutation.computeSystemMutationId(b.shard(),SystemMutationType.APPLY_SHARD_CONTROL,b.logicalIdentity(),
                SystemMutation.computeMutationHash(b.shard(),SystemMutationType.APPLY_SHARD_CONTROL,b.retryUntil(),b.canonicalBytes()));
    }
    private static TargetQuotaMutation applied(TargetCheckpointDependencies d,SystemMutation input){
        final var first=d.resultRecord(firstKey(input.systemMutationId()));
        if(first==null||!first.appliedFirst())return null;
        final var history=d.sourceHistories().stream().filter(h->same(h.input().canonicalEnvelope(),input.canonicalEnvelope())).findFirst().orElseThrow();
        requireHistoryFirst(history,first);
        if(!first.mutation().equals(history.decidedAt())||!same(first.mutation().mutationDigest(),Bytes.sha256(input.canonicalEnvelope())))throw new IllegalStateException("incarnation lifecycle changed the original accepted Source frame");
        d.requireSystemPosition(first);return first.mutation();
    }
    private static void requireHistoryFirst(TargetRetirementSourceHistoryRecord history,TargetResultRecord first){
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.mutation().equals(history.decidedAt())
                ||!same(first.typedPayload(),SystemMutationResult.from(history.input(),ApplyStatus.APPLIED,StableCode.OK,history.decidedAt().source().canonicalBytes()).encode()))
            throw new IllegalStateException("incarnation lifecycle lost its exact APPLIED/OK Source first");
    }
    public static TargetQuotaMutation requireClosed(TargetCheckpointDependencies d,TargetResourceReclaimBody body,SystemMutation input,boolean allowApplied,Runnable current){
        final var facts=Objects.requireNonNull(body.nativeFacts());facts.requireFor(body);current.run();
        final var fence=TargetNativeRetainedReclaim.fence(facts);final var fenceFirst=d.requireTimeFence(fence);
        if(!fenceFirst.mutation().equals(facts.fence().appliedAt())
                ||d.ingressFence().closedThroughEpochMs()<TargetTimeFenceBody.decode(fence.mutation().canonicalBody()).closeThrough())throw new IllegalStateException("incarnation lifecycle lost its original closure fence");
        final TargetQuotaMutation at=input==null?null:applied(d,input);
        if(at!=null&&!allowApplied)throw new IllegalStateException("incarnation lifecycle was already applied");
        final var owner=expected(body);final var actual=d.incarnation(owner.key());
        if(at==null){
            if(actual==null||!same(owner.canonicalBytes(),actual.canonicalBytes()))throw new IllegalStateException("incarnation changed after selection");
        }
        if(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationDrain drain){
            final var first=Objects.requireNonNull(d.resultRecord(firstKey(closeId(drain.close()))),"original Target Close first");
            closeFirst(drain,first);d.requireSystemPosition(first);
            if(at==null){
                final var stored=d.controls().stream().filter(v->v.valueType()==TargetCloseRecord.VALUE_TYPE&&same(v.key(),drain.close().key())).findFirst().orElseThrow();
                if(!same(stored.payload(),drain.close().canonicalBytes()))throw new IllegalStateException("drain replaced original Close marker");
            }else{
                final var successor=owner.drainingProjection(at);
                if(actual!=null&&!same(actual.canonicalBytes(),successor.canonicalBytes()))throw new IllegalStateException("drain lost exact irreversible successor");
                if(actual==null)requireRetiredSuccessor(d,successor);
            }
        }else{
            final var retire=(TargetResourceReclaimBody.IncarnationRetire)body.operations().getFirst();
            requireDrain(d,retire.expected());
            if(at==null){
                if(d.ownerRecordCount(owner.identity())!=1)throw new IllegalStateException("actual independent ledger still has incarnation-owned records");
                final var primary=Objects.requireNonNull(d.quotaCounter(owner.identity()));final var mirror=Objects.requireNonNull(d.quotaCounter(owner.tenantIdentity()));
                if(!primary.usage().equals(owner.ownContribution())||!mirror.usage().equals(owner.tenantContribution()))throw new IllegalStateException("incarnation still has allocated or retained resources");
                primary.mutation().requireCoveredByFloor(body.floor());mirror.mutation().requireCoveredByFloor(body.floor());
                final var grants=d.controls().stream().filter(v->v.valueType()==TargetQuotaGrantActivation.VALUE_TYPE).map(v->TargetQuotaGrantActivation.decode(v.payload()))
                        .filter(v->v.allocation()!=null&&v.allocation().identity().equals(owner.identity())).toList();
                if(grants.size()!=1||!same(grants.getFirst().canonicalBytes(),retire.zeroGrant().canonicalBytes()))throw new IllegalStateException("retirement changed its live zero-fee grant reference");
            }else{
                if(actual!=null)throw new IllegalStateException("retired incarnation descriptor survives");
                requireZero(d.quotaCounter(owner.identity()),at);requireZero(d.quotaCounter(owner.tenantIdentity()),at);
                if(d.ownerRecordCount(owner.identity())!=0||d.controls().stream().anyMatch(v->same(v.key(),retire.zeroGrant().key())))throw new IllegalStateException("retired incarnation retains Native references");
            }
        }
        current.run();return at;
    }
    public static TargetQuotaMutation requirePreparedClosed(TargetRetirementLedgerCursor ledger,TargetResourceReclaimBody body,SystemMutation input,boolean allowApplied,Runnable current){
        final var d=ledger.dependenciesForClosure();
        final var facts=Objects.requireNonNull(body.nativeFacts());facts.requireFor(body);current.run();
        final var fence=TargetNativeRetainedReclaim.fence(facts);final var fenceFirst=d.requireTimeFence(fence);
        if(!fenceFirst.mutation().equals(facts.fence().appliedAt())
                ||d.ingressFence().closedThroughEpochMs()<TargetTimeFenceBody.decode(fence.mutation().canonicalBody()).closeThrough())throw new IllegalStateException("incarnation lifecycle lost its original closure fence");
        final TargetQuotaMutation at=input==null?null:preparedApplied(d,input);
        if(at!=null&&!allowApplied)throw new IllegalStateException("incarnation lifecycle was already applied");
        final var owner=expected(body);final var actual=d.incarnation(owner.key());
        if(at==null){
            if(actual==null||!same(owner.canonicalBytes(),actual.canonicalBytes()))throw new IllegalStateException("incarnation changed after selection");
        }
        if(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationDrain drain){
            final var first=Objects.requireNonNull(d.resultRecord(firstKey(closeId(drain.close()))),"original Target Close first");
            closeFirst(drain,first);d.requireSystemPosition(first);
            if(at==null){
                final var stored=Objects.requireNonNull(d.control(drain.close().key()),"original Target Close marker");
                if(stored.valueType()!=TargetCloseRecord.VALUE_TYPE)throw new IllegalStateException("drain Close marker changed type");
                if(!same(stored.payload(),drain.close().canonicalBytes()))throw new IllegalStateException("drain replaced original Close marker");
            }else{
                final var successor=owner.drainingProjection(at);
                if(actual!=null&&!same(actual.canonicalBytes(),successor.canonicalBytes()))throw new IllegalStateException("drain lost exact irreversible successor");
                if(actual==null)requirePreparedRetiredSuccessor(d,successor);
            }
        }else{
            final var retire=(TargetResourceReclaimBody.IncarnationRetire)body.operations().getFirst();
            requirePreparedDrain(d,retire.expected());
            if(at==null){
                if(d.ownerRecordCount(owner.identity())!=1)throw new IllegalStateException("actual independent ledger still has incarnation-owned records");
                final var primary=Objects.requireNonNull(d.quotaCounter(owner.identity()));final var mirror=Objects.requireNonNull(d.quotaCounter(owner.tenantIdentity()));
                if(!primary.usage().equals(owner.ownContribution())||!mirror.usage().equals(owner.tenantContribution()))throw new IllegalStateException("incarnation still has allocated or retained resources");
                primary.mutation().requireCoveredByFloor(body.floor());mirror.mutation().requireCoveredByFloor(body.floor());
                final var grant=d.control(retire.zeroGrant().key());
                if(ledger.grantCount(owner.identity())!=1||grant==null||grant.valueType()!=TargetQuotaGrantActivation.VALUE_TYPE
                        ||!same(grant.payload(),retire.zeroGrant().canonicalBytes()))throw new IllegalStateException("retirement changed its live zero-fee grant reference");
            }else{
                if(actual!=null)throw new IllegalStateException("retired incarnation descriptor survives");
                requireZero(d.quotaCounter(owner.identity()),at);requireZero(d.quotaCounter(owner.tenantIdentity()),at);
                if(d.ownerRecordCount(owner.identity())!=0||d.control(retire.zeroGrant().key())!=null)throw new IllegalStateException("retired incarnation retains Native references");
            }
        }
        current.run();return at;
    }

    private static TargetQuotaMutation preparedApplied(TargetCheckpointDependencies d,SystemMutation input){
        final var first=d.resultRecord(firstKey(input.systemMutationId()));if(first==null||!first.appliedFirst())return null;
        final var history=Objects.requireNonNull(d.sourceHistory(input.systemMutationId()),"actual incarnation lifecycle history");
        if(!same(history.input().canonicalEnvelope(),input.canonicalEnvelope()))throw new IllegalStateException("incarnation lifecycle changed exact original input");
        requireHistoryFirst(history,first);
        if(!first.mutation().equals(history.decidedAt())||!same(first.mutation().mutationDigest(),Bytes.sha256(input.canonicalEnvelope())))throw new IllegalStateException("incarnation lifecycle changed the original accepted Source frame");
        d.requireSystemPosition(first);return first.mutation();
    }
    private static void requirePreparedDrain(TargetCheckpointDependencies d,TargetQuotaIncarnation owner){
        final var position=Objects.requireNonNull(d.resultRecord(positionKey(owner.latestMutation())),"original Source drain POSITION");
        final var history=Objects.requireNonNull(d.sourceHistory(position.logicalId()),"original Source drain history");requireDrainHistory(history,owner);
        final var first=Objects.requireNonNull(d.resultRecord(firstKey(history.input().systemMutationId())));requireHistoryFirst(history,first);position.requireFirst(first);
    }
    private static void requirePreparedRetiredSuccessor(TargetCheckpointDependencies d,TargetQuotaIncarnation drained){
        final var counter=Objects.requireNonNull(d.quotaCounter(drained.identity()));
        final var position=Objects.requireNonNull(d.resultRecord(positionKey(counter.mutation())));
        final var history=Objects.requireNonNull(d.sourceHistory(position.logicalId()),"retired incarnation actual Source history");
        requireRetireHistory(history,drained);requireZero(counter,history.decidedAt());requireZero(d.quotaCounter(drained.tenantIdentity()),history.decidedAt());
        if(preparedApplied(d,history.input())==null)throw new IllegalStateException("missing actual APPLIED incarnation retirement");
    }
    private static void requireZero(TargetQuotaCounter counter,TargetQuotaMutation at){
        if(counter==null||!counter.usage().equals(TargetQuotaUsage.empty())||!counter.mutation().equals(at))throw new IllegalStateException("retirement lost its exact retained zero counter");
    }
    private static void requireDrain(TargetCheckpointDependencies d,TargetQuotaIncarnation owner){
        final var position=Objects.requireNonNull(d.resultRecord(positionKey(owner.latestMutation())),"original Source drain POSITION");
        final var history=d.sourceHistories().stream().filter(h->same(h.input().systemMutationId(),position.logicalId())).findFirst().orElseThrow();
        requireDrainHistory(history,owner);final var first=Objects.requireNonNull(d.resultRecord(firstKey(history.input().systemMutationId())));
        requireHistoryFirst(history,first);
        position.requireFirst(first);
    }
    private static void requireDrainHistory(TargetRetirementSourceHistoryRecord history,TargetQuotaIncarnation owner){
        if(!history.requiredTuple().equals(ProtocolTuple.targetIncarnationReclaim())||!history.decidedAt().equals(owner.latestMutation()))throw new IllegalStateException("incarnation has another drain history origin");
        final var body=TargetResourceReclaimBody.decode(history.input().canonicalBody());
        if(!(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationDrain drain)
                ||!same(drain.expected().drainingProjection(history.decidedAt()).canonicalBytes(),owner.canonicalBytes()))throw new IllegalStateException("incarnation changed its original drain before/after");
    }
    private static void requireRetiredSuccessor(TargetCheckpointDependencies d,TargetQuotaIncarnation drained){
        final var counter=Objects.requireNonNull(d.quotaCounter(drained.identity()));
        final var position=Objects.requireNonNull(d.resultRecord(positionKey(counter.mutation())));
        final var history=d.sourceHistories().stream().filter(h->same(h.input().systemMutationId(),position.logicalId())).findFirst().orElseThrow();
        requireRetireHistory(history,drained);requireZero(counter,history.decidedAt());requireZero(d.quotaCounter(drained.tenantIdentity()),history.decidedAt());
        if(applied(d,history.input())==null)throw new IllegalStateException("missing actual APPLIED incarnation retirement");
    }
    private static void requireRetireHistory(TargetRetirementSourceHistoryRecord history,TargetQuotaIncarnation drained){
        if(!history.requiredTuple().equals(ProtocolTuple.targetIncarnationReclaim()))throw new IllegalStateException("zero counter lacks its original retirement Source");
        final var body=TargetResourceReclaimBody.decode(history.input().canonicalBody());
        if(!(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationRetire retire)||!same(retire.expected().canonicalBytes(),drained.canonicalBytes()))
            throw new IllegalStateException("retirement changed exact drained incarnation");
    }
    public static void requireCurrent(TargetStoreBackend.Reader reader,SystemMutation input,Runnable current){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!TargetResourceReclaimStore.matchesExpected(reader,body))throw new IllegalStateException("incarnation lifecycle Native values changed");
        if(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationDrain drain){
            final var first=result(reader,List.of(),firstKey(closeId(drain.close())));closeFirst(drain,first);
            result(reader,List.of(),positionKey(first.mutation())).requireFirst(first);
        }else requireDrain(reader,List.of(),expected(body));
        current.run();
    }
    private static void requireDrain(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetQuotaIncarnation owner){
        final var position=result(view,overlay,positionKey(owner.latestMutation()));
        final var history=TargetRetirementSourceHistoryRecord.decode(payload(view,overlay,TargetRetirementSourceHistoryRecord.key(position.logicalId()),TargetRetirementSourceHistoryRecord.VALUE_TYPE));
        requireDrainHistory(history,owner);final var first=result(view,overlay,firstKey(history.input().systemMutationId()));
        requireHistoryFirst(history,first);position.requireFirst(first);
    }
    public static void requireAppliedDecision(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,TargetResourceReclaimBody body,TargetQuotaMutation at){
        final var owner=expected(body);final byte[] raw=view.projected(ColumnFamily.META,owner.key(),overlay);
        if(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationDrain drain){
            final var first=result(view,overlay,firstKey(closeId(drain.close())));closeFirst(drain,first);result(view,overlay,positionKey(first.mutation())).requireFirst(first);
            final var successor=owner.drainingProjection(at);
            if(raw!=null){if(!same(TargetValueEnvelope.decode(raw,TargetQuotaIncarnation.VALUE_TYPE).payload(),successor.canonicalBytes()))throw new IllegalStateException("drain changed irreversible Native successor");}
            else{
                final var counter=TargetQuotaCounter.decode(payload(view,overlay,owner.identity().key(),TargetQuotaCounter.VALUE_TYPE));
                final var position=result(view,overlay,positionKey(counter.mutation()));
                final var history=TargetRetirementSourceHistoryRecord.decode(payload(view,overlay,TargetRetirementSourceHistoryRecord.key(position.logicalId()),TargetRetirementSourceHistoryRecord.VALUE_TYPE));
                requireRetireHistory(history,successor);requireZero(counter,history.decidedAt());
                requireZero(TargetQuotaCounter.decode(payload(view,overlay,owner.tenantIdentity().key(),TargetQuotaCounter.VALUE_TYPE)),history.decidedAt());
                final var retired=result(view,overlay,firstKey(history.input().systemMutationId()));
                requireHistoryFirst(history,retired);position.requireFirst(retired);
            }
        }else{
            final var retire=(TargetResourceReclaimBody.IncarnationRetire)body.operations().getFirst();requireDrain(view,overlay,owner);
            if(raw!=null||view.projected(ColumnFamily.META,retire.zeroGrant().key(),overlay)!=null)throw new IllegalStateException("retirement retained descriptor/grant");
            requireCounterClosure(view,overlay,retire,body.floor(),at);
        }
    }
    /** During original Source fee derivation, typed business deletes precede generated counter edits.
     * This verifies that exact before/after projection; persisted/cold reads must observe the real zero successors. */
    private static void requireCounterClosure(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetResourceReclaimBody.IncarnationRetire retire,RecoveryFloorRef floor,TargetQuotaMutation at){
        final var owner=retire.expected();
        final var primary=TargetQuotaCounter.decode(payload(view,overlay,owner.identity().key(),TargetQuotaCounter.VALUE_TYPE));
        final var mirror=TargetQuotaCounter.decode(payload(view,overlay,owner.tenantIdentity().key(),TargetQuotaCounter.VALUE_TYPE));
        if(primary.mutation().equals(at)||mirror.mutation().equals(at)){
            requireZero(primary,at);requireZero(mirror,at);return;
        }
        requireDeletedProjection(view,overlay,owner.key(),TargetQuotaIncarnation.VALUE_TYPE,owner.canonicalBytes());
        requireDeletedProjection(view,overlay,retire.zeroGrant().key(),TargetQuotaGrantActivation.VALUE_TYPE,retire.zeroGrant().canonicalBytes());
        if(!primary.usage().equals(owner.ownContribution())||!mirror.usage().equals(owner.tenantContribution()))throw new IllegalStateException("retirement accounting projection changed actual descriptor-only counters");
        primary.mutation().requireCoveredByFloor(floor);mirror.mutation().requireCoveredByFloor(floor);
        at.requireAfter(primary.mutation());at.requireAfter(mirror.mutation());
    }
    private static void requireDeletedProjection(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key,int type,byte[] expected){
        final var edit=overlay.stream().filter(e->e.family()==ColumnFamily.META&&same(e.key(),key)).findFirst().orElseThrow(
                ()->new IllegalStateException("persisted retirement lacks its actual zero-counter successors"));
        final byte[] actual=view.projected(ColumnFamily.META,key,List.of());
        if(edit.after()!=null||edit.before()==null||actual==null||!same(edit.before(),actual)
                ||!same(TargetValueEnvelope.decode(actual,type).payload(),expected))throw new IllegalStateException("retirement is not the exact original Source business deletion projection");
    }
    /** A real retained zero counter plus its exact APPLIED retirement, never absence or a caller lifecycle flag. */
    public static boolean isRetired(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity identity){
        final byte[] raw=reader.get(ColumnFamily.META,identity.key());if(raw==null)return false;
        final var counter=TargetQuotaCounter.decodeForStore(identity.key(),TargetValueEnvelope.decode(raw,TargetQuotaCounter.VALUE_TYPE).payload(),scope.shard());
        if(!counter.usage().equals(TargetQuotaUsage.empty()))return false;
        final var position=result(reader,List.of(),positionKey(counter.mutation()));
        final byte[] key=TargetRetirementSourceHistoryRecord.key(position.logicalId());
        final var history=TargetRetirementSourceHistoryRecord.decodeForStore(key,payload(reader,List.of(),key,TargetRetirementSourceHistoryRecord.VALUE_TYPE),scope,lineage);
        if(!history.requiredTuple().equals(ProtocolTuple.targetIncarnationReclaim()))return false;
        final var body=TargetResourceReclaimBody.decode(history.input().canonicalBody());
        if(!(body.operations().getFirst() instanceof TargetResourceReclaimBody.IncarnationRetire retire)||!retire.expected().identity().equals(identity))return false;
        final var sourceOwner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        TargetRetirementSourceHistoryStore.requireFirst(reader,List.of(),scope,lineage,sourceOwner,history,reader.aggregate().mutation());
        reader.requireWithinElapsedBudget();return true;
    }
    /** New source-ordered capability validates every actual drain descriptor without changing older origins. */
    public static void requireDrainingOrigin(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIncarnation owner,TargetQuotaMutation frontier){
        final byte[] key=TargetProtocolActivationRecord.key(ProtocolTuple.targetIncarnationReclaim());
        final byte[] raw=view.projected(ColumnFamily.META,key,overlay);if(raw==null)return;
        final var activation=TargetProtocolActivationRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetProtocolActivationRecord.VALUE_TYPE).payload(),scope,lineage);
        if(activation.appliedAt().source().compareTo(owner.latestMutation().source())>0)return;
        if(!activation.body().payload().descriptor().supportsIncarnationReclaim()||activation.appliedAt().source().compareTo(owner.latestMutation().source())==0)
            throw new IllegalStateException("drain lacks its actual prior independent capability");
        owner.latestMutation().requireAtOrBefore(frontier);requireDrain(view,overlay,owner);
    }
}
