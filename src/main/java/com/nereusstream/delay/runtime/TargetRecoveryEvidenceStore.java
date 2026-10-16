package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.StoreMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Recovery owner/reference edits share the actual first successful Outcome's native accounting batch. */
public final class TargetRecoveryEvidenceStore {
    private TargetRecoveryEvidenceStore(){}

    public static List<TargetStoreBackend.Edit> adopt(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            TargetKafkaReceiptMapping mapping,TargetQuotaAttemptBudget attempt,TargetQuotaMutation stamp,SystemMutation outcome,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery snapshot){
        Objects.requireNonNull(snapshot,"full authenticated prepared recovery snapshot").requireExact(outcome);
        final var proof=snapshot.verified().proof();final var ref=snapshot.verified().reference();
        requireSame(mapping.canonicalBytes(),proof.mapping().canonicalBytes());requireBudget(attempt,proof.attempt());
        stamp.requireAfter(attempt.mutation());
        final var original=TargetExternalEvidenceStore.readAdoption(reader,mapping,scope,lineage);
        requireSame(original.anchor().primaryOwner().canonicalBytes(),proof.frozenOwner().canonicalBytes());
        final var owner=new TargetRecoveryEvidenceOwner(ref,proof.frozenOwner(),mapping.mutation(),mapping.mutationHash(),
                Bytes.sha256(proof.observer().canonicalBytes()),proof.observedAt(),outcome.type(),outcome.systemMutationId(),outcome.mutationHash(),stamp);
        owner.requireProof(proof);owner.requireSource(outcome,stamp.source());
        final var reference=owner.referenceRecord();reference.requireAttempt(attempt);
        if(!attempt.allocated().covers(original.anchor().blobCharge().add(owner.adoptedCharge())))
            throw new IllegalStateException("original frozen Attempt cannot pay exact anchor/recovery object and native STATE rows");
        for(byte[] key:List.of(owner.key(),reference.key(),reference.attemptKey()))
            if(reader.get(ColumnFamily.META,key)!=null)throw new IllegalStateException("new first Outcome already has recovery owner/reference/Attempt alias");
        return List.of(reader.replace(ColumnFamily.META,owner.key(),TargetRecoveryEvidenceOwner.VALUE_TYPE,owner.canonicalBytes()),
                reader.replace(ColumnFamily.META,reference.key(),TargetRecoveryEvidenceReference.VALUE_TYPE,reference.canonicalBytes()),
                reader.replace(ColumnFamily.META,reference.attemptKey(),TargetRecoveryEvidenceReference.VALUE_TYPE,reference.canonicalBytes()));
    }

    /** Only a guarded read of real owner/ref/first/POSITION/settled slot/Attempt facts can produce this token. */
    public static final class Adoption {
        private final TargetRecoveryEvidenceOwner owner;
        private final TargetRecoveryEvidenceReference reference;
        private final long retryUntilEpochMs;
        private Adoption(TargetRecoveryEvidenceOwner owner,TargetRecoveryEvidenceReference reference,long retryUntilEpochMs){
            this.owner=owner;this.reference=reference;this.retryUntilEpochMs=retryUntilEpochMs;
        }
        public TargetRecoveryEvidenceOwner owner(){return owner;}
        public TargetRecoveryEvidenceReference reference(){return reference;}
        public long retryUntilEpochMs(){return retryUntilEpochMs;}
    }

    public enum ClosedUseConclusion { RETAINED_APPLIED, RETAINED_REJECTED, SOURCE_FENCED }

    /** Actual facts at a protected Native read cut; no caller can manufacture a completed-use receipt. */
    public static final class ClosedUse {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedPreparedUse prepared;
        private final ClosedUseConclusion conclusion;
        private final byte[] firstDigest,positionDigest;
        private ClosedUse(OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedPreparedUse prepared,
                ClosedUseConclusion conclusion,TargetResultRecord first,TargetResultRecord position){
            this.prepared=prepared;this.conclusion=conclusion;
            firstDigest=first==null?new byte[0]:first.digest();positionDigest=position==null?new byte[0]:position.digest();
        }
        public OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedPreparedUse prepared(){return prepared;}
        public ClosedUseConclusion conclusion(){return conclusion;}
        public byte[] firstDigest(){return Bytes.copy(firstDigest);}
        public byte[] positionDigest(){return Bytes.copy(positionDigest);}
    }

    /** Preparation input only. The Source writer must recheck actual facts and durably retain their obligations. */
    public static final class ClosedUses {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure;
        private final TargetRecoveryEvidenceOwner owner;
        private final StoreMetadata store;
        private final TargetQuotaMutation cut;
        private final IngressFenceState fence;
        private final List<ClosedUse> uses;
        private ClosedUses(OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                TargetRecoveryEvidenceOwner owner,StoreMetadata store,TargetQuotaMutation cut,
                IngressFenceState fence,List<ClosedUse> uses){
            this.closure=closure;this.owner=owner;this.store=store;this.cut=cut;this.fence=fence;this.uses=List.copyOf(uses);
        }
        public OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure(){return closure;}
        public TargetRecoveryEvidenceOwner owner(){return owner;}
        public StoreMetadata store(){return store;}
        public TargetQuotaMutation cut(){return cut;}
        public IngressFenceState fence(){return fence;}
        public List<ClosedUse> uses(){return uses;}
    }

    /** Reads the full sealed set; a missing first without a committed fence is still possible future adoption. */
    public static ClosedUses readClosedUses(TargetStoreBackend backend,BoundedReadBudget reads,TargetQuotaScope scope,
            byte[] lineage,OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,Adoption applied,
            TargetStoreBackend.ReadAuthority authority){
        final var expected=Objects.requireNonNull(applied).owner();Objects.requireNonNull(closure).requireRecoveryOwner(expected);
        if(scope.target()!=null)throw new IllegalArgumentException("closed uses require the original Shard read scope");
        return backend.guardedRead(reads,reader->{
            final var owner=TargetRecoveryEvidenceOwner.decodeForStore(expected.key(),
                    payload(reader,expected.key(),TargetRecoveryEvidenceOwner.VALUE_TYPE),scope.shard(),scope.tenantScope(),lineage);
            requireSame(owner.canonicalBytes(),expected.canonicalBytes());closure.requireRecoveryOwner(owner);
            final var cut=reader.aggregate().mutation();owner.adoptedAt().requireAtOrBefore(cut);
            cut.requireAtOrBefore(reader.sourceSequence(),reader.source());
            final var reference=owner.referenceRecord();
            requireSame(reference.canonicalBytes(),payload(reader,reference.key(),TargetRecoveryEvidenceReference.VALUE_TYPE));
            requireSame(reference.canonicalBytes(),payload(reader,reference.attemptKey(),TargetRecoveryEvidenceReference.VALUE_TYPE));
            final byte[] attemptKey=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT},owner.reference().attemptId());
            final var attempt=TargetQuotaAttemptBudget.decodeForStore(attemptKey,
                    payload(reader,attemptKey,TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
            reference.requireAttempt(attempt);attempt.mutation().requireAtOrBefore(cut);
            if(!owner.adoptedAt().equals(attempt.resolvedAt())||!Arrays.equals(owner.admissionHash(),attempt.admissionDigest()))
                throw new IllegalStateException("closed proof lost its original resolved Attempt");
            final byte[] mappingKey=Bytes.concat(new byte[]{TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG,TargetKeyCodec.KEY_FORMAT},owner.reference().mappingId());
            final var mapping=TargetKafkaReceiptMapping.decodeForStore(mappingKey,
                    payload(reader,mappingKey,TargetKafkaReceiptMapping.VALUE_TYPE),scope.shard());
            if(!mapping.mutation().equals(owner.admissionMutation())||!Arrays.equals(mapping.mutationHash(),owner.admissionHash())
                    ||!Arrays.equals(mapping.admission().publishAttemptId(),owner.reference().attemptId()))
                throw new IllegalStateException("closed proof changed original Admission provenance");
            final var original=TargetExternalEvidenceStore.readAdoption(reader,mapping,scope,lineage);
            requireSame(original.anchor().primaryOwner().canonicalBytes(),owner.primaryOwner().canonicalBytes());
            final var live=descriptor(reader,owner.primaryOwner().identity(),scope,lineage);
            if(!live.accounting().equals(owner.primaryOwner().accounting()))throw new IllegalStateException("closed proof changed frozen accounting");
            live.latestMutation().requireAtOrBefore(cut);
            final byte[] rawFence=reader.get(ColumnFamily.META,KeyCodec.metaFixed(4));
            final var fence=rawFence==null?new IngressFenceState(IngressFenceState.OPEN,null)
                    :IngressFenceState.decode(TargetValueEnvelope.decode(rawFence,1).payload());
            final var uses=new ArrayList<ClosedUse>();boolean adoptingFirst=false;
            for(var use:closure.preparedUses()){
                final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},use.id());
                final byte[] raw=reader.get(ColumnFamily.DEDUPE,key);
                if(raw==null){
                    if(fence.proofId()==null||fence.closedThroughEpochMs()<use.retryUntil())
                        throw new IllegalStateException("sealed use still has possible future first adoption");
                    uses.add(new ClosedUse(use,ClosedUseConclusion.SOURCE_FENCED,null,null));continue;
                }
                final var first=TargetResultRecord.decode(TargetValueEnvelope.decode(raw,TargetResultRecord.VALUE_TYPE).payload());
                first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
                final var result=SystemMutationResult.decode(first.typedPayload());
                if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!Arrays.equals(result.mutationId(),use.id())
                        ||!Arrays.equals(result.mutationHash(),use.hash())||result.retryUntilEpochMs()!=use.retryUntil()
                        ||result.mutationType()!=SystemMutationType.PUBLISH_OUTCOME&&result.mutationType()!=SystemMutationType.EVIDENCE_RESOLUTION
                        ||first.primaryIdentity().kind()!=TargetQuotaIdentity.Kind.SHARD)
                    throw new IllegalStateException("closed prepared use differs from actual first result");
                first.mutation().requireAtOrBefore(cut);final var root=descriptor(reader,first.primaryIdentity(),scope,lineage);
                root.latestMutation().requireAtOrBefore(cut);first.requireOwner(root);
                final byte[] positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},first.mutation().source().canonicalBytes());
                final var position=TargetResultRecord.decode(TargetValueEnvelope.decode(required(reader,ColumnFamily.DEDUPE,positionKey),TargetResultRecord.VALUE_TYPE).payload());
                position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(root);
                if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(first.mutation()))
                    throw new IllegalStateException("closed first lost original POSITION companion");
                if(Arrays.equals(use.id(),owner.outcomeId())){
                    if(result.applyStatus()!=ApplyStatus.APPLIED||result.mutationType()!=owner.outcomeType()
                            ||!Arrays.equals(result.mutationHash(),owner.outcomeHash())||!first.mutation().equals(owner.adoptedAt()))
                        throw new IllegalStateException("closed proof lost actual adopting first");
                    adoptingFirst=true;
                }
                uses.add(new ClosedUse(use,result.applyStatus()==ApplyStatus.APPLIED
                        ?ClosedUseConclusion.RETAINED_APPLIED:ClosedUseConclusion.RETAINED_REJECTED,first,position));
            }
            if(!adoptingFirst)throw new IllegalStateException("closed proof lacks its actual sealed adopting first");
            reader.requireWithinElapsedBudget();return new ClosedUses(closure,owner,reader.metadata(),cut,fence,uses);
        },authority);
    }
    public static Adoption readAdoption(TargetStoreBackend backend,BoundedReadBudget reads,TargetQuotaScope scope,byte[] lineage,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery snapshot,SystemMutation outcome,TargetStoreBackend.ReadAuthority authority){
        snapshot.requireExact(outcome);
        return backend.guardedRead(reads,reader->{
            final var proof=snapshot.verified().proof();final var ref=snapshot.verified().reference();
            final byte[] ownerKey=TargetRecoveryEvidenceOwner.key(ref.proofDigest());
            final var owner=TargetRecoveryEvidenceOwner.decodeForStore(ownerKey,payload(reader,ownerKey,TargetRecoveryEvidenceOwner.VALUE_TYPE),scope.shard(),scope.tenantScope(),lineage);
            owner.requireProof(proof);owner.requireSource(outcome,owner.adoptedAt().source());owner.adoptedAt().requireAtOrBefore(reader.aggregate().mutation());
            final var expected=owner.referenceRecord();
            final var reference=TargetRecoveryEvidenceReference.decodeForStore(expected.key(),payload(reader,expected.key(),TargetRecoveryEvidenceReference.VALUE_TYPE),scope.shard(),lineage);
            reference.requireOwner(owner);requireSame(reference.canonicalBytes(),payload(reader,reference.attemptKey(),TargetRecoveryEvidenceReference.VALUE_TYPE));
            final byte[] firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},owner.outcomeId());
            final var first=TargetResultRecord.decode(TargetValueEnvelope.decode(required(reader,ColumnFamily.DEDUPE,firstKey),TargetResultRecord.VALUE_TYPE).payload());
            first.requireStored(firstKey,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
            final var result=SystemMutationResult.decode(first.typedPayload());
            if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.mutation().equals(owner.adoptedAt())
                    ||result.applyStatus()!=ApplyStatus.APPLIED||result.mutationType()!=owner.outcomeType()
                    ||!Arrays.equals(result.mutationId(),owner.outcomeId())||!Arrays.equals(result.mutationHash(),owner.outcomeHash())
                    ||!Arrays.equals(result.appliedSourcePosition(),owner.adoptedAt().source().canonicalBytes()))
                throw new IllegalStateException("recovery adoption lacks its actual exact APPLIED first result");
            final byte[] positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},owner.adoptedAt().source().canonicalBytes());
            final var position=TargetResultRecord.decode(TargetValueEnvelope.decode(required(reader,ColumnFamily.DEDUPE,positionKey),TargetResultRecord.VALUE_TYPE).payload());
            position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());
            position.requireFirst(first);
            if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(owner.adoptedAt()))
                throw new IllegalStateException("recovery adoption lost its exact Source POSITION companion");
            final var root=descriptor(reader,new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null),scope,lineage);
            first.requireOwner(root);position.requireOwner(root);
            final byte[] budgetKey=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT},ref.attemptId());
            final var budget=TargetQuotaAttemptBudget.decodeForStore(budgetKey,payload(reader,budgetKey,TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
            reference.requireAttempt(budget);
            if(budget.resolvedAt()==null||!budget.resolvedAt().equals(owner.adoptedAt())||!Arrays.equals(budget.admissionDigest(),owner.admissionHash()))
                throw new IllegalStateException("recovery adoption lacks its exact resolved original Attempt");
            budget.mutation().requireAtOrBefore(reader.aggregate().mutation());
            final var original=TargetExternalEvidenceStore.readAdoption(reader,proof.mapping(),scope,lineage);
            requireSame(original.anchor().primaryOwner().canonicalBytes(),owner.primaryOwner().canonicalBytes());
            final var live=descriptor(reader,owner.primaryOwner().identity(),scope,lineage);
            if(!live.accounting().equals(owner.primaryOwner().accounting()))throw new IllegalStateException("recovery adoption changed frozen accounting");
            final byte[] slotKey=TargetKafkaReceiptSlot.key(proof.mapping().admission().channel());
            final var slot=TargetKafkaReceiptSlot.decodeForStore(slotKey,payload(reader,slotKey,TargetKafkaReceiptSlot.VALUE_TYPE),scope.shard());
            if(Arrays.equals(slot.pendingMappingId(),ref.mappingId()))throw new IllegalStateException("adopted recovery mapping is still pending");
            if(!Arrays.equals(slot.recoveryLineage(),lineage)||Long.compareUnsigned(slot.lastAllocatedSequence(),proof.mapping().logicalSequence())<0)
                throw new IllegalStateException("recovery adoption lost original slot sequence/lineage");
            owner.adoptedAt().requireAtOrBefore(slot.mutation());
            slot.mutation().requireAtOrBefore(reader.aggregate().mutation());reader.requireWithinElapsedBudget();
            return new Adoption(owner,reference,outcome.retryUntilEpochMs());
        },authority);
    }

    /** Subtract only this original Attempt's actually adopted share; never infer ownership from a Catalog flag. */
    public static CapacityVector remainingAttemptCharge(TargetRecordAccounting.View reader,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaAttemptBudget budget,TargetQuotaScope scope,byte[] lineage,CapacityVector afterAnchor){
        final byte[] alias=reader.projected(ColumnFamily.META,TargetRecoveryEvidenceReference.attemptKey(budget.publishAttemptId()),overlay);
        if(alias==null)return afterAnchor;
        final var reference=TargetRecoveryEvidenceReference.decodeForStore(TargetRecoveryEvidenceReference.attemptKey(budget.publishAttemptId()),
                TargetValueEnvelope.decode(alias,TargetRecoveryEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
        reference.requireAttempt(budget);
        final byte[] key=TargetRecoveryEvidenceOwner.key(reference.proofDigest());
        final byte[] raw=reader.projected(ColumnFamily.META,key,overlay);
        if(raw==null)throw new IllegalStateException("Attempt recovery alias lost unique owner");
        final var owner=TargetRecoveryEvidenceOwner.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetRecoveryEvidenceOwner.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope(),lineage);
        reference.requireOwner(owner);
        final byte[] exact=reader.projected(ColumnFamily.META,reference.key(),overlay);
        if(exact==null)throw new IllegalStateException("Attempt recovery alias lost exact Source reference");
        requireSame(alias,exact);
        if(!Arrays.equals(budget.admissionDigest(),owner.admissionHash())||budget.resolvedAt()==null||!budget.resolvedAt().equals(owner.adoptedAt()))
            throw new IllegalStateException("recovery reserve transfer lacks original resolved Attempt provenance");
        return budget.phase()==TargetQuotaAttemptBudget.Phase.RELEASED?afterAnchor:afterAnchor.subtract(owner.adoptedCharge());
    }
    private static void requireBudget(TargetQuotaAttemptBudget current,TargetQuotaAttemptBudget observed){
        if(!current.locator().equals(observed.locator())||!current.primaryIdentity().equals(observed.primaryIdentity())
                ||!current.accounting().equals(observed.accounting())||!Arrays.equals(current.tenantScope(),observed.tenantScope())
                ||!Arrays.equals(current.recoveryLineage(),observed.recoveryLineage())||!Arrays.equals(current.publishAttemptId(),observed.publishAttemptId())
                ||!Arrays.equals(current.admissionDigest(),observed.admissionDigest())||current.executionBytes()!=observed.executionBytes()
                ||!current.allocated().equals(observed.allocated())||!current.commitment().equals(observed.commitment())
                ||current.resolvedAt()!=null||observed.resolvedAt()!=null
                ||current.phase()!=TargetQuotaAttemptBudget.Phase.ADMITTED&&current.phase()!=TargetQuotaAttemptBudget.Phase.UNKNOWN
                ||observed.phase()!=TargetQuotaAttemptBudget.Phase.ADMITTED&&observed.phase()!=TargetQuotaAttemptBudget.Phase.UNKNOWN)
            throw new IllegalStateException("recovery proof differs from original frozen unresolved Attempt");
        observed.mutation().requireAtOrBefore(current.mutation());
    }
    private static TargetQuotaIncarnation descriptor(TargetStoreBackend.Reader reader,TargetQuotaIdentity identity,TargetQuotaScope scope,byte[] lineage){
        final byte[] key=identity.key();key[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner=TargetQuotaIncarnation.decodeForStore(key,payload(reader,key,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!owner.identity().equals(identity)||!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("recovery owner changed original incarnation");return owner;
    }
    private static byte[] payload(TargetStoreBackend.Reader reader,byte[] key,int type){return TargetValueEnvelope.decode(required(reader,ColumnFamily.META,key),type).payload();}
    private static byte[] required(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key){return Objects.requireNonNull(reader.get(family,key),"actual retained recovery record");}
    private static void requireSame(byte[] left,byte[] right){if(!Arrays.equals(left,right))throw new IllegalStateException("recovery record changed its original exact bytes");}
}
