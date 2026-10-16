package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Actual Native preparation for an already closed unadopted Catalog object; no I/O-settlement or deletion authority. */
public final class TargetStagingEvidenceStore {
    private TargetStagingEvidenceStore() {}

    public static final class ClosedUses {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure;
        private final StoreMetadata store;
        private final TargetQuotaMutation cut;
        private final IngressFenceState fence;
        private final List<TargetEvidenceRetirementContext.Use> uses;
        private final TargetQuotaIncarnation feeOwner;
        private ClosedUses(OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,StoreMetadata store,
                TargetQuotaMutation cut,IngressFenceState fence,List<TargetEvidenceRetirementContext.Use> uses,TargetQuotaIncarnation feeOwner){
            this.closure=closure;this.store=store;this.cut=cut;this.fence=fence;this.uses=List.copyOf(uses);this.feeOwner=feeOwner;
        }
        public OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure(){return closure;}
        public TargetQuotaIncarnation feeOwner(){return feeOwner;}
        public TargetEvidenceRetirementContext retirementContext(TargetEvidenceRetirementCopy copy){
            if(!(copy.resource().provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging)
                    ||!Arrays.equals(closure.canonicalBytes(),copy.closureImage()))
                throw new IllegalArgumentException("staging preparation changed original closed reservation");
            closure.requireObjectFingerprint(copy.resource());
            final var context=new TargetEvidenceRetirementContext(closure.revision(),closure.digest(),closure.canonicalBytes().length,
                    store,cut,fence,uses,copy.digest(),copy.canonicalBytes().length,feeOwner);
            copy.requireContext(context);return context;
        }
    }

    public static ClosedUses readClosedUses(TargetStoreBackend backend,BoundedReadBudget reads,TargetQuotaScope scope,
            byte[] lineage,OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,TargetStoreBackend.ReadAuthority authority){
        if(scope.target()!=null||reads.maxBytes()==Long.MAX_VALUE||closure.preparedUses().size()>TargetEvidenceRetirementContext.MAX_USES)
            throw new IllegalArgumentException("staging preparation requires actual SHARD and finite complete uses/read bounds");
        return backend.guardedRead(reads,reader->{
            final var cut=reader.aggregate().mutation();cut.requireAtOrBefore(reader.sourceSequence(),reader.source());
            final var identity=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
            final byte[] ownerKey=identity.key();ownerKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final var owner=TargetQuotaIncarnation.decodeForStore(ownerKey,payload(reader,ColumnFamily.META,ownerKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
            if(!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("staging SHARD changed original recovery lineage");
            owner.latestMutation().requireAtOrBefore(cut);
            final byte[] rawFence=reader.get(ColumnFamily.META,KeyCodec.metaFixed(4));
            final var fence=rawFence==null?new IngressFenceState(IngressFenceState.OPEN,null)
                    :IngressFenceState.decode(TargetValueEnvelope.decode(rawFence,1).payload());
            final var uses=new ArrayList<TargetEvidenceRetirementContext.Use>();
            for(var use:closure.preparedUses()){
                final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},use.id());
                final byte[] raw=reader.get(ColumnFamily.DEDUPE,key);
                if(raw==null){
                    if(fence.proofId()==null||fence.closedThroughEpochMs()<use.retryUntil())
                        throw new IllegalStateException("staging sealed use still permits a future first adoption");
                    uses.add(new TargetEvidenceRetirementContext.Use(use.id(),use.hash(),use.retryUntil(),
                            TargetEvidenceRetirementContext.Conclusion.SOURCE_FENCED,new byte[0],new byte[0]));continue;
                }
                final var first=TargetResultRecord.decode(TargetValueEnvelope.decode(raw,TargetResultRecord.VALUE_TYPE).payload());
                first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
                final var result=SystemMutationResult.decode(first.typedPayload());
                final boolean family=closure.subtype()==3
                        ?result.mutationType()==SystemMutationType.PUBLISH_OUTCOME||result.mutationType()==SystemMutationType.EVIDENCE_RESOLUTION
                        :result.mutationType()==SystemMutationType.TARGET_PUBLISH_ADMISSION;
                if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.primaryIdentity().equals(identity)||!family
                        ||!Arrays.equals(result.mutationId(),use.id())||!Arrays.equals(result.mutationHash(),use.hash())
                        ||result.retryUntilEpochMs()!=use.retryUntil()||result.applyStatus()!=ApplyStatus.REJECTED
                        ||!Arrays.equals(result.appliedSourcePosition(),first.mutation().source().canonicalBytes()))
                    throw new IllegalStateException("staging use lacks exact retained rejected first");
                first.mutation().requireAtOrBefore(cut);first.requireOwner(owner);
                final byte[] positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},first.mutation().source().canonicalBytes());
                final var position=TargetResultRecord.decode(payload(reader,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
                position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(owner);
                if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(first.mutation()))
                    throw new IllegalStateException("staging rejected first lost its exact POSITION companion");
                uses.add(new TargetEvidenceRetirementContext.Use(use.id(),use.hash(),use.retryUntil(),
                        TargetEvidenceRetirementContext.Conclusion.RETAINED_REJECTED,first.digest(),position.digest()));
            }
            TargetEvidenceRetirementStore.requireStagingUnadopted(reader,List.of(),scope,lineage,identity,closure.subtype(),
                    closure.objectDigest(),closure.exactKey(),owner,cut);
            reader.requireWithinElapsedBudget();return new ClosedUses(closure,reader.metadata(),cut,fence,uses,owner);
        },authority);
    }
    private static byte[] payload(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(reader.get(family,key),"retained staging Native row"),type).payload();
    }
}
