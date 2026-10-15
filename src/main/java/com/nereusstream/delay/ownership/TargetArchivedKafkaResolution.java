package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.TargetPublishOutcomeStore;
import java.util.Objects;

/** A Root-signed exact v7 Source image whose real Catalog object/prepared seal finished before handoff. */
public final class TargetArchivedKafkaResolution {
    private final TargetPublishOutcomeStore.KafkaUnknown original;
    private final SystemMutation mutation;
    private final OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery prepared;
    private final WorkerPublishOutcomeMutationFactory.OutcomeContext context;
    private TargetArchivedKafkaResolution(TargetPublishOutcomeStore.KafkaUnknown original,SystemMutation mutation,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery prepared,WorkerPublishOutcomeMutationFactory.OutcomeContext context){
        this.original=original;this.mutation=mutation;this.prepared=prepared;this.context=context;
    }
    /** Run only on the original bounded owned I/O operation; this performs actual Catalog I/O. */
    public static TargetArchivedKafkaResolution prepare(TargetPublishOutcomeStore.KafkaUnknown actual,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.RecoveryPublication publication,AuthorIdentity service,StableCode code,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context,TargetPublishOutcomeMutationFactory signer,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,TargetRecoveryProofTrust trust,Runnable current){
        Objects.requireNonNull(current).run();
        final var mutation=Objects.requireNonNull(signer).createKafkaResolutionWithRecoveryProof(actual,publication,service,code,context);
        current.run();final var sealed=Objects.requireNonNull(catalog).sealRecoveryPrepared(publication,mutation,trust,current);
        sealed.requireExact(mutation);current.run();return new TargetArchivedKafkaResolution(actual,mutation,sealed,context);
    }
    public SystemMutation mutation(){return mutation;}
    public OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery prepared(){return prepared;}
    public WorkerPublishOutcomeMutationFactory.OutcomeContext context(){return context;}
    public void requireUnknown(TargetPublishOutcomeStore.KafkaUnknown actual){original.requireSame(actual);prepared.requireExact(mutation);}
}
