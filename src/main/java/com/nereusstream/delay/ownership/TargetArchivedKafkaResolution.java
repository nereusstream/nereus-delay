package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.TargetPublishOutcomeStore;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** A Root-signed exact v7 Source image whose real Catalog object/prepared seal finished before handoff. */
public final class TargetArchivedKafkaResolution {
    private final TargetPublishOutcomeStore.KafkaUnknown original;
    private final SystemMutation mutation;
    private final OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery prepared;
    private final WorkerPublishOutcomeMutationFactory.OutcomeContext context;
    private final SourceCompletion completion;

    @FunctionalInterface
    public interface SourceCompletion {
        /** Actual Source first must be reread; present means real Native APPLIED adoption, empty means actual REJECTED. */
        CompletableFuture<Optional<com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption>> complete(
                TargetArchivedKafkaResolution exact);
    }

    private TargetArchivedKafkaResolution(
            TargetPublishOutcomeStore.KafkaUnknown original,
            SystemMutation mutation,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery prepared,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context,
            SourceCompletion completion) {
        this.original = original;
        this.mutation = mutation;
        this.prepared = prepared;
        this.context = context;
        this.completion = completion;
    }
    /** Run only on the original bounded owned I/O operation; this performs actual Catalog I/O. */
    public static TargetArchivedKafkaResolution prepare(
            TargetPublishOutcomeStore.KafkaUnknown actual,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.RecoveryPublication publication,
            AuthorIdentity service,
            StableCode code,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context,
            TargetPublishOutcomeMutationFactory signer,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            TargetRecoveryProofTrust trust,
            Runnable current) {
        return prepare(actual, publication, service, code, context, signer, catalog, trust, current, null);
    }

    public static TargetArchivedKafkaResolution prepare(
            TargetPublishOutcomeStore.KafkaUnknown actual,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.RecoveryPublication publication,
            AuthorIdentity service,
            StableCode code,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context,
            TargetPublishOutcomeMutationFactory signer,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            TargetRecoveryProofTrust trust,
            Runnable current,
            SourceCompletion completion) {
        Objects.requireNonNull(current).run();
        final var mutation = Objects.requireNonNull(signer)
                .createKafkaResolutionWithRecoveryProof(actual, publication, service, code, context);
        current.run();
        final var sealed = Objects.requireNonNull(catalog).sealRecoveryPrepared(publication, mutation, trust, current);
        sealed.requireExact(mutation);
        current.run();
        return new TargetArchivedKafkaResolution(actual, mutation, sealed, context, completion);
    }

    public SystemMutation mutation() {
        return mutation;
    }

    public OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery prepared() {
        return prepared;
    }

    public WorkerPublishOutcomeMutationFactory.OutcomeContext context() {
        return context;
    }

    public void requireUnknown(TargetPublishOutcomeStore.KafkaUnknown actual) {
        original.requireSame(actual);
        prepared.requireExact(mutation);
    }

    public CompletableFuture<Optional<com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption>>
            completeSource() {
        if (completion == null) {
            throw new IllegalStateException("actual Root Source completion/adoption is not configured");
        }
        return Objects.requireNonNull(completion.complete(this), "owned Root Source completion");
    }

    public void requireAdoption(com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption actual) {
        actual.owner().requireProof(prepared.verified().proof());
        actual.owner().requireSource(mutation, actual.owner().adoptedAt().source());
    }
}
