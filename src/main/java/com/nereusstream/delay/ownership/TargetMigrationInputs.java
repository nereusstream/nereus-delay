package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetMigrationImporter;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.store.*;
import java.security.PublicKey;
import java.util.List;
import java.util.Objects;

/** Original maintenance acquisition. It supplies protected materials, never a completed migration result. */
public interface TargetMigrationInputs extends LegacyMigrationBundle.Held, TargetMigrationImporter.Held,
        CheckpointInputFuture.Cleanup {
    enum SourceDomain { TARGET, LEGACY, UNKNOWN }
    @FunctionalInterface
    interface Provider {
        /** Register the hold and every native owner on opening before allocating; partial failures retain Cleanup. */
        TargetMigrationInputs open(TargetMigrationProcessDriver.Opening opening);
    }

    record PlanAuthority(long generation, byte[] predecessor, byte[] oldLineage, byte[] newLineage,
            TargetMigrationObjectReference oldManifest, TargetMigrationObjectReference closedWriterDomain,
            byte[] managementAuthorityDigest, TargetMigrationObjectReference readerEvidence) {
        public PlanAuthority {
            if (generation == 0 || (generation == 1) != (predecessor == null))
                throw new IllegalArgumentException("migration requires its exact preceding generation");
            if (predecessor != null) Bytes.requireLength(predecessor, 32, "preceding migration root");
            Bytes.requireLength(oldLineage, 16, "old lineage"); Bytes.requireLength(newLineage, 16, "new lineage");
            Bytes.requireLength(managementAuthorityDigest, 32, "independent management authority");
            predecessor = predecessor == null ? null : Bytes.copy(predecessor);
            oldLineage = Bytes.copy(oldLineage); newLineage = Bytes.copy(newLineage);
            managementAuthorityDigest = Bytes.copy(managementAuthorityDigest);
            Objects.requireNonNull(oldManifest); Objects.requireNonNull(closedWriterDomain); Objects.requireNonNull(readerEvidence);
        }
        @Override public byte[] predecessor() { return predecessor == null ? null : Bytes.copy(predecessor); }
        @Override public byte[] oldLineage() { return Bytes.copy(oldLineage); }
        @Override public byte[] newLineage() { return Bytes.copy(newLineage); }
        @Override public byte[] managementAuthorityDigest() { return Bytes.copy(managementAuthorityDigest); }
    }

    record ControlPreparation(TargetMigrationControlMutationFactory factory, byte[] operationId,
            ControlAuthorizationContext authenticated, ControlOperationAuthorization.TargetScopeProof exactScope,
            long registrationRetryUntil, long sourceRetryUntil) {
        public ControlPreparation {
            Objects.requireNonNull(factory); Objects.requireNonNull(authenticated); Objects.requireNonNull(exactScope);
            Bytes.requireLength(operationId, 32, "migration Control operation ID"); operationId = Bytes.copy(operationId);
            if (registrationRetryUntil < 0 || registrationRetryUntil == Long.MAX_VALUE
                    || sourceRetryUntil < 0 || sourceRetryUntil == Long.MAX_VALUE)
                throw new IllegalArgumentException("migration Control requires independent finite retry limits");
        }
        @Override public byte[] operationId() { return Bytes.copy(operationId); }
    }

    CheckpointManifest manifest();
    CheckpointManifestLimits physicalLimits();
    LegacyCheckpointStateInventory.ReadLimits inventoryLimits();
    LegacyMigrationBundle.Limits bundleLimits();
    LegacyMigrationBundle.ImmutableObjects immutableObjects();
    PlanAuthority planAuthority();
    TargetQuotaAccounting accounting();
    TargetMigrationImporter.Limits importLimits();
    LegacyCheckpointTailReplayer.LegacyApplierFactory legacyApplierFactory();
    PublicKey legacySystemVerificationKey();
    ControlPreparation controlPreparation();
    ControlTargetRegistrationAuthority registration();
    /** Independently acquired physical Source management/retention handle; ordinary SERVICE rights are insufficient. */
    TargetScopeDeploymentAllocation.SourceHandle originalManagementSource();
    TargetScopeDeploymentAllocation.RegistryHandle originalManagementRegistry();
    TargetStoreBackend.CommitAuthority commits();
    TargetMigrationStore.Authority activationAuthority();
    TargetMigrationImporter.CandidateSealer candidateSealer();

    /** Original reserved encoded-origin construction window; this is within retainedBytes, not an extra pool. */
    long maximumRetainedOriginBytes();
    /** Pure local classification from the independently held original writer/producer history; never today's Profile. */
    SourceDomain classifySource(SourceReplayEntry original, TargetMigrationPlan plan);
    /** Same original custody/Source/Pin/reader/mapping and paid-copy protection, after the maintenance Owner exited. */
    void requireCompatibilityCurrent(TargetMigrationPlan plan, StoreMetadata actualTarget);
    /** Durable original Source/Pin protection excludes truncation/rollback of C→through across Process exit. */
    void requireReplayRetention(TargetMigrationPlan plan, SourcePosition through);
    /** Exact original old command authorization/control/strict scope, not authority inferred from Native ownership. */
    void requireNativeOwnedContract(TargetMigrationPlan plan, TargetStoreBackend.Reader actual,
            PreparedCommand original, SourcePosition at);

    /** Exact post-A publication and same migrationId/A/old→new CAS, backed by the original Catalog/assignment. */
    TargetMigrationAdoption adoption();

    /** Includes SDK/protection operations retained after failed acquisition or response loss. */
    boolean ioExited();
    void requestStop();
    Throwable pendingIoFailure();
    @Override void close();
}
