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
        /**
         * Register the hold and every native owner on opening before allocating; partial failures retain Cleanup.
         * A compatibilityRecovery opening reacquires the exact original C materials/frozen contracts and durable
         * Source/Journal/Pin/retention responsibility named by recoveredRoot; it grants no new ACTIVATE permission.
         */
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
    /** Original actual Native read authority, retained by this same Process acquisition. */
    default TargetStoreBackend.ReadAuthority reads() {
        throw new IllegalStateException("migration custody lacks its original Native read authority");
    }
    /** Reacquires the actual already-paid receiver; the private cursor performs exact GET/full receipt audit. */
    default TargetMigrationArchiveHeld.Cursor archiveRecovery(TargetMigrationProcessDriver.Opening original,
            com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover takeover) {
        throw new TargetSourceAuthorityPreparation.InputUnavailable("actual archive receiver/retention acquisition is unavailable");
    }
    /** Same actual paid cumulative reopen/read window, not a fresh per-page allowance. */
    default BoundedReadBudget archiveReads() {
        throw new TargetSourceAuthorityPreparation.InputUnavailable("archive receiver lacks its original cumulative read allocation");
    }
    /** Shared Source/credential/control/mapping protection after T, independent of the exited legacy DB sub-loan. */
    default void requireArchiveCurrent(TargetMigrationPlan plan, StoreMetadata actual,
            com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover takeover) {
        throw new TargetSourceAuthorityPreparation.InputUnavailable("new archive interpretation lacks its actual persistent owner protection");
    }
    /** Local revocation/expiry facts of that same acquired protection; no Store/Registry/SDK I/O or locks. */
    default void requireArchiveLocalCurrent(TargetMigrationPlan plan, StoreMetadata actual,
            com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover takeover) {
        throw new TargetSourceAuthorityPreparation.InputUnavailable("new archive interpretation lacks its original local protection token");
    }
    interface LegacyRuntimeLoan extends CheckpointInputFuture.Cleanup, AutoCloseable {
        void requireTransfer(com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover takeover,
                TargetMigrationArchiveHeld.RuntimeHold actualNewOwner);
        void requestStop();
        boolean ioExited();
        @Override void close();
    }
    /** Actual legacy-only registration/reader/Journal/DB custody, separate from the shared Inputs acquisition. */
    default LegacyRuntimeLoan legacyRuntimeLoan() {
        throw new TargetSourceAuthorityPreparation.InputUnavailable("original owner cannot independently exit its legacy-only runtime sub-loan");
    }
    /** Complete original frozen writer/control domain and durable anti-reopening material, retained by the actual new owner. */
    default com.nereusstream.delay.runtime.TargetMigrationArchiveClosedContract.Materials archiveClosedMaterials(
            com.nereusstream.delay.runtime.TargetMigrationRetirementStore.AppliedIntent intention) {
        throw new TargetSourceAuthorityPreparation.InputUnavailable("archive lacks its actual complete closed-contract materials");
    }
    /** Optional exact protected Source content. Archive baseline bytes never require a fabricated pre-C frame. */
    default SourceReplayEntry archiveSource(SourcePosition at, BoundedReadBudget cumulative) {
        for (var record : externalRecords()) if (record.kind().equals("SOURCE")
                && java.util.Arrays.equals(record.source().canonicalBytes(), at.canonicalBytes())) {
            final byte[] raw = record.originalBytes();
            if (!cumulative.tryCharge(at.canonicalBytes().length, raw.length))
                throw new IllegalStateException("original archive Source evidence exhausted its shared cumulative read budget");
            final var frame = ShardLogFrame.decode(raw);
            return frame.recordKind() == ProtocolTuple.CLIENT_COMMAND
                    ? new SourceReplayRecord(CommandCodec.decodeFrame(raw), at, null, null)
                    : new SourceReplayMutation(SystemMutation.decodeTargetFrame(raw), at, null, null);
        }
        throw new TargetSourceAuthorityPreparation.InputUnavailable("exact protected Source content is unavailable; retain original custody");
    }
    TargetMigrationStore.Authority activationAuthority();
    /** Complete original independent management-issuer statement for a newly prepared A2, never the Source/A signer. */
    default TargetMigrationRootIssuerAuthorization rootIssuerAuthorization(TargetMigrationPlan plan) {
        throw new IllegalStateException("new migration activation lacks the actual independent original Root issuer authorization");
    }
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
