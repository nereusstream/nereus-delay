package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import com.nereusstream.delay.store.TargetMigrationArchiveReceived;
import java.util.Objects;

/** Same actual Source hold, with the repository's privately prepared COPY/T proof attached to its exact frame. */
final class TargetMigrationArchiveSourceInputs implements TargetSourceAuthorityLoader.HeldInputs {
    private final TargetSourceAuthorityLoader.HeldInputs original;
    private final TargetSourceAuthorityLoader.Migration material;
    TargetMigrationArchiveSourceInputs(TargetSourceAuthorityLoader.HeldInputs original,
            TargetSourceAuthorityLoader.Migration base, TargetMigrationArchivePreparation.PreparedBatch copy,
            TargetMigrationArchivePreparation.PreparedTakeover takeover, TargetMigrationArchiveReceived received) {
        this.original = Objects.requireNonNull(original);
        if ((copy == null) == (takeover == null)) throw new IllegalArgumentException("archive Source requires its one actual COPY or T preparation");
        final var request = copy == null ? takeover.request() : copy.request();
        if (!base.prepared().request().equals(request.operationRequest()))
            throw new IllegalArgumentException("archive Source changed its exact original Prepared candidate");
        material = new TargetSourceAuthorityLoader.Migration(base.prepared(), base.authority(), base.settlement(),
                base.importLimits(), base.commit(), base.retirement(), new TargetSourceApplyRuntime.MigrationArchiveInputs(
                        copy, takeover, received, Objects.requireNonNull(base.archive()).original()));
    }
    @Override public SourceReplayEntry entry() { return original.entry(); }
    @Override public TargetSourceAuthorityLoader.Inputs inputs() { return material; }
    // The Source acquisition charges its own original decoded material. The proof shares the caller's
    // separately reserved maps/packet/receiver loan; it does not acquire or release a second physical share.
    @Override public long retainedBytes() { return original.retainedBytes(); }
    @Override public TargetScopeFeeAuthority scopeFees() { return original.scopeFees(); }
    @Override public TargetRetirementPreparation.Protection retirementPreparationProtection(TargetSourceApplyRuntime.ReclaimRead source) {
        return original.retirementPreparationProtection(source);
    }
    @Override public TargetSloWriterOrigin.Protection detachWriterOrigin(TargetSloWriterOrigin.PreparationRequired request) {
        return original.detachWriterOrigin(request);
    }
    @Override public TargetEvidenceRetirementStore.ConfirmationApproval confirmationAfterPreparation(
            TargetSourceAuthorityLoader.PendingConfirmation input, TargetSourceApplyRuntime.ConfirmationRead source,
            TargetRetirementPreparation.Proof proof) { return original.confirmationAfterPreparation(input, source, proof); }
    @Override public TargetSourceAuthorityLoader.ArchiveTailInputs archiveTailInputs(TargetMigrationArchivePreparation.TailPreparationRequired request) {
        return original.archiveTailInputs(request);
    }
    @Override public void requireCurrent() { original.requireCurrent(); }
    @Override public void requireInlinePayloadIndependent(TargetSourceApplyRuntime.ReclaimRead source) { original.requireInlinePayloadIndependent(source); }
    @Override public void requireIncarnationIndependent(TargetSourceApplyRuntime.ReclaimRead source) { original.requireIncarnationIndependent(source); }
    @Override public void requireNativeRetainedIndependent(TargetSourceApplyRuntime.ReclaimRead source) { original.requireNativeRetainedIndependent(source); }
    @Override public void requireUncommittedObjectIndependent(TargetSourceApplyRuntime.ReclaimRead source) { original.requireUncommittedObjectIndependent(source); }
    @Override public void requireUncommittedObjectHistoricalConfirmation(TargetSourceApplyRuntime.ReclaimRead source) { original.requireUncommittedObjectHistoricalConfirmation(source); }
    @Override public void requireObjectPayloadIndependent(TargetSourceApplyRuntime.ReclaimRead source) { original.requireObjectPayloadIndependent(source); }
    @Override public void requireObjectPayloadHistoricalConfirmation(TargetSourceApplyRuntime.ReclaimRead source) { original.requireObjectPayloadHistoricalConfirmation(source); }
    @Override public void close() { original.close(); }
}
