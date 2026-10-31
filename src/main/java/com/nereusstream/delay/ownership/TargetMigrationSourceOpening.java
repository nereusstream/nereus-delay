package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

/** Borrowed SDK view of a privately minted original Process acquisition. It is not an authority factory. */
public sealed interface TargetMigrationSourceOpening
        permits TargetMigrationProcessDriver.Opening, TargetMigrationMaintenance.Opening {
    TargetWorkerProcessRuntime.Assembly graph();

    TargetMigrationConfiguration configuration();

    void requireCurrent();
    /** Pure local ownership/token checks only; called immediately around the native SDK handoff. */
    void requireLocalCurrent();

    SourceAssignment assignment();

    default SourceAssignment acceptedAssignment() {
        return assignment();
    }

    OwnerLease ownerLease();

    OwnerIdentity ownerIdentity();

    SourcePosition baseline();
    /** Local borrow from this exact acquisition's complete Source freeze and original paid codec/SDK owner. */
    TargetWorkerSourceStartup.FrameDecodeLoan acquireFrameDecode(
            SourcePosition actual, long bytes, Long generation, byte[] guardAttestation);

    TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames();

    TargetDestinationSdkBudget sdkBudget();

    SystemWriterAppendBudget systemWriterBudget();

    default SystemWriterAppendBudget systemWriters() {
        return systemWriterBudget();
    }

    void retainBackend(TargetOwnedBackendGroup actual);

    void retainRecoverySource(TargetWorkerRecoverySource actual);

    void retainControlWriter(TargetMigrationControlWriter actual);

    void retainCleanup(CheckpointInputFuture.Cleanup actual);

    void bindSourceIdentity(
            LocalBrokerMtlsResolver.SourceReceipt receipt, OwnerIdentity actual, TrustedUtcIntervalEvidence now);
}
