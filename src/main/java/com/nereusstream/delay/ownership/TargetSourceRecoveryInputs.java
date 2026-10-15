package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.OwnerIdentity;

/** One deployment-owned immutable recovery hold for one exact startup Owner, not an apply/runtime authority. */
public interface TargetSourceRecoveryInputs extends AutoCloseable {
    @FunctionalInterface
    interface Provider {
        /**
         * Use the supplied bound before allocation; retain partial failures with CheckpointInputFuture.Cleanup.
         * Register the actual Journal SDK owner on the original startup before opening native resources.
         * The composition root owns this provider; the startup owns each returned hold through Host exit.
         */
        TargetSourceRecoveryInputs load(TargetWorkerSourceStartup.RecoveryInputsOpening opening,
                OwnerIdentity owner, SourceAssignment assignment,
                long maximumRetainedBytes, Runnable guard);
    }

    /** Actual SDK writer retained before opening; independent of this hold and the shared client lifetime. */
    interface JournalWriter {
        void requireBinding(TargetWorkerSourceStartup.RecoveryInputsOpening opening);
        PulsarAttemptJournal journal();
        TargetOwnedBackendGroup backend();
        com.nereusstream.delay.adapter.PulsarJournalResource resource();
        String producerName();
        /** Observe one original native close; backend termination cannot satisfy normal handoff. */
        boolean closeForHandoff(TargetWorkerSourceStartup.JournalHandoff permit);
    }

    OwnerIdentity owner();
    LocalBrokerMtlsResolver.SourceReceipt credential();
    int maximumFrameBytes();
    PulsarAttemptJournal journal();
    TargetOwnedBackendGroup journalBackend();
    default JournalWriter journalWriter() { return null; }
    long retainedBytes();
    /** Local retained Source/version/pin protection; no new acquisition/I/O at apply, valid across frozen preparation. */
    void requireSourceCurrent();
    /** Retained protection remains current after writer close; this method does not grant write authority. */
    void requireJournalCurrent();
    /** Release this original protection hold after Source/Store/SDK obligations exit; failure retains it for retry. */
    @Override void close();
}
