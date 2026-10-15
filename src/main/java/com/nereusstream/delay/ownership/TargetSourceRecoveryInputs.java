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
        TargetSourceRecoveryInputs load(OwnerIdentity owner, SourceAssignment assignment,
                long maximumRetainedBytes, Runnable guard);
    }

    OwnerIdentity owner();
    LocalBrokerMtlsResolver.SourceReceipt credential();
    int maximumFrameBytes();
    PulsarAttemptJournal journal();
    TargetOwnedBackendGroup journalBackend();
    long retainedBytes();
    /** Local retained Source/version/pin protection; no new acquisition/I/O at apply, valid across frozen preparation. */
    void requireSourceCurrent();
    void requireJournalCurrent();
    /** Release this original protection hold after Source/Store/SDK obligations exit; failure retains it for retry. */
    @Override void close();
}
