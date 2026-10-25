package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;

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
    /** May be created by the original strict prepare callback, after the actual Source reader is bound. */
    default TargetRecoveryFenceWriter fenceWriter() { return null; }
    long retainedBytes();
    /**
     * Stable declaration that this original hold requests E4 recovery responsibilities, including an
     * independently authenticated no-old-domain result. This selects the strict recovery branch only;
     * it grants no qualification. Retained E4 facts also require that branch even when this returns false.
     * Ordinary recovery with complete empty pre/post E4 censuses does not call the strict hooks below.
     */
    default boolean requiresSloRecoveryQualification() { return false; }
    /**
     * Prepares one exact recovery Fence using this already-held input, on the original graph I/O
     * executor and outside Store locks. Before preparing it, retain the actual old local writers,
     * their SDK/I/O exit qualification, and independently complete old Scope/REGISTER admission and
     * legacy legal-range material under the original protections and retained-byte bound.
     * {@code original} proves Native facts only through its cut; its census or largest observed
     * deadline cannot establish the complete old range or select a recovery deadline.
     *
     * <p>The original independent FENCE role, qualified UTC and signer must authorize this frame.
     * Its retry-until is independently finite and strictly later than the qualified old deadline.
     * Before any native append, use its original {@code RecoveryInputsOpening.prepareFenceAppend}
     * or {@code appendFence} so the Root retains the exact Prepared bytes and issues its narrow permit.
     * Register the actual Fence writer/backend before opening native resources. The input must
     * retain every partial/native UNKNOWN and cleanup handle with the backend registered before
     * allocation, including on failure. Do not acquire a replacement hold or wait for the next
     * Source apply. Returning Prepared is neither append persistence nor an APPLIED Fence proof.
     * A returned frame without this opening's append permit must already have its actual APPLIED
     * first/POS at the original audited cut; it cannot substitute an unowned external append.</p>
     * <p>A null result requires this original input to independently authenticate and continuously
     * protect the absence of an old legal SLO domain; an empty Native census cannot establish that
     * absence, and any positive post-replay SLO audit must reject that null preparation.</p>
     */
    default TargetTimeFenceMutationFactory.Prepared prepareSloRecoveryFence(
            TargetStoreBootstrap.SloRecoveryAudit original, Runnable guard) {
        throw new IllegalStateException("complete original SLO recovery and independent FENCE qualification is not configured");
    }
    /**
     * Independently requalifies this same hold's complete original ranges/material and the actual
     * replayed cut, only after the post-prefix Source has verified this exact Fence's APPLIED first
     * and POSITION. Runs on the original graph I/O executor outside Store locks; the supplied guard
     * and original retained protections remain required throughout. No empty result, census,
     * maximum observed deadline or caller-provided DTO establishes completeness.
     *
     * <p>This does not refund or release original physical UNKNOWN, attempt or Journal obligations,
     * replace the held input, or wait for another Source apply. It independently authenticates old-domain
     * closure and the protected census only through the already replayed cut: it must not await the
     * successor Source apply of a CLOSE, gap or timeout event that it newly writes, and an open Native62
     * record does not invalidate an already APPLIED cumulative IngressFence covering that domain's D.</p>
     */
    default void requireSloRecoveryComplete(TargetStoreBootstrap.SloRecoveryAudit original,
            TargetStoreBootstrap.SloRecoveryAudit replayed, TargetTimeFenceMutationFactory.Prepared exactFence,
            Runnable guard) {
        throw new IllegalStateException("independent complete original SLO recovery qualification is not configured");
    }
    /**
     * Local retained Source/version/pin protection; no new acquisition/I/O at apply, valid across frozen
     * preparation. After strict SLO qualification this also checks the same hold's retained original
     * Scope/range/writer protections. A qualification cannot outlive those protections or this hold.
     */
    void requireSourceCurrent();
    /** Retained protection remains current after writer close; this method does not grant write authority. */
    void requireJournalCurrent();
    /** Release this original protection hold after Source/Store/SDK obligations exit; failure retains it for retry. */
    @Override void close();
}
