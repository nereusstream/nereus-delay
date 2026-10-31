package com.nereusstream.delay.ownership;

/** One original recovery-only Source producer generation; it never acquires ordinary Worker write authority. */
public interface TargetRecoveryFenceWriter {
    /** Immutable opening/allocation binding, retained after normal producer close. */
    void requireBinding(TargetWorkerSourceStartup.RecoveryInputsOpening opening);

    TargetOwnedBackendGroup backend();

    /**
     * Append only the Root-retained exact type-4 frame. The actual native send must call
     * {@link TargetWorkerSourceStartup.RecoveryFenceAppend#beforeSdk} immediately before SDK handoff.
     * UNKNOWN retains its original producer, budget and cleanup obligations.
     */
    ShardLogMutationAppender.AppendOutcome append(
            TargetTimeFenceMutationFactory.Prepared exact, TargetWorkerSourceStartup.RecoveryFenceAppend permit);

    /** Actual opening, append calls, callbacks and any producer cleanup have exited. */
    boolean operationsExited();

    /**
     * True only after this exact producer normally closed and its opening/calls/callbacks/cleanup
     * actually exited. Shared-client shutdown or backend termination cannot satisfy this handoff.
     */
    boolean closeForHandoff(TargetWorkerSourceStartup.RecoveryFenceHandoff permit);
}
