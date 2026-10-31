package com.nereusstream.delay.ownership;

/** Actual system appender and its Host-owned SDK generation, kept alive through the final Source checkpoint. */
public interface OwnedSourceSystemWriter extends ShardLogMutationAppender {
    TargetWorkerShardRuntime sourceWriterWorker();

    TargetOwnedBackendGroup ownedBackendGroup();

    boolean readyForAppending();

    boolean resourcesTerminated();

    default TargetScopeExposureSource.CopyContract payloadSummaryCopyContract() {
        throw new IllegalStateException(
                "actual Source producer lacks independently paid payload summary initialization");
    }

    default boolean payloadSummaryOperationsExited() {
        throw new IllegalStateException("actual Source producer lacks native payload summary operation ownership");
    }

    default AppendOutcome appendPayloadSummary(
            com.nereusstream.delay.protocol.SystemMutation input,
            TargetPayloadSummaryInitializationAppendPermit permit) {
        throw new IllegalStateException("actual Source producer lacks payload summary CONTROL SDK handoff");
    }
}
