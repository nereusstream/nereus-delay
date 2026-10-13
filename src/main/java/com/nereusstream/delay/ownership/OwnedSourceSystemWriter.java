package com.nereusstream.delay.ownership;

/** Actual system appender and its Host-owned SDK generation, kept alive through the final Source checkpoint. */
public interface OwnedSourceSystemWriter extends ShardLogMutationAppender {
    TargetWorkerShardRuntime sourceWriterWorker();
    TargetOwnedBackendGroup ownedBackendGroup();
    boolean readyForAppending();
    boolean resourcesTerminated();
}
