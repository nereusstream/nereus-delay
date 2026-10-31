package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;

/** Original verified no-ACK SDK reader, its delivery proof and actual native cleanup responsibility. */
public interface TargetWorkerRecoverySource extends AutoCloseable {
    void requireStartup(SourceAssignment assignment, long maximumFrameBytes,
            WorkClassExecutionRegistry classes, SharedRocksDbResources resources);
    SourceReplayCursor<SourceReplayEntry> replayCursor();
    SourceReplaySuccessor successorAfter(SourcePosition position);
    /**
     * One finite native tail observation after the original signed barrier, before probing its old EOF.
     * Runs on the caller's owned I/O pool. A captured prefix is not an old-writer/UNKNOWN settlement proof.
     */
    default void captureReplayTail(){
        throw new IllegalStateException("reader cannot capture its original guarded finite replay tail");
    }
    /** Local original-reader checks only; every delivered entry must already be reflected in actualApplied. */
    default boolean replayTailReached(SourcePosition actualApplied){
        throw new IllegalStateException("reader has no original captured-prefix consumption proof");
    }
    /** Actual decoded/delivered finite prefix only. This grants no Store progress, ACK or recovery completion. */
    default java.util.Optional<SourcePosition> capturedTailReadThrough() {
        throw new IllegalStateException("reader has no original captured-prefix delivery proof");
    }
    /** Private Target-startup path: the actual SDK reader must own the independent post-seek proof. */
    default void requireReaderBinding(SourceAssignment assignment, SourcePosition positionedCut) {
        throw new IllegalStateException("reader has no independent Target startup binding");
    }
    default void requireReaderEntry(SourceReplayEntry entry, SourceAssignment assignment, SourcePosition positionedCut) {
        throw new IllegalStateException("reader has no independent Target entry proof");
    }
    TargetOwnedBackendGroup backend();
    boolean cleanupComplete();
    @Override void close();
}
