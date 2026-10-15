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
