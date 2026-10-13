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
    TargetOwnedBackendGroup backend();
    boolean cleanupComplete();
    @Override void close();
}
