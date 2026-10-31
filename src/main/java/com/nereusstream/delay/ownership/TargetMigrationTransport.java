package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SourcePosition;

/** Implemented by the actual configured K1/P1 migration factory, using the original Process graph. */
public interface TargetMigrationTransport {
    void requireBinding(TargetMigrationSourceOpening original);
    boolean advance();
    TargetWorkerRecoverySource reader();
    TargetMigrationControlWriter writer();

    /** Capture another finite original tail under the same hold/producer after the prior reader really exits. */
    void openReaderAfter(SourcePosition actualApplied);
    void requestStop();
    boolean ioExited();
    boolean cleanupComplete();
    Throwable pendingIoFailure();
}
