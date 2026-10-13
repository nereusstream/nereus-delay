package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Retains the exact SDK handle and its resource charge after a Target client close failed. */
public final class KafkaClientArtifactTargetCleanupFailure extends IllegalStateException
        implements CheckpointInputFuture.Cleanup {
    private static final long serialVersionUID = 1L;
    private final transient Runnable close;
    private volatile boolean complete;
    public KafkaClientArtifactTargetCleanupFailure(Throwable cause, Runnable close) {
        super("Target Kafka native cleanup is not confirmed", Objects.requireNonNull(cause, "original failure"));
        this.close = Objects.requireNonNull(close, "same native handle cleanup");
    }
    @Override public boolean cleanupComplete() { return complete; }
    /** The owning input/task schedules this command on its original bounded I/O executor. */
    @Override public CompletableFuture<Void> retryCleanup() {
        retryClose(); return CompletableFuture.completedFuture(null);
    }
    public synchronized void retryClose() {
        if (!complete) { close.run(); complete = true; }
    }
}
