package com.nereusstream.delay.ownership;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Retains the original SDK cleanup command after actual close failed; no replacement client is created. */
public class TargetSdkCleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
    private static final long serialVersionUID = 1L;
    private final transient Runnable close;
    private volatile boolean complete;

    public TargetSdkCleanupFailure(Throwable cause, Runnable close) {
        super("Target SDK native cleanup is not confirmed", Objects.requireNonNull(cause, "original failure"));
        this.close = Objects.requireNonNull(close, "same native cleanup");
    }

    @Override
    public boolean cleanupComplete() {
        return complete;
    }

    /** The owning I/O task schedules this command on the same finite executor. */
    @Override
    public CompletableFuture<Void> retryCleanup() {
        try {
            retryClose();
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException | Error failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    public synchronized void retryClose() {
        if (!complete) {
            close.run();
            complete = true;
        }
    }
}
