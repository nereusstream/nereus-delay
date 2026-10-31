package com.nereusstream.delay.ownership;

import java.util.concurrent.CompletableFuture;

/** One exact read operation; its result and native cursor cleanup are separate lifetime obligations. */
public interface TargetSourceBacklogRead {
    TargetSourceBacklog.Snapshot read();

    CompletableFuture<Void> requestClose();

    CompletableFuture<Void> retryClose();

    boolean cleanupComplete();

    Throwable cleanupFailure();

    /** Exception retains the operation so a caller can settle the exact cursor instead of losing its handle. */
    final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetSourceBacklogRead operation;

        public Failure(final TargetSourceBacklogRead operation, final Throwable failure) {
            super("Source backlog operation failed; native cleanup must be settled separately", failure);
            this.operation = java.util.Objects.requireNonNull(operation, "operation");
        }

        public TargetSourceBacklogRead operation() {
            return operation;
        }
    }
}
