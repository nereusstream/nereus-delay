package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CheckpointInputFutureTest {
    @Test
    void cancelledDependentViewCannotReleaseOriginalRead() {
        final var owned = new CompletableFuture<String>();
        final var tracked = CheckpointInputFuture.track(owned);
        final var dependent = tracked.thenApply(String::length);
        assertTrue(dependent.cancel(false));
        assertFalse(CheckpointInputFuture.ioExited(dependent));
        assertFalse(owned.isDone());
        owned.complete("value");
        assertTrue(CheckpointInputFuture.ioExited(dependent));
    }

    @Test
    void replacedResultCannotClaimIoExit() {
        final var owned = new CompletableFuture<String>();
        final var tracked = CheckpointInputFuture.track(owned);
        assertTrue(tracked.complete("early"));
        assertFalse(tracked.ioExited());
        owned.complete("original");
        assertTrue(tracked.ioExited());
    }

    @Test
    void exceptionalResultRetainsTheExactNativeCleanupHandle() {
        final var owned = new CompletableFuture<String>();
        final var tracked = CheckpointInputFuture.track(owned);
        final var failure = new RetainedCleanup();
        owned.completeExceptionally(failure);
        assertFalse(tracked.ioExited());
        assertSame(failure, tracked.ownedFailure());
        final var copied = tracked.copy();
        assertFalse(CheckpointInputFuture.ioExited(copied));
        assertSame(failure, CheckpointInputFuture.ownedFailure(copied));
        failure.closed.set(true);
        assertTrue(CheckpointInputFuture.ioExited(copied));
    }

    private static final class RetainedCleanup extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public boolean cleanupComplete() { return closed.get(); }
    }
}
