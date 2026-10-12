package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CheckpointInputFutureTest {
    @Test
    void dependentStopPreventsQueuedReadFromStarting() throws Exception {
        try (var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2),
                new ThreadPoolExecutor.AbortPolicy())) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            final var called = new AtomicBoolean();
            executor.execute(() -> {
                entered.countDown();
                try { release.await(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final var owned = OwnedCheckpointIo.submit(guard -> { called.set(true); return "value"; },
                        () -> {}, executor);
                final var dependent = owned.receipt().thenApply(String::length);
                CheckpointInputFuture.requestStop(dependent);
                assertTrue(CheckpointInputFuture.ioExited(dependent));
                assertTrue(assertThrows(ExecutionException.class,
                        () -> dependent.get(5, TimeUnit.SECONDS)).getCause().getMessage().contains("stopped before"));
                assertEquals(0, executor.getQueue().size());
                owned.run();
                assertFalse(called.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void startedReadStopStillRetainsFailedNativeCleanup() throws Exception {
        try (var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2),
                new ThreadPoolExecutor.AbortPolicy())) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            final var cleanup = new RetainedCleanup();
            final var owned = OwnedCheckpointIo.submit(guard -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) { throw new IllegalStateException("read gate expired"); }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                throw cleanup;
            }, () -> {}, executor);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                owned.requestStop();
                assertFalse(owned.ioExited());
                release.countDown();
                assertSame(cleanup, assertThrows(ExecutionException.class,
                        () -> owned.receipt().get(5, TimeUnit.SECONDS)).getCause());
                assertFalse(owned.ioExited());
                cleanup.closed.set(true);
                assertTrue(owned.ioExited());
            } finally {
                release.countDown();
            }
        }
    }

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
