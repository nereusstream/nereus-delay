package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OwnedSourceCursorTest {
    @Test
    void lateOpenAndFailedCloseRetainExactHandleUntilItsSuccessfulCleanup() {
        final var opening = new CompletableFuture<Object>();
        final var firstClose = new CompletableFuture<Void>();
        final var retryClose = new CompletableFuture<Void>();
        final var attempts = new AtomicInteger();
        final Object nativeCursor = new Object();
        final var owned = new OwnedSourceCursor<>(opening, cursor -> {
            assertSame(nativeCursor, cursor);
            return attempts.incrementAndGet() == 1 ? firstClose : retryClose;
        });
        assertThrows(TimeoutException.class, () -> owned.read(1, cursor -> cursor));
        final var detached = owned.requestClose();
        detached.cancel(false);
        assertFalse(owned.cleanupComplete());
        opening.complete(nativeCursor);
        assertEquals(1, attempts.get());
        assertFalse(owned.cleanupComplete());
        firstClose.completeExceptionally(new IllegalStateException("native close failed"));
        assertFalse(owned.cleanupComplete());
        assertEquals("native close failed", owned.cleanupFailure().getMessage());
        owned.retryClose();
        assertEquals(2, attempts.get());
        retryClose.complete(null);
        assertTrue(owned.cleanupComplete());
    }

    @Test
    void closeDoesNotRaceActiveSdkReadOrTreatCallerFutureAsAuthority() throws Exception {
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var closing = new CompletableFuture<Void>();
        final var closeCalls = new AtomicInteger();
        final var owned = new OwnedSourceCursor<>(CompletableFuture.completedFuture(new Object()), cursor -> {
            closeCalls.incrementAndGet();
            return closing;
        });
        try (var executor = Executors.newSingleThreadExecutor()) {
            final var read = executor.submit(() -> owned.read(1000, cursor -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("read not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return 1;
            }));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            owned.requestClose().complete(null);
            assertEquals(0, closeCalls.get());
            assertFalse(owned.cleanupComplete());
            release.countDown();
            assertEquals(1, read.get(5, TimeUnit.SECONDS));
            assertEquals(1, closeCalls.get());
            closing.complete(null);
            assertTrue(owned.cleanupComplete());
        } finally {
            release.countDown();
        }
    }

    @Test
    void failedOpenIsUnprovenUntilBackendShutdownButNeverStartedOpenCanBeAbandoned() {
        final var opening = new CompletableFuture<Object>();
        final var owned = new OwnedSourceCursor<>(opening, cursor -> {
            throw new AssertionError("failed opening has no returned native cursor");
        });
        opening.completeExceptionally(new IllegalStateException("SDK subscribe failed before internal close"));
        assertThrows(IllegalStateException.class, owned::backendShutdownCompleted);
        owned.requestClose();
        assertFalse(owned.cleanupComplete());
        assertEquals(
                "SDK subscribe failed before internal close",
                owned.cleanupFailure().getMessage());
        owned.backendShutdownCompleted(); // Test-only exact backend-owner shutdown evidence.
        assertTrue(owned.cleanupComplete());
        final var skipped = new OwnedSourceCursor<>(new CompletableFuture<>(), cursor -> {
            throw new AssertionError("SDK was not invoked");
        });
        skipped.abandonBeforeOpen();
        assertTrue(skipped.cleanupComplete());
        assertThrows(IllegalStateException.class, () -> skipped.read(1, cursor -> cursor));
    }
}
