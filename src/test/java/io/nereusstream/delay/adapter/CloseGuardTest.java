package io.nereusstream.delay.adapter;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloseGuardTest {
    @Test
    void failedCloseRemainsFencedButCanBeRetried() {
        final CloseGuard guard = new CloseGuard();
        final AtomicInteger attempts = new AtomicInteger();

        assertThrows(IllegalStateException.class, () -> guard.close(() -> {
            if (attempts.getAndIncrement() == 0) {
                throw new IllegalStateException("close failed");
            }
        }));
        assertTrue(guard.isClosed());

        guard.close(attempts::incrementAndGet);
        guard.close(attempts::incrementAndGet);
        assertEquals(2, attempts.get());
    }

    @Test
    void acceptedInvocationDoesNotLetCloseAdmitASecondTransportCall() throws Exception {
        final CloseGuard guard = new CloseGuard();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean closeCompleted = new AtomicBoolean();
        final CompletableFuture<Integer> invocation = CompletableFuture.supplyAsync(() ->
                guard.invokeIfOpen(() -> {
                    entered.countDown();
                    try {
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(exception);
                    }
                    return 7;
                }, () -> 0));

        assertTrue(entered.await(5, TimeUnit.SECONDS));
        final CompletableFuture<Void> close = CompletableFuture.runAsync(() -> {
            guard.close(() -> closeCompleted.set(true));
        });
        close.get(5, TimeUnit.SECONDS);
        assertTrue(closeCompleted.get());
        assertEquals(0, guard.invokeIfOpen(() -> 9, () -> 0));

        release.countDown();
        assertEquals(7, invocation.get(5, TimeUnit.SECONDS));
    }
}
