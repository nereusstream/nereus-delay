package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CheckpointRegistryLookupTest {
    @Test
    void repeatedRunnableCannotCompleteWhileOriginalIoIsStillRunning() throws Exception {
        try (var executor = pool()) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            final var calls = new AtomicInteger();
            final var owned = CheckpointRegistryLookup.submitOwned(
                    guard -> {
                        if (calls.incrementAndGet() == 1) {
                            entered.countDown();
                            await(release);
                        }
                        return "value";
                    },
                    Duration.ofSeconds(5),
                    () -> {},
                    executor,
                    System::nanoTime);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                owned.run();
                assertEquals(1, calls.get());
                assertFalse(owned.ioExited());
                release.countDown();
                assertEquals("value", owned.receipt().get(5, TimeUnit.SECONDS));
                assertTrue(owned.ioExited());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void stoppedExecutorQueueCannotStartRemovedRegistration() throws Exception {
        try (var executor = pool()) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            final var called = new AtomicBoolean();
            executor.execute(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final var owned = CheckpointRegistryLookup.submitOwned(
                        guard -> {
                            called.set(true);
                            return "value";
                        },
                        Duration.ofSeconds(5),
                        () -> {},
                        executor,
                        System::nanoTime);
                final var removed = executor.shutdownNow();
                assertTrue(removed.contains(owned));
                assertFalse(owned.ioExited());
                owned.requestStop();
                assertTrue(owned.ioExited());
                assertTrue(assertThrows(
                                ExecutionException.class, () -> owned.receipt().get(5, TimeUnit.SECONDS))
                        .getCause()
                        .getMessage()
                        .contains("stopped before I/O"));
                removed.forEach(Runnable::run); // Even a retained Runnable cannot resurrect the original operation.
                assertFalse(called.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void registrationProtectionRunsOnTheFiniteExecutor() throws Exception {
        try (var executor = pool()) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            final var called = new AtomicBoolean();
            final Thread caller = Thread.currentThread();
            executor.execute(() -> {
                entered.countDown();
                await(release);
            });
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final var result = CheckpointRegistryLookup.submitProtected(
                        guard -> "value",
                        Duration.ofSeconds(5),
                        () -> {
                            assertTrue(Thread.currentThread() != caller);
                            called.set(true);
                        },
                        executor,
                        System::nanoTime);
                assertFalse(called.get());
                release.countDown();
                assertEquals("value", result.get(5, TimeUnit.SECONDS));
                assertTrue(called.get());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void slowFinalProtectionCannotAcceptAPastDeadlineResult() throws Exception {
        try (var executor = pool()) {
            final var nanos = new AtomicLong();
            final var finished = new AtomicBoolean();
            final var result = CheckpointRegistryLookup.submit(
                    () -> {
                        finished.set(true);
                        return "value";
                    },
                    Duration.ofNanos(10),
                    () -> {
                        if (finished.get()) {
                            nanos.set(10);
                        }
                    },
                    executor,
                    nanos::get);
            assertTrue(assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS))
                    .getCause()
                    .getMessage()
                    .contains("deadline"));
        }
    }

    @Test
    void synchronousCasStepsReceiveTheSameDeadlineGuard() throws Exception {
        try (var executor = pool()) {
            final var nanos = new AtomicLong();
            final var wrote = new AtomicBoolean();
            final var result = CheckpointRegistryLookup.submitProtected(
                    guard -> {
                        guard.run(); // First metadata read completed within the budget.
                        nanos.set(10);
                        guard.run(); // Must stop before starting a later CAS/write.
                        wrote.set(true);
                        return "value";
                    },
                    Duration.ofNanos(10),
                    () -> {},
                    executor,
                    nanos::get);
            assertTrue(assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS))
                    .getCause()
                    .getMessage()
                    .contains("deadline"));
            assertFalse(wrote.get());
        }
    }

    @Test
    void queuedExpiryDoesNotStartIoOrCloseCallerExecutor() throws Exception {
        try (var executor = pool()) {
            final var entered = new CountDownLatch(1);
            final var release = new CountDownLatch(1);
            executor.execute(() -> {
                entered.countDown();
                await(release);
            });
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                final var nanos = new AtomicLong();
                final var called = new AtomicBoolean();
                final var result = CheckpointRegistryLookup.submit(
                        () -> {
                            called.set(true);
                            return "late";
                        },
                        Duration.ofNanos(10),
                        () -> {},
                        executor,
                        nanos::get);
                nanos.set(10);
                release.countDown();
                assertTrue(assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS))
                        .getCause()
                        .getMessage()
                        .contains("deadline"));
                assertFalse(called.get());
                assertFalse(executor.isShutdown());
                assertEquals("next", executor.submit(() -> "next").get(5, TimeUnit.SECONDS));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void lateIoCompletionAndLostProtectionCannotSupplyAValue() throws Exception {
        try (var executor = pool()) {
            final var nanos = new AtomicLong();
            final var late = CheckpointRegistryLookup.submit(
                    () -> {
                        nanos.set(10);
                        return "late";
                    },
                    Duration.ofNanos(10),
                    () -> {},
                    executor,
                    nanos::get);
            assertTrue(assertThrows(ExecutionException.class, () -> late.get(5, TimeUnit.SECONDS))
                    .getCause()
                    .getMessage()
                    .contains("deadline"));
            final var live = new AtomicBoolean(true);
            final var lost = CheckpointRegistryLookup.submit(
                    () -> {
                        live.set(false);
                        return "unprotected";
                    },
                    Duration.ofNanos(10),
                    () -> {
                        if (!live.get()) {
                            throw new IllegalStateException("Owner lost");
                        }
                    },
                    executor,
                    nanos::get);
            assertEquals(
                    "Owner lost",
                    assertThrows(ExecutionException.class, () -> lost.get(5, TimeUnit.SECONDS))
                            .getCause()
                            .getMessage());
            assertFalse(executor.isShutdown());
        }
    }

    @Test
    void unboundedOrInlineExecutionAndInvalidDeadlinesRejectBeforeIo() {
        final var called = new AtomicBoolean();
        try (var unbounded = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
                var inline = pool()) {
            inline.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            for (var executor : new ThreadPoolExecutor[] {unbounded, inline}) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CheckpointRegistryLookup.submit(
                                () -> {
                                    called.set(true);
                                    return "value";
                                },
                                Duration.ofSeconds(1),
                                () -> {},
                                executor,
                                () -> 0));
            }
        }
        try (var executor = pool()) {
            for (var duration :
                    new Duration[] {Duration.ZERO, Duration.ofNanos(Long.MAX_VALUE), Duration.ofSeconds(Long.MAX_VALUE)
                    }) {
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CheckpointRegistryLookup.submit(
                                () -> {
                                    called.set(true);
                                    return "value";
                                },
                                duration,
                                () -> {},
                                executor,
                                () -> 0));
            }
        }
        assertFalse(called.get());
    }

    private static ThreadPoolExecutor pool() {
        return new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2), new ThreadPoolExecutor.AbortPolicy());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test gate timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
