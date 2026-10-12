package com.nereusstream.delay.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Shared finite executor/deadline checks for checkpoint Oxia value reads and intent registration. */
final class CheckpointRegistryLookup {
    private CheckpointRegistryLookup() {}

    static <T> CompletableFuture<T> submit(Supplier<T> operation, Duration timeout, Runnable protection,
            ThreadPoolExecutor executor, LongSupplier nanos) {
        Objects.requireNonNull(operation, "operation");
        return submitBounded(guard -> operation.get(), timeout, protection, executor, nanos, true);
    }

    /** Supplies the same acceptance deadline and protection to each synchronous CAS step. */
    static <T> CompletableFuture<T> submitProtected(Function<Runnable, T> operation, Duration timeout,
            Runnable protection, ThreadPoolExecutor executor, LongSupplier nanos) {
        return submitBounded(operation, timeout, protection, executor, nanos, false);
    }

    private static <T> CompletableFuture<T> submitBounded(Function<Runnable, T> operation, Duration timeout,
            Runnable protection, ThreadPoolExecutor executor, LongSupplier nanos, boolean checkBeforeAdmission) {
        Objects.requireNonNull(operation, "operation");
        final Runnable guard = newGuard(timeout, protection, executor, nanos, checkBeforeAdmission);
        return CompletableFuture.supplyAsync(() -> {
            guard.run();
            final T result = operation.apply(guard);
            guard.run();
            return result;
        }, executor);
    }

    static <T> OwnedLookup<T> submitOwned(Function<Runnable, T> operation, Duration timeout,
            Runnable protection, ThreadPoolExecutor executor, LongSupplier nanos) {
        final var owned = new OwnedLookup<>(Objects.requireNonNull(operation, "operation"),
                newGuard(timeout, protection, executor, nanos, false), executor);
        executor.execute(owned);
        return owned;
    }

    private static Runnable newGuard(Duration timeout, Runnable protection, ThreadPoolExecutor executor,
            LongSupplier nanos, boolean checkBeforeAdmission) {
        Objects.requireNonNull(protection, "protection");
        Objects.requireNonNull(nanos, "nanos");
        final long duration;
        try { duration = Objects.requireNonNull(timeout, "timeout").toNanos(); }
        catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Registry timeout overflow", overflow);
        }
        if (duration <= 0 || duration == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Registry timeout requires a finite positive duration");
        }
        final var pool = Objects.requireNonNull(executor, "executor");
        if (pool.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) pool.getQueue().size() + pool.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || pool.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("Registry executor requires finite threads/queue and AbortPolicy");
        }
        final long started = nanos.getAsLong();
        if (checkBeforeAdmission) { protection.run(); }
        return () -> {
            requireDeadline(nanos, started, duration);
            protection.run();
            requireDeadline(nanos, started, duration);
        };
    }

    /** Owns the exact queued Runnable and serializes no-start proof against the actual executor start. */
    static final class OwnedLookup<T> implements Runnable {
        private final Function<Runnable, T> operation;
        private final Runnable guard;
        private final ThreadPoolExecutor executor;
        private final CompletableFuture<T> receipt = new CompletableFuture<>();
        private boolean started;
        private boolean stopping;
        private boolean exited;

        private OwnedLookup(Function<Runnable, T> operation, Runnable guard, ThreadPoolExecutor executor) {
            this.operation = operation;
            this.guard = guard;
            this.executor = executor;
        }

        CompletableFuture<T> receipt() { return receipt.copy(); }

        synchronized boolean ioExited() { return exited; }

        void requestStop() {
            synchronized (this) {
                if (exited) { return; }
                stopping = true;
                if (started) { return; }
                exited = true;
            }
            executor.remove(this);
            receipt.completeExceptionally(new IllegalStateException("registration stopped before metadata I/O"));
        }

        private void requireCurrent() {
            requireNotStopped();
            guard.run();
            requireNotStopped();
        }

        private synchronized void requireNotStopped() {
            if (stopping) { throw new IllegalStateException("registration I/O stop requested"); }
        }

        @Override
        public void run() {
            synchronized (this) {
                if (started || exited) { return; }
                started = true;
            }
            T value = null;
            Throwable failure = null;
            try {
                requireCurrent();
                value = operation.apply(this::requireCurrent);
                requireCurrent();
            } catch (Throwable rejected) {
                failure = rejected;
            }
            synchronized (this) {
                if (stopping && failure == null) {
                    failure = new IllegalStateException("registration I/O stop requested");
                }
                exited = true;
            }
            if (failure == null) { receipt.complete(value); }
            else { receipt.completeExceptionally(failure); }
        }
    }

    private static void requireDeadline(LongSupplier nanos, long started, long duration) {
        final long elapsed = nanos.getAsLong() - started;
        if (elapsed < 0 || elapsed >= duration) {
            throw new IllegalStateException("checkpoint Registry lookup deadline expired or clock moved backwards");
        }
    }
}
