package com.nereusstream.delay.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Shared admission/deadline checks for the two existing synchronous Oxia checkpoint value catalogs. */
final class CheckpointRegistryLookup {
    private CheckpointRegistryLookup() {}

    static <T> CompletableFuture<T> submit(Supplier<T> operation, Duration timeout, Runnable protection,
            ThreadPoolExecutor executor, LongSupplier nanos) {
        Objects.requireNonNull(operation, "operation");
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
        protection.run();
        return CompletableFuture.supplyAsync(() -> {
            requireDeadline(nanos, started, duration);
            protection.run();
            final T result = operation.get();
            protection.run();
            requireDeadline(nanos, started, duration);
            return result;
        }, pool);
    }

    private static void requireDeadline(LongSupplier nanos, long started, long duration) {
        final long elapsed = nanos.getAsLong() - started;
        if (elapsed < 0 || elapsed >= duration) {
            throw new IllegalStateException("checkpoint Registry lookup deadline expired or clock moved backwards");
        }
    }
}
