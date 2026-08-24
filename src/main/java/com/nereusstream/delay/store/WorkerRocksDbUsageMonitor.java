package com.nereusstream.delay.store;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Periodically observes the physical usage of every open shard DB in a
 * Worker. The monitor owns no RocksDB handles; its source is registered by
 * the shared resource owner and is removed before a store starts native
 * teardown.
 *
 * <p>An observation failure is evidence failure, not an indication that the
 * DB is empty. The failure callback therefore fences the same Worker safety
 * gate used by the runtime envelope monitor.</p>
 */
public final class WorkerRocksDbUsageMonitor implements AutoCloseable {
    private final Duration interval;
    private final Supplier<List<RocksDbUsageSnapshot>> probe;
    private final Consumer<Throwable> failureConsumer;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<List<RocksDbUsageSnapshot>> lastObservation = new AtomicReference<>(List.of());
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();
    private ScheduledFuture<?> scheduledProbe;
    private boolean closeCompleted;

    /** Starts a daemon physical-usage monitor for one Worker. */
    public static WorkerRocksDbUsageMonitor start(
            final Duration interval,
            final Supplier<List<RocksDbUsageSnapshot>> probe,
            final Consumer<Throwable> failureConsumer) {
        final WorkerRocksDbUsageMonitor monitor =
                new WorkerRocksDbUsageMonitor(interval, probe, failureConsumer, daemonThreadExecutor());
        monitor.start();
        return monitor;
    }

    /** Deterministic constructor used by tests and lifecycle integrations. */
    WorkerRocksDbUsageMonitor(
            final Duration interval,
            final Supplier<List<RocksDbUsageSnapshot>> probe,
            final Consumer<Throwable> failureConsumer,
            final ScheduledExecutorService executor) {
        this.interval = requireInterval(interval);
        this.probe = Objects.requireNonNull(probe, "probe");
        this.failureConsumer = Objects.requireNonNull(failureConsumer, "failureConsumer");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /** Starts periodic observation; repeated starts are idempotent. */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("RocksDB usage monitor is closed");
        }
        if (scheduledProbe != null) {
            return;
        }
        final long intervalNanos;
        try {
            intervalNanos = interval.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("RocksDB usage interval overflows nanoseconds", overflow);
        }
        scheduledProbe =
                executor.scheduleWithFixedDelay(this::runScheduledProbe, 0, intervalNanos, TimeUnit.NANOSECONDS);
    }

    /** Runs one bounded physical observation immediately. */
    public void pollNow() {
        if (closed.get()) {
            throw new IllegalStateException("RocksDB usage monitor is closed");
        }
        try {
            final List<RocksDbUsageSnapshot> observation =
                    Objects.requireNonNull(probe.get(), "RocksDB usage probe returned null");
            lastObservation.set(List.copyOf(observation));
        } catch (RuntimeException | Error failure) {
            recordFailure(failure);
        }
    }

    /** Returns the last complete observation, or an empty list before one. */
    public List<RocksDbUsageSnapshot> lastObservation() {
        return lastObservation.get();
    }

    public Throwable lastFailure() {
        return lastFailure.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public synchronized void close() {
        if (closeCompleted) {
            return;
        }
        closed.set(true);
        Throwable closeFailure = null;
        if (scheduledProbe != null) {
            try {
                scheduledProbe.cancel(false);
                scheduledProbe = null;
            } catch (RuntimeException | Error failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        try {
            executor.shutdownNow();
        } catch (RuntimeException | Error failure) {
            closeFailure = appendCloseFailure(closeFailure, failure);
        }
        if (closeFailure != null) {
            throwUnchecked(closeFailure);
        }
        closeCompleted = true;
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    private void runScheduledProbe() {
        if (!closed.get()) {
            pollNow();
        }
    }

    private void recordFailure(final Throwable failure) {
        lastFailure.compareAndSet(null, failure);
        try {
            failureConsumer.accept(failure);
        } catch (RuntimeException | Error secondary) {
            if (secondary != failure) {
                failure.addSuppressed(secondary);
            }
        }
    }

    private static Duration requireInterval(final Duration value) {
        Objects.requireNonNull(value, "interval");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("RocksDB usage interval must be positive");
        }
        return value;
    }

    private static ScheduledExecutorService daemonThreadExecutor() {
        final ThreadFactory factory = runnable -> {
            final Thread thread = new Thread(runnable, "nereus-delay-rocksdb-usage");
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }
}
