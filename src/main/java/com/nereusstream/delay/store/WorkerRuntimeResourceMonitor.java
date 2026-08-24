package com.nereusstream.delay.store;

import java.nio.file.Path;
import java.time.Duration;
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
 * Periodically revalidates the Worker runtime resource envelope.
 *
 * <p>The monitor is deliberately separate from the RocksDB resource owner so
 * its lifecycle is explicit: a service starts one after startup validation and
 * closes it before releasing the shared resources.  Any probe or validation
 * failure is recorded as a sticky {@code DRAIN_OR_MIGRATE} safety breach; a
 * later healthy observation cannot silently reopen business admission.</p>
 */
public final class WorkerRuntimeResourceMonitor implements AutoCloseable {
    private final Duration interval;
    private final Supplier<WorkerRuntimeResourceObservation> probe;
    private final Consumer<WorkerRuntimeResourceObservation> observationConsumer;
    private final Consumer<Throwable> failureConsumer;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();
    private ScheduledFuture<?> scheduledProbe;
    private boolean closeCompleted;

    /** Starts a daemon monitor for the supplied Worker resource envelope. */
    public static WorkerRuntimeResourceMonitor start(
            final Path rootPath, final Duration interval, final SharedRocksDbResources resources) {
        Objects.requireNonNull(rootPath, "rootPath");
        Objects.requireNonNull(resources, "resources");
        final WorkerRuntimeResourceMonitor monitor = new WorkerRuntimeResourceMonitor(
                interval,
                () -> WorkerRuntimeResourceProbe.observe(rootPath),
                resources::revalidateRuntime,
                resources::recordRuntimeProbeFailure,
                daemonThreadExecutor());
        monitor.start();
        return monitor;
    }

    /**
     * Creates an explicitly controlled monitor.  The injected executor is a
     * deterministic seam for tests and platform lifecycle integration.
     */
    WorkerRuntimeResourceMonitor(
            final Duration interval,
            final Supplier<WorkerRuntimeResourceObservation> probe,
            final Consumer<WorkerRuntimeResourceObservation> observationConsumer,
            final Consumer<Throwable> failureConsumer,
            final ScheduledExecutorService executor) {
        this.interval = requireInterval(interval);
        this.probe = Objects.requireNonNull(probe, "probe");
        this.observationConsumer = Objects.requireNonNull(observationConsumer, "observationConsumer");
        this.failureConsumer = Objects.requireNonNull(failureConsumer, "failureConsumer");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /** Starts periodic probing; repeated starts are idempotent. */
    public synchronized void start() {
        if (closed.get()) {
            throw new IllegalStateException("runtime resource monitor is closed");
        }
        if (scheduledProbe != null) {
            return;
        }
        final long intervalNanos;
        try {
            intervalNanos = interval.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("runtime probe interval overflows nanoseconds", overflow);
        }
        scheduledProbe =
                executor.scheduleWithFixedDelay(this::runScheduledProbe, 0, intervalNanos, TimeUnit.NANOSECONDS);
    }

    /** Runs one bounded probe immediately, without waiting for the scheduler. */
    public void pollNow() {
        if (closed.get()) {
            throw new IllegalStateException("runtime resource monitor is closed");
        }
        try {
            final WorkerRuntimeResourceObservation observation =
                    Objects.requireNonNull(probe.get(), "runtime probe returned null");
            observationConsumer.accept(observation);
        } catch (RuntimeException | Error failure) {
            recordFailure(failure);
        }
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
            throw new IllegalArgumentException("runtime probe interval must be positive");
        }
        return value;
    }

    private static ScheduledExecutorService daemonThreadExecutor() {
        final ThreadFactory factory = runnable -> {
            final Thread thread = new Thread(runnable, "nereus-delay-runtime-probe");
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }
}
