package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Periodically gives the Target fleet one bounded reservation GC turn. */
public final class TargetWorkerMaintenanceLoop implements AutoCloseable {
    private final TargetWorkerShardFleetRuntime fleet;
    private final SchedulerBudget budget;
    private final Consumer<Throwable> failureConsumer;
    private final ScheduledExecutorService executor;
    private final boolean ownsExecutor;
    private final long intervalNanos;
    private final Object turnLock = new Object();
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private volatile boolean closed;
    private volatile Thread activeTurnThread;
    private ScheduledFuture<?> scheduled;

    /**
     * Starts one daemon maintenance scheduler. The failure callback is diagnostic; the host must
     * close this loop before deciding and performing Owner drain.
     */
    public static TargetWorkerMaintenanceLoop start(
            final TargetWorkerShardFleetRuntime fleet,
            final SchedulerBudget budget,
            final Duration interval,
            final Consumer<Throwable> failureConsumer) {
        final var executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final var thread = new Thread(runnable, "nereus-delay-target-reservation-gc");
            thread.setDaemon(true);
            return thread;
        });
        final TargetWorkerMaintenanceLoop loop;
        try {
            loop = new TargetWorkerMaintenanceLoop(fleet, budget, interval, failureConsumer, executor, true);
            loop.start();
        } catch (RuntimeException | Error failure) {
            executor.shutdown();
            throw failure;
        }
        return loop;
    }

    /** Injected scheduler seam for host integration and deterministic lifecycle tests. */
    TargetWorkerMaintenanceLoop(
            final TargetWorkerShardFleetRuntime fleet,
            final SchedulerBudget budget,
            final Duration interval,
            final Consumer<Throwable> failureConsumer,
            final ScheduledExecutorService executor) {
        this(fleet, budget, interval, failureConsumer, executor, false);
    }

    private TargetWorkerMaintenanceLoop(
            final TargetWorkerShardFleetRuntime fleet,
            final SchedulerBudget budget,
            final Duration interval,
            final Consumer<Throwable> failureConsumer,
            final ScheduledExecutorService executor,
            final boolean ownsExecutor) {
        this.fleet = Objects.requireNonNull(fleet, "fleet");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.failureConsumer = Objects.requireNonNull(failureConsumer, "failureConsumer");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownsExecutor = ownsExecutor;
        final var exactInterval = Objects.requireNonNull(interval, "interval");
        if (exactInterval.isZero() || exactInterval.isNegative()) {
            throw new IllegalArgumentException("Target maintenance interval must be positive");
        }
        try {
            intervalNanos = exactInterval.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Target maintenance interval exceeds nanoseconds", overflow);
        }
    }

    /** Starts fixed-delay turns; a normal turn failure does not suppress the next tick. */
    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Target maintenance loop is closed");
        }
        if (scheduled == null) {
            scheduled = Objects.requireNonNull(
                    executor.scheduleWithFixedDelay(this::runScheduledTick, 0, intervalNanos, TimeUnit.NANOSECONDS),
                    "maintenance schedule");
        }
    }

    /** Runs a tick immediately under the same single-turn lock as the scheduler. */
    public void pollNow() {
        runTick(true);
    }

    public Throwable firstFailure() {
        return firstFailure.get();
    }

    private void runScheduledTick() {
        runTick(false);
    }

    private void runTick(final boolean requireOpen) {
        Throwable failure = null;
        synchronized (turnLock) {
            if (closed) {
                if (requireOpen) {
                    throw new IllegalStateException("Target maintenance loop is closed");
                }
                return;
            }
            activeTurnThread = Thread.currentThread();
            try {
                fleet.runNextMaintenanceTurnIfPresent(budget);
            } catch (RuntimeException | Error caught) {
                failure = caught;
                firstFailure.compareAndSet(null, caught);
            } finally {
                activeTurnThread = null;
            }
        }
        if (failure != null) {
            try {
                failureConsumer.accept(failure);
            } catch (RuntimeException callbackFailure) {
                if (callbackFailure != failure) {
                    failure.addSuppressed(callbackFailure);
                }
            } catch (Error callbackFatal) {
                if (callbackFatal != failure) {
                    failure.addSuppressed(callbackFatal);
                }
                if (failure instanceof Error originalFatal) {
                    throw originalFatal;
                }
                throw callbackFatal;
            }
        }
        if (failure instanceof Error fatal) {
            throw fatal;
        }
    }

    /** Cancels future ticks and waits until any in-flight GC turn has left the Worker graph. */
    @Override
    public void close() {
        if (activeTurnThread == Thread.currentThread()) {
            throw new IllegalStateException("cannot close Target maintenance loop from its GC turn");
        }
        Throwable closeFailure = null;
        synchronized (this) {
            closed = true;
            if (scheduled != null) {
                try {
                    scheduled.cancel(false);
                } catch (RuntimeException | Error failure) {
                    closeFailure = failure;
                }
                scheduled = null;
            }
            if (ownsExecutor) {
                try {
                    executor.shutdown();
                } catch (RuntimeException | Error failure) {
                    if (closeFailure == null) {
                        closeFailure = failure;
                    } else if (closeFailure != failure) {
                        closeFailure.addSuppressed(failure);
                    }
                }
            }
        }
        synchronized (turnLock) {
            // The lock is the in-flight turn boundary. Owner drain may start after it is released.
        }
        if (closeFailure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (closeFailure instanceof Error errorFailure) {
            throw errorFailure;
        }
    }
}
