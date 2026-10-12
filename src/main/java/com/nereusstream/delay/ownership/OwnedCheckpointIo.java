package com.nereusstream.delay.ownership;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/** Owns the exact queued Runnable and serializes no-start proof against the actual executor start. */
public final class OwnedCheckpointIo<T> implements Runnable {
    private final Function<Runnable, T> operation;
    private final Runnable guard;
    private final ThreadPoolExecutor executor;
    private final CompletableFuture<T> receipt = new CompletableFuture<>();
    private final CheckpointInputFuture<T> view = CheckpointInputFuture.track(receipt, this::requestStop);
    private boolean started;
    private boolean stopping;
    private boolean exited;

    public static <T> OwnedCheckpointIo<T> submit(Function<Runnable, T> operation, Runnable guard,
            ThreadPoolExecutor executor) {
        final var pool = Objects.requireNonNull(executor, "executor");
        if (pool.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) pool.getQueue().size() + pool.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || pool.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("checkpoint executor requires finite threads/queue and AbortPolicy");
        }
        final var owned = new OwnedCheckpointIo<>(Objects.requireNonNull(operation, "operation"),
                Objects.requireNonNull(guard, "guard"), pool);
        pool.execute(owned);
        return owned;
    }

    private OwnedCheckpointIo(Function<Runnable, T> operation, Runnable guard, ThreadPoolExecutor executor) {
        this.operation = operation;
        this.guard = guard;
        this.executor = executor;
    }

    public CompletableFuture<T> receipt() { return view.copy(); }

    public synchronized boolean ioExited() { return exited && view.ioExited(); }

    public void requestStop() {
        synchronized (this) {
            if (exited) { return; }
            stopping = true;
            if (started) { return; }
            exited = true;
        }
        executor.remove(this);
        receipt.completeExceptionally(new IllegalStateException("checkpoint input stopped before I/O"));
    }

    private void requireCurrent() {
        requireNotStopped();
        guard.run();
        requireNotStopped();
    }

    private synchronized void requireNotStopped() {
        if (stopping) { throw new IllegalStateException("checkpoint input I/O stop requested"); }
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
                failure = new IllegalStateException("checkpoint input I/O stop requested");
            }
            exited = true;
        }
        if (failure == null) { receipt.complete(value); }
        else { receipt.completeExceptionally(failure); }
    }
}
