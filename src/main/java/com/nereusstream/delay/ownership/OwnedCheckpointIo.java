package com.nereusstream.delay.ownership;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/** Owns the exact queued Runnable and serializes no-start proof against the actual executor start. */
public final class OwnedCheckpointIo<T> implements Runnable {
    /** An explicit local stop, distinct from an operation, authority or provider failure. */
    public static final class StoppedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private StoppedException(String message) { super(message); }
    }
    private final Function<Runnable, T> operation;
    private final Runnable guard;
    private final ThreadPoolExecutor executor;
    private final java.util.function.Consumer<T> discard;
    private final CompletableFuture<T> receipt = new CompletableFuture<>();
    private final CheckpointInputFuture<T> view = CheckpointInputFuture.track(receipt, this::requestStop,
            this::retryCleanup, this::cleanupIdle);
    private OwnedCheckpointIo<CompletableFuture<Void>> cleanupAttempt;
    private CompletableFuture<Void> cleanupCompletion;
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
        return submitResource(operation, guard, null, pool);
    }

    /** A newly opened native value must be closed if the final guard/stop rejects transfer to its caller. */
    public static <T> OwnedCheckpointIo<T> submitResource(Function<Runnable, T> operation, Runnable guard,
            java.util.function.Consumer<T> discard, ThreadPoolExecutor pool) {
        Objects.requireNonNull(pool, "executor");
        if (pool.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) pool.getQueue().size() + pool.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || pool.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("owned I/O requires finite threads/queue and AbortPolicy");
        }
        final var owned = new OwnedCheckpointIo<>(Objects.requireNonNull(operation, "operation"),
                Objects.requireNonNull(guard, "guard"), pool, discard);
        pool.execute(owned);
        return owned;
    }

    private OwnedCheckpointIo(Function<Runnable, T> operation, Runnable guard, ThreadPoolExecutor executor,
            java.util.function.Consumer<T> discard) {
        this.operation = operation;
        this.guard = guard;
        this.executor = executor;
        this.discard = discard;
    }

    public CompletableFuture<T> receipt() { return view.copy(); }

    /** Actual retained success, even if a detached receipt was cancelled; used to close an untransferred SDK. */
    public java.util.Optional<T> completedValue() {
        if (!receipt.isDone() || receipt.isCompletedExceptionally()) { return java.util.Optional.empty(); }
        return java.util.Optional.ofNullable(receipt.join());
    }

    public synchronized boolean ioExited() { return exited && view.ioExited(); }
    /** The original queued method has exited; retained native/protection cleanup may still remain. */
    public synchronized boolean operationExited() { return exited; }

    /** One explicit same-handle cleanup request on the original executor; never closes the shared client. */
    public synchronized void retryCleanup() {
        final var failure = view.ownedFailure();
        if (!exited || !(failure instanceof CheckpointInputFuture.Cleanup cleanup) || cleanup.cleanupComplete()) {
            return;
        }
        if (cleanupAttempt != null && !cleanupIdle()) { return; }
        try {
            final var next = submit(ignored -> {
                final var closing = Objects.requireNonNull(cleanup.retryCleanup(), "cleanup future");
                synchronized (this) { cleanupCompletion = closing; }
                return closing;
            }, () -> {}, executor);
            // The task cannot publish its completion under this monitor before this assignment.
            cleanupCompletion = null;
            cleanupAttempt = next;
        } catch (java.util.concurrent.RejectedExecutionException full) {
            if (executor.isShutdown()) { throw full; }
            // No task was accepted. Keep the previous exited handle and original cleanup obligation.
        }
    }

    private synchronized boolean cleanupIdle() {
        if (cleanupAttempt == null) { return true; }
        if (!cleanupAttempt.ioExited()) { return false; }
        return cleanupCompletion == null || cleanupCompletion.isDone();
    }

    public void requestStop() {
        synchronized (this) {
            if (exited) {
                if (cleanupAttempt != null) { cleanupAttempt.requestStop(); }
                return;
            }
            stopping = true;
            if (started) { return; }
            exited = true;
        }
        executor.remove(this);
        receipt.completeExceptionally(new StoppedException("checkpoint input stopped before I/O"));
    }

    private void requireCurrent() {
        requireNotStopped();
        guard.run();
        requireNotStopped();
    }

    private synchronized void requireNotStopped() {
        if (stopping) { throw new StoppedException("checkpoint input I/O stop requested"); }
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
                failure = new StoppedException("checkpoint input I/O stop requested");
            }
            if(failure==null)exited=true;
        }
        if (failure != null && value != null && discard != null) {
            try { discard.accept(value); }
            catch (Throwable cleanup) { if(cleanup!=failure)cleanup.addSuppressed(failure); failure = cleanup; }
        }
        synchronized (this) { exited = true; }
        if (failure == null) { receipt.complete(value); }
        else { receipt.completeExceptionally(failure); }
    }
}
