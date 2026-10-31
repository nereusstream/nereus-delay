package com.nereusstream.delay.ownership;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/** Tracks one owned Source observation cursor, including late open and failed close; no executor or cancellation. */
public final class OwnedSourceCursor<T> {
    private final CompletableFuture<T> opening;
    private final Function<T, CompletableFuture<Void>> closer;
    private final TargetOwnedBackendGroup backend;
    private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
    private T resource;
    private boolean reading;
    private boolean closing;
    private boolean closeInFlight;
    private Throwable openFailure;
    private Throwable closeFailure;

    public OwnedSourceCursor(final CompletableFuture<T> opening, final Function<T, CompletableFuture<Void>> closer) {
        this(opening, closer, null);
    }

    public OwnedSourceCursor(
            final CompletableFuture<T> opening,
            final Function<T, CompletableFuture<Void>> closer,
            final TargetOwnedBackendGroup backend) {
        this.backend = backend;
        this.opening = Objects.requireNonNull(opening, "opening");
        this.closer = Objects.requireNonNull(closer, "closer");
        opening.whenComplete((opened, failure) -> {
            synchronized (this) {
                if (failure != null || opened == null) {
                    openFailure = failure == null
                            ? new IllegalStateException("Source cursor opening returned null")
                            : failure;
                    // A failed open can precede SDK cleanup. Only backend-owner shutdown can settle this case.
                } else {
                    resource = opened;
                }
            }
            tryClose();
        });
    }

    public <R> R read(final long timeoutMs, final Function<T, R> action)
            throws InterruptedException, ExecutionException, TimeoutException {
        Objects.requireNonNull(action, "action");
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("Source cursor wait must be finite and positive");
        }
        synchronized (this) {
            if (closing || reading) {
                throw new IllegalStateException("Source cursor is closing or already in use");
            }
            reading = true;
        }
        try {
            return action.apply(opening.get(timeoutMs, TimeUnit.MILLISECONDS));
        } finally {
            endRead();
        }
    }

    /** Always call after beginRead, including after its timeout/failure; close never races an active SDK read. */
    private void endRead() {
        synchronized (this) {
            if (!reading) {
                throw new IllegalStateException("Source cursor has no active read");
            }
            reading = false;
        }
        tryClose();
    }

    /** Returns a detached future; caller cancellation/completion cannot alter the owned cleanup authority. */
    public CompletableFuture<Void> requestClose() {
        synchronized (this) {
            closing = true;
        }
        tryClose();
        return cleanup.copy();
    }

    /** Retries only cleanup of the same native handle; never opens a replacement cursor. */
    public CompletableFuture<Void> retryClose() {
        synchronized (this) {
            closing = true;
            if (!closeInFlight) {
                closeFailure = null;
            }
        }
        tryClose();
        return cleanup.copy();
    }

    public synchronized boolean cleanupComplete() {
        return cleanup.isDone() && !cleanup.isCompletedExceptionally();
    }

    public synchronized boolean readActive() {
        return reading;
    }

    public synchronized boolean operationExited() {
        return !reading && opening.isDone();
    }

    public synchronized Throwable cleanupFailure() {
        return closeFailure == null ? openFailure : closeFailure;
    }

    public synchronized boolean failedOpenNeedsBackendShutdown() {
        return closing && !reading && opening.isDone() && openFailure != null && !cleanup.isDone();
    }

    /** Provider holds its open-start lock and proves no SDK construction/subscription was invoked. */
    public void abandonBeforeOpen() {
        synchronized (this) {
            if (reading || resource != null || opening.isDone() || closeInFlight) {
                throw new IllegalStateException("Source cursor opening already started or completed");
            }
            closing = true;
        }
        opening.completeExceptionally(new IllegalStateException("Source cursor closed before SDK opening"));
        cleanup.complete(null);
    }

    /**
     * Provider must call only after the exact SDK backend owner's shutdown has completed. This settles failed open
     * whose native cursor is unavailable; it never cancels open or an active read, or releases other physical proof.
     */
    public void backendShutdownCompleted() {
        synchronized (this) {
            if (!closing || reading || !opening.isDone() || openFailure == null) {
                throw new IllegalStateException("Source failed-open cleanup lacks completed backend shutdown scope");
            }
        }
        cleanup.complete(null);
    }

    /** Only the original registered owner can supply the actual native termination fact, after its Host permit. */
    public void backendShutdownCompleted(TargetOwnedBackendGroup original) {
        if (backend == null
                || backend != original
                || !backend.nativeResourcesTerminated()
                || backend.applicationReady()) {
            throw new IllegalStateException("Source cursor has no original terminated backend owner");
        }
        synchronized (this) {
            if (!closing || reading || !opening.isDone()) {
                throw new IllegalStateException("Source operation still has live opening/read work");
            }
            resource = null;
            closeInFlight = false;
        }
        cleanup.complete(null);
    }

    private void tryClose() {
        final T selected;
        synchronized (this) {
            if (!closing || reading || resource == null || closeInFlight || closeFailure != null || cleanup.isDone()) {
                return;
            }
            closeInFlight = true;
            selected = resource;
        }
        final CompletableFuture<Void> closingFuture;
        try {
            closingFuture = Objects.requireNonNull(closer.apply(selected), "Source cursor close future");
        } catch (Throwable failure) {
            recordCloseFailure(failure);
            return;
        }
        closingFuture.whenComplete((ignored, failure) -> {
            if (failure != null) {
                recordCloseFailure(failure);
            } else {
                synchronized (this) {
                    closeInFlight = false;
                    resource = null;
                }
                cleanup.complete(null);
            }
        });
    }

    private synchronized void recordCloseFailure(final Throwable failure) {
        closeInFlight = false;
        closeFailure = failure;
    }
}
