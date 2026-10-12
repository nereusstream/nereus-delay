package com.nereusstream.delay.ownership;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Detached checkpoint input view retaining the provider's actual I/O and native cleanup completion. */
public final class CheckpointInputFuture<T> extends CompletableFuture<T> {
    /** A provider retains the exact native cleanup handle after its read method returned exceptionally. */
    public interface Cleanup {
        boolean cleanupComplete();
    }

    private final CompletableFuture<?> owned;

    private CheckpointInputFuture(CompletableFuture<?> owned) { this.owned = owned; }

    /** Caller must keep the owned future private and complete it only after the actual read method exits. */
    public static <T> CheckpointInputFuture<T> track(CompletableFuture<T> owned) {
        final var view = new CheckpointInputFuture<T>(Objects.requireNonNull(owned, "owned"));
        owned.whenComplete((value, failure) -> {
            if (failure == null) { view.complete(value); }
            else { view.completeExceptionally(failure); }
        });
        return view;
    }

    /** Dependent/copy views keep the same provider I/O fact even if their own result is cancelled or replaced. */
    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() { return new CheckpointInputFuture<U>(owned); }

    public boolean ioExited() {
        if (!owned.isDone()) { return false; }
        final var failure = ownedFailure();
        return !(failure instanceof Cleanup cleanup) || cleanup.cleanupComplete();
    }

    public Throwable ownedFailure() {
        if (!owned.isDone()) { return null; }
        try { owned.join(); return null; }
        catch (RuntimeException failure) {
            Throwable actual = failure;
            while (actual instanceof CompletionException && actual.getCause() != null) { actual = actual.getCause(); }
            return actual;
        }
    }

    public static boolean ioExited(CompletableFuture<?> future) {
        return future == null || (future instanceof CheckpointInputFuture<?> tracked
                ? tracked.ioExited() : future.isDone());
    }

    public static Throwable ownedFailure(CompletableFuture<?> future) {
        return future instanceof CheckpointInputFuture<?> tracked ? tracked.ownedFailure() : null;
    }
}
