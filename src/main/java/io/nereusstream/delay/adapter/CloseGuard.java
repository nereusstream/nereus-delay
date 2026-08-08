package io.nereusstream.delay.adapter;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Fences new work as soon as close is requested while allowing a failed
 * underlying close operation to be retried.  A transport close can fail after
 * it has released only part of its resources; treating the first failure as
 * terminal would make a later lifecycle retry a no-op and could strand the
 * channel or producer.
 */
final class CloseGuard {
    private boolean requested;
    private boolean completed;
    private boolean closing;

    synchronized boolean isClosed() {
        return requested;
    }

    /**
     * Atomically accepts one synchronous transport invocation against the
     * same gate that linearizes {@link #close(Runnable)}.  A plain
     * {@code isClosed()} check followed by a call is not sufficient: close
     * could otherwise complete between the check and the transport
     * invocation.  Once accepted, the invocation is allowed to finish while
     * close performs its retryable teardown; it is an already accepted call,
     * not new work admitted after the close linearization point.
     */
    <T> T invokeIfOpen(final Supplier<T> action, final Supplier<T> closedValue) {
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(closedValue, "closedValue");
        synchronized (this) {
            if (requested) {
                return closedValue.get();
            }
        }
        // The open decision is the invocation's linearization point.  Do not
        // hold the guard while executing transport code: a synchronous
        // transport may block, and close must still be able to fence future
        // calls and run its retryable teardown.
        return action.get();
    }

    void close(final Runnable action) {
        Objects.requireNonNull(action, "action");
        synchronized (this) {
            if (completed) {
                return;
            }
            requested = true;
            if (closing) {
                return;
            }
            // Keep completed=false when the action throws.  The next close
            // call remains fenced against new work but can retry the native
            // teardown.
            closing = true;
        }
        try {
            action.run();
            synchronized (this) {
                completed = true;
            }
        } finally {
            synchronized (this) {
                closing = false;
            }
        }
    }
}
