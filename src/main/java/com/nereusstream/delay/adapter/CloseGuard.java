package com.nereusstream.delay.adapter;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Fences new work as soon as close is requested while allowing a failed
 * underlying close operation to be retried. A transport close can fail after
 * it has released only part of its resources; treating the first failure as
 * terminal would make a later lifecycle retry a no-op and could strand the
 * channel or producer.
 */
final class CloseGuard {
    private boolean requested;
    private boolean completed;
    private boolean closing;
    /** Number of transport invocations accepted before the close fence. */
    private int acceptedInvocations;

    synchronized boolean isClosed() {
        return requested;
    }

    /**
     * Atomically accepts one synchronous transport invocation against the
     * same gate that linearizes {@link #close(Runnable)}. A plain
     * {@code isClosed()} check followed by a call is not sufficient: close
     * could otherwise complete between the check and the transport
     * invocation. Once accepted, the invocation is allowed to finish while
     * close performs its retryable teardown; it is an already accepted call,
     * not new work admitted after the close linearization point.
     */
    <T> T invokeIfOpen(final Supplier<T> action, final Supplier<T> closedValue) {
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(closedValue, "closedValue");
        final boolean accepted;
        synchronized (this) {
            if (requested) {
                accepted = false;
            } else {
                // This increment is the invocation's linearization point.
                // Once it is visible, close() may fence future calls but
                // must treat this one as already accepted even though the
                // potentially blocking action runs outside the monitor.
                acceptedInvocations++;
                accepted = true;
            }
        }
        if (!accepted) {
            return closedValue.get();
        }
        // Do not hold the guard while executing transport code: a synchronous
        // transport may block, and close must still be able to fence future
        // calls and run its retryable teardown. The accepted-invocation
        // count makes the decision above atomic with the close fence; there
        // is no check-then-call window after the lock is released.
        try {
            return action.get();
        } finally {
            synchronized (this) {
                if (acceptedInvocations <= 0) {
                    throw new IllegalStateException("close guard invocation accounting underflow");
                }
                acceptedInvocations--;
            }
        }
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
            // Keep completed=false when the action throws. The next close
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
