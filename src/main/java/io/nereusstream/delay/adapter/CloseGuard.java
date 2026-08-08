package io.nereusstream.delay.adapter;

import java.util.Objects;

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

    synchronized void close(final Runnable action) {
        Objects.requireNonNull(action, "action");
        if (completed) {
            return;
        }
        requested = true;
        if (closing) {
            return;
        }
        // Keep completed=false when the action throws.  The next close call
        // remains fenced against new work but can retry the native teardown.
        closing = true;
        try {
            action.run();
            completed = true;
        } finally {
            closing = false;
        }
    }
}
