package io.nereusstream.delay.transport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Deterministic registry used by the SDK composition and conformance tests. */
public final class InMemoryCommandTransportRegistry implements CommandTransportRegistry {
    private final Map<CommandTransportKey, CommandTransport> transports = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean closeCompleted;

    public synchronized void register(final CommandTransport transport) {
        Objects.requireNonNull(transport, "transport");
        if (closed.get()) {
            throw new IllegalStateException("transport registry is closed");
        }
        final CommandTransport previous = transports.putIfAbsent(transport.key(), transport);
        if (previous != null) {
            throw new IllegalArgumentException("transport key is already registered");
        }
    }

    @Override
    public synchronized CommandTransport exact(final CommandTransportKey key) {
        Objects.requireNonNull(key, "key");
        if (closed.get()) {
            return null;
        }
        return transports.get(key);
    }

    @Override
    public synchronized void close() {
        if (closeCompleted) {
            return;
        }
        closed.set(true);
        Throwable first = null;
        final ArrayList<Map.Entry<CommandTransportKey, CommandTransport>> entries =
                new ArrayList<>(transports.entrySet());
        for (Map.Entry<CommandTransportKey, CommandTransport> entry : entries) {
            try {
                entry.getValue().close();
                transports.remove(entry.getKey(), entry.getValue());
            } catch (RuntimeException | Error failure) {
                first = appendCloseFailure(first, failure);
            }
        }
        if (first != null) {
            throwUnchecked(first);
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
}
