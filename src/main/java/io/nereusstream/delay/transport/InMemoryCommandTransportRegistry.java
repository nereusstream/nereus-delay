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
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        final ArrayList<CommandTransport> values = new ArrayList<>(transports.values());
        transports.clear();
        RuntimeException first = null;
        for (CommandTransport transport : values) {
            try {
                transport.close();
            } catch (RuntimeException failure) {
                if (first == null) {
                    first = failure;
                } else {
                    first.addSuppressed(failure);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
