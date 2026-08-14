package io.nereusstream.delay.transport;

/** Exact key lookup for guarded command transports. */
public interface CommandTransportRegistry extends AutoCloseable {
    CommandTransport exact(CommandTransportKey key);

    @Override
    default void close() {
        // Implementations close registered transports.
    }
}
