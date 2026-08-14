package io.nereusstream.delay.transport;

import java.util.concurrent.CompletionStage;

/** Guarded physical send boundary. */
public interface CommandTransport extends AutoCloseable {
    CommandTransportKey key();

    CompletionStage<? extends TransportResult> send(TransportRequest request,
                                                     TransportOwnershipPermit ownershipPermit);

    @Override
    default void close() {
        // Implementations close their client/producer resources here.
    }

}
